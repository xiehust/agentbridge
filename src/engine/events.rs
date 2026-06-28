//! Event loop processor for the new engine.
//!
//! Behavior per event kind:
//! - Text → StreamPreview with throttled live edits
//! - ToolUse/Thinking → freeze and detach preview
//! - PermissionRequest → show buttons, BLOCK waiting for user decision
//! - Result → finalize preview
//! - Error → discard preview

#![allow(dead_code)] // EventLoopResult.output_tokens reserved for future telemetry

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::mpsc;

use crate::agent::PermissionResponder;
use crate::config::DisplayConfig;
use crate::core::event::AgentEvent;
use crate::core::platform::{Button, PlatformCapabilities, PreviewHandle, ReplyCtx};
use crate::core::streaming::StreamPreview;
use crate::engine::PermissionDecision;

/// Idle timeout for detecting stalled agent sessions (5 minutes).
const EVENT_IDLE_TIMEOUT: Duration = Duration::from_secs(300);

/// Outcome of processing the agent event stream.
pub struct EventLoopResult {
    /// The complete response text produced by the agent.
    pub final_text: String,
    /// Agent session ID returned in the Result event (if any).
    pub session_id: Option<String>,
    /// Token usage from the Result event.
    pub input_tokens: u32,
    pub output_tokens: u32,
}

/// Process agent events, driving the streaming preview and handling
/// tool-use notifications, permission requests, and the final result.
///
/// `perm_rx` receives permission decisions from the message handler when
/// the user responds to a permission prompt (Allow/Deny/Allow All).
/// `agent_stdin` is used to write control_response back to the agent.
/// `approve_all` tracks whether the user has chosen "Allow All" for this session.
#[allow(clippy::too_many_arguments)] // event loop needs many context refs; context struct would not reduce complexity
pub async fn process_agent_events(
    platform: &Arc<dyn PlatformCapabilities>,
    ctx: &dyn ReplyCtx,
    rx: &mut mpsc::Receiver<AgentEvent>,
    perm_rx: &mut mpsc::Receiver<PermissionDecision>,
    responder: &Arc<dyn PermissionResponder>,
    approve_all: &mut bool,
    pending_flag: &Arc<AtomicBool>,
    stopped_flag: &Arc<AtomicBool>,
    stop_typing_in: Option<Box<dyn FnOnce() + Send>>,
    display: &DisplayConfig,
    event_broadcast: &Arc<tokio::sync::broadcast::Sender<(String, crate::core::event::AgentEvent)>>,
    session_key_for_broadcast: &str,
    // When true (the tmux+hook backend), tool-use events are coalesced into a
    // single in-place-edited progress message instead of one chat message per
    // tool — so a long, tool-heavy turn shows live progress without spamming.
    // claude/acp leave this false and keep their existing per-tool reply.
    tool_progress_inplace: bool,
) -> Result<EventLoopResult> {
    let mut preview = StreamPreview::new();
    let mut handle: Option<Box<dyn PreviewHandle>> = None;
    // Typing indicator is already started by the caller (process_and_drain)
    // before agent spawn, so the user sees feedback during spawn time.
    let mut stop_typing = stop_typing_in;
    let mut tool_count: usize = 0;
    // In-place tool-progress state (tool_progress_inplace mode only): the live
    // progress message handle and the running list of tools this turn.
    let mut progress_handle: Option<Box<dyn PreviewHandle>> = None;
    let mut progress_tools: Vec<String> = Vec::new();
    // Accumulating reply-text message (ReplyChunk events, hook mode): a SEPARATE
    // self-editing message holding the turn's inter-tool thinking/text. Kept
    // distinct from progress_handle so the two never freeze each other; once the
    // buffer nears the platform message cap it is finalized and a fresh one
    // continues (saturate-then-new), so nothing is truncated.
    let mut reply_handle: Option<Box<dyn PreviewHandle>> = None;
    let mut reply_buf: String = String::new();

    let mut result_info = EventLoopResult {
        final_text: String::new(),
        session_id: None,
        input_tokens: 0,
        output_tokens: 0,
    };

    // The overall idle deadline; recomputed each time an event resets it. We
    // poll for events on a short tick so a /stop (which sets stopped_flag) is
    // honoured PROMPTLY even when the agent sends no further events — otherwise
    // the loop would block on recv() and keep the typing indicator running
    // until the next event (which, on an interrupted tmux turn, never comes).
    const STOP_POLL: Duration = Duration::from_millis(250);
    let mut idle_deadline = tokio::time::Instant::now() + EVENT_IDLE_TIMEOUT;

    loop {
        // Honour a pending /stop before blocking again.
        if stopped_flag.load(Ordering::Acquire) {
            tracing::info!("event loop stopped by /stop command");
            discard_preview(platform, &mut handle).await;
            break;
        }

        let event = match tokio::time::timeout(STOP_POLL, rx.recv()).await {
            Ok(Some(event)) => {
                idle_deadline = tokio::time::Instant::now() + EVENT_IDLE_TIMEOUT;
                event
            }
            Ok(None) => {
                tracing::warn!("agent event channel closed unexpectedly");
                discard_preview(platform, &mut handle).await;
                break;
            }
            Err(_) => {
                // No event this tick. Loop back to re-check the stop flag; only
                // give up once the full idle timeout has elapsed with no events.
                if tokio::time::Instant::now() >= idle_deadline {
                    tracing::error!("agent session idle timeout ({:?})", EVENT_IDLE_TIMEOUT);
                    discard_preview(platform, &mut handle).await;
                    let _ = platform
                        .reply(ctx, "💤 等太久了，Agent 没响应")
                        .await;
                    break;
                }
                continue;
            }
        };

        // Broadcast event for gateway forwarding
        match event_broadcast.send((session_key_for_broadcast.to_string(), event.clone())) {
            Ok(n) => {
                tracing::info!(receivers = n, "event broadcast sent");
            }
            Err(_) => {
                tracing::warn!("event broadcast: no receivers");
            }
        }

        // Check stop signal (set by /stop command)
        if stopped_flag.load(Ordering::Acquire) {
            tracing::info!("event loop stopped by /stop command");
            discard_preview(platform, &mut handle).await;
            break;
        }

        match event {
            // ----- Streamed text -----
            AgentEvent::Text { content } => {
                if content.is_empty() {
                    continue;
                }

                if let Some(stop_fn) = stop_typing.take() {
                    stop_fn();
                }

                let should_update = preview.append_text(&content);

                if should_update {
                    let display = preview.display_text();

                    match &handle {
                        None => {
                            if let Some(updater) = platform.as_message_updater() {
                                match updater.send_preview(ctx, &display).await {
                                    Ok(h) => {
                                        handle = Some(h);
                                        preview.mark_sent();
                                    }
                                    Err(e) => {
                                        tracing::warn!(error = %e, "send_preview failed");
                                    }
                                }
                            }
                        }
                        Some(h) => {
                            if let Some(updater) = platform.as_message_updater() {
                                if let Err(e) =
                                    updater.update_preview(h.as_ref(), &display).await
                                {
                                    tracing::warn!(error = %e, "update_preview failed");
                                }
                                preview.mark_sent();
                            }
                        }
                    }
                }
            }

            // ----- Thinking -----
            AgentEvent::Thinking { content } => {
                // `verbose` is the master "show the work" switch; thinking is
                // part of the work, so it is suppressed when verbose is off.
                if display.verbose && display.thinking_messages && !content.is_empty() {
                    freeze_and_detach_preview(platform, &mut preview, &mut handle).await;
                    let max = display.thinking_max_len;
                    let truncated: String = if content.chars().count() > max {
                        format!("{}...", content.chars().take(max).collect::<String>())
                    } else {
                        content
                    };
                    let _ = platform.reply(ctx, &format!("🧠 {}", truncated)).await;
                }
            }

            // ----- Reply chunk (transcript thinking/text, hook mode) -----
            AgentEvent::ReplyChunk { content, thinking } => {
                // Gated by the master verbose switch: this is the inter-tool
                // "show the work" stream. When quiet, only the final reply ships.
                if display.verbose && !content.trim().is_empty() {
                    append_reply_chunk(
                        platform,
                        ctx,
                        &mut reply_handle,
                        &mut reply_buf,
                        &content,
                        thinking,
                    )
                    .await;
                }
            }

            // ----- Tool use notification -----
            AgentEvent::ToolUse { tool, input, .. } => {
                tool_count += 1;
                if !display.verbose || !display.tool_messages {
                    // Quiet (verbose off) or tool messages disabled — count the
                    // tool (the Result branch needs the count) but show nothing.
                } else if tool_progress_inplace {
                    // Coalesce into one in-place-edited progress message: append
                    // this tool to the running list and edit the single message,
                    // so an N-tool turn produces ~1 message, not N (FR-5.2).
                    progress_tools.push(format_tool_label(&tool, &input, display.tool_max_len));
                    let body = render_progress(&progress_tools, false);
                    if let Some(updater) = platform.as_message_updater() {
                        match progress_handle.as_ref() {
                            None => match updater.send_preview(ctx, &body).await {
                                Ok(h) => progress_handle = Some(h),
                                Err(e) => tracing::warn!(error = %e, "progress send_preview failed"),
                            },
                            Some(h) => {
                                if let Err(e) = updater.update_preview(h.as_ref(), &body).await {
                                    tracing::warn!(error = %e, "progress update_preview failed");
                                }
                            }
                        }
                    }
                } else {
                    // Existing per-tool reply (claude/acp backends): one chat
                    // message per tool, unchanged.
                    freeze_and_detach_preview(platform, &mut preview, &mut handle).await;
                    let max = display.tool_max_len;
                    let formatted_input = match tool.as_str() {
                        "Bash" => {
                            let cmd = if input.chars().count() > max {
                                format!("{}...", input.chars().take(max).collect::<String>())
                            } else {
                                input.clone()
                            };
                            format!("```bash\n{}\n```", cmd)
                        }
                        _ => {
                            if input.chars().count() > max {
                                format!("`{}...`", input.chars().take(max).collect::<String>())
                            } else {
                                format!("`{}`", input)
                            }
                        }
                    };
                    let text = format!("⚡ {} › {}", tool, formatted_input);
                    let _ = platform.reply(ctx, &text).await;
                }
            }

            // ----- Tool result -----
            AgentEvent::ToolResult { output, is_error, .. } => {
                if display.verbose && display.tool_messages && !output.is_empty() {
                    let icon = if is_error { "💥" } else { "✓" };
                    let max = display.tool_max_len;
                    let truncated: String = if output.chars().count() > max {
                        format!("{}...", output.chars().take(max).collect::<String>())
                    } else {
                        output
                    };
                    let _ = platform.reply(ctx, &format!("{} {}", icon, truncated)).await;
                }
            }

            // ----- Permission request (blocks until user decides) -----
            AgentEvent::PermissionRequest {
                request_id,
                tool,
                input,
                options,
            } => {
                // Check approve_all flag first
                if *approve_all {
                    tracing::debug!(request_id = %request_id, tool = %tool, "auto-approving (approve_all)");
                    let _ = responder.respond(&request_id, true).await;
                    let text = format!("⚡ {} › auto", tool);
                    let _ = platform.reply(ctx, &text).await;
                    continue;
                }

                // Freeze and detach preview
                freeze_and_detach_preview(platform, &mut preview, &mut handle).await;

                // Signal that a permission is pending (so handle_pending_permission activates)
                pending_flag.store(true, Ordering::Release);

                // Show permission prompt with buttons
                let input_summary = summarise_json(&input, 200);
                let text = format!(
                    "🔐 需要你的确认\n⚡ 工具: {}\n📋 参数: {}",
                    tool, input_summary
                );

                if let Some(btn_sender) = platform.as_inline_button_sender() {
                    // ACP agents supply their own options — map each to a button.
                    // Claude (options empty) falls back to the default 3-button UI.
                    let buttons = if options.is_empty() {
                        vec![
                            Button {
                                text: "👍 放行".to_string(),
                                callback_data: format!("perm_approve:{}", request_id),
                            },
                            Button {
                                text: "🚫 拦截".to_string(),
                                callback_data: format!("perm_deny:{}", request_id),
                            },
                            Button {
                                text: "⚡ 全部放行".to_string(),
                                callback_data: format!("perm_allow_all:{}", request_id),
                            },
                        ]
                    } else {
                        options
                            .iter()
                            .map(|o| Button {
                                text: o.label.clone(),
                                callback_data: format!("perm_opt:{}:{}", request_id, o.option_id),
                            })
                            .collect()
                    };
                    let _ = btn_sender.send_with_buttons(ctx, &text, &buttons).await;
                } else {
                    let hint = format!(
                        "{}\n\n回复 `allow` 允许 / `deny` 拒绝 / `allow all` 全部允许",
                        text
                    );
                    let _ = platform.reply(ctx, &hint).await;
                }

                // *** BLOCK waiting for user decision ***
                tracing::info!(request_id = %request_id, tool = %tool, "waiting for permission decision");

                let decision = tokio::time::timeout(
                    Duration::from_secs(600), // 10 min timeout for permission
                    perm_rx.recv(),
                )
                .await;

                // Clear pending flag as soon as we get a decision
                pending_flag.store(false, Ordering::Release);

                match decision {
                    Ok(Some(PermissionDecision::Allow)) => {
                        tracing::info!(request_id = %request_id, "permission: allowed");
                        let _ = responder.respond(&request_id, true).await;
                        let _ = platform.reply(ctx, "👍 已放行").await;
                    }
                    Ok(Some(PermissionDecision::Deny)) => {
                        tracing::info!(request_id = %request_id, "permission: denied");
                        let _ = responder.respond(&request_id, false).await;
                        let _ = platform.reply(ctx, "🚫 已拦截").await;
                    }
                    Ok(Some(PermissionDecision::AllowAll)) => {
                        tracing::info!(request_id = %request_id, "permission: allow all for session");
                        *approve_all = true;
                        let _ = responder.respond(&request_id, true).await;
                        let _ = platform.reply(ctx, "👍 已放行（后续自动通过）").await;
                    }
                    Ok(None) => {
                        tracing::warn!("permission channel closed, auto-denying");
                        let _ = responder.respond(&request_id, false).await;
                        break;
                    }
                    Err(_) => {
                        tracing::warn!("permission timeout (10 min), auto-denying");
                        let _ = responder.respond(&request_id, false).await;
                        let _ = platform.reply(ctx, "💤 权限确认超时，已自动拦截").await;
                    }
                }
            }

            // ----- System handshake -----
            AgentEvent::System { session_id, .. } => {
                tracing::debug!(session_id = %session_id, "agent system handshake");
            }

            // ----- Final result -----
            AgentEvent::Result {
                content,
                session_id,
                input_tokens,
                output_tokens,
            } => {
                // The empty-resume guard skips a spurious empty Result so it
                // doesn't end a turn prematurely (a claude-backend resume quirk).
                // It must NOT apply in hook mode: there an empty Result is the
                // intentional turn-end safety net (an interrupted turn fires no
                // Stop hook), and swallowing it would leave the session stuck
                // busy forever — the very hang the safety net exists to prevent.
                if !tool_progress_inplace
                    && input_tokens == 0
                    && output_tokens == 0
                    && content.is_empty()
                    && !preview.was_active()
                {
                    tracing::debug!("skipping empty resume result");
                    continue;
                }

                if let Some(stop_fn) = stop_typing.take() {
                    stop_fn();
                }

                // End-of-turn: settle the in-place progress message into its
                // final "done" form (all steps ✓), so it reads as a completed
                // trace beneath the final answer rather than a frozen "处理中…".
                if let Some(h) = progress_handle.take() {
                    if !progress_tools.is_empty() {
                        if let Some(updater) = platform.as_message_updater() {
                            let body = render_progress(&progress_tools, true);
                            if let Err(e) = updater.update_preview(h.as_ref(), &body).await {
                                tracing::warn!(error = %e, "progress finalize update failed");
                            }
                        }
                    }
                }
                progress_tools.clear();

                // The accumulating reply message (reply_handle/reply_buf) needs
                // no reset here: the loop `break`s on Result, so these locals are
                // dropped, and the next turn is a fresh `process_agent_events`
                // call with new state. The finalized message stays in the chat.

                preview.finish();

                let raw_final = if content.is_empty() && preview.was_active() {
                    preview.final_text().to_owned()
                } else if content.is_empty() {
                    "(no response)".to_string()
                } else {
                    content.clone()
                };
                // Markdown tables don't render on chat platforms (the pipes show
                // literally and misalign on a phone); rewrite them into an
                // aligned monospace code block — the closest a chat platform
                // gets to a real table.
                let mut final_text = crate::core::text_format::tables_to_aligned(&raw_final);

                // Append context indicator as percentage of context window
                if display.context_indicator && input_tokens > 0 && display.context_window > 0 {
                    let pct = (input_tokens * 100 / display.context_window).min(100);
                    final_text.push_str(&format!("\n\n[ctx: ~{}%]", pct));
                }

                if tool_count > 0 {
                    discard_preview(platform, &mut handle).await;
                    let _ = platform.reply(ctx, &final_text).await;
                } else {
                    match &handle {
                        Some(h) => {
                            if let Some(updater) = platform.as_message_updater() {
                                let _ =
                                    updater.update_preview(h.as_ref(), &final_text).await;
                            }
                        }
                        None => {
                            let _ = platform.reply(ctx, &final_text).await;
                        }
                    }
                }

                result_info = EventLoopResult {
                    final_text,
                    session_id: Some(session_id),
                    input_tokens,
                    output_tokens,
                };
                break;
            }

            // ----- Error -----
            AgentEvent::Error { message } => {
                if let Some(stop_fn) = stop_typing.take() {
                    stop_fn();
                }
                discard_preview(platform, &mut handle).await;
                let error_text = format!("💥 {}", message);
                let _ = platform.reply(ctx, &error_text).await;
                result_info.final_text = error_text;
                break;
            }
        }
    }

    if let Some(stop_fn) = stop_typing.take() {
        stop_fn();
    }

    if result_info.final_text.is_empty() && preview.was_active() {
        let text = preview.final_text().to_owned();
        discard_preview(platform, &mut handle).await;
        let _ = platform.reply(ctx, &text).await;
        result_info.final_text = text;
    }

    Ok(result_info)
}

// ---------------------------------------------------------------------------
// Tool-progress helpers (in-place mode)
// ---------------------------------------------------------------------------

/// A compact label for one tool call in the progress list, e.g. `Bash: ls -la`.
/// Truncation is char-boundary-safe (never byte-slices multibyte/CJK text).
fn format_tool_label(tool: &str, input: &str, max: usize) -> String {
    let input = input.trim();
    if input.is_empty() {
        return tool.to_string();
    }
    let shown: String = if input.chars().count() > max {
        format!("{}…", input.chars().take(max).collect::<String>())
    } else {
        input.to_string()
    };
    // Collapse newlines so a multiline command stays a single progress line.
    let shown = shown.replace('\n', " ");
    format!("{}: {}", tool, shown)
}

/// Render the running tool list into one progress message. While the turn is
/// live (`done == false`) the last tool is marked "current" (`▸`); when the
/// turn ends (`done == true`) every tool is marked done (`✓`) and the header
/// flips to a completion line, so the single edited message reads as live
/// progress and then settles into a "what I did" trace.
fn render_progress(tools: &[String], done: bool) -> String {
    if tools.is_empty() {
        return if done { "✓ 完成".to_string() } else { "⚡ 处理中…".to_string() };
    }
    let n = tools.len();
    let mut lines = Vec::with_capacity(n + 1);
    lines.push(if done {
        format!("✓ 完成 ({} 步)", n)
    } else {
        format!("⚡ 处理中… ({} 步)", n)
    });
    // Cap the visible list so a very long turn doesn't grow an unbounded
    // message; keep the most recent few (the tail is what's "current").
    const MAX_SHOWN: usize = 8;
    let start = n.saturating_sub(MAX_SHOWN);
    if start > 0 {
        lines.push(format!("…(前 {} 步省略)", start));
    }
    for (i, t) in tools[start..].iter().enumerate() {
        let idx = start + i + 1;
        // The tail item is "current" only mid-turn; once done, all are ✓.
        let marker = if !done && start + i + 1 == n { "▸" } else { "✓" };
        lines.push(format!("{} {}. {}", marker, idx, t));
    }
    lines.join("\n")
}

// ---------------------------------------------------------------------------
// Accumulating reply message (ReplyChunk, hook mode)
// ---------------------------------------------------------------------------

/// Soft cap for the accumulating reply message. Below the platform hard limit
/// (Discord 2000) with headroom for the next chunk and a marker, so we finalize
/// and start a fresh message before any single edit would overflow.
const REPLY_MSG_CAP: usize = 1900;

/// Append a transcript reply chunk to the accumulating message, editing it in
/// place. When the buffer would exceed [`REPLY_MSG_CAP`] the current message is
/// left finalized and a fresh one is started with this chunk (saturate-then-new),
/// so the full reply is preserved across several messages and nothing is
/// truncated. All length checks are char-boundary-safe (CJK text is common).
async fn append_reply_chunk(
    platform: &Arc<dyn PlatformCapabilities>,
    ctx: &dyn ReplyCtx,
    handle: &mut Option<Box<dyn PreviewHandle>>,
    buf: &mut String,
    content: &str,
    thinking: bool,
) {
    let Some(updater) = platform.as_message_updater() else {
        // No editable-message capability: fall back to a one-off reply so the
        // text is at least delivered.
        let _ = platform.reply(ctx, content).await;
        return;
    };

    let piece = if thinking {
        format!("💭 {}", content.trim())
    } else {
        content.trim().to_string()
    };

    // A single piece can itself exceed the platform cap (a long transcript
    // block). Split it into cap-sized, char-boundary-safe segments first; each
    // segment is then placed like a normal chunk. Without this, send_preview on
    // an over-cap body is rejected (Discord 400 "Must be 2000 or fewer").
    for segment in split_chars(&piece, REPLY_MSG_CAP) {
        // Would appending overflow the current message? Count chars, not bytes.
        let projected = buf.chars().count() + 1 + segment.chars().count();
        let start_fresh = handle.is_none() || projected > REPLY_MSG_CAP;

        if start_fresh {
            // Finalize the current message (leave it as the trace) and open a
            // fresh one with this segment.
            *buf = segment;
            match updater.send_preview(ctx, buf).await {
                Ok(h) => *handle = Some(h),
                Err(e) => {
                    tracing::warn!(error = %e, "reply chunk send_preview failed");
                    *handle = None;
                }
            }
        } else {
            if !buf.is_empty() {
                buf.push('\n');
            }
            buf.push_str(&segment);
            if let Some(h) = handle.as_ref() {
                if let Err(e) = updater.update_preview(h.as_ref(), buf).await {
                    tracing::warn!(error = %e, "reply chunk update_preview failed");
                }
            }
        }
    }
}

/// Split `s` into segments of at most `max` characters (not bytes), never
/// cutting inside a codepoint. Returns at least one segment (possibly empty for
/// an empty input is avoided by the caller).
fn split_chars(s: &str, max: usize) -> Vec<String> {
    if s.chars().count() <= max {
        return vec![s.to_string()];
    }
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut n = 0;
    for ch in s.chars() {
        cur.push(ch);
        n += 1;
        if n >= max {
            out.push(std::mem::take(&mut cur));
            n = 0;
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

// ---------------------------------------------------------------------------
// Preview helpers
// ---------------------------------------------------------------------------

async fn freeze_and_detach_preview(
    platform: &Arc<dyn PlatformCapabilities>,
    preview: &mut StreamPreview,
    handle: &mut Option<Box<dyn PreviewHandle>>,
) {
    if !preview.was_active() || preview.is_idle() {
        return;
    }
    preview.freeze();
    if let Some(h) = handle.take() {
        if let Some(updater) = platform.as_message_updater() {
            let text = preview.final_text();
            if !text.is_empty() {
                let _ = updater.update_preview(h.as_ref(), text).await;
            }
        }
    }
    preview.reset();
}

async fn discard_preview(
    platform: &Arc<dyn PlatformCapabilities>,
    handle: &mut Option<Box<dyn PreviewHandle>>,
) {
    if let Some(h) = handle.take() {
        if let Some(updater) = platform.as_message_updater() {
            let _ = updater.delete_preview(h.as_ref()).await;
        }
    }
}

fn summarise_json(value: &serde_json::Value, max_len: usize) -> String {
    let s = value.to_string();
    if s.chars().count() > max_len {
        format!("{}...", s.chars().take(max_len).collect::<String>())
    } else {
        s
    }
}

#[cfg(test)]
mod tests {
    use super::{format_tool_label, render_progress};

    #[test]
    fn format_tool_label_truncates_on_char_boundary() {
        // A multibyte/CJK input must never be byte-sliced (would panic). Use a
        // small max so truncation triggers, then assert the result is valid and
        // ends with the ellipsis marker.
        let label = format_tool_label("Bash", "你好世界你好世界", 3);
        assert_eq!(label, "Bash: 你好世…");
    }

    #[test]
    fn format_tool_label_no_input_is_tool_name() {
        assert_eq!(format_tool_label("Read", "", 40), "Read");
        assert_eq!(format_tool_label("Read", "   ", 40), "Read");
    }

    #[test]
    fn format_tool_label_collapses_newlines() {
        // A multiline command stays one progress line.
        let label = format_tool_label("Bash", "echo a\necho b", 40);
        assert_eq!(label, "Bash: echo a echo b");
    }

    #[test]
    fn render_progress_marks_current_then_done() {
        let tools = vec!["Bash: ls".to_string(), "Edit: a.rs".to_string()];
        let live = render_progress(&tools, false);
        assert!(live.starts_with("⚡ 处理中… (2 步)"));
        assert!(live.contains("✓ 1. Bash: ls"), "earlier step is done: {live}");
        assert!(live.contains("▸ 2. Edit: a.rs"), "tail step is current: {live}");

        let done = render_progress(&tools, true);
        assert!(done.starts_with("✓ 完成 (2 步)"));
        assert!(done.contains("✓ 2. Edit: a.rs"), "all steps done when finished: {done}");
        assert!(!done.contains('▸'), "no current marker once done: {done}");
    }

    #[test]
    fn render_progress_caps_long_list() {
        let tools: Vec<String> = (0..20).map(|i| format!("Tool{i}")).collect();
        let out = render_progress(&tools, false);
        assert!(out.contains("…(前 12 步省略)"), "elides all but the last 8: {out}");
        // The most recent step is shown and marked current.
        assert!(out.contains("▸ 20. Tool19"), "{out}");
        // An early step is not shown.
        assert!(!out.contains("Tool0\n") && !out.contains(" 1. Tool0"), "{out}");
    }

    #[test]
    fn render_progress_empty() {
        assert_eq!(render_progress(&[], false), "⚡ 处理中…");
        assert_eq!(render_progress(&[], true), "✓ 完成");
    }

    // -- Event-loop integration: in-place progress contract --------------------
    //
    // Drives the real `process_agent_events` with a synthetic event stream and a
    // minimal in-crate mock platform, asserting the Bolt-2 contract end to end:
    // a tool-heavy turn yields ONE evolving progress message (not one chat
    // message per tool), finalized on Result. The MockPlatform in tests/common
    // can't be used here (integration tests can't reach this binary-only
    // module), so a small local mock is defined.

    use super::{process_agent_events, AgentEvent, DisplayConfig};
    use crate::agent::PermissionResponder;
    use crate::core::platform::{
        MessageUpdater, Platform, PlatformCapabilities, PreviewHandle, ReplyCtx,
    };
    use crate::engine::PermissionDecision;
    use anyhow::Result as AnyResult;
    use async_trait::async_trait;
    use std::any::Any;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::sync::{broadcast, mpsc};

    #[derive(Debug, Clone)]
    enum Rec {
        Reply(String),
        PreviewCreate(String),
        PreviewUpdate(String),
        PreviewDelete,
    }

    #[derive(Debug)]
    struct TestCtx;
    impl ReplyCtx for TestCtx {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn session_key_hint(&self) -> String {
            "test:ctx".to_string()
        }
        fn clone_box(&self) -> Box<dyn ReplyCtx> {
            Box::new(TestCtx)
        }
    }

    #[derive(Debug)]
    struct TestHandle;
    impl PreviewHandle for TestHandle {
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    struct TestPlatform {
        rec: Arc<Mutex<Vec<Rec>>>,
        previews_created: Arc<AtomicU64>,
    }

    impl TestPlatform {
        fn new() -> Self {
            Self {
                rec: Arc::new(Mutex::new(Vec::new())),
                previews_created: Arc::new(AtomicU64::new(0)),
            }
        }
    }

    #[async_trait]
    impl Platform for TestPlatform {
        fn name(&self) -> &str {
            "test"
        }
        async fn start(&self, _handler: crate::core::platform::MessageHandler) -> AnyResult<()> {
            Ok(())
        }
        async fn reply(&self, _ctx: &dyn ReplyCtx, content: &str) -> AnyResult<()> {
            self.rec.lock().unwrap().push(Rec::Reply(content.to_string()));
            Ok(())
        }
        async fn send(&self, ctx: &dyn ReplyCtx, content: &str) -> AnyResult<()> {
            self.reply(ctx, content).await
        }
        async fn stop(&self) -> AnyResult<()> {
            Ok(())
        }
    }

    impl PlatformCapabilities for TestPlatform {
        fn as_message_updater(&self) -> Option<&dyn MessageUpdater> {
            Some(self)
        }
    }

    #[async_trait]
    impl MessageUpdater for TestPlatform {
        async fn send_preview(
            &self,
            _ctx: &dyn ReplyCtx,
            text: &str,
        ) -> AnyResult<Box<dyn PreviewHandle>> {
            self.previews_created.fetch_add(1, Ordering::SeqCst);
            self.rec.lock().unwrap().push(Rec::PreviewCreate(text.to_string()));
            Ok(Box::new(TestHandle))
        }
        async fn update_preview(&self, _handle: &dyn PreviewHandle, text: &str) -> AnyResult<()> {
            self.rec.lock().unwrap().push(Rec::PreviewUpdate(text.to_string()));
            Ok(())
        }
        async fn delete_preview(&self, _handle: &dyn PreviewHandle) -> AnyResult<()> {
            self.rec.lock().unwrap().push(Rec::PreviewDelete);
            Ok(())
        }
    }

    struct NoopResponder;
    #[async_trait]
    impl PermissionResponder for NoopResponder {
        async fn respond(&self, _request_id: &str, _allow: bool) -> AnyResult<()> {
            Ok(())
        }
    }

    fn tool_ev(name: &str, input: &str) -> AgentEvent {
        AgentEvent::ToolUse {
            id: String::new(),
            tool: name.to_string(),
            input: input.to_string(),
        }
    }

    fn result_ev(content: &str) -> AgentEvent {
        AgentEvent::Result {
            content: content.to_string(),
            session_id: "s1".to_string(),
            input_tokens: 0,
            output_tokens: 0,
        }
    }

    fn reply_ev(content: &str, thinking: bool) -> AgentEvent {
        AgentEvent::ReplyChunk {
            content: content.to_string(),
            thinking,
        }
    }

    async fn drive(events: Vec<AgentEvent>, inplace: bool) -> Vec<Rec> {
        drive_with_display(events, inplace, DisplayConfig::default()).await
    }

    async fn drive_with_display(
        events: Vec<AgentEvent>,
        inplace: bool,
        display: DisplayConfig,
    ) -> Vec<Rec> {
        let tp = Arc::new(TestPlatform::new());
        let rec = Arc::clone(&tp.rec);
        let platform: Arc<dyn PlatformCapabilities> = tp;
        let ctx = TestCtx;

        let (tx, mut rx) = mpsc::channel::<AgentEvent>(32);
        for ev in events {
            tx.send(ev).await.unwrap();
        }
        drop(tx);

        let (_perm_tx, mut perm_rx) = mpsc::channel::<PermissionDecision>(1);
        let responder: Arc<dyn PermissionResponder> = Arc::new(NoopResponder);
        let mut approve_all = false;
        let pending = Arc::new(AtomicBool::new(false));
        let stopped = Arc::new(AtomicBool::new(false));
        let (btx, _brx) = broadcast::channel(32);
        let btx = Arc::new(btx);

        process_agent_events(
            &platform, &ctx, &mut rx, &mut perm_rx, &responder, &mut approve_all,
            &pending, &stopped, None, &display, &btx, "test:session", inplace,
        )
        .await
        .expect("event loop ok");

        let out = rec.lock().unwrap().clone();
        out
    }

    #[tokio::test]
    async fn stop_flag_breaks_loop_promptly_and_stops_typing() {
        // Regression: /stop set the stopped flag while the loop was blocked on
        // recv() with no further events (an interrupted tmux turn). The loop
        // must wake within the poll interval, break, and run the post-loop
        // typing-stop — not hang until the 300s idle timeout with the phone
        // stuck "typing…".
        let tp = Arc::new(TestPlatform::new());
        let platform: Arc<dyn PlatformCapabilities> = tp;
        let ctx = TestCtx;

        // Channel stays OPEN with NO events queued — mimics an interrupted turn.
        let (_tx_keepalive, mut rx) = mpsc::channel::<AgentEvent>(8);
        let (_perm_tx, mut perm_rx) = mpsc::channel::<PermissionDecision>(1);
        let responder: Arc<dyn PermissionResponder> = Arc::new(NoopResponder);
        let mut approve_all = false;
        let pending = Arc::new(AtomicBool::new(false));
        let stopped = Arc::new(AtomicBool::new(true)); // /stop already fired
        let display = DisplayConfig::default();
        let (btx, _brx) = broadcast::channel(8);
        let btx = Arc::new(btx);

        // Typing indicator with a flag the stop-closure flips, so we can assert
        // the loop actually stopped typing on its way out.
        let typing = Arc::new(AtomicBool::new(true));
        let typing_for_closure = Arc::clone(&typing);
        let stop_typing: Box<dyn FnOnce() + Send> =
            Box::new(move || typing_for_closure.store(false, Ordering::SeqCst));

        let start = tokio::time::Instant::now();
        process_agent_events(
            &platform, &ctx, &mut rx, &mut perm_rx, &responder, &mut approve_all,
            &pending, &stopped, Some(stop_typing), &display, &btx, "test:session", true,
        )
        .await
        .expect("event loop ok");
        let elapsed = start.elapsed();

        assert!(
            elapsed < std::time::Duration::from_secs(5),
            "loop must break promptly on /stop, took {elapsed:?}"
        );
        assert!(
            !typing.load(Ordering::SeqCst),
            "typing indicator must be stopped when the loop exits on /stop"
        );
    }

    #[tokio::test]
    async fn inplace_mode_coalesces_tools_into_one_progress_message() {
        let rec = drive(
            vec![
                tool_ev("Bash", "ls -la"),
                tool_ev("Edit", "/x/a.rs"),
                tool_ev("Read", "/x/b.rs"),
                result_ev("最终答案"),
            ],
            true,
        )
        .await;

        // Exactly one preview created — not one per tool.
        let creates = rec.iter().filter(|r| matches!(r, Rec::PreviewCreate(_))).count();
        assert_eq!(creates, 1, "exactly one progress preview: {rec:?}");

        // Never deleted — the trace stays in the chat.
        assert!(
            !rec.iter().any(|r| matches!(r, Rec::PreviewDelete)),
            "progress must not be deleted: {rec:?}"
        );

        // The last preview write is the finalize (done form, all three tools).
        let last_preview = rec
            .iter()
            .rev()
            .find_map(|r| match r {
                Rec::PreviewUpdate(t) | Rec::PreviewCreate(t) => Some(t.clone()),
                _ => None,
            })
            .expect("a preview write");
        assert!(last_preview.starts_with("✓ 完成 (3 步)"), "final: {last_preview}");
        assert!(last_preview.contains("Bash: ls -la"), "final: {last_preview}");
        assert!(!last_preview.contains('▸'), "no current marker: {last_preview}");

        // Final answer replied; no per-tool "⚡" spam.
        assert!(
            rec.iter().any(|r| matches!(r, Rec::Reply(c) if c.contains("最终答案"))),
            "final answer replied: {rec:?}"
        );
        assert!(
            !rec.iter().any(|r| matches!(r, Rec::Reply(c) if c.starts_with("⚡ "))),
            "no per-tool chat spam: {rec:?}"
        );
    }

    #[tokio::test]
    async fn non_inplace_mode_keeps_per_tool_messages() {
        let rec = drive(
            vec![tool_ev("Bash", "ls"), tool_ev("Edit", "/x/a.rs"), result_ev("done")],
            false,
        )
        .await;

        let tool_msgs = rec
            .iter()
            .filter(|r| matches!(r, Rec::Reply(c) if c.starts_with("⚡ ")))
            .count();
        assert_eq!(tool_msgs, 2, "one chat message per tool: {rec:?}");
        assert!(
            !rec.iter().any(|r| matches!(r, Rec::PreviewCreate(t) if t.contains("处理中"))),
            "no progress preview in non-inplace mode: {rec:?}"
        );
    }

    #[tokio::test]
    async fn reply_chunks_accumulate_into_one_message() {
        // Several thinking/text chunks under the cap → ONE preview, edited in
        // place, accumulating all of them (with the 💭 marker on thinking).
        let rec = drive(
            vec![
                reply_ev("先看前端结构", true),     // thinking
                reply_ev("我打算改 App.tsx", false), // text
                reply_ev("改完跑测试", false),       // text
                result_ev("最终答案"),
            ],
            true,
        )
        .await;

        let creates = rec.iter().filter(|r| matches!(r, Rec::PreviewCreate(_))).count();
        assert_eq!(creates, 1, "all chunks share one message: {rec:?}");

        // The last edit holds all three pieces, thinking marked with 💭.
        let last = rec
            .iter()
            .rev()
            .find_map(|r| match r {
                Rec::PreviewUpdate(t) | Rec::PreviewCreate(t) => Some(t.clone()),
                _ => None,
            })
            .expect("a preview write");
        assert!(last.contains("💭 先看前端结构"), "thinking marked: {last}");
        assert!(last.contains("我打算改 App.tsx"), "{last}");
        assert!(last.contains("改完跑测试"), "{last}");
        // Final answer still delivered as its own reply (not part of the chunk msg).
        assert!(
            rec.iter().any(|r| matches!(r, Rec::Reply(c) if c.contains("最终答案"))),
            "final answer replied: {rec:?}"
        );
    }

    #[tokio::test]
    async fn reply_chunks_saturate_then_open_new_message() {
        // A chunk that pushes the buffer past the cap starts a fresh message
        // (saturate-then-new), so nothing is truncated.
        let big = "字".repeat(1000); // 2 of these (+marker) exceed REPLY_MSG_CAP(1900)
        let rec = drive(
            vec![
                reply_ev(&big, false),
                reply_ev(&big, false),
                result_ev("done"),
            ],
            true,
        )
        .await;

        let creates = rec.iter().filter(|r| matches!(r, Rec::PreviewCreate(_))).count();
        assert_eq!(creates, 2, "second chunk opens a new message: {rec:?}");
    }

    #[tokio::test]
    async fn single_oversized_reply_chunk_is_split() {
        // Regression: a single ReplyChunk longer than the cap used to be sent
        // whole → Discord 400 "Must be 2000 or fewer". It must be split into
        // multiple messages, each within the cap.
        let huge = "字".repeat(5000); // ~2.6× REPLY_MSG_CAP in one chunk
        let rec = drive(vec![reply_ev(&huge, false), result_ev("done")], true).await;

        let creates: Vec<&Rec> = rec
            .iter()
            .filter(|r| matches!(r, Rec::PreviewCreate(_)))
            .collect();
        assert!(creates.len() >= 3, "5000 chars split into ≥3 messages: {}", creates.len());
        // Every emitted body is within the cap.
        for r in &rec {
            if let Rec::PreviewCreate(t) | Rec::PreviewUpdate(t) = r {
                assert!(
                    t.chars().count() <= 1900,
                    "every message within cap, got {} chars",
                    t.chars().count()
                );
            }
        }
    }

    #[tokio::test]
    async fn reply_chunks_do_not_create_progress_message() {
        // A reply-only turn (no tools) must not produce a tool-progress message.
        let rec = drive(vec![reply_ev("just thinking", true), result_ev("done")], true).await;
        assert!(
            !rec.iter().any(|r| matches!(r, Rec::PreviewCreate(t) if t.contains("处理中") || t.contains("完成"))),
            "no progress message from reply chunks: {rec:?}"
        );
    }

    #[tokio::test]
    async fn verbose_off_suppresses_progress_and_reply_chunks_but_keeps_final() {
        // The /verbose-off contract: tool progress + thinking/text are silenced,
        // only the final reply ships.
        let quiet = DisplayConfig { verbose: false, ..Default::default() };
        let rec = drive_with_display(
            vec![
                reply_ev("内部思考", true),
                tool_ev("Bash", "ls"),
                tool_ev("Edit", "/x/a.rs"),
                reply_ev("中间说明", false),
                result_ev("最终答案"),
            ],
            true,
            quiet,
        )
        .await;

        // No progress preview, no reply-chunk preview during the turn.
        assert!(
            !rec.iter().any(|r| matches!(r, Rec::PreviewCreate(_) | Rec::PreviewUpdate(_))),
            "verbose off must emit no in-progress messages: {rec:?}"
        );
        assert!(
            !rec.iter().any(|r| matches!(r, Rec::Reply(c) if c.starts_with("⚡") || c.starts_with("💭"))),
            "verbose off must not relay tool/thinking chatter: {rec:?}"
        );
        // The final answer is still delivered.
        assert!(
            rec.iter().any(|r| matches!(r, Rec::Reply(c) if c.contains("最终答案"))),
            "final reply must still ship when quiet: {rec:?}"
        );
    }
}
