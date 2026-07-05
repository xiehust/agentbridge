//! Tmux backend: control an existing tmux session running Claude Code interactively
//! via `tmux send-keys` / `tmux capture-pane`.

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::config::TmuxConfig;
use crate::core::event::AgentEvent;
use crate::core::message::{FileAttachment, ImageAttachment};

use super::super::{AgentSession, PermissionResponder};

// ---------------------------------------------------------------------------
// TmuxSession
// ---------------------------------------------------------------------------

/// A live session backed by a tmux pane. Communication happens via
/// `tmux send-keys` (input) and `tmux capture-pane` (output polling).
pub struct TmuxSession {
    session_name: String,
    events_rx: mpsc::Receiver<AgentEvent>,
    alive: Arc<AtomicBool>,
    // Whether agentbridge owns this cc (it auto-started it) vs merely attached
    // to a user-started, phone+computer-shared one. Owned sessions may be torn
    // down on `close()`; attached ones must never be — destroying a shared cc
    // out from under the user is the bug this guards against.
    owns_session: bool,
    // A clone of the poll task's event sender, exposed so the hook receiver can
    // inject Stop/PostToolUse-derived events into the SAME channel the engine
    // drains. Retaining this clone means the channel stays open after the poll
    // task exits, so `recv() -> None` no longer signals a dead session — that
    // detection now relies on the `alive` flag plus the engine's idle timeout.
    hook_sender: mpsc::Sender<AgentEvent>,
    _poll_handle: JoinHandle<()>,
}

impl TmuxSession {
    /// A clone of this session's event sender, for the hook route registry to
    /// inject relayed hook events into the live event channel.
    pub fn hook_sender(&self) -> mpsc::Sender<AgentEvent> {
        self.hook_sender.clone()
    }
}

#[async_trait]
impl AgentSession for TmuxSession {
    async fn send(&self, prompt: &str) -> Result<()> {
        if !self.alive.load(Ordering::Relaxed) {
            return Err(anyhow!("tmux: session '{}' is not alive", self.session_name));
        }
        tmux_send_keys(&self.session_name, prompt).await?;
        Ok(())
    }

    async fn send_with_attachments(
        &self,
        prompt: &str,
        _images: &[ImageAttachment],
        _files: &[FileAttachment],
    ) -> Result<()> {
        // Tmux cannot send binary attachments; just send the text prompt.
        self.send(prompt).await
    }

    async fn respond_permission(&self, _request_id: &str, allow: bool) -> Result<()> {
        if !self.alive.load(Ordering::Relaxed) {
            return Err(anyhow!("tmux: session not alive"));
        }
        tmux_send_permission(&self.session_name, allow).await?;
        Ok(())
    }

    fn permission_responder(&self) -> Arc<dyn PermissionResponder> {
        Arc::new(TmuxPermissionResponder {
            session_name: self.session_name.clone(),
            alive: Arc::clone(&self.alive),
        })
    }

    fn take_events(&mut self) -> Option<mpsc::Receiver<AgentEvent>> {
        let replacement = mpsc::channel(1).1;
        Some(std::mem::replace(&mut self.events_rx, replacement))
    }

    fn replace_events(&mut self, rx: mpsc::Receiver<AgentEvent>) {
        self.events_rx = rx;
    }

    fn events(&mut self) -> &mut mpsc::Receiver<AgentEvent> {
        &mut self.events_rx
    }

    fn drain_stale_events(&mut self) {
        while self.events_rx.try_recv().is_ok() {}
    }

    fn session_id(&self) -> Option<String> {
        // Tmux sessions do not have agentbridge session IDs.
        None
    }

    fn alive(&self) -> bool {
        self.alive.load(Ordering::Relaxed)
    }

    async fn interrupt(&self) -> Result<()> {
        // /stop on the tmux backend must NOT kill the session: it is the
        // user-owned cc that phone and computer share. Send Escape to abort the
        // in-flight turn (interrupts generation and dismisses a selection menu),
        // leaving the cc alive at its prompt for the next message. The session
        // stays `alive` — only the current turn ends.
        if !self.alive.load(Ordering::Relaxed) {
            return Ok(());
        }
        tmux_send_key_raw(&self.session_name, "Escape").await
    }

    async fn close(&self) -> Result<()> {
        // Attached (not owned) session: this is the user's shared cc. Never send
        // /exit — that would close the very Claude Code the user drives from both
        // phone and computer. The lifecycle commands that call close() (/new,
        // /dir, /switch, /delete, /resume, /attach) only mean to drop
        // agentbridge's binding, not destroy the terminal. Interrupt the current
        // turn so nothing is left mid-stream, then detach without killing.
        if !self.owns_session {
            let _ = self.interrupt().await;
            self.alive.store(false, Ordering::Release);
            return Ok(());
        }
        // Owned (auto-started) session: agentbridge created this cc, so it is
        // ours to tear down. Send /exit to gracefully close Claude, then C-c
        // as a fallback.
        let _ = tmux_send_keys(&self.session_name, "/exit").await;
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let _ = Command::new("tmux")
            .args(["send-keys", "-t", &self.session_name, "C-c", ""])
            .output()
            .await;
        self.alive.store(false, Ordering::Release);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// TmuxPermissionResponder
// ---------------------------------------------------------------------------

struct TmuxPermissionResponder {
    session_name: String,
    alive: Arc<AtomicBool>,
}

#[async_trait]
impl PermissionResponder for TmuxPermissionResponder {
    async fn respond(&self, _request_id: &str, allow: bool) -> Result<()> {
        if !self.alive.load(Ordering::Relaxed) {
            return Err(anyhow!("tmux: session not alive"));
        }
        tmux_send_permission(&self.session_name, allow).await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// TmuxAgent (factory)
// ---------------------------------------------------------------------------

/// Factory that creates or attaches to a tmux session running Claude Code.
pub struct TmuxAgent {
    work_dir: PathBuf,
    tmux_config: TmuxConfig,
    #[allow(dead_code)]
    project_name: String,
}

impl TmuxAgent {
    pub fn new(work_dir: PathBuf, tmux_config: TmuxConfig, project_name: String) -> Self {
        Self {
            work_dir,
            tmux_config,
            project_name,
        }
    }

    /// Start (or attach to) the tmux session and return a TmuxSession.
    pub async fn start_session(&self) -> Result<TmuxSession> {
        let session_name = &self.tmux_config.session;

        // Check if the tmux session already exists.
        let exists = tmux_has_session(session_name).await;

        if !exists && self.tmux_config.auto_start {
            tracing::info!(
                session = %session_name,
                work_dir = %self.work_dir.display(),
                "tmux: creating new session and starting claude"
            );
            // Create a new detached tmux session. A fixed width/height keeps
            // Claude's soft-wrapping stable across captures, so the line diff
            // does not misalign when the controlling client's size differs.
            let output = Command::new("tmux")
                .args([
                    "new-session",
                    "-d",
                    "-s",
                    session_name,
                    "-x",
                    "120",
                    "-y",
                    "40",
                    "-c",
                    &self.work_dir.display().to_string(),
                ])
                .output()
                .await?;
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(anyhow!(
                    "tmux: failed to create session '{}': {}",
                    session_name,
                    stderr.trim()
                ));
            }
            // Start claude inside the session, then block until its TUI input
            // box is actually ready before returning. A fixed sleep raced the
            // launch: on a slow cold start the engine's first user message
            // landed in a not-yet-ready prompt and sat there unsubmitted.
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            tmux_send_keys(session_name, "claude").await?;
            wait_for_claude_ready(session_name).await;
        } else if !exists {
            return Err(anyhow!(
                "tmux: session '{}' does not exist and auto_start is disabled",
                session_name
            ));
        } else {
            tracing::info!(
                session = %session_name,
                "tmux: attaching to existing session"
            );
        }

        let alive = Arc::new(AtomicBool::new(true));
        let (event_tx, event_rx) = mpsc::channel::<AgentEvent>(128);
        // Clone the sender before moving the original into the poll task. The
        // hook receiver uses this clone to inject relayed events into the same
        // channel `event_rx` feeds — letting the engine consume hook Stop/Result
        // events through its existing event loop without a second channel.
        let hook_sender = event_tx.clone();

        let poll_session = session_name.clone();
        let poll_work_dir = self.work_dir.display().to_string();
        let poll_auto_restart = self.tmux_config.auto_restart;
        let poll_hook_relay = self.tmux_config.hook_relay;
        let poll_alive = Arc::clone(&alive);
        let poll_handle = tokio::spawn(async move {
            poll_loop(
                poll_session,
                poll_work_dir,
                poll_auto_restart,
                poll_hook_relay,
                poll_alive,
                event_tx,
            )
            .await;
        });

        Ok(TmuxSession {
            session_name: session_name.clone(),
            events_rx: event_rx,
            alive,
            // We own the cc only if we auto-started it; otherwise we attached to
            // a user-owned, shared session and must not tear it down.
            owns_session: self.tmux_config.auto_start,
            hook_sender,
            _poll_handle: poll_handle,
        })
    }
}

// ---------------------------------------------------------------------------
// Background polling loop
// ---------------------------------------------------------------------------

/// Polls `tmux capture-pane` every 150ms, diffs against previous output,
/// and emits AgentEvent for new lines.
async fn poll_loop(
    session_name: String,
    work_dir: String,
    auto_restart: bool,
    hook_relay: bool,
    alive: Arc<AtomicBool>,
    tx: mpsc::Sender<AgentEvent>,
) {
    let poll_period = std::time::Duration::from_millis(150);
    let mut interval = tokio::time::interval(poll_period);

    // Turn lifecycle is tracked off Claude's footer: "esc to interrupt" while a
    // turn is running, "? for shortcuts" once the input box is idle again. We
    // emit reply blocks only on the idle edge, so streaming never leaks partial
    // copies. `had_content` records that a turn ran so completion fires once.
    let mut had_content = false;

    // Reply blocks already forwarded, keyed by their text. `capture-pane`
    // re-shows the same block every poll until it scrolls off, so dedup by
    // content is what stops re-emitting. Bounded below to avoid unbounded growth.
    let mut seen_blocks: std::collections::HashSet<String> = std::collections::HashSet::new();

    // Seed the baseline with whatever is already on screen. When attaching a
    // session the user has been using, the pane is full of prior `⏺` replies;
    // without this they'd all be re-emitted as "new" on the first poll, flooding
    // the chat with stale history. Only replies produced AFTER attach should go
    // out. (No-op for a freshly auto-started session — its screen has no blocks.)
    let mut initial_screen_hash: u64 = 0;
    if let Ok(initial) = tmux_capture_pane(&session_name).await {
        for block in extract_reply_blocks(&initial) {
            seen_blocks.insert(block);
        }
        initial_screen_hash = hash_lines(&initial);
    }

    // auto_restart is bounded to a single attempt to avoid a tight crash-loop if
    // claude refuses to start; after that we surface the error and exit.
    let mut restart_used = false;

    // Heartbeat throttle: while a turn is busy we emit a Thinking event roughly
    // every HEARTBEAT_TICKS polls. This both resets the engine's idle timeout
    // (so a long task is not killed as "no response") and shows live progress in
    // the chat. 150ms * 80 ≈ 12s between heartbeats — visible but not spammy.
    const HEARTBEAT_TICKS: u32 = 80;
    let mut busy_ticks: u32 = 0;

    // Has the turn shown "busy" since the last result we emitted? A real turn
    // always passes through busy (claude is thinking/running tools). Requiring
    // this gates out mid-turn idle pauses — between two tool calls the screen
    // can sit still long enough to look "settled", but we won't emit again
    // until claude has gone busy and come back. One emit per busy→idle cycle.
    let mut saw_busy_since_emit = false;
    let mut last_heartbeat = String::new();

    // Hash of the previous poll's screen, to detect when the screen has stopped
    // changing (stable) vs is still streaming. A turn that runs several tools
    // produces one final emit, not one per tool/pause, because the screen only
    // goes stable once the turn has truly settled.
    let mut prev_screen_hash: u64 = initial_screen_hash;
    // Polls the screen must stay byte-identical before a turn counts as settled.
    // At 150ms/poll, 6 ticks ≈ 0.9s of no on-screen change.
    const STABLE_TICKS: u32 = 6;
    // Extra settle margin before the hook-mode safety-net Result fires, giving a
    // slightly-late Stop hook time to win the common case. At 150ms/poll, ~20
    // ticks ≈ 3s of post-settle quiet before we conclude no Stop is coming.
    const HOOK_SAFETY_NET_TICKS: u32 = 20;
    let mut stable_ticks: u32 = 0;

    loop {
        interval.tick().await;

        if !alive.load(Ordering::Relaxed) {
            break;
        }

        // Check session still exists.
        if !tmux_has_session(&session_name).await {
            if auto_restart && !restart_used {
                restart_used = true;
                tracing::warn!(
                    session = %session_name,
                    "tmux: session disappeared, attempting auto_restart"
                );
                match restart_session(&session_name, &work_dir).await {
                    Ok(()) => {
                        // Drop the dedup set so the freshly redrawn screen is
                        // not replayed as a flood of "new" blocks.
                        seen_blocks.clear();
                        had_content = false;
                        continue;
                    }
                    Err(e) => {
                        alive.store(false, Ordering::Release);
                        let _ = tx
                            .send(AgentEvent::Error {
                                message: format!(
                                    "tmux: auto_restart of '{}' failed: {}",
                                    session_name, e
                                ),
                            })
                            .await;
                        break;
                    }
                }
            }
            alive.store(false, Ordering::Release);
            let _ = tx
                .send(AgentEvent::Error {
                    message: format!("tmux: session '{}' no longer exists", session_name),
                })
                .await;
            break;
        }

        // Capture the pane content (last 100 lines).
        let current_lines = match tmux_capture_pane(&session_name).await {
            Ok(lines) => lines,
            Err(e) => {
                tracing::debug!(error = %e, "tmux: capture-pane failed");
                continue;
            }
        };

        // A permission menu takes priority: surface it as a PermissionRequest
        // and skip reply extraction this tick.
        let screen = current_lines.join("\n");
        if contains_permission_prompt(&screen) {
            let menu = extract_permission_menu(&current_lines);
            if seen_blocks.insert(menu.clone()) {
                let request_id = format!("tmux-perm-{}", uuid::Uuid::new_v4());
                let _ = tx
                    .send(AgentEvent::PermissionRequest {
                        request_id,
                        tool: "tmux_permission".to_string(),
                        input: serde_json::json!({ "prompt": menu }),
                        options: vec![],
                    })
                    .await;
                had_content = true;
            }
            continue;
        }

        // An interactive selection menu (AskUserQuestion chooser) also waits for
        // the user, but is NOT a Yes/No permission prompt. Relay its text so the
        // phone can answer (reply the option number), then end the turn so the
        // session unlocks. Crucially this must short-circuit BEFORE the idle/
        // safety-net logic: the menu's `❯ 1. …` cursor line otherwise looks like
        // the input box returned and would fire a spurious empty Result while the
        // menu is still up — leaving phone and terminal out of sync.
        if contains_selection_menu(&current_lines) {
            let menu = extract_selection_menu(&current_lines);
            if !menu.is_empty() && seen_blocks.insert(menu.clone()) {
                let _ = tx.send(AgentEvent::Text { content: menu }).await;
                // End the turn once so the engine relays the menu and unlocks;
                // the user's reply re-enters via send-keys into the open menu.
                let _ = tx
                    .send(AgentEvent::Result {
                        content: String::new(),
                        session_id: String::new(),
                        input_tokens: 0,
                        output_tokens: 0,
                    })
                    .await;
                saw_busy_since_emit = false;
                had_content = false;
            }
            continue;
        }

        // CRITICAL: do not emit while Claude is still streaming. A `capture-pane`
        // snapshot shows a reply block GROWING in place (not appended), so each
        // poll would yield a longer string — defeating dedup and spamming the
        // chat with partial copies interleaved with spinner frames. Instead,
        // only extract+emit once the turn is idle, so each reply is sent exactly
        // once, whole and clean.
        //
        // Busy detection lives in screen_is_busy(): the legacy "esc to
        // interrupt" hint plus the newer bare spinner status line (`✽ …`).
        // Done footers ("Cooked for 9s") must never count — they persist in
        // scrollback and would wedge the turn forever.
        let busy = screen_is_busy(&current_lines);
        // Idle = not busy, and an input box is present. Defined negatively so it
        // survives footer changes and a tmux status bar (which appears when a
        // human also attaches the session and hid the "? for shortcuts" line).
        let has_input_box = current_lines.iter().any(|l| l.trim_start().starts_with('❯'));
        let idle = !busy && has_input_box;

        if busy {
            // Turn in progress: don't emit the (still-growing) reply yet, but
            // send a throttled heartbeat so the engine's idle timeout resets and
            // the user sees progress. The heartbeat text is the live status line
            // (e.g. "Billowing… (1m 42s · ↓ 2.9k tokens)") when available.
            had_content = true;
            saw_busy_since_emit = true;
            busy_ticks += 1;
            if busy_ticks >= HEARTBEAT_TICKS {
                busy_ticks = 0;
                if should_emit_heartbeat(hook_relay) {
                    // Scrape mode: visible `🧠 Working…` heartbeat with the live
                    // status line, so long tasks show progress in the chat.
                    let status = current_lines
                        .iter()
                        .rev()
                        .map(|l| l.trim())
                        .find(|l| is_done_footer(l))
                        .unwrap_or("Working…")
                        .to_string();
                    // Only send if it changed, to avoid identical repeats.
                    if status != last_heartbeat {
                        last_heartbeat = status.clone();
                        let _ = tx.send(AgentEvent::Thinking { content: status }).await;
                    }
                } else {
                    // Hook mode: the visible heartbeat is suppressed (it would
                    // render as a message and detach the preview, BR-14), so a
                    // long quiet turn used to trip the engine's idle timeout —
                    // the turn was aborted ("等太久了") and the eventual Stop
                    // hook answer discarded as stale. Emit a SILENT keepalive
                    // instead: resets the idle timer, renders nothing.
                    let _ = tx.send(AgentEvent::Keepalive).await;
                }
            }
            continue;
        }
        busy_ticks = 0;

        // Unified settle detector: track how long the screen has been
        // byte-identical. A turn is "settled" only when the screen has stopped
        // changing for STABLE_TICKS polls AND we're idle (input box back, not
        // busy). This single signal replaces separate idle/stable counters and
        // ensures a multi-tool turn produces ONE final result, not one per
        // tool/pause (mid-turn the screen keeps changing, resetting the count).
        let cur_hash = hash_lines(&current_lines);
        if cur_hash == prev_screen_hash {
            stable_ticks += 1;
        } else {
            stable_ticks = 0;
            prev_screen_hash = cur_hash;
        }
        // Hook mode handles turn completion differently from scrape mode, so it
        // branches BEFORE the one-emit settle gate below. The turn's reply text
        // comes from Claude Code's Stop hook (relayed into this same channel);
        // the poll loop emits NO pane-scraped text/image. But it MUST still emit
        // a turn-completion Result as a *safety net* — otherwise an interrupted
        // turn (which fires no Stop hook) leaves process_agent_events waiting
        // forever and the session stuck busy (observed live). The hook's Stop
        // normally arrives first (it carries the text) and ends the turn; this
        // empty Result only wins when no hook came. First Result wins; the loser
        // is dropped (process_agent_events breaks and stops draining).
        //
        // The safety net fires only after the screen has been quiet for a margin
        // well beyond the normal settle, AND the turn actually ran (saw busy) —
        // so a slightly-late Stop still beats it and a mid-turn pause never trips
        // it.
        if hook_relay {
            let safety_net = idle
                && saw_busy_since_emit
                && stable_ticks >= STABLE_TICKS + HOOK_SAFETY_NET_TICKS;
            // Diagnostic: while a turn is pending completion (it went busy and no
            // Result has fired yet), log the settle state each tick. If the Stop
            // hook is lost, this trace shows whether the safety-net is converging
            // (stable_ticks climbing to the threshold) or stuck (idle never true,
            // or stable_ticks resetting because the screen keeps changing).
            if saw_busy_since_emit {
                tracing::debug!(
                    busy,
                    idle,
                    stable_ticks,
                    safety_net,
                    "tmux hook: awaiting turn completion"
                );
            }
            if safety_net {
                tracing::debug!("tmux hook mode: settle safety-net Result (no Stop hook seen)");
                let _ = tx
                    .send(AgentEvent::Result {
                        content: String::new(),
                        session_id: String::new(),
                        input_tokens: 0,
                        output_tokens: 0,
                    })
                    .await;
                saw_busy_since_emit = false;
                had_content = false;
            }
            continue;
        }

        // Settled = idle, screen stable, AND the turn actually ran (saw busy).
        // The saw_busy gate is what makes it ONE emit per turn: a mid-turn pause
        // between tools is idle+stable too, but busy hasn't recurred, and we
        // already consumed the flag — so we wait for the turn to truly finish.
        let settled = idle && stable_ticks >= STABLE_TICKS && saw_busy_since_emit;
        tracing::debug!(
            busy, idle, stable_ticks, settled, saw_busy_since_emit,
            "tmux poll state"
        );
        if !settled {
            continue;
        }
        saw_busy_since_emit = false;

        // Settled: emit any reply blocks not yet sent (text-scrape mode).
        let mut emitted_new = false;
        let blocks = extract_reply_blocks(&current_lines);
        for block in blocks {
            if seen_blocks.insert(block.clone()) {
                let _ = tx.send(AgentEvent::Text { content: block }).await;
                emitted_new = true;
            }
        }

        // Signal turn completion once, after a turn that produced output.
        if emitted_new || had_content {
            let _ = tx
                .send(AgentEvent::Result {
                    content: String::new(),
                    session_id: String::new(),
                    input_tokens: 0,
                    output_tokens: 0,
                })
                .await;
            had_content = false;
        }
    }
}

/// Stable hash of the captured screen lines, used by the settle detector to
/// tell when the terminal screen has stopped changing across polls.
fn hash_lines(lines: &[String]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    for l in lines {
        l.trim_end().hash(&mut h);
    }
    h.finish()
}

/// Recreate a tmux session and relaunch claude inside it. Mirrors the auto_start
/// path so an auto_restart resumes from the same launch command.
async fn restart_session(session_name: &str, work_dir: &str) -> Result<()> {
    let output = Command::new("tmux")
        .args([
            "new-session", "-d", "-s", session_name,
            "-x", "120", "-y", "40", "-c", work_dir,
        ])
        .output()
        .await?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!(
            "tmux: failed to recreate session '{}': {}",
            session_name,
            stderr.trim()
        ));
    }
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    tmux_send_keys(session_name, "claude").await?;
    wait_for_claude_ready(session_name).await;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tmux helpers
// ---------------------------------------------------------------------------

/// Wait until Claude's TUI input box is ready to accept a prompt.
///
/// Claude's cold start (config scan, MCP, plugins) can take several seconds;
/// the input box `❯` plus the box border only appear once it is ready. Polling
/// for that shape is robust to however slow the start is, unlike a fixed sleep.
/// Bounded so a launch failure does not hang the session forever.
async fn wait_for_claude_ready(session_name: &str) {
    const MAX_WAIT: std::time::Duration = std::time::Duration::from_secs(20);
    let deadline = tokio::time::Instant::now() + MAX_WAIT;
    loop {
        if let Ok(lines) = tmux_capture_pane(session_name).await {
            // Ready signals: the version banner has rendered and the input box
            // border is present (a full-width run of ─ near the bottom).
            let has_banner = lines.iter().any(|l| l.contains("Claude Code v"));
            let has_box = lines
                .iter()
                .any(|l| l.trim().chars().count() > 20 && l.trim().chars().all(|c| c == '─'));
            if has_banner && has_box {
                // Small settle so the box is interactive, not mid-render.
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                return;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            tracing::warn!(
                session = %session_name,
                "tmux: claude readiness wait timed out; sending prompt anyway"
            );
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
}

/// Run a subprocess with a hard timeout, killing the child if it overruns.
///
/// tmux calls sit on the engine's turn path, which holds the per-session lock
/// and only releases it when the turn returns. An unbounded `.output().await`
/// against a wedged tmux server never returns, so the lock is stranded `busy`
/// and every later message hangs until the process is restarted. Bounding the
/// call turns that permanent hang into a recoverable per-turn error.
async fn output_bounded(
    cmd: &mut Command,
    timeout: std::time::Duration,
    what: &str,
) -> Result<std::process::Output> {
    // Kill the child on drop so a timed-out (hung) tmux client is reaped rather
    // than left orphaned.
    cmd.kill_on_drop(true);
    match tokio::time::timeout(timeout, cmd.output()).await {
        Ok(Ok(output)) => Ok(output),
        Ok(Err(e)) => Err(anyhow!("{what}: spawn failed: {e}")),
        Err(_) => Err(anyhow!("{what}: timed out after {timeout:?}")),
    }
}

/// Hard timeout for every tmux CLI call on the turn/poll path.
const TMUX_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Run a `tmux` subcommand with the standard bounded timeout.
async fn run_tmux(args: &[&str], what: &str) -> Result<std::process::Output> {
    let mut cmd = Command::new("tmux");
    cmd.args(args);
    output_bounded(&mut cmd, TMUX_TIMEOUT, what).await
}

/// Check if a tmux session exists.
async fn tmux_has_session(session_name: &str) -> bool {
    run_tmux(&["has-session", "-t", session_name], "tmux has-session")
        .await
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Send text to a tmux session via send-keys, then press Enter. The `--` guard
/// stops tmux from treating leading dashes as options; the text is otherwise
/// typed verbatim (no shell is involved).
async fn tmux_send_keys(session_name: &str, text: &str) -> Result<()> {
    let escaped = escape_for_tmux(text);
    let output = run_tmux(
        &["send-keys", "-t", session_name, "--", escaped.as_str(), "Enter"],
        "tmux send-keys",
    )
    .await?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!("tmux send-keys failed: {}", stderr.trim()));
    }
    Ok(())
}

/// Send raw keys (like "1", "3", "C-c") followed by Enter.
async fn tmux_send_raw_keys(session_name: &str, keys: &str) -> Result<()> {
    let output = run_tmux(
        &["send-keys", "-t", session_name, keys, "Enter"],
        "tmux send-keys",
    )
    .await?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!("tmux send-keys failed: {}", stderr.trim()));
    }
    Ok(())
}

/// Send a single key by tmux key name (e.g. "Escape"), with NO trailing Enter.
/// Used for control keys like interrupt, where appending Enter would submit a
/// stray empty line into the prompt.
async fn tmux_send_key_raw(session_name: &str, key: &str) -> Result<()> {
    let output = run_tmux(&["send-keys", "-t", session_name, key], "tmux send-keys").await?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!("tmux send-keys failed: {}", stderr.trim()));
    }
    Ok(())
}

/// Answer Claude Code's permission menu. The prompt is a numbered list
/// (`❯ 1. Yes / 2. Yes, and don't ask again / 3. No`) navigated with digit
/// keys, so approve maps to `1` and deny maps to `3` (each followed by Enter),
/// not the `y`/`n` keys a classic yes/no prompt would use.
async fn tmux_send_permission(session_name: &str, allow: bool) -> Result<()> {
    let key = if allow { "1" } else { "3" };
    tmux_send_raw_keys(session_name, key).await
}

/// Capture the last 100 lines from the tmux pane.
async fn tmux_capture_pane(session_name: &str) -> Result<Vec<String>> {
    let output = run_tmux(
        &["capture-pane", "-t", session_name, "-p", "-S", "-100"],
        "tmux capture-pane",
    )
    .await?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!("tmux capture-pane failed: {}", stderr.trim()));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let lines: Vec<String> = stdout.lines().map(|l| l.to_string()).collect();
    Ok(lines)
}

/// Prepare text for `tmux send-keys`.
///
/// `tmux send-keys -- <arg>` hands the argument to tmux as a single argv token
/// with no shell involved, so `$`, quotes, backticks, `\` and `;` are all typed
/// literally. Shell-style escaping here would corrupt the prompt (e.g.
/// `echo $HOME` becoming `echo \$HOME` inside Claude). The `--` guard in the
/// caller stops tmux from treating leading dashes as options, which is the only
/// real hazard, so the text passes through unchanged.
fn escape_for_tmux(text: &str) -> String {
    text.to_string()
}

// ---------------------------------------------------------------------------
// Output extraction (whitelist Claude's `⏺` reply blocks)
// ---------------------------------------------------------------------------

/// Whether a captured screen shows an ACTIVE turn.
///
/// Two fingerprints, because the cc TUI changed across versions:
/// 1. Legacy: the literal "esc to interrupt" hint, shown only while a turn
///    runs (anywhere on screen).
/// 2. 2026-07 TUI: the hint is gone and the input box stays visible during
///    the turn; the live signal is the spinner status line (`✽ Zigzagging…`).
///    Discriminators, both load-bearing:
///    - the TRAILING ELLIPSIS: a finished turn's footer ("✻ Brewed for 56s")
///      persists in scrollback and must not read as busy;
///    - the TAIL BOUND: the live spinner sits near the input box, so only the
///      last few lines are checked — glyph+ellipsis text inside old reply
///      prose can never wedge the turn.
fn screen_is_busy(lines: &[String]) -> bool {
    if lines.iter().any(|l| l.contains("esc to interrupt")) {
        return true;
    }
    const SPINNER_GLYPHS: [char; 6] = ['✢', '✳', '✶', '✻', '✽', '✺'];
    const TAIL: usize = 15;
    lines.iter().rev().take(TAIL).any(|l| {
        let t = l.trim();
        t.chars()
            .next()
            .is_some_and(|c| SPINNER_GLYPHS.contains(&c))
            && (t.ends_with('…') || t.ends_with("..."))
    })
}

/// Whether the poll loop should emit a visible heartbeat (`Thinking`) this tick.
/// Gated off in hook relay mode because the heartbeat renders as a user-visible
/// `🧠 Working…` message and detaches the live preview (BR-14).
fn should_emit_heartbeat(hook_relay: bool) -> bool {
    !hook_relay
}

/// Whether a line ends a reply block: Claude's "<verb> for Ns" footer
/// (Cooked / Brewed / Worked / Grooving / …) or a spinner status line. The
/// verb set is open, so match the shape — a leading non-`⏺` glyph plus a
/// `for <n>s` tail, or a trailing ellipsis — rather than a fixed word list.
fn is_done_footer(line: &str) -> bool {
    let t = line.trim();
    if t.is_empty() {
        return false;
    }
    // Spinner glyph (any non-word leading symbol) + "... for Ns".
    let has_for_seconds = t.contains(" for ") && t.trim_end().ends_with('s');
    let is_status = t.ends_with('…') || t.ends_with("...");
    let starts_with_glyph = t
        .chars()
        .next()
        .map(|c| !c.is_alphanumeric() && c != '⏺' && c != '-' && c != '•')
        .unwrap_or(false);
    starts_with_glyph && (has_for_seconds || is_status)
}

/// True if a captured line begins a Claude reply block (the `⏺` marker, after
/// optional leading whitespace).
fn is_reply_start(line: &str) -> bool {
    line.trim_start().starts_with('⏺')
}

/// True if a `⏺` block is a TOOL CALL (`⏺ Bash(...)`, `⏺ Write(...)`, etc.)
/// rather than a prose reply. Claude marks both with `⏺`, but tool calls have
/// the shape `<CapitalizedName>(` as the first token. These must not be
/// forwarded to chat as if they were the assistant's answer. Detect the shape
/// (capitalized identifier immediately followed by `(`) rather than a fixed
/// tool-name list, so new tools are covered automatically.
fn is_tool_call_block(first_line_after_marker: &str) -> bool {
    let t = first_line_after_marker.trim_start();
    let Some(paren) = t.find('(') else { return false };
    let name = &t[..paren];
    !name.is_empty()
        && name.chars().next().map(|c| c.is_ascii_uppercase()).unwrap_or(false)
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// True for lines that end a reply block when scanning its continuation.
/// Continuations are blank lines or 2-space-indented text; anything else
/// (spinner footer, border rule, input box, next prompt) terminates the block.
fn is_block_boundary(line: &str) -> bool {
    let t = line.trim();
    if t.is_empty() {
        return false; // blank lines belong to the block (inter-paragraph gaps)
    }
    // Border rules.
    if t.chars().all(|c| c == '─' || c == '╌') {
        return true;
    }
    // Input box / next user prompt.
    if t.starts_with('❯') {
        return true;
    }
    is_done_footer(line)
}

/// Extract complete Claude reply blocks from a captured screen.
///
/// Each block starts at a `⏺` line and runs until a boundary (spinner footer,
/// border, or input prompt). The leading `⏺ ` / `  ` indentation is stripped so
/// the chat shows clean prose. This whitelist approach means spinners, banners,
/// tips and borders are never forwarded, and a reply is always captured whole
/// (no truncated tails from line-diff misalignment). Trailing blank lines are
/// trimmed so a block's text is stable across polls (needed for dedup).
fn extract_reply_blocks(lines: &[String]) -> Vec<String> {
    let mut blocks = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        if !is_reply_start(&lines[i]) {
            i += 1;
            continue;
        }
        // Collect this block.
        let mut block_lines: Vec<String> = Vec::new();
        // First line: strip the `⏺ ` marker.
        let first = lines[i].trim_start();
        let first = first.strip_prefix('⏺').unwrap_or(first).trim_start();
        let is_tool = is_tool_call_block(first);
        block_lines.push(first.to_string());
        i += 1;
        // Continuation lines until a boundary or the next `⏺`.
        while i < lines.len() && !is_reply_start(&lines[i]) && !is_block_boundary(&lines[i]) {
            // Strip the uniform 2-space continuation indent if present.
            let line = lines[i].strip_prefix("  ").unwrap_or(&lines[i]);
            block_lines.push(line.to_string());
            i += 1;
        }
        // Skip tool-call blocks (`⏺ Bash(...)` etc.) — they are not the
        // assistant's prose reply and must not reach the chat.
        if is_tool {
            continue;
        }
        // Trim trailing blank lines for a stable, clean block.
        while block_lines.last().map(|l| l.trim().is_empty()).unwrap_or(false) {
            block_lines.pop();
        }
        let text = block_lines.join("\n").trim().to_string();
        if !text.is_empty() {
            blocks.push(text);
        }
    }
    blocks
}

/// Extract the permission-menu region (from the question line through the
/// numbered options) for surfacing to the user.
fn extract_permission_menu(lines: &[String]) -> String {
    let start = lines
        .iter()
        .position(|l| l.to_lowercase().contains("do you want to"));
    let Some(start) = start else {
        return lines.join("\n");
    };
    let mut out = Vec::new();
    for line in &lines[start..] {
        let t = line.trim();
        if t.contains("Esc to cancel") || t.contains("Tab to amend") {
            break;
        }
        if !t.is_empty() {
            out.push(t.to_string());
        }
    }
    out.join("\n")
}

/// Detect Claude Code's permission prompt in the captured terminal output.
///
/// Claude Code asks for permission with a numbered menu, e.g.:
///   Do you want to make this edit?
///   ❯ 1. Yes
///     2. Yes, and don't ask again this session
///     3. No, and tell Claude what to do differently
/// Matching that menu shape (a numbered "Yes" option plus a numbered "No"
/// option or an explicit question) avoids firing on ordinary assistant prose
/// like "I'll approve the PR" or "you don't have permission", which previously
/// produced spurious permission requests that blocked the turn.
fn contains_permission_prompt(content: &str) -> bool {
    let lower = content.to_lowercase();
    let has_yes_option = lower.contains("1. yes") || lower.contains("1.yes");
    let has_no_option = lower.contains("2. no")
        || lower.contains("3. no")
        || lower.contains("2.no")
        || lower.contains("3.no");
    let asks = lower.contains("do you want to");
    has_yes_option && (has_no_option || asks)
}

/// Detect a Claude Code *interactive selection* menu (e.g. the AskUserQuestion
/// chooser: a question, numbered `❯ 1. …` options, and a
/// `Enter to select · … · Esc to cancel` footer).
///
/// This is distinct from `contains_permission_prompt` (the Yes/No tool-approval
/// menu): it has no fixed "1. Yes" shape, and its `❯` cursor sits on an option
/// line — which the idle detector would otherwise mistake for the input box
/// having returned, prematurely ending the turn while the menu still waits.
fn contains_selection_menu(lines: &[String]) -> bool {
    let has_footer = lines.iter().any(|l| {
        let t = l.trim();
        t.contains("Enter to select") || (t.contains("to navigate") && t.contains("to cancel"))
    });
    // At least one numbered option present (the chooser body), to avoid firing
    // on prose that merely mentions one of those phrases.
    let has_numbered_option = lines.iter().any(|l| {
        let t = l.trim_start().trim_start_matches('❯').trim_start();
        let mut cs = t.chars();
        matches!(cs.next(), Some(c) if c.is_ascii_digit()) && t[1..].starts_with('.')
    });
    has_footer && has_numbered_option
}

/// Numbered-option digit (1-9) rendered as a keycap emoji, for the menu format.
fn keycap(n: u32) -> Option<&'static str> {
    match n {
        1 => Some("1\u{fe0f}\u{20e3}"),
        2 => Some("2\u{fe0f}\u{20e3}"),
        3 => Some("3\u{fe0f}\u{20e3}"),
        4 => Some("4\u{fe0f}\u{20e3}"),
        5 => Some("5\u{fe0f}\u{20e3}"),
        6 => Some("6\u{fe0f}\u{20e3}"),
        7 => Some("7\u{fe0f}\u{20e3}"),
        8 => Some("8\u{fe0f}\u{20e3}"),
        9 => Some("9\u{fe0f}\u{20e3}"),
        _ => None,
    }
}

/// If a (cursor-stripped) line starts a numbered option (`1. foo`), return
/// `(number, rest)`; else `None`.
fn parse_option_line(t: &str) -> Option<(u32, &str)> {
    let dot = t.find('.')?;
    let (num, rest) = t.split_at(dot);
    let n: u32 = num.parse().ok()?;
    Some((n, rest[1..].trim_start()))
}

/// Render a selection menu (AskUserQuestion chooser) into a phone-friendly,
/// icon-blocked message: the question with a ❓ header, each option as a bold
/// keycap line, and sub-descriptions as `·` bullets. The `❯` cursor is stripped
/// so the text is identical regardless of which option the cursor sits on —
/// keeping dedup stable as the cursor moves (relayed once, not per move).
fn extract_selection_menu(lines: &[String]) -> String {
    // First pass: collect the meaningful lines (cursor/chrome stripped), in
    // order, splitting into the question region (before option 1) and options.
    let mut cleaned: Vec<String> = Vec::new();
    for line in lines {
        let t = line.trim();
        if t.is_empty() {
            continue;
        }
        if t.contains("Enter to select")
            || (t.contains("to navigate") && t.contains("to cancel"))
        {
            break;
        }
        // Horizontal rules.
        if t.chars().all(|c| c == '─' || c == '╌') {
            continue;
        }
        // Tab-strip row (e.g. "← ☐ 预填策略 ☐ 谁来写 ✔ Submit →") — UI chrome.
        if t.contains('☐') || t.contains('☑') || t.contains("Submit") {
            continue;
        }
        cleaned.push(t.trim_start_matches('❯').trim_start().to_string());
    }

    let mut out: Vec<String> = Vec::new();
    let mut seen_option = false;
    for t in &cleaned {
        match parse_option_line(t) {
            Some((n, rest)) => {
                // Drop the trailing meta-options the phone can't use as-is.
                let low = rest.to_lowercase();
                if low.starts_with("type something") || low.starts_with("chat about this") {
                    continue;
                }
                seen_option = true;
                let icon = keycap(n).unwrap_or("•");
                out.push(format!("{} **{}**", icon, rest));
            }
            None => {
                if seen_option {
                    // Indented sub-description under the current option.
                    out.push(format!("   · {}", t));
                } else {
                    // Question region (before any option). First line gets the
                    // ❓ header; following lines are sub-text of the question.
                    if out.is_empty() {
                        out.push(format!("\u{2753} **{}**", t));
                    } else {
                        out.push(t.clone());
                    }
                }
            }
        }
    }

    if out.is_empty() {
        return String::new();
    }
    out.push("\n\u{1f4ac} 直接回复序号选择".to_string());
    out.join("\n").trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn screen(lines: &[&str]) -> Vec<String> {
        lines.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn busy_detects_legacy_esc_hint() {
        let s = screen(&["  some output", "✳ Working… (esc to interrupt)", "❯ "]);
        assert!(screen_is_busy(&s));
    }

    #[test]
    fn busy_detects_bare_spinner_status_line() {
        // Real capture from a 2026-07 cc TUI: the "esc to interrupt" hint is
        // gone and the input box stays visible DURING the turn — the old
        // fingerprint read this as idle, so the settle safety-net fired
        // mid-turn and ended it with an empty Result (answer lost).
        let s = screen(&[
            "❯ 现在怎么样了",
            "✽ Zigzagging…",
            "                    100% context used",
            "──────────────────────────",
            "❯ ",
            "──────────────────────────",
            "  [AIDLC] ready | opus-4-8 ctx:100%",
            "  ⏵⏵ bypass permissions on (shift+tab to cycle) · ← for agents",
        ]);
        assert!(screen_is_busy(&s), "live spinner status line must read as busy");
    }

    #[test]
    fn done_footer_is_not_busy() {
        // Finished-turn footers persist in scrollback ("✻ Brewed for 56s",
        // real capture) — they must NOT read as busy or the turn would wedge
        // forever. The discriminator is the trailing ellipsis.
        let s = screen(&[
            "✻ Brewed for 56s",
            "──────────────────────────",
            "❯ ",
            "──────────────────────────",
            "  ⏵⏵ bypass permissions on (shift+tab to cycle) · ← for agents",
        ]);
        assert!(!screen_is_busy(&s));
    }

    #[test]
    fn spinner_text_deep_in_scrollback_is_not_busy() {
        // A glyph+ellipsis line inside old reply prose must not wedge the
        // turn: the spinner check is bounded to the tail of the capture.
        let mut lines: Vec<String> = vec!["✻ pondering…".to_string()];
        for i in 0..30 {
            lines.push(format!("  reply prose line {}", i));
        }
        lines.push("❯ ".to_string());
        lines.push("  ⏵⏵ bypass permissions on".to_string());
        assert!(!screen_is_busy(&lines));
    }

    #[test]
    fn hook_mode_gates_off_heartbeat() {
        // In hook relay mode the visible heartbeat must not fire — it renders as
        // a `🧠 Working…` message and detaches the live preview (BR-14). Scraped
        // text/image are gated by the `hook_relay` branch in poll_loop (which
        // emits only an empty safety-net Result); scrape mode keeps the heartbeat.
        assert!(!should_emit_heartbeat(true));
        assert!(should_emit_heartbeat(false));
    }

    #[tokio::test]
    async fn output_bounded_errs_when_child_exceeds_timeout() {
        // tmux subprocess calls sit on the turn's lock-holding path. If tmux
        // wedges, an unbounded `.output().await` never returns, so the session
        // lock is stranded `busy` and every later message hangs until restart.
        // output_bounded must surface an Err on timeout rather than park.
        let mut cmd = Command::new("sleep");
        cmd.arg("30");
        let start = std::time::Instant::now();
        let result =
            output_bounded(&mut cmd, std::time::Duration::from_millis(100), "sleep-test").await;
        assert!(
            result.is_err(),
            "a child exceeding the timeout must return Err, not hang"
        );
        assert!(
            start.elapsed() < std::time::Duration::from_secs(5),
            "output_bounded must return promptly on timeout, not wait for the child"
        );
    }

    #[tokio::test]
    async fn output_bounded_returns_output_when_child_completes() {
        // The happy path must still yield the child's output unchanged.
        let mut cmd = Command::new("true");
        let out = output_bounded(&mut cmd, std::time::Duration::from_secs(5), "true-test")
            .await
            .expect("a fast child within the timeout must return Ok");
        assert!(out.status.success());
    }

    // A real AskUserQuestion chooser captured from the cc TUI (the one that
    // wedged the turn live). The `❯` cursor sits on an OPTION line, not an input
    // box — which is exactly what the idle detector used to misread.
    fn selection_menu_screen(cursor_on: usize) -> Vec<String> {
        let opts = [
            "分层(推荐)",
            "只留空否决",
            "全留空",
            "全预填",
            "Type something.",
        ];
        let mut v = vec![
            "  先确认两件需要你拍板的事，再动手：".to_string(),
            "──────────────────────────────".to_string(),
            "←  ☐ 预填策略  ☐ 谁来写  ✔ Submit  →".to_string(),
            "".to_string(),
            "decide_prefill 的预填策略用哪个?".to_string(),
            "".to_string(),
        ];
        for (i, o) in opts.iter().enumerate() {
            let marker = if i == cursor_on { "❯ " } else { "  " };
            v.push(format!("{}{}. {}", marker, i + 1, o));
        }
        v.push("──────────────────────────────".to_string());
        v.push("Enter to select · Tab/Arrow keys to navigate · Esc to cancel".to_string());
        v
    }

    #[test]
    fn detects_selection_menu_but_not_as_permission() {
        let screen = selection_menu_screen(0);
        assert!(contains_selection_menu(&screen), "should detect the chooser");
        // It must NOT be mistaken for a Yes/No permission prompt.
        assert!(!contains_permission_prompt(&screen.join("\n")));
    }

    #[test]
    fn selection_menu_extract_is_cursor_position_independent() {
        // The cursor moving between options must not change the extracted text,
        // or dedup would re-relay the menu on every arrow-key move.
        let a = extract_selection_menu(&selection_menu_screen(0));
        let b = extract_selection_menu(&selection_menu_screen(3));
        assert_eq!(a, b, "menu text must be stable across cursor moves");
        // Icon-blocked format: question gets a ❓ header, options become bold
        // keycap lines, cursor/chrome stripped.
        assert!(a.contains("\u{2753}"), "question header present: {a}");
        assert!(a.contains("decide_prefill 的预填策略用哪个?"), "got: {a}");
        assert!(a.contains("1\u{fe0f}\u{20e3}"), "option 1 keycap: {a}");
        assert!(a.contains("**分层(推荐)**"), "option text bold: {a}");
        assert!(a.contains("\u{1f4ac} 直接回复序号选择"), "reply hint: {a}");
        assert!(!a.contains('❯'));
        assert!(!a.contains("Enter to select"));
        assert!(!a.contains("──"));
        assert!(!a.contains("Submit"), "tab strip dropped: {a}");
    }

    #[test]
    fn ordinary_prose_is_not_a_selection_menu() {
        // Prose that happens to contain a number must not trip the detector
        // (no footer present).
        let lines: Vec<String> = [
            "我建议的执行顺序：先 Step 1，再 Step 2。",
            "1. 这是一个普通的列表项，不是菜单",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        assert!(!contains_selection_menu(&lines));
    }

    #[test]
    fn escape_for_tmux_handles_special_chars() {
        // `tmux send-keys -- <arg>` involves no shell, so text passes through
        // verbatim; escaping would corrupt the prompt typed into Claude.
        assert_eq!(escape_for_tmux("hello"), "hello");
        assert_eq!(escape_for_tmux("a;b"), "a;b");
        assert_eq!(escape_for_tmux("a\\b"), "a\\b");
        assert_eq!(escape_for_tmux("he said \"hi\""), "he said \"hi\"");
        assert_eq!(escape_for_tmux("echo $HOME"), "echo $HOME");
        assert_eq!(escape_for_tmux("`cmd`"), "`cmd`");
    }

    // Real screen sample captured from claude TUI (v2.1.183) via capture-pane.
    fn sample_screen() -> Vec<String> {
        [
            "~/warren_ws",
            "❯ claude",
            " ▐▛███▜▌   Claude Code v2.1.183",
            "  ▘▘ ▝▝    ~/warren_ws",
            "❯ 你好",
            "",
            "⏺ 你好！👋",
            "",
            "  有什么可以帮你的吗?我可以帮你处理:",
            "",
            "  - 写代码 / 调试 — 新功能、修 bug",
            "  - 看代码 / Review — 代码审查",
            "",
            "  你现在在做什么?直接告诉我就行 😊",
            "",
            "✻ Worked for 6s",
            "────────────────────────",
            "❯ ",
            "────────────────────────",
            "  ? for shortcuts · ← for agents",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }

    #[test]
    fn extract_reply_blocks_pulls_clean_reply() {
        let blocks = extract_reply_blocks(&sample_screen());
        assert_eq!(blocks.len(), 1, "exactly one ⏺ block expected");
        let b = &blocks[0];
        // Content present, marker + indent stripped.
        assert!(b.starts_with("你好！👋"), "got: {b}");
        assert!(b.contains("有什么可以帮你的吗"));
        assert!(b.contains("- 写代码 / 调试"));
        assert!(b.contains("直接告诉我就行 😊"));
        // No chrome leaked in.
        assert!(!b.contains("⏺"));
        assert!(!b.contains("Worked for"));
        assert!(!b.contains("─"));
        assert!(!b.contains("❯"));
        assert!(!b.contains("for shortcuts"));
        assert!(!b.contains("Claude Code v"));
    }

    #[test]
    fn extract_reply_blocks_ignores_pure_chrome_screen() {
        // A "thinking" screen with no ⏺ block yields nothing.
        let lines: Vec<String> = [
            "❯ 你好",
            "✢ Pontificating…",
            "────────",
            "❯ ",
            "  esc to interrupt",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        assert!(extract_reply_blocks(&lines).is_empty());
    }

    #[test]
    fn extract_reply_blocks_dedup_is_stable_across_polls() {
        // The same screen captured twice must yield byte-identical block text
        // so a HashSet dedup suppresses the re-emit.
        let a = extract_reply_blocks(&sample_screen());
        let b = extract_reply_blocks(&sample_screen());
        assert_eq!(a, b);
    }

    #[test]
    fn extract_reply_blocks_skips_tool_calls() {
        // `⏺ Bash(...)` is a tool call, not prose — must NOT be forwarded.
        let lines: Vec<String> = [
            "⏺ 让我拉一下当前状态",
            "⏺ Bash(cd /tmp && ls)",
            "  ⎿  {\"status\":\"ok\"}",
            "⏺ 系统状态全绿，进度如下：",
            "✻ Worked for 5s",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let blocks = extract_reply_blocks(&lines);
        // Only the two prose blocks, not the Bash call.
        assert_eq!(blocks.len(), 2, "got: {blocks:?}");
        assert!(blocks[0].contains("让我拉一下"));
        assert!(blocks[1].contains("系统状态全绿"));
        assert!(!blocks.iter().any(|b| b.contains("Bash(")));
    }

    #[test]
    fn is_tool_call_block_shape() {
        assert!(is_tool_call_block("Bash(cd /tmp)"));
        assert!(is_tool_call_block("Write(/a/b.txt)"));
        assert!(is_tool_call_block("Read(file)"));
        // Prose must not be mistaken for a tool call.
        assert!(!is_tool_call_block("收到，消息正常打进来了 ✅"));
        assert!(!is_tool_call_block("能，而且签章这块修过了"));
        assert!(!is_tool_call_block("Hi! 👋 (ready)")); // space before paren
    }

    #[test]
    fn extract_reply_blocks_two_separate_replies() {
        let lines: Vec<String> = [
            "⏺ First reply",
            "✻ Brewed for 3s",
            "❯ next question",
            "⏺ Second reply",
            "✻ Worked for 2s",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let blocks = extract_reply_blocks(&lines);
        assert_eq!(blocks, vec!["First reply", "Second reply"]);
    }

    #[test]
    fn is_done_footer_matches_any_verb() {
        assert!(is_done_footer("✻ Cooked for 9s"));
        assert!(is_done_footer("✻ Brewed for 7s"));
        assert!(is_done_footer("✻ Worked for 6s"));
        assert!(is_done_footer("✢ Pontificating…"));
        assert!(is_done_footer("✽ Grooving…"));
        // Real reply content must NOT look like a footer.
        assert!(!is_done_footer("⏺ Hihi! 👋"));
        assert!(!is_done_footer("  - 写代码 / 调试"));
        assert!(!is_done_footer("你现在在做什么?"));
    }

    #[test]
    fn extract_permission_menu_pulls_question_and_options() {
        let lines: Vec<String> = [
            "⏺ Write(/tmp/x.txt)",
            "────────",
            " Do you want to create x.txt?",
            " ❯ 1. Yes",
            "   2. Yes, allow all",
            "   3. No",
            " Esc to cancel · Tab to amend",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        let menu = extract_permission_menu(&lines);
        assert!(menu.contains("Do you want to create x.txt?"));
        assert!(menu.contains("1. Yes"));
        assert!(menu.contains("3. No"));
        assert!(!menu.contains("Esc to cancel"));
    }

    #[test]
    fn contains_permission_prompt_detects_patterns() {
        // A realistic Claude-style permission menu must be detected.
        let menu = "Do you want to make this edit?\n\
                    ❯ 1. Yes\n\
                    \x20 2. Yes, and don't ask again this session\n\
                    \x20 3. No, and tell Claude what to do differently";
        assert!(contains_permission_prompt(menu));

        // Ordinary assistant prose must NOT trigger a permission request.
        assert!(!contains_permission_prompt("I'll approve the PR for you."));
        assert!(!contains_permission_prompt(
            "you don't have permission to edit this file"
        ));
        assert!(!contains_permission_prompt("Hello world"));
    }

    /// `/stop` (interrupt) must NOT destroy the tmux session — it is the
    /// user-owned cc shared between phone and computer. Spin a bare tmux session
    /// (a plain shell — no claude needed), interrupt it, and assert it is still
    /// alive and the tmux session still exists. Requires `tmux` on PATH.
    #[tokio::test]
    async fn interrupt_keeps_tmux_session_alive() {
        use crate::agent::AgentSession;
        if !command_exists("tmux").await {
            eprintln!("skip: tmux not on PATH");
            return;
        }
        let sess = "ab-test-interrupt-keepalive";
        let _ = Command::new("tmux").args(["kill-session", "-t", sess]).output().await;
        // A bare detached session running the user's shell — stands in for an
        // attached cc without needing claude.
        let created = Command::new("tmux")
            .args(["new-session", "-d", "-s", sess, "-x", "80", "-y", "24"])
            .output()
            .await
            .map(|o| o.status.success())
            .unwrap_or(false);
        if !created {
            eprintln!("skip: could not create tmux session");
            return;
        }

        let session = TmuxSession {
            session_name: sess.to_string(),
            events_rx: mpsc::channel(1).1,
            alive: Arc::new(AtomicBool::new(true)),
            owns_session: false, // attached, not owned — the case under test
            hook_sender: mpsc::channel(1).0,
            _poll_handle: tokio::spawn(async {}),
        };

        session.interrupt().await.expect("interrupt should succeed");

        // The whole point: interrupt did NOT close the session.
        assert!(session.alive(), "interrupt must leave the session alive");
        assert!(
            tmux_has_session(sess).await,
            "interrupt must NOT kill the tmux session (that would close the user's cc)"
        );

        let _ = Command::new("tmux").args(["kill-session", "-t", sess]).output().await;
    }

    /// `close()` on an ATTACHED (not-owned) session — reached by /new, /dir,
    /// /switch, /delete, /resume, /attach via cleanup_agent_session — must NOT
    /// destroy the user's shared tmux session, only detach agentbridge's binding.
    #[tokio::test]
    async fn close_does_not_kill_attached_session() {
        use crate::agent::AgentSession;
        if !command_exists("tmux").await {
            eprintln!("skip: tmux not on PATH");
            return;
        }
        let sess = "ab-test-close-attached";
        let _ = Command::new("tmux").args(["kill-session", "-t", sess]).output().await;
        let created = Command::new("tmux")
            .args(["new-session", "-d", "-s", sess, "-x", "80", "-y", "24"])
            .output()
            .await
            .map(|o| o.status.success())
            .unwrap_or(false);
        if !created {
            eprintln!("skip: could not create tmux session");
            return;
        }

        let session = TmuxSession {
            session_name: sess.to_string(),
            events_rx: mpsc::channel(1).1,
            alive: Arc::new(AtomicBool::new(true)),
            owns_session: false, // attached → close() must not destroy it
            hook_sender: mpsc::channel(1).0,
            _poll_handle: tokio::spawn(async {}),
        };

        session.close().await.expect("close should succeed");

        // close() detaches (marks not-alive) but leaves the user's cc running.
        assert!(!session.alive(), "close marks the binding inactive");
        assert!(
            tmux_has_session(sess).await,
            "close() must NOT kill an attached session (it is the user's shared cc)"
        );

        let _ = Command::new("tmux").args(["kill-session", "-t", sess]).output().await;
    }

    async fn command_exists(cmd: &str) -> bool {
        Command::new("sh")
            .args(["-c", &format!("command -v {}", cmd)])
            .output()
            .await
            .map(|o| o.status.success())
            .unwrap_or(false)
    }
}
