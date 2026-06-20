//! Tmux backend: control an existing tmux session running Claude Code interactively
//! via `tmux send-keys` / `tmux capture-pane`.

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::process::Command;
use tokio::sync::{mpsc, Mutex};
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
    last_output: Arc<Mutex<Vec<String>>>,
    // The only live sender lives inside the poll task. Keeping a clone here
    // would hold the channel open forever, so the engine could never observe
    // `recv() -> None` when the poll task exits on a dead session.
    _poll_handle: JoinHandle<()>,
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

    async fn close(&self) -> Result<()> {
        // Send /exit to gracefully close Claude, then mark dead.
        let _ = tmux_send_keys(&self.session_name, "/exit").await;
        // Give it a moment, then send C-c as fallback.
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
            // Create a new detached tmux session.
            let output = Command::new("tmux")
                .args([
                    "new-session",
                    "-d",
                    "-s",
                    session_name,
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
            // Start claude inside the session.
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            tmux_send_keys(session_name, "claude").await?;
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
        let last_output: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let (event_tx, event_rx) = mpsc::channel::<AgentEvent>(128);

        // Spawn background polling task. The task owns the only sender clone so
        // the channel closes (recv -> None) when the task exits on a dead session.
        let poll_session = session_name.clone();
        let poll_work_dir = self.work_dir.display().to_string();
        let poll_auto_restart = self.tmux_config.auto_restart;
        let poll_alive = Arc::clone(&alive);
        let poll_last_output = Arc::clone(&last_output);
        let poll_handle = tokio::spawn(async move {
            poll_loop(
                poll_session,
                poll_work_dir,
                poll_auto_restart,
                poll_alive,
                poll_last_output,
                event_tx,
            )
            .await;
        });

        Ok(TmuxSession {
            session_name: session_name.clone(),
            events_rx: event_rx,
            alive,
            last_output,
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
    alive: Arc<AtomicBool>,
    last_output: Arc<Mutex<Vec<String>>>,
    tx: mpsc::Sender<AgentEvent>,
) {
    let poll_period = std::time::Duration::from_millis(150);
    let mut interval = tokio::time::interval(poll_period);

    // Turn-completion is detected by output quiescence rather than by trying to
    // recognise Claude Code's boxed input prompt: its last captured line is the
    // box bottom border (`╰─...╯`), never a bare `>`, so a trailing-char check
    // never fires and every turn would hang until the engine's idle timeout.
    // Instead: once a turn has produced output (`had_content`), if no new line
    // appears for QUIESCENCE_TICKS consecutive polls (~1500ms), treat the turn
    // as finished.
    const QUIESCENCE_TICKS: u32 = 10;
    let mut had_content = false;
    let mut quiet_ticks: u32 = 0;

    // auto_restart is bounded to a single attempt to avoid a tight crash-loop if
    // claude refuses to start; after that we surface the error and exit.
    let mut restart_used = false;

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
                        // Resync the diff baseline so the freshly redrawn screen
                        // is not replayed as a flood of "new" output.
                        *last_output.lock().await = Vec::new();
                        had_content = false;
                        quiet_ticks = 0;
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

        // Diff against last known output, then drop the lock before any await:
        // holding the mutex across `tx.send().await` would needlessly serialise
        // the lock with channel back-pressure.
        let new_lines = {
            let mut prev = last_output.lock().await;
            let new_lines = diff_lines(&prev, &current_lines);
            *prev = current_lines;
            new_lines
        };

        if new_lines.is_empty() {
            // No new output this tick. If the turn already produced content and
            // has now been quiet long enough, the turn is finished.
            if had_content {
                quiet_ticks += 1;
                if quiet_ticks >= QUIESCENCE_TICKS {
                    let _ = tx
                        .send(AgentEvent::Result {
                            content: String::new(),
                            session_id: String::new(),
                            input_tokens: 0,
                            output_tokens: 0,
                        })
                        .await;
                    had_content = false;
                    quiet_ticks = 0;
                }
            }
            continue;
        }

        // New output arrived: the turn is still active, so reset the quiet timer.
        quiet_ticks = 0;
        let content = new_lines.join("\n");

        // Check for permission request patterns.
        if contains_permission_prompt(&content) {
            let request_id = format!("tmux-perm-{}", uuid::Uuid::new_v4());
            let _ = tx
                .send(AgentEvent::PermissionRequest {
                    request_id,
                    tool: "tmux_permission".to_string(),
                    input: serde_json::json!({"prompt": content.clone()}),
                    options: vec![],
                })
                .await;
        } else {
            let _ = tx.send(AgentEvent::Text { content }).await;
            had_content = true;
        }
    }
}

/// Recreate a tmux session and relaunch claude inside it. Mirrors the auto_start
/// path so an auto_restart resumes from the same launch command.
async fn restart_session(session_name: &str, work_dir: &str) -> Result<()> {
    let output = Command::new("tmux")
        .args(["new-session", "-d", "-s", session_name, "-c", work_dir])
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
    Ok(())
}

// ---------------------------------------------------------------------------
// Tmux helpers
// ---------------------------------------------------------------------------

/// Check if a tmux session exists.
async fn tmux_has_session(session_name: &str) -> bool {
    Command::new("tmux")
        .args(["has-session", "-t", session_name])
        .output()
        .await
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Send text to a tmux session via send-keys, then press Enter. The `--` guard
/// stops tmux from treating leading dashes as options; the text is otherwise
/// typed verbatim (no shell is involved).
async fn tmux_send_keys(session_name: &str, text: &str) -> Result<()> {
    let escaped = escape_for_tmux(text);
    let output = Command::new("tmux")
        .args(["send-keys", "-t", session_name, "--", &escaped, "Enter"])
        .output()
        .await?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!("tmux send-keys failed: {}", stderr.trim()));
    }
    Ok(())
}

/// Send raw keys (like "1", "3", "C-c") followed by Enter.
async fn tmux_send_raw_keys(session_name: &str, keys: &str) -> Result<()> {
    let output = Command::new("tmux")
        .args(["send-keys", "-t", session_name, keys, "Enter"])
        .output()
        .await?;
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
    let output = Command::new("tmux")
        .args(["capture-pane", "-t", session_name, "-p", "-S", "-100"])
        .output()
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
// Output diffing and pattern detection
// ---------------------------------------------------------------------------

/// Find lines in `current` that are new compared to `previous`.
/// Uses a simple suffix-match: find the longest suffix of `previous` that
/// appears as a prefix of `current`, then the remainder is "new".
fn diff_lines(previous: &[String], current: &[String]) -> Vec<String> {
    if previous.is_empty() {
        // If we had no previous output, treat only non-empty trailing lines as new
        // to avoid flooding with the initial screen content.
        return Vec::new();
    }

    if current.is_empty() {
        return Vec::new();
    }

    // Find where the previous output ends in the current output.
    // Look for the last N lines of previous that match a subsequence in current.
    let prev_trimmed: Vec<&str> = previous.iter().map(|s| s.trim_end()).collect();
    let curr_trimmed: Vec<&str> = current.iter().map(|s| s.trim_end()).collect();

    // Try to find the last line of previous in current (from the end backward).
    if let Some(last_prev) = prev_trimmed.last() {
        if !last_prev.is_empty() {
            // Search for this line in current (from end).
            for i in (0..curr_trimmed.len()).rev() {
                if curr_trimmed[i] == *last_prev {
                    // Everything after index i is new.
                    let new_start = i + 1;
                    if new_start < current.len() {
                        let new_lines: Vec<String> = current[new_start..]
                            .iter()
                            .filter(|l| !l.trim().is_empty())
                            .cloned()
                            .collect();
                        return new_lines;
                    }
                    return Vec::new();
                }
            }
        }
    }

    // Fallback: if previous is completely different from current (e.g. screen cleared),
    // return the trailing non-empty lines of current as new content.
    let new_lines: Vec<String> = current
        .iter()
        .filter(|l| !l.trim().is_empty())
        .cloned()
        .collect();
    // Only return if there are fewer lines than the full capture (indicates partial new content).
    if new_lines.len() < current.len() / 2 {
        new_lines
    } else {
        // Too much output changed at once; skip to avoid flooding.
        Vec::new()
    }
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

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn diff_lines_empty_previous_returns_empty() {
        let prev: Vec<String> = vec![];
        let curr = vec!["line1".to_string(), "line2".to_string()];
        // First capture returns empty (we skip the initial screen).
        assert!(diff_lines(&prev, &curr).is_empty());
    }

    #[test]
    fn diff_lines_finds_new_content() {
        let prev = vec![
            "old line 1".to_string(),
            "old line 2".to_string(),
        ];
        let curr = vec![
            "old line 1".to_string(),
            "old line 2".to_string(),
            "new line 1".to_string(),
            "new line 2".to_string(),
        ];
        let new = diff_lines(&prev, &curr);
        assert_eq!(new, vec!["new line 1", "new line 2"]);
    }

    #[test]
    fn diff_lines_no_change_returns_empty() {
        let lines = vec!["line1".to_string(), "line2".to_string()];
        assert!(diff_lines(&lines, &lines).is_empty());
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
}
