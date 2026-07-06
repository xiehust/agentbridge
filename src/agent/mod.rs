//! New Claude Code agent adapter built on the `AgentSession` trait.
//!
//! This replaces the one-shot `send()` model with a persistent subprocess
//! session that stays alive across messages.  The `AgentSession` trait
//! abstracts the transport so other agent backends can be plugged in later.
//!
//! Key trait design points:
//! - `AgentSession` trait defines the contract for any agent backend.
//! - `ClaudeSession` keeps a long-lived child process (`claude`).
//! - `ClaudeAgent` is a factory that spawns or resumes sessions.
//! - Permission requests are surfaced as `AgentEvent::PermissionRequest` and
//!   can be responded to via `respond_permission()`.
//! - Graceful shutdown with escalation (close stdin -> SIGTERM -> SIGKILL).

#![allow(dead_code)] // trait methods define API contract; some paths reserved for future agent backends

pub mod acp;
pub mod registry;
pub mod tmux;

use anyhow::{Context, Result};
use async_trait::async_trait;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{mpsc, Mutex};

use crate::config::AgentConfig;
use crate::core::event::AgentEvent;
use crate::core::message::{FileAttachment, ImageAttachment};

/// Sentinel value for AgentSessionID that tells the agent to use --continue
/// (resume most recent session) instead of a specific session ID.
pub const CONTINUE_SESSION: &str = "__continue__";

// ---------------------------------------------------------------------------
// AgentSession trait
// ---------------------------------------------------------------------------

/// Trait for an active agent session (persistent subprocess).
///
/// Implementations keep a child process alive and translate between the
/// platform-neutral `AgentEvent` stream and the backend-specific wire format.
#[async_trait]
pub trait AgentSession: Send + Sync {
    /// Send a user message to the agent.
    async fn send(&self, prompt: &str) -> Result<()>;

    /// Send a user message with images and files.
    async fn send_with_attachments(
        &self,
        prompt: &str,
        images: &[ImageAttachment],
        files: &[FileAttachment],
    ) -> Result<()>;

    /// Respond to a permission request.
    async fn respond_permission(&self, request_id: &str, allow: bool) -> Result<()>;

    /// Get a clonable handle that can respond to permission requests out-of-band.
    /// The engine uses this to answer permissions without holding the session mutex.
    fn permission_responder(&self) -> Arc<dyn PermissionResponder>;

    /// Get the event receiver channel.
    ///
    /// Take the event receiver (can only be called once — subsequent calls return None).
    fn take_events(&mut self) -> Option<mpsc::Receiver<AgentEvent>>;

    /// Put an event receiver back into the session after a turn completes,
    /// so the next turn can reuse the same subprocess.
    fn replace_events(&mut self, rx: mpsc::Receiver<AgentEvent>);

    /// Get mutable reference to the event receiver.
    fn events(&mut self) -> &mut mpsc::Receiver<AgentEvent>;

    /// Drain any stale events buffered in the receiver without blocking.
    /// Called between turns to discard leftover events from a prior turn.
    fn drain_stale_events(&mut self);

    /// Get current session ID (for resume).
    fn session_id(&self) -> Option<String>;

    /// Whether the agent process is still alive.
    fn alive(&self) -> bool;

    /// Interrupt the current turn WITHOUT destroying the session.
    ///
    /// This backs `/stop`. The default is `close()`, which is correct for
    /// backends whose session is a per-turn-disposable subprocess (claude
    /// `--print`, ACP): the next message simply spawns a fresh one. A backend
    /// that *attaches* to a long-lived, user-owned session (tmux) must override
    /// this to merely abort the in-flight turn — destroying the shared session
    /// on `/stop` would kill the very Claude Code the user is sitting in front
    /// of, breaking the "phone and computer drive the same cc" contract.
    async fn interrupt(&self) -> Result<()> {
        self.close().await
    }

    /// Gracefully close the session.
    async fn close(&self) -> Result<()>;
}

/// Clonable handle for responding to permission requests out-of-band.
///
/// The engine holds one of these while processing an agent turn so it can
/// reply to `PermissionRequest` events without needing the session itself.
#[async_trait]
pub trait PermissionResponder: Send + Sync {
    async fn respond(&self, request_id: &str, allow: bool) -> Result<()>;
}

// ---------------------------------------------------------------------------
// ClaudeSession
// ---------------------------------------------------------------------------

/// A live Claude Code session backed by a child process communicating over
/// stream-json on stdin/stdout.
pub struct ClaudeSession {
    stdin: Arc<Mutex<ChildStdin>>,
    pub(crate) events_rx: mpsc::Receiver<AgentEvent>,
    session_id: Arc<Mutex<Option<String>>>,
    alive: Arc<AtomicBool>,
    child: Arc<Mutex<Child>>,
    work_dir: PathBuf,
}

#[async_trait]
impl AgentSession for ClaudeSession {
    async fn send(&self, prompt: &str) -> Result<()> {
        let msg = serde_json::json!({
            "type": "user",
            "message": {
                "role": "user",
                "content": prompt
            }
        });
        self.write_json(&msg).await
    }

    async fn send_with_attachments(
        &self,
        prompt: &str,
        images: &[ImageAttachment],
        files: &[FileAttachment],
    ) -> Result<()> {
        // Images become base64 content blocks (and are also saved to disk);
        // files are saved to disk and referenced by path in the text prompt.
        let mut content_blocks = Vec::new();
        let mut saved_image_paths = Vec::new();
        let mut saved_file_paths = Vec::new();

        // Determine attachment directory (inside work dir)
        let work_dir = self.work_dir().await;
        let attach_dir = std::path::Path::new(&work_dir)
            .join(".agentbridge")
            .join("attachments");
        let _ = tokio::fs::create_dir_all(&attach_dir).await;

        // Save and encode images
        for (i, img) in images.iter().enumerate() {
            let ext = ext_from_mime(&img.mime_type);
            let fname = format!(
                "img_{}_{}{}",
                chrono::Utc::now().timestamp_millis(),
                i,
                ext
            );
            let fpath = attach_dir.join(&fname);
            if let Err(e) = tokio::fs::write(&fpath, &img.data).await {
                tracing::error!(error = %e, "failed to save image to disk");
                continue;
            }
            saved_image_paths.push(fpath.display().to_string());

            let b64 = base64_encode(&img.data);
            let mime = if img.mime_type.is_empty() {
                "image/png"
            } else {
                &img.mime_type
            };
            content_blocks.push(serde_json::json!({
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": mime,
                    "data": b64
                }
            }));
        }

        // Save files to disk (Claude Code reads them via file path)
        for (i, file) in files.iter().enumerate() {
            let fname = format!(
                "file_{}_{}",
                chrono::Utc::now().timestamp_millis(),
                if file.filename.is_empty() {
                    format!("file_{}", i)
                } else {
                    file.filename.clone()
                }
            );
            let fpath = attach_dir.join(&fname);
            if let Err(e) = tokio::fs::write(&fpath, &file.data).await {
                tracing::error!(error = %e, "failed to save file to disk");
                continue;
            }
            saved_file_paths.push(fpath.display().to_string());
        }

        // Build text part: user prompt + file/image path references
        let mut text_part = prompt.to_string();
        if text_part.is_empty() && !saved_file_paths.is_empty() {
            text_part = "Please analyze the attached file(s).".to_string();
        } else if text_part.is_empty() {
            text_part = "Please analyze the attached image(s).".to_string();
        }
        if !saved_image_paths.is_empty() {
            text_part.push_str(&format!(
                "\n\n(Images also saved locally: {})",
                saved_image_paths.join(", ")
            ));
        }
        if !saved_file_paths.is_empty() {
            text_part.push_str(&format!(
                "\n\n(Files saved locally, please read them: {})",
                saved_file_paths.join(", ")
            ));
        }

        content_blocks.push(serde_json::json!({
            "type": "text",
            "text": text_part
        }));

        let msg = serde_json::json!({
            "type": "user",
            "message": {
                "role": "user",
                "content": content_blocks
            }
        });
        self.write_json(&msg).await
    }

    async fn respond_permission(&self, request_id: &str, allow: bool) -> Result<()> {
        let response = if allow {
            serde_json::json!({
                "type": "control_response",
                "response": {
                    "subtype": "success",
                    "request_id": request_id,
                    "response": {
                        "behavior": "allow",
                        "updatedInput": {}
                    }
                }
            })
        } else {
            serde_json::json!({
                "type": "control_response",
                "response": {
                    "subtype": "success",
                    "request_id": request_id,
                    "response": {
                        "behavior": "deny",
                        "message": "Permission denied by user."
                    }
                }
            })
        };
        self.write_json(&response).await
    }

    fn permission_responder(&self) -> Arc<dyn PermissionResponder> {
        Arc::new(ClaudePermissionResponder {
            stdin: Arc::clone(&self.stdin),
        })
    }

    fn take_events(&mut self) -> Option<mpsc::Receiver<AgentEvent>> {
        Some(std::mem::replace(&mut self.events_rx, mpsc::channel(1).1))
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
        // We cannot do async here; use try_lock for a best-effort read.
        self.session_id
            .try_lock()
            .ok()
            .and_then(|guard| guard.clone())
    }

    fn alive(&self) -> bool {
        self.alive.load(Ordering::Relaxed)
    }

    async fn close(&self) -> Result<()> {
        graceful_shutdown(&self.stdin, &self.child, &self.alive).await
    }
}

/// Permission responder for ClaudeSession — writes control_response to the
/// subprocess stdin. Clonable via Arc so the engine can hold it while the
/// session itself is locked in the interactive state mutex.
struct ClaudePermissionResponder {
    stdin: Arc<Mutex<ChildStdin>>,
}

#[async_trait]
impl PermissionResponder for ClaudePermissionResponder {
    async fn respond(&self, request_id: &str, allow: bool) -> Result<()> {
        write_permission_response(&self.stdin, request_id, allow).await;
        Ok(())
    }
}

impl ClaudeSession {
    /// Get a clone of the stdin handle for writing permission responses.
    pub fn stdin_handle(&self) -> Arc<Mutex<ChildStdin>> {
        Arc::clone(&self.stdin)
    }

    /// Get the work directory this session is running in.
    async fn work_dir(&self) -> String {
        self.work_dir.display().to_string()
    }

    /// Write a JSON value followed by a newline to the child's stdin.
    async fn write_json(&self, value: &serde_json::Value) -> Result<()> {
        let bytes = serde_json::to_vec(value)?;
        let mut stdin_lock = self.stdin.lock().await;
        stdin_lock
            .write_all(&bytes)
            .await
            .context("write to claude stdin")?;
        stdin_lock
            .write_all(b"\n")
            .await
            .context("write newline to claude stdin")?;
        stdin_lock
            .flush()
            .await
            .context("flush claude stdin")?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ClaudeAgent (factory / manager)
// ---------------------------------------------------------------------------

/// Factory that spawns or resumes Claude Code sessions.
pub struct ClaudeAgent {
    work_dir: PathBuf,
    config: AgentConfig,
    project_name: String,
}

impl ClaudeAgent {
    pub fn new(work_dir: PathBuf, config: AgentConfig, project_name: String) -> Self {
        Self {
            work_dir,
            config,
            project_name,
        }
    }

    /// Start a new session or resume an existing one.
    ///
    /// If `session_id` is provided and the resume fails, a warning is logged
    /// and a fresh session is started instead.
    pub async fn start_session(
        &self,
        session_id: Option<&str>,
        model: Option<&str>,
        mode: &str,
        work_dir_override: Option<&str>,
    ) -> Result<ClaudeSession> {
        match self
            .try_start_session(session_id, model, mode, work_dir_override)
            .await
        {
            Ok(session) => Ok(session),
            Err(e) if session_id.is_some() => {
                tracing::warn!(
                    session_id = ?session_id,
                    error = %e,
                    "agent: resume failed, starting fresh session"
                );
                self.try_start_session(None, model, mode, work_dir_override)
                    .await
            }
            Err(e) => Err(e),
        }
    }

    /// Internal: attempt to spawn the claude process with the given parameters.
    async fn try_start_session(
        &self,
        session_id: Option<&str>,
        model: Option<&str>,
        mode: &str,
        work_dir_override: Option<&str>,
    ) -> Result<ClaudeSession> {
        let effective_work_dir = work_dir_override
            .map(PathBuf::from)
            .unwrap_or_else(|| self.work_dir.clone());

        let args = self.build_args(session_id, model, mode, &effective_work_dir);

        tracing::info!(
            work_dir = %effective_work_dir.display(),
            mode = %mode,
            model = ?model,
            session_id = ?session_id,
            "agent: spawning claude session"
        );

        let mut cmd = Command::new("claude");
        cmd.args(&args)
            .current_dir(&effective_work_dir)
            .env("AGENTBRIDGE_PROJECT", &self.project_name)
            .env_remove("CLAUDECODE")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = cmd.spawn().context("failed to spawn claude process")?;
        let stdin = child
            .stdin
            .take()
            .context("claude process has no stdin")?;
        let stdout = child
            .stdout
            .take()
            .context("claude process has no stdout")?;

        let stdin = Arc::new(Mutex::new(stdin));
        let alive = Arc::new(AtomicBool::new(true));
        let session_id_store = Arc::new(Mutex::new(None::<String>));
        let child = Arc::new(Mutex::new(child));

        let (tx, rx) = mpsc::channel::<AgentEvent>(128);

        // Determine if we should auto-approve permissions
        let auto_approve = matches!(mode, "yolo" | "auto" | "bypassPermissions");

        // Spawn the background read loop
        let read_stdin = Arc::clone(&stdin);
        let read_alive = Arc::clone(&alive);
        let read_session_id = Arc::clone(&session_id_store);
        let read_child = Arc::clone(&child);
        tokio::spawn(async move {
            read_loop(
                stdout,
                tx,
                read_stdin,
                read_child,
                read_alive,
                read_session_id,
                auto_approve,
            )
            .await;
        });

        Ok(ClaudeSession {
            stdin,
            events_rx: rx,
            session_id: session_id_store,
            alive,
            child,
            work_dir: effective_work_dir,
        })
    }

    /// Build the argument list for the `claude` CLI.
    fn build_args(
        &self,
        session_id: Option<&str>,
        model: Option<&str>,
        mode: &str,
        effective_work_dir: &std::path::Path,
    ) -> Vec<String> {
        let mut args: Vec<String> = vec![
            "--output-format".into(),
            "stream-json".into(),
            "--input-format".into(),
            "stream-json".into(),
            "--permission-prompt-tool".into(),
            "stdio".into(),
            "--verbose".into(),
            // Skip global plugins, hooks, LSP, CLAUDE.md auto-discovery.
            // agentbridge handles platform communication; Claude should not
            // load MCP plugins (e.g. Discord) that conflict with our transport.
            "--bare".into(),
        ];

        // Max turns
        args.push("--max-turns".into());
        args.push(self.config.max_turns.unwrap_or(50).to_string());

        // Permission mode
        if mode != "default" && !mode.is_empty() {
            match mode {
                "yolo" => {
                    args.push("--permission-mode".into());
                    args.push("bypassPermissions".into());
                }
                _ => {
                    args.push("--permission-mode".into());
                    args.push(mode.into());
                }
            }
        }

        // Model
        if let Some(m) = model {
            args.push("--model".into());
            args.push(m.into());
        }

        // Session resume
        if let Some(sid) = session_id {
            match sid {
                "" => {
                    // Empty string — truly fresh session, no resume.
                }
                CONTINUE_SESSION => {
                    // --continue grabs the most recent session in the workspace,
                    // which may belong to an active CLI terminal. Fork it so the
                    // platform conversation gets its own independent context branch.
                    args.push("--continue".into());
                    args.push("--fork-session".into());
                }
                _ => {
                    // Resuming a known session ID — safe to resume directly.
                    args.push("--resume".into());
                    args.push(sid.into());
                }
            }
        }

        // Allowed tools
        if !self.config.allowed_tools.is_empty() {
            args.push("--allowedTools".into());
            args.push(self.config.allowed_tools.join(","));
        }

        // System prompt
        let sys_prompt = format!(
            "You are running inside agentbridge, a bridge that connects you to messaging platforms. \
             Your normal text responses are automatically delivered to the user — just reply normally.\n\n\
             ## Formatting\n\
             Keep responses concise. Use markdown. Do NOT include your thinking process in the response — \
             thinking is handled separately and not shown to the user. Never prefix responses with emoji \
             like 💭 or show chain-of-thought reasoning in your reply text.\n\n\
             ## Project\n\
             Name: {project}\n\
             Work directory: {work_dir}",
            project = self.project_name,
            work_dir = effective_work_dir.display(),
        );
        args.push("--append-system-prompt".into());
        args.push(sys_prompt);

        args
    }
}

// ---------------------------------------------------------------------------
// Read loop -- parse stdout JSON lines into AgentEvent
// ---------------------------------------------------------------------------

async fn read_loop(
    stdout: tokio::process::ChildStdout,
    tx: mpsc::Sender<AgentEvent>,
    stdin: Arc<Mutex<ChildStdin>>,
    child: Arc<Mutex<Child>>,
    alive: Arc<AtomicBool>,
    session_id: Arc<Mutex<Option<String>>>,
    auto_approve: bool,
) {
    let reader = BufReader::new(stdout);
    let mut lines = reader.lines();

    while let Ok(Some(line)) = lines.next_line().await {
        if line.is_empty() {
            continue;
        }

        let raw: serde_json::Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => {
                tracing::debug!(line = %line, "agent: non-JSON line");
                continue;
            }
        };

        let event_type = raw.get("type").and_then(|t| t.as_str()).unwrap_or("");
        tracing::debug!(event_type = %event_type, "agent: event");

        match event_type {
            "system" => {
                handle_system_event(&raw, &tx, &session_id).await;
            }

            "assistant" => {
                if let Some(events) = parse_assistant_message(&raw) {
                    for evt in events {
                        if tx.send(evt).await.is_err() {
                            break;
                        }
                    }
                }
            }

            "user" => {
                handle_user_event(&raw);
            }

            "result" => {
                handle_result_event(&raw, &tx, &session_id).await;
                // Do NOT break — the process stays alive for the next turn.
                // The engine's event loop will stop reading after Result,
                // then send the next message. We continue reading stdout
                // so we can process the next turn's events.
            }

            "control_request" => {
                handle_control_request(&raw, &stdin, &tx, auto_approve).await;
            }

            "control_cancel_request" => {
                let rid = raw
                    .get("request_id")
                    .and_then(|r| r.as_str())
                    .unwrap_or("");
                tracing::debug!(request_id = %rid, "agent: permission cancelled");
            }

            "error" => {
                let message = raw
                    .get("error")
                    .and_then(|e| e.get("message"))
                    .and_then(|m| m.as_str())
                    .or_else(|| raw.get("message").and_then(|m| m.as_str()))
                    .unwrap_or("unknown error")
                    .to_string();
                let _ = tx.send(AgentEvent::Error { message }).await;
            }

            _ => {
                tracing::trace!(event_type = %event_type, "agent: unhandled event type");
            }
        }
    }

    // Mark session as no longer alive
    alive.store(false, Ordering::Release);

    // Wait for child to fully exit
    if let Ok(mut child_lock) = child.try_lock() {
        let _ = child_lock.wait().await;
    }
}

// ---------------------------------------------------------------------------
// Event handlers
// ---------------------------------------------------------------------------

/// Handle a `"system"` event -- extract session_id, tools, skills.
async fn handle_system_event(
    raw: &serde_json::Value,
    tx: &mpsc::Sender<AgentEvent>,
    session_id_store: &Arc<Mutex<Option<String>>>,
) {
    let sid = raw
        .get("session_id")
        .and_then(|s| s.as_str())
        .unwrap_or("")
        .to_string();

    let tools: Vec<String> = raw
        .get("tools")
        .and_then(|t| t.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    let skills: Vec<String> = raw
        .get("skills")
        .and_then(|t| t.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    if !sid.is_empty() {
        tracing::info!(session_id = %sid, "agent: session established");
        let mut store = session_id_store.lock().await;
        *store = Some(sid.clone());
    }

    let _ = tx
        .send(AgentEvent::System {
            session_id: sid,
            tools,
            skills,
        })
        .await;
}

/// Handle a `"user"` event -- log tool errors for debugging.
fn handle_user_event(raw: &serde_json::Value) {
    if let Some(content) = raw
        .get("message")
        .and_then(|m| m.get("content"))
        .and_then(|c| c.as_array())
    {
        for item in content {
            if item.get("type").and_then(|t| t.as_str()) == Some("tool_result")
                && item.get("is_error").and_then(|e| e.as_bool()) == Some(true) {
                    let err = item.get("content").and_then(|c| c.as_str()).unwrap_or("");
                    tracing::debug!(error = %err, "agent: tool error");
                }
        }
    }
}

/// Handle a `"result"` event -- extract session_id, tokens, content.
async fn handle_result_event(
    raw: &serde_json::Value,
    tx: &mpsc::Sender<AgentEvent>,
    session_id_store: &Arc<Mutex<Option<String>>>,
) {
    let content = raw
        .get("result")
        .and_then(|r| r.as_str())
        .unwrap_or("")
        .to_string();

    let sid = raw
        .get("session_id")
        .and_then(|s| s.as_str())
        .unwrap_or("")
        .to_string();

    let input_tokens = raw
        .get("usage")
        .and_then(|u| u.get("input_tokens"))
        .and_then(|t| t.as_u64())
        .unwrap_or(0) as u32;

    let output_tokens = raw
        .get("usage")
        .and_then(|u| u.get("output_tokens"))
        .and_then(|t| t.as_u64())
        .unwrap_or(0) as u32;

    if !sid.is_empty() {
        tracing::info!(session_id = %sid, input_tokens, output_tokens, "agent: result");
        let mut store = session_id_store.lock().await;
        *store = Some(sid.clone());
    }

    let _ = tx
        .send(AgentEvent::Result {
            content,
            session_id: sid,
            input_tokens,
            output_tokens,
        })
        .await;
}

/// Handle a `"control_request"` -- auto-approve and emit visibility event.
async fn handle_control_request(
    raw: &serde_json::Value,
    stdin: &Arc<Mutex<ChildStdin>>,
    tx: &mpsc::Sender<AgentEvent>,
    auto_approve: bool,
) {
    let request_id = raw
        .get("request_id")
        .and_then(|r| r.as_str())
        .unwrap_or("")
        .to_string();

    let request = match raw.get("request") {
        Some(r) => r,
        None => return,
    };

    let subtype = request
        .get("subtype")
        .and_then(|s| s.as_str())
        .unwrap_or("");

    if subtype != "can_use_tool" {
        tracing::debug!(subtype = %subtype, "agent: unknown control request subtype");
        return;
    }

    let tool_name = request
        .get("tool_name")
        .and_then(|t| t.as_str())
        .unwrap_or("unknown")
        .to_string();

    let input = request
        .get("input")
        .cloned()
        .unwrap_or(serde_json::json!({}));

    if auto_approve {
        tracing::debug!(tool = %tool_name, request_id = %request_id, "agent: auto-approving");
        write_permission_response(stdin, &request_id, true).await;

        // Emit a ToolUse event for visibility
        let input_summary = summarize_tool_input(&tool_name, &input);
        let _ = tx
            .send(AgentEvent::ToolUse {
                id: request_id,
                tool: tool_name,
                input: input_summary,
            })
            .await;
    } else {
        // Forward the permission request to the caller for interactive approval
        tracing::info!(
            tool = %tool_name,
            request_id = %request_id,
            "agent: permission request"
        );
        let _ = tx
            .send(AgentEvent::PermissionRequest {
                request_id,
                tool: tool_name,
                input,
                options: vec![],
            })
            .await;
    }
}

// ---------------------------------------------------------------------------
// Permission response
// ---------------------------------------------------------------------------

/// Write a control_response to the child's stdin.
pub async fn write_permission_response(
    stdin: &Arc<Mutex<ChildStdin>>,
    request_id: &str,
    allow: bool,
) {
    let response = if allow {
        serde_json::json!({
            "type": "control_response",
            "response": {
                "subtype": "success",
                "request_id": request_id,
                "response": {
                    "behavior": "allow",
                    "updatedInput": {}
                }
            }
        })
    } else {
        serde_json::json!({
            "type": "control_response",
            "response": {
                "subtype": "success",
                "request_id": request_id,
                "response": {
                    "behavior": "deny",
                    "message": "Permission denied by user."
                }
            }
        })
    };

    let mut stdin_lock = stdin.lock().await;
    if let Ok(bytes) = serde_json::to_vec(&response) {
        let _ = stdin_lock.write_all(&bytes).await;
        let _ = stdin_lock.write_all(b"\n").await;
        let _ = stdin_lock.flush().await;
    }
}

// ---------------------------------------------------------------------------
// Assistant message parsing
// ---------------------------------------------------------------------------

/// Parse assistant message content blocks into AgentEvent values.
fn parse_assistant_message(raw: &serde_json::Value) -> Option<Vec<AgentEvent>> {
    let content = raw.get("message")?.get("content")?.as_array()?;
    let mut events = Vec::new();

    for item in content {
        let content_type = item.get("type").and_then(|t| t.as_str()).unwrap_or("");
        match content_type {
            "text" => {
                if let Some(text) = item.get("text").and_then(|t| t.as_str()) {
                    if !text.is_empty() {
                        events.push(AgentEvent::Text {
                            content: text.to_string(),
                        });
                    }
                }
            }
            "tool_use" => {
                let id = item
                    .get("id")
                    .and_then(|i| i.as_str())
                    .unwrap_or("")
                    .to_string();
                let tool = item
                    .get("name")
                    .and_then(|n| n.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                // Skip AskUserQuestion tool_use events
                if tool == "AskUserQuestion" {
                    continue;
                }
                let input = item
                    .get("input")
                    .map(|i| summarize_tool_input(&tool, i))
                    .unwrap_or_default();
                events.push(AgentEvent::ToolUse { id, tool, input });
            }
            "thinking" => {
                if let Some(thinking) = item.get("thinking").and_then(|t| t.as_str()) {
                    if !thinking.is_empty() {
                        events.push(AgentEvent::Thinking {
                            content: thinking.to_string(),
                        });
                    }
                }
            }
            _ => {}
        }
    }

    if events.is_empty() {
        None
    } else {
        Some(events)
    }
}

// ---------------------------------------------------------------------------
// Tool input summarization
// ---------------------------------------------------------------------------

/// Produce a short human-readable summary of a tool invocation's input.
fn summarize_tool_input(tool_name: &str, input: &serde_json::Value) -> String {
    match tool_name {
        "Read" | "Glob" | "Grep" => {
            if let Some(path) = input
                .get("file_path")
                .or(input.get("pattern"))
                .and_then(|p| p.as_str())
            {
                return path.to_string();
            }
        }
        "Edit" | "Write" => {
            if let Some(path) = input.get("file_path").and_then(|p| p.as_str()) {
                return path.to_string();
            }
        }
        "Bash" => {
            if let Some(cmd) = input.get("command").and_then(|c| c.as_str()) {
                if cmd.chars().count() > 100 {
                    return format!("{}...", cmd.chars().take(100).collect::<String>());
                }
                return cmd.to_string();
            }
        }
        _ => {}
    }

    // Fallback: JSON truncated to 200 chars
    let s = serde_json::to_string(input).unwrap_or_default();
    if s.chars().count() > 200 {
        format!("{}...", s.chars().take(200).collect::<String>())
    } else {
        s
    }
}

// ---------------------------------------------------------------------------
// Graceful shutdown
// ---------------------------------------------------------------------------

/// Gracefully shut down the child process:
/// 1. Close stdin
/// 2. Wait up to 120 s for exit
/// 3. SIGTERM + wait 5 s
/// 4. SIGKILL
async fn graceful_shutdown(
    stdin: &Arc<Mutex<ChildStdin>>,
    child: &Arc<Mutex<Child>>,
    alive: &Arc<AtomicBool>,
) -> Result<()> {
    // 1. Close stdin (drop the writer)
    {
        let mut stdin_lock = stdin.lock().await;
        let _ = stdin_lock.shutdown().await;
    }

    // 2. Wait up to 3 s for the process to exit on its own
    let exited = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            {
                let mut child_lock = child.lock().await;
                if let Ok(Some(_status)) = child_lock.try_wait() {
                    return;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
    })
    .await;

    if exited.is_ok() {
        alive.store(false, Ordering::Release);
        tracing::info!("agent: session exited gracefully");
        return Ok(());
    }

    // 3. SIGTERM (via /bin/kill to avoid a libc dependency)
    tracing::warn!("agent: session did not exit in 3s, sending SIGTERM");
    {
        let child_lock = child.lock().await;
        if let Some(pid) = child_lock.id() {
            let _ = std::process::Command::new("kill")
                .args(["-s", "TERM", &pid.to_string()])
                .status();
        }
    }

    // Wait 2 more seconds
    let exited2 = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            {
                let mut child_lock = child.lock().await;
                if let Ok(Some(_status)) = child_lock.try_wait() {
                    return;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
    })
    .await;

    if exited2.is_ok() {
        alive.store(false, Ordering::Release);
        tracing::info!("agent: session exited after SIGTERM");
        return Ok(());
    }

    // 4. SIGKILL
    tracing::warn!("agent: session did not exit after SIGTERM, sending SIGKILL");
    {
        let mut child_lock = child.lock().await;
        let _ = child_lock.kill().await;
    }

    alive.store(false, Ordering::Release);
    tracing::info!("agent: session killed");
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Map MIME type to file extension.
fn ext_from_mime(mime: &str) -> &'static str {
    match mime {
        "image/png" => ".png",
        "image/jpeg" | "image/jpg" => ".jpg",
        "image/gif" => ".gif",
        "image/webp" => ".webp",
        "image/svg+xml" => ".svg",
        "application/pdf" => ".pdf",
        "text/plain" => ".txt",
        "application/json" => ".json",
        _ => "",
    }
}

/// Simple base64 encoder (avoids pulling in a separate crate).
pub(crate) fn base64_encode(data: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::with_capacity(data.len().div_ceil(3) * 4);

    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;

        result.push(CHARS[((triple >> 18) & 0x3F) as usize] as char);
        result.push(CHARS[((triple >> 12) & 0x3F) as usize] as char);

        if chunk.len() > 1 {
            result.push(CHARS[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }

        if chunk.len() > 2 {
            result.push(CHARS[(triple & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
    }

    result
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summarize_read_tool() {
        let input = serde_json::json!({"file_path": "/src/main.rs"});
        assert_eq!(summarize_tool_input("Read", &input), "/src/main.rs");
    }

    #[test]
    fn summarize_grep_tool() {
        let input = serde_json::json!({"pattern": "fn main"});
        assert_eq!(summarize_tool_input("Grep", &input), "fn main");
    }

    #[test]
    fn summarize_glob_tool() {
        let input = serde_json::json!({"pattern": "**/*.rs"});
        assert_eq!(summarize_tool_input("Glob", &input), "**/*.rs");
    }

    #[test]
    fn summarize_edit_tool() {
        let input = serde_json::json!({"file_path": "/src/lib.rs", "old_string": "x", "new_string": "y"});
        assert_eq!(summarize_tool_input("Edit", &input), "/src/lib.rs");
    }

    #[test]
    fn summarize_write_tool() {
        let input = serde_json::json!({"file_path": "/tmp/out.txt", "content": "hello"});
        assert_eq!(summarize_tool_input("Write", &input), "/tmp/out.txt");
    }

    #[test]
    fn summarize_bash_short() {
        let input = serde_json::json!({"command": "ls -la"});
        assert_eq!(summarize_tool_input("Bash", &input), "ls -la");
    }

    #[test]
    fn summarize_bash_long_truncates() {
        let long_cmd = "x".repeat(200);
        let input = serde_json::json!({"command": long_cmd});
        let summary = summarize_tool_input("Bash", &input);
        assert!(summary.len() <= 104); // 100 + "..."
        assert!(summary.ends_with("..."));
    }

    #[test]
    fn summarize_unknown_tool_json() {
        let input = serde_json::json!({"key": "value"});
        let summary = summarize_tool_input("CustomTool", &input);
        assert_eq!(summary, r#"{"key":"value"}"#);
    }

    #[test]
    fn summarize_unknown_tool_truncates_long_json() {
        let long_val = "v".repeat(300);
        let input = serde_json::json!({"key": long_val});
        let summary = summarize_tool_input("CustomTool", &input);
        assert!(summary.len() <= 204); // 200 + "..."
        assert!(summary.ends_with("..."));
    }

    #[test]
    fn base64_encode_works() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"Hello, World!"), "SGVsbG8sIFdvcmxkIQ==");
    }

    #[test]
    fn parse_assistant_text() {
        let raw = serde_json::json!({
            "type": "assistant",
            "message": {
                "content": [
                    {"type": "text", "text": "Hello"}
                ]
            }
        });
        let events = parse_assistant_message(&raw).unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            AgentEvent::Text { content } => assert_eq!(content, "Hello"),
            _ => panic!("expected Text event"),
        }
    }

    #[test]
    fn parse_assistant_thinking() {
        let raw = serde_json::json!({
            "type": "assistant",
            "message": {
                "content": [
                    {"type": "thinking", "thinking": "Let me think..."}
                ]
            }
        });
        let events = parse_assistant_message(&raw).unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            AgentEvent::Thinking { content } => assert_eq!(content, "Let me think..."),
            _ => panic!("expected Thinking event"),
        }
    }

    #[test]
    fn parse_assistant_tool_use() {
        let raw = serde_json::json!({
            "type": "assistant",
            "message": {
                "content": [
                    {
                        "type": "tool_use",
                        "id": "tu_123",
                        "name": "Read",
                        "input": {"file_path": "/tmp/test.rs"}
                    }
                ]
            }
        });
        let events = parse_assistant_message(&raw).unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            AgentEvent::ToolUse { id, tool, input } => {
                assert_eq!(id, "tu_123");
                assert_eq!(tool, "Read");
                assert_eq!(input, "/tmp/test.rs");
            }
            _ => panic!("expected ToolUse event"),
        }
    }

    #[test]
    fn parse_assistant_skips_ask_user() {
        let raw = serde_json::json!({
            "type": "assistant",
            "message": {
                "content": [
                    {
                        "type": "tool_use",
                        "id": "tu_456",
                        "name": "AskUserQuestion",
                        "input": {"question": "what?"}
                    }
                ]
            }
        });
        let events = parse_assistant_message(&raw);
        assert!(events.is_none());
    }

    #[test]
    fn parse_assistant_empty_text_skipped() {
        let raw = serde_json::json!({
            "type": "assistant",
            "message": {
                "content": [
                    {"type": "text", "text": ""}
                ]
            }
        });
        let events = parse_assistant_message(&raw);
        assert!(events.is_none());
    }

    #[test]
    fn parse_assistant_mixed_content() {
        let raw = serde_json::json!({
            "type": "assistant",
            "message": {
                "content": [
                    {"type": "thinking", "thinking": "hmm"},
                    {"type": "text", "text": "Here you go"},
                    {
                        "type": "tool_use",
                        "id": "tu_789",
                        "name": "Bash",
                        "input": {"command": "echo hi"}
                    }
                ]
            }
        });
        let events = parse_assistant_message(&raw).unwrap();
        assert_eq!(events.len(), 3);
        assert!(matches!(&events[0], AgentEvent::Thinking { .. }));
        assert!(matches!(&events[1], AgentEvent::Text { .. }));
        assert!(matches!(&events[2], AgentEvent::ToolUse { .. }));
    }

    #[test]
    fn claude_agent_build_args_defaults() {
        let agent = ClaudeAgent::new(
            PathBuf::from("/work"),
            AgentConfig::default(),
            "test-proj".to_string(),
        );
        let args = agent.build_args(None, None, "default", &PathBuf::from("/work"));

        assert!(args.contains(&"--output-format".to_string()));
        assert!(args.contains(&"stream-json".to_string()));
        assert!(args.contains(&"--input-format".to_string()));
        assert!(args.contains(&"--permission-prompt-tool".to_string()));
        assert!(args.contains(&"stdio".to_string()));
        assert!(args.contains(&"--verbose".to_string()));
        assert!(args.contains(&"--max-turns".to_string()));
        assert!(args.contains(&"50".to_string()));
        // No --permission-mode for "default"
        assert!(!args.contains(&"--permission-mode".to_string()));
        // No --model
        assert!(!args.contains(&"--model".to_string()));
        // No --resume
        assert!(!args.contains(&"--resume".to_string()));
    }

    #[test]
    fn claude_agent_build_args_with_overrides() {
        let config = AgentConfig {
            mode: "default".to_string(),
            model: None,
            allowed_tools: vec!["Bash".to_string(), "Read".to_string()],
            max_turns: Some(10),
        };
        let agent = ClaudeAgent::new(
            PathBuf::from("/work"),
            config,
            "proj".to_string(),
        );
        let args = agent.build_args(
            Some("sess-123"),
            Some("claude-sonnet-4-20250514"),
            "yolo",
            &PathBuf::from("/other"),
        );

        // Max turns from config
        let mt_idx = args.iter().position(|a| a == "--max-turns").unwrap();
        assert_eq!(args[mt_idx + 1], "10");

        // Permission mode: yolo -> bypassPermissions
        let pm_idx = args.iter().position(|a| a == "--permission-mode").unwrap();
        assert_eq!(args[pm_idx + 1], "bypassPermissions");

        // Model
        let m_idx = args.iter().position(|a| a == "--model").unwrap();
        assert_eq!(args[m_idx + 1], "claude-sonnet-4-20250514");

        // Resume
        let r_idx = args.iter().position(|a| a == "--resume").unwrap();
        assert_eq!(args[r_idx + 1], "sess-123");

        // Allowed tools
        let at_idx = args.iter().position(|a| a == "--allowedTools").unwrap();
        assert_eq!(args[at_idx + 1], "Bash,Read");

        // System prompt contains project and work dir
        let sp_idx = args.iter().position(|a| a == "--append-system-prompt").unwrap();
        let sp = &args[sp_idx + 1];
        assert!(sp.contains("proj"));
        assert!(sp.contains("/other"));
    }

    #[test]
    fn claude_agent_build_args_continue_session() {
        let agent = ClaudeAgent::new(
            PathBuf::from("/work"),
            AgentConfig::default(),
            "test-proj".to_string(),
        );
        let args = agent.build_args(
            Some(CONTINUE_SESSION),
            None,
            "default",
            &PathBuf::from("/work"),
        );

        // __continue__ should use --continue --fork-session, not --resume
        assert!(args.contains(&"--continue".to_string()));
        assert!(args.contains(&"--fork-session".to_string()));
        assert!(!args.contains(&"--resume".to_string()));
    }

    #[test]
    fn claude_agent_build_args_empty_session_id_no_resume() {
        let agent = ClaudeAgent::new(
            PathBuf::from("/work"),
            AgentConfig::default(),
            "test-proj".to_string(),
        );
        let args = agent.build_args(Some(""), None, "default", &PathBuf::from("/work"));

        // Empty session ID should not resume
        assert!(!args.contains(&"--resume".to_string()));
        assert!(!args.contains(&"--continue".to_string()));
    }
}
