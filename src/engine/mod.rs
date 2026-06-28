//! New engine: the central message router.
//!
//! Receives messages from platforms via the `MessageHandler` callback,
//! performs dedup, access control, alias resolution, command detection,
//! manages session locks (try_lock or queue), dispatches to the agent,
//! and drives the `StreamPreview` state machine.
//!
//! The engine interacts with platforms exclusively through the capability
//! traits defined in `crate::core::platform` -- it never knows about
//! concrete platform implementations.

#![allow(clippy::too_many_arguments)] // engine pipeline functions need many state refs; plumbing a context struct would not reduce complexity

pub mod commands;
pub mod events;
pub mod skills;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::{mpsc, Mutex};

use crate::config::{AliasConfig, AppConfig, ProjectConfig};
use crate::core::event::AgentEvent;
use crate::core::message::IncomingMessage;
use crate::core::platform::{MessageHandler, PlatformCapabilities, ReplyCtx};
use crate::core::session::{QueuedMessage, Session, SessionManager};
use crate::agent::{AgentSession, PermissionResponder};
use std::collections::HashSet;
use crate::cron::CronScheduler;
use crate::dedup::DedupTracker;
use crate::ratelimit::RateLimiter;

/// Maximum number of messages that can be queued per session.
const MAX_QUEUED_MESSAGES: usize = 5;

// ---------------------------------------------------------------------------
// Permission decision types for the pending-permission flow
// ---------------------------------------------------------------------------

/// User's decision on a permission request.
#[derive(Debug, Clone)]
pub enum PermissionDecision {
    Allow,
    Deny,
    AllowAll,
}

/// Engine-level interactive state per session key.
/// Holds the core session + agent session + message queue + permission state.
struct EngineInteractiveState {
    session: Arc<Session>,
    agent_session: Option<Box<dyn AgentSession>>,
    pending_messages: std::collections::VecDeque<QueuedMessage>,
    /// Channel sender for resolving pending permission requests.
    perm_tx: Option<mpsc::Sender<PermissionDecision>>,
    /// True ONLY when a PermissionRequest is actively waiting for user response.
    has_pending_permission: Arc<AtomicBool>,
    /// Set to true by /stop to signal the event loop to abort immediately.
    stopped: Arc<AtomicBool>,
    /// When true, auto-approve all subsequent permission requests for this session.
    approve_all: bool,
    /// Per-channel override for the `verbose` display switch, set by `/verbose`.
    /// `None` means "follow config default"; `Some(b)` overrides it at runtime.
    verbose_override: Option<bool>,
}

impl EngineInteractiveState {
    fn new(session: Arc<Session>) -> Self {
        Self {
            session,
            agent_session: None,
            pending_messages: std::collections::VecDeque::new(),
            perm_tx: None,
            has_pending_permission: Arc::new(AtomicBool::new(false)),
            stopped: Arc::new(AtomicBool::new(false)),
            approve_all: false,
            verbose_override: None,
        }
    }

    fn queue_message(&mut self, msg: QueuedMessage) -> bool {
        if self.pending_messages.len() >= MAX_QUEUED_MESSAGES {
            return false;
        }
        self.pending_messages.push_back(msg);
        true
    }

    fn queue_len(&self) -> usize {
        self.pending_messages.len()
    }

    fn drain_next(&mut self) -> Option<QueuedMessage> {
        self.pending_messages.pop_front()
    }
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

/// The central message router.
///
/// Receives messages from platforms, routes them through dedup, access
/// control, alias resolution, and command detection, then dispatches to
/// the agent and streams the response back.
pub struct Engine {
    config: ProjectConfig,
    #[allow(dead_code)]
    app_config: AppConfig,
    platforms: Vec<Arc<dyn PlatformCapabilities>>,
    sessions: Arc<SessionManager>,
    interactive_states: Arc<Mutex<HashMap<String, EngineInteractiveState>>>,
    /// Per-user active agent name. Missing entry = default_agent from config.
    active_agents: Arc<Mutex<HashMap<String, String>>>,
    dedup: Arc<DedupTracker>,
    rate_limiter: Arc<RateLimiter>,
    /// Per (session_key, agent_name) model override set via /model command.
    model_override: Arc<tokio::sync::Mutex<HashMap<(String, String), String>>>,
    /// Per (session_key, agent_name) mode override set via /mode command.
    mode_override: Arc<tokio::sync::Mutex<HashMap<(String, String), String>>>,
    cron_scheduler: Arc<CronScheduler>,
    skill_registry: Arc<skills::SkillRegistry>,
    /// Broadcast channel for gateway event forwarding.
    /// Events are sent here so the gateway client can relay them.
    event_broadcast: Arc<tokio::sync::broadcast::Sender<(String, crate::core::event::AgentEvent)>>,
    /// Routes Claude Code hook events to the matching session's event channel,
    /// keyed by canonicalized work_dir. Bound when a tmux session starts and
    /// unbound when it is cleaned up.
    hook_route: Arc<crate::hook_route::HookRouteRegistry>,
}

impl Engine {
    /// Create a new engine for the given project.
    pub fn new(config: ProjectConfig, app_config: AppConfig) -> Self {
        let sessions = Arc::new(SessionManager::new(
            &app_config.data_dir,
            &config.work_dir,
        ));
        let rate_limiter = Arc::new(RateLimiter::new(&config.rate_limit));
        let cron_scheduler = Arc::new(CronScheduler::new(&app_config.data_dir));

        // Initialize skill registry
        let scan_dirs = skills::default_scan_dirs(&config.work_dir);
        let skill_registry = Arc::new(skills::SkillRegistry::new(&scan_dirs));
        let (event_broadcast, _) = tokio::sync::broadcast::channel(256);
        let event_broadcast = Arc::new(event_broadcast);
        let skill_count = skill_registry.list_all().len();
        if skill_count > 0 {
            tracing::info!(count = skill_count, "skills discovered");
        }

        // Rehydrate per-key active agent selections from the persisted session
        // store so that a restart does not silently snap every user back to the
        // project default. We only trust sessions that still carry a non-empty
        // `agent_type` (and only those still referenced by `active_keys`),
        // otherwise we fall back to the config default on first message.
        let known_agents: std::collections::HashSet<String> = config
            .resolved_agents()
            .into_iter()
            .map(|a| a.name)
            .collect();
        let mut hydrated: HashMap<String, String> = HashMap::new();
        for (key, session) in sessions.active_sessions() {
            let agent_type = session.agent_type.trim();
            if agent_type.is_empty() {
                continue;
            }
            if !known_agents.is_empty() && !known_agents.contains(agent_type) {
                tracing::debug!(
                    key = %key,
                    agent = %agent_type,
                    "skipping hydrated agent — not in current config",
                );
                continue;
            }
            hydrated.insert(key, agent_type.to_string());
        }
        if !hydrated.is_empty() {
            tracing::info!(count = hydrated.len(), "restored active agent selections");
        }

        Self {
            config,
            app_config,
            platforms: Vec::new(),
            sessions,
            interactive_states: Arc::new(Mutex::new(HashMap::new())),
            active_agents: Arc::new(Mutex::new(hydrated)),
            dedup: Arc::new(DedupTracker::new()),
            rate_limiter,
            model_override: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            mode_override: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            cron_scheduler,
            skill_registry,
            event_broadcast,
            hook_route: Arc::new(crate::hook_route::HookRouteRegistry::new()),
        }
    }

    /// The hook route registry, shared with the hook receiver so inbound hook
    /// events resolve to this engine's bound sessions.
    #[allow(dead_code)] // public accessor; main injects a shared registry via set_hook_route
    pub fn hook_route(&self) -> Arc<crate::hook_route::HookRouteRegistry> {
        Arc::clone(&self.hook_route)
    }

    /// Replace this engine's hook route registry with a shared one so a single
    /// hook receiver can resolve bindings across multiple project engines. Must
    /// be called before `start()` (before any session can bind).
    pub fn set_hook_route(&mut self, hook_route: Arc<crate::hook_route::HookRouteRegistry>) {
        self.hook_route = hook_route;
    }

    /// Start all configured platforms with the engine's message handler.
    ///
    /// Platforms are created from the project's platform configs and stored
    /// in `self.platforms`.
    pub async fn start(&mut self) -> Result<()> {
        for platform_cfg in &self.config.platforms {
            let platform = create_platform_capabilities(platform_cfg)?;
            let handler = self.make_handler();
            platform.start(handler).await?;
            self.platforms.push(platform);
        }

        // Register all commands (built-in + custom + skills) with platforms.
        let all_commands = self.collect_all_commands();
        for p in &self.platforms {
            if let Err(e) = p.register_commands(&all_commands).await {
                tracing::warn!(platform = %p.name(), error = %e, "failed to register commands");
            }
        }

        // Start cron scheduler with platform map for message dispatch
        let mut platform_map = std::collections::HashMap::new();
        for p in &self.platforms {
            platform_map.insert(p.name().to_string(), Arc::clone(p));
        }
        let cron_handler = self.make_handler();
        self.cron_scheduler.start(cron_handler, platform_map, Arc::clone(&self.active_agents));

        tracing::info!(project = %self.config.name, "engine started");
        Ok(())
    }

    /// Get a message handler for external injection (gateway, webhook).
    pub fn handler(&self) -> MessageHandler {
        self.make_handler()
    }

    /// Resolve names for any Discord sessions that don't have one yet.
    ///
    /// Fetches `GET /channels/{id}` for each unnamed `discord:<id>` session
    /// and writes the returned `name` back to SessionManager. Safe to call
    /// concurrently with normal traffic — rename_session is idempotent.
    pub async fn backfill_session_names(&self) {
        // Locate Discord token from project config
        let discord_token = self.config.platforms.iter()
            .find(|p| p.platform_type == "discord")
            .and_then(|p| p.options.get("token"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let Some(token) = discord_token else { return };

        // Collect unnamed discord session keys
        let candidates: Vec<String> = self.sessions.list_all()
            .into_iter()
            .filter(|(_, s)| s.name.is_none())
            .map(|(key, _)| key)
            .filter(|key| key.starts_with("discord:"))
            .collect();

        if candidates.is_empty() {
            return;
        }
        tracing::info!(count = candidates.len(), "backfilling discord session names");

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .ok();
        let Some(client) = client else { return };

        for key in candidates {
            let channel_id = match key.split_once(':').map(|x| x.1) {
                Some(id) => id,
                None => continue,
            };
            let url = format!("https://discord.com/api/v10/channels/{}", channel_id);
            match client.get(&url)
                .header("Authorization", format!("Bot {}", token))
                .send().await
            {
                Ok(resp) if resp.status().is_success() => {
                    if let Ok(data) = resp.json::<serde_json::Value>().await {
                        if let Some(name) = data.get("name").and_then(|v| v.as_str()) {
                            let name = name.trim();
                            if !name.is_empty() {
                                self.sessions.rename_session(&key, name);
                                tracing::info!(session_key = %key, name = %name, "session renamed");
                            }
                        }
                    }
                }
                Ok(resp) => {
                    tracing::debug!(session_key = %key, status = %resp.status(), "discord channel fetch failed");
                }
                Err(e) => {
                    tracing::debug!(session_key = %key, error = %e, "discord channel fetch error");
                }
            }
        }
    }

    /// Get all platforms as a map.
    pub fn platforms_map(&self) -> std::collections::HashMap<String, Arc<dyn PlatformCapabilities>> {
        self.platforms.iter().map(|p| (p.name().to_string(), Arc::clone(p))).collect()
    }

    /// Subscribe to engine events for gateway forwarding.
    /// Returns a receiver that gets (session_key, AgentEvent) tuples.
    pub fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<(String, crate::core::event::AgentEvent)> {
        self.event_broadcast.subscribe()
    }

    /// Collect all commands: built-in + custom + skills.
    fn collect_all_commands(&self) -> Vec<crate::core::platform::BotCommand> {
        use crate::core::platform::BotCommand;
        let mut cmds = vec![
            BotCommand { name: "new".into(), description: "New session".into() },
            BotCommand { name: "list".into(), description: "List sessions".into() },
            BotCommand { name: "switch".into(), description: "Switch session".into() },
            BotCommand { name: "resume".into(), description: "List sessions, or resume one".into() },
            BotCommand { name: "current".into(), description: "Current session info".into() },
            BotCommand { name: "delete".into(), description: "Delete session".into() },
            BotCommand { name: "name".into(), description: "Rename session".into() },
            BotCommand { name: "stop".into(), description: "Stop current task".into() },
            BotCommand { name: "compress".into(), description: "Compress context".into() },
            BotCommand { name: "history".into(), description: "Session history".into() },
            BotCommand { name: "model".into(), description: "Switch model".into() },
            BotCommand { name: "mode".into(), description: "Switch permission mode".into() },
            BotCommand { name: "agent".into(), description: "Switch agent backend".into() },
            BotCommand { name: "dir".into(), description: "Change work directory".into() },
            BotCommand { name: "attach".into(), description: "Bind channel to a tmux session".into() },
            BotCommand { name: "verbose".into(), description: "Toggle showing tool progress + thinking".into() },
            BotCommand { name: "status".into(), description: "Show status".into() },
            BotCommand { name: "skills".into(), description: "List skills".into() },
            BotCommand { name: "cron".into(), description: "Scheduled tasks".into() },
            BotCommand { name: "commands".into(), description: "List custom commands".into() },
            BotCommand { name: "sync".into(), description: "Sync sessions".into() },
            BotCommand { name: "help".into(), description: "Show help".into() },
        ];

        // Custom commands from config
        for c in &self.config.commands {
            cmds.push(BotCommand {
                name: c.name.clone(),
                description: c.description.clone(),
            });
        }

        // Skills from filesystem
        for s in self.skill_registry.list_all() {
            cmds.push(BotCommand {
                name: s.name.clone(),
                description: s.description.clone(),
            });
        }

        cmds
    }

    /// Gracefully shut down all platforms.
    pub async fn stop(&self) -> Result<()> {
        for p in &self.platforms {
            p.stop().await?;
        }
        tracing::info!(project = %self.config.name, "engine stopped");
        Ok(())
    }

    /// Create the `MessageHandler` closure that platforms invoke when a
    /// message arrives.
    ///
    /// The closure captures `Arc` references to all shared state so it can
    /// be sent across threads. Each incoming message is processed in its
    /// own spawned task.
    fn make_handler(&self) -> MessageHandler {
        let config = self.config.clone();
        let sessions = self.sessions.clone();
        let interactive_states = self.interactive_states.clone();
        let active_agents = self.active_agents.clone();
        let dedup = self.dedup.clone();
        let rate_limiter = self.rate_limiter.clone();
        let model_override = self.model_override.clone();
        let mode_override = self.mode_override.clone();
        let cron_scheduler = self.cron_scheduler.clone();
        let skill_registry = self.skill_registry.clone();
        let event_broadcast = self.event_broadcast.clone();
        let hook_route = self.hook_route.clone();

        Arc::new(
            move |platform: Arc<dyn PlatformCapabilities>, msg: IncomingMessage| {
                let config = config.clone();
                let sessions = sessions.clone();
                let interactive_states = interactive_states.clone();
                let active_agents = active_agents.clone();
                let dedup = dedup.clone();
                let rate_limiter = rate_limiter.clone();
                let model_override = model_override.clone();
                let mode_override = mode_override.clone();
                let cron_scheduler = cron_scheduler.clone();
                let skill_registry = skill_registry.clone();
                let event_broadcast = event_broadcast.clone();
                let hook_route = hook_route.clone();

                tokio::spawn(async move {
                    let reply_ctx = msg.reply_ctx.clone();
                    if let Err(e) = handle_message(
                        &config,
                        &sessions,
                        &interactive_states,
                        &active_agents,
                        &dedup,
                        &rate_limiter,
                        &model_override,
                        &mode_override,
                        &cron_scheduler,
                        &skill_registry,
                        &event_broadcast,
                        &hook_route,
                        platform.clone(),
                        msg,
                    )
                    .await
                    {
                        tracing::error!(error = %e, "message handling failed");
                        let err_msg = format!("💥 出错了: {}", e);
                        let _ = platform.reply(reply_ctx.as_ref(), &err_msg).await;
                    }
                });
            },
        )
    }
}

// ---------------------------------------------------------------------------
// Core message handling pipeline
// ---------------------------------------------------------------------------

/// The main routing logic for an incoming message.
///
/// Steps:
/// 1. Dedup check
/// 2. Access control
/// 3. STT (voice -> text, if applicable)
/// 4. Banned words filter
/// 5. Rate limiting
/// 6. Alias resolution
/// 7. Command detection (starts with `/`)
/// 8. Auto-sync pull
/// 9. Session try_lock or queue
/// 10. Start agent, run event loop
/// 11. Auto-sync push
/// 12. Unlock session, drain queue
async fn handle_message(
    config: &ProjectConfig,
    sessions: &SessionManager,
    interactive_states: &Mutex<HashMap<String, EngineInteractiveState>>,
    active_agents: &Mutex<HashMap<String, String>>,
    dedup: &DedupTracker,
    rate_limiter: &RateLimiter,
    model_override: &tokio::sync::Mutex<HashMap<(String, String), String>>,
    mode_override: &tokio::sync::Mutex<HashMap<(String, String), String>>,
    cron_scheduler: &CronScheduler,
    skill_registry: &skills::SkillRegistry,
    event_broadcast: &Arc<tokio::sync::broadcast::Sender<(String, AgentEvent)>>,
    hook_route: &Arc<crate::hook_route::HookRouteRegistry>,
    platform: Arc<dyn PlatformCapabilities>,
    mut msg: IncomingMessage,
) -> Result<()> {
    tracing::info!(
        id = %msg.id,
        from = %msg.from,
        platform = platform.name(),
        text_len = msg.text.len(),
        text_preview = %msg.text.chars().take(60).collect::<String>(),
        starts_with_slash = msg.text.starts_with('/'),
        "handle_message: received"
    );

    // 1. Dedup
    if !dedup.check(&msg.id) {
        tracing::debug!(id = %msg.id, "duplicate message dropped");
        return Ok(());
    }

    // 2. Access control
    if !is_allowed(&config.allow_from, &msg.from) {
        tracing::warn!(id = %msg.id, from = %msg.from, platform = platform.name(), "access denied");
        return Ok(());
    }

    // 3. STT: transcribe voice if present and no text
    if msg.text.is_empty() {
        if let Some(ref voice_path) = msg.voice {
            let speech_cfg = config.speech.clone().unwrap_or_default();
            match crate::speech::transcribe(&speech_cfg, std::path::Path::new(voice_path)).await {
                Ok(text) => {
                    tracing::info!(chars = text.len(), "STT: transcribed voice message");
                    msg.text = text;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "STT: transcription failed");
                    platform
                        .reply(msg.reply_ctx.as_ref(), &format!("🎙️ 语音识别失败: {}", e))
                        .await?;
                    return Ok(());
                }
            }
        }
    }

    // 4. Banned words filter
    if contains_banned_word(&msg.text, &config.banned_words) {
        tracing::info!(from = %msg.from, "message dropped by banned words filter");
        return Ok(());
    }

    // 5. Rate limiting
    if !rate_limiter.check(&msg.from) {
        platform
            .reply(msg.reply_ctx.as_ref(), "🐢 慢一点~ 消息太频繁了")
            .await?;
        return Ok(());
    }

    // 6. Alias resolution
    let mut msg = resolve_alias(msg, &config.aliases);

    // 7. Command detection
    if msg.text.starts_with('/') {
        // Try built-in commands first (these return immediately with a reply)
        let handled = handle_command_message(
            config,
            sessions,
            interactive_states,
            active_agents,
            model_override,
            mode_override,
            cron_scheduler,
            skill_registry,
            hook_route,
            platform.clone(),
            &msg,
        )
        .await?;
        if handled {
            return Ok(());
        }
        // Not a built-in command. Check custom commands and skills -- these
        // rewrite msg.text to a prompt and fall through to the agent dispatch.
        let parts: Vec<&str> = msg.text[1..].splitn(2, char::is_whitespace).collect();
        let cmd = parts[0];
        let args = parts.get(1).copied().unwrap_or("").trim();

        // Check custom commands (config-defined)
        if let Some(custom) = config.commands.iter().find(|c| c.name == cmd) {
            tracing::info!(command = %cmd, "executing custom command via agent");
            let prompt = if args.is_empty() {
                custom.prompt.clone()
            } else {
                format!("{}\n\nUser arguments: {}", custom.prompt, args)
            };
            msg.text = prompt;
            // Fall through to agent dispatch below
        }
        // Check skills (file-system discovered)
        else if let Some(skill) = skill_registry.resolve(cmd) {
            tracing::info!(skill = %skill.name, source = %skill.source.display(), "executing skill via agent");
            msg.text = skills::build_skill_invocation_prompt(skill, args);
            // Fall through to agent dispatch below
        } else {
            // Unknown command
            platform
                .reply(
                    msg.reply_ctx.as_ref(),
                    &format!("❓ 未知命令: /{}\n输入 /help 查看可用命令。", cmd),
                )
                .await?;
            return Ok(());
        }
    }

    // 8. Auto-sync pull
    if let Some(ref sync_cfg) = config.sync {
        if sync_cfg.auto {
            if let Err(e) = crate::sync::run_sync(sync_cfg, crate::sync::Direction::Pull) {
                tracing::warn!(error = %e, "auto-sync pull failed");
            }
        }
    }

    // 8.5. Check for pending permission (BEFORE session lock!)
    // Pending-permission handling must run before TryLock so the permission
    // reply is processed even when the session is otherwise busy.
    let session_key = make_session_key(&platform, &msg);
    if handle_pending_permission(interactive_states, &session_key, &platform, &msg).await {
        return Ok(());
    }

    // 9. Session try_lock or queue

    // Ensure interactive state exists (creates session via SessionManager
    // if needed, but then stores and reuses the SAME Arc<Session>).
    let session = {
        let mut states = interactive_states.lock().await;
        let state = states
            .entry(session_key.clone())
            .or_insert_with(|| {
                let s = sessions.get_or_create(&session_key);
                EngineInteractiveState::new(s)
            });
        Arc::clone(&state.session)
    };

    // Auto-name unnamed sessions from channel_name (e.g. Discord thread name)
    if session.name.is_none() {
        if let Some(ref name) = msg.channel_name {
            sessions.rename_session(&session_key, name);
        }
    }

    // Try to acquire the session lock (explicit, not RAII guard).
    let locked = session.try_lock_explicit();
    tracing::info!(session = %&session.id[..8], locked, busy = session.is_busy(), "try_lock result");

    if !locked {
        // Check for /btw — inject mid-turn message into running agent
        let trimmed = msg.text.trim();
        if trimmed.starts_with("/btw ") || trimmed.starts_with("/btw\n") {
            let btw_text = trimmed.strip_prefix("/btw").unwrap_or("").trim();
            if !btw_text.is_empty() {
                let sent = {
                    let states = interactive_states.lock().await;
                    if let Some(state) = states.get(&session_key) {
                        if let Some(ref cs) = state.agent_session {
                            if cs.alive() {
                                cs.send(btw_text).await.is_ok()
                            } else { false }
                        } else { false }
                    } else { false }
                };
                if sent {
                    let _ = platform.reply_quoted(msg.reply_ctx.as_ref(), "💬 已注入").await;
                } else {
                    let _ = platform.reply(msg.reply_ctx.as_ref(), "💬 注入失败，没有运行中的 Agent").await;
                }
                return Ok(());
            }
        }

        // Session is busy -- queue the message for the running turn.
        let queued = QueuedMessage {
            text: msg.text.clone(),
            images: vec![],
            files: vec![],
            voice: msg.voice.clone(),
            from: msg.from.clone(),
            reply_ctx: msg.reply_ctx.clone(),
        };

        let queued_ok = {
            let mut states = interactive_states.lock().await;
            if let Some(state) = states.get_mut(&session_key) {
                if state.queue_message(queued) {
                    tracing::info!(from = %msg.from, queue_depth = state.queue_len(), "message queued for busy session");
                    true
                } else {
                    false
                }
            } else {
                false
            }
        };

        if queued_ok {
            let _ = platform
                .reply_quoted(msg.reply_ctx.as_ref(), "⏳ 排队中，前面还有任务在跑~")
                .await;

            // Race guard: the drain loop in process_and_drain may have just
            // finished (session unlocked) between our TryLock failure and the
            // queue append. Re-try TryLock -- if it succeeds, no one is
            // draining the queue so we must start a processor ourselves.
            if session.try_lock_explicit() {
                tracing::info!(session = %&session.id[..8], "race guard: re-acquired lock, draining orphaned queue");
                let agent_name = resolve_active_agent(active_agents, &session_key, config).await;
                let agent_entry = config.find_agent(&agent_name);
                let effective_model = {
                    let lock = model_override.lock().await;
                    lock.get(&(session_key.clone(), agent_name.clone()))
                        .cloned()
                        .or_else(|| agent_entry.as_ref().and_then(|e| e.model.clone()))
                };
                let effective_mode = {
                    let lock = mode_override.lock().await;
                    lock.get(&(session_key.clone(), agent_name.clone()))
                        .cloned()
                        .unwrap_or_else(|| {
                            agent_entry
                                .as_ref()
                                .map(|e| e.mode.clone())
                                .unwrap_or_else(|| config.agent.mode.clone())
                        })
                };
                drain_orphaned_queue(
                    config, sessions, active_agents, interactive_states, hook_route,
                    &session_key, &session, &platform,
                    &effective_model, &effective_mode,
                ).await;
            }
        } else {
            platform
                .reply(msg.reply_ctx.as_ref(), "🚫 队列满了，等一下再发吧")
                .await?;
        }

        return Ok(());
    }

    // Auto-reset idle sessions AFTER lock acquired.
    if config.reset_on_idle_mins > 0 {
        let idle_mins = (chrono::Utc::now() - session.updated_at).num_minutes();
        if idle_mins >= config.reset_on_idle_mins as i64 && session.agent_session_id.is_none() {
            tracing::info!(session_key = %session_key, idle_mins, "auto-resetting idle session");
            let new_s = sessions.new_session(&session_key, None);
            // Release old lock, cleanup state, re-lock new session
            session.unlock();
            cleanup_agent_session(config, sessions, interactive_states, hook_route, &session_key).await;
            if !new_s.try_lock_explicit() {
                return Ok(()); // shouldn't happen, but be safe
            }
            // Update session reference for process_and_drain
            let session = {
                let mut states = interactive_states.lock().await;
                let state = states
                    .entry(session_key.clone())
                    .or_insert_with(|| EngineInteractiveState::new(Arc::clone(&new_s)));
                Arc::clone(&state.session)
            };
            let _ = session; // rebind
        }
    }

    tracing::info!(
        from = %msg.from,
        session = %&session.id[..8],
        "routing to agent"
    );

    let current_agent = resolve_active_agent(active_agents, &session_key, config).await;
    let current_entry = config.find_agent(&current_agent);

    let model_override_snap = {
        let lock = model_override.lock().await;
        lock.get(&(session_key.clone(), current_agent.clone()))
            .cloned()
            .or_else(|| current_entry.as_ref().and_then(|e| e.model.clone()))
    };
    let mode_override_snap = {
        let lock = mode_override.lock().await;
        lock.get(&(session_key.clone(), current_agent.clone()))
            .cloned()
            .unwrap_or_else(|| {
                current_entry
                    .as_ref()
                    .map(|e| e.mode.clone())
                    .unwrap_or_else(|| config.agent.mode.clone())
            })
    };

    // No need to spawn -- handle_message is already in a spawned task, so the
    // outer handler already provides the task context.
    process_and_drain(
        config,
        sessions,
        model_override_snap,
        mode_override_snap,
        active_agents,
        interactive_states,
        event_broadcast,
        hook_route,
        &session_key,
        &session,
        &platform,
        &msg,
    )
    .await;

    Ok(())
}

// ---------------------------------------------------------------------------
// Agent dispatch: process_and_drain
// ---------------------------------------------------------------------------

/// Process a user message through the agent and drain any queued messages
/// afterward. The session lock is released explicitly inside the drain loop
/// while holding the state mutex so queued messages can't race the unlock.
///
/// This function is called from a spawned task -- it owns the session lock
/// for the duration, and is responsible for unlocking it.
async fn process_and_drain(
    config: &ProjectConfig,
    sessions: &SessionManager,
    effective_model: Option<String>,
    effective_mode: String,
    active_agents: &Mutex<HashMap<String, String>>,
    interactive_states: &Mutex<HashMap<String, EngineInteractiveState>>,
    event_broadcast: &Arc<tokio::sync::broadcast::Sender<(String, AgentEvent)>>,
    hook_route: &Arc<crate::hook_route::HookRouteRegistry>,
    session_key: &str,
    session: &Arc<Session>,
    platform: &Arc<dyn PlatformCapabilities>,
    msg: &IncomingMessage,
) {
    // session.unlock() is NOT deferred here — it is called explicitly in
    // the drain loop below while holding state mutex to close the race window.
    // A safety net ensures the lock is released on early-return / panic paths.
    // We use a simple wrapper struct whose Drop checks is_busy.
    struct UnlockGuard(Arc<Session>);
    impl Drop for UnlockGuard {
        fn drop(&mut self) {
            if self.0.is_busy() {
                self.0.unlock();
                tracing::warn!(session = %&self.0.id[..8], "safety-net unlock on early return");
            }
        }
    }
    let _safety_guard = UnlockGuard(Arc::clone(session));

    // Start typing indicator IMMEDIATELY — before agent spawn/send, so the
    // user sees feedback during the 1-2s spawn time.
    let mut stop_typing: Option<Box<dyn FnOnce() + Send>> = None;
    if let Some(typing) = platform.as_typing_indicator() {
        match typing.start_typing(msg.reply_ctx.as_ref()).await {
            Ok(stop_fn) => stop_typing = Some(stop_fn),
            Err(e) => tracing::warn!(error = %e, "failed to start typing indicator"),
        }
    }

    let session_work_dir = sessions.get_work_dir(session_key);
    let session_tmux = sessions.get_tmux_session(session_key);
    // Resolve the per-channel verbose override once for this turn.
    let display = effective_display(config, interactive_states, session_key).await;

    // Get or create agent session + send the user message + take event rx.
    let rx = {
        let mut states = interactive_states.lock().await;
        let state = match states.get_mut(session_key) {
            Some(s) => s,
            None => {
                tracing::error!(session_key, "no interactive state for session");
                session.unlock();

                return;
            }
        };

        let need_new = state.agent_session.as_ref().is_none_or(|s| !s.alive());
        if need_new {
            // Read from SessionManager (authoritative source), NOT from cached
            // state.session Arc which may be stale after set_agent_session_id().
            let agent_session_id = sessions.get_agent_session_id(session_key);
            match start_agent_session_for_key(
                config,
                active_agents,
                session_key,
                agent_session_id.as_deref(),
                effective_model.as_deref(),
                &effective_mode,
                session_work_dir.as_deref(),
                session_tmux.as_deref(),
                hook_route,
            )
            .await
            {
                Ok(new_session) => {
                    state.agent_session = Some(new_session);
                }
                Err(e) => {
                    tracing::error!(error = %e, "failed to start agent session");
                    if let Some(stop_fn) = stop_typing.take() { stop_fn(); }
                    let _ = platform.reply(msg.reply_ctx.as_ref(), &format!("💥 Agent 启动失败: {}", e)).await;
                    session.unlock();
                    return;
                }
            }
        }

        // Drain stale events from previous turn before sending new message
        let cs = state.agent_session.as_mut().unwrap();
        cs.drain_stale_events();

        let prompt = msg.text.clone();

        // Send initial message
        tracing::info!(session_key, prompt_len = prompt.len(), "sending prompt to agent");
        if let Err(e) = cs.send(&prompt).await {
            tracing::error!(error = %e, "failed to send message to agent");
            if let Some(stop_fn) = stop_typing.take() { stop_fn(); }
            let _ = platform.reply(msg.reply_ctx.as_ref(), &format!("💥 发送失败: {}", e)).await;
            session.unlock();
            return;
        }

        // Take the event receiver
        match cs.take_events() {
            Some(rx) => rx,
            None => {
                tracing::error!("events already taken from agent session");
                if let Some(stop_fn) = stop_typing.take() { stop_fn(); }
                let _ = platform.reply(msg.reply_ctx.as_ref(), "💥 Agent 会话异常，发 /new 重建").await;
                session.unlock();
                return;
            }
        }
    };

    // Create permission channel and get a permission responder for the event loop.
    let (perm_tx, mut perm_rx) = mpsc::channel::<PermissionDecision>(1);
    let (responder, mut local_approve_all, pending_flag, stopped_flag) = {
        let mut states = interactive_states.lock().await;
        let state = states.get_mut(session_key).unwrap();
        state.perm_tx = Some(perm_tx);
        state.has_pending_permission.store(false, Ordering::Relaxed);
        state.stopped.store(false, Ordering::Relaxed);
        let responder = state.agent_session.as_ref().unwrap().permission_responder();
        let aa = state.approve_all;
        let pf = Arc::clone(&state.has_pending_permission);
        let sf = Arc::clone(&state.stopped);
        (responder, aa, pf, sf)
    };

    // Run the event loop for this turn (pass typing indicator ownership)
    let reply_ctx: &dyn ReplyCtx = msg.reply_ctx.as_ref();
    let inplace_progress = uses_inplace_tool_progress(active_agents, session_key, config).await;
    let (rx, result) = run_event_loop_and_save(
        platform, reply_ctx, rx, &mut perm_rx, &responder,
        &mut local_approve_all, &pending_flag, &stopped_flag, stop_typing,
        &display, event_broadcast, sessions, session_key, inplace_progress,
    ).await;

    // If the event loop produced no real result (empty result + channel closed),
    // the resume likely failed. Clear the stale session ID so next attempt
    // starts fresh instead of looping on the same broken resume.
    let agent_dead = {
        let states = interactive_states.lock().await;
        states.get(session_key)
            .and_then(|s| s.agent_session.as_ref())
            .is_none_or(|cs| !cs.alive())
    };
    if agent_dead && result.as_ref().is_none_or(|r| r.final_text.is_empty()) {
        // Resume failed — retry with a fresh session inline so the user's
        // message isn't lost.
        tracing::warn!(session_key, "agent died without producing output, retrying with fresh session");
        sessions.set_agent_session_id(session_key, "");

        // Spawn fresh agent (using active backend) and re-send the user's message
        let retry_ok = {
            let mut states = interactive_states.lock().await;
            if let Some(state) = states.get_mut(session_key) {
                let session_work_dir = sessions.get_work_dir(session_key);
                let session_tmux = sessions.get_tmux_session(session_key);
                match start_agent_session_for_key(
                    config, active_agents, session_key,
                    None, effective_model.as_deref(), &effective_mode,
                    session_work_dir.as_deref(),
                    session_tmux.as_deref(),
                    hook_route,
                ).await {
                    Ok(cs) => {
                        state.agent_session = Some(cs);
                        true
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "retry: failed to start fresh agent");
                        let _ = platform.reply(msg.reply_ctx.as_ref(), &format!("💥 Agent 启动失败: {}", e)).await;
                        false
                    }
                }
            } else { false }
        };

        if retry_ok {
            // Re-send the user's message and run event loop again
            let send_ok = {
                let mut states = interactive_states.lock().await;
                if let Some(state) = states.get_mut(session_key) {
                    let cs = state.agent_session.as_mut().unwrap();
                    cs.drain_stale_events();
                    cs.send(&msg.text).await.is_ok()
                } else { false }
            };

            if send_ok {
                let rx = {
                    let mut states = interactive_states.lock().await;
                    let state = states.get_mut(session_key).unwrap();
                    state.agent_session.as_mut().unwrap().take_events().unwrap()
                };

                let (perm_tx2, mut perm_rx2) = mpsc::channel::<PermissionDecision>(1);
                let (responder2, mut aa2, pf2) = {
                    let mut states = interactive_states.lock().await;
                    let state = states.get_mut(session_key).unwrap();
                    state.perm_tx = Some(perm_tx2);
                    state.has_pending_permission.store(false, Ordering::Relaxed);
                    let responder = state.agent_session.as_ref().unwrap().permission_responder();
                    let aa = state.approve_all;
                    let pf = Arc::clone(&state.has_pending_permission);
                    (responder, aa, pf)
                };

                // Start typing for retry
                let mut retry_typing: Option<Box<dyn FnOnce() + Send>> = None;
                if let Some(typing) = platform.as_typing_indicator() {
                    if let Ok(stop_fn) = typing.start_typing(msg.reply_ctx.as_ref()).await {
                        retry_typing = Some(stop_fn);
                    }
                }

                let reply_ctx: &dyn ReplyCtx = msg.reply_ctx.as_ref();
                let sf2 = Arc::new(AtomicBool::new(false));
                let (rx2, _) = run_event_loop_and_save(
                    platform, reply_ctx, rx, &mut perm_rx2, &responder2,
                    &mut aa2, &pf2, &sf2, retry_typing, &display, event_broadcast, sessions, session_key,
                    inplace_progress,
                ).await;

                local_approve_all = aa2;
                put_events_back(interactive_states, session_key, rx2).await;

                // Clear perm state from retry
                {
                    let mut states = interactive_states.lock().await;
                    if let Some(state) = states.get_mut(session_key) {
                        state.approve_all = local_approve_all;
                        state.perm_tx = None;
                        state.has_pending_permission.store(false, Ordering::Relaxed);
                    }
                }

                // Continue to drain below
            }
        }
    } else {
        // Normal path: store back approve_all and clear perm state
        {
            let mut states = interactive_states.lock().await;
            if let Some(state) = states.get_mut(session_key) {
                state.approve_all = local_approve_all;
                state.perm_tx = None;
                state.has_pending_permission.store(false, Ordering::Relaxed);
            }
        }

        // Put the receiver back
        put_events_back(interactive_states, session_key, rx).await;
    }

    // Auto-sync push
    if let Some(ref sync_cfg) = config.sync {
        if sync_cfg.auto {
            if let Err(e) = crate::sync::run_sync(sync_cfg, crate::sync::Direction::Push) {
                tracing::warn!(error = %e, "auto-sync push failed");
            }
        }
    }

    // Auto-compress if token threshold exceeded
    if config.auto_compress.enabled {
        if let Some(ref r) = result {
            if r.input_tokens > config.auto_compress.max_tokens {
                tracing::info!(
                    input_tokens = r.input_tokens,
                    threshold = config.auto_compress.max_tokens,
                    "auto-compress: token threshold exceeded, sending /compact"
                );
                let mut states = interactive_states.lock().await;
                if let Some(state) = states.get_mut(session_key) {
                    if let Some(ref mut cs) = state.agent_session {
                        if cs.alive() {
                            // Send /compact and drain the resulting events
                            // (otherwise they pile up as stale events)
                            let _ = cs.send("/compact").await;
                            cs.drain_stale_events();
                            // Wait a bit for the compress to produce events, then drain again
                            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                            cs.drain_stale_events();
                        }
                    }
                }
            }
        }
    }

    // Drain any messages queued while this turn was running.
    drain_pending_messages(
        config, sessions, active_agents, interactive_states, event_broadcast, hook_route, session_key, session, platform,
        &effective_model, &effective_mode,
    ).await;

    // Safety net (_safety_guard) will check is_busy() on drop.
    // Since drain_pending_messages already called session.unlock(),
    // is_busy() returns false and the guard does nothing.
}

/// Run the event loop for one turn: process events, save session ID.
/// Returns the event receiver (so it can be put back) and the result.
async fn run_event_loop_and_save(
    platform: &Arc<dyn PlatformCapabilities>,
    reply_ctx: &dyn ReplyCtx,
    mut rx: tokio::sync::mpsc::Receiver<AgentEvent>,
    perm_rx: &mut mpsc::Receiver<PermissionDecision>,
    responder: &Arc<dyn PermissionResponder>,
    approve_all: &mut bool,
    pending_flag: &Arc<AtomicBool>,
    stopped_flag: &Arc<AtomicBool>,
    stop_typing: Option<Box<dyn FnOnce() + Send>>,
    display: &crate::config::DisplayConfig,
    event_broadcast: &Arc<tokio::sync::broadcast::Sender<(String, AgentEvent)>>,
    sessions: &SessionManager,
    session_key: &str,
    tool_progress_inplace: bool,
) -> (tokio::sync::mpsc::Receiver<AgentEvent>, Option<events::EventLoopResult>) {
    match events::process_agent_events(platform, reply_ctx, &mut rx, perm_rx, responder, approve_all, pending_flag, stopped_flag, stop_typing, display, event_broadcast, session_key, tool_progress_inplace).await {
        Ok(result) => {
            // Save agent session ID for resume (but never persist the
            // __continue__ sentinel).
            if let Some(ref sid) = result.session_id {
                if !sid.is_empty() && sid != crate::agent::CONTINUE_SESSION {
                    sessions.set_agent_session_id(session_key, sid);
                }
            }
            (rx, Some(result))
        }
        Err(e) => {
            tracing::error!(error = %e, "event loop error");
            (rx, None)
        }
    }
}

/// Put the event receiver back into the agent session so future messages
/// can reuse the same process.
async fn put_events_back(
    interactive_states: &Mutex<HashMap<String, EngineInteractiveState>>,
    session_key: &str,
    rx: tokio::sync::mpsc::Receiver<AgentEvent>,
) {
    let mut states = interactive_states.lock().await;
    if let Some(state) = states.get_mut(session_key) {
        if let Some(ref mut cs) = state.agent_session {
            cs.replace_events(rx);
        }
    }
}

// ---------------------------------------------------------------------------
// Drain pending messages
// ---------------------------------------------------------------------------

/// Drain all pending messages from the queue, processing each through the
/// agent event loop.
///
/// KEY INSIGHT: The session is unlocked inside this function WHILE HOLDING
/// the state mutex, preventing the race where the queue appears empty but
/// a new message arrives before unlock.
///
/// Returns `true` if the session was unlocked by this function.
async fn drain_pending_messages(
    config: &ProjectConfig,
    sessions: &SessionManager,
    active_agents: &Mutex<HashMap<String, String>>,
    interactive_states: &Mutex<HashMap<String, EngineInteractiveState>>,
    event_broadcast: &Arc<tokio::sync::broadcast::Sender<(String, AgentEvent)>>,
    hook_route: &Arc<crate::hook_route::HookRouteRegistry>,
    session_key: &str,
    session: &Arc<Session>,
    platform: &Arc<dyn PlatformCapabilities>,
    effective_model: &Option<String>,
    effective_mode: &str,
) -> bool {
    loop {
        // Lock state mutex, check queue, unlock session if empty.
        let queued = {
            let mut states = interactive_states.lock().await;
            let state = match states.get_mut(session_key) {
                Some(s) => s,
                None => {
                    // No state -- just unlock
                    session.unlock();
                    return true;
                }
            };

            if state.pending_messages.is_empty() {
                // Queue is empty. Unlock session WHILE HOLDING state mutex.
                // This is the critical race-guard: no one can queue a message
                // between seeing "empty" and "unlocked" because they'd need
                // the state mutex to queue, and we hold it.
                session.unlock();
                tracing::info!(session = %&session.id[..8], "drain: queue empty, session unlocked");
                return true;
                // state mutex drops here
            }

            // Has items -- take the next one
            let q = state.drain_next().unwrap();
            tracing::info!(
                from = %q.from,
                remaining = state.queue_len(),
                "drain: processing queued message"
            );
            q
            // state mutex drops here
        };

        // Check agent session is still alive
        let alive = {
            let states = interactive_states.lock().await;
            states.get(session_key)
                .and_then(|s| s.agent_session.as_ref())
                .is_some_and(|cs| cs.alive())
        };

        if !alive {
            tracing::warn!(session_key, "drain: agent session dead, recreating");
            let session_work_dir = sessions.get_work_dir(session_key);
            let session_tmux = sessions.get_tmux_session(session_key);
            // Read from SessionManager (authoritative), not stale cached Arc
            let agent_session_id = sessions.get_agent_session_id(session_key);
            match start_agent_session_for_key(
                config,
                active_agents,
                session_key,
                agent_session_id.as_deref(),
                effective_model.as_deref(),
                effective_mode,
                session_work_dir.as_deref(),
                session_tmux.as_deref(),
                hook_route,
            ).await {
                Ok(new_session) => {
                    let mut states = interactive_states.lock().await;
                    if let Some(state) = states.get_mut(session_key) {
                        state.agent_session = Some(new_session);
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, "drain: failed to restart agent");
                    let _ = platform.reply(queued.reply_ctx.as_ref(), &format!("💥 Agent 会话已断开: {}", e)).await;
                    let mut states = interactive_states.lock().await;
                    if let Some(state) = states.get_mut(session_key) {
                        notify_dropped_queue(&mut state.pending_messages, platform).await;
                    }
                    session.unlock();
                    return true;
                }
            }
        }

        // Send the queued message to the agent and take event rx
        let rx = {
            let mut states = interactive_states.lock().await;
            let state = match states.get_mut(session_key) {
                Some(s) => s,
                None => {
                    session.unlock();
                    return true;
                }
            };

            let cs = match state.agent_session.as_mut() {
                Some(cs) => cs,
                None => {
                    let _ = platform.reply(queued.reply_ctx.as_ref(), "💥 没有活跃的 Agent 会话").await;
                    session.unlock();
                    return true;
                }
            };

            // Drain stale events before starting next turn
            cs.drain_stale_events();

            if let Err(e) = cs.send(&queued.text).await {
                tracing::error!(error = %e, "drain: failed to send queued message");
                let _ = platform.reply(queued.reply_ctx.as_ref(), &format!("💥 发送排队消息失败: {}", e)).await;
                session.unlock();
                return true;
            }

            match cs.take_events() {
                Some(rx) => rx,
                None => {
                    tracing::error!("drain: events already taken");
                    session.unlock();
                    return true;
                }
            }
        };

        // Create permission channel for this turn
        let (perm_tx, mut perm_rx) = mpsc::channel::<PermissionDecision>(1);
        let (responder, mut local_approve_all, pending_flag, stopped_flag) = {
            let mut states = interactive_states.lock().await;
            let state = states.get_mut(session_key).unwrap();
            state.perm_tx = Some(perm_tx);
            state.has_pending_permission.store(false, Ordering::Relaxed);
            state.stopped.store(false, Ordering::Relaxed);
            let responder = state.agent_session.as_ref().unwrap().permission_responder();
            let aa = state.approve_all;
            let pf = Arc::clone(&state.has_pending_permission);
            let sf = Arc::clone(&state.stopped);
            (responder, aa, pf, sf)
        };

        // Start typing for queued message turn
        let mut queued_stop_typing: Option<Box<dyn FnOnce() + Send>> = None;
        if let Some(typing) = platform.as_typing_indicator() {
            if let Ok(stop_fn) = typing.start_typing(queued.reply_ctx.as_ref()).await {
                queued_stop_typing = Some(stop_fn);
            }
        }

        // Process event loop for this queued message
        let reply_ctx: &dyn ReplyCtx = queued.reply_ctx.as_ref();
        let inplace_progress = uses_inplace_tool_progress(active_agents, session_key, config).await;
        let display = effective_display(config, interactive_states, session_key).await;
        let (rx, _result) = run_event_loop_and_save(
            platform, reply_ctx, rx, &mut perm_rx, &responder,
            &mut local_approve_all, &pending_flag, &stopped_flag, queued_stop_typing,
            &display, event_broadcast, sessions, session_key, inplace_progress,
        ).await;

        // Store back approve_all and clear perm state
        {
            let mut states = interactive_states.lock().await;
            if let Some(state) = states.get_mut(session_key) {
                state.approve_all = local_approve_all;
                state.perm_tx = None;
                state.has_pending_permission.store(false, Ordering::Relaxed);
            }
        }

        // Put receiver back
        put_events_back(interactive_states, session_key, rx).await;

        // Loop continues -- check queue again
    }
}

/// Drain orphaned queue -- called from the race guard when a message was
/// queued but the drain loop already exited.
async fn drain_orphaned_queue(
    config: &ProjectConfig,
    sessions: &SessionManager,
    active_agents: &Mutex<HashMap<String, String>>,
    interactive_states: &Mutex<HashMap<String, EngineInteractiveState>>,
    hook_route: &Arc<crate::hook_route::HookRouteRegistry>,
    session_key: &str,
    session: &Arc<Session>,
    platform: &Arc<dyn PlatformCapabilities>,
    effective_model: &Option<String>,
    effective_mode: &str,
) {
    // Check agent session is alive
    let (alive, has_state) = {
        let states = interactive_states.lock().await;
        match states.get(session_key) {
            Some(s) => (
                s.agent_session.as_ref().is_some_and(|cs| cs.alive()),
                true,
            ),
            None => (false, false),
        }
    };

    if !has_state || !alive {
        tracing::warn!(session_key, "drain_orphaned: no state or dead agent, unlocking");
        session.unlock();
        return;
    }

    // Delegate to drain_pending_messages which handles the race-free unlock.
    drain_pending_messages(
        config, sessions, active_agents, interactive_states, &Arc::new(tokio::sync::broadcast::channel(1).0),
        hook_route, session_key, session, platform,
        effective_model, effective_mode,
    ).await;
}

// ---------------------------------------------------------------------------
// Pending permission handler
// ---------------------------------------------------------------------------

/// Check if there's a pending permission for this session and handle the
/// user's response. Called BEFORE session lock in the message handler.
///
/// Returns true if the message was consumed as a permission response.
async fn handle_pending_permission(
    interactive_states: &Mutex<HashMap<String, EngineInteractiveState>>,
    session_key: &str,
    platform: &Arc<dyn PlatformCapabilities>,
    msg: &IncomingMessage,
) -> bool {
    let (perm_tx, has_pending) = {
        let states = interactive_states.lock().await;
        match states.get(session_key) {
            Some(state) => (state.perm_tx.clone(), state.has_pending_permission.clone()),
            None => return false,
        }
    };

    // Only intercept if there's an ACTUAL pending permission request
    if !has_pending.load(Ordering::Relaxed) {
        return false;
    }

    let tx = match perm_tx {
        Some(tx) => tx,
        None => return false,
    };

    let lower = msg.text.trim().to_lowercase();
    let decision = if is_allow_all_response(&lower) {
        Some(PermissionDecision::AllowAll)
    } else if is_allow_response(&lower) {
        Some(PermissionDecision::Allow)
    } else if is_deny_response(&lower) {
        Some(PermissionDecision::Deny)
    } else {
        None
    };

    match decision {
        Some(d) => {
            let _ = tx.send(d).await;
            true
        }
        None => {
            // Not a permission response — show buttons if supported, text fallback otherwise
            let hint = "🔐 有权限请求等待确认：";
            if let Some(btn_sender) = platform.as_inline_button_sender() {
                let buttons = vec![
                    crate::core::platform::Button {
                        text: "👍 放行".to_string(),
                        callback_data: "perm_text:allow".to_string(),
                    },
                    crate::core::platform::Button {
                        text: "🚫 拦截".to_string(),
                        callback_data: "perm_text:deny".to_string(),
                    },
                    crate::core::platform::Button {
                        text: "⚡ 全部放行".to_string(),
                        callback_data: "perm_text:allow_all".to_string(),
                    },
                ];
                let _ = btn_sender
                    .send_with_buttons(msg.reply_ctx.as_ref(), hint, &buttons)
                    .await;
            } else {
                let _ = platform
                    .reply(
                        msg.reply_ctx.as_ref(),
                        &format!("{}\n回复 `allow` 允许 / `deny` 拒绝 / `allow all` 全部允许", hint),
                    )
                    .await;
            }
            true // Still consumed — don't route to agent
        }
    }
}

/// Check if text is an "allow all" response.
fn is_allow_all_response(s: &str) -> bool {
    matches!(
        s,
        "allow all"
            | "allowall"
            | "approve all"
            | "yes all"
            | "允许所有"
            | "允许全部"
            | "全部允许"
            | "所有允许"
            | "都允许"
            | "全部同意"
    )
}

/// Check if text is an "allow" response.
fn is_allow_response(s: &str) -> bool {
    matches!(
        s,
        "allow" | "yes" | "y" | "ok" | "允许" | "同意" | "可以" | "好" | "好的" | "是" | "确认" | "approve"
    )
}

/// Check if text is a "deny" response.
fn is_deny_response(s: &str) -> bool {
    matches!(
        s,
        "deny" | "no" | "n" | "reject" | "拒绝" | "不允许" | "不行" | "不" | "否" | "取消" | "cancel"
    )
}

/// Notify user about dropped queued messages when agent dies.
async fn notify_dropped_queue(
    queue: &mut std::collections::VecDeque<QueuedMessage>,
    platform: &Arc<dyn PlatformCapabilities>,
) {
    while let Some(msg) = queue.pop_front() {
        let _ = platform
            .reply(msg.reply_ctx.as_ref(), "💤 Agent 已断开，排队消息已丢弃")
            .await;
    }
}

// ---------------------------------------------------------------------------
// Stale event draining
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Agent session cleanup
// ---------------------------------------------------------------------------

/// Clean up the agent session for a session key (called on /new, /switch, /delete).
/// Closes the agent process gracefully so the next message spawns a fresh one.
/// Clean up the interactive state for a session key.
///
/// Removes the ENTIRE interactive state entry (not just the agent session)
/// so the next message recreates it from SessionManager with a fresh
/// `Arc<Session>`.
///
/// Without this, the stale `Arc<Session>` in EngineInteractiveState would
/// still hold the old agent_session_id even after SessionManager was updated.
async fn cleanup_agent_session(
    config: &ProjectConfig,
    sessions: &SessionManager,
    interactive_states: &Mutex<HashMap<String, EngineInteractiveState>>,
    hook_route: &crate::hook_route::HookRouteRegistry,
    session_key: &str,
) {
    // Unbind this session's hook route so a hook arriving after teardown is no
    // longer relayed (and stops holding a stale sender). Use the same work_dir
    // resolution the bind site uses: per-channel /dir override, else the
    // project default. Also unbind the tmux session name when this session has
    // one recorded (set on /attach), mirroring the two-key bind.
    let work_dir = sessions
        .get_work_dir(session_key)
        .unwrap_or_else(|| config.work_dir.display().to_string());
    let tmux_session = sessions.get_tmux_session(session_key);
    hook_route.unbind(&work_dir, tmux_session.as_deref());

    let mut states = interactive_states.lock().await;
    if let Some(state) = states.remove(session_key) {
        // Unlock the session's busy flag so it doesn't stay stuck forever.
        // process_and_drain has a safety guard that checks is_busy() on drop,
        // but removing the state means it can't find the session to unlock.
        state.session.unlock();

        if let Some(session) = state.agent_session {
            tracing::info!(session_key = %session_key, "cleaning up interactive state");
            tokio::spawn(async move {
                if let Err(e) = session.close().await {
                    tracing::warn!(error = %e, "failed to close agent session");
                }
            });
        }
    }
}

// ---------------------------------------------------------------------------
// Command handling bridge
// ---------------------------------------------------------------------------

/// Try built-in commands only. Returns `true` if the command was handled,
/// `false` if it should be checked against custom commands and skills.
async fn handle_command_message(
    config: &ProjectConfig,
    sessions: &SessionManager,
    interactive_states: &Mutex<HashMap<String, EngineInteractiveState>>,
    active_agents: &Mutex<HashMap<String, String>>,
    model_override: &tokio::sync::Mutex<HashMap<(String, String), String>>,
    mode_override: &tokio::sync::Mutex<HashMap<(String, String), String>>,
    cron_scheduler: &CronScheduler,
    skill_registry: &skills::SkillRegistry,
    hook_route: &crate::hook_route::HookRouteRegistry,
    platform: Arc<dyn PlatformCapabilities>,
    msg: &IncomingMessage,
) -> Result<bool> {
    let parts: Vec<&str> = msg.text[1..].splitn(2, char::is_whitespace).collect();
    let cmd = parts[0];
    let args = parts.get(1).copied().unwrap_or("").trim();
    let session_key = make_session_key(&platform, msg);

    // /agent — switch or list agents
    if cmd == "agent" {
        let reply = handle_agent_command(
            config,
            sessions,
            interactive_states,
            active_agents,
            &session_key,
            args,
        )
        .await;
        platform.reply(msg.reply_ctx.as_ref(), &reply).await?;
        return Ok(true);
    }

    // Handle /compress — send /compact to the running agent
    if cmd == "compress" || cmd == "compact" {
        let sent = {
            let mut states = interactive_states.lock().await;
            if let Some(state) = states.get_mut(&session_key) {
                if let Some(ref mut cs) = state.agent_session {
                    if cs.alive() {
                        let _ = cs.send("/compact").await;
                        // Drain the resulting events
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                        cs.drain_stale_events();
                        true
                    } else { false }
                } else { false }
            } else { false }
        };
        let msg_text = if sent {
            "🗜️ 上下文压缩已触发"
        } else {
            "🗜️ 没有运行中的 Agent 会话"
        };
        platform.reply(msg.reply_ctx.as_ref(), msg_text).await?;
        return Ok(true);
    }

    // Handle /stop command — abort the CURRENT TURN without destroying the
    // session. For a per-turn-disposable backend (claude/acp) interrupt()
    // defaults to close(); for the tmux backend it sends Escape so the shared,
    // user-owned cc keeps running (killing it on /stop would close the very
    // session the user shares between phone and computer).
    if cmd == "stop" || cmd == "cancel" {
        // Signal the running event loop to stop immediately, unlock the session,
        // and interrupt the agent — all WITHOUT removing the interactive state
        // or unbinding the hook route, so the session stays bridged.
        {
            let states = interactive_states.lock().await;
            if let Some(state) = states.get(&session_key) {
                state.stopped.store(true, Ordering::Release);
                state.session.unlock();
                if let Some(agent) = state.agent_session.as_ref() {
                    if let Err(e) = agent.interrupt().await {
                        tracing::warn!(error = %e, "failed to interrupt agent session");
                    }
                }
            }
        }
        platform
            .reply(msg.reply_ctx.as_ref(), "🛑 已中断当前任务")
            .await?;
        return Ok(true);
    }

    // Handle /verbose command — per-channel toggle for the "show the work"
    // switch (tool progress + thinking / inter-tool text). Quiet mode leaves
    // only the final reply. `/verbose` with no arg toggles; `on`/`off` set it.
    if cmd == "verbose" || cmd == "quiet" {
        let arg = args.trim().to_lowercase();
        // `/quiet` is sugar for `/verbose off`.
        let target = if cmd == "quiet" {
            Some(false)
        } else {
            match arg.as_str() {
                "on" | "true" | "1" | "开" => Some(true),
                "off" | "false" | "0" | "关" => Some(false),
                "" => None, // toggle
                _ => None,
            }
        };
        let core_session = sessions.get_or_create(&session_key);
        let new_val = {
            let mut states = interactive_states.lock().await;
            let state = states
                .entry(session_key.clone())
                .or_insert_with(|| EngineInteractiveState::new(core_session));
            let current = state.verbose_override.unwrap_or(config.display.verbose);
            let v = target.unwrap_or(!current);
            state.verbose_override = Some(v);
            v
        };
        let text = if new_val {
            "🔊 详细模式开:会显示工具调用进度和思考过程"
        } else {
            "🤫 安静模式开:只发最终回复,过程不显示"
        };
        platform.reply(msg.reply_ctx.as_ref(), text).await?;
        return Ok(true);
    }

    // Handle /skills command (list available skills)
    if cmd == "skills" {
        let all_skills = skill_registry.list_all();
        if all_skills.is_empty() {
            platform
                .reply(msg.reply_ctx.as_ref(), "🔍 暂无可用技能。")
                .await?;
        } else {
            let mut lines = vec![format!("🎯 可用技能 ({}):", all_skills.len())];
            for s in &all_skills {
                let desc = if s.description.is_empty() {
                    "".to_string()
                } else {
                    format!(" — {}", s.description)
                };
                lines.push(format!("  /{}{}", s.name, desc));
            }
            platform
                .reply(msg.reply_ctx.as_ref(), &lines.join("\n"))
                .await?;
        }
        return Ok(true);
    }

    // Try built-in commands. Agent name comes from per-user ActiveAgent map
    // (falls back to config.default_agent_name() if unset).
    let agent_name = resolve_active_agent(active_agents, &session_key, config).await;
    let reply = commands::dispatch(
        cmd,
        args,
        config,
        sessions,
        &session_key,
        &agent_name,
        model_override,
        mode_override,
        &config.commands,
        cron_scheduler,
    )
    .await;

    if let Some(reply_text) = reply {
        // If session or work directory changed, kill the running agent process
        // so the next message spawns a fresh one in the correct context.
        if matches!(cmd, "new" | "switch" | "delete" | "dir" | "cd" | "attach" | "resume") {
            let session_key = make_session_key(&platform, msg);
            cleanup_agent_session(config, sessions, interactive_states, hook_route, &session_key).await;
        }
        platform.reply(msg.reply_ctx.as_ref(), &reply_text).await?;
        return Ok(true);
    }

    // Not a built-in command -- caller will check custom commands and skills
    Ok(false)
}

// ---------------------------------------------------------------------------
// Utility functions
// ---------------------------------------------------------------------------

/// Start a fresh agent session using the active backend for this session_key.
///
/// Reads the active agent name from `active_agents` (falling back to
/// `config.default_agent_name()`), looks up the matching `AgentEntry`, and
/// dispatches through the agent registry. Returns a `Box<dyn AgentSession>`.
async fn start_agent_session_for_key(
    config: &ProjectConfig,
    active_agents: &Mutex<HashMap<String, String>>,
    session_key: &str,
    resume_session_id: Option<&str>,
    model_override: Option<&str>,
    mode_override: &str,
    work_dir_override: Option<&str>,
    tmux_session_override: Option<&str>,
    hook_route: &Arc<crate::hook_route::HookRouteRegistry>,
) -> Result<Box<dyn AgentSession>> {
    let agent_name = resolve_active_agent(active_agents, session_key, config).await;
    let entry = config
        .find_agent(&agent_name)
        .ok_or_else(|| anyhow::anyhow!("unknown active agent: {}", agent_name))?;

    let mode_opt = if mode_override.is_empty() {
        None
    } else {
        Some(mode_override)
    };
    crate::agent::registry::start_session_for_entry(
        &entry,
        config.work_dir.clone(),
        &config.name,
        resume_session_id,
        model_override,
        mode_opt,
        work_dir_override,
        session_key,
        tmux_session_override,
        hook_route,
    )
    .await
}

/// Build the effective `DisplayConfig` for a turn: the project's `display`
/// config with the per-channel `verbose` override (set by `/verbose`) applied.
/// Returns an owned config so the event loop borrows a stable value.
async fn effective_display(
    config: &ProjectConfig,
    interactive_states: &Mutex<HashMap<String, EngineInteractiveState>>,
    session_key: &str,
) -> crate::config::DisplayConfig {
    let mut display = config.display.clone();
    let states = interactive_states.lock().await;
    if let Some(v) = states.get(session_key).and_then(|s| s.verbose_override) {
        display.verbose = v;
    }
    display
}

/// Whether this session's active backend relays tool progress in place.
///
/// True only for the tmux backend with `hook_relay` on: there PostToolUse
/// hooks arrive as `ToolUse` events and the event loop coalesces them into a
/// single in-place-edited progress message. The claude/acp backends keep their
/// existing per-tool reply, so this is false for them.
async fn uses_inplace_tool_progress(
    active_agents: &Mutex<HashMap<String, String>>,
    session_key: &str,
    config: &ProjectConfig,
) -> bool {
    let agent_name = resolve_active_agent(active_agents, session_key, config).await;
    config
        .find_agent(&agent_name)
        .map(|e| e.backend == "tmux" && e.tmux.as_ref().is_some_and(|t| t.hook_relay))
        .unwrap_or(false)
}

/// Resolve the active agent name for a session key, falling back to the
/// project default when no explicit selection has been made.
async fn resolve_active_agent(
    active_agents: &Mutex<HashMap<String, String>>,
    session_key: &str,
    config: &ProjectConfig,
) -> String {
    let map = active_agents.lock().await;
    if let Some(name) = map.get(session_key) {
        return name.clone();
    }
    drop(map);
    config.default_agent_name()
}

/// Handle `/agent` and `/agent <name>` commands.
async fn handle_agent_command(
    config: &ProjectConfig,
    sessions: &SessionManager,
    interactive_states: &Mutex<HashMap<String, EngineInteractiveState>>,
    active_agents: &Mutex<HashMap<String, String>>,
    session_key: &str,
    args: &str,
) -> String {
    let agents = config.resolved_agents();
    let current = resolve_active_agent(active_agents, session_key, config).await;

    if args.is_empty() {
        let mut lines = vec![format!("Current agent: {}", current)];
        lines.push("Available:".to_string());
        for a in &agents {
            let marker = if a.name == current { " <-" } else { "" };
            let extra = if a.backend == "acp" {
                a.acp
                    .as_ref()
                    .map(|c| format!(" ({})", c.command))
                    .unwrap_or_default()
            } else {
                " (claude)".to_string()
            };
            lines.push(format!("  {}{}{}", a.name, extra, marker));
        }
        return lines.join("\n");
    }

    let target_name = args.trim();
    let known: HashSet<&str> = agents.iter().map(|a| a.name.as_str()).collect();
    if !known.contains(target_name) {
        return format!(
            "Unknown agent: {}\nAvailable: {}",
            target_name,
            agents.iter().map(|a| a.name.as_str()).collect::<Vec<_>>().join(", ")
        );
    }
    if target_name == current {
        return format!("Already using agent: {}", target_name);
    }

    // Busy detection: refuse to switch if the current session is actively processing.
    let busy = {
        let states = interactive_states.lock().await;
        states
            .get(session_key)
            .map(|s| {
                s.session.is_busy()
                    || s.has_pending_permission.load(Ordering::Relaxed)
                    || !s.pending_messages.is_empty()
            })
            .unwrap_or(false)
    };
    if busy {
        return format!(
            "Agent {} is busy. Run /stop first, then switch.",
            current
        );
    }

    // Take the old agent session out of state so we can close it off the hot path.
    let old_session = {
        let mut states = interactive_states.lock().await;
        states.remove(session_key).and_then(|mut s| s.agent_session.take())
    };

    // Record the new active agent *before* returning the reply so the next
    // incoming message routes to the right backend.
    {
        let mut map = active_agents.lock().await;
        map.insert(session_key.to_string(), target_name.to_string());
    }

    // Start a fresh session scoped to the new agent.
    let _ = sessions.new_session_with_agent(session_key, None, target_name);

    // Close the old subprocess in the background — with shortened timeouts this
    // is typically <1s, but we never want to block the reply on it.
    if let Some(cs) = old_session {
        tokio::spawn(async move {
            if let Err(e) = cs.close().await {
                tracing::warn!(error = %e, "agent: background close failed");
            }
        });
    }

    format!("Switched to agent: {}. New session started.", target_name)
}

/// Check whether a user is in the allow-list.
///
/// The allow-list is a comma-separated string of user IDs, or `"*"` to
/// allow everyone.
fn is_allowed(allow_from: &str, user_id: &str) -> bool {
    if allow_from == "*" {
        return true;
    }
    allow_from.split(',').any(|id| id.trim() == user_id)
}

/// Derive a session key from the platform name and message context.
///
/// In group chats the key is based on the channel ID; in DMs it is based
/// on the sender.
fn make_session_key(platform: &Arc<dyn PlatformCapabilities>, msg: &IncomingMessage) -> String {
    let base = if msg.is_group {
        msg.channel_id.as_deref().unwrap_or(&msg.from)
    } else {
        &msg.from
    };
    format!("{}:{}", platform.name(), base)
}

/// If the message text starts with a configured alias trigger, rewrite it
/// as a slash command.
fn resolve_alias(mut msg: IncomingMessage, aliases: &[AliasConfig]) -> IncomingMessage {
    let text = msg.text.trim();
    for alias in aliases {
        if text == alias.trigger || text.starts_with(&format!("{} ", alias.trigger)) {
            let rest = text.strip_prefix(&alias.trigger).unwrap_or("").trim();
            msg.text = if rest.is_empty() {
                alias.command.clone()
            } else {
                format!("{} {}", alias.command, rest)
            };
            break;
        }
    }
    msg
}

/// Check if the text contains any banned word (case-insensitive).
fn contains_banned_word(text: &str, banned_words: &[String]) -> bool {
    if banned_words.is_empty() {
        return false;
    }
    let lower = text.to_lowercase();
    banned_words
        .iter()
        .any(|w| lower.contains(&w.to_lowercase()))
}

/// Build a prompt string from the incoming message, including image
/// references if present.
#[allow(dead_code)]
fn build_prompt(msg: &IncomingMessage) -> String {
    let mut prompt = msg.text.clone();
    for img in &msg.images {
        if prompt.is_empty() {
            prompt = format!("The user sent an image: {}", img.filename);
        } else {
            prompt = format!("{}\n\nThe user sent an image: {}", prompt, img.filename);
        }
    }
    if prompt.is_empty() && !msg.images.is_empty() {
        prompt = "The user sent an image.".to_string();
    }
    prompt
}

// ---------------------------------------------------------------------------
// Platform factory (stub -- will be replaced by registry)
// ---------------------------------------------------------------------------

/// Create a platform from config.
///
/// This is a placeholder. The real implementation will use the platform
/// registry to look up the factory function by type name.
fn create_platform_capabilities(
    config: &crate::config::PlatformConfig,
) -> Result<Arc<dyn PlatformCapabilities>> {
    match config.platform_type.as_str() {
        "telegram" => crate::platforms::telegram::create(&config.options),
        "discord" => crate::platforms::discord::create(&config.options),
        "feishu" | "lark" => crate::platforms::feishu::create(&config.options),
        other => anyhow::bail!("Unknown platform type: {other}"),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_allowed_wildcard() {
        assert!(is_allowed("*", "anyone"));
    }

    #[test]
    fn is_allowed_exact() {
        assert!(is_allowed("123,456", "123"));
        assert!(is_allowed("123,456", "456"));
        assert!(!is_allowed("123,456", "789"));
    }

    #[test]
    fn is_allowed_trims() {
        assert!(is_allowed("123, 456", "456"));
    }

    #[test]
    fn banned_word_match() {
        assert!(contains_banned_word(
            "this is secret info",
            &["secret".to_string()]
        ));
    }

    #[test]
    fn banned_word_case_insensitive() {
        assert!(contains_banned_word(
            "TOP SECRET",
            &["secret".to_string()]
        ));
    }

    #[test]
    fn banned_word_no_match() {
        assert!(!contains_banned_word(
            "hello world",
            &["secret".to_string()]
        ));
    }

    #[test]
    fn banned_word_empty_list() {
        assert!(!contains_banned_word("anything", &[]));
    }

    #[test]
    fn build_prompt_text_only() {
        use crate::core::message::IncomingMessage;
        // We need a concrete ReplyCtx for testing -- use a minimal stub.
        let msg = IncomingMessage {
            id: "1".to_string(),
            from: "u".to_string(),
            from_name: None,
            text: "hello".to_string(),
            images: vec![],
            files: vec![],
            voice: None,
            is_group: false,
            channel_id: None,
            channel_name: None,
            reply_ctx: Box::new(StubReplyCtx),
        };
        assert_eq!(build_prompt(&msg), "hello");
    }

    #[test]
    fn build_prompt_with_image() {
        use crate::core::message::{ImageAttachment, IncomingMessage};
        let msg = IncomingMessage {
            id: "1".to_string(),
            from: "u".to_string(),
            from_name: None,
            text: "describe this".to_string(),
            images: vec![ImageAttachment {
                data: vec![],
                mime_type: "image/png".to_string(),
                filename: "photo.png".to_string(),
            }],
            files: vec![],
            voice: None,
            is_group: false,
            channel_id: None,
            channel_name: None,
            reply_ctx: Box::new(StubReplyCtx),
        };
        let prompt = build_prompt(&msg);
        assert!(prompt.contains("describe this"));
        assert!(prompt.contains("photo.png"));
    }

    // Minimal ReplyCtx implementation for tests.
    #[derive(Debug, Clone)]
    struct StubReplyCtx;

    impl crate::core::platform::ReplyCtx for StubReplyCtx {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn session_key_hint(&self) -> String {
            "test".to_string()
        }
        fn clone_box(&self) -> Box<dyn crate::core::platform::ReplyCtx> {
            Box::new(self.clone())
        }
    }

    // ---- /agent command tests (U6) ----

    fn test_project_with_agents() -> ProjectConfig {
        let yaml = r#"
projects:
  - name: test-proj
    work_dir: /tmp
    agents:
      - name: claude
        backend: claude
      - name: kiro
        backend: acp
        acp:
          command: echo
          args: []
    default_agent: claude
    platforms:
      - type: telegram
        options:
          token: "t"
"#;
        serde_yaml::from_str::<crate::config::AppConfig>(yaml)
            .unwrap()
            .projects
            .remove(0)
    }

    async fn fresh_sessions() -> (Arc<SessionManager>, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().unwrap();
        let sm = Arc::new(SessionManager::new(
            tmp.path(),
            std::path::Path::new("/tmp/test-proj"),
        ));
        (sm, tmp)
    }

    #[tokio::test]
    async fn agent_command_lists_available() {
        let config = test_project_with_agents();
        let (sessions, _tmp) = fresh_sessions().await;
        let states = Mutex::new(HashMap::new());
        let active = Mutex::new(HashMap::new());

        let reply = handle_agent_command(
            &config,
            &sessions,
            &states,
            &active,
            "telegram:user1",
            "",
        )
        .await;
        assert!(reply.contains("Current agent: claude"));
        assert!(reply.contains("claude"));
        assert!(reply.contains("kiro"));
        assert!(reply.contains("(echo)"));
    }

    #[tokio::test]
    async fn agent_command_unknown_name_reports() {
        let config = test_project_with_agents();
        let (sessions, _tmp) = fresh_sessions().await;
        let states = Mutex::new(HashMap::new());
        let active = Mutex::new(HashMap::new());

        let reply = handle_agent_command(
            &config,
            &sessions,
            &states,
            &active,
            "telegram:user1",
            "nonexistent",
        )
        .await;
        assert!(reply.contains("Unknown agent"));
    }

    #[tokio::test]
    async fn agent_command_same_agent_reports_already() {
        let config = test_project_with_agents();
        let (sessions, _tmp) = fresh_sessions().await;
        let states = Mutex::new(HashMap::new());
        let active = Mutex::new(HashMap::new());

        let reply = handle_agent_command(
            &config,
            &sessions,
            &states,
            &active,
            "telegram:user1",
            "claude",
        )
        .await;
        assert!(reply.contains("Already"));
    }

    #[tokio::test]
    async fn agent_command_switches_and_creates_new_session() {
        let config = test_project_with_agents();
        let (sessions, _tmp) = fresh_sessions().await;
        let states = Mutex::new(HashMap::new());
        let active = Mutex::new(HashMap::new());

        let reply = handle_agent_command(
            &config,
            &sessions,
            &states,
            &active,
            "telegram:user1",
            "kiro",
        )
        .await;
        assert!(reply.contains("Switched to agent: kiro"));
        assert!(reply.contains("New session started"));

        // active_agents should now reflect the switch.
        let active_map = active.lock().await;
        assert_eq!(active_map.get("telegram:user1").map(|s| s.as_str()), Some("kiro"));
        drop(active_map);

        // A kiro-typed session should exist.
        let kiro_sessions = sessions.list_for_agent("telegram:user1", "kiro");
        assert_eq!(kiro_sessions.len(), 1);
    }

    #[tokio::test]
    async fn agent_command_refuses_switch_when_busy() {
        let config = test_project_with_agents();
        let (sessions, _tmp) = fresh_sessions().await;
        let session = sessions.get_or_create("telegram:user1");
        // Lock the session to simulate an active turn.
        assert!(session.try_lock_explicit());

        let mut map: HashMap<String, EngineInteractiveState> = HashMap::new();
        map.insert(
            "telegram:user1".to_string(),
            EngineInteractiveState::new(Arc::clone(&session)),
        );
        let states = Mutex::new(map);
        let active = Mutex::new(HashMap::new());

        let reply = handle_agent_command(
            &config,
            &sessions,
            &states,
            &active,
            "telegram:user1",
            "kiro",
        )
        .await;
        assert!(reply.contains("is busy"));

        // ActiveAgent should NOT have been written.
        let active_map = active.lock().await;
        assert!(active_map.get("telegram:user1").is_none());
    }

    #[tokio::test]
    async fn resolve_active_agent_falls_back_to_default() {
        let config = test_project_with_agents();
        let active = Mutex::new(HashMap::new());
        let name = resolve_active_agent(&active, "telegram:u", &config).await;
        assert_eq!(name, "claude");
    }

    #[tokio::test]
    async fn resolve_active_agent_uses_override() {
        let config = test_project_with_agents();
        let active = Mutex::new(HashMap::new());
        {
            let mut map = active.lock().await;
            map.insert("telegram:u".to_string(), "kiro".to_string());
        }
        let name = resolve_active_agent(&active, "telegram:u", &config).await;
        assert_eq!(name, "kiro");
    }

    /// Regression: after a restart the engine must rehydrate `active_agents`
    /// from persisted sessions, so users who had previously `/agent kiro`ed
    /// do not silently snap back to the project default.
    #[tokio::test]
    async fn engine_new_hydrates_active_agents_from_sessions() {
        use crate::config::AppConfig;

        let tmp = tempfile::TempDir::new().unwrap();
        let work = tmp.path().join("work");
        std::fs::create_dir_all(&work).unwrap();

        // First boot: create a "kiro" session for alice and a "claude" one for bob.
        {
            let sm = SessionManager::new(tmp.path(), &work);
            sm.new_session_with_agent("discord:alice", None, "kiro");
            sm.new_session_with_agent("discord:bob", None, "claude");
        }

        // Build a ProjectConfig whose default_agent is "claude" (not kiro),
        // so the fallback would *wrongly* send alice to claude if we didn't hydrate.
        let yaml = format!(
            r#"
data_dir: {}
projects:
  - name: test-proj
    work_dir: {}
    agents:
      - name: claude
        backend: claude
      - name: kiro
        backend: acp
        acp:
          command: echo
          args: []
    default_agent: claude
    platforms: []
"#,
            tmp.path().display(),
            work.display(),
        );
        let mut app: AppConfig = serde_yaml::from_str(&yaml).unwrap();
        let project = app.projects.remove(0);

        let engine = Engine::new(project, app);
        let map = engine.active_agents.lock().await;
        assert_eq!(map.get("discord:alice").map(String::as_str), Some("kiro"));
        assert_eq!(map.get("discord:bob").map(String::as_str), Some("claude"));
    }

    /// If a persisted session references an agent that no longer exists in
    /// the current config, we must *not* hydrate it (stale config drift) —
    /// `resolve_active_agent` would otherwise try to spawn a missing backend.
    #[tokio::test]
    async fn engine_new_skips_unknown_agents_when_hydrating() {
        use crate::config::AppConfig;

        let tmp = tempfile::TempDir::new().unwrap();
        let work = tmp.path().join("work");
        std::fs::create_dir_all(&work).unwrap();

        {
            let sm = SessionManager::new(tmp.path(), &work);
            sm.new_session_with_agent("discord:alice", None, "long-gone-agent");
        }

        let yaml = format!(
            r#"
data_dir: {}
projects:
  - name: test-proj
    work_dir: {}
    agents:
      - name: claude
        backend: claude
    default_agent: claude
    platforms: []
"#,
            tmp.path().display(),
            work.display(),
        );
        let mut app: AppConfig = serde_yaml::from_str(&yaml).unwrap();
        let project = app.projects.remove(0);
        let engine = Engine::new(project, app);
        let map = engine.active_agents.lock().await;
        assert!(map.get("discord:alice").is_none());
    }
}
