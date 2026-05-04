//! Discord platform adapter (new core-traits version).
//!
//! Uses the Discord Gateway (WebSocket) for receiving messages and the REST API
//! for sending replies. Supports streaming message edits, slash commands,
//! inline buttons, image uploads, and auto-thread creation.

pub mod types;

use anyhow::{Context, Result};
use async_trait::async_trait;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, Notify};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

use crate::core::message::IncomingMessage;
use crate::core::platform::{
    Button, ImageSender, InlineButtonSender, MessageHandler, MessageUpdater, Platform,
    PlatformCapabilities, PreviewHandle, ReplyCtx, TypingIndicator,
};

use types::{DiscordPreviewHandle, DiscordReplyCtx, InteractionReplyCtx};

const DISCORD_API_BASE: &str = "https://discord.com/api/v10";
/// GUILDS (1) | GUILD_MESSAGES (512) | MESSAGE_CONTENT (32768) = 33281
const GATEWAY_INTENTS: u64 = 33281;
/// Messages older than this are considered duplicates and dropped.
const DEDUP_TTL: Duration = Duration::from_secs(120);

type WsSink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, WsMessage>;
type WsStream = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct DiscordOptions {
    pub token: String,
    pub allow_from: Option<String>,
    /// Optional guild ID for instant slash command registration.
    pub guild_id: Option<String>,
    /// If true, respond to ALL messages in guild (no @mention needed).
    #[serde(default)]
    pub group_reply_all: bool,
    /// If true, each Discord thread gets its own independent session.
    #[serde(default = "default_true")]
    pub thread_isolation: bool,
}

fn default_true() -> bool {
    true
}

/// Per-session RESUME state captured from the READY event.
///
/// When the gateway disconnects, we try RESUME first (preserving Discord's
/// view of our session — the same `session_id`, dedup window, and presence)
/// before falling back to a cold IDENTIFY. This keeps the "idle for hours,
/// come back, keep talking" UX intact across transient network blips.
#[derive(Debug, Clone)]
struct ResumeState {
    session_id: String,
    resume_gateway_url: String,
}

// ---------------------------------------------------------------------------
// DiscordPlatform
// ---------------------------------------------------------------------------

pub struct DiscordPlatform {
    token: String,
    allow_from: Option<HashSet<String>>,
    client: reqwest::Client,
    running: Arc<AtomicBool>,
    /// Our own bot user ID, populated after READY event.
    bot_user_id: Arc<Mutex<Option<String>>>,
    guild_id: Option<String>,
    thread_isolation: bool,
    group_reply_all: bool,
    /// Weak self-reference so the gateway loop can pass `Arc<dyn PlatformCapabilities>`
    /// to the message handler.
    self_ref: std::sync::Mutex<Option<std::sync::Weak<dyn PlatformCapabilities>>>,
    /// Outgoing rate limiter (5 msg/sec per channel for Discord).
    outgoing_limiter: crate::outgoing_ratelimit::OutgoingRateLimiter,
    /// Captured from READY, cleared on INVALID_SESSION. Shared across reconnects
    /// so the next connection attempt can RESUME instead of starting fresh.
    resume_state: Arc<Mutex<Option<ResumeState>>>,
    /// Latest gateway sequence number. -1 means no dispatch event seen yet.
    /// Shared with heartbeat_loop so heartbeats carry the current seq, and
    /// with the RESUME path which needs the last seq Discord saw us ack.
    last_sequence: Arc<AtomicI64>,
}

impl DiscordPlatform {
    pub fn new(opts: DiscordOptions) -> Self {
        let allow_from = opts.allow_from.and_then(|s| {
            if s == "*" {
                None
            } else {
                Some(s.split(',').map(|x| x.trim().to_string()).collect())
            }
        });

        Self {
            token: opts.token,
            allow_from,
            client: reqwest::Client::new(),
            running: Arc::new(AtomicBool::new(false)),
            bot_user_id: Arc::new(Mutex::new(None)),
            guild_id: opts.guild_id,
            thread_isolation: opts.thread_isolation,
            group_reply_all: opts.group_reply_all,
            self_ref: std::sync::Mutex::new(None),
            outgoing_limiter: crate::outgoing_ratelimit::OutgoingRateLimiter::new(),
            resume_state: Arc::new(Mutex::new(None)),
            last_sequence: Arc::new(AtomicI64::new(-1)),
        }
    }

    /// Inject the weak self-reference after wrapping in Arc.
    /// Called by the factory function.
    pub fn set_self_ref(&self, weak: std::sync::Weak<dyn PlatformCapabilities>) {
        *self.self_ref.lock().unwrap() = Some(weak);
    }

    // -----------------------------------------------------------------------
    // REST helpers
    // -----------------------------------------------------------------------

    /// Call the Discord REST API and return the JSON response.
    async fn api_request(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<&serde_json::Value>,
    ) -> Result<serde_json::Value> {
        api_request_with(&self.client, &self.token, method, path, body).await
    }

    /// Resolve the effective channel id from a `ReplyCtx` trait object.
    /// Supports both `DiscordReplyCtx` and `InteractionReplyCtx`.
    fn resolve_channel_id(ctx: &dyn ReplyCtx) -> Result<String> {
        if let Some(dc) = ctx.as_any().downcast_ref::<DiscordReplyCtx>() {
            // Prefer thread_id (replies go to the thread, not the parent channel).
            Ok(dc.thread_id.as_ref().unwrap_or(&dc.channel_id).clone())
        } else if let Some(ic) = ctx.as_any().downcast_ref::<InteractionReplyCtx>() {
            Ok(ic.channel_id.clone())
        } else {
            anyhow::bail!("discord: unsupported ReplyCtx type");
        }
    }
}

// ---------------------------------------------------------------------------
// Shared REST helper (usable outside &self methods)
// ---------------------------------------------------------------------------

async fn api_request_with(
    client: &reqwest::Client,
    token: &str,
    method: reqwest::Method,
    path: &str,
    body: Option<&serde_json::Value>,
) -> Result<serde_json::Value> {
    let url = format!("{}{}", DISCORD_API_BASE, path);
    let mut req = client
        .request(method, &url)
        .header("Authorization", format!("Bot {}", token));

    if let Some(json) = body {
        req = req.json(json);
    }

    let resp = req.send().await.context("Discord API request failed")?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();

    if !status.is_success() {
        anyhow::bail!("Discord API error {}: {}", status, text);
    }

    if text.is_empty() {
        Ok(serde_json::Value::Null)
    } else {
        serde_json::from_str(&text).context("Failed to parse Discord API response")
    }
}

/// Cap backoff at 30s — the QQBot pattern — so we don't go silent for
/// minutes after a long outage.
const MAX_BACKOFF_SECS: u64 = 30;

fn next_backoff(current: u64) -> u64 {
    (current * 2).min(MAX_BACKOFF_SECS)
}

/// Sleep for `secs` seconds ± up to 20% jitter, so many bots reconnecting
/// after a shared Discord blip don't align on the same tick. No extra
/// dependency: seeds jitter from the current monotonic nanos.
async fn sleep_with_jitter(secs: u64) {
    let base_ms = secs.saturating_mul(1000);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    let jitter_range = base_ms / 5; // ±20%
    let offset = if jitter_range > 0 {
        (nanos % (jitter_range * 2)) as i64 - jitter_range as i64
    } else {
        0
    };
    let total_ms = (base_ms as i64 + offset).max(0) as u64;
    tokio::time::sleep(Duration::from_millis(total_ms)).await;
}

/// Get the Gateway WebSocket URL from Discord.
async fn get_gateway_url(client: &reqwest::Client, token: &str) -> Result<String> {
    let url = format!("{}/gateway/bot", DISCORD_API_BASE);
    let resp = client
        .get(&url)
        .header("Authorization", format!("Bot {}", token))
        .send()
        .await
        .context("Failed to get Discord gateway URL")?;
    let body: serde_json::Value = resp.json().await?;
    let ws_url = body["url"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("No gateway URL in response"))?;
    Ok(format!("{}/?v=10&encoding=json", ws_url))
}

// ---------------------------------------------------------------------------
// Platform trait
// ---------------------------------------------------------------------------

#[async_trait]
impl Platform for DiscordPlatform {
    fn name(&self) -> &str {
        "discord"
    }

    async fn start(&self, handler: MessageHandler) -> Result<()> {
        self.running.store(true, Ordering::Relaxed);

        let running = self.running.clone();
        let token = self.token.clone();
        let allow_from = self.allow_from.clone();
        let bot_user_id = self.bot_user_id.clone();
        let guild_id = self.guild_id.clone();
        let client = self.client.clone();
        let thread_isolation = self.thread_isolation;
        let group_reply_all = self.group_reply_all;
        let resume_state = self.resume_state.clone();
        let last_sequence = self.last_sequence.clone();

        // Upgrade the weak self-reference so the gateway loop can pass
        // Arc<dyn PlatformCapabilities> into every handler call.
        let self_weak = self
            .self_ref
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("discord: self_ref not set; call set_self_ref first"))?;

        // Spawn reconnection loop
        tokio::spawn(async move {
            let mut backoff_secs = 1u64;

            while running.load(Ordering::Relaxed) {
                // Prefer resume_gateway_url when we have prior session state.
                // Falls back to GET /gateway/bot on cold start or after an
                // INVALID_SESSION cleared the resume state.
                let (gateway_url, is_resume) = {
                    let rs = resume_state.lock().await;
                    match rs.as_ref() {
                        Some(state) => (
                            format!("{}/?v=10&encoding=json", state.resume_gateway_url),
                            true,
                        ),
                        None => match get_gateway_url(&client, &token).await {
                            Ok(url) => (url, false),
                            Err(e) => {
                                tracing::error!(error = %e, backoff = backoff_secs, "discord: failed to get gateway URL, retrying");
                                sleep_with_jitter(backoff_secs).await;
                                backoff_secs = next_backoff(backoff_secs);
                                continue;
                            }
                        },
                    }
                };

                tracing::info!(is_resume, "discord: connecting to gateway {}", gateway_url);

                let ws_result = connect_async(&gateway_url).await;
                let (ws_stream, _) = match ws_result {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::error!(error = %e, backoff = backoff_secs, "discord: gateway connect failed, retrying");
                        sleep_with_jitter(backoff_secs).await;
                        backoff_secs = next_backoff(backoff_secs);
                        continue;
                    }
                };

                tracing::info!("discord: gateway connected");

                let (ws_sink, ws_read) = ws_stream.split();
                let ws_sink = Arc::new(Mutex::new(ws_sink));

                let reached_ready = gateway_event_loop(
                    ws_read,
                    ws_sink,
                    running.clone(),
                    token.clone(),
                    allow_from.clone(),
                    bot_user_id.clone(),
                    guild_id.clone(),
                    thread_isolation,
                    group_reply_all,
                    handler.clone(),
                    client.clone(),
                    self_weak.clone(),
                    resume_state.clone(),
                    last_sequence.clone(),
                )
                .await;

                // Only reset backoff when this connection actually lived —
                // reached READY (fresh session) or RESUMED (resumed session).
                // If we never got past Hello, keep backing off so a Discord
                // outage doesn't turn into a tight reconnect loop.
                if reached_ready {
                    backoff_secs = 1;
                }

                if running.load(Ordering::Relaxed) {
                    tracing::warn!(backoff = backoff_secs, "discord: gateway disconnected, reconnecting");
                    sleep_with_jitter(backoff_secs).await;
                    backoff_secs = next_backoff(backoff_secs);
                }
            }
        });

        tracing::info!("discord: started with auto-reconnect");
        Ok(())
    }

    async fn register_commands(&self, commands: &[crate::core::platform::BotCommand]) -> Result<()> {
        // Wait for bot_user_id to be populated (READY event)
        let app_id = {
            let mut retries = 0;
            loop {
                let uid = self.bot_user_id.lock().await;
                if let Some(ref id) = *uid {
                    break id.clone();
                }
                drop(uid);
                retries += 1;
                if retries > 30 {
                    anyhow::bail!("discord: timed out waiting for READY to register commands");
                }
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        };

        // Build slash command JSON from the command list
        let slash_cmds: Vec<serde_json::Value> = commands
            .iter()
            .filter_map(|c| {
                // Discord requires: lowercase, only letters/numbers/hyphens, max 32 chars
                let name: String = c.name.to_lowercase()
                    .replace([':', '_'], "-")
                    .chars()
                    .filter(|ch| ch.is_alphanumeric() || *ch == '-')
                    .take(32)
                    .collect();
                let desc: String = c.description.chars().take(100).collect();
                if name.is_empty() {
                    return None; // skip invalid names
                }
                Some(serde_json::json!({
                    "name": name,
                    "description": if desc.is_empty() { "Command".to_string() } else { desc },
                    "options": [{
                        "name": "args",
                        "type": 3,
                        "description": "optional arguments",
                        "required": false
                    }]
                }))
            })
            .collect();

        // Log command names for debugging
        for c in &slash_cmds {
            tracing::debug!(name = %c["name"], "discord: registering command");
        }

        let guild_id = self.guild_id.clone();
        let url = if let Some(ref gid) = guild_id {
            format!("{}/applications/{}/guilds/{}/commands", DISCORD_API_BASE, app_id, gid)
        } else {
            format!("{}/applications/{}/commands", DISCORD_API_BASE, app_id)
        };

        let resp = self.client.put(&url)
            .header("Authorization", format!("Bot {}", self.token))
            .json(&slash_cmds)
            .send()
            .await?;

        if resp.status().is_success() {
            let scope = if guild_id.is_some() { "guild (instant)" } else { "global (up to 1h)" };
            tracing::info!(count = slash_cmds.len(), scope = scope, "discord: commands registered");
        } else {
            let text = resp.text().await.unwrap_or_default();
            tracing::warn!(response = %text, "discord: command registration failed");
        }
        Ok(())
    }

    async fn reply(&self, ctx: &dyn ReplyCtx, content: &str) -> Result<()> {
        // Interaction context: edit the original deferred response or send follow-up.
        if let Some(ic) = ctx.as_any().downcast_ref::<InteractionReplyCtx>() {
            let app_id = self.bot_user_id.lock().await.clone().unwrap_or_default();
            if ic.claim_first_response() {
                // Edit the original deferred response.
                let path = format!(
                    "/webhooks/{}/{}/messages/@original",
                    app_id, ic.interaction_token
                );
                let body = serde_json::json!({ "content": content });
                self.api_request(reqwest::Method::PATCH, &path, Some(&body))
                    .await?;
            } else {
                // Follow-up message via webhook.
                let path = format!("/webhooks/{}/{}", app_id, ic.interaction_token);
                let body = serde_json::json!({ "content": content });
                self.api_request(reqwest::Method::POST, &path, Some(&body))
                    .await?;
            }
            return Ok(());
        }

        // Regular channel/thread context — chunk if > 2000 chars.
        let channel_id = Self::resolve_channel_id(ctx)?;
        let chunks = chunk_message(content, 2000);
        for chunk in chunks {
            self.outgoing_limiter.wait("discord", &channel_id).await;
            let body = serde_json::json!({ "content": chunk });
            self.api_request(
                reqwest::Method::POST,
                &format!("/channels/{}/messages", channel_id),
                Some(&body),
            )
            .await?;
        }
        Ok(())
    }

    async fn send(&self, ctx: &dyn ReplyCtx, content: &str) -> Result<()> {
        self.reply(ctx, content).await
    }

    async fn reply_quoted(&self, ctx: &dyn ReplyCtx, content: &str) -> Result<()> {
        if ctx.as_any().downcast_ref::<InteractionReplyCtx>().is_some() {
            return self.reply(ctx, content).await;
        }

        let channel_id = Self::resolve_channel_id(ctx)?;
        self.outgoing_limiter.wait("discord", &channel_id).await;

        // Try with message_reference first
        if let Some(dc) = ctx.as_any().downcast_ref::<DiscordReplyCtx>() {
            if let Some(ref mid) = dc.message_id {
                let body = serde_json::json!({
                    "content": content,
                    "message_reference": { "message_id": mid },
                    "allowed_mentions": { "replied_user": false }
                });
                let result = self
                    .api_request(
                        reqwest::Method::POST,
                        &format!("/channels/{}/messages", channel_id),
                        Some(&body),
                    )
                    .await;

                match result {
                    Ok(_) => return Ok(()),
                    Err(e) => {
                        // Fallback to plain reply if message_reference fails
                        // (e.g. system messages, deleted messages)
                        tracing::debug!(error = %e, "reply_quoted fallback to plain reply");
                    }
                }
            }
        }

        // Fallback: send without message_reference
        let body = serde_json::json!({ "content": content });
        self.api_request(
            reqwest::Method::POST,
            &format!("/channels/{}/messages", channel_id),
            Some(&body),
        )
        .await?;
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        self.running.store(false, Ordering::Relaxed);
        tracing::info!("discord: stopped");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// PlatformCapabilities
// ---------------------------------------------------------------------------

impl PlatformCapabilities for DiscordPlatform {
    fn as_message_updater(&self) -> Option<&dyn MessageUpdater> {
        Some(self)
    }

    fn as_image_sender(&self) -> Option<&dyn ImageSender> {
        Some(self)
    }

    fn as_inline_button_sender(&self) -> Option<&dyn InlineButtonSender> {
        Some(self)
    }

    fn as_typing_indicator(&self) -> Option<&dyn TypingIndicator> {
        Some(self)
    }
}

// ---------------------------------------------------------------------------
// MessageUpdater -- streaming preview (send / edit / delete)
// ---------------------------------------------------------------------------

#[async_trait]
impl MessageUpdater for DiscordPlatform {
    async fn send_preview(
        &self,
        ctx: &dyn ReplyCtx,
        text: &str,
    ) -> Result<Box<dyn PreviewHandle>> {
        let channel_id = Self::resolve_channel_id(ctx)?;
        self.outgoing_limiter.wait("discord", &channel_id).await;
        let body = serde_json::json!({ "content": text });
        let resp = self
            .api_request(
                reqwest::Method::POST,
                &format!("/channels/{}/messages", channel_id),
                Some(&body),
            )
            .await?;

        let message_id = resp["id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("discord: no message id in send_preview response"))?
            .to_string();

        Ok(Box::new(DiscordPreviewHandle {
            channel_id,
            message_id,
        }))
    }

    async fn update_preview(&self, handle: &dyn PreviewHandle, text: &str) -> Result<()> {
        let h = handle
            .as_any()
            .downcast_ref::<DiscordPreviewHandle>()
            .ok_or_else(|| anyhow::anyhow!("discord: invalid preview handle type"))?;

        self.outgoing_limiter.wait("discord", &h.channel_id).await;
        let body = serde_json::json!({ "content": text });
        let _ = self
            .api_request(
                reqwest::Method::PATCH,
                &format!("/channels/{}/messages/{}", h.channel_id, h.message_id),
                Some(&body),
            )
            .await;
        Ok(())
    }

    async fn delete_preview(&self, handle: &dyn PreviewHandle) -> Result<()> {
        let h = handle
            .as_any()
            .downcast_ref::<DiscordPreviewHandle>()
            .ok_or_else(|| anyhow::anyhow!("discord: invalid preview handle type"))?;

        let _ = self
            .api_request(
                reqwest::Method::DELETE,
                &format!("/channels/{}/messages/{}", h.channel_id, h.message_id),
                None,
            )
            .await;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ImageSender -- send image as multipart file upload
// ---------------------------------------------------------------------------

#[async_trait]
impl ImageSender for DiscordPlatform {
    async fn send_image(
        &self,
        ctx: &dyn ReplyCtx,
        data: &[u8],
        filename: &str,
        mime: &str,
    ) -> Result<()> {
        let channel_id = Self::resolve_channel_id(ctx)?;
        let url = format!(
            "{}/channels/{}/messages",
            DISCORD_API_BASE, channel_id
        );

        let part = reqwest::multipart::Part::bytes(data.to_vec())
            .file_name(filename.to_string())
            .mime_str(mime)?;

        let form = reqwest::multipart::Form::new().part("files[0]", part);

        let resp = self
            .client
            .post(&url)
            .header("Authorization", format!("Bot {}", self.token))
            .multipart(form)
            .send()
            .await
            .context("discord: image upload failed")?;

        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("discord: image upload error: {}", text);
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// InlineButtonSender -- messages with action-row button components
// ---------------------------------------------------------------------------

#[async_trait]
impl InlineButtonSender for DiscordPlatform {
    async fn send_with_buttons(
        &self,
        ctx: &dyn ReplyCtx,
        text: &str,
        buttons: &[Button],
    ) -> Result<Box<dyn PreviewHandle>> {
        let channel_id = Self::resolve_channel_id(ctx)?;
        self.outgoing_limiter.wait("discord", &channel_id).await;

        // Build Discord button components. Style 1 = Primary (blurple).
        let btn_components: Vec<serde_json::Value> = buttons
            .iter()
            .enumerate()
            .map(|(i, b)| {
                serde_json::json!({
                    "type": 2,           // Button
                    "style": 1,          // Primary
                    "label": b.text,
                    "custom_id": if b.callback_data.is_empty() {
                        format!("btn_{}", i)
                    } else {
                        b.callback_data.clone()
                    },
                })
            })
            .collect();

        let body = serde_json::json!({
            "content": text,
            "components": [{
                "type": 1,  // Action Row
                "components": btn_components,
            }],
        });

        let resp = self
            .api_request(
                reqwest::Method::POST,
                &format!("/channels/{}/messages", channel_id),
                Some(&body),
            )
            .await?;

        let message_id = resp["id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("discord: no message id in button response"))?
            .to_string();

        Ok(Box::new(DiscordPreviewHandle {
            channel_id,
            message_id,
        }))
    }

    async fn answer_callback(&self, callback_id: &str, text: &str) -> Result<()> {
        // Discord interaction callbacks are answered via the interaction endpoint.
        // callback_id here is expected to be "interaction_id:interaction_token".
        let parts: Vec<&str> = callback_id.splitn(2, ':').collect();
        if parts.len() != 2 {
            anyhow::bail!("discord: invalid callback_id format, expected 'id:token'");
        }
        let (interaction_id, interaction_token) = (parts[0], parts[1]);

        let body = serde_json::json!({
            "type": 4,  // CHANNEL_MESSAGE_WITH_SOURCE
            "data": { "content": text, "flags": 64 },  // flags 64 = ephemeral
        });

        let path = format!(
            "/interactions/{}/{}/callback",
            interaction_id, interaction_token
        );
        self.api_request(reqwest::Method::POST, &path, Some(&body))
            .await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// TypingIndicator -- POST /channels/{id}/typing in a loop
// ---------------------------------------------------------------------------

#[async_trait]
impl TypingIndicator for DiscordPlatform {
    async fn start_typing(&self, ctx: &dyn ReplyCtx) -> Result<Box<dyn FnOnce() + Send>> {
        let channel_id = Self::resolve_channel_id(ctx)?;
        let token = self.token.clone();
        let client = self.client.clone();
        let active = Arc::new(AtomicBool::new(true));
        let active_clone = active.clone();

        // Discord typing indicator lasts ~10 seconds; we re-send every 8 seconds.
        tokio::spawn(async move {
            while active_clone.load(Ordering::Relaxed) {
                let url = format!(
                    "{}/channels/{}/typing",
                    DISCORD_API_BASE, channel_id
                );
                let _ = client
                    .post(&url)
                    .header("Authorization", format!("Bot {}", token))
                    .send()
                    .await;

                tokio::time::sleep(Duration::from_secs(8)).await;
            }
        });

        Ok(Box::new(move || {
            active.store(false, Ordering::Relaxed);
        }))
    }
}

// ---------------------------------------------------------------------------
// Factory function
// ---------------------------------------------------------------------------

/// Create a new Discord platform from a JSON config value.
pub fn create(config: &serde_json::Value) -> Result<Arc<dyn PlatformCapabilities>> {
    let opts: DiscordOptions = serde_json::from_value(config.clone())?;
    let arc: Arc<DiscordPlatform> = Arc::new(DiscordPlatform::new(opts));
    // Inject weak self-reference so the gateway loop can pass Arc to the handler.
    let weak: std::sync::Weak<dyn PlatformCapabilities> =
        Arc::downgrade(&(arc.clone() as Arc<dyn PlatformCapabilities>));
    arc.set_self_ref(weak);
    Ok(arc)
}

// ---------------------------------------------------------------------------
// Gateway event loop
// ---------------------------------------------------------------------------

/// Main Gateway event loop. Runs until the connection drops or `running` is cleared.
///
/// Returns `true` if this connection reached READY before dropping. The outer
/// reconnect loop uses that signal to distinguish "clean disconnect of a live
/// session" (safe to reset backoff) from "never fully connected" (keep
/// backoff climbing).
#[allow(clippy::too_many_arguments)]
async fn gateway_event_loop(
    mut ws_read: WsStream,
    ws_sink: Arc<Mutex<WsSink>>,
    running: Arc<AtomicBool>,
    token: String,
    allow_from: Option<HashSet<String>>,
    bot_user_id: Arc<Mutex<Option<String>>>,
    guild_id: Option<String>,
    thread_isolation: bool,
    group_reply_all: bool,
    handler: MessageHandler,
    client: reqwest::Client,
    self_weak: std::sync::Weak<dyn PlatformCapabilities>,
    resume_state: Arc<Mutex<Option<ResumeState>>>,
    last_sequence: Arc<AtomicI64>,
) -> bool {
    let mut reached_ready = false;
    let mut heartbeat_handle: Option<tokio::task::JoinHandle<()>> = None;
    let seen_ids: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    let seen_timestamps: Arc<Mutex<Vec<(String, Instant)>>> =
        Arc::new(Mutex::new(Vec::new()));

    // Heartbeat ACK watchdog. The heartbeat task sets `ack_pending` before
    // sending; the dispatch loop clears it on opcode 11. If the heartbeat
    // task finds the flag still set next tick, Discord has gone silent on
    // our keepalives and we bail out of this loop so the outer reconnect
    // can RESUME. `dead_notify` lets the heartbeat task pop us out of the
    // select immediately instead of waiting 120s.
    let ack_pending = Arc::new(AtomicBool::new(false));
    let dead_notify = Arc::new(Notify::new());

    // Liveness accounting: without a periodic heartbeat log, a healthy idle
    // connection is indistinguishable from a wedged one — both produce zero
    // logs. Emit one line per LIVENESS_LOG_INTERVAL so long-idle sessions
    // still show evidence of liveness.
    let loop_started = Instant::now();
    let mut last_event_at = loop_started;
    let mut last_liveness_log = loop_started;
    let mut ack_count_since_log: u64 = 0;
    const LIVENESS_LOG_INTERVAL: Duration = Duration::from_secs(300);

    while running.load(Ordering::Relaxed) {
        let msg = tokio::select! {
            msg = ws_read.next() => msg,
            _ = dead_notify.notified() => {
                tracing::warn!("discord: heartbeat watchdog fired, closing gateway");
                break;
            }
            _ = tokio::time::sleep(Duration::from_secs(120)) => {
                tracing::warn!("discord: gateway read timeout");
                break;
            }
        };

        let msg = match msg {
            Some(Ok(m)) => m,
            Some(Err(e)) => {
                tracing::error!(error = %e, "discord: gateway error");
                break;
            }
            None => {
                tracing::warn!("discord: gateway stream ended");
                break;
            }
        };

        let text = match msg {
            WsMessage::Text(t) => t,
            WsMessage::Close(_) => {
                tracing::info!("discord: gateway closed by server");
                break;
            }
            _ => continue,
        };

        let payload: serde_json::Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "discord: failed to parse gateway payload");
                continue;
            }
        };

        // Update sequence number (shared with heartbeat task + outer reconnect
        // for the RESUME payload).
        if let Some(s) = payload["s"].as_u64() {
            last_sequence.store(s as i64, Ordering::Release);
        }

        let opcode = payload["op"].as_u64().unwrap_or(0);

        match opcode {
            // Dispatch (opcode 0)
            0 => {
                last_event_at = Instant::now();
                let event_name = payload["t"].as_str().unwrap_or("");
                let data = &payload["d"];

                tracing::info!(event = %event_name, "discord: gateway dispatch event");

                match event_name {
                    "READY" => {
                        reached_ready = true;
                        if let Some(user_id) = data["user"]["id"].as_str() {
                            let mut uid = bot_user_id.lock().await;
                            *uid = Some(user_id.to_string());
                            tracing::info!(bot_user_id = %user_id, "discord: READY");

                            // Slash commands are now registered by the engine
                            // via register_commands() after skills are discovered.
                        }
                        // Capture RESUME state. Discord returns resume_gateway_url
                        // pointing at the specific edge that holds our session;
                        // using it (instead of the generic /gateway/bot URL) is
                        // what lets RESUME actually succeed.
                        if let (Some(sid), Some(rurl)) = (
                            data["session_id"].as_str(),
                            data["resume_gateway_url"].as_str(),
                        ) {
                            let mut rs = resume_state.lock().await;
                            *rs = Some(ResumeState {
                                session_id: sid.to_string(),
                                resume_gateway_url: rurl.to_string(),
                            });
                            tracing::info!("discord: RESUME state captured");
                        }
                    }
                    "RESUMED" => {
                        reached_ready = true;
                        tracing::info!("discord: session resumed successfully");
                    }
                    "MESSAGE_CREATE" => {
                        // Dedup: skip messages we have already seen.
                        if let Some(msg_id) = data["id"].as_str() {
                            let now = Instant::now();
                            {
                                let mut seen = seen_ids.lock().await;
                                if seen.contains(msg_id) {
                                    tracing::debug!(msg_id, "discord: dedup hit, skipping");
                                    continue;
                                }
                                seen.insert(msg_id.to_string());
                            }
                            {
                                let mut ts = seen_timestamps.lock().await;
                                ts.push((msg_id.to_string(), now));
                                // Evict entries older than DEDUP_TTL.
                                let cutoff = now - DEDUP_TTL;
                                let mut evicted = Vec::new();
                                ts.retain(|(id, t)| {
                                    if *t < cutoff {
                                        evicted.push(id.clone());
                                        false
                                    } else {
                                        true
                                    }
                                });
                                if !evicted.is_empty() {
                                    let mut seen = seen_ids.lock().await;
                                    for id in &evicted {
                                        seen.remove(id);
                                    }
                                }
                            }
                        }

                        let bot_uid = bot_user_id.lock().await.clone();
                        if let Some(incoming) = parse_message_create(
                            data,
                            &allow_from,
                            bot_uid.as_deref(),
                            guild_id.as_deref(),
                            group_reply_all,
                        ) {
                            // Auto-thread creation for guild messages.
                            let incoming = if thread_isolation {
                                maybe_create_thread(&client, &token, data, incoming).await
                            } else {
                                incoming
                            };
                            if let Some(platform_arc) = self_weak.upgrade() {
                                tracing::debug!(
                                    msg_id = incoming.id,
                                    from = %incoming.from,
                                    "discord: dispatching MESSAGE_CREATE"
                                );
                                handler(platform_arc, incoming);
                            } else {
                                tracing::warn!("discord: platform dropped, cannot dispatch message");
                            }
                        }
                    }
                    "INTERACTION_CREATE" => {
                        if let Some(incoming) =
                            parse_interaction(data, &client, &token).await
                        {
                            if let Some(platform_arc) = self_weak.upgrade() {
                                tracing::debug!(
                                    interaction_id = incoming.id,
                                    "discord: dispatching INTERACTION_CREATE"
                                );
                                handler(platform_arc, incoming);
                            } else {
                                tracing::warn!("discord: platform dropped, cannot dispatch interaction");
                            }
                        }
                    }
                    _ => {}
                }
            }
            // Hello (opcode 10) -- send RESUME if we have state, else IDENTIFY,
            // then start the heartbeat task.
            10 => {
                let heartbeat_interval =
                    payload["d"]["heartbeat_interval"].as_u64().unwrap_or(41250);

                tracing::info!(interval_ms = heartbeat_interval, "discord: received Hello");

                let resume_payload = {
                    let rs = resume_state.lock().await;
                    rs.as_ref().map(|state| {
                        let seq = last_sequence.load(Ordering::Acquire);
                        serde_json::json!({
                            "op": 6,
                            "d": {
                                "token": token,
                                "session_id": state.session_id,
                                "seq": seq,
                            }
                        })
                    })
                };

                let outgoing = match resume_payload {
                    Some(p) => {
                        tracing::info!("discord: sending RESUME");
                        p
                    }
                    None => {
                        tracing::info!("discord: sending IDENTIFY");
                        serde_json::json!({
                            "op": 2,
                            "d": {
                                "token": token,
                                "intents": GATEWAY_INTENTS,
                                "properties": {
                                    "os": "linux",
                                    "browser": "agentbridge",
                                    "device": "agentbridge"
                                }
                            }
                        })
                    }
                };

                {
                    let mut sink = ws_sink.lock().await;
                    if let Err(e) = sink.send(WsMessage::Text(outgoing.to_string())).await {
                        tracing::error!(error = %e, "discord: failed to send IDENTIFY/RESUME");
                        break;
                    }
                }

                // Spawn heartbeat task, handing it the shared ACK watchdog
                // + the live sequence atomic. The old design passed a static
                // snapshot of `sequence` — under RESUME that means Discord
                // would replay from seq=None, losing any missed events.
                if let Some(h) = heartbeat_handle.take() {
                    h.abort();
                }
                heartbeat_handle = Some(tokio::spawn(heartbeat_loop(
                    ws_sink.clone(),
                    running.clone(),
                    heartbeat_interval,
                    last_sequence.clone(),
                    ack_pending.clone(),
                    dead_notify.clone(),
                )));
            }
            // Heartbeat ACK (opcode 11) — clear the watchdog flag AND, if the
            // idle-liveness window elapsed, log one line so long quiet periods
            // produce evidence of liveness.
            11 => {
                ack_pending.store(false, Ordering::Release);
                ack_count_since_log += 1;
                let now = Instant::now();
                if now.duration_since(last_liveness_log) >= LIVENESS_LOG_INTERVAL {
                    let idle_secs = now.duration_since(last_event_at).as_secs();
                    tracing::info!(
                        acks = ack_count_since_log,
                        idle_secs,
                        "discord: gateway alive (no dispatch events)"
                    );
                    last_liveness_log = now;
                    ack_count_since_log = 0;
                }
            }
            // Reconnect (opcode 7): keep RESUME state so we come back to the
            // same session. Invalid Session (opcode 9): Discord has dropped
            // our session — must clear state so the reconnect loop falls
            // back to a fresh IDENTIFY.
            7 => {
                tracing::warn!("discord: server requested reconnect (op 7)");
                break;
            }
            9 => {
                let resumable = payload["d"].as_bool().unwrap_or(false);
                tracing::warn!(resumable, "discord: INVALID_SESSION");
                if !resumable {
                    let mut rs = resume_state.lock().await;
                    *rs = None;
                    last_sequence.store(-1, Ordering::Release);
                }
                break;
            }
            // Heartbeat request (opcode 1) — ad-hoc heartbeat on server demand.
            1 => {
                let seq = last_sequence.load(Ordering::Acquire);
                let d = if seq >= 0 {
                    serde_json::Value::from(seq)
                } else {
                    serde_json::Value::Null
                };
                let heartbeat = serde_json::json!({"op": 1, "d": d});
                let mut sink = ws_sink.lock().await;
                let _ = sink.send(WsMessage::Text(heartbeat.to_string())).await;
            }
            _ => {}
        }
    }

    // Cleanup
    if let Some(h) = heartbeat_handle.take() {
        h.abort();
    }
    tracing::info!(reached_ready, "discord: gateway event loop ended");
    reached_ready
}

// ---------------------------------------------------------------------------
// Heartbeat loop
// ---------------------------------------------------------------------------

async fn heartbeat_loop(
    ws_sink: Arc<Mutex<WsSink>>,
    running: Arc<AtomicBool>,
    interval_ms: u64,
    last_sequence: Arc<AtomicI64>,
    ack_pending: Arc<AtomicBool>,
    dead_notify: Arc<Notify>,
) {
    let interval = Duration::from_millis(interval_ms);

    // Jitter the first heartbeat per Discord's recommendation.
    let jitter = Duration::from_millis(interval_ms / 2);
    tokio::time::sleep(jitter).await;

    loop {
        if !running.load(Ordering::Relaxed) {
            break;
        }

        // ACK watchdog (QQBot pattern): if the previous heartbeat's ACK
        // never cleared this flag, the gateway has gone silent on us even
        // though the TCP socket is still up. Bail out so the outer loop
        // can reconnect — RESUME preserves session state.
        if ack_pending.load(Ordering::Acquire) {
            tracing::warn!("discord: heartbeat ACK missed, declaring connection dead");
            dead_notify.notify_one();
            break;
        }

        let seq = last_sequence.load(Ordering::Acquire);
        let d = if seq >= 0 {
            serde_json::Value::from(seq)
        } else {
            serde_json::Value::Null
        };
        let heartbeat = serde_json::json!({"op": 1, "d": d});

        ack_pending.store(true, Ordering::Release);
        {
            let mut sink = ws_sink.lock().await;
            if let Err(e) = sink.send(WsMessage::Text(heartbeat.to_string())).await {
                tracing::error!(error = %e, "discord: heartbeat send failed");
                dead_notify.notify_one();
                break;
            }
        }

        tokio::time::sleep(interval).await;
    }
}

// ---------------------------------------------------------------------------
// Slash command registration (BulkOverwrite)
// ---------------------------------------------------------------------------

// Hardcoded register_slash_commands removed — now dynamic via
// Platform::register_commands() called from Engine::start().

// ---------------------------------------------------------------------------
// Message / interaction parsing
// ---------------------------------------------------------------------------

/// Parse a Discord MESSAGE_CREATE event into an IncomingMessage.
fn parse_message_create(
    data: &serde_json::Value,
    allow_from: &Option<HashSet<String>>,
    bot_user_id: Option<&str>,
    config_guild_id: Option<&str>,
    group_reply_all: bool,
) -> Option<IncomingMessage> {
    // Ignore bot messages (including our own).
    if data["author"]["bot"].as_bool().unwrap_or(false) {
        return None;
    }

    let author_id = data["author"]["id"].as_str()?;
    let message_id = data["id"].as_str()?.to_string();

    // Access control.
    if let Some(ref allowed) = allow_from {
        if !allowed.contains(author_id) {
            tracing::warn!(
                msg_id = %message_id,
                from = author_id,
                "discord: ACL rejected message"
            );
            return None;
        }
    }

    let content = data["content"].as_str().unwrap_or("").to_string();
    let channel_id = data["channel_id"].as_str()?.to_string();
    let guild_id = data["guild_id"].as_str();

    let is_dm = guild_id.is_none();

    // For guild messages: respond if group_reply_all, bot mentioned, or guild_id matches.
    if !is_dm && !group_reply_all {
        let mut is_relevant = false;

        if let Some(mentions) = data["mentions"].as_array() {
            if let Some(bot_id) = bot_user_id {
                is_relevant = mentions
                    .iter()
                    .any(|m| m["id"].as_str() == Some(bot_id));
            }
        }

        if let (Some(gid), Some(cfg_gid)) = (guild_id, config_guild_id) {
            if gid == cfg_gid {
                is_relevant = true;
            }
        }

        if !is_relevant {
            return None;
        }
    }

    if content.is_empty() {
        tracing::debug!(msg_id = %message_id, "discord: empty content, skipping");
        return None;
    }

    // Strip bot mention from content.
    let clean_content = if let Some(bot_id) = bot_user_id {
        content
            .replace(&format!("<@{}>", bot_id), "")
            .replace(&format!("<@!{}>", bot_id), "")
            .trim()
            .to_string()
    } else {
        content
    };

    if clean_content.is_empty() {
        tracing::debug!(msg_id = %message_id, "discord: content empty after mention strip, skipping");
        return None;
    }

    let author_name = data["author"]["username"].as_str().map(|s| s.to_string());

    let channel_name = data.get("thread")
        .and_then(|t| t["name"].as_str())
        .map(|s| s.to_string());

    Some(IncomingMessage {
        id: message_id.clone(),
        from: author_id.to_string(),
        from_name: author_name,
        text: clean_content,
        images: vec![],
        files: vec![],
        voice: None,
        is_group: !is_dm,
        channel_id: Some(channel_id.clone()),
        channel_name,
        reply_ctx: Box::new(DiscordReplyCtx {
            channel_id,
            message_id: Some(message_id),
            thread_id: None,
        }),
    })
}

/// Parse a Discord INTERACTION_CREATE event (slash command) into an IncomingMessage.
/// Also sends the deferred acknowledgment (type 5).
async fn parse_interaction(
    data: &serde_json::Value,
    client: &reqwest::Client,
    token: &str,
) -> Option<IncomingMessage> {
    // Handle application command (type 2) and message component (type 3 — button clicks).
    let interaction_type = data["type"].as_u64()?;
    if interaction_type != 2 && interaction_type != 3 {
        return None;
    }

    let interaction_id = data["id"].as_str()?;
    let interaction_token = data["token"].as_str()?;
    let channel_id = data["channel_id"].as_str()?.to_string();
    let author_id = data["member"]["user"]["id"]
        .as_str()
        .or_else(|| data["user"]["id"].as_str())?
        .to_string();

    let cmd_data = data.get("data")?;

    // Button click (type 3): custom_id carries the action, e.g.
    //   perm_approve:<id>, perm_deny:<id>, perm_allow_all:<id>, perm_opt:<id>:<option>
    // Convert to text form so handle_pending_permission picks it up as a decision.
    let text = if interaction_type == 3 {
        let custom_id = cmd_data["custom_id"].as_str()?;
        // Ack the component interaction (type 6 = DEFERRED_UPDATE_MESSAGE — "I heard you").
        let ack_url = format!(
            "{}/interactions/{}/{}/callback",
            DISCORD_API_BASE, interaction_id, interaction_token
        );
        let ack_body = serde_json::json!({"type": 6});
        let _ = client
            .post(&ack_url)
            .header("Authorization", format!("Bot {}", token))
            .json(&ack_body)
            .send()
            .await;
        // Map button custom_id → text command that handle_pending_permission understands.
        if custom_id.starts_with("perm_approve") {
            "allow".to_string()
        } else if custom_id.starts_with("perm_allow_all") {
            "allow all".to_string()
        } else if custom_id.starts_with("perm_deny") {
            "deny".to_string()
        } else if let Some(rest) = custom_id.strip_prefix("perm_text:") {
            // Fallback text-mode buttons used by handle_pending_permission.
            match rest {
                "allow" => "allow".to_string(),
                "allow_all" => "allow all".to_string(),
                "deny" => "deny".to_string(),
                other => other.to_string(),
            }
        } else if custom_id.starts_with("perm_opt:") {
            // ACP agents send options via custom_id = "perm_opt:<req_id>:<option_id>".
            // For now map to allow (kiro's default option is usually allow).
            // TODO: wire specific option_id through to the engine when we need fine-grained ACP control.
            "allow".to_string()
        } else {
            tracing::debug!(custom_id, "discord: unknown button custom_id");
            return None;
        }
    } else {
        // Slash command (type 2): reconstruct from name + options.
        let cmd_name = cmd_data["name"].as_str()?;
        let mut text = format!("/{}", cmd_name);
        if let Some(options) = cmd_data["options"].as_array() {
            for opt in options {
                if let Some(val) = opt["value"].as_str() {
                    text.push(' ');
                    text.push_str(val);
                }
            }
        }
        // Acknowledge with DEFERRED_CHANNEL_MESSAGE_WITH_SOURCE (type 5).
        let ack_url = format!(
            "{}/interactions/{}/{}/callback",
            DISCORD_API_BASE, interaction_id, interaction_token
        );
        let ack_body = serde_json::json!({"type": 5});
        let _ = client
            .post(&ack_url)
            .header("Authorization", format!("Bot {}", token))
            .json(&ack_body)
            .send()
            .await;
        text
    };

    Some(IncomingMessage {
        id: interaction_id.to_string(),
        from: author_id,
        from_name: None,
        text,
        images: vec![],
        files: vec![],
        voice: None,
        is_group: true,
        channel_id: Some(channel_id.clone()),
        channel_name: None,
        reply_ctx: Box::new(InteractionReplyCtx {
            interaction_id: interaction_id.to_string(),
            interaction_token: interaction_token.to_string(),
            channel_id,
            first_response_sent: Arc::new(AtomicBool::new(false)),
        }),
    })
}

/// If the message is in a regular guild channel (not a thread/DM), create a
/// thread from that message and redirect the reply context to the new thread.
async fn maybe_create_thread(
    client: &reqwest::Client,
    token: &str,
    data: &serde_json::Value,
    mut msg: IncomingMessage,
) -> IncomingMessage {
    let guild_id = data["guild_id"].as_str();
    let is_dm = guild_id.is_none();

    if is_dm {
        return msg;
    }

    let channel_id = data["channel_id"].as_str().unwrap_or("");
    let message_id = data["id"].as_str().unwrap_or("");

    if channel_id.is_empty() || message_id.is_empty() {
        return msg;
    }

    let thread_name = {
        let raw = data["content"].as_str().unwrap_or("");
        let cleaned = strip_mentions(raw);
        let cleaned: String = cleaned.chars().filter(|c| !c.is_control()).collect();
        let trimmed = cleaned.trim();
        if trimmed.is_empty() {
            "session".to_string()
        } else {
            // Truncate by chars (not bytes) to avoid splitting multibyte UTF-8
            let truncated: String = trimmed.chars().take(90).collect();
            truncated
        }
    };

    let url = format!(
        "{}/channels/{}/messages/{}/threads",
        DISCORD_API_BASE, channel_id, message_id
    );
    let body = serde_json::json!({
        "name": thread_name,
        "auto_archive_duration": 1440
    });

    match client
        .post(&url)
        .header("Authorization", format!("Bot {}", token))
        .json(&body)
        .send()
        .await
    {
        Ok(resp) => {
            if let Ok(thread_data) = resp.json::<serde_json::Value>().await {
                if let Some(thread_id) = thread_data["id"].as_str() {
                    msg.reply_ctx = Box::new(DiscordReplyCtx {
                        channel_id: thread_id.to_string(),
                        message_id: Some(message_id.to_string()),
                        thread_id: Some(thread_id.to_string()),
                    });
                    msg.channel_id = Some(thread_id.to_string());
                    msg.channel_name = thread_data["name"].as_str().map(|s| s.to_string());
                    tracing::info!(thread_id = %thread_id, "discord: created thread for message");
                }
            }
        }
        Err(e) => {
            tracing::debug!(error = %e, "discord: failed to create thread, using channel directly");
        }
    }

    msg
}

// ---------------------------------------------------------------------------
// Mention stripping
// ---------------------------------------------------------------------------

/// Strip all Discord mentions (<@id>, <@!id>, <#id>, <@&id>) from text.
fn strip_mentions(text: &str) -> String {
    let mut result = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '<' {
            let mut inside = String::new();
            let mut found_close = false;
            for inner in chars.by_ref() {
                if inner == '>' {
                    found_close = true;
                    break;
                }
                inside.push(inner);
            }
            if found_close && (inside.starts_with('@') || inside.starts_with('#')) {
                continue;
            }
            result.push('<');
            result.push_str(&inside);
            if found_close {
                result.push('>');
            }
        } else {
            result.push(c);
        }
    }

    result
}

// ---------------------------------------------------------------------------
// Message chunking
// ---------------------------------------------------------------------------

/// Split a message into chunks that fit within `max_len` characters.
/// Tries to split at newlines; falls back to char boundary.
fn chunk_message(text: &str, max_len: usize) -> Vec<&str> {
    if text.len() <= max_len {
        return vec![text];
    }

    let mut chunks = Vec::new();
    let mut remaining = text;

    while !remaining.is_empty() {
        if remaining.len() <= max_len {
            chunks.push(remaining);
            break;
        }

        // Try to split at the last newline within max_len
        let search_range = &remaining[..max_len.min(remaining.len())];
        let split_at = search_range
            .rfind('\n')
            .map(|i| i + 1) // include the newline in the current chunk
            .unwrap_or_else(|| {
                // No newline found — split at char boundary
                let mut i = max_len;
                while i > 0 && !remaining.is_char_boundary(i) {
                    i -= 1;
                }
                i
            });

        if split_at == 0 {
            // Safety fallback
            chunks.push(remaining);
            break;
        }

        chunks.push(&remaining[..split_at]);
        remaining = &remaining[split_at..];
    }

    chunks
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_user_mention() {
        assert_eq!(strip_mentions("<@123456> hello"), " hello");
    }

    #[test]
    fn strip_nickname_mention() {
        assert_eq!(strip_mentions("<@!789> hi"), " hi");
    }

    #[test]
    fn strip_role_mention() {
        assert_eq!(strip_mentions("<@&999> team"), " team");
    }

    #[test]
    fn strip_channel_mention() {
        assert_eq!(strip_mentions("<#general> check this"), " check this");
    }

    #[test]
    fn strip_multiple_mentions() {
        assert_eq!(
            strip_mentions("<@111> <@222> please help"),
            "  please help"
        );
    }

    #[test]
    fn preserve_non_mention_angle_brackets() {
        assert_eq!(strip_mentions("a < b > c"), "a < b > c");
        assert_eq!(strip_mentions("<not_a_mention>"), "<not_a_mention>");
    }

    #[test]
    fn plain_text_unchanged() {
        assert_eq!(strip_mentions("hello world"), "hello world");
    }

    #[test]
    fn empty_string() {
        assert_eq!(strip_mentions(""), "");
    }

    #[test]
    fn factory_creates_platform() {
        let config = serde_json::json!({
            "token": "test-token-123",
            "guild_id": "999",
        });
        let platform = create(&config).unwrap();
        assert_eq!(platform.name(), "discord");
    }

    #[test]
    fn factory_with_allow_from() {
        let config = serde_json::json!({
            "token": "tok",
            "allow_from": "user1,user2",
        });
        let platform = create(&config).unwrap();
        assert_eq!(platform.name(), "discord");
    }

    #[test]
    fn factory_wildcard_allow() {
        let config = serde_json::json!({
            "token": "tok",
            "allow_from": "*",
        });
        let platform = create(&config).unwrap();
        assert_eq!(platform.name(), "discord");
    }

    #[test]
    fn capabilities_all_present() {
        let config = serde_json::json!({ "token": "t" });
        let p = create(&config).unwrap();
        assert!(p.as_message_updater().is_some());
        assert!(p.as_image_sender().is_some());
        assert!(p.as_inline_button_sender().is_some());
        assert!(p.as_typing_indicator().is_some());
    }

    #[test]
    fn resolve_channel_from_discord_ctx() {
        let ctx = DiscordReplyCtx {
            channel_id: "ch1".into(),
            message_id: None,
            thread_id: None,
        };
        assert_eq!(
            DiscordPlatform::resolve_channel_id(&ctx).unwrap(),
            "ch1"
        );
    }

    #[test]
    fn resolve_channel_prefers_thread_id() {
        let ctx = DiscordReplyCtx {
            channel_id: "ch1".into(),
            message_id: None,
            thread_id: Some("th1".into()),
        };
        assert_eq!(
            DiscordPlatform::resolve_channel_id(&ctx).unwrap(),
            "th1"
        );
    }

    #[test]
    fn resolve_channel_from_interaction_ctx() {
        let ctx = InteractionReplyCtx {
            interaction_id: "i1".into(),
            interaction_token: "tok".into(),
            channel_id: "ch2".into(),
            first_response_sent: Arc::new(AtomicBool::new(false)),
        };
        assert_eq!(
            DiscordPlatform::resolve_channel_id(&ctx).unwrap(),
            "ch2"
        );
    }

    #[test]
    fn parse_message_ignores_bots() {
        let data = serde_json::json!({
            "author": {"id": "1", "bot": true},
            "content": "hello",
            "channel_id": "c1",
            "id": "m1",
        });
        assert!(parse_message_create(&data, &None, None, None, false).is_none());
    }

    #[test]
    fn parse_message_dm() {
        let data = serde_json::json!({
            "author": {"id": "user1", "username": "bob"},
            "content": "hello",
            "channel_id": "dm1",
            "id": "m1",
        });
        let msg = parse_message_create(&data, &None, None, None, false).unwrap();
        assert_eq!(msg.text, "hello");
        assert_eq!(msg.from, "user1");
        assert!(!msg.is_group);
    }

    #[test]
    fn parse_message_guild_requires_mention() {
        let data = serde_json::json!({
            "author": {"id": "user1", "username": "bob"},
            "content": "hello",
            "channel_id": "c1",
            "id": "m1",
            "guild_id": "g1",
            "mentions": [],
        });
        // No mention, no matching guild -- should be None.
        assert!(parse_message_create(&data, &None, Some("bot1"), None, false).is_none());
    }

    #[test]
    fn parse_message_guild_with_mention() {
        let data = serde_json::json!({
            "author": {"id": "user1", "username": "bob"},
            "content": "<@bot1> do something",
            "channel_id": "c1",
            "id": "m1",
            "guild_id": "g1",
            "mentions": [{"id": "bot1"}],
        });
        let msg = parse_message_create(&data, &None, Some("bot1"), None, false).unwrap();
        assert_eq!(msg.text, "do something");
        assert!(msg.is_group);
    }

    #[test]
    fn parse_message_allow_from_blocks() {
        let allowed: HashSet<String> = vec!["allowed_user".to_string()].into_iter().collect();
        let data = serde_json::json!({
            "author": {"id": "other_user", "username": "eve"},
            "content": "hello",
            "channel_id": "c1",
            "id": "m1",
        });
        assert!(parse_message_create(&data, &Some(allowed), None, None, false).is_none());
    }
}
