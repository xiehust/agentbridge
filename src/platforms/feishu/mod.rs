//! Feishu (Lark) platform adapter.
//!
//! Receives group/p2p messages over Feishu's WebSocket long-connection (via the
//! community `feishu-sdk` crate, which encapsulates the proprietary handshake)
//! and sends replies through the IM REST API. Mirrors the Discord/Telegram
//! adapters: implements the `Platform` + capability traits, builds an
//! `IncomingMessage` per inbound message, and invokes the engine's
//! `MessageHandler`.
//!
//! Outbound messages are interactive cards (`msg_type: "interactive"`) whose
//! body is a single Markdown component, so the model's Markdown — tables, bold,
//! lists, code — renders natively. Operation ids:
//! - send  → `im.v1.message.create`  (POST /messages)
//! - edit  → `im.v1.message.patch`   (PATCH /messages/:id)  — streaming preview
//! - delete→ `im.v1.message.delete`  (DELETE /messages/:id)

pub mod types;

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::Deserialize;

use feishu_sdk::core::{
    Config, Error as FsError, LogLevel, Logger, LoggerRef, FEISHU_BASE_URL, LARK_BASE_URL,
};
use feishu_sdk::event::{Event, EventDispatcher, EventDispatcherConfig, EventHandler, EventResp};
use feishu_sdk::ws::StreamClient;
use feishu_sdk::Client;

use crate::core::message::IncomingMessage;
use crate::core::platform::{
    MessageHandler, MessageUpdater, Platform, PlatformCapabilities, PreviewHandle, ReplyCtx,
    TypingIndicator,
};

use types::{FeishuPreviewHandle, FeishuReplyCtx};

/// The emoji shown on the user's message while the bot is working ("on it").
const WORKING_EMOJI: &str = "OnIt";

/// Config options for a `feishu` (or `lark`) platform entry.
#[derive(Debug, Deserialize)]
pub struct FeishuOptions {
    pub app_id: String,
    pub app_secret: String,
    /// "feishu" (default, feishu.cn) or "lark" (larksuite.com).
    #[serde(default)]
    pub domain: Option<String>,
    /// Comma-separated allowed user ids, or "*" / unset for everyone.
    #[serde(default)]
    pub allow_from: Option<String>,
    /// Respond to every group message (true) vs only when @mentioned (false).
    #[serde(default = "default_true")]
    pub group_reply_all: bool,
}

fn default_true() -> bool {
    true
}

/// Bridge the SDK's internal logging into `tracing`. The SDK logs the entire
/// long-connection lifecycle (event dispatch, "no handler registered",
/// challenge requests) that is otherwise invisible — a silently-dead feishu
/// channel is undiagnosable without it.
#[derive(Debug)]
struct TracingLogger;

impl Logger for TracingLogger {
    // No custom `target:` — the default (this module's path) already matches
    // the `agentbridge=info` EnvFilter; a foreign target would be filtered out.
    fn log(&self, level: LogLevel, message: &str) {
        match level {
            LogLevel::Error => tracing::error!("feishu-sdk: {message}"),
            LogLevel::Warn => tracing::warn!("feishu-sdk: {message}"),
            LogLevel::Info => tracing::info!("feishu-sdk: {message}"),
            LogLevel::Debug => tracing::debug!("feishu-sdk: {message}"),
        }
    }

    fn is_enabled(&self, level: LogLevel) -> bool {
        level >= LogLevel::Debug
    }
}

fn tracing_logger() -> LoggerRef {
    Arc::new(TracingLogger)
}

pub struct FeishuPlatform {
    app_id: String,
    app_secret: String,
    base_url: String,
    allow_from: Option<HashSet<String>>,
    group_reply_all: bool,
    /// REST client for sending/editing messages.
    client: Client,
    /// Weak self-reference so the event handler can pass `Arc<dyn ...>` to the
    /// engine's MessageHandler.
    self_ref: std::sync::Mutex<Option<std::sync::Weak<dyn PlatformCapabilities>>>,
}

impl FeishuPlatform {
    pub fn new(opts: FeishuOptions) -> Result<Self> {
        let base_url = match opts.domain.as_deref() {
            Some("lark") => LARK_BASE_URL.to_string(),
            _ => FEISHU_BASE_URL.to_string(),
        };
        let allow_from = opts.allow_from.and_then(|s| {
            if s.trim() == "*" || s.trim().is_empty() {
                None
            } else {
                Some(s.split(',').map(|x| x.trim().to_string()).collect())
            }
        });
        let config = Config::builder(&opts.app_id, &opts.app_secret)
            .base_url(&base_url)
            .build();
        let client = Client::new(config).map_err(|e| anyhow!("feishu client init: {e:?}"))?;
        Ok(Self {
            app_id: opts.app_id,
            app_secret: opts.app_secret,
            base_url,
            allow_from,
            group_reply_all: opts.group_reply_all,
            client,
            self_ref: std::sync::Mutex::new(None),
        })
    }

    fn set_self_ref(&self, weak: std::sync::Weak<dyn PlatformCapabilities>) {
        *self.self_ref.lock().unwrap() = Some(weak);
    }

    /// Send a text message to a chat, returning the created message id.
    /// Send an interactive card whose body is one Markdown component, returning
    /// the created message id. The card renders the model's raw Markdown
    /// (bold, lists, tables, code) natively — far richer than plain text.
    async fn send_card(&self, chat_id: &str, markdown: &str) -> Result<String> {
        let body = serde_json::json!({
            "receive_id": chat_id,
            "msg_type": "interactive",
            "content": build_card(markdown).to_string(),
        });
        let req = self
            .client
            .operation("im.v1.message.create")
            .query_param("receive_id_type", "chat_id")
            .body_json(&body)
            .map_err(|e| anyhow!("feishu body: {e:?}"))?;
        // Bound the SDK send: it runs on the engine's turn path, which holds the
        // per-session lock until the turn returns. An unbounded hang here strands
        // the lock and wedges the channel until restart.
        let resp = tokio::time::timeout(std::time::Duration::from_secs(30), req.send())
            .await
            .map_err(|_| anyhow!("feishu send: timed out after 30s"))?
            .map_err(|e| anyhow!("feishu send: {e:?}"))?;
        // Feishu returns HTTP 200 even on app-level failures (non-zero `code`
        // in the body), so the response body is the real signal — log it.
        let body_str = String::from_utf8_lossy(&resp.body);
        match parse_message_id(&resp.body) {
            Some(id) => {
                tracing::info!(status = resp.status, message_id = %id, "feishu card sent");
                Ok(id)
            }
            None => {
                tracing::warn!(status = resp.status, body = %body_str, "feishu card send failed");
                Err(anyhow!("feishu send failed: {body_str}"))
            }
        }
    }

    /// Add an emoji reaction to a message, returning the reaction id (needed to
    /// remove it later). Used as the "bot is working" indicator on the user's
    /// message — the Feishu equivalent of a typing indicator.
    async fn add_reaction(&self, message_id: &str, emoji_type: &str) -> Result<String> {
        let body = serde_json::json!({ "reaction_type": { "emoji_type": emoji_type } });
        let req = self
            .client
            .operation("im.v1.message_reaction.create")
            .path_param("message_id", message_id)
            .body_json(&body)
            .map_err(|e| anyhow!("feishu reaction body: {e:?}"))?;
        let resp = tokio::time::timeout(std::time::Duration::from_secs(15), req.send())
            .await
            .map_err(|_| anyhow!("feishu reaction: timed out after 15s"))?
            .map_err(|e| anyhow!("feishu reaction: {e:?}"))?;
        serde_json::from_slice::<serde_json::Value>(&resp.body)
            .ok()
            .and_then(|v| v.pointer("/data/reaction_id").and_then(|r| r.as_str()).map(String::from))
            .ok_or_else(|| anyhow!("feishu reaction: no reaction_id"))
    }

}

#[async_trait]
impl Platform for FeishuPlatform {
    fn name(&self) -> &str {
        "feishu"
    }

    /// Feishu replies are interactive cards with a Markdown component, which
    /// renders tables/bold/lists natively — so the engine sends raw Markdown
    /// rather than the code-block table fallback used for plain-text platforms.
    fn renders_markdown(&self) -> bool {
        true
    }

    async fn start(&self, handler: MessageHandler) -> Result<()> {
        let self_weak = self
            .self_ref
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| anyhow!("feishu: self_ref not set"))?;

        // Builds a fresh stream client (dispatcher included — the SDK consumes
        // it per stream, so every reconnect needs a new one).
        let app_id = self.app_id.clone();
        let app_secret = self.app_secret.clone();
        let base_url = self.base_url.clone();
        let allow_from = self.allow_from.clone();
        let group_reply_all = self.group_reply_all;
        let build_stream = move |handler: MessageHandler,
                                 self_weak: std::sync::Weak<dyn PlatformCapabilities>| {
            let app_id = app_id.clone();
            let app_secret = app_secret.clone();
            let base_url = base_url.clone();
            let allow_from = allow_from.clone();
            async move {
                let dispatcher =
                    EventDispatcher::new(EventDispatcherConfig::new(), tracing_logger());
                dispatcher
                    .register_handler(Box::new(MessageReceiveHandler {
                        handler,
                        self_weak,
                        allow_from,
                        group_reply_all,
                    }))
                    .await;
                let mut config = Config::builder(&app_id, &app_secret)
                    .base_url(&base_url)
                    .build();
                // Route the stream client's internal logs (frame dispatch,
                // reconnects) through tracing instead of the SDK's raw
                // eprintln default.
                config.logger = tracing_logger();
                StreamClient::builder(config)
                    .event_dispatcher(dispatcher)
                    .build()
                    .map_err(|e| anyhow!("feishu stream build: {e:?}"))
            }
        };

        // First build happens before spawning so a bad config still fails
        // start() loudly at startup.
        let first = build_stream(handler.clone(), self_weak.clone()).await?;

        let app_id = self.app_id.clone();
        // `stream.start()` blocks (internal auto-reconnect) until it gives up.
        // The engine's start loop must NOT block here, so the long-connection
        // runs on its own task — and unlike before, in a RECONNECT LOOP: if
        // the SDK's internal reconnect ever gives up (auth blip, fatal WS
        // error), we rebuild the stream with backoff instead of leaving the
        // platform silently dead until process restart.
        tokio::spawn(async move {
            let mut stream = Some(first);
            let mut backoff_secs: u64 = 5;
            loop {
                let s = match stream.take() {
                    Some(s) => s,
                    None => match build_stream(handler.clone(), self_weak.clone()).await {
                        Ok(s) => s,
                        Err(e) => {
                            tracing::error!(app_id = %app_id, error = %e, backoff_secs, "feishu: stream rebuild failed, retrying");
                            tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;
                            backoff_secs = (backoff_secs * 2).min(60);
                            continue;
                        }
                    },
                };
                tracing::info!(app_id = %app_id, "feishu: starting long-connection");
                let started = tokio::time::Instant::now();
                match s.start().await {
                    Ok(()) => tracing::warn!(app_id = %app_id, "feishu long-connection ended"),
                    Err(e) => tracing::error!(app_id = %app_id, error = ?e, "feishu long-connection ended with error"),
                }
                // A connection that survived a while was healthy — reset the
                // backoff so a one-off drop reconnects quickly.
                if started.elapsed() > std::time::Duration::from_secs(60) {
                    backoff_secs = 5;
                }
                tracing::warn!(app_id = %app_id, backoff_secs, "feishu: reconnecting long-connection");
                tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;
                backoff_secs = (backoff_secs * 2).min(60);
            }
        });
        Ok(())
    }

    async fn reply(&self, ctx: &dyn ReplyCtx, content: &str) -> Result<()> {
        let chat_id = chat_id_of(ctx)?;
        self.send_card(&chat_id, content).await?;
        Ok(())
    }

    async fn send(&self, ctx: &dyn ReplyCtx, content: &str) -> Result<()> {
        self.reply(ctx, content).await
    }

    async fn stop(&self) -> Result<()> {
        // The long-connection has no explicit teardown handle here; the process
        // lifecycle owns it. Nothing to do.
        Ok(())
    }
}

impl PlatformCapabilities for FeishuPlatform {
    fn as_message_updater(&self) -> Option<&dyn MessageUpdater> {
        Some(self)
    }

    fn as_typing_indicator(&self) -> Option<&dyn TypingIndicator> {
        Some(self)
    }
}

#[async_trait]
impl MessageUpdater for FeishuPlatform {
    async fn send_preview(
        &self,
        ctx: &dyn ReplyCtx,
        text: &str,
    ) -> Result<Box<dyn PreviewHandle>> {
        let chat_id = chat_id_of(ctx)?;
        let message_id = self.send_card(&chat_id, text).await?;
        Ok(Box::new(FeishuPreviewHandle { message_id }))
    }

    async fn update_preview(&self, handle: &dyn PreviewHandle, text: &str) -> Result<()> {
        let h = handle
            .as_any()
            .downcast_ref::<FeishuPreviewHandle>()
            .ok_or_else(|| anyhow!("feishu: wrong preview handle type"))?;
        // Edit in place via message.patch with the updated card content.
        let body = serde_json::json!({
            "content": build_card(text).to_string(),
        });
        let req = self
            .client
            .operation("im.v1.message.patch")
            .path_param("message_id", &h.message_id)
            .body_json(&body)
            .map_err(|e| anyhow!("feishu patch body: {e:?}"))?;
        // Bounded like send_card: preview edits fire constantly during
        // streaming and run on the engine's lock-holding turn path — the SDK
        // has NO default timeout, so an unbounded hang here strands the
        // session lock.
        let resp = tokio::time::timeout(std::time::Duration::from_secs(30), req.send())
            .await
            .map_err(|_| anyhow!("feishu patch: timed out after 30s"))?
            .map_err(|e| anyhow!("feishu patch: {e:?}"))?;
        // Surface app-level failures (HTTP 200 + non-zero code).
        let body_str = String::from_utf8_lossy(&resp.body);
        if !body_str.contains("\"code\":0") {
            tracing::warn!(status = resp.status, body = %body_str, "feishu card patch failed");
        }
        Ok(())
    }

    async fn delete_preview(&self, handle: &dyn PreviewHandle) -> Result<()> {
        let h = handle
            .as_any()
            .downcast_ref::<FeishuPreviewHandle>()
            .ok_or_else(|| anyhow!("feishu: wrong preview handle type"))?;
        let req = self
            .client
            .operation("im.v1.message.delete")
            .path_param("message_id", &h.message_id);
        tokio::time::timeout(std::time::Duration::from_secs(15), req.send())
            .await
            .map_err(|_| anyhow!("feishu delete: timed out after 15s"))?
            .map_err(|e| anyhow!("feishu delete: {e:?}"))?;
        Ok(())
    }
}

#[async_trait]
impl TypingIndicator for FeishuPlatform {
    /// "Bot is working" indicator: add a 🫡 (OnIt) reaction to the user's
    /// message; the returned callback removes it once the reply is sent. This
    /// is the Feishu stand-in for a typing indicator — feishu has no bot typing
    /// API, but a reaction on the triggering message reads the same: "received,
    /// working on it." Reaction add/remove was verified live before shipping.
    async fn start_typing(&self, ctx: &dyn ReplyCtx) -> Result<Box<dyn FnOnce() + Send>> {
        let message_id = ctx
            .as_any()
            .downcast_ref::<FeishuReplyCtx>()
            .and_then(|c| c.message_id.clone())
            .ok_or_else(|| anyhow!("feishu: no message_id to react to"))?;

        let reaction_id = self.add_reaction(&message_id, WORKING_EMOJI).await?;

        // The stop callback is sync; spawn the async removal. Clone the client
        // (it's cheap and Clone) so the closure owns what it needs.
        let client = self.client.clone();
        Ok(Box::new(move || {
            tokio::spawn(async move {
                let req = client
                    .operation("im.v1.message_reaction.delete")
                    .path_param("message_id", &message_id)
                    .path_param("reaction_id", &reaction_id);
                // Bounded so a hung connection doesn't leak this task forever.
                match tokio::time::timeout(std::time::Duration::from_secs(15), req.send()).await {
                    Ok(Err(e)) => {
                        tracing::warn!(error = %e, "feishu: failed to remove working reaction");
                    }
                    Err(_) => {
                        tracing::warn!("feishu: reaction removal timed out after 15s");
                    }
                    Ok(Ok(_)) => {}
                }
            });
        }))
    }
}

// ---------------------------------------------------------------------------
// Inbound message handler (feishu-sdk EventHandler)
// ---------------------------------------------------------------------------

struct MessageReceiveHandler {
    handler: MessageHandler,
    self_weak: std::sync::Weak<dyn PlatformCapabilities>,
    allow_from: Option<HashSet<String>>,
    group_reply_all: bool,
}

impl EventHandler for MessageReceiveHandler {
    fn event_type(&self) -> &str {
        "im.message.receive_v1"
    }

    fn handle(
        &self,
        event: Event,
    ) -> Pin<Box<dyn Future<Output = Result<Option<EventResp>, FsError>> + Send + '_>> {
        Box::pin(async move {
            if let Some(incoming) = self.parse_incoming(&event) {
                if let Some(platform) = self.self_weak.upgrade() {
                    (self.handler)(platform, incoming);
                }
            }
            Ok(None)
        })
    }
}

impl MessageReceiveHandler {
    /// Turn a Feishu message-receive event into an `IncomingMessage`, applying
    /// the @mention gate and allow-list. Returns `None` when the message should
    /// be ignored.
    fn parse_incoming(&self, event: &Event) -> Option<IncomingMessage> {
        let payload = event.event.as_ref()?;
        let msg = payload.get("message")?;
        let chat_id = msg.get("chat_id")?.as_str()?.to_string();
        let message_id = msg.get("message_id").and_then(|v| v.as_str()).map(String::from);
        let chat_type = msg.get("chat_type").and_then(|v| v.as_str()).unwrap_or("");
        let msg_type = msg.get("message_type").and_then(|v| v.as_str()).unwrap_or("");
        let sender_id = payload
            .pointer("/sender/sender_id/open_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        // Only handle text for now; other types are ignored (mirrors MVP scope).
        if msg_type != "text" {
            tracing::debug!(msg_type, "feishu: non-text message ignored");
            return None;
        }

        // content is a JSON string like {"text":"...@bot ..."}.
        let content_raw = msg.get("content").and_then(|v| v.as_str()).unwrap_or("");
        let text = serde_json::from_str::<serde_json::Value>(content_raw)
            .ok()
            .and_then(|c| c.get("text").and_then(|t| t.as_str()).map(String::from))
            .unwrap_or_default();

        // Group @mention gate: when not group_reply_all, require a bot mention.
        let is_group = chat_type == "group";
        if is_group && !self.group_reply_all {
            let mentioned = msg
                .get("mentions")
                .and_then(|m| m.as_array())
                .map(|a| !a.is_empty())
                .unwrap_or(false);
            if !mentioned {
                tracing::debug!(%chat_id, "feishu: group message without mention ignored");
                return None;
            }
        }

        // Strip "@_user_N" mention tokens Feishu injects into text.
        let clean = strip_mention_tokens(&text);
        if clean.trim().is_empty() {
            return None;
        }

        // Allow-list check (by sender open_id).
        if let Some(ref allow) = self.allow_from {
            if !allow.contains(&sender_id) {
                tracing::debug!(user = %sender_id, "feishu: unauthorized sender ignored");
                return None;
            }
        }

        Some(IncomingMessage {
            id: message_id.clone().unwrap_or_default(),
            from: sender_id,
            from_name: None,
            text: clean,
            images: vec![],
            files: vec![],
            voice: None,
            is_group,
            channel_id: Some(chat_id.clone()),
            channel_name: None,
            reply_ctx: Box::new(FeishuReplyCtx { chat_id, message_id }),
        })
    }
}

/// Remove Feishu's `@_user_1` / `@_all` mention placeholder tokens from text.
fn strip_mention_tokens(text: &str) -> String {
    text.split_whitespace()
        .filter(|w| !w.starts_with("@_"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Build a Feishu interactive card (JSON 2.0) whose body is one Markdown
/// element holding `markdown`. Feishu renders Markdown — including tables,
/// bold, lists, and code — natively inside this component.
fn build_card(markdown: &str) -> serde_json::Value {
    serde_json::json!({
        "schema": "2.0",
        "config": { "wide_screen_mode": true },
        "body": {
            "elements": [
                { "tag": "markdown", "content": markdown }
            ]
        }
    })
}

/// Pull `data.message_id` out of an IM API JSON response body.
fn parse_message_id(body: &[u8]) -> Option<String> {
    let v: serde_json::Value = serde_json::from_slice(body).ok()?;
    v.pointer("/data/message_id")
        .and_then(|m| m.as_str())
        .map(String::from)
}

/// Extract the chat_id from a reply context.
fn chat_id_of(ctx: &dyn ReplyCtx) -> Result<String> {
    ctx.as_any()
        .downcast_ref::<FeishuReplyCtx>()
        .map(|c| c.chat_id.clone())
        .ok_or_else(|| anyhow!("feishu: reply ctx is not a FeishuReplyCtx"))
}

/// Factory: build a Feishu platform from config options.
pub fn create(config: &serde_json::Value) -> Result<Arc<dyn PlatformCapabilities>> {
    let opts: FeishuOptions = serde_json::from_value(config.clone())?;
    let arc: Arc<FeishuPlatform> = Arc::new(FeishuPlatform::new(opts)?);
    let weak: std::sync::Weak<dyn PlatformCapabilities> =
        Arc::downgrade(&(arc.clone() as Arc<dyn PlatformCapabilities>));
    arc.set_self_ref(weak);
    Ok(arc)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_mentions_removes_at_tokens() {
        assert_eq!(strip_mention_tokens("@_user_1 hello world"), "hello world");
        assert_eq!(strip_mention_tokens("no mention here"), "no mention here");
        assert_eq!(strip_mention_tokens("@_all 大家好"), "大家好");
    }

    #[test]
    fn build_card_wraps_markdown() {
        let card = build_card("**hi**\n| a | b |\n|---|---|\n| 1 | 2 |");
        assert_eq!(card["schema"], "2.0");
        let el = &card["body"]["elements"][0];
        assert_eq!(el["tag"], "markdown");
        assert!(el["content"].as_str().unwrap().contains("| a | b |"));
    }

    #[test]
    fn parse_message_id_from_body() {
        let body = br#"{"code":0,"data":{"message_id":"om_abc123"},"msg":"success"}"#;
        assert_eq!(parse_message_id(body).as_deref(), Some("om_abc123"));
    }

    #[test]
    fn parse_message_id_missing() {
        assert_eq!(parse_message_id(b"{\"code\":0}"), None);
        assert_eq!(parse_message_id(b"not json"), None);
    }
}
