//! Feishu (Lark) platform adapter.
//!
//! Receives group/p2p messages over Feishu's WebSocket long-connection (via the
//! community `feishu-sdk` crate, which encapsulates the proprietary handshake)
//! and sends replies through the IM REST API. Mirrors the Discord/Telegram
//! adapters: implements the `Platform` + capability traits, builds an
//! `IncomingMessage` per inbound message, and invokes the engine's
//! `MessageHandler`.
//!
//! Outbound message ids:
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

use feishu_sdk::core::{noop_logger, Config, Error as FsError, FEISHU_BASE_URL, LARK_BASE_URL};
use feishu_sdk::event::{Event, EventDispatcher, EventDispatcherConfig, EventHandler, EventResp};
use feishu_sdk::ws::StreamClient;
use feishu_sdk::Client;

use crate::core::message::IncomingMessage;
use crate::core::platform::{
    MessageHandler, MessageUpdater, Platform, PlatformCapabilities, PreviewHandle, ReplyCtx,
};

use types::{FeishuPreviewHandle, FeishuReplyCtx};

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
    async fn send_text(&self, chat_id: &str, text: &str) -> Result<String> {
        let body = serde_json::json!({
            "receive_id": chat_id,
            "msg_type": "text",
            "content": serde_json::json!({ "text": text }).to_string(),
        });
        let resp = self
            .client
            .operation("im.v1.message.create")
            .query_param("receive_id_type", "chat_id")
            .body_json(&body)
            .map_err(|e| anyhow!("feishu body: {e:?}"))?
            .send()
            .await
            .map_err(|e| anyhow!("feishu send: {e:?}"))?;
        parse_message_id(&resp.body)
            .ok_or_else(|| anyhow!("feishu send: no message_id in response"))
    }
}

#[async_trait]
impl Platform for FeishuPlatform {
    fn name(&self) -> &str {
        "feishu"
    }

    async fn start(&self, handler: MessageHandler) -> Result<()> {
        let self_weak = self
            .self_ref
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| anyhow!("feishu: self_ref not set"))?;

        let dispatcher = EventDispatcher::new(EventDispatcherConfig::new(), noop_logger());
        dispatcher
            .register_handler(Box::new(MessageReceiveHandler {
                handler,
                self_weak,
                allow_from: self.allow_from.clone(),
                group_reply_all: self.group_reply_all,
            }))
            .await;

        let config = Config::builder(&self.app_id, &self.app_secret)
            .base_url(&self.base_url)
            .build();
        let stream = StreamClient::builder(config)
            .event_dispatcher(dispatcher)
            .build()
            .map_err(|e| anyhow!("feishu stream build: {e:?}"))?;

        let app_id = self.app_id.clone();
        tracing::info!(app_id = %app_id, "feishu: starting long-connection");
        // `stream.start()` blocks (internal auto-reconnect) until the process
        // ends. The engine's start loop must NOT block here — it still has to
        // register commands, start cron, and start other platforms — so the
        // long-connection runs on its own task, mirroring Discord's gateway loop.
        tokio::spawn(async move {
            if let Err(e) = stream.start().await {
                tracing::error!(app_id = %app_id, error = ?e, "feishu long-connection ended");
            }
        });
        Ok(())
    }

    async fn reply(&self, ctx: &dyn ReplyCtx, content: &str) -> Result<()> {
        let chat_id = chat_id_of(ctx)?;
        self.send_text(&chat_id, content).await?;
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
}

#[async_trait]
impl MessageUpdater for FeishuPlatform {
    async fn send_preview(
        &self,
        ctx: &dyn ReplyCtx,
        text: &str,
    ) -> Result<Box<dyn PreviewHandle>> {
        let chat_id = chat_id_of(ctx)?;
        let message_id = self.send_text(&chat_id, text).await?;
        Ok(Box::new(FeishuPreviewHandle { message_id }))
    }

    async fn update_preview(&self, handle: &dyn PreviewHandle, text: &str) -> Result<()> {
        let h = handle
            .as_any()
            .downcast_ref::<FeishuPreviewHandle>()
            .ok_or_else(|| anyhow!("feishu: wrong preview handle type"))?;
        // Edit in place via message.patch. content is the same JSON-string shape.
        let body = serde_json::json!({
            "content": serde_json::json!({ "text": text }).to_string(),
        });
        self.client
            .operation("im.v1.message.patch")
            .path_param("message_id", &h.message_id)
            .body_json(&body)
            .map_err(|e| anyhow!("feishu patch body: {e:?}"))?
            .send()
            .await
            .map_err(|e| anyhow!("feishu patch: {e:?}"))?;
        Ok(())
    }

    async fn delete_preview(&self, handle: &dyn PreviewHandle) -> Result<()> {
        let h = handle
            .as_any()
            .downcast_ref::<FeishuPreviewHandle>()
            .ok_or_else(|| anyhow!("feishu: wrong preview handle type"))?;
        self.client
            .operation("im.v1.message.delete")
            .path_param("message_id", &h.message_id)
            .send()
            .await
            .map_err(|e| anyhow!("feishu delete: {e:?}"))?;
        Ok(())
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
