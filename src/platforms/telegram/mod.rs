//! Telegram platform adapter (new core-trait implementation).
//!
//! Uses long polling (no public IP needed).
//! Supports: text replies, streaming edits, inline keyboards, image sending,
//! typing indicators, voice/photo download.

#![allow(dead_code)] // CallbackEvent fields are consumed by pattern matching in handler dispatch

pub mod types;

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use serde::Deserialize;
use tokio::sync::{mpsc, Mutex};

use crate::core::message::{ImageAttachment, IncomingMessage};
use crate::core::platform::{
    Button, ImageSender, InlineButtonSender, MessageHandler, MessageUpdater, Platform,
    PlatformCapabilities, PreviewHandle, ReplyCtx, TypingIndicator,
};

use types::{TelegramPreviewHandle, TelegramReplyCtx};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct TelegramOptions {
    pub token: String,
    pub allow_from: Option<String>,
}

// ---------------------------------------------------------------------------
// Callback event forwarded from the poll loop to the engine.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct CallbackEvent {
    pub id: String,
    pub from: String,
    pub data: String,
    pub reply_ctx: TelegramReplyCtx,
}

// ---------------------------------------------------------------------------
// TelegramPlatform
// ---------------------------------------------------------------------------

pub struct TelegramPlatform {
    token: String,
    allow_from: Option<HashSet<String>>,
    client: reqwest::Client,
    running: Arc<AtomicBool>,
    callback_tx: Arc<Mutex<Option<mpsc::Sender<CallbackEvent>>>>,
    /// Weak self-reference so the poll loop can pass `Arc<dyn PlatformCapabilities>`
    /// to the message handler.
    self_ref: std::sync::Mutex<Option<std::sync::Weak<dyn PlatformCapabilities>>>,
    outgoing_limiter: crate::outgoing_ratelimit::OutgoingRateLimiter,
}

impl TelegramPlatform {
    pub fn new(opts: TelegramOptions) -> Self {
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
            callback_tx: Arc::new(Mutex::new(None)),
            self_ref: std::sync::Mutex::new(None),
            outgoing_limiter: crate::outgoing_ratelimit::OutgoingRateLimiter::new(),
        }
    }

    /// Inject the weak self-reference after wrapping in Arc.
    /// Called by the factory function.
    pub fn set_self_ref(&self, weak: std::sync::Weak<dyn PlatformCapabilities>) {
        *self.self_ref.lock().unwrap() = Some(weak);
    }

    /// Low-level Telegram Bot API call.
    async fn api_call(&self, method: &str, params: &serde_json::Value) -> Result<serde_json::Value> {
        let url = format!("https://api.telegram.org/bot{}/{}", self.token, method);
        let resp = self.client.post(&url).json(params).send().await?;
        let body: serde_json::Value = resp.json().await?;
        Ok(body)
    }

    /// Downcast a trait-object ReplyCtx to our concrete type.
    fn downcast_ctx(ctx: &dyn ReplyCtx) -> Result<&TelegramReplyCtx> {
        ctx.as_any()
            .downcast_ref::<TelegramReplyCtx>()
            .ok_or_else(|| anyhow::anyhow!("telegram: ReplyCtx is not a TelegramReplyCtx"))
    }

    /// Downcast a trait-object PreviewHandle to our concrete type.
    fn downcast_handle(handle: &dyn PreviewHandle) -> Result<&TelegramPreviewHandle> {
        handle
            .as_any()
            .downcast_ref::<TelegramPreviewHandle>()
            .ok_or_else(|| anyhow::anyhow!("telegram: PreviewHandle is not a TelegramPreviewHandle"))
    }

    /// Send a message, splitting into chunks if it exceeds Telegram's 4096-char limit.
    /// Returns the message_id of the *last* chunk sent.
    async fn send_message_chunked(
        &self,
        chat_id: i64,
        thread_id: Option<i64>,
        content: &str,
    ) -> Result<i64> {
        const MAX_LEN: usize = 4096;

        let chunks = split_message(content, MAX_LEN);
        let mut last_msg_id: i64 = 0;

        for chunk in &chunks {
            let mut params = serde_json::json!({
                "chat_id": chat_id,
                "text": chunk,
                "parse_mode": "HTML",
            });
            if let Some(tid) = thread_id {
                params["message_thread_id"] = serde_json::json!(tid);
            }
            let resp = self.api_call("sendMessage", &params).await?;
            last_msg_id = resp["result"]["message_id"].as_i64().unwrap_or(0);
        }

        Ok(last_msg_id)
    }
}

// ---------------------------------------------------------------------------
// Platform (core required trait)
// ---------------------------------------------------------------------------

#[async_trait]
impl Platform for TelegramPlatform {
    fn name(&self) -> &str {
        "telegram"
    }

    async fn start(&self, handler: MessageHandler) -> Result<()> {
        self.running.store(true, Ordering::Relaxed);

        let running = self.running.clone();
        let token = self.token.clone();
        let allow_from = self.allow_from.clone();
        let client = self.client.clone();
        let callback_tx = self.callback_tx.clone();

        // Upgrade the weak self-reference so the poll loop can pass
        // Arc<dyn PlatformCapabilities> into every handler call.
        let self_weak = self
            .self_ref
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| anyhow::anyhow!("telegram: self_ref not set; call set_self_ref first"))?;

        tokio::spawn(poll_loop(
            token,
            allow_from,
            running,
            client,
            handler,
            callback_tx,
            self_weak,
        ));

        tracing::info!("telegram: long-poll started");
        Ok(())
    }

    async fn reply(&self, ctx: &dyn ReplyCtx, content: &str) -> Result<()> {
        let tg = Self::downcast_ctx(ctx)?;
        self.outgoing_limiter.wait("telegram", &tg.chat_id.to_string()).await;
        self.send_message_chunked(tg.chat_id, tg.thread_id, content)
            .await?;
        Ok(())
    }

    async fn send(&self, ctx: &dyn ReplyCtx, content: &str) -> Result<()> {
        // For Telegram, send and reply are identical (both go to the same chat).
        self.reply(ctx, content).await
    }

    async fn stop(&self) -> Result<()> {
        self.running.store(false, Ordering::Relaxed);
        tracing::info!("telegram: stopped");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// PlatformCapabilities
// ---------------------------------------------------------------------------

impl PlatformCapabilities for TelegramPlatform {
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
// MessageUpdater -- streaming preview (send, edit, delete)
// ---------------------------------------------------------------------------

#[async_trait]
impl MessageUpdater for TelegramPlatform {
    async fn send_preview(
        &self,
        ctx: &dyn ReplyCtx,
        text: &str,
    ) -> Result<Box<dyn PreviewHandle>> {
        let tg = Self::downcast_ctx(ctx)?;
        let mut params = serde_json::json!({
            "chat_id": tg.chat_id,
            "text": text,
            "parse_mode": "HTML",
        });
        if let Some(tid) = tg.thread_id {
            params["message_thread_id"] = serde_json::json!(tid);
        }
        let resp = self.api_call("sendMessage", &params).await?;
        let msg_id = resp["result"]["message_id"]
            .as_i64()
            .unwrap_or(0);

        Ok(Box::new(TelegramPreviewHandle {
            chat_id: tg.chat_id,
            message_id: msg_id,
        }))
    }

    async fn update_preview(&self, handle: &dyn PreviewHandle, text: &str) -> Result<()> {
        let h = Self::downcast_handle(handle)?;
        let params = serde_json::json!({
            "chat_id": h.chat_id,
            "message_id": h.message_id,
            "text": text,
            "parse_mode": "HTML",
        });
        // Ignore "message is not modified" errors from Telegram.
        let _ = self.api_call("editMessageText", &params).await;
        Ok(())
    }

    async fn delete_preview(&self, handle: &dyn PreviewHandle) -> Result<()> {
        let h = Self::downcast_handle(handle)?;
        let params = serde_json::json!({
            "chat_id": h.chat_id,
            "message_id": h.message_id,
        });
        let _ = self.api_call("deleteMessage", &params).await;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ImageSender -- send photos via multipart upload
// ---------------------------------------------------------------------------

#[async_trait]
impl ImageSender for TelegramPlatform {
    async fn send_image(
        &self,
        ctx: &dyn ReplyCtx,
        data: &[u8],
        filename: &str,
        mime: &str,
    ) -> Result<()> {
        let tg = Self::downcast_ctx(ctx)?;

        let part = reqwest::multipart::Part::bytes(data.to_vec())
            .file_name(filename.to_string())
            .mime_str(mime)?;

        let mut form = reqwest::multipart::Form::new()
            .text("chat_id", tg.chat_id.to_string())
            .part("photo", part);

        if let Some(tid) = tg.thread_id {
            form = form.text("message_thread_id", tid.to_string());
        }

        let url = format!("https://api.telegram.org/bot{}/sendPhoto", self.token);
        self.client.post(&url).multipart(form).send().await?;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// InlineButtonSender -- inline keyboard buttons + callback acknowledgement
// ---------------------------------------------------------------------------

#[async_trait]
impl InlineButtonSender for TelegramPlatform {
    async fn send_with_buttons(
        &self,
        ctx: &dyn ReplyCtx,
        text: &str,
        buttons: &[Button],
    ) -> Result<Box<dyn PreviewHandle>> {
        let tg = Self::downcast_ctx(ctx)?;

        let inline_buttons: Vec<serde_json::Value> = buttons
            .iter()
            .map(|b| {
                serde_json::json!({
                    "text": b.text,
                    "callback_data": b.callback_data,
                })
            })
            .collect();

        let mut params = serde_json::json!({
            "chat_id": tg.chat_id,
            "text": text,
            "reply_markup": {
                "inline_keyboard": [inline_buttons]
            },
        });
        if let Some(tid) = tg.thread_id {
            params["message_thread_id"] = serde_json::json!(tid);
        }

        let resp = self.api_call("sendMessage", &params).await?;
        let msg_id = resp["result"]["message_id"].as_i64().unwrap_or(0);

        Ok(Box::new(TelegramPreviewHandle {
            chat_id: tg.chat_id,
            message_id: msg_id,
        }))
    }

    async fn answer_callback(&self, callback_id: &str, text: &str) -> Result<()> {
        let params = serde_json::json!({
            "callback_query_id": callback_id,
            "text": text,
        });
        let _ = self.api_call("answerCallbackQuery", &params).await;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// TypingIndicator -- send "typing" chat action in a loop
// ---------------------------------------------------------------------------

#[async_trait]
impl TypingIndicator for TelegramPlatform {
    async fn start_typing(&self, ctx: &dyn ReplyCtx) -> Result<Box<dyn FnOnce() + Send>> {
        let tg = Self::downcast_ctx(ctx)?;
        let chat_id = tg.chat_id;
        let token = self.token.clone();
        let client = self.client.clone();

        let stop_flag = Arc::new(AtomicBool::new(false));
        let flag_clone = stop_flag.clone();

        // Send the first typing action immediately.
        let params = serde_json::json!({
            "chat_id": chat_id,
            "action": "typing",
        });
        let _ = self.api_call("sendChatAction", &params).await;

        // Spawn a repeating task that re-sends every 4 seconds.
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(4));
            interval.tick().await; // skip the immediate first tick
            loop {
                interval.tick().await;
                if flag_clone.load(Ordering::Relaxed) {
                    break;
                }
                let url = format!("https://api.telegram.org/bot{}/sendChatAction", token);
                let params = serde_json::json!({
                    "chat_id": chat_id,
                    "action": "typing",
                });
                let _ = client.post(&url).json(&params).send().await;
            }
        });

        Ok(Box::new(move || {
            stop_flag.store(true, Ordering::Relaxed);
        }))
    }
}

// ---------------------------------------------------------------------------
// Factory function
// ---------------------------------------------------------------------------

pub fn create(config: &serde_json::Value) -> Result<Arc<dyn PlatformCapabilities>> {
    let opts: TelegramOptions = serde_json::from_value(config.clone())?;
    let arc: Arc<TelegramPlatform> = Arc::new(TelegramPlatform::new(opts));
    // Inject weak self-reference so the poll loop can pass Arc to the handler.
    let weak: std::sync::Weak<dyn PlatformCapabilities> = Arc::downgrade(&(arc.clone() as Arc<dyn PlatformCapabilities>));
    arc.set_self_ref(weak);
    Ok(arc)
}

// ---------------------------------------------------------------------------
// Long-polling loop
// ---------------------------------------------------------------------------

async fn poll_loop(
    token: String,
    allow_from: Option<HashSet<String>>,
    running: Arc<AtomicBool>,
    client: reqwest::Client,
    handler: MessageHandler,
    callback_tx: Arc<Mutex<Option<mpsc::Sender<CallbackEvent>>>>,
    self_weak: std::sync::Weak<dyn PlatformCapabilities>,
) {
    let mut offset: i64 = 0;
    let mut backoff_secs: u64 = 1;

    while running.load(Ordering::Relaxed) {
        let params = serde_json::json!({
            "offset": offset,
            "timeout": 30,
            "allowed_updates": ["message", "edited_message", "callback_query"],
        });

        let url = format!("https://api.telegram.org/bot{}/getUpdates", token);
        match client.post(&url).json(&params).send().await {
            Ok(resp) => {
                backoff_secs = 1; // reset on success
                if let Ok(data) = resp.json::<serde_json::Value>().await {
                    if let Some(updates) = data["result"].as_array() {
                        for update in updates {
                            if let Some(uid) = update["update_id"].as_i64() {
                                offset = uid + 1;
                            }

                            // Handle callback queries from inline buttons.
                            if let Some(cb) = parse_callback_query(update) {
                                let lock = callback_tx.lock().await;
                                if let Some(ref tx) = *lock {
                                    let _ = tx.send(cb).await;
                                }
                                continue;
                            }

                            // Handle regular messages (text, photo, voice).
                            if let Some(msg) =
                                parse_incoming_message(update, &allow_from, &token, &client).await
                            {
                                if let Some(platform_arc) = self_weak.upgrade() {
                                    handler(platform_arc, msg);
                                } else {
                                    // Platform was dropped; stop polling.
                                    tracing::info!("telegram: platform dropped, stopping poll loop");
                                    return;
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, backoff = backoff_secs, "telegram: poll error, retrying");
                tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)).await;
                backoff_secs = (backoff_secs * 2).min(60);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Message parsing helpers
// ---------------------------------------------------------------------------

/// Parse a Telegram update into an `IncomingMessage`, handling text, photos, and voice.
async fn parse_incoming_message(
    update: &serde_json::Value,
    allow_from: &Option<HashSet<String>>,
    token: &str,
    client: &reqwest::Client,
) -> Option<IncomingMessage> {
    let message = update.get("message").or_else(|| update.get("edited_message"))?;

    let from_id = message["from"]["id"].as_i64().unwrap_or(0).to_string();
    let msg_id = message["message_id"].as_i64().unwrap_or(0);

    // ACL check.
    if let Some(ref allowed) = allow_from {
        if !allowed.contains(&from_id) {
            tracing::warn!(
                msg_id,
                from = %from_id,
                "telegram: ACL rejected message"
            );
            return None;
        }
    }

    let text = message["text"]
        .as_str()
        .or_else(|| message["caption"].as_str())
        .unwrap_or("")
        .to_string();

    // --- Photo attachments ---
    let mut images = Vec::new();
    let mut has_photo = false;
    if let Some(photo_array) = message["photo"].as_array() {
        has_photo = !photo_array.is_empty();
        // Telegram sends multiple resolutions; pick the largest (last).
        if let Some(largest) = photo_array.last() {
            if let Some(file_id) = largest["file_id"].as_str() {
                match download_file(token, file_id, client).await {
                    Ok((data, filename)) => {
                        images.push(ImageAttachment {
                            data,
                            mime_type: "image/jpeg".to_string(),
                            filename,
                        });
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "telegram: failed to download photo");
                    }
                }
            }
        }
    }

    // --- Voice attachments ---
    let mut voice_path = None;
    if let Some(voice) = message.get("voice") {
        if let Some(file_id) = voice["file_id"].as_str() {
            match download_voice_to_temp(token, file_id, client).await {
                Ok(path) => {
                    voice_path = Some(path);
                }
                Err(e) => {
                    tracing::warn!(error = %e, "telegram: failed to download voice");
                }
            }
        }
    }

    // Skip messages with no text, no photo, and no voice.
    if text.is_empty() && !has_photo && voice_path.is_none() {
        return None;
    }

    let chat_id = message["chat"]["id"].as_i64().unwrap_or(0);
    let message_id = message["message_id"].as_i64();
    let thread_id = message["message_thread_id"].as_i64();
    let chat_type = message["chat"]["type"].as_str().unwrap_or("private");

    let channel_name = message["chat"]["title"].as_str().map(|s| s.to_string());

    Some(IncomingMessage {
        id: message_id.unwrap_or(0).to_string(),
        from: from_id,
        from_name: message["from"]["first_name"].as_str().map(|s| s.to_string()),
        text,
        images,
        files: Vec::new(),
        voice: voice_path,
        is_group: chat_type != "private",
        channel_id: Some(chat_id.to_string()),
        channel_name,
        reply_ctx: Box::new(TelegramReplyCtx {
            chat_id,
            thread_id,
            message_id,
        }),
    })
}

/// Parse a callback_query from a Telegram update.
fn parse_callback_query(update: &serde_json::Value) -> Option<CallbackEvent> {
    let cq = update.get("callback_query")?;
    let id = cq["id"].as_str()?.to_string();
    let from = cq["from"]["id"].as_i64().unwrap_or(0).to_string();
    let data = cq["data"].as_str().unwrap_or("").to_string();

    let chat_id = cq["message"]["chat"]["id"].as_i64().unwrap_or(0);
    let message_id = cq["message"]["message_id"].as_i64();
    let thread_id = cq["message"]["message_thread_id"].as_i64();

    Some(CallbackEvent {
        id,
        from,
        data,
        reply_ctx: TelegramReplyCtx {
            chat_id,
            thread_id,
            message_id,
        },
    })
}

// ---------------------------------------------------------------------------
// File download helpers
// ---------------------------------------------------------------------------

/// Download a file from Telegram by file_id. Returns (bytes, filename).
async fn download_file(
    token: &str,
    file_id: &str,
    client: &reqwest::Client,
) -> Result<(Vec<u8>, String)> {
    // Step 1: resolve file_path via getFile.
    let url = format!("https://api.telegram.org/bot{}/getFile", token);
    let params = serde_json::json!({ "file_id": file_id });
    let resp = client.post(&url).json(&params).send().await?;
    let body: serde_json::Value = resp.json().await?;

    let file_path = body["result"]["file_path"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("getFile returned no file_path"))?;

    // Derive a filename from the path.
    let filename = file_path
        .rsplit('/')
        .next()
        .unwrap_or("photo.jpg")
        .to_string();

    // Step 2: download the actual bytes.
    let download_url = format!("https://api.telegram.org/file/bot{}/{}", token, file_path);
    let data = client.get(&download_url).send().await?.bytes().await?.to_vec();

    Ok((data, filename))
}

/// Download a voice message to a temporary file and return its path.
async fn download_voice_to_temp(
    token: &str,
    file_id: &str,
    client: &reqwest::Client,
) -> Result<String> {
    // Step 1: resolve file_path.
    let url = format!("https://api.telegram.org/bot{}/getFile", token);
    let params = serde_json::json!({ "file_id": file_id });
    let resp = client.post(&url).json(&params).send().await?;
    let body: serde_json::Value = resp.json().await?;

    let file_path = body["result"]["file_path"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("getFile returned no file_path"))?;

    // Step 2: download bytes.
    let download_url = format!("https://api.telegram.org/file/bot{}/{}", token, file_path);
    let file_bytes = client.get(&download_url).send().await?.bytes().await?;

    // Step 3: write to temp file.
    let temp_dir = std::env::temp_dir().join("agentbridge_voice");
    std::fs::create_dir_all(&temp_dir)?;

    let extension = file_path.rsplit('.').next().unwrap_or("ogg");
    let filename = format!(
        "{}_{}.{}",
        file_id,
        chrono::Utc::now().timestamp(),
        extension
    );
    let local_path = temp_dir.join(&filename);
    std::fs::write(&local_path, &file_bytes)?;

    Ok(local_path.display().to_string())
}

// ---------------------------------------------------------------------------
// Utility: split a message that exceeds Telegram's 4096-char limit.
// ---------------------------------------------------------------------------

fn split_message(text: &str, max_len: usize) -> Vec<&str> {
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

        // Try to split at a newline boundary within the limit.
        let split_at = remaining[..max_len]
            .rfind('\n')
            .map(|pos| pos + 1) // include the newline in the current chunk
            .unwrap_or(max_len);

        let (chunk, rest) = remaining.split_at(split_at);
        chunks.push(chunk);
        remaining = rest;
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
    fn test_split_message_short() {
        let chunks = split_message("hello", 4096);
        assert_eq!(chunks, vec!["hello"]);
    }

    #[test]
    fn test_split_message_exact() {
        let text = "a".repeat(4096);
        let chunks = split_message(&text, 4096);
        assert_eq!(chunks.len(), 1);
    }

    #[test]
    fn test_split_message_long() {
        let text = "a".repeat(5000);
        let chunks = split_message(&text, 4096);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].len(), 4096);
        assert_eq!(chunks[1].len(), 904);
    }

    #[test]
    fn test_split_message_at_newline() {
        let mut text = "a".repeat(4000);
        text.push('\n');
        text.push_str(&"b".repeat(200));
        let chunks = split_message(&text, 4096);
        assert_eq!(chunks.len(), 2);
        assert!(chunks[0].ends_with('\n'));
    }

    #[test]
    fn test_reply_ctx_session_key() {
        let ctx = TelegramReplyCtx {
            chat_id: 12345,
            thread_id: None,
            message_id: Some(1),
        };
        assert_eq!(ctx.session_key_hint(), "telegram:12345");
    }

    #[test]
    fn test_reply_ctx_clone_box() {
        let ctx = TelegramReplyCtx {
            chat_id: 42,
            thread_id: Some(7),
            message_id: Some(99),
        };
        let boxed: Box<dyn ReplyCtx> = Box::new(ctx);
        let cloned = boxed.clone_box();
        assert_eq!(cloned.session_key_hint(), "telegram:42");

        // Verify the concrete type survived the clone.
        let concrete = cloned.as_any().downcast_ref::<TelegramReplyCtx>().unwrap();
        assert_eq!(concrete.thread_id, Some(7));
    }

    #[test]
    fn test_preview_handle_downcast() {
        let handle = TelegramPreviewHandle {
            chat_id: 100,
            message_id: 200,
        };
        let boxed: Box<dyn PreviewHandle> = Box::new(handle);
        let concrete = boxed.as_any().downcast_ref::<TelegramPreviewHandle>().unwrap();
        assert_eq!(concrete.chat_id, 100);
        assert_eq!(concrete.message_id, 200);
    }

    #[test]
    fn test_parse_callback_query() {
        let update = serde_json::json!({
            "update_id": 1,
            "callback_query": {
                "id": "cb123",
                "from": { "id": 42 },
                "data": "approve",
                "message": {
                    "chat": { "id": 100 },
                    "message_id": 55
                }
            }
        });
        let cb = parse_callback_query(&update).unwrap();
        assert_eq!(cb.id, "cb123");
        assert_eq!(cb.from, "42");
        assert_eq!(cb.data, "approve");
        assert_eq!(cb.reply_ctx.chat_id, 100);
        assert_eq!(cb.reply_ctx.message_id, Some(55));
    }

    #[test]
    fn test_parse_callback_query_missing() {
        let update = serde_json::json!({
            "update_id": 1,
            "message": { "text": "hello" }
        });
        assert!(parse_callback_query(&update).is_none());
    }

    #[test]
    fn test_telegram_options_deserialize() {
        let json = serde_json::json!({
            "token": "123:ABC",
            "allow_from": "100,200"
        });
        let opts: TelegramOptions = serde_json::from_value(json).unwrap();
        assert_eq!(opts.token, "123:ABC");
        assert_eq!(opts.allow_from.unwrap(), "100,200");
    }

    #[test]
    fn test_allow_from_wildcard() {
        let opts = TelegramOptions {
            token: "t".into(),
            allow_from: Some("*".into()),
        };
        let platform = TelegramPlatform::new(opts);
        assert!(platform.allow_from.is_none());
    }

    #[test]
    fn test_allow_from_list() {
        let opts = TelegramOptions {
            token: "t".into(),
            allow_from: Some("100, 200, 300".into()),
        };
        let platform = TelegramPlatform::new(opts);
        let set = platform.allow_from.unwrap();
        assert!(set.contains("100"));
        assert!(set.contains("200"));
        assert!(set.contains("300"));
        assert_eq!(set.len(), 3);
    }
}
