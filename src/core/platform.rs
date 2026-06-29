#![allow(dead_code)] // trait definitions are API contracts; some methods are only used by specific platforms

use std::any::Any;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use super::message::IncomingMessage;

// ---------------------------------------------------------------------------
// ReplyCtx -- opaque trait object carrying platform-private routing info.
// Platforms downcast via `as_any()` to recover their concrete type.
// ---------------------------------------------------------------------------

pub trait ReplyCtx: Send + Sync + std::fmt::Debug {
    fn as_any(&self) -> &dyn Any;

    /// Human-readable hint used to derive the session key for this context.
    fn session_key_hint(&self) -> String;

    /// Clone this context into a new boxed trait object.
    fn clone_box(&self) -> Box<dyn ReplyCtx>;
}

impl Clone for Box<dyn ReplyCtx> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

// ---------------------------------------------------------------------------
// PreviewHandle -- opaque handle returned by streaming-preview methods.
// ---------------------------------------------------------------------------

pub trait PreviewHandle: Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

// ---------------------------------------------------------------------------
// Button -- inline button for platforms that support them (Telegram, Discord).
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Button {
    pub text: String,
    pub callback_data: String,
}

// ---------------------------------------------------------------------------
// MessageHandler -- callback the engine registers with each platform.
// ---------------------------------------------------------------------------

pub type MessageHandler =
    Arc<dyn Fn(Arc<dyn PlatformCapabilities>, IncomingMessage) + Send + Sync>;

// ---------------------------------------------------------------------------
// PlatformFactory -- constructor stored in the registry.
// ---------------------------------------------------------------------------

pub type PlatformFactory = fn(config: &serde_json::Value) -> Result<Arc<dyn PlatformCapabilities>>;

// ---------------------------------------------------------------------------
// Core Platform trait (required for every platform).
// ---------------------------------------------------------------------------

#[async_trait]
pub trait Platform: Send + Sync {
    /// Short identifier, e.g. "telegram", "discord".
    fn name(&self) -> &str;

    /// Start listening for incoming messages and dispatch them via `handler`.
    async fn start(&self, handler: MessageHandler) -> Result<()>;

    /// Reply in the same context (thread / channel) as the original message.
    async fn reply(&self, ctx: &dyn ReplyCtx, content: &str) -> Result<()>;

    /// Send a new message to the context (may or may not quote the original).
    async fn send(&self, ctx: &dyn ReplyCtx, content: &str) -> Result<()>;

    /// Reply with a visible quote/reference to the original message.
    /// Used for notification messages (queue status, tool use) so the user
    /// can see which message triggered the notification.
    /// Default: falls back to regular reply().
    async fn reply_quoted(&self, ctx: &dyn ReplyCtx, content: &str) -> Result<()> {
        self.reply(ctx, content).await
    }

    /// Register slash commands with the platform's native menu.
    /// Called after all commands and skills are discovered.
    /// Default: no-op (not all platforms support command menus).
    async fn register_commands(&self, _commands: &[BotCommand]) -> Result<()> {
        Ok(())
    }

    /// Whether this platform natively renders Markdown (including tables) in
    /// message bodies. When true, the engine sends the model's raw Markdown
    /// through unchanged; when false (the default — Discord, Telegram), the
    /// engine rewrites Markdown tables into an aligned code block, since plain
    /// chat text doesn't render `| --- |`. Feishu cards override this to true.
    fn renders_markdown(&self) -> bool {
        false
    }

    /// Gracefully shut down the platform connection.
    async fn stop(&self) -> Result<()>;
}

/// A command to register with a platform's native menu (Discord slash commands, etc.)
#[derive(Debug, Clone)]
pub struct BotCommand {
    pub name: String,
    pub description: String,
}

// ---------------------------------------------------------------------------
// Capability traits -- platforms implement only the ones they support.
// ---------------------------------------------------------------------------

/// Streaming preview: send a placeholder, then update it in-place.
#[async_trait]
pub trait MessageUpdater: Platform {
    async fn send_preview(
        &self,
        ctx: &dyn ReplyCtx,
        text: &str,
    ) -> Result<Box<dyn PreviewHandle>>;

    async fn update_preview(&self, handle: &dyn PreviewHandle, text: &str) -> Result<()>;

    async fn delete_preview(&self, handle: &dyn PreviewHandle) -> Result<()>;
}

/// Send image data directly.
#[async_trait]
pub trait ImageSender: Platform {
    async fn send_image(
        &self,
        ctx: &dyn ReplyCtx,
        data: &[u8],
        filename: &str,
        mime: &str,
    ) -> Result<()>;
}

/// Send arbitrary file data.
#[async_trait]
pub trait FileSender: Platform {
    async fn send_file(
        &self,
        ctx: &dyn ReplyCtx,
        data: &[u8],
        filename: &str,
    ) -> Result<()>;
}

/// Inline buttons (Telegram inline keyboards, Discord buttons).
#[async_trait]
pub trait InlineButtonSender: Platform {
    async fn send_with_buttons(
        &self,
        ctx: &dyn ReplyCtx,
        text: &str,
        buttons: &[Button],
    ) -> Result<Box<dyn PreviewHandle>>;

    async fn answer_callback(&self, callback_id: &str, text: &str) -> Result<()>;
}

/// Typing / "working..." indicator.
#[async_trait]
pub trait TypingIndicator: Platform {
    /// Returns a callback that, when invoked, stops the indicator.
    async fn start_typing(&self, ctx: &dyn ReplyCtx) -> Result<Box<dyn FnOnce() + Send>>;
}

// ---------------------------------------------------------------------------
// PlatformCapabilities -- query-interface pattern for capability detection.
// ---------------------------------------------------------------------------

pub trait PlatformCapabilities: Platform {
    fn as_message_updater(&self) -> Option<&dyn MessageUpdater> {
        None
    }
    fn as_image_sender(&self) -> Option<&dyn ImageSender> {
        None
    }
    fn as_file_sender(&self) -> Option<&dyn FileSender> {
        None
    }
    fn as_inline_button_sender(&self) -> Option<&dyn InlineButtonSender> {
        None
    }
    fn as_typing_indicator(&self) -> Option<&dyn TypingIndicator> {
        None
    }
}
