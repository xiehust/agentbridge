//! Feishu-specific ReplyCtx and PreviewHandle types.

#![allow(dead_code)] // some fields carry context consumed only via trait methods

use std::any::Any;

use crate::core::platform::{PreviewHandle, ReplyCtx};

/// Routing context for a Feishu chat (group or p2p) message.
///
/// `chat_id` is the stable per-chat identifier; it keys the session, mirroring
/// Discord's channel_id. `message_id` is the triggering message (used for
/// reply-threading if desired).
#[derive(Debug, Clone)]
pub struct FeishuReplyCtx {
    pub chat_id: String,
    pub message_id: Option<String>,
}

impl ReplyCtx for FeishuReplyCtx {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn session_key_hint(&self) -> String {
        format!("feishu:{}", self.chat_id)
    }

    fn clone_box(&self) -> Box<dyn ReplyCtx> {
        Box::new(self.clone())
    }
}

/// Handle to a Feishu message that can be edited (patch) or deleted in place,
/// for streaming previews.
#[derive(Debug)]
pub struct FeishuPreviewHandle {
    pub message_id: String,
}

impl PreviewHandle for FeishuPreviewHandle {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_key_uses_chat_id() {
        let ctx = FeishuReplyCtx {
            chat_id: "oc_abc".into(),
            message_id: Some("om_1".into()),
        };
        assert_eq!(ctx.session_key_hint(), "feishu:oc_abc");
    }

    #[test]
    fn clone_box_preserves_type() {
        let ctx = FeishuReplyCtx {
            chat_id: "oc_xyz".into(),
            message_id: None,
        };
        let boxed = ctx.clone_box();
        let down = boxed.as_any().downcast_ref::<FeishuReplyCtx>().unwrap();
        assert_eq!(down.chat_id, "oc_xyz");
    }

    #[test]
    fn preview_handle_downcast() {
        let h = FeishuPreviewHandle { message_id: "om_9".into() };
        let down = h.as_any().downcast_ref::<FeishuPreviewHandle>().unwrap();
        assert_eq!(down.message_id, "om_9");
    }
}
