//! Read assistant `thinking` / `text` blocks from a Claude Code transcript.
//!
//! Claude Code appends every turn to a JSONL transcript (one JSON object per
//! line). In hook mode the bridge has the `transcript_path` from each hook
//! payload, so it can recover the `thinking` and `text` the model produced
//! *between* tool calls — content that no single hook event carries.
//!
//! [`read_blocks_after`] reads the file, skips everything up to and including a
//! cursor line (`after_uuid`), then returns the `thinking` + `text` blocks of
//! the assistant lines that follow, in order, plus the new cursor (the uuid of
//! the last assistant line seen). A caller persists that cursor per session and
//! passes it back next time, so each block is relayed exactly once.

use serde::Deserialize;

/// Which kind of assistant content block this is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockKind {
    Thinking,
    Text,
}

/// One relayable assistant block plus the uuid of the transcript line it came
/// from (used to advance the per-session cursor).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TranscriptBlock {
    pub kind: BlockKind,
    pub text: String,
    /// The `uuid` of the assistant line this block belongs to.
    pub line_uuid: String,
}

/// Result of a transcript read: the new blocks and the advanced cursor.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReadResult {
    pub blocks: Vec<TranscriptBlock>,
    /// The uuid of the last assistant line consumed, or `None` if the file held
    /// no assistant lines after the cursor. When `None`, the caller keeps its
    /// existing cursor unchanged.
    pub last_uuid: Option<String>,
}

// --- Minimal JSONL line shape (only the fields we need) --------------------

#[derive(Deserialize)]
struct Line {
    #[serde(rename = "type")]
    kind: Option<String>,
    uuid: Option<String>,
    message: Option<Message>,
}

#[derive(Deserialize)]
struct Message {
    // `content` is an array of blocks for assistant lines, but a plain STRING
    // for a typed user input. Deserialize as untyped JSON so a user line (or
    // any other shape) still parses — otherwise the whole line is dropped and
    // its uuid can't anchor the cursor (which broke seed-to-user-input).
    content: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct ContentBlock {
    #[serde(rename = "type")]
    kind: Option<String>,
    /// Present on `text` blocks.
    text: Option<String>,
    /// Present on `thinking` blocks (the reasoning text lives here, not in
    /// `text`).
    thinking: Option<String>,
}

/// Read the assistant `thinking` / `text` blocks that follow `after_uuid`.
///
/// Reads `path` (async; the hot path must not block the runtime). If
/// `after_uuid` is `Some`, every line up to and including the line with that
/// uuid is skipped, so only newer content is returned; if `None`, the whole
/// file is scanned from the start. A line that fails to parse is skipped rather
/// than aborting the read. A file that cannot be read yields an empty result
/// (the relay degrades to silence, never an error to the caller).
pub async fn read_blocks_after(path: &str, after_uuid: Option<&str>) -> ReadResult {
    let raw = match tokio::fs::read_to_string(path).await {
        Ok(s) => s,
        Err(e) => {
            tracing::debug!(error = %e, path, "transcript read failed; skipping relay");
            return ReadResult::default();
        }
    };
    parse_after(&raw, after_uuid)
}

/// Pure parsing core, separated from I/O so it can be unit-tested directly.
fn parse_after(raw: &str, after_uuid: Option<&str>) -> ReadResult {
    // When a cursor is given, skip lines until we have passed the cursor line.
    // `None` cursor means "start from the beginning" — already past.
    let mut passed_cursor = after_uuid.is_none();
    let mut blocks = Vec::new();
    let mut last_uuid: Option<String> = None;

    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let parsed: Line = match serde_json::from_str(line) {
            Ok(p) => p,
            Err(_) => continue, // tolerate a malformed/partial line
        };

        let uuid = parsed.uuid.as_deref();

        // Still seeking the cursor line: skip until we see it, then start
        // collecting from the NEXT line.
        if !passed_cursor {
            if uuid == after_uuid {
                passed_cursor = true;
            }
            continue;
        }

        if parsed.kind.as_deref() != Some("assistant") {
            continue;
        }
        let Some(line_uuid) = parsed.uuid.clone() else {
            continue; // an assistant line without a uuid can't anchor the cursor
        };
        // `content` is untyped JSON; only an array carries relayable blocks. A
        // non-array (or absent) content still advances the cursor.
        let content_blocks: Vec<ContentBlock> = parsed
            .message
            .and_then(|m| m.content)
            .and_then(|c| serde_json::from_value(c).ok())
            .unwrap_or_default();

        for b in content_blocks {
            match b.kind.as_deref() {
                Some("text") => {
                    if let Some(t) = non_empty(b.text) {
                        blocks.push(TranscriptBlock {
                            kind: BlockKind::Text,
                            text: t,
                            line_uuid: line_uuid.clone(),
                        });
                    }
                }
                Some("thinking") => {
                    if let Some(t) = non_empty(b.thinking) {
                        blocks.push(TranscriptBlock {
                            kind: BlockKind::Thinking,
                            text: t,
                            line_uuid: line_uuid.clone(),
                        });
                    }
                }
                _ => {} // tool_use and others are not relayed here
            }
        }
        last_uuid = Some(line_uuid);
    }

    ReadResult { blocks, last_uuid }
}

/// `Some(trimmed)` if the string is present and not whitespace-only.
fn non_empty(s: Option<String>) -> Option<String> {
    s.filter(|t| !t.trim().is_empty())
}

/// Find the cursor to SEED a freshly-attached session to: the uuid of the most
/// recent *real user input* line (a typed prompt, not a tool_result), so that
/// the assistant content of the CURRENT turn — everything after that input — is
/// relayed, while all prior turns (history) are skipped.
///
/// Returns `None` if no real user input is found (e.g. an empty/odd transcript),
/// in which case the caller should fall back to seeding at the tail.
pub async fn seed_cursor(path: &str) -> Option<String> {
    let raw = tokio::fs::read_to_string(path).await.ok()?;
    seed_cursor_from(&raw)
}

/// Pure core of [`seed_cursor`], unit-testable.
fn seed_cursor_from(raw: &str) -> Option<String> {
    let mut last_input: Option<String> = None;
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        // Parse loosely: we need type, uuid, and whether `message.content` is a
        // plain string / has a text block (real input) vs a tool_result array.
        let v: serde_json::Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if v.get("type").and_then(|t| t.as_str()) != Some("user") {
            continue;
        }
        let Some(uuid) = v.get("uuid").and_then(|u| u.as_str()) else {
            continue;
        };
        if is_real_user_input(&v) {
            last_input = Some(uuid.to_string());
        }
    }
    last_input
}

/// True when a `user` line is a typed prompt rather than a tool_result the
/// harness injected. A real prompt's `message.content` is a string, or an array
/// with no `tool_result` block; a tool result is an array containing a
/// `tool_result` block.
fn is_real_user_input(v: &serde_json::Value) -> bool {
    let content = match v.get("message").and_then(|m| m.get("content")) {
        Some(c) => c,
        None => return false,
    };
    match content {
        serde_json::Value::String(_) => true,
        serde_json::Value::Array(arr) => !arr.iter().any(|b| {
            b.get("type").and_then(|t| t.as_str()) == Some("tool_result")
        }),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Build a minimal assistant JSONL line with the given uuid and blocks.
    // Each block is (kind, text) where kind is "text" | "thinking".
    fn assistant_line(uuid: &str, blocks: &[(&str, &str)]) -> String {
        let content: Vec<serde_json::Value> = blocks
            .iter()
            .map(|(k, t)| match *k {
                "text" => serde_json::json!({"type": "text", "text": t}),
                "thinking" => serde_json::json!({"type": "thinking", "thinking": t, "signature": "x"}),
                "tool_use" => serde_json::json!({"type": "tool_use", "name": t, "input": {}}),
                _ => unreachable!(),
            })
            .collect();
        serde_json::json!({
            "type": "assistant",
            "uuid": uuid,
            "message": {"role": "assistant", "content": content}
        })
        .to_string()
    }

    fn user_line(uuid: &str) -> String {
        serde_json::json!({"type": "user", "uuid": uuid, "message": {"role": "user", "content": "hi"}})
            .to_string()
    }

    #[test]
    fn extracts_thinking_and_text_in_order() {
        let jsonl = [
            assistant_line("a1", &[("thinking", "let me think"), ("text", "I'll look")]),
            assistant_line("a2", &[("text", "now editing"), ("tool_use", "Edit")]),
        ]
        .join("\n");

        let r = parse_after(&jsonl, None);
        assert_eq!(r.blocks.len(), 3);
        assert_eq!(r.blocks[0].kind, BlockKind::Thinking);
        assert_eq!(r.blocks[0].text, "let me think");
        assert_eq!(r.blocks[1].kind, BlockKind::Text);
        assert_eq!(r.blocks[1].text, "I'll look");
        assert_eq!(r.blocks[2].text, "now editing");
        assert_eq!(r.last_uuid.as_deref(), Some("a2"));
    }

    #[test]
    fn cursor_skips_already_seen_lines() {
        let jsonl = [
            assistant_line("a1", &[("text", "first")]),
            assistant_line("a2", &[("text", "second")]),
            assistant_line("a3", &[("text", "third")]),
        ]
        .join("\n");

        // Cursor at a1 → only a2, a3 returned.
        let r = parse_after(&jsonl, Some("a1"));
        assert_eq!(r.blocks.len(), 2);
        assert_eq!(r.blocks[0].text, "second");
        assert_eq!(r.blocks[1].text, "third");
        assert_eq!(r.last_uuid.as_deref(), Some("a3"));
    }

    #[test]
    fn reading_twice_with_advanced_cursor_yields_nothing() {
        // The dedup invariant: relay once, then re-read with the returned
        // cursor → no repeats.
        let jsonl = [
            assistant_line("a1", &[("text", "first")]),
            assistant_line("a2", &[("text", "second")]),
        ]
        .join("\n");

        let first = parse_after(&jsonl, None);
        assert_eq!(first.last_uuid.as_deref(), Some("a2"));
        let second = parse_after(&jsonl, first.last_uuid.as_deref());
        assert!(second.blocks.is_empty(), "no repeats: {:?}", second.blocks);
        // Cursor unchanged (no new assistant lines).
        assert_eq!(second.last_uuid, None);
    }

    #[test]
    fn malformed_lines_are_skipped() {
        let jsonl = [
            assistant_line("a1", &[("text", "good")]),
            "{ this is not valid json".to_string(),
            "".to_string(),
            assistant_line("a2", &[("text", "also good")]),
        ]
        .join("\n");

        let r = parse_after(&jsonl, None);
        assert_eq!(r.blocks.len(), 2);
        assert_eq!(r.blocks[0].text, "good");
        assert_eq!(r.blocks[1].text, "also good");
    }

    #[test]
    fn non_assistant_and_empty_blocks_ignored() {
        let jsonl = [
            user_line("u1"),
            assistant_line("a1", &[("thinking", "   "), ("text", "real")]),
            user_line("u2"),
        ]
        .join("\n");

        let r = parse_after(&jsonl, None);
        // Whitespace-only thinking dropped; only the real text remains.
        assert_eq!(r.blocks.len(), 1);
        assert_eq!(r.blocks[0].kind, BlockKind::Text);
        assert_eq!(r.blocks[0].text, "real");
        // Cursor still advanced to the assistant line.
        assert_eq!(r.last_uuid.as_deref(), Some("a1"));
    }

    #[test]
    fn cjk_content_intact() {
        let jsonl = assistant_line("a1", &[("text", "我先看一下前端结构 😊"), ("thinking", "嗯,这里要改 useState")]);
        let r = parse_after(&jsonl, None);
        assert_eq!(r.blocks[0].text, "我先看一下前端结构 😊");
        assert_eq!(r.blocks[1].text, "嗯,这里要改 useState");
    }

    #[test]
    fn missing_cursor_uuid_yields_nothing_until_found() {
        // If the cursor uuid isn't in the file (e.g. transcript rotated), we
        // never "pass" it, so nothing is returned — safe, no duplicate flood.
        let jsonl = assistant_line("a1", &[("text", "content")]);
        let r = parse_after(&jsonl, Some("nonexistent"));
        assert!(r.blocks.is_empty());
        assert_eq!(r.last_uuid, None);
    }

    #[test]
    fn empty_input() {
        let r = parse_after("", None);
        assert!(r.blocks.is_empty());
        assert_eq!(r.last_uuid, None);
    }

    fn user_input_line(uuid: &str, text: &str) -> String {
        serde_json::json!({"type":"user","uuid":uuid,"message":{"role":"user","content":text}})
            .to_string()
    }

    fn tool_result_line(uuid: &str) -> String {
        serde_json::json!({
            "type":"user","uuid":uuid,
            "message":{"role":"user","content":[{"type":"tool_result","content":"ok"}]}
        })
        .to_string()
    }

    #[test]
    fn seed_cursor_picks_last_real_user_input() {
        // Among history with several turns, seed to the MOST RECENT typed input,
        // so the current turn (after it) is what gets relayed.
        let jsonl = [
            user_input_line("u1", "第一轮提问"),
            assistant_line("a1", &[("text", "第一轮回复")]),
            user_input_line("u2", "第二轮提问"),
            assistant_line("a2", &[("text", "第二轮回复")]),
        ]
        .join("\n");
        assert_eq!(seed_cursor_from(&jsonl).as_deref(), Some("u2"));
    }

    #[test]
    fn seed_cursor_ignores_tool_results() {
        // A tool_result is a `user` line too, but NOT a typed prompt — it must
        // not be mistaken for the turn boundary.
        let jsonl = [
            user_input_line("u1", "提问"),
            assistant_line("a1", &[("text", "回复"), ("tool_use", "Bash")]),
            tool_result_line("tr1"),
            assistant_line("a2", &[("text", "继续")]),
        ]
        .join("\n");
        // Seed to u1 (the real input), NOT tr1 (the tool result).
        assert_eq!(seed_cursor_from(&jsonl).as_deref(), Some("u1"));
    }

    #[test]
    fn seed_cursor_none_when_no_user_input() {
        let jsonl = assistant_line("a1", &[("text", "orphan")]);
        assert_eq!(seed_cursor_from(&jsonl), None);
    }

}
