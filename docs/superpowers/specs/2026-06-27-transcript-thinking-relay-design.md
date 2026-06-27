# Design: relay inter-tool thinking/text from the transcript

## Problem

In hook mode, agentbridge relays only two things to chat:

- **Stop** — the turn's final reply (`last_assistant_message`).
- **PostToolUse** — the tool name + an input hint.

A turn's `thinking` and `text` blocks that occur *between* tool calls are
lost. Measured on a real transcript (24 turns): the dominant per-turn block
sequence is `thinking → text → tool_use` repeating — `thinking` recurs
throughout a turn (6/24 turns), not only at the start. So the reasoning and
the "here's what I'm about to do" commentary that precede each tool call
never reach the phone until the (possibly far-off) Stop.

This feature relays those inter-tool `thinking` + `text` blocks, **complete**
(not a rolling window), without breaking the single in-place progress message
from the tool-progress feature.

## Data flow

```
PostToolUse hook fires
  -> hook script includes transcript_path (new field)
  -> receiver resolves the session route + its transcript cursor
  -> read transcript JSONL, take assistant lines AFTER last_seen_uuid,
     extract thinking+text blocks, advance the cursor
  -> inject the text as ReplyChunk events into the session's event channel
  -> (then) inject the existing ToolUse progress event
Stop hook fires
  -> unchanged: relays last_assistant_message as the final Result
     (does NOT flush the transcript — see Decision 2)
```

## On the phone (two independent, self-editing messages)

```
💭 我先看前端结构…
   接下来改 App.tsx,加 useState…
   改完跑测试…              <- accumulates ALL thinking+text; at ~1900 chars
                               it freezes and a new message continues

⚡ 处理中… (3 步)            <- the separate tool-progress message
✓ 1. Read: App.tsx
▸ 2. Edit: App.tsx
```

The two messages have independent `PreviewHandle`s and never freeze each
other.

## Units

### 1. `src/transcript.rs` (new — pure logic)

```
struct TranscriptBlock { kind: BlockKind /* Thinking | Text */, text: String, uuid: String }
struct ReadResult { blocks: Vec<TranscriptBlock>, last_uuid: Option<String> }
fn read_blocks_after(path: &str, after_uuid: Option<&str>) -> ReadResult
```

- Reads the JSONL file, skips lines until `after_uuid` is found (or from the
  start if `None`), then collects `thinking` and `text` content blocks from
  subsequent `assistant` lines in order, tagging each with its line `uuid`.
- `last_uuid` = the uuid of the last assistant line consumed (the new cursor).
- Pure except for the file read; a parse error on any line is skipped, not
  fatal. Returns empty blocks (cursor unchanged) if the file can't be read.
- Unit-tested against sample JSONL: cursor advance, uuid-based dedup
  (same input twice with the advanced cursor yields nothing), thinking+text
  both extracted in order, malformed lines skipped, CJK content intact
  (char-boundary-safe; never byte-slice).

### 2. `hook_route` — per-session transcript cursor

- Each session route entry carries `last_seen_uuid: Option<String>`.
- **The cursor and the transcript read MUST be serialized per session** to
  avoid a read-modify-write race (axum runs each request as its own task;
  cc fires PostToolUse rapidly and the 2s POSTs can overlap). A per-session
  `Mutex` (or `tokio::sync::Mutex`) guards the WHOLE critical section
  "read cursor → read_blocks_after → advance cursor", not just the field.
- New API roughly: `with_transcript<F>(session_key, f)` that runs `f` holding
  the per-session lock, or an explicit `lock_transcript(session_key) -> Guard`.

### 3. `events.rs` — accumulating reply message (Decision 1)

- New `AgentEvent::ReplyChunk { content: String, thinking: bool }`.
- The event loop keeps a SEPARATE accumulating-message state, independent of
  the tool-progress `progress_handle`:
  - `reply_handle: Option<Box<dyn PreviewHandle>>`, `reply_buf: String`.
  - On `ReplyChunk`: append (`💭 ` prefix when `thinking`) to `reply_buf`;
    if no handle yet, `send_preview`; else `update_preview`. When `reply_buf`
    would exceed ~1900 chars, finalize the current message (leave it as-is)
    and start a fresh `send_preview` with the overflow (Decision: saturate
    then new). Char-boundary-safe split.
  - Reset on the `Result` (turn end), like `progress_*`.
- Does NOT touch `progress_handle`, does NOT emit `AgentEvent::Thinking`
  (so it never triggers `freeze_and_detach_preview`). This is why the two
  messages don't break each other.
- Gated to hook mode via the existing `tool_progress_inplace` flag (claude/acp
  never emit `ReplyChunk`, so no behavior change for them).

### 4. `hook_receiver` — orchestration

- `HookPayload` gains `transcript_path: Option<String>` (the script already
  receives it from Claude Code; just forward it).
- PostToolUse: holding the per-session transcript lock, `read_blocks_after`,
  inject one `ReplyChunk` per block (preserving order), then inject the
  existing `ToolUse`. The reply message is created before/independently of the
  progress message; ordering of the two chat messages is NOT promised
  (Decision 3).
- Stop: unchanged — relays `last_assistant_message`; does NOT flush the
  transcript (Decision 2).

## Decisions (confirmed)

1. **Complete, not rolling.** thinking+text accumulate in one message; at the
   2000-char Discord limit the message is finalized and a new one continues.
   (`StreamPreview` is unsuitable — it truncates from the head; we manage the
   accumulating buffer ourselves.)
2. **Stop does not flush the transcript.** PostToolUse fills in the
   inter-tool thinking/text; Stop keeps its current behavior
   (`last_assistant_message` → final Result). Zero duplication, zero
   regression risk to the working Stop path.
3. **No ordering promise between the two messages.** Reply text and tool
   progress are two independent Discord messages with independent handles;
   which appears first is incidental.
4. **thinking + text both relayed**, thinking prefixed `💭`.
5. **uuid dedup** via the per-session cursor.

## Failure safety

- transcript unreadable / parse error → skip the thinking/text relay for that
  hook; the existing ToolUse / Stop relay is unaffected (graceful degrade).
- Never panic on CJK: all truncation/splitting is char-boundary-safe.

## Risks to verify live (not design blockers)

- **Flush latency (Problem 4):** does the thinking/text preceding a tool fire
  land in the transcript by the time that tool's PostToolUse arrives? If
  flushing lags, a block surfaces one hook late — acceptable, but confirm it
  is not "always one behind."
- **Tail text (Problem 2b):** the text after the LAST tool, before Stop. If it
  equals `last_assistant_message` it is covered by Stop; confirm nothing is
  dropped when a turn ends with `…U T t`.

## Testing

- `transcript.rs`: pure-function unit tests (sample JSONL) — cursor advance,
  uuid dedup, thinking+text extraction order, malformed-line skip, CJK intact.
- `events.rs`: drive the loop with `ReplyChunk` events via the existing mock
  platform — assert accumulation into one message, saturate-then-new at the
  cap, and that `progress_handle` is untouched.
- `hook_receiver`: orchestration — per-session lock serializes concurrent
  PostToolUse (no duplicate blocks); PostToolUse flushes, Stop does not.

## Out of scope (noted, not touched)

- `StreamPreview` line 112-113 uses a raw byte slice
  (`&text[text.len()-MAX..]`) — a latent CJK panic. Pre-existing, unrelated to
  this feature; left alone (flagged for a separate fix).
