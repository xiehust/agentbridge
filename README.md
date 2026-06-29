# agentbridge

[![CI](https://github.com/warren830/agentbridge/actions/workflows/ci.yml/badge.svg)](https://github.com/warren830/agentbridge/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Bridge Claude Code (and other AI coding agents) to your favorite chat apps. Control your AI coding agent from Telegram, Discord, Feishu/Lark, and more — without a public IP or reverse proxy.

## Quick Start

### Install

Requires [Rust](https://rustup.rs) (stable) and a working `claude` CLI on PATH.

**From source:**

```bash
git clone https://github.com/warren830/agentbridge.git
cd agentbridge
cargo build --release
# binary is at target/release/agentbridge — put it on your PATH:
sudo cp target/release/agentbridge /usr/local/bin/   # or: cp ... ~/.local/bin/
```

Verify with `agentbridge --version`.

### Configure

```bash
agentbridge init
```

This creates `~/.agentbridge/config.yaml`. A minimal example:

```yaml
language: en
projects:
  - name: my-project
    work_dir: "/path/to/your/project"
    agents:
      - name: claude
        backend: claude
        mode: yolo              # default | yolo | plan | auto
        # model: sonnet         # optional; see "Models" below
    default_agent: claude
    platforms:
      - type: telegram
        options:
          token: "123456:ABC-DEF"
      - type: discord
        options:
          token: "your-discord-bot-token"
          guild_id: "1234567890"       # optional, restrict to one server
          group_reply_all: true        # reply to every message (vs only @mentions)
          thread_isolation: false      # see "Working with multiple repos" below
      - type: feishu                   # Feishu / Lark (long-connection, no public URL needed)
        options:
          app_id: "cli_xxxxxxxx"
          app_secret: "your-app-secret"
          group_reply_all: true        # reply to every group message (vs only @mentions)
          # domain: lark               # optional: "lark" for larksuite.com (default feishu.cn)
    allow_from: "*"                    # or comma-separated user IDs
    rate_limit:
      max_messages: 20
      window_secs: 60
```

Run `agentbridge doctor` to validate the config before launching.

### Run

```bash
agentbridge           # start in foreground
agentbridge daemon install && agentbridge daemon start   # systemd user service
```

---

## Features

- **Multi-platform**: Telegram, Discord, and Feishu/Lark adapters, pluggable architecture for more
- **Multi-agent**: Claude Code (native) + any ACP-compatible agent (Kiro CLI, Cursor, Gemini ACP mode); switch between agents mid-conversation with `/agent`
- **Streaming responses**: Live message editing shows agent output as it generates
- **Session management**: Multiple named sessions per user, persist across restarts, per-agent isolation
- **Model/mode switching**: Change models and permission modes on the fly with `/model` / `/mode`
- **Thread isolation (Discord)**: Use separate Discord threads per session / per repo
- **Per-session work_dir**: Bind a session to a specific repo with `/dir <path>`
- **Cron jobs**: Schedule recurring prompts (e.g. daily status summaries)
- **Custom commands**: Project-specific slash commands with prompt templates
- **Inline permission buttons**: Approve tool calls with native buttons (not text replies)
- **Rate limiting**: Per-user sliding-window rate limiter
- **Access control**: Restrict which users can interact with the bot
- **Image support**: Send photos to Claude (Telegram)
- **Banned words filter**: Silently drop messages matching configured words
- **i18n**: English, Chinese, Japanese UI strings
- **Daemon mode**: Run as a systemd user service
- **Web Dashboard (beta)**: Nuxt 3 + Vue 3 management UI with live WebSocket session stream

---

## Models

### Default behavior

agentbridge does **not** set a default model itself. When spawning `claude` it omits `--model`, so Claude CLI falls back to whatever is configured in `~/.claude/settings.json` (usually `sonnet`).

You can pin a model in three places (later wins):

1. **agentbridge config** — `agents[].model` (sticky across restarts)
2. **Chat command** — `/model <name>` (per-session override; survives restart via the session file)
3. **Claude CLI settings** — `~/.claude/settings.json` (affects every `claude` invocation on the host)

### Valid model values (Claude backend)

Anything Claude CLI understands:

| Alias | Resolves to (as of 2026-04) |
|-------|----------------------------|
| `sonnet` | latest Claude Sonnet |
| `opus` | latest Claude Opus |
| `opus[1m]` | latest Opus with 1M context |
| `haiku` | latest Claude Haiku |
| `claude-sonnet-4-6` | specific version, 1M context |
| `claude-opus-4-7` | specific version |

Use `claude --help` on your host to see what your CLI version supports.

### ACP backend caveat

For ACP agents (Kiro CLI, Cursor, Gemini ACP mode), the `model` field in config and `/model` command are **currently ignored** — the model is selected by the ACP agent itself. For `kiro-cli` the default is `auto` (AWS-side task-based routing). To pin a model for kiro, either:

- Use `kiro-cli settings` to set a default on the host (affects every kiro-cli invocation)
- Wait for `model` passthrough support in agentbridge's ACP adapter (planned)

### Checking the active model

In any chat:

```
/model                          # show current model for current session+agent
/model claude-sonnet-4-6        # switch for this session (next turn)
```

---

## Agents

agentbridge supports multiple AI agent backends. Each project can declare multiple agents and users can switch between them at runtime.

```yaml
projects:
  - name: my-project
    agents:
      - name: claude
        backend: claude
        mode: yolo
        # model: sonnet
      - name: kiro
        backend: acp
        acp:
          command: kiro-cli
          args: ["acp"]
          display_name: "Kiro"
    default_agent: claude
```

Switch between agents in chat:

```
/agent                  # show current agent + list
/agent kiro             # switch to kiro (starts a fresh session for that agent)
/agent claude           # switch back
```

Each agent keeps its own session list. `/list`, `/new`, `/switch` operate on the currently active agent only. Cron jobs remember which agent created them.

**Backward compatibility**: old configs with a single `agent: { ... }` block (no `agents:` array) still work and are internally normalized to a single-agent setup named `claude`.

---

## Working with multiple repos

Recommended pattern for managing multiple repositories from one bot: **one Discord thread per repo**, each bound to its repo via `/dir`.

### Setup

In your config, use `thread_isolation: false` (the default) and pick a single `work_dir` as the fallback:

```yaml
projects:
  - name: workspace
    work_dir: /home/you/code          # fallback for unbound threads
    agents:
      - name: claude
        backend: claude
        mode: yolo
    default_agent: claude
    platforms:
      - type: discord
        options:
          token: "..."
          guild_id: "..."
          thread_isolation: false     # ← you manage threads, bot doesn't
          group_reply_all: true
```

### Workflow

1. In Discord, create one channel (or use DM) as your AI workspace
2. For each repo, right-click the channel → Create Thread → name it after the repo
3. In the thread, send your first message: `/dir /home/you/code/<repo-name>`
4. That thread is now bound to that repo. Every message in this thread operates on this `work_dir` with its own persistent session

Your setup ends up looking like:

```
#ai-coding
 ├─ [Thread] agentbridge   →  /dir /home/you/code/agentbridge
 ├─ [Thread] my-app        →  /dir /home/you/code/my-app
 └─ [Thread] side-project  →  /dir /home/you/code/side-project
```

Each thread has:
- Its own `work_dir` (via `/dir`)
- Its own agent session (persisted across restarts)
- Optionally its own active agent (`/agent kiro` in that thread only)
- Optionally its own model (`/model opus` in that thread only)

### When to use `thread_isolation: true` instead

`thread_isolation: true` makes the bot **auto-create a new thread for every @mention in the parent channel**. That's the right choice for **one-shot task** flows ("quick question → thread → done"), not for persistent per-repo workspaces.

### Known limitations

- **No `/project` command** — a user ID is bound to one project per agentbridge instance. Multiple repos = multiple threads within one project, not multiple projects.
- **No auto-cleanup of archived threads** — if you archive a Discord thread, its session stays in `sessions.json`. Use `/list` + `/delete` to clean up manually.
- **Thread name ↔ session name is one-way** — agentbridge picks up the Discord thread name on first contact, but later renames in Discord won't sync back.

---

## Chat Commands

| Command | Description |
|---------|-------------|
| `/help` | Show available commands |
| `/status` | Show project + session + agent status |
| `/new [name]` | Create a new session (for current agent) |
| `/list` | List sessions for the current agent |
| `/switch <id>` | Switch to a different session |
| `/current` | Show current session info |
| `/delete` | Delete current session |
| `/agent [name]` | Show current agent, or switch to a different agent |
| `/model [name]` | Show or change the active model (Claude backend only) |
| `/mode [mode]` | Show or change the permission mode (`default`/`yolo`/`plan`/`auto`) |
| `/dir [path]` or `/cd [path]` | Show or change the working directory for this session |
| `/stop` | Cancel the current running turn |
| `/compress` or `/compact` | Manually trigger context compression |
| `/btw <message>` | Inject a message into the running agent mid-turn |
| `/cron add <schedule> <prompt>` | Add a scheduled task |
| `/cron list` | List scheduled tasks |
| `/cron del <id>` | Delete a scheduled task |
| `/commands` | List custom commands |
| `/skills` | List available skills |

---

## CLI Commands

```
agentbridge                    Start the bridge (foreground)
agentbridge run                Same as above
agentbridge init               Interactive setup wizard
agentbridge doctor             Check configuration health
agentbridge daemon install     Install as a systemd user service
agentbridge daemon start       Start the background service
agentbridge daemon stop        Stop the background service
agentbridge daemon status      Show service status
agentbridge daemon logs        Tail service logs
agentbridge daemon uninstall   Remove the service
```

---

## Platform Setup

### Telegram

1. Create a bot via [@BotFather](https://t.me/BotFather)
2. Copy the bot token into your config
3. Set `allow_from` to your Telegram user ID for access control
4. Full guide: [docs/telegram.md](docs/telegram.md)

### Discord

1. Create an application at the [Discord Developer Portal](https://discord.com/developers/applications)
2. Create a bot, enable **Message Content Intent**, copy the token
3. Generate an invite URL with `bot` + `applications.commands` scope and at minimum: Send Messages, Create Public Threads, Read Message History, Embed Links, Use Slash Commands
4. Copy the bot token into your config
5. Full guide: [docs/discord.md](docs/discord.md) (includes auth, intents, invite link, systemd)

### Feishu / Lark

Why Feishu, vs Telegram/Discord:

- **No public URL / no webhook server.** It uses Feishu's WebSocket
  long-connection: agentbridge dials *out* to Feishu and receives events on
  that socket. Runs from a laptop or behind NAT with zero tunnelling — nothing
  to expose, no firewall rules, no ngrok.
- **Rich rendering.** Replies are sent as **interactive cards**, so the model's
  Markdown — tables, bold, lists, code blocks — renders natively. (On
  Discord/Telegram, Markdown tables don't render, so agentbridge falls back to
  aligned code-block tables.)
- **No app review to use it internally.** A custom app (企业自建应用) works
  inside your own tenant immediately after publishing a version — no store
  submission.
- **Native "working" feedback.** Since there's no bot typing API, agentbridge
  reacts 🫡 to your message on receipt and clears it when the reply lands.

Setup:

1. Create a **custom app** (企业自建应用) at the [Feishu Open Platform](https://open.feishu.cn) (or [Lark](https://open.larksuite.com))
2. **Add the Bot capability** to the app (添加应用能力 → 机器人)
3. **Permissions** (权限管理) — enable:
   - `im:message` — read messages sent to the bot
   - `im:message:send_as_bot` — send messages as the bot
4. **Event subscription** (事件与回调 → 事件配置):
   - Set the subscription mode to **「使用长连接接收事件」 (long-connection)**
     — ⚠️ agentbridge must be running for this option to save (it requires a
     live connection)
   - Add the event **接收消息 `im.message.receive_v1`**
5. **Publish a version** (版本管理与发布) so the bot capability + permissions take effect
6. Copy the **App ID** and **App Secret** (凭证与基础信息) into your config
7. **Add the bot to a group** — each group is one session, like a Discord channel
8. Use `/attach <tmux-session>` in the group to bind it to a Claude Code session

Notes:
- A "received, working on it" indicator is shown by reacting 🫡 to your message
  and removing it when the reply is sent (Feishu has no bot typing API).
- A Feishu app's long-connection is **cluster mode** — only one client per app
  receives each message. Don't point two tools (e.g. agentbridge and another
  bot) at the **same** app; give each its own app.

---

## License

MIT
