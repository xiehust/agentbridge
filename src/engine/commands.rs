//! Slash command handling for the new engine.
//!
//! Each command returns a plain `String` reply. The engine dispatches to
//! the appropriate function based on the command name and sends the result
//! back to the user.

use std::collections::HashMap;

use crate::agent::CONTINUE_SESSION;
use crate::config::{CommandConfig, ProjectConfig};
use crate::core::session::SessionManager;
use crate::cron::CronScheduler;

type OverrideMap = tokio::sync::Mutex<HashMap<(String, String), String>>;

// ---------------------------------------------------------------------------
// Available permission modes
// ---------------------------------------------------------------------------

const AVAILABLE_MODES: &[&str] = &["default", "yolo", "plan", "auto"];

/// Whether `args` names a recognised permission mode. Used by the engine to
/// decide if a `/mode` invocation actually changed anything (and therefore
/// requires an agent restart).
pub fn is_valid_mode(args: &str) -> bool {
    AVAILABLE_MODES.contains(&args)
}

// ---------------------------------------------------------------------------
// Command dispatcher
// ---------------------------------------------------------------------------

/// Dispatch a slash command and return the reply text.
///
/// Returns `None` if the command is not a recognised built-in, in which case
/// the caller should check custom commands or report "unknown command".
#[allow(clippy::too_many_arguments)] // command dispatch hub needs engine state refs; context struct would add churn
pub async fn dispatch(
    cmd: &str,
    args: &str,
    config: &ProjectConfig,
    sessions: &SessionManager,
    session_key: &str,
    agent_name: &str,
    model_override: &OverrideMap,
    mode_override: &OverrideMap,
    custom_commands: &[CommandConfig],
    cron_scheduler: &CronScheduler,
) -> Option<String> {
    match cmd {
        "help" | "start" => Some(cmd_help(custom_commands)),
        "status" => Some(
            cmd_status(config, sessions, session_key, agent_name, model_override, mode_override).await,
        ),
        "new" => Some(cmd_new(sessions, session_key, agent_name, args)),
        "list" => Some(cmd_list(sessions, session_key, agent_name)),
        "switch" => Some(cmd_switch(sessions, session_key, args)),
        "resume" => Some(cmd_resume(sessions, session_key, agent_name, args)),
        "current" => Some(cmd_current(sessions, session_key)),
        "delete" => Some(cmd_delete(sessions, session_key)),
        "model" => Some(cmd_model(config, session_key, agent_name, model_override, args).await),
        "mode" => Some(cmd_mode(config, session_key, agent_name, mode_override, args).await),
        "cron" => Some(cmd_cron(cron_scheduler, session_key, agent_name, args).await),
        "commands" => Some(cmd_list_commands(custom_commands)),
        "name" | "rename" => Some(cmd_name(sessions, session_key, args)),
        "history" => Some(cmd_history(sessions, session_key)),
        "dir" | "cd" => Some(cmd_dir(config, sessions, session_key, args)),
        "attach" => Some(cmd_attach(sessions, session_key, args).await),
        "sync" => Some(cmd_sync(config, args)),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Individual command implementations
// ---------------------------------------------------------------------------

fn cmd_help(custom_commands: &[CommandConfig]) -> String {
    let mut help = "\
/new [name] - New session\n\
/list - List sessions\n\
/switch <id> - Switch session\n\
/resume [id] - List sessions, or resume one\n\
/current - Current session info\n\
/delete - Delete current session\n\
/name [name] - Rename current session\n\
/stop - Interrupt current task (keeps the session)\n\
/verbose [on|off] - Toggle showing tool progress + thinking\n\
/compress - Compress context\n\
/history - Session history\n\
/btw <msg> - Inject mid-turn message\n\
/model [name] - Switch model\n\
/mode [mode] - Switch permission mode\n\
/agent [name] - Switch agent backend\n\
/cron add|list|del - Scheduled tasks\n\
/skills - List available skills\n\
/commands - List custom commands\n\
/dir [path] - View/change work directory\n\
/attach [tmux-session] - Bind channel to a tmux session (tmux backend)\n\
/sync [pull|push] - Sync session\n\
/status - Show status\n\
/help - Show help"
        .to_string();

    if !custom_commands.is_empty() {
        help.push_str("\n\nCustom commands:");
        for cmd in custom_commands {
            help.push_str(&format!("\n/{} - {}", cmd.name, cmd.description));
        }
    }

    help
}

async fn cmd_status(
    config: &ProjectConfig,
    sessions: &SessionManager,
    key: &str,
    agent_name: &str,
    model_override: &OverrideMap,
    mode_override: &OverrideMap,
) -> String {
    let session = sessions.get_or_create(key);
    let agent_entry = config.find_agent(agent_name);
    let agent_model = agent_entry.as_ref().and_then(|e| e.model.clone());
    let agent_mode = agent_entry
        .as_ref()
        .map(|e| e.mode.clone())
        .unwrap_or_else(|| config.agent.mode.clone());

    let effective_model = {
        let lock = model_override.lock().await;
        lock.get(&(key.to_string(), agent_name.to_string()))
            .cloned()
            .or(agent_model)
            .unwrap_or_else(|| "default".to_string())
    };
    let effective_mode = {
        let lock = mode_override.lock().await;
        lock.get(&(key.to_string(), agent_name.to_string()))
            .cloned()
            .unwrap_or(agent_mode)
    };
    format!(
        "Status\nProject: {}\nAgent: {}\nMode: {}\nModel: {}\nSession: {} ({})",
        config.name,
        agent_name,
        effective_mode,
        effective_model,
        &session.id[..8],
        session.name.as_deref().unwrap_or("unnamed"),
    )
}

fn cmd_new(sessions: &SessionManager, key: &str, agent_name: &str, args: &str) -> String {
    let name = if args.is_empty() {
        None
    } else {
        Some(args.to_string())
    };
    let session = sessions.new_session_with_agent(key, name, agent_name);
    format!("New session created: {}", &session.id[..8])
}

fn cmd_list(sessions: &SessionManager, key: &str, agent_name: &str) -> String {
    let list = sessions.list_for_agent(key, agent_name);
    if list.is_empty() {
        return "No sessions.".to_string();
    }

    let active = sessions.get_or_create(key);

    list.iter()
        .enumerate()
        .map(|(i, s)| {
            let name = s.name.as_deref().unwrap_or("unnamed");
            let marker = if s.id == active.id { " <-" } else { "" };
            let date = s.created_at.format("%m-%d %H:%M");
            format!("{}. {} [{}] {}{}", i + 1, name, &s.id[..8], date, marker)
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn cmd_switch(sessions: &SessionManager, key: &str, args: &str) -> String {
    if args.is_empty() {
        return "Usage: /switch <number or id>".to_string();
    }

    // Try as list number first
    if let Ok(num) = args.parse::<usize>() {
        let list = sessions.list(key);
        if num == 0 || num > list.len() {
            return format!("Invalid number. {} sessions total.", list.len());
        }
        let target = &list[num - 1];
        if let Some(s) = sessions.switch_session(key, &target.id) {
            return format!(
                "Switched to: {} [{}]",
                s.name.as_deref().unwrap_or("unnamed"),
                &s.id[..8]
            );
        }
    }

    // Try as ID prefix
    let list = sessions.list(key);
    let matches: Vec<_> = list.iter().filter(|s| s.id.starts_with(args)).collect();
    match matches.len() {
        0 => "No matching session found.".to_string(),
        1 => {
            if let Some(s) = sessions.switch_session(key, &matches[0].id) {
                format!(
                    "Switched to: {} [{}]",
                    s.name.as_deref().unwrap_or("unnamed"),
                    &s.id[..8]
                )
            } else {
                "Switch failed.".to_string()
            }
        }
        n => format!("Matched {} sessions, please provide a more specific ID.", n),
    }
}

/// `/resume` — mirrors Claude CLI's resume flow over agentbridge's session
/// model. With no argument it lists sessions (so the user can pick one); with
/// an argument it switches to that session, identical to `/switch`.
fn cmd_resume(sessions: &SessionManager, key: &str, agent_name: &str, args: &str) -> String {
    if args.is_empty() {
        let list = sessions.list_for_agent(key, agent_name);
        if list.is_empty() {
            return "No sessions to resume. Use /new to start one.".to_string();
        }
        return format!(
            "{}\n\nResume one with /resume <number or id>.",
            cmd_list(sessions, key, agent_name)
        );
    }
    cmd_switch(sessions, key, args)
}

fn cmd_current(sessions: &SessionManager, key: &str) -> String {
    let session = sessions.get_or_create(key);
    format!(
        "Current session\nID: {}\nName: {}\nCreated: {}",
        &session.id[..8],
        session.name.as_deref().unwrap_or("unnamed"),
        session.created_at.format("%Y-%m-%d %H:%M"),
    )
}

fn cmd_delete(sessions: &SessionManager, key: &str) -> String {
    let current = sessions.get_or_create(key);
    let old_id = current.id.clone();
    sessions.delete_session(key, &old_id);
    let new_session = sessions.get_or_create(key);
    format!(
        "Deleted session [{}]\nNew session: [{}]",
        &old_id[..8],
        &new_session.id[..8]
    )
}

async fn cmd_model(
    config: &ProjectConfig,
    session_key: &str,
    agent_name: &str,
    model_override: &OverrideMap,
    args: &str,
) -> String {
    let default_model = config
        .find_agent(agent_name)
        .and_then(|e| e.model)
        .or_else(|| config.agent.model.clone());
    let map_key = (session_key.to_string(), agent_name.to_string());

    if args.is_empty() {
        let lock = model_override.lock().await;
        let override_val = lock.get(&map_key).cloned();
        let effective = override_val
            .clone()
            .or(default_model)
            .unwrap_or_else(|| "default".to_string());
        let source = if override_val.is_some() {
            "override"
        } else {
            "config"
        };
        format!("Current model: {} ({}) [agent: {}]", effective, source, agent_name)
    } else {
        let new_model = args.to_string();
        let mut lock = model_override.lock().await;
        lock.insert(map_key, new_model.clone());
        format!(
            "Model set to: {} [agent: {}]\nTakes effect on next message.",
            new_model, agent_name
        )
    }
}

async fn cmd_mode(
    config: &ProjectConfig,
    session_key: &str,
    agent_name: &str,
    mode_override: &OverrideMap,
    args: &str,
) -> String {
    let default_mode = config
        .find_agent(agent_name)
        .map(|e| e.mode)
        .unwrap_or_else(|| config.agent.mode.clone());
    let map_key = (session_key.to_string(), agent_name.to_string());

    if args.is_empty() {
        let lock = mode_override.lock().await;
        let override_val = lock.get(&map_key).cloned();
        let effective = override_val.clone().unwrap_or_else(|| default_mode.clone());
        let source = if override_val.is_some() {
            "override"
        } else {
            "config"
        };
        let modes_list = AVAILABLE_MODES.join(", ");
        format!(
            "Current mode: {} ({}) [agent: {}]\nAvailable: {}",
            effective, source, agent_name, modes_list
        )
    } else {
        if !AVAILABLE_MODES.contains(&args) {
            let modes_list = AVAILABLE_MODES.join(", ");
            return format!("Unknown mode: {}\nAvailable: {}", args, modes_list);
        }
        let mut lock = mode_override.lock().await;
        lock.insert(map_key, args.to_string());
        format!(
            "Mode set to: {} [agent: {}]\nTakes effect on next message.",
            args, agent_name
        )
    }
}

async fn cmd_cron(
    scheduler: &CronScheduler,
    session_key: &str,
    agent_name: &str,
    args: &str,
) -> String {
    let parts: Vec<&str> = args.splitn(2, char::is_whitespace).collect();
    let sub_cmd = parts.first().copied().unwrap_or("");
    let sub_args = parts.get(1).copied().unwrap_or("").trim();

    match sub_cmd {
        "add" => {
            // Expected format: /cron add <min> <hour> <day> <month> <weekday> <prompt>
            let tokens: Vec<&str> = sub_args.splitn(6, char::is_whitespace).collect();
            if tokens.len() < 6 {
                return "Usage: /cron add <min> <hour> <day> <month> <weekday> <prompt>\n\
                        Example: /cron add 0 6 * * * Good morning summary"
                    .to_string();
            }
            let cron_expr = format!(
                "{} {} {} {} {} *",
                tokens[0], tokens[1], tokens[2], tokens[3], tokens[4]
            );
            let prompt = tokens[5].to_string();
            // Char-boundary-safe truncation: a raw &prompt[..30] panics when a
            // CJK cron description is cut inside a multibyte char.
            let description = if prompt.chars().count() > 30 {
                format!("{}...", prompt.chars().take(30).collect::<String>())
            } else {
                prompt.clone()
            };

            match scheduler
                .add_job(
                    cron_expr.clone(),
                    prompt,
                    description,
                    session_key.to_string(),
                    agent_name.to_string(),
                )
                .await
            {
                Ok(job) => format!(
                    "Cron job added:\nID: {}\nSchedule: {}\nPrompt: {}",
                    job.id, job.cron_expr, job.prompt
                ),
                Err(e) => format!("Failed to add cron job: {}", e),
            }
        }
        "list" => {
            let jobs = scheduler.list_jobs().await;
            if jobs.is_empty() {
                return "No cron jobs configured.".to_string();
            }
            jobs.iter()
                .enumerate()
                .map(|(i, j)| {
                    let status = if j.enabled { "ON" } else { "OFF" };
                    format!(
                        "{}. [{}] {} | {} | {}",
                        i + 1,
                        j.id,
                        status,
                        j.cron_expr,
                        j.description
                    )
                })
                .collect::<Vec<_>>()
                .join("\n")
        }
        "del" | "delete" | "rm" => {
            let id = sub_args.trim();
            if id.is_empty() {
                return "Usage: /cron del <id>".to_string();
            }
            if scheduler.delete_job(id).await {
                format!("Cron job {} deleted.", id)
            } else {
                format!("Cron job {} not found.", id)
            }
        }
        _ => "Usage: /cron add|list|del\n\
              /cron add <min> <hour> <day> <month> <weekday> <prompt>\n\
              /cron list\n\
              /cron del <id>"
            .to_string(),
    }
}

fn cmd_list_commands(custom_commands: &[CommandConfig]) -> String {
    if custom_commands.is_empty() {
        return "No custom commands configured.\n\
                Define commands in config.yaml under 'commands:'."
            .to_string();
    }
    let mut result = "Custom commands:\n".to_string();
    for cmd in custom_commands {
        result.push_str(&format!("/{} - {}\n", cmd.name, cmd.description));
    }
    result
}

fn cmd_history(sessions: &SessionManager, key: &str) -> String {
    let all = sessions.list(key);
    if all.is_empty() {
        return "No sessions found.".to_string();
    }
    let active_id = {
        let current = sessions.get_or_create(key);
        current.id.clone()
    };
    let mut lines = vec!["Session history:".to_string()];
    for (i, s) in all.iter().enumerate() {
        let marker = if s.id == active_id { "▶" } else { "◻" };
        let name = s.name.as_deref().unwrap_or("unnamed");
        let agent_id = s.agent_session_id.as_deref().unwrap_or("-");
        let short_id = if agent_id.len() > 8 { &agent_id[..8] } else { agent_id };
        lines.push(format!(
            "{} {}. {} | {} | agent:{}",
            marker, i + 1, name, s.updated_at.format("%m-%d %H:%M"), short_id
        ));
    }
    lines.join("\n")
}

fn cmd_name(sessions: &SessionManager, key: &str, args: &str) -> String {
    let name = args.trim();
    if name.is_empty() {
        let session = sessions.get_or_create(key);
        let current = session.name.as_deref().unwrap_or("(unnamed)");
        return format!("Current session name: {}\nUsage: /name <new name>", current);
    }
    if sessions.rename_session(key, name) {
        format!("Session renamed to: {}", name)
    } else {
        "No active session to rename.".to_string()
    }
}

fn cmd_dir(
    config: &ProjectConfig,
    sessions: &SessionManager,
    key: &str,
    args: &str,
) -> String {
    if args.is_empty() {
        let session_dir = sessions.get_work_dir(key);
        let effective = session_dir
            .as_deref()
            .unwrap_or(config.work_dir.to_str().unwrap_or("."));
        let source = if session_dir.is_some() {
            "session override"
        } else {
            "config default"
        };
        return format!("Work directory: {}\n(source: {})", effective, source);
    }

    if args == "reset" {
        sessions.set_work_dir(key, config.work_dir.to_str().unwrap_or("."));
        // Use __continue__ to resume the most recent session in the target directory
        sessions.set_agent_session_id(key, CONTINUE_SESSION);
        return format!("📁 已切换回默认目录: {}", config.work_dir.display());
    }

    let path = std::path::Path::new(args);
    let resolved = if path.is_absolute() {
        path.to_path_buf()
    } else {
        let base = sessions
            .get_work_dir(key)
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| config.work_dir.clone());
        base.join(path)
    };

    if !resolved.exists() {
        return format!("Path does not exist: {}", resolved.display());
    }

    if !resolved.is_dir() {
        return format!("Not a directory: {}", resolved.display());
    }

    let canonical = resolved.canonicalize().unwrap_or(resolved);
    sessions.set_work_dir(key, canonical.to_str().unwrap_or("."));
    // Use __continue__ to resume the most recent session in the target directory
    // (--continue --fork-session picks up the latest session in that workspace)
    sessions.set_agent_session_id(key, CONTINUE_SESSION);
    format!("📁 已切换到: {}", canonical.display())
}

/// `/attach [tmux-session-name]` — bind THIS channel to a specific tmux session
/// (tmux backend). With no argument, lists the running tmux sessions so the user
/// can pick one. Different channels can `/attach` different sessions, letting one
/// bot drive several hand-started `cc` sessions concurrently.
async fn cmd_attach(sessions: &SessionManager, key: &str, args: &str) -> String {
    let target = args.trim();
    if target.is_empty() {
        let current = sessions
            .get_tmux_session(key)
            .map(|s| format!("\n当前已绑定: {}", s))
            .unwrap_or_default();
        return match list_tmux_sessions().await {
            Some(list) if !list.is_empty() => format!(
                "可 attach 的 tmux 会话:\n{}\n\n用 /attach <会话名> 绑定本频道。{}",
                list.join("\n"),
                current
            ),
            Some(_) => format!("没有运行中的 tmux 会话。{}", current),
            None => format!("无法列出 tmux 会话(tmux 未运行?)。{}", current),
        };
    }
    sessions.set_tmux_session(key, target);
    // Drop the current session id so the next message attaches fresh.
    sessions.set_agent_session_id(key, "");
    format!("🔗 本频道已绑定 tmux 会话: {}", target)
}

/// List running tmux session names via `tmux ls`. Returns None if tmux is
/// unavailable or no server is running.
async fn list_tmux_sessions() -> Option<Vec<String>> {
    let output = tokio::process::Command::new("tmux")
        .args(["list-sessions", "-F", "#{session_name}"])
        .output()
        .await
        .ok()?;
    if !output.status.success() {
        return Some(Vec::new());
    }
    let names: Vec<String> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect();
    Some(names)
}

fn cmd_sync(config: &ProjectConfig, args: &str) -> String {
    let sync_config = match &config.sync {
        Some(c) => c,
        None => {
            return "Sync not configured.\n\
                    Add to config.yaml:\n\n\
                    sync:\n  remote: \"your-mac:~/.claude/\""
                .to_string()
        }
    };

    let direction = match args.trim() {
        "" | "pull" => crate::sync::Direction::Pull,
        "push" => crate::sync::Direction::Push,
        other => return format!("Unknown sync direction: {}\nUsage: /sync [pull|push]", other),
    };

    match crate::sync::run_sync(sync_config, direction) {
        Ok(result) => result.to_string(),
        Err(e) => format!("Sync failed: {}", e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_manager() -> (TempDir, SessionManager) {
        let tmp = TempDir::new().unwrap();
        let mgr = SessionManager::new(tmp.path(), tmp.path());
        (tmp, mgr)
    }

    #[tokio::test]
    async fn attach_binds_channel_to_named_session() {
        let (_tmp, mgr) = test_manager();
        let key = "discord:chanA";
        let reply = cmd_attach(&mgr, key, "nova-bidding").await;
        assert!(reply.contains("nova-bidding"), "got: {reply}");
        assert_eq!(mgr.get_tmux_session(key).as_deref(), Some("nova-bidding"));
    }

    #[tokio::test]
    async fn attach_different_channels_bind_different_sessions() {
        let (_tmp, mgr) = test_manager();
        cmd_attach(&mgr, "discord:A", "proj-a").await;
        cmd_attach(&mgr, "discord:B", "proj-b").await;
        assert_eq!(mgr.get_tmux_session("discord:A").as_deref(), Some("proj-a"));
        assert_eq!(mgr.get_tmux_session("discord:B").as_deref(), Some("proj-b"));
    }

    #[test]
    fn resume_no_args_no_sessions_prompts_new() {
        let (_tmp, mgr) = test_manager();
        let reply = cmd_resume(&mgr, "discord:chan", "claude", "");
        assert!(reply.contains("No sessions"), "got: {reply}");
        assert!(reply.contains("/new"));
    }

    #[test]
    fn resume_no_args_lists_sessions_with_hint() {
        let (_tmp, mgr) = test_manager();
        mgr.new_session_with_agent("discord:chan", Some("first".into()), "claude");
        let reply = cmd_resume(&mgr, "discord:chan", "claude", "");
        assert!(reply.contains("first"), "got: {reply}");
        assert!(reply.contains("/resume <number or id>"), "got: {reply}");
    }

    #[test]
    fn resume_with_id_prefix_switches_session() {
        let (_tmp, mgr) = test_manager();
        let s1 = mgr.new_session_with_agent("discord:chan", Some("one".into()), "claude");
        // Make "two" the active session, then resume back to s1 by ID prefix.
        mgr.new_session_with_agent("discord:chan", Some("two".into()), "claude");
        let reply = cmd_resume(&mgr, "discord:chan", "claude", &s1.id[..8]);
        assert!(reply.contains("Switched to"), "got: {reply}");
        assert_eq!(mgr.get_or_create("discord:chan").id, s1.id);
    }

    #[test]
    fn valid_modes_recognised() {
        for m in AVAILABLE_MODES {
            assert!(is_valid_mode(m), "{m} should be valid");
        }
        assert!(!is_valid_mode(""));
        assert!(!is_valid_mode("bogus"));
    }

    #[tokio::test]
    async fn model_override_set_and_reported() {
        let yaml = r#"
name: t
work_dir: /tmp
agents:
  - name: claude
    backend: claude
"#;
        let config: ProjectConfig = serde_yaml::from_str(yaml).unwrap();
        let overrides: OverrideMap = tokio::sync::Mutex::new(HashMap::new());
        let set_reply =
            cmd_model(&config, "discord:chan", "claude", &overrides, "claude-5").await;
        assert!(set_reply.contains("claude-5"), "got: {set_reply}");
        let show_reply = cmd_model(&config, "discord:chan", "claude", &overrides, "").await;
        assert!(show_reply.contains("claude-5"), "got: {show_reply}");
        assert!(show_reply.contains("override"), "got: {show_reply}");
    }
}

