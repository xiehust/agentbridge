//! Non-blocking session management with try-lock and message queuing.
//!
//! Callers attempt to acquire a session lock without blocking. If the session
//! is busy, messages are queued and drained when the lock is released.

#![allow(dead_code)] // InteractiveState / SessionGuard are used by tests and reserved for future queue use

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use super::message::{FileAttachment, ImageAttachment};
use super::platform::ReplyCtx;

// ---------------------------------------------------------------------------
// Session
// ---------------------------------------------------------------------------

/// A chat session with an atomic busy flag for non-blocking lock acquisition.
#[derive(Debug, Serialize, Deserialize)]
pub struct Session {
    pub id: String,
    pub name: Option<String>,
    pub agent_session_id: Option<String>,
    #[serde(default)]
    pub agent_type: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub work_dir: Option<String>,
    /// tmux backend: an explicit session name this channel attaches to
    /// (set via `/attach`). Lets different channels drive different tmux
    /// sessions. None → fall back to config/derived name.
    #[serde(default)]
    pub tmux_session: Option<String>,
    #[serde(skip)]
    busy: AtomicBool,
}

impl Session {
    /// Create a new session with a random UUID.
    pub fn new(name: Option<String>) -> Self {
        Self::new_with_agent_type(name, String::new())
    }

    pub fn new_with_agent_type(name: Option<String>, agent_type: String) -> Self {
        let now = Utc::now();
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            name,
            agent_session_id: None,
            agent_type,
            created_at: now,
            updated_at: now,
            work_dir: None,
            tmux_session: None,
            busy: AtomicBool::new(false),
        }
    }

    pub fn effective_agent_type(&self) -> &str {
        if self.agent_type.is_empty() {
            "claude"
        } else {
            &self.agent_type
        }
    }

    /// Attempt to acquire the session lock without blocking.
    ///
    /// Returns `Some(SessionGuard)` if the lock was acquired, or `None` if
    /// the session is already busy.
    pub fn try_lock(&self) -> Option<SessionGuard<'_>> {
        if self
            .busy
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            Some(SessionGuard { session: self })
        } else {
            None
        }
    }

    /// Check whether the session is currently locked.
    pub fn is_busy(&self) -> bool {
        self.busy.load(Ordering::Relaxed)
    }

    /// Explicitly release the session lock.
    ///
    /// Unlike the RAII `SessionGuard`, this can be called at a precise point
    /// while other state is held, which is critical for the drain-loop race
    /// guard: we unlock the session while holding the interactive-state mutex
    /// so that no message can be queued between "queue is empty" and "session
    /// unlocked".
    pub fn unlock(&self) {
        self.busy.store(false, Ordering::Release);
    }

    /// Explicitly set the session as busy (locked).
    ///
    /// Returns `true` if the lock was acquired, `false` if already busy.
    /// This is the same CAS as `try_lock` but without creating a guard.
    pub fn try_lock_explicit(&self) -> bool {
        self.busy
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }
}

// ---------------------------------------------------------------------------
// SessionGuard (RAII unlock)
// ---------------------------------------------------------------------------

/// RAII guard that releases the session lock on drop.
///
/// Callers should update `session.updated_at` before dropping if the session
/// was mutated.
pub struct SessionGuard<'a> {
    session: &'a Session,
}

impl SessionGuard<'_> {
    /// Borrow the underlying session.
    pub fn session(&self) -> &Session {
        self.session
    }
}

impl Drop for SessionGuard<'_> {
    fn drop(&mut self) {
        self.session.busy.store(false, Ordering::Release);
    }
}

// ---------------------------------------------------------------------------
// QueuedMessage
// ---------------------------------------------------------------------------

/// A message waiting for a busy session to become available.
pub struct QueuedMessage {
    pub text: String,
    pub images: Vec<ImageAttachment>,
    pub files: Vec<FileAttachment>,
    pub voice: Option<String>,
    pub from: String,
    pub reply_ctx: Box<dyn ReplyCtx>,
}

// ---------------------------------------------------------------------------
// InteractiveState
// ---------------------------------------------------------------------------

/// Per-session state for active conversations, including a bounded message
/// queue for messages that arrive while the session is busy.
pub struct InteractiveState {
    pub session: Arc<Session>,
    pub pending_messages: VecDeque<QueuedMessage>,
}

/// Maximum number of messages that can be queued per session.
const MAX_QUEUED_MESSAGES: usize = 5;

impl InteractiveState {
    /// Create a new interactive state wrapping a session.
    pub fn new(session: Arc<Session>) -> Self {
        Self {
            session,
            pending_messages: VecDeque::new(),
        }
    }

    /// Queue a message for later processing.
    ///
    /// Returns `true` if the message was queued, `false` if the queue is full.
    pub fn queue_message(&mut self, msg: QueuedMessage) -> bool {
        if self.pending_messages.len() >= MAX_QUEUED_MESSAGES {
            return false;
        }
        self.pending_messages.push_back(msg);
        true
    }

    /// Drain the next queued message, if any.
    pub fn drain_next(&mut self) -> Option<QueuedMessage> {
        self.pending_messages.pop_front()
    }

    /// Number of messages currently in the queue.
    pub fn queue_len(&self) -> usize {
        self.pending_messages.len()
    }
}

// ---------------------------------------------------------------------------
// SessionManager
// ---------------------------------------------------------------------------

/// Persistent session state serialized to/from JSON.
#[derive(Debug, Default, Serialize, Deserialize)]
struct PersistedState {
    sessions: HashMap<String, SessionData>,
    active_keys: HashMap<String, String>, // session_key -> session_id
}

/// Serializable session data (without the atomic busy flag).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SessionData {
    id: String,
    key: String,
    name: Option<String>,
    agent_session_id: Option<String>,
    #[serde(default)]
    agent_type: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    work_dir: Option<String>,
    #[serde(default)]
    tmux_session: Option<String>,
}

impl SessionData {
    fn into_session(self) -> Session {
        Session {
            id: self.id,
            name: self.name,
            agent_session_id: self.agent_session_id,
            agent_type: self.agent_type,
            created_at: self.created_at,
            updated_at: self.updated_at,
            work_dir: self.work_dir,
            tmux_session: self.tmux_session,
            busy: AtomicBool::new(false),
        }
    }

    fn from_session(session: &Session, key: &str) -> Self {
        Self {
            id: session.id.clone(),
            key: key.to_string(),
            name: session.name.clone(),
            agent_session_id: session.agent_session_id.clone(),
            agent_type: session.agent_type.clone(),
            created_at: session.created_at,
            updated_at: session.updated_at,
            work_dir: session.work_dir.clone(),
            tmux_session: session.tmux_session.clone(),
        }
    }
}

/// CRUD session manager with JSON persistence and try-lock support.
pub struct SessionManager {
    data_dir: PathBuf,
    sessions: Mutex<HashMap<String, Arc<Session>>>,
    /// Maps a session key (e.g. "user:alice") to its active session ID.
    active_keys: Mutex<HashMap<String, String>>,
    /// Reverse map: session_id -> key, for persistence.
    session_keys: Mutex<HashMap<String, String>>,
}

impl SessionManager {
    /// Create a new session manager rooted at `base_dir/sessions/<encoded_work_dir>`.
    ///
    /// The work_dir is encoded the same way Claude Code encodes project paths
    /// (e.g. `/home/ubuntu/agentbridge` → `-home-ubuntu-agentbridge`), ensuring
    /// each work directory gets its own isolated session storage.
    ///
    /// `work_dir` is canonicalized first so that equivalent paths (relative,
    /// symlinked, trailing slash) always map to the same on-disk state file.
    /// If canonicalization fails (e.g. the directory does not yet exist) we
    /// fall back to the raw path — this preserves backwards-compatibility with
    /// previously-written `state.json` locations.
    pub fn new(base_dir: &Path, work_dir: &Path) -> Self {
        let resolved = work_dir
            .canonicalize()
            .unwrap_or_else(|_| work_dir.to_path_buf());
        let encoded = resolved.to_string_lossy().replace('/', "-");
        let data_dir = base_dir.join("sessions").join(&encoded);
        fs::create_dir_all(&data_dir).ok();

        tracing::debug!(
            work_dir = %resolved.display(),
            state_file = %data_dir.join("state.json").display(),
            "session manager initialized",
        );

        let mut mgr = Self {
            data_dir,
            sessions: Mutex::new(HashMap::new()),
            active_keys: Mutex::new(HashMap::new()),
            session_keys: Mutex::new(HashMap::new()),
        };
        mgr.load_state();
        mgr
    }

    /// Get the active session for a key, or create a new one.
    pub fn get_or_create(&self, key: &str) -> Arc<Session> {
        let active = self.active_keys.lock().unwrap();
        if let Some(session_id) = active.get(key).cloned() {
            drop(active);
            let sessions = self.sessions.lock().unwrap();
            if let Some(session) = sessions.get(&session_id) {
                return Arc::clone(session);
            }
        } else {
            drop(active);
        }

        // Create a new session
        self.new_session(key, None)
    }

    /// Create a brand-new session for a key, making it the active session.
    ///
    /// Inherits the `work_dir` from the previous active session for this key
    /// so that per-thread directory overrides survive session resets.
    pub fn new_session(&self, key: &str, name: Option<String>) -> Arc<Session> {
        self.new_session_with_agent(key, name, "")
    }

    pub fn new_session_with_agent(
        &self,
        key: &str,
        name: Option<String>,
        agent_type: &str,
    ) -> Arc<Session> {
        let prev_work_dir = self.get_work_dir(key);

        let mut session = Session::new_with_agent_type(name, agent_type.to_string());
        session.work_dir = prev_work_dir;
        let session = Arc::new(session);
        let id = session.id.clone();

        let mut sessions = self.sessions.lock().unwrap();
        sessions.insert(id.clone(), Arc::clone(&session));
        drop(sessions);

        let mut active = self.active_keys.lock().unwrap();
        active.insert(key.to_string(), id.clone());
        drop(active);

        let mut keys = self.session_keys.lock().unwrap();
        keys.insert(id, key.to_string());
        drop(keys);

        self.persist();
        session
    }

    /// List every `(session_key, currently-active session)` pair.
    ///
    /// Unlike [`list_all`] this only returns the session each key is
    /// currently pointing at (via `active_keys`), so callers can restore
    /// per-key state (e.g. last-used agent) after a restart without
    /// accidentally resurrecting historical sessions.
    pub fn active_sessions(&self) -> Vec<(String, Arc<Session>)> {
        let active = self.active_keys.lock().unwrap();
        let sessions = self.sessions.lock().unwrap();
        active
            .iter()
            .filter_map(|(key, session_id)| {
                sessions
                    .get(session_id)
                    .map(|s| (key.clone(), Arc::clone(s)))
            })
            .collect()
    }

    /// List every (session_key, session) pair. Used for cross-platform
    /// bookkeeping such as backfilling session names on startup.
    pub fn list_all(&self) -> Vec<(String, Arc<Session>)> {
        let sessions = self.sessions.lock().unwrap();
        let keys = self.session_keys.lock().unwrap();
        sessions
            .iter()
            .filter_map(|(id, s)| keys.get(id).map(|k| (k.clone(), Arc::clone(s))))
            .collect()
    }

    pub fn list_for_agent(&self, key: &str, agent_type: &str) -> Vec<Arc<Session>> {
        let sessions = self.sessions.lock().unwrap();
        let keys = self.session_keys.lock().unwrap();

        let mut result: Vec<Arc<Session>> = sessions
            .values()
            .filter(|s| {
                keys.get(&s.id).map(|k| k.as_str()) == Some(key)
                    && s.effective_agent_type() == agent_type
            })
            .map(Arc::clone)
            .collect();

        result.sort_by_key(|s| std::cmp::Reverse(s.updated_at));
        result
    }

    /// List all sessions belonging to a key, most recently updated first.
    pub fn list(&self, key: &str) -> Vec<Arc<Session>> {
        let sessions = self.sessions.lock().unwrap();
        let keys = self.session_keys.lock().unwrap();

        let mut result: Vec<Arc<Session>> = sessions
            .values()
            .filter(|s| keys.get(&s.id).map(|k| k.as_str()) == Some(key))
            .map(Arc::clone)
            .collect();

        result.sort_by_key(|s| std::cmp::Reverse(s.updated_at));
        result
    }

    /// Switch the active session for a key. Returns `None` if the session
    /// does not exist or does not belong to the given key.
    pub fn switch_session(&self, key: &str, session_id: &str) -> Option<Arc<Session>> {
        let sessions = self.sessions.lock().unwrap();
        let session = sessions.get(session_id)?;

        let keys = self.session_keys.lock().unwrap();
        if keys.get(session_id).map(|k| k.as_str()) != Some(key) {
            return None;
        }
        let result = Arc::clone(session);
        drop(keys);
        drop(sessions);

        let mut active = self.active_keys.lock().unwrap();
        active.insert(key.to_string(), session_id.to_string());
        drop(active);

        self.persist();
        Some(result)
    }

    /// Delete a session. If it was the active session for its key, the active
    /// mapping is removed.
    pub fn delete_session(&self, key: &str, session_id: &str) {
        let mut sessions = self.sessions.lock().unwrap();
        sessions.remove(session_id);
        drop(sessions);

        let mut active = self.active_keys.lock().unwrap();
        if active.get(key).map(|s| s.as_str()) == Some(session_id) {
            active.remove(key);
        }
        drop(active);

        let mut keys = self.session_keys.lock().unwrap();
        keys.remove(session_id);
        drop(keys);

        self.persist();
    }

    /// Set the work directory override for the active session of a key.
    pub fn set_work_dir(&self, key: &str, work_dir: &str) {
        // Ensure a session exists for this key first. Without this, `/dir` as
        // the very FIRST message in a channel was a silent no-op: there was no
        // active session yet, so the directory was dropped and the next message
        // spawned the agent in the default work_dir instead.
        let _ = self.get_or_create(key);

        let active = self.active_keys.lock().unwrap();
        if let Some(session_id) = active.get(key).cloned() {
            drop(active);
            let sessions = self.sessions.lock().unwrap();
            if let Some(session) = sessions.get(&session_id) {
                // Safety: we need interior mutability for work_dir. Since Session
                // is behind Arc we rebuild the persisted state from SessionData.
                // For the in-memory Arc<Session>, consumers should re-fetch after
                // calling set_work_dir.
                let _ = session; // acknowledge; actual mutation via persist round-trip
                drop(sessions);

                // Rebuild the session with the new work_dir
                let mut all = self.sessions.lock().unwrap();
                if let Some(old) = all.remove(&session_id) {
                    let keys = self.session_keys.lock().unwrap();
                    let key_str = keys
                        .get(&session_id)
                        .cloned()
                        .unwrap_or_else(|| key.to_string());
                    drop(keys);

                    let new_session = Arc::new(Session {
                        id: old.id.clone(),
                        name: old.name.clone(),
                        agent_session_id: old.agent_session_id.clone(),
                        agent_type: old.agent_type.clone(),
                        created_at: old.created_at,
                        updated_at: Utc::now(),
                        work_dir: Some(work_dir.to_string()),
                        tmux_session: old.tmux_session.clone(),
                        busy: AtomicBool::new(false),
                    });
                    all.insert(session_id.clone(), new_session);
                    drop(all);

                    let mut sk = self.session_keys.lock().unwrap();
                    sk.insert(session_id, key_str);
                    drop(sk);
                }

                self.persist();
            }
        }
    }

    /// Get the work directory for the active session of a key.
    pub fn get_work_dir(&self, key: &str) -> Option<String> {
        let active = self.active_keys.lock().unwrap();
        let session_id = active.get(key)?;
        let sessions = self.sessions.lock().unwrap();
        sessions.get(session_id)?.work_dir.clone()
    }

    /// Get the explicit tmux session name bound to a key (via `/attach`).
    pub fn get_tmux_session(&self, key: &str) -> Option<String> {
        let active = self.active_keys.lock().unwrap();
        let session_id = active.get(key)?;
        let sessions = self.sessions.lock().unwrap();
        sessions.get(session_id)?.tmux_session.clone()
    }

    /// Bind a key to an explicit tmux session name (via `/attach`). Ensures a
    /// session exists first (mirrors set_work_dir, so `/attach` works as a
    /// channel's very first command). Rebuilds the session in place.
    pub fn set_tmux_session(&self, key: &str, tmux_session: &str) {
        let _ = self.get_or_create(key);
        let active = self.active_keys.lock().unwrap();
        if let Some(session_id) = active.get(key).cloned() {
            drop(active);
            let mut all = self.sessions.lock().unwrap();
            if let Some(old) = all.remove(&session_id) {
                let new_session = Arc::new(Session {
                    id: old.id.clone(),
                    name: old.name.clone(),
                    agent_session_id: old.agent_session_id.clone(),
                    agent_type: old.agent_type.clone(),
                    created_at: old.created_at,
                    updated_at: Utc::now(),
                    work_dir: old.work_dir.clone(),
                    tmux_session: Some(tmux_session.to_string()),
                    busy: AtomicBool::new(false),
                });
                all.insert(session_id, new_session);
                drop(all);
                self.persist();
            }
        }
    }

    /// Rename the active session for a key.
    pub fn rename_session(&self, key: &str, new_name: &str) -> bool {
        let active = self.active_keys.lock().unwrap();
        if let Some(session_id) = active.get(key).cloned() {
            drop(active);
            let mut all = self.sessions.lock().unwrap();
            if let Some(old) = all.remove(&session_id) {
                let new_session = Arc::new(Session {
                    id: old.id.clone(),
                    name: Some(new_name.to_string()),
                    agent_session_id: old.agent_session_id.clone(),
                    agent_type: old.agent_type.clone(),
                    created_at: old.created_at,
                    updated_at: old.updated_at,
                    work_dir: old.work_dir.clone(),
                    tmux_session: old.tmux_session.clone(),
                    busy: AtomicBool::new(old.is_busy()),
                });
                all.insert(session_id, new_session);
                drop(all);
                self.persist();
                return true;
            }
        }
        false
    }

    /// Get the agent session ID for the active session of a key.
    pub fn get_agent_session_id(&self, key: &str) -> Option<String> {
        let active = self.active_keys.lock().unwrap();
        let session_id = active.get(key)?;
        let sessions = self.sessions.lock().unwrap();
        sessions.get(session_id)?.agent_session_id.clone()
    }

    /// Set the agent session ID for the active session of a key (for resume).
    /// Empty string is treated as None (no session to resume).
    pub fn set_agent_session_id(&self, key: &str, agent_sid: &str) {
        let active = self.active_keys.lock().unwrap();
        if let Some(session_id) = active.get(key).cloned() {
            drop(active);
            let sessions = self.sessions.lock().unwrap();
            if sessions.contains_key(&session_id) {
                drop(sessions);
                // Rebuild session with updated agent_session_id
                let mut all = self.sessions.lock().unwrap();
                if let Some(old) = all.remove(&session_id) {
                    let sid_value = if agent_sid.is_empty() {
                        None
                    } else {
                        Some(agent_sid.to_string())
                    };
                    let new_session = Arc::new(Session {
                        id: old.id.clone(),
                        name: old.name.clone(),
                        agent_session_id: sid_value,
                        agent_type: old.agent_type.clone(),
                        created_at: old.created_at,
                        updated_at: Utc::now(),
                        work_dir: old.work_dir.clone(),
                        tmux_session: old.tmux_session.clone(),
                        busy: AtomicBool::new(false),
                    });
                    all.insert(session_id, new_session);
                    drop(all);
                }
                self.persist();
            }
        }
    }

    /// Save current state to `state.json`.
    ///
    /// The `__continue__` sentinel is never persisted — it is only used
    /// transiently when starting an agent. Storing it on disk would cause
    /// the agent to fork a random CLI session on every restart.
    /// (The __continue__ sentinel is stripped before persisting.)
    pub fn persist(&self) {
        let sessions = self.sessions.lock().unwrap();
        let active_keys = self.active_keys.lock().unwrap();
        let session_keys = self.session_keys.lock().unwrap();

        let mut data_sessions = HashMap::new();
        for (id, session) in sessions.iter() {
            let key = session_keys
                .get(id)
                .cloned()
                .unwrap_or_default();
            let mut sd = SessionData::from_session(session, &key);
            // Strip __continue__ sentinel before persisting
            if sd.agent_session_id.as_deref() == Some("__continue__") {
                sd.agent_session_id = None;
            }
            data_sessions.insert(id.clone(), sd);
        }

        let state = PersistedState {
            sessions: data_sessions,
            active_keys: active_keys.clone(),
        };

        let path = self.data_dir.join("state.json");
        if let Ok(data) = serde_json::to_string_pretty(&state) {
            fs::write(path, data).ok();
        }
    }

    /// Load state from `state.json`.
    fn load_state(&mut self) {
        let path = self.data_dir.join("state.json");
        if !path.exists() {
            return;
        }

        let content = match fs::read_to_string(&path) {
            Ok(c) => c,
            Err(_) => return,
        };

        let state: PersistedState = match serde_json::from_str(&content) {
            Ok(s) => s,
            Err(_) => return,
        };

        let mut sessions = HashMap::new();
        let mut session_keys = HashMap::new();

        for (id, mut data) in state.sessions {
            let key = data.key.clone();
            // Strip __continue__ sentinel on load so the agent resumes by ID
            // rather than re-entering "continue most recent" mode.
            if data.agent_session_id.as_deref() == Some("__continue__") {
                data.agent_session_id = None;
            }
            let session = Arc::new(data.into_session());
            sessions.insert(id.clone(), session);
            session_keys.insert(id, key);
        }

        *self.sessions.lock().unwrap() = sessions;
        *self.active_keys.lock().unwrap() = state.active_keys;
        *self.session_keys.lock().unwrap() = session_keys;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    // -- Session try_lock tests --

    #[test]
    fn try_lock_succeeds_when_not_busy() {
        let session = Session::new(None);
        assert!(!session.is_busy());

        let guard = session.try_lock();
        assert!(guard.is_some());
        assert!(session.is_busy());
    }

    #[test]
    fn try_lock_fails_when_already_locked() {
        let session = Session::new(None);
        let _guard = session.try_lock().unwrap();

        // Second lock attempt should fail
        let second = session.try_lock();
        assert!(second.is_none());
        assert!(session.is_busy());
    }

    #[test]
    fn drop_guard_releases_lock() {
        let session = Session::new(None);

        {
            let _guard = session.try_lock().unwrap();
            assert!(session.is_busy());
        }
        // Guard dropped
        assert!(!session.is_busy());

        // Can re-acquire
        let guard2 = session.try_lock();
        assert!(guard2.is_some());
    }

    #[test]
    fn guard_provides_session_access() {
        let session = Session::new(Some("test-session".to_string()));
        let guard = session.try_lock().unwrap();
        assert_eq!(guard.session().name.as_deref(), Some("test-session"));
    }

    // -- InteractiveState queue tests --

    #[derive(Debug, Clone)]
    struct MockReplyCtx;
    impl crate::core::platform::ReplyCtx for MockReplyCtx {
        fn as_any(&self) -> &dyn std::any::Any { self }
        fn session_key_hint(&self) -> String { "test".to_string() }
        fn clone_box(&self) -> Box<dyn crate::core::platform::ReplyCtx> { Box::new(self.clone()) }
    }

    fn make_queued_message(text: &str) -> QueuedMessage {
        QueuedMessage {
            text: text.to_string(),
            images: vec![],
            files: vec![],
            voice: None,
            from: "test-user".to_string(),
            reply_ctx: Box::new(MockReplyCtx),
        }
    }

    #[test]
    fn queue_message_accepts_up_to_limit() {
        let session = Arc::new(Session::new(None));
        let mut state = InteractiveState::new(session);

        for i in 0..MAX_QUEUED_MESSAGES {
            let msg = make_queued_message(&format!("msg-{}", i));
            assert!(state.queue_message(msg));
        }

        assert_eq!(state.queue_len(), MAX_QUEUED_MESSAGES);

        // Next message should be rejected
        let overflow = make_queued_message("overflow");
        assert!(!state.queue_message(overflow));
    }

    #[test]
    fn drain_next_returns_fifo_order() {
        let session = Arc::new(Session::new(None));
        let mut state = InteractiveState::new(session);

        state.queue_message(make_queued_message("first"));
        state.queue_message(make_queued_message("second"));
        state.queue_message(make_queued_message("third"));

        assert_eq!(state.drain_next().unwrap().text, "first");
        assert_eq!(state.drain_next().unwrap().text, "second");
        assert_eq!(state.drain_next().unwrap().text, "third");
        assert!(state.drain_next().is_none());
    }

    #[test]
    fn drain_frees_queue_capacity() {
        let session = Arc::new(Session::new(None));
        let mut state = InteractiveState::new(session);

        // Fill the queue
        for i in 0..MAX_QUEUED_MESSAGES {
            state.queue_message(make_queued_message(&format!("msg-{}", i)));
        }
        assert!(!state.queue_message(make_queued_message("reject")));

        // Drain one, should have room for one more
        state.drain_next();
        assert!(state.queue_message(make_queued_message("new")));
    }

    // -- SessionManager CRUD tests --

    fn make_manager() -> (SessionManager, TempDir) {
        let tmp = TempDir::new().unwrap();
        let mgr = SessionManager::new(tmp.path(), Path::new("/test/project"));
        (mgr, tmp)
    }

    #[test]
    fn get_or_create_returns_same_session() {
        let (mgr, _tmp) = make_manager();
        let s1 = mgr.get_or_create("user:alice");
        let s2 = mgr.get_or_create("user:alice");
        assert_eq!(s1.id, s2.id);
    }

    #[test]
    fn set_work_dir_as_first_action_persists() {
        // Regression: `/dir` as the FIRST message in a fresh channel used to be
        // a silent no-op because no session existed yet. set_work_dir must now
        // create the session and store the directory.
        let (mgr, _tmp) = make_manager();
        let key = "discord:brand-new-channel";
        assert!(mgr.get_work_dir(key).is_none(), "precondition: no session yet");

        mgr.set_work_dir(key, "/Users/me/Documents/project-x");

        assert_eq!(
            mgr.get_work_dir(key).as_deref(),
            Some("/Users/me/Documents/project-x"),
            "work_dir set before any session existed must persist"
        );
    }

    #[test]
    fn different_keys_get_different_sessions() {
        let (mgr, _tmp) = make_manager();
        let s1 = mgr.get_or_create("user:alice");
        let s2 = mgr.get_or_create("user:bob");
        assert_ne!(s1.id, s2.id);
    }

    #[test]
    fn new_session_creates_fresh_and_becomes_active() {
        let (mgr, _tmp) = make_manager();
        let s1 = mgr.get_or_create("user:alice");
        let s2 = mgr.new_session("user:alice", Some("second".to_string()));
        assert_ne!(s1.id, s2.id);
        assert_eq!(s2.name.as_deref(), Some("second"));

        // Active session should now be s2
        let active = mgr.get_or_create("user:alice");
        assert_eq!(active.id, s2.id);
    }

    #[test]
    fn list_returns_sessions_for_key_only() {
        let (mgr, _tmp) = make_manager();
        mgr.get_or_create("user:alice");
        mgr.new_session("user:alice", Some("s2".to_string()));
        mgr.get_or_create("user:bob");

        assert_eq!(mgr.list("user:alice").len(), 2);
        assert_eq!(mgr.list("user:bob").len(), 1);
    }

    #[test]
    fn switch_session_changes_active() {
        let (mgr, _tmp) = make_manager();
        let s1 = mgr.get_or_create("user:alice");
        let _s2 = mgr.new_session("user:alice", None);

        // Switch back to s1
        let switched = mgr.switch_session("user:alice", &s1.id);
        assert!(switched.is_some());
        assert_eq!(switched.unwrap().id, s1.id);

        let active = mgr.get_or_create("user:alice");
        assert_eq!(active.id, s1.id);
    }

    #[test]
    fn switch_session_rejects_cross_key() {
        let (mgr, _tmp) = make_manager();
        let _s1 = mgr.get_or_create("user:alice");
        let s2 = mgr.get_or_create("user:bob");

        let cross = mgr.switch_session("user:alice", &s2.id);
        assert!(cross.is_none());
    }

    #[test]
    fn delete_session_removes_it() {
        let (mgr, _tmp) = make_manager();
        let s1 = mgr.get_or_create("user:alice");
        mgr.delete_session("user:alice", &s1.id);

        assert_eq!(mgr.list("user:alice").len(), 0);

        // A new get_or_create should produce a different session
        let s2 = mgr.get_or_create("user:alice");
        assert_ne!(s2.id, s1.id);
    }

    #[test]
    fn work_dir_get_set() {
        let (mgr, _tmp) = make_manager();
        mgr.get_or_create("user:alice");

        assert_eq!(mgr.get_work_dir("user:alice"), None);

        mgr.set_work_dir("user:alice", "/tmp/custom");
        assert_eq!(
            mgr.get_work_dir("user:alice"),
            Some("/tmp/custom".to_string())
        );
    }

    #[test]
    fn persistence_survives_reload() {
        let tmp = TempDir::new().unwrap();
        let session_id;

        {
            let mgr = SessionManager::new(tmp.path(), Path::new("/test/dir"));
            let s = mgr.new_session("user:alice", Some("persist-test".to_string()));
            session_id = s.id.clone();
        }

        // Reload from disk
        let mgr2 = SessionManager::new(tmp.path(), Path::new("/test/dir"));
        let active = mgr2.get_or_create("user:alice");
        assert_eq!(active.id, session_id);
        assert_eq!(active.name.as_deref(), Some("persist-test"));
    }

    #[test]
    fn try_lock_integrates_with_manager_sessions() {
        let (mgr, _tmp) = make_manager();
        let session = mgr.get_or_create("user:alice");

        // Lock via try_lock
        let guard = session.try_lock();
        assert!(guard.is_some());
        assert!(session.is_busy());

        // Another get_or_create returns the same Arc, still busy
        let same = mgr.get_or_create("user:alice");
        assert_eq!(same.id, session.id);
        assert!(same.is_busy());

        // Drop guard, session becomes free
        drop(guard);
        assert!(!session.is_busy());
        assert!(!same.is_busy());
    }

    // -- agent_type tests (U5) --

    #[test]
    fn session_default_agent_type_is_claude() {
        let s = Session::new(None);
        assert_eq!(s.agent_type, "");
        assert_eq!(s.effective_agent_type(), "claude");
    }

    #[test]
    fn session_explicit_agent_type() {
        let s = Session::new_with_agent_type(None, "kiro".to_string());
        assert_eq!(s.agent_type, "kiro");
        assert_eq!(s.effective_agent_type(), "kiro");
    }

    #[test]
    fn new_session_with_agent_type() {
        let (mgr, _tmp) = make_manager();
        let s = mgr.new_session_with_agent("user:alice", Some("kiro-sess".into()), "kiro");
        assert_eq!(s.agent_type, "kiro");
        assert_eq!(s.name.as_deref(), Some("kiro-sess"));
    }

    #[test]
    fn list_for_agent_filters_by_type() {
        let (mgr, _tmp) = make_manager();
        mgr.new_session_with_agent("user:alice", Some("claude-1".into()), "claude");
        mgr.new_session_with_agent("user:alice", Some("kiro-1".into()), "kiro");
        mgr.new_session_with_agent("user:alice", Some("kiro-2".into()), "kiro");

        let claude_sessions = mgr.list_for_agent("user:alice", "claude");
        assert_eq!(claude_sessions.len(), 1);
        assert_eq!(claude_sessions[0].name.as_deref(), Some("claude-1"));

        let kiro_sessions = mgr.list_for_agent("user:alice", "kiro");
        assert_eq!(kiro_sessions.len(), 2);
    }

    #[test]
    fn list_for_agent_empty_type_matches_claude() {
        let (mgr, _tmp) = make_manager();
        mgr.new_session("user:alice", Some("old-session".into()));

        let sessions = mgr.list_for_agent("user:alice", "claude");
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].name.as_deref(), Some("old-session"));
    }

    #[test]
    fn agent_type_persists_to_disk() {
        let tmp = TempDir::new().unwrap();
        let session_id;

        {
            let mgr = SessionManager::new(tmp.path(), Path::new("/test/dir"));
            let s = mgr.new_session_with_agent("user:alice", None, "kiro");
            session_id = s.id.clone();
        }

        let mgr2 = SessionManager::new(tmp.path(), Path::new("/test/dir"));
        let sessions = mgr2.list("user:alice");
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].id, session_id);
        assert_eq!(sessions[0].agent_type, "kiro");
    }

    #[test]
    fn old_session_without_agent_type_defaults_to_claude() {
        let tmp = TempDir::new().unwrap();

        {
            let mgr = SessionManager::new(tmp.path(), Path::new("/test/dir"));
            mgr.new_session("user:alice", Some("old".into()));
        }

        let mgr2 = SessionManager::new(tmp.path(), Path::new("/test/dir"));
        let sessions = mgr2.list("user:alice");
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].agent_type, "");
        assert_eq!(sessions[0].effective_agent_type(), "claude");
    }

    // -- active_sessions + work_dir canonicalization --

    #[test]
    fn active_sessions_returns_only_current_sessions_per_key() {
        let (mgr, _tmp) = make_manager();
        mgr.new_session_with_agent("user:alice", Some("first".into()), "claude");
        // Second new_session_with_agent replaces the active pointer for alice.
        let latest = mgr.new_session_with_agent("user:alice", Some("second".into()), "kiro");
        mgr.new_session_with_agent("user:bob", Some("bob-s".into()), "claude");

        let mut active = mgr.active_sessions();
        active.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(active.len(), 2);
        assert_eq!(active[0].0, "user:alice");
        assert_eq!(active[0].1.id, latest.id);
        assert_eq!(active[0].1.agent_type, "kiro");
        assert_eq!(active[1].0, "user:bob");
    }

    #[test]
    fn session_manager_reloads_after_canonicalization() {
        // Even if the caller passes a trailing slash / double slash / ".",
        // the state file should be shared because canonicalize normalizes it.
        let tmp = TempDir::new().unwrap();
        let real_dir = tmp.path().join("project");
        fs::create_dir_all(&real_dir).unwrap();

        let session_id;
        {
            let mgr = SessionManager::new(tmp.path(), &real_dir);
            let s = mgr.new_session_with_agent("user:alice", Some("s".into()), "claude");
            session_id = s.id.clone();
        }

        // Reopen with a non-canonical variant (trailing "/./").
        let alt = real_dir.join(".");
        let mgr2 = SessionManager::new(tmp.path(), &alt);
        let active = mgr2.get_or_create("user:alice");
        assert_eq!(active.id, session_id);
    }
}
