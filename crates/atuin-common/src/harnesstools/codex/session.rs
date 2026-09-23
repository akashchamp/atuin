use std::io::Read;
use std::path::{Path, PathBuf};

use futures::{Stream, StreamExt};
use serde::Deserialize;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::sync::watch;
use typed_builder::TypedBuilder;

use crate::fs::tree_watcher::{NodeContext, TreeWatcher};
use crate::harnesstools::codex::Codex;
use crate::harnesstools::session::model::{
    Content, MessageId, Role, StopReason, ToolCallId, ToolResult, ToolUse, Usage,
};
use crate::harnesstools::session::{
    Listener, Message, MessageError, Observable, RuntimeError, Session, SessionId, Sessions,
    WatchError, scan_sessions,
};
use crate::json::jsonl;
use crate::sync::BlockingPool;
use crate::utils::{env_nonempty, home_dir};

#[derive(Debug, Clone, TypedBuilder)]
pub struct CodexSessions {
    #[builder(default, setter(strip_option, into))]
    root: Option<PathBuf>,
    /// Runs every file read of the sessions this finds.
    pool: BlockingPool,
}

impl CodexSessions {
    fn resolve_root(&self) -> PathBuf {
        self.root.clone().unwrap_or_else(|| {
            env_nonempty("CODEX_HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|| home_dir().join(".codex"))
                .join("sessions")
        })
    }
}

impl Sessions for CodexSessions {
    type Listener = CodexListener;

    fn listener(&self) -> Result<CodexListener, RuntimeError> {
        let root = self.resolve_root();
        if !root.is_dir() {
            return Err(RuntimeError::NotFound(root));
        }
        Ok(CodexListener {
            root,
            pool: self.pool.clone(),
        })
    }

    fn existing(
        &self,
    ) -> Result<impl Stream<Item = Result<CodexSession, RuntimeError>> + Send + 'static, RuntimeError>
    {
        let root = self.resolve_root();
        if !root.is_dir() {
            return Err(RuntimeError::NotFound(root));
        }
        let pool = self.pool.clone();
        Ok(async_stream::stream! {
            let sessions_pool = pool.clone();
            let scan = pool
                .run(move || {
                    scan_sessions(root, |path, is_file| {
                        CodexListener::open_session(path, is_file, &sessions_pool)
                    })
                })
                .await;
            match scan {
                Ok(items) => {
                    for item in items {
                        yield item;
                    }
                }
                Err(cancelled) => yield Err(RuntimeError::Io(std::io::Error::other(cancelled))),
            }
        })
    }
}

impl Observable for Codex {
    type Sessions = CodexSessions;

    fn sessions(&self, pool: BlockingPool) -> CodexSessions {
        CodexSessions::builder().pool(pool).build()
    }
}

#[derive(Debug, Clone)]
pub struct CodexListener {
    root: PathBuf,
    pool: BlockingPool,
}

impl CodexListener {
    /// Build a read-once session for an accepted codex rollout file (no change signal), or `None`.
    fn open_session(path: &Path, is_file: bool, pool: &BlockingPool) -> Option<CodexSession> {
        let name = path.file_name()?.to_string_lossy();
        if !is_file || !name.starts_with("rollout-") || !name.ends_with(".jsonl") {
            return None;
        }
        let stem = path.file_stem()?.to_string_lossy();
        let mut groups: Vec<&str> = stem.rsplitn(6, '-').collect();
        groups.truncate(5);
        groups.reverse();
        let id = groups.join("-");
        Some(CodexSession::open(SessionId::from(id), path.to_path_buf(), pool.clone()))
    }

    /// The session for an accepted file, paired with the change signal the watcher keeps alive
    /// for as long as the file exists.
    fn accept(ctx: &NodeContext, pool: &BlockingPool) -> Option<(CodexSession, watch::Sender<()>)> {
        let mut session = Self::open_session(ctx.path(), ctx.is_file(), pool)?;
        let (signal, rx) = watch::channel(());
        session.changes = Some(rx);
        Some((session, signal))
    }
}

impl Listener for CodexListener {
    type Session = CodexSession;

    fn watch(self) -> impl Stream<Item = Result<CodexSession, WatchError>> + Send + 'static {
        let root = self.root;
        let pool = self.pool;
        async_stream::stream! {
            let (tx, rx) = flume::unbounded::<CodexSession>();
            let _watcher = match TreeWatcher::builder().recursive(true).watch(&root, move |ctx| {
                let (session, signal) = Self::accept(&ctx, &pool)?;
                let _ = tx.send(session);
                Some(signal)
            }) {
                Ok(watcher) => watcher,
                Err(err) => {
                    yield Err(WatchError::from(err));
                    return;
                }
            };
            while let Ok(session) = rx.recv_async().await {
                yield Ok(session);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct CodexSession {
    id: SessionId,
    path: PathBuf,
    /// Wakes [`messages`](Session::messages) on each change to the file; `None` reads it once.
    changes: Option<watch::Receiver<()>>,
    pool: BlockingPool,
}

impl CodexSession {
    /// A session over the file as it stands: [`messages`](Session::messages) ends at its end.
    #[must_use]
    pub fn open(id: SessionId, path: PathBuf, pool: BlockingPool) -> Self {
        Self {
            id,
            path,
            changes: None,
            pool,
        }
    }

    fn stamper(&self, before: u64) -> Stamper {
        Stamper {
            session: self.id.clone(),
            path: self.path.clone(),
            pool: self.pool.clone(),
            before,
            usage_recorded: (before == 0).then_some(false),
        }
    }
}

impl Session for CodexSession {
    type Message = CodexMessage;

    fn id(&self) -> SessionId {
        self.id.clone()
    }

    async fn message_at(&self, at: u64) -> Option<CodexMessage> {
        let mut message: CodexMessage = jsonl::value_at(&self.path, at, &self.pool).await?;
        self.stamper(at).stamp(&mut message).await;
        Some(message)
    }

    fn messages_from(
        self,
        from: u64,
    ) -> impl Stream<Item = Result<(u64, CodexMessage), MessageError>> + Send + 'static {
        let mut stamper = self.stamper(from);
        let lines = jsonl::follow_from::<CodexMessage>(self.path, from, self.changes, self.pool);
        async_stream::stream! {
            futures::pin_mut!(lines);
            while let Some(line) = lines.next().await {
                yield match line {
                    Ok((at, mut message)) => {
                        stamper.stamp(&mut message).await;
                        Ok((at, message))
                    }
                    Err(err) => Err(MessageError::from(err)),
                };
            }
        }
    }

    fn read(&self) -> impl Stream<Item = Result<CodexMessage, MessageError>> + Send + 'static {
        let mut stamper = self.stamper(0);
        let lines = jsonl::read_all::<CodexMessage>(self.path.clone(), self.pool.clone());
        async_stream::stream! {
            futures::pin_mut!(lines);
            while let Some(line) = lines.next().await {
                yield match line {
                    Ok(mut message) => {
                        stamper.stamp(&mut message).await;
                        Ok(message)
                    }
                    Err(err) => Err(MessageError::from(err)),
                };
            }
        }
    }
}

/// Tells each line of one rollout what only its reader knows (see [`LineContext`]).
struct Stamper {
    session: SessionId,
    path: PathBuf,
    pool: BlockingPool,
    /// Where reading started: lines before it were never seen, so whether one of them was a
    /// `token_usage_record` is looked up in the file, once, when a `token_count` needs to know.
    before: u64,
    /// Whether a line so far was a `token_usage_record`; `None` until known.
    usage_recorded: Option<bool>,
}

impl Stamper {
    async fn stamp(&mut self, message: &mut CodexMessage) {
        if message.kind == TOKEN_USAGE_RECORD {
            self.usage_recorded = Some(true);
        } else if self.usage_recorded.is_none() && message.is_token_count() {
            let (path, before) = (self.path.clone(), self.before);
            let found = self
                .pool
                .run(move || prefix_contains(&path, before, TOKEN_USAGE_RECORD_MARKER))
                .await
                .is_ok_and(|found| found.unwrap_or(false));
            self.usage_recorded = Some(found);
        }
        message.context = LineContext {
            session: Some(self.session.clone()),
            usage_recorded: self.usage_recorded == Some(true),
        };
    }
}

/// Whether the first `end` bytes of the file at `path` contain `needle`, read in chunks so a
/// long rollout is never held in memory whole.
fn prefix_contains(path: &Path, end: u64, needle: &[u8]) -> std::io::Result<bool> {
    const CHUNK: usize = 64 * 1024;
    let mut file = std::fs::File::open(path)?.take(end);
    let finder = memchr::memmem::Finder::new(needle);
    let keep = needle.len().saturating_sub(1);
    let mut window: Vec<u8> = Vec::with_capacity(CHUNK + keep);
    let mut chunk = vec![0; CHUNK];
    loop {
        let read = file.read(&mut chunk)?;
        if read == 0 {
            return Ok(false);
        }
        window.extend_from_slice(&chunk[..read]);
        if finder.find(&window).is_some() {
            return Ok(true);
        }
        // Keep a needle's length less one, so a match straddling two chunks is still found.
        window.drain(..window.len().saturating_sub(keep));
    }
}

const TOKEN_USAGE_RECORD: &str = "token_usage_record";

/// How a `token_usage_record` line names its type. Unescaped quotes cannot occur inside a JSON
/// string, so a transcript merely quoting the name does not match.
const TOKEN_USAGE_RECORD_MARKER: &[u8] = b"\"type\":\"token_usage_record\"";

/// What a line cannot tell about itself, which the reader of its rollout knows.
#[derive(Debug, Clone, Default)]
struct LineContext {
    /// The rollout's own session. A `session_meta` naming another was copied in from the
    /// rollout a fork or subagent started from (codex-rs `ForkPersistence::Copied`).
    session: Option<SessionId>,
    /// An earlier line was a `token_usage_record`: this rollout records usage per model call,
    /// so its `token_count` lines only repeat it.
    usage_recorded: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CodexMessage {
    #[serde(rename = "type")]
    kind: String,
    timestamp: Option<String>,
    payload: Option<serde_json::Value>,
    #[serde(skip)]
    context: LineContext,
}

/// Whether a command output reports a non-zero exit, in either shape Codex has used: the
/// `Process exited with code N` header, or an `"exit_code":N` field in a JSON envelope.
fn codex_output_failed(output: &serde_json::Value) -> bool {
    let texts: Vec<&str> = match output {
        serde_json::Value::String(s) => vec![s],
        serde_json::Value::Array(blocks) => {
            blocks.iter().filter_map(|b| b["text"].as_str()).collect()
        }
        _ => Vec::new(),
    };
    // The last occurrence: the header follows the output, which may quote an earlier one.
    texts.iter().any(|text| {
        ["Process exited with code ", "\"exit_code\":"].iter().any(|marker| {
            text.rfind(marker).is_some_and(|at| {
                let code: String = text[at + marker.len()..]
                    .trim_start()
                    .chars()
                    .take_while(|c| c.is_ascii_digit() || *c == '-')
                    .collect();
                code.parse::<i64>().is_ok_and(|c| c != 0)
            })
        })
    })
}

/// Start and end markers of the fragments Codex injects into the conversation as `user`
/// messages, per codex-rs `core/src/context/contextual_user_message.rs`
/// (`CONTEXTUAL_USER_FRAGMENT_MATCHERS`) and the frozen legacy list in
/// `thread-store/src/local/rollout_migration/rollback.rs` (`is_known_contextual_user_text`).
const CONTEXTUAL_USER_MARKERS: &[(&str, &str)] = &[
    ("# AGENTS.md instructions", "</INSTRUCTIONS>"),
    // Older rollouts wrapped AGENTS.md in `USER_INSTRUCTIONS_OPEN_TAG`.
    ("<user_instructions>", "</user_instructions>"),
    ("<environment_context>", "</environment_context>"),
    ("<agent_message_board_notification>", "</agent_message_board_notification>"),
    ("<skill>", "</skill>"),
    ("<user_shell_command>", "</user_shell_command>"),
    ("<turn_aborted>", "</turn_aborted>"),
    ("<subagent_notification>", "</subagent_notification>"),
    ("<codex_internal_context", "</codex_internal_context>"),
    ("<goal_context>", "</goal_context>"),
    ("<recommended_plugins>", "</recommended_plugins>"),
    // codex-rs `parse_hook_prompt_fragment`: `<hook_prompt hook_run_id="...">`.
    ("<hook_prompt ", "</hook_prompt>"),
];

/// Warnings Codex once injected as `user` messages (the `Legacy*Warning` fragments).
const CONTEXTUAL_USER_PREFIXES: &[&str] = &[
    "Warning: The maximum number of unified exec processes you can keep open is",
    "Warning: apply_patch was requested via ",
    "Warning: Your account was flagged for potentially high-risk cyber activity",
];

fn starts_with_ignore_case(text: &str, prefix: &str) -> bool {
    text.get(..prefix.len()).is_some_and(|head| head.eq_ignore_ascii_case(prefix))
}

fn ends_with_ignore_case(text: &str, suffix: &str) -> bool {
    text.len() >= suffix.len()
        && text
            .get(text.len() - suffix.len()..)
            .is_some_and(|tail| tail.eq_ignore_ascii_case(suffix))
}

/// Whether `text` is a fragment of harness-injected context rather than something the user
/// typed (codex-rs `is_contextual_user_fragment`).
fn is_contextual_user_text(text: &str) -> bool {
    let text = text.trim();
    CONTEXTUAL_USER_MARKERS.iter().any(|(start, end)| {
        starts_with_ignore_case(text, start) && ends_with_ignore_case(text, end)
    }) || CONTEXTUAL_USER_PREFIXES.iter().any(|prefix| text.starts_with(prefix))
        || text
            .strip_prefix("<external_")
            .and_then(|rest| rest.split_once('>'))
            .is_some_and(|(key, _)| text.ends_with(&format!("</external_{key}>")))
}

/// A stable key for one `TokenUsage` object, for lines that carry no id of their own.
fn usage_key(usage: &serde_json::Value) -> Option<String> {
    let field = |name: &str| usage.get(name).and_then(serde_json::Value::as_u64).unwrap_or(0);
    usage.is_object().then(|| {
        format!(
            "{}.{}.{}.{}.{}",
            field("input_tokens"),
            field("cached_input_tokens"),
            field("output_tokens"),
            field("reasoning_output_tokens"),
            field("total_tokens"),
        )
    })
}

/// Our [`Usage`] for a Codex `TokenUsage`. Codex (and the Responses API) count cached input
/// inside `input_tokens` (codex-rs `TokenUsage::non_cached_input`), where [`Usage::input`] is
/// the fresh input alone. `output_tokens` already includes `reasoning_output_tokens`.
fn usage_of(usage: &serde_json::Value) -> Option<Usage> {
    if !usage.is_object() {
        return None;
    }
    let field = |name: &str| usage.get(name).and_then(serde_json::Value::as_u64);
    let cache_read = field("cached_input_tokens");
    Some(Usage {
        input: field("input_tokens").map(|input| input.saturating_sub(cache_read.unwrap_or(0))),
        output: field("output_tokens"),
        cache_read,
        cache_write: field("cache_write_input_tokens"),
    })
}

impl CodexMessage {
    fn block(value: &serde_json::Value) -> Content {
        match value["type"].as_str() {
            Some("input_text" | "output_text" | "text") => {
                Content::Text(value["text"].as_str().unwrap_or_default().to_owned())
            }
            _ => Content::Other(value.clone()),
        }
    }

    fn payload_type(&self) -> Option<&str> {
        self.payload.as_ref()?["type"].as_str()
    }

    fn is_event(&self, kind: &str) -> bool {
        self.kind == "event_msg" && self.payload_type() == Some(kind)
    }

    fn is_token_count(&self) -> bool {
        self.is_event("token_count")
    }

    /// A `session_meta` another rollout's: one a fork or subagent copied in from its parent,
    /// which describes the parent, not this session.
    fn is_copied_meta(&self) -> bool {
        self.kind == "session_meta"
            && self.context.session.as_ref().is_some_and(|own| {
                self.payload
                    .as_ref()
                    .and_then(|p| p["id"].as_str())
                    .is_some_and(|id| id != own.as_ref())
            })
    }

    /// This rollout's own `session_meta` payload.
    fn own_meta(&self) -> Option<&serde_json::Value> {
        (self.kind == "session_meta" && !self.is_copied_meta()).then_some(self.payload.as_ref()?)
    }

    /// The `info` of a `token_count` that reports usage of its own: only in a rollout that
    /// records none per model call, and not for a snapshot of no new tokens (a rate-limit
    /// refresh, an estimate after compaction, the window filled after an overflow).
    fn token_count_info(&self) -> Option<&serde_json::Value> {
        if !self.is_token_count() || self.context.usage_recorded {
            return None;
        }
        let info = self.payload.as_ref()?.get("info").filter(|info| !info.is_null())?;
        let last = info.get("last_token_usage")?;
        let tokens = |name: &str| last.get(name).and_then(serde_json::Value::as_u64).unwrap_or(0);
        (tokens("input_tokens") + tokens("output_tokens") > 0).then_some(info)
    }

    /// The model call a usage line accounts for. A `token_usage_record` names its response.
    /// A `token_count` names none, so it is keyed on the thread's running total, which is
    /// unique per call and survives being copied into a fork (whose copies are re-stamped with
    /// the child's timestamps), and which repeats verbatim when Codex re-sends a snapshot.
    fn usage_turn(&self) -> Option<String> {
        if self.kind == TOKEN_USAGE_RECORD {
            let payload = self.payload.as_ref()?;
            return match payload["response_id"].as_str().filter(|id| !id.is_empty()) {
                Some(response) => Some(response.to_owned()),
                None => {
                    usage_key(&payload["thread_token_usage"]).map(|key| format!("thread:{key}"))
                }
            };
        }
        let info = self.token_count_info()?;
        match usage_key(&info["total_token_usage"]) {
            Some(total) => Some(format!("token_count:{total}")),
            None => Some(format!(
                "token_count:{}:{}",
                self.timestamp.as_deref().unwrap_or_default(),
                usage_key(&info["last_token_usage"])?
            )),
        }
    }

    /// Why the turn this event ends failed or stopped, when it did.
    fn failure(&self) -> Option<(StopReason, String)> {
        let payload = self.payload.as_ref()?;
        if self.is_event("turn_aborted") {
            let reason = payload["reason"].as_str().unwrap_or("aborted");
            return Some((StopReason::Aborted, reason.to_owned()));
        }
        if self.is_event("turn_complete") {
            let error = payload.get("error").filter(|e| !e.is_null())?;
            let message =
                error["message"].as_str().map_or_else(|| error.to_string(), str::to_owned);
            return Some((StopReason::Error, message));
        }
        None
    }

    /// Whether this `user` message is context the harness injected (AGENTS.md, the environment,
    /// skills, notifications) rather than a user turn: any of its texts is a contextual fragment
    /// (codex-rs `event_mapping::is_contextual_user_message_content`).
    fn is_contextual(&self) -> bool {
        self.payload.as_ref().and_then(|p| p["content"].as_array()).is_some_and(|blocks| {
            blocks.iter().any(|block| {
                block["type"] == "input_text"
                    && block["text"].as_str().is_some_and(is_contextual_user_text)
            })
        })
    }
}

impl Message for CodexMessage {
    fn id(&self) -> Option<MessageId> {
        // Usage lines carry no id, but name the model call they account for; keyed on nothing,
        // two in one millisecond would share a synthetic id and one would be dropped.
        if self.kind == TOKEN_USAGE_RECORD || self.is_token_count() {
            return self.usage_turn().map(|turn| MessageId::from(format!("{turn}#usage")));
        }
        if self.is_copied_meta() {
            return None;
        }
        // Prefer the per-record `id` (ctc_/ctco_/msg_...) over `call_id`: a tool call and its
        // output share one `call_id`, so keying identity on it would collide the two records and
        // the dedup gate would drop the output. Older rollouts have no per-record id at all, so
        // an output falling back to `call_id` is suffixed to keep it distinct from its call.
        // `call_id` linkage lives in the content, not here.
        let p = self.payload.as_ref()?;
        if let Some(id) = p["id"].as_str() {
            return Some(MessageId::from(id.to_owned()));
        }
        let call_id = p["call_id"].as_str()?;
        let is_output = p["type"].as_str().is_some_and(|t| t.ends_with("_output"));
        Some(MessageId::from(if is_output {
            format!("{call_id}#out")
        } else {
            call_id.to_owned()
        }))
    }

    fn role(&self) -> Role {
        if self.kind == "compacted" {
            return Role::System;
        }
        if self.failure().is_some() {
            return Role::Assistant;
        }
        let payload = self.payload.as_ref();
        match self.payload_type() {
            Some(
                "function_call" | "custom_tool_call" | "local_shell_call" | "web_search_call"
                | "tool_search_call" | "reasoning",
            ) => Role::Assistant,
            Some("function_call_output" | "custom_tool_call_output" | "tool_search_output") => {
                Role::Tool
            }
            _ => match payload.and_then(|p| p["role"].as_str()).unwrap_or(self.kind.as_str()) {
                "user" if self.is_contextual() => Role::System,
                "user" => Role::User,
                "assistant" => Role::Assistant,
                "system" | "developer" => Role::System,
                "tool" => Role::Tool,
                other => Role::Other(other.to_owned()),
            },
        }
    }

    fn timestamp(&self) -> Option<OffsetDateTime> {
        self.timestamp.as_deref().and_then(|s| OffsetDateTime::parse(s, &Rfc3339).ok())
    }

    fn content(&self) -> Vec<Content> {
        let Some(payload) = self.payload.as_ref() else {
            return Vec::new();
        };
        if self.kind == "compacted" {
            return payload["message"]
                .as_str()
                .filter(|summary| !summary.is_empty())
                .map(|summary| vec![Content::Summary(summary.to_owned())])
                .unwrap_or_default();
        }
        if let Some((_, why)) = self.failure() {
            return vec![Content::Error(why)];
        }
        if self.kind == "event_msg" || self.kind == "session_meta" {
            return Vec::new();
        }
        let call_id =
            |key: &str| ToolCallId::from(payload[key].as_str().unwrap_or_default().to_owned());
        let tool_use = |id: ToolCallId, name: &str, input: &serde_json::Value| {
            vec![Content::ToolUse(ToolUse {
                id,
                name: name.to_owned(),
                input: input.clone(),
            })]
        };
        match payload["type"].as_str() {
            // Do not retain summaries, encrypted reasoning, or other reasoning payloads.
            Some("reasoning") => vec![Content::ReasoningSummary { tokens: None }],
            Some("function_call") => tool_use(
                call_id("call_id"),
                payload["name"].as_str().unwrap_or_default(),
                &payload["arguments"],
            ),
            Some("custom_tool_call") => tool_use(
                call_id("call_id"),
                payload["name"].as_str().unwrap_or_default(),
                &payload["input"],
            ),
            // Its output comes back as a `function_call_output` under the same `call_id`.
            Some("local_shell_call") => tool_use(
                call_id(if payload["call_id"].is_string() {
                    "call_id"
                } else {
                    "id"
                }),
                "local_shell",
                &payload["action"],
            ),
            // Run by the API: the results feed the model directly and no output item follows.
            Some("web_search_call") => tool_use(call_id("id"), "web_search", &payload["action"]),
            Some("tool_search_call") => tool_use(
                call_id(if payload["call_id"].is_string() {
                    "call_id"
                } else {
                    "id"
                }),
                "tool_search",
                &payload["arguments"],
            ),
            Some("function_call_output" | "custom_tool_call_output") => {
                vec![Content::ToolResult(ToolResult {
                    call: call_id("call_id"),
                    output: payload["output"].clone(),
                    error: codex_output_failed(&payload["output"]),
                })]
            }
            Some("tool_search_output") => vec![Content::ToolResult(ToolResult {
                call: call_id(if payload["call_id"].is_string() {
                    "call_id"
                } else {
                    "id"
                }),
                output: payload["tools"].clone(),
                error: payload["status"].as_str().is_some_and(|s| s != "completed"),
            })],
            _ => match &payload["content"] {
                serde_json::Value::Array(blocks) => blocks.iter().map(Self::block).collect(),
                serde_json::Value::String(text) => vec![Content::Text(text.clone())],
                _ => Vec::new(),
            },
        }
    }

    fn model(&self) -> Option<String> {
        if self.is_copied_meta() {
            return None;
        }
        self.payload.as_ref()?.get("model")?.as_str().map(str::to_owned)
    }

    fn usage(&self) -> Option<Usage> {
        if self.kind == TOKEN_USAGE_RECORD {
            return usage_of(self.payload.as_ref()?.get("usage")?);
        }
        usage_of(&self.token_count_info()?["last_token_usage"])
    }

    fn stop_reason(&self) -> Option<StopReason> {
        self.failure().map(|(reason, _)| reason)
    }

    fn cwd(&self) -> Option<PathBuf> {
        if self.is_copied_meta() {
            return None;
        }
        self.payload.as_ref()?.get("cwd")?.as_str().map(PathBuf::from)
    }

    fn git_branch(&self) -> Option<String> {
        self.own_meta()?["git"]["branch"].as_str().map(str::to_owned)
    }

    /// The thread a fork or subagent started from (codex-rs `SessionMeta::forked_from_id`,
    /// `parent_thread_id`, and before those `source.subagent.thread_spawn.parent_thread_id`).
    fn parent_session(&self) -> Option<SessionId> {
        let meta = self.own_meta()?;
        [
            &meta["forked_from_id"],
            &meta["parent_thread_id"],
            &meta["source"]["subagent"]["thread_spawn"]["parent_thread_id"],
        ]
        .into_iter()
        .find_map(serde_json::Value::as_str)
        .map(|parent| SessionId::from(parent.to_owned()))
    }

    /// Only usage lines name their model call (see `usage_turn`): Codex items carry no
    /// response id.
    fn turn_id(&self) -> Option<String> {
        self.usage_turn()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use futures::{StreamExt, TryStreamExt};
    use rstest::rstest;

    use super::*;
    use crate::sync::BlockingPool;

    fn pool() -> BlockingPool {
        BlockingPool::new(std::num::NonZeroUsize::MIN)
    }
    use crate::harnesstools::session::model::{Content, Role};
    use crate::harnesstools::session::{Message, Session, Sessions};

    #[rstest]
    fn normalizes_a_codex_assistant_message() {
        let raw = serde_json::json!({
            "timestamp": "2026-09-18T10:00:00Z",
            "type": "message",
            "payload": {
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "done"}],
            },
        })
        .to_string();
        let m: CodexMessage = serde_json::from_str(&raw).unwrap();
        assert_eq!(m.role(), Role::Assistant);
        assert_eq!(m.content(), vec![Content::Text("done".into())]);
    }

    #[rstest]
    fn turn_context_exposes_model_and_cwd() {
        let raw = serde_json::json!({
            "type": "turn_context",
            "payload": {"model": "gpt-5.6-terra", "effort": "medium", "cwd": "/work/atuin"},
        })
        .to_string();
        let m: CodexMessage = serde_json::from_str(&raw).unwrap();
        assert_eq!(m.model(), Some("gpt-5.6-terra".to_owned()));
        assert_eq!(m.cwd(), Some(PathBuf::from("/work/atuin")));
    }

    #[rstest]
    fn token_usage_record_exposes_usage() {
        let raw = serde_json::json!({
            "type": "token_usage_record",
            "payload": {
                "usage": {
                    "input_tokens": 100,
                    "output_tokens": 50,
                    "cached_input_tokens": 20,
                    "cache_write_input_tokens": 0,
                },
            },
        })
        .to_string();
        let m: CodexMessage = serde_json::from_str(&raw).unwrap();
        assert_eq!(
            m.usage(),
            Some(Usage {
                input: Some(80),
                output: Some(50),
                cache_read: Some(20),
                cache_write: Some(0)
            })
        );
    }

    #[rstest]
    fn enrichment_is_none_when_the_harness_did_not_provide_it() {
        let raw = serde_json::json!({
            "type": "response_item",
            "payload": {"type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "hi"}]},
        })
        .to_string();
        let m: CodexMessage = serde_json::from_str(&raw).unwrap();
        assert_eq!(m.model(), None);
        assert_eq!(m.usage(), None);
        assert_eq!(m.stop_reason(), None);
        assert_eq!(m.git_branch(), None);
    }

    #[rstest]
    fn normalizes_a_codex_function_call() {
        let raw = serde_json::json!({
            "type": "response_item",
            "payload": {
                "type": "function_call",
                "name": "shell",
                "arguments": "{\"cmd\":\"ls\"}",
                "call_id": "c1",
            },
        })
        .to_string();
        let m: CodexMessage = serde_json::from_str(&raw).unwrap();
        assert!(matches!(m.content().as_slice(), [Content::ToolUse(_)]));
    }

    #[rstest]
    fn normalizes_a_codex_custom_tool_call() {
        let call = serde_json::json!({
            "type": "response_item",
            "payload": {"type": "custom_tool_call", "name": "shell", "input": "ls", "call_id": "c1"},
        })
        .to_string();
        let m: CodexMessage = serde_json::from_str(&call).unwrap();
        assert_eq!(m.role(), Role::Assistant);
        assert!(
            matches!(m.content().as_slice(), [Content::ToolUse(u)] if u.id.to_string() == "c1")
        );

        let output = serde_json::json!({
            "type": "response_item",
            "payload": {"type": "custom_tool_call_output", "call_id": "c1", "output": "files"},
        })
        .to_string();
        let m: CodexMessage = serde_json::from_str(&output).unwrap();
        assert_eq!(m.role(), Role::Tool);
        assert!(
            matches!(m.content().as_slice(), [Content::ToolResult(r)] if r.call.to_string() == "c1")
        );
    }

    #[rstest]
    fn tool_call_and_output_have_distinct_ids_despite_shared_call_id() {
        let call = serde_json::json!({
            "type": "response_item",
            "payload": {"type": "custom_tool_call", "id": "ctc_1", "call_id": "call_x", "name": "sh", "input": "ls"},
        })
        .to_string();
        let output = serde_json::json!({
            "type": "response_item",
            "payload": {"type": "custom_tool_call_output", "id": "ctco_1", "call_id": "call_x", "output": "files"},
        })
        .to_string();
        let call: CodexMessage = serde_json::from_str(&call).unwrap();
        let output: CodexMessage = serde_json::from_str(&output).unwrap();
        assert_ne!(call.id(), output.id(), "call and its output must not share a source id");
        assert_eq!(call.id(), Some(MessageId::from("ctc_1".to_owned())));
        assert_eq!(output.id(), Some(MessageId::from("ctco_1".to_owned())));
    }

    /// Older rollouts carry no per-record id: the call keys on `call_id` and its output must
    /// still get a distinct id, or the dedup gate drops every tool result.
    #[rstest]
    #[case("function_call", "function_call_output")]
    #[case("custom_tool_call", "custom_tool_call_output")]
    fn id_less_tool_output_does_not_collide_with_its_call(
        #[case] call_kind: &str,
        #[case] output_kind: &str,
    ) {
        let call: CodexMessage = serde_json::from_str(
            &serde_json::json!({
                "type": "response_item",
                "payload": {"type": call_kind, "call_id": "call_x", "name": "sh", "arguments": "ls"},
            })
            .to_string(),
        )
        .unwrap();
        let output: CodexMessage = serde_json::from_str(
            &serde_json::json!({
                "type": "response_item",
                "payload": {"type": output_kind, "call_id": "call_x", "output": "files"},
            })
            .to_string(),
        )
        .unwrap();
        assert_eq!(call.id(), Some(MessageId::from("call_x".to_owned())));
        assert_eq!(output.id(), Some(MessageId::from("call_x#out".to_owned())));
    }

    #[rstest]
    #[case(serde_json::json!("ok\nProcess exited with code 0"), false)]
    #[case(serde_json::json!("boom\nProcess exited with code 2"), true)]
    #[case(serde_json::json!([{"type": "output_text", "text": "{\"output\":\"x\",\"exit_code\":0}"}]), false)]
    #[case(serde_json::json!([{"type": "output_text", "text": "{\"output\":\"x\",\"exit_code\":1}"}]), true)]
    #[case(serde_json::json!("plain text"), false)]
    #[case(serde_json::json!("killed\nProcess exited with code -9"), true)]
    #[case(serde_json::json!("log: Process exited with code 1\nProcess exited with code 0"), false)]
    #[case(serde_json::json!([{"type": "output_text", "text": "{\"output\": \"x\", \"exit_code\": 3}"}]), true)]
    fn tool_output_error_is_derived_from_exit_code(
        #[case] output: serde_json::Value,
        #[case] error: bool,
    ) {
        let m: CodexMessage = serde_json::from_str(
            &serde_json::json!({
                "type": "response_item",
                "payload": {"type": "function_call_output", "call_id": "c1", "output": output},
            })
            .to_string(),
        )
        .unwrap();
        assert!(matches!(m.content().as_slice(), [Content::ToolResult(r)] if r.error == error));
    }

    #[rstest]
    #[case("turn_aborted", Some(StopReason::Aborted))]
    #[case("token_count", None)]
    fn turn_aborted_events_end_the_turn(#[case] kind: &str, #[case] expected: Option<StopReason>) {
        let m: CodexMessage = serde_json::from_str(
            &serde_json::json!({"type": "event_msg", "payload": {"type": kind}}).to_string(),
        )
        .unwrap();
        assert_eq!(m.stop_reason(), expected);
    }

    #[rstest]
    fn listener_reports_not_found_for_a_missing_root() {
        let sessions =
            CodexSessions::builder().root(PathBuf::from("/no/such/codex")).pool(pool()).build();
        assert!(matches!(sessions.listener(), Err(RuntimeError::NotFound(_))));
    }

    #[rstest]
    #[tokio::test]
    async fn messages_streams_turns_from_a_rollout_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rollout-2026-09-18-th1.jsonl");
        let body = [
            serde_json::json!({"type": "session_meta", "payload": {"id": "th1"}}).to_string(),
            serde_json::json!({
                "type": "message",
                "payload": {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hi"}]},
            })
            .to_string(),
        ]
        // Trailing newline required: messages() withholds an unterminated final line until a
        // later write completes it (a real session ends every record with a newline).
        .join("\n")
            + "\n";
        std::fs::write(&path, body).unwrap();

        let session = CodexSession::open(SessionId::from("th1".to_owned()), path, pool());
        let roles: Vec<Role> =
            session.messages().take(2).map_ok(|m| m.role()).try_collect().await.unwrap();
        assert_eq!(roles.last(), Some(&Role::User));
    }

    #[rstest]
    #[tokio::test]
    async fn watch_emits_sessions_as_files_appear() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("2026").join("09").join("19");
        std::fs::create_dir_all(&sub).unwrap();
        let sid = "0a1b2c3d-4e5f-6789-abcd-ef0123456789";
        std::fs::write(
            sub.join(format!("rollout-2026-09-19T00-00-00-{sid}.jsonl")),
            serde_json::json!({"type": "session_meta", "payload": {"id": sid}}).to_string(),
        )
        .unwrap();

        let listener = CodexSessions::builder()
            .root(dir.path().to_path_buf())
            .pool(pool())
            .build()
            .listener()
            .unwrap();
        let seen: Vec<SessionId> = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            listener.watch().take(1).map_ok(|s| s.id()).try_collect(),
        )
        .await
        .expect("watch() did not emit a session within 10s")
        .unwrap();
        assert_eq!(seen, vec![SessionId::from(sid.to_owned())]);
    }

    fn rollout(dir: &Path) -> PathBuf {
        let path =
            dir.join("rollout-2026-09-19T00-00-00-0a1b2c3d-4e5f-6789-abcd-ef0123456789.jsonl");
        std::fs::write(
            &path,
            serde_json::json!({"type": "session_meta", "payload": {"id": "th1"}}).to_string()
                + "\n",
        )
        .unwrap();
        path
    }

    #[rstest]
    #[tokio::test]
    async fn messages_yields_lines_appended_after_discovery() {
        let dir = tempfile::tempdir().unwrap();
        let path = rollout(dir.path());

        let listener = CodexSessions::builder()
            .root(dir.path().to_path_buf())
            .pool(pool())
            .build()
            .listener()
            .unwrap();
        // The watch stream owns the watcher: it must outlive the message stream.
        let mut sessions = std::pin::pin!(listener.watch());
        let session = sessions.next().await.unwrap().unwrap();
        let mut messages = std::pin::pin!(session.messages());
        assert!(messages.next().await.unwrap().is_ok());

        let mut file = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
        std::io::Write::write_all(
            &mut file,
            (serde_json::json!({
                "type": "message",
                "payload": {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hi"}]},
            })
            .to_string()
                + "\n")
                .as_bytes(),
        )
        .unwrap();
        drop(file);
        assert_eq!(messages.next().await.unwrap().unwrap().role(), Role::User);
    }

    #[rstest]
    #[tokio::test]
    async fn messages_ends_when_the_session_file_is_removed() {
        let dir = tempfile::tempdir().unwrap();
        let path = rollout(dir.path());

        let listener = CodexSessions::builder()
            .root(dir.path().to_path_buf())
            .pool(pool())
            .build()
            .listener()
            .unwrap();
        let mut sessions = std::pin::pin!(listener.watch());
        let session = sessions.next().await.unwrap().unwrap();
        let mut messages = std::pin::pin!(session.messages());
        assert!(messages.next().await.unwrap().is_ok());

        std::fs::remove_file(&path).unwrap();
        // A change signalled for the vanished path may surface as an I/O error first; the
        // stream must still end once the watcher drops the file's handler. Removal is detected
        // by a filesystem event or, if that is missed, by the periodic full scan (the content
        // poll cannot see a vanished file).
        loop {
            match messages.next().await {
                None => break,
                Some(Err(_)) => {}
                Some(Ok(m)) => panic!("unexpected message after removal: {m:?}"),
            }
        }
    }

    /// The fixture records usage per model call and snapshots it after each: only the nine
    /// records count.
    #[rstest]
    #[tokio::test]
    async fn a_real_rollout_counts_each_recorded_call_once() {
        let path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/codex/session1.jsonl");
        let session = CodexSession::open(SessionId::from("s".to_owned()), path, pool());
        let lines: Vec<CodexMessage> = session.read().try_collect().await.unwrap();
        let usage: Vec<&CodexMessage> = lines.iter().filter(|m| m.usage().is_some()).collect();
        assert_eq!(usage.len(), 9);
        assert!(usage.iter().all(|m| m.kind == TOKEN_USAGE_RECORD && m.turn_id().is_some()));
    }

    #[rstest]
    #[case(include_str!("../../../tests/fixtures/codex/session1.jsonl"))]
    fn normalizes_a_real_redacted_session(#[case] jsonl: &str) {
        let msgs: Vec<CodexMessage> = jsonl
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| serde_json::from_str::<CodexMessage>(l).expect("fixture record parses"))
            .collect();
        assert!(msgs.len() >= 15);

        let mut saw_user = false;
        let mut saw_assistant = false;
        let mut tool_uses = 0usize;
        let mut tool_results = 0usize;
        for m in &msgs {
            let _ = m.timestamp();
            saw_user |= m.role() == Role::User;
            saw_assistant |= m.role() == Role::Assistant;
            for c in m.content() {
                match c {
                    Content::ToolUse(u) => {
                        assert!(!u.name.is_empty(), "custom_tool_call normalized to an empty name");
                        assert!(!u.id.to_string().is_empty(), "tool call lost its id");
                        tool_uses += 1;
                    }
                    Content::ToolResult(r) => {
                        assert!(!r.call.to_string().is_empty(), "tool result lost its call id");
                        tool_results += 1;
                    }
                    _ => {}
                }
            }
        }
        assert!(saw_user && saw_assistant, "expected both user and assistant turns");
        assert!(tool_uses >= 1, "expected at least one normalized tool call");
        assert!(tool_results >= 1, "expected at least one normalized tool result");
    }

    fn line(raw: &serde_json::Value) -> CodexMessage {
        serde_json::from_str(&raw.to_string()).unwrap()
    }

    fn token_usage(input: u64, cached: u64, output: u64) -> serde_json::Value {
        serde_json::json!({
            "input_tokens": input, "cached_input_tokens": cached, "cache_write_input_tokens": 0,
            "output_tokens": output, "reasoning_output_tokens": 0, "total_tokens": input + output,
        })
    }

    fn token_count(total: &serde_json::Value, last: &serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "timestamp": "2025-09-18T10:00:00.000Z", "type": "event_msg",
            "payload": {"type": "token_count", "info": {
                "total_token_usage": total, "last_token_usage": last,
                "model_context_window": 258_400,
            }, "rate_limits": null},
        })
    }

    fn usage_record(response: &str, usage: &serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "timestamp": "2026-09-18T10:00:00.000Z", "type": "token_usage_record",
            "payload": {"turn_id": "t1", "response_id": response, "usage": usage,
                "turn_token_usage": usage, "thread_token_usage": usage},
        })
    }

    /// Rollouts written before `token_usage_record` existed report usage only on
    /// `event_msg`/`token_count`: `info.last_token_usage` is the per-call delta.
    #[rstest]
    fn legacy_token_count_usage_is_captured() {
        let m = line(&token_count(
            &token_usage(30_000, 20_000, 300),
            &token_usage(15_000, 10_000, 100),
        ));
        assert_eq!(
            m.usage(),
            Some(Usage {
                input: Some(5_000),
                output: Some(100),
                cache_read: Some(10_000),
                cache_write: Some(0),
            })
        );
        assert!(m.turn_id().is_some(), "usage-bearing line without a turn id");
        assert!(m.id().is_some());
    }

    /// Codex re-sends the same snapshot (a rate-limit refresh); it names the same model call,
    /// so the pipeline counts it once. Another call's snapshot names a different one.
    #[rstest]
    fn repeated_token_count_snapshots_name_one_turn() {
        let first = line(&token_count(&token_usage(30, 20, 3), &token_usage(15, 10, 1)));
        let again = line(&token_count(&token_usage(30, 20, 3), &token_usage(15, 10, 1)));
        let next = line(&token_count(&token_usage(45, 30, 4), &token_usage(15, 10, 1)));
        assert_eq!(first.turn_id(), again.turn_id());
        assert_eq!(first.id(), again.id());
        assert_ne!(first.turn_id(), next.turn_id());
        assert_ne!(first.id(), next.id());
    }

    /// Snapshots that report no new tokens: rate limits only, an estimate after compaction
    /// (`recompute_token_usage`), the window filled after an overflow (`fill_to_context_window`).
    #[rstest]
    #[case(serde_json::json!({"timestamp": "2025-09-18T10:00:00.000Z", "type": "event_msg",
        "payload": {"type": "token_count", "info": null, "rate_limits": {}}}))]
    #[case(token_count(&token_usage(30, 20, 3), &serde_json::json!({"input_tokens": 0,
        "cached_input_tokens": 0, "output_tokens": 0, "reasoning_output_tokens": 0, "total_tokens": 900})))]
    fn token_count_without_new_tokens_has_no_usage(#[case] raw: serde_json::Value) {
        let m = line(&raw);
        assert_eq!(m.usage(), None);
        assert_eq!(m.turn_id(), None);
        assert_eq!(m.id(), None, "an empty snapshot would keep a row");
    }

    /// Modern rollouts write a `token_usage_record` per model call before the `token_count`
    /// snapshot of the same call (codex-rs `turn.rs`: `record_observed_response_completed`,
    /// then `send_token_count_event`); once one is seen, snapshots repeat usage already counted.
    #[rstest]
    fn token_count_after_a_usage_record_has_no_usage() {
        let mut m = line(&token_count(&token_usage(30, 20, 3), &token_usage(15, 10, 1)));
        m.context.usage_recorded = true;
        assert_eq!(m.usage(), None);
        assert_eq!(m.turn_id(), None);
        assert_eq!(m.id(), None);
    }

    /// Codex/OpenAI `input_tokens` already includes `cached_input_tokens`
    /// (codex-rs `TokenUsage::non_cached_input`), where `Usage::input` is fresh input only.
    #[rstest]
    fn usage_input_excludes_cached_input() {
        let m = line(&usage_record("resp_1", &token_usage(14_161, 9_984, 123)));
        let usage = m.usage().unwrap();
        assert_eq!(usage.cache_read, Some(9_984));
        assert_eq!(usage.input, Some(14_161 - 9_984), "cached input counted as fresh input too");
    }

    /// A usage record names its model call by `response_id`, which also keys its row: the line
    /// has no id, and id-less lines in one millisecond would otherwise collide.
    #[rstest]
    fn usage_record_is_keyed_by_its_response() {
        let a = line(&usage_record("resp_a", &token_usage(10, 0, 5)));
        let b = line(&usage_record("resp_b", &token_usage(10, 0, 5)));
        assert_eq!(a.turn_id().as_deref(), Some("resp_a"));
        assert_ne!(a.id(), b.id());
        assert!(a.id().is_some());
    }

    /// `session_meta` carries `git: {commit_hash, branch, repository_url}`
    /// (codex-rs `SessionMetaLine.git`).
    #[rstest]
    fn session_meta_git_branch_is_read() {
        let m = line(&serde_json::json!({
            "timestamp": "2026-09-18T10:00:00.000Z", "type": "session_meta",
            "payload": {"id": "child", "cwd": "/work", "git": {"branch": "main", "commit_hash": "abc"}},
        }));
        assert_eq!(m.git_branch().as_deref(), Some("main"));
    }

    /// A forked or subagent rollout names its origin in `session_meta.forked_from_id` /
    /// `parent_thread_id` (codex-rs `SessionMeta`), or, in older rollouts, under
    /// `source.subagent.thread_spawn`.
    #[rstest]
    #[case(serde_json::json!({"id": "child", "cwd": "/work", "forked_from_id": "parent"}))]
    #[case(serde_json::json!({"id": "child", "cwd": "/work", "parent_thread_id": "parent"}))]
    #[case(serde_json::json!({"id": "child", "cwd": "/work",
        "source": {"subagent": {"thread_spawn": {"parent_thread_id": "parent", "depth": 1}}}}))]
    fn session_meta_names_its_parent(#[case] payload: serde_json::Value) {
        let m = line(&serde_json::json!({
            "timestamp": "2026-09-18T10:00:00.000Z", "type": "session_meta", "payload": payload,
        }));
        assert_eq!(m.parent_session(), Some(SessionId::from("parent".to_owned())));
    }

    /// A fork copies its parent's rollout, `session_meta` included, after its own
    /// (codex-rs `ForkPersistence::Copied`). The copy describes the parent: it must not
    /// replace the child's parent link (with the grandparent) or its session context.
    #[rstest]
    #[tokio::test]
    async fn a_fork_keeps_its_own_session_meta() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rollout-child.jsonl");
        let body = [
            serde_json::json!({"type": "session_meta",
                "payload": {"id": "child", "forked_from_id": "parent", "cwd": "/work"}}),
            serde_json::json!({"type": "session_meta", "payload": {"id": "parent",
                "forked_from_id": "grandparent", "cwd": "/elsewhere", "git": {"branch": "old"}}}),
        ]
        .map(|l| l.to_string() + "\n")
        .concat();
        std::fs::write(&path, body).unwrap();

        let session = CodexSession::open(SessionId::from("child".to_owned()), path, pool());
        let lines: Vec<CodexMessage> = session.read().try_collect().await.unwrap();
        let parents: Vec<_> = lines.iter().map(Message::parent_session).collect();
        assert_eq!(parents, vec![Some(SessionId::from("parent".to_owned())), None]);
        assert_eq!(lines[1].cwd(), None);
        assert_eq!(lines[1].git_branch(), None);
        assert_eq!(lines[1].id(), None);
    }

    fn write_lines(path: &Path, lines: &[serde_json::Value]) {
        let body: String = lines.iter().map(|l| l.to_string() + "\n").collect();
        std::fs::write(path, body).unwrap();
    }

    /// Usage over a whole rollout: a legacy one counts its snapshots, a modern one only its
    /// records, and one written by both (a legacy session resumed by a newer Codex) its
    /// snapshots until records begin.
    #[rstest]
    #[case::legacy(vec![
        token_count(&token_usage(10, 0, 1), &token_usage(10, 0, 1)),
        token_count(&token_usage(30, 0, 3), &token_usage(20, 0, 2)),
    ], vec![1, 2])]
    #[case::modern(vec![
        usage_record("resp_a", &token_usage(10, 0, 1)),
        token_count(&token_usage(10, 0, 1), &token_usage(10, 0, 1)),
        usage_record("resp_b", &token_usage(20, 0, 2)),
        token_count(&token_usage(30, 0, 3), &token_usage(20, 0, 2)),
    ], vec![1, 2])]
    #[case::resumed(vec![
        token_count(&token_usage(10, 0, 1), &token_usage(10, 0, 1)),
        usage_record("resp_b", &token_usage(20, 0, 2)),
        token_count(&token_usage(30, 0, 3), &token_usage(20, 0, 2)),
    ], vec![1, 2])]
    #[tokio::test]
    async fn a_rollout_reports_each_call_once(
        #[case] lines: Vec<serde_json::Value>,
        #[case] expected: Vec<u64>,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rollout-s.jsonl");
        write_lines(&path, &lines);
        let session = CodexSession::open(SessionId::from("s".to_owned()), path, pool());
        let outputs: Vec<u64> = session
            .messages()
            .try_filter_map(|m| async move { Ok(m.usage().and_then(|u| u.output)) })
            .try_collect()
            .await
            .unwrap();
        assert_eq!(outputs, expected);
    }

    /// Resumed past a usage record, a snapshot still knows the rollout records usage: the
    /// reader looks the lines it skipped up in the file.
    #[rstest]
    #[case::from_messages(true)]
    #[case::from_message_at(false)]
    #[tokio::test]
    async fn a_resumed_reader_still_knows_usage_is_recorded(#[case] follow: bool) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rollout-s.jsonl");
        let record = usage_record("resp_a", &token_usage(10, 0, 1));
        write_lines(&path, &[
            record.clone(),
            token_count(&token_usage(10, 0, 1), &token_usage(10, 0, 1)),
        ]);
        let past_record = u64::try_from(record.to_string().len() + 1).unwrap();
        let end = std::fs::metadata(&path).unwrap().len();

        let session = CodexSession::open(SessionId::from("s".to_owned()), path, pool());
        let snapshot = if follow {
            let mut lines = std::pin::pin!(session.messages_from(past_record));
            lines.next().await.unwrap().unwrap().1
        } else {
            session.message_at(end).await.unwrap()
        };
        assert!(snapshot.is_token_count());
        assert_eq!(snapshot.usage(), None, "a recorded call counted twice after a resume");
    }

    #[rstest]
    #[case(b"abc", 3, b"abc".as_slice(), true)]
    #[case(b"abc", 2, b"abc".as_slice(), false)]
    #[case(b"xyz", 3, b"abc".as_slice(), false)]
    fn prefix_contains_stops_at_the_end(
        #[case] body: &[u8],
        #[case] end: u64,
        #[case] needle: &[u8],
        #[case] expected: bool,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        std::fs::write(&path, body).unwrap();
        assert_eq!(prefix_contains(&path, end, needle).unwrap(), expected);
    }

    /// A marker split across two read chunks is still found.
    #[rstest]
    fn prefix_contains_finds_a_needle_across_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        let mut body = vec![b'x'; 64 * 1024 - 5];
        body.extend_from_slice(TOKEN_USAGE_RECORD_MARKER);
        std::fs::write(&path, &body).unwrap();
        let len = u64::try_from(body.len()).unwrap();
        assert!(prefix_contains(&path, len, TOKEN_USAGE_RECORD_MARKER).unwrap());
    }

    /// A `compacted` line carries the compaction summary in `payload.message` (codex-rs
    /// `CompactedItemWire.message`).
    #[rstest]
    fn compacted_summary_is_kept() {
        let m = line(&serde_json::json!({
            "timestamp": "2026-09-18T10:00:00.000Z", "type": "compacted",
            "payload": {"message": "Summary of the conversation so far", "replacement_history": []},
        }));
        assert_eq!(m.content(), vec![Content::Summary(
            "Summary of the conversation so far".to_owned()
        )]);
        assert_eq!(m.role(), Role::System);
    }

    /// `turn_complete` persists terminal error details in `payload.error` (codex-rs
    /// `TurnCompleteEvent.error`), `turn_aborted` its reason (`TurnAbortedEvent.reason`).
    #[rstest]
    #[case(serde_json::json!({"type": "turn_complete", "turn_id": "t1", "last_agent_message": null,
        "error": {"message": "stream disconnected before completion", "codex_error_info": null}}),
        Some((StopReason::Error, "stream disconnected before completion")))]
    #[case(serde_json::json!({"type": "turn_aborted", "turn_id": "t1", "reason": "interrupted"}),
        Some((StopReason::Aborted, "interrupted")))]
    #[case(serde_json::json!({"type": "turn_complete", "turn_id": "t1", "last_agent_message": "ok"}),
        None)]
    fn turn_failures_report_an_error(
        #[case] payload: serde_json::Value,
        #[case] expected: Option<(StopReason, &str)>,
    ) {
        let m = line(&serde_json::json!({
            "timestamp": "2026-09-18T10:00:00.000Z", "type": "event_msg", "payload": payload,
        }));
        assert_eq!(m.stop_reason(), expected.as_ref().map(|(reason, _)| reason.clone()));
        let errors: Vec<Content> =
            expected.iter().map(|(_, why)| Content::Error((*why).to_owned())).collect();
        assert_eq!(m.content(), errors);
    }

    /// Codex injects AGENTS.md, `<environment_context>` and other context as `role: "user"`
    /// messages (codex-rs `core/src/context/contextual_user_message.rs`,
    /// `is_contextual_user_fragment`); they are not user turns.
    #[rstest]
    #[case("# AGENTS.md instructions for /work\n\n<INSTRUCTIONS>\nbe nice\n</INSTRUCTIONS>")]
    #[case("<environment_context>\n  <cwd>/work</cwd>\n</environment_context>")]
    #[case("<user_instructions>\nbe nice\n</user_instructions>")]
    #[case("<skill>\n<name>demo</name>\n</skill>")]
    #[case("<external_ide_context>open: a.rs</external_ide_context>")]
    #[case("<codex_internal_context source=\"goal\">\nx\n</codex_internal_context>")]
    #[case("<hook_prompt hook_run_id=\"h1\">do it</hook_prompt>")]
    #[case(
        "Warning: apply_patch was requested via exec_command. Use the apply_patch tool instead of \
         exec_command."
    )]
    fn contextual_user_messages_are_system(#[case] text: &str) {
        let m = line(&serde_json::json!({
            "timestamp": "2026-09-18T10:00:00.000Z", "type": "response_item",
            "payload": {"type": "message", "id": "msg_1", "role": "user",
                "content": [{"type": "input_text", "text": text}]},
        }));
        assert_eq!(m.role(), Role::System, "harness-injected context captured as a user turn");
    }

    #[rstest]
    #[case("please read AGENTS.md")]
    #[case("<environment_context> is what codex sends")]
    #[case("<external_a>mismatched</external_b>")]
    fn typed_user_messages_stay_user_turns(#[case] text: &str) {
        let m = line(&serde_json::json!({
            "type": "response_item",
            "payload": {"type": "message", "id": "msg_1", "role": "user",
                "content": [{"type": "input_text", "text": text}]},
        }));
        assert_eq!(m.role(), Role::User);
    }

    /// `developer` is the Responses API system role.
    #[rstest]
    fn developer_role_is_system() {
        let m = line(&serde_json::json!({
            "type": "response_item",
            "payload": {"type": "message", "id": "msg_1", "role": "developer",
                "content": [{"type": "input_text", "text": "<permissions instructions>"}]},
        }));
        assert_eq!(m.role(), Role::System);
    }

    /// Tool calls other than function/custom calls (codex-rs `ResponseItem::LocalShellCall`,
    /// `WebSearchCall`, `ToolSearchCall`) are tool uses too.
    #[rstest]
    #[case(serde_json::json!({"type": "local_shell_call", "id": "lsh_1", "call_id": "c1",
        "status": "completed", "action": {"type": "exec", "command": ["ls"]}}), "c1", "local_shell")]
    #[case(serde_json::json!({"type": "web_search_call", "id": "ws_1", "status": "completed",
        "action": {"type": "search", "query": "rust"}}), "ws_1", "web_search")]
    #[case(serde_json::json!({"type": "tool_search_call", "id": "ts_1", "call_id": "c2",
        "execution": "server", "arguments": {}}), "c2", "tool_search")]
    fn other_tool_calls_normalize_to_tool_use(
        #[case] payload: serde_json::Value,
        #[case] call: &str,
        #[case] name: &str,
    ) {
        let m = line(&serde_json::json!({"type": "response_item", "payload": payload}));
        assert_eq!(m.role(), Role::Assistant);
        assert!(
            matches!(m.content().as_slice(), [Content::ToolUse(u)] if u.id.as_ref() == call && u.name == name),
            "tool call lost: {:?}",
            m.content()
        );
    }

    /// A tool search's results come back as `tool_search_output` under its `call_id`; a local
    /// shell call's as a `function_call_output`.
    #[rstest]
    #[case(serde_json::json!({"type": "tool_search_output", "call_id": "c2", "status": "completed",
        "execution": "server", "tools": []}), "c2#out")]
    #[case(serde_json::json!({"type": "function_call_output", "call_id": "c1",
        "output": "files\nProcess exited with code 0"}), "c1#out")]
    fn other_tool_outputs_answer_their_call(#[case] payload: serde_json::Value, #[case] id: &str) {
        let m = line(&serde_json::json!({"type": "response_item", "payload": payload}));
        assert_eq!(m.role(), Role::Tool);
        assert_eq!(m.id(), Some(MessageId::from(id.to_owned())));
        assert!(matches!(m.content().as_slice(), [Content::ToolResult(r)] if !r.error));
    }
}
