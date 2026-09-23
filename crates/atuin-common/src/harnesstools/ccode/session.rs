use std::path::{Path, PathBuf};

use futures::{Stream, TryStreamExt};
use serde::Deserialize;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::sync::watch;
use typed_builder::TypedBuilder;

use crate::fs::tree_watcher::{NodeContext, TreeWatcher};
use crate::harnesstools::ccode::Ccode;
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
pub struct CcodeSessions {
    #[builder(default, setter(strip_option, into))]
    root: Option<PathBuf>,
    /// Runs every file read of the sessions this finds.
    pool: BlockingPool,
}

impl CcodeSessions {
    fn resolve_root(&self) -> PathBuf {
        self.root.clone().unwrap_or_else(|| {
            env_nonempty("CLAUDE_CONFIG_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| home_dir().join(".claude"))
                .join("projects")
        })
    }
}

impl Sessions for CcodeSessions {
    type Listener = CcodeListener;

    fn listener(&self) -> Result<CcodeListener, RuntimeError> {
        let root = self.resolve_root();
        if !root.is_dir() {
            return Err(RuntimeError::NotFound(root));
        }
        Ok(CcodeListener {
            root,
            pool: self.pool.clone(),
        })
    }

    fn existing(
        &self,
    ) -> Result<impl Stream<Item = Result<CcodeSession, RuntimeError>> + Send + 'static, RuntimeError>
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
                        CcodeListener::open_session(path, is_file, &sessions_pool)
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

impl Observable for Ccode {
    type Sessions = CcodeSessions;

    fn sessions(&self, pool: BlockingPool) -> CcodeSessions {
        CcodeSessions::builder().pool(pool).build()
    }
}

#[derive(Debug, Clone)]
pub struct CcodeListener {
    root: PathBuf,
    pool: BlockingPool,
}

impl CcodeListener {
    /// Build a read-once session for an accepted `jsonl` file (no change signal), or `None`.
    fn open_session(path: &Path, is_file: bool, pool: &BlockingPool) -> Option<CcodeSession> {
        if !is_file || path.extension().is_none_or(|ext| ext != "jsonl") {
            return None;
        }
        let id = path.file_stem()?.to_string_lossy().into_owned();
        Some(CcodeSession::open(SessionId::from(id), path.to_path_buf(), pool.clone()))
    }

    /// The session for an accepted file, paired with the change signal the watcher keeps alive
    /// for as long as the file exists.
    fn accept(ctx: &NodeContext, pool: &BlockingPool) -> Option<(CcodeSession, watch::Sender<()>)> {
        let mut session = Self::open_session(ctx.path(), ctx.is_file(), pool)?;
        let (signal, rx) = watch::channel(());
        session.changes = Some(rx);
        Some((session, signal))
    }
}

impl Listener for CcodeListener {
    type Session = CcodeSession;

    fn watch(self) -> impl Stream<Item = Result<CcodeSession, WatchError>> + Send + 'static {
        let root = self.root;
        let pool = self.pool;
        async_stream::stream! {
            let (tx, rx) = flume::unbounded::<CcodeSession>();
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
pub struct CcodeSession {
    id: SessionId,
    path: PathBuf,
    /// Wakes [`messages`](Session::messages) on each change to the file; `None` reads it once.
    changes: Option<watch::Receiver<()>>,
    pool: BlockingPool,
}

impl CcodeSession {
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
}

impl Session for CcodeSession {
    type Message = CcodeMessage;

    fn id(&self) -> SessionId {
        self.id.clone()
    }

    async fn message_at(&self, at: u64) -> Option<CcodeMessage> {
        jsonl::value_at(&self.path, at, &self.pool).await
    }

    fn messages_from(
        self,
        from: u64,
    ) -> impl Stream<Item = Result<(u64, CcodeMessage), MessageError>> + Send + 'static {
        jsonl::follow_from::<CcodeMessage>(self.path, from, self.changes, self.pool)
            .map_err(MessageError::from)
    }

    fn read(&self) -> impl Stream<Item = Result<CcodeMessage, MessageError>> + Send + 'static {
        jsonl::read_all::<CcodeMessage>(self.path.clone(), self.pool.clone())
            .map_err(MessageError::from)
    }
}

/// One line of a Claude Code transcript (`~/.claude/projects/<project>/<session>.jsonl`).
///
/// Loosely typed fields stay [`serde_json::Value`]: a line whose shape a newer Claude Code
/// changed must still parse.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CcodeMessage {
    #[serde(rename = "type")]
    kind: String,
    subtype: Option<String>,
    uuid: Option<String>,
    timestamp: Option<String>,
    message: Option<serde_json::Value>,
    content: Option<serde_json::Value>,
    cwd: Option<PathBuf>,
    git_branch: Option<String>,
    ai_title: Option<String>,
    custom_title: Option<String>,
    /// The title of a legacy `summary` line.
    summary: Option<Box<serde_json::Value>>,
    parent_uuid: Option<String>,
    /// The predecessor of a line written with a null `parentUuid`: a `compact_boundary`.
    logical_parent_uuid: Option<String>,
    session_id: Option<String>,
    /// `{sessionId, messageUuid}` on every line `/branch` (`--fork-session`) copied.
    forked_from: Option<Box<serde_json::Value>>,
    attachment: Option<Box<serde_json::Value>>,
    /// Who submitted a user line: absent or `{"kind": "human"}` for the user.
    origin: Option<Box<serde_json::Value>>,
    is_meta: Option<bool>,
    is_compact_summary: Option<bool>,
    is_api_error_message: Option<bool>,
    /// The error class of an API error line (`rate_limit`, `unknown`, ...).
    error: Option<Box<serde_json::Value>>,
}

/// The model Claude Code names on the assistant lines it writes itself (API errors, canned
/// replies): no model produced them.
const SYNTHETIC_MODEL: &str = "<synthetic>";

/// Tags wrapping output Claude Code captured from a command the user ran (`!cmd`, `/cmd`):
/// execution payload, never text anyone typed.
const OUTPUT_TAGS: [&str; 5] = [
    "local-command-stdout",
    "local-command-stderr",
    "bash-stdout",
    "bash-stderr",
    "bash-exit-code",
];

/// Prefixes of user-role text that Claude Code wrote itself (its own `MN` / `jy`
/// classification): command output, background task notifications, the local-command caveat.
const INJECTED_PREFIXES: [&str; 8] = [
    "<local-command-stdout>",
    "<local-command-stderr>",
    "<bash-stdout>",
    "<bash-stderr>",
    "<local-command-caveat>",
    "<task-notification>",
    "<tick>",
    "[Request interrupted by user",
];

fn ccode_stop_reason(raw: &str) -> StopReason {
    match raw {
        "end_turn" => StopReason::EndTurn,
        "max_tokens" => StopReason::MaxTokens,
        "tool_use" => StopReason::ToolUse,
        "stop_sequence" => StopReason::StopSequence,
        "refusal" => StopReason::Refusal,
        other => StopReason::Other(other.to_owned()),
    }
}

/// The text between the first `<name>` and its `</name>`, or `None` without both.
fn tag<'a>(text: &'a str, name: &str) -> Option<&'a str> {
    let open = format!("<{name}>");
    let close = format!("</{name}>");
    let start = text.find(&open)? + open.len();
    let len = text[start..].find(&close)?;
    Some(&text[start..start + len])
}

/// `text` without any [`OUTPUT_TAGS`] element; an unclosed one runs to the end.
fn strip_output(text: &str) -> String {
    let mut out = text.to_owned();
    for name in OUTPUT_TAGS {
        let open = format!("<{name}>");
        let close = format!("</{name}>");
        while let Some(start) = out.find(&open) {
            let end = out[start..].find(&close).map_or(out.len(), |i| start + i + close.len());
            out.replace_range(start..end, "");
        }
    }
    out
}

/// A user-role text as the user typed it: command output removed, and a slash command
/// (`<command-name>`) or bash-mode input (`<bash-input>`) record rendered as its command line,
/// the way Claude Code itself replays it (`ycr` in CC 2.1.281). `None` when nothing remains.
fn typed_text(text: &str) -> Option<String> {
    let text = strip_output(text);
    let trimmed = text.trim_start();
    if trimmed.starts_with("<command-") {
        if let Some(name) = tag(trimmed, "command-name") {
            let args = tag(trimmed, "command-args").unwrap_or_default().trim();
            return Some(if args.is_empty() {
                name.to_owned()
            } else {
                format!("{name} {args}")
            });
        }
    } else if trimmed.starts_with("<bash-input>")
        && let Some(command) = tag(trimmed, "bash-input")
    {
        return Some(format!("! {command}"));
    }
    (!trimmed.trim_end().is_empty()).then_some(text)
}

/// Whether a user line's or queued command's `origin` names the user: Claude Code writes none,
/// or `{"kind": "human"}`, for a prompt the user submitted (`sE` in CC 2.1.281); anything else
/// (`task-notification`, `peer`, `auto-continuation`, ...) is the harness speaking.
fn human_origin(origin: Option<&serde_json::Value>) -> bool {
    origin.is_none_or(|o| o.is_null() || o["kind"].as_str().is_none_or(|kind| kind == "human"))
}

/// The text of every text block (or a plain string), joined.
fn joined_text(raw: &serde_json::Value) -> Option<String> {
    match raw {
        serde_json::Value::String(text) => Some(text.clone()),
        serde_json::Value::Array(blocks) => {
            let texts: Vec<&str> = blocks.iter().filter_map(|b| b["text"].as_str()).collect();
            (!texts.is_empty()).then(|| texts.join("\n"))
        }
        _ => None,
    }
}

impl CcodeMessage {
    fn block(value: &serde_json::Value) -> Content {
        match value["type"].as_str() {
            Some("text") => Content::Text(value["text"].as_str().unwrap_or_default().to_owned()),
            Some("thinking" | "redacted_thinking") => Content::ReasoningSummary { tokens: None },
            Some("tool_use") => Content::ToolUse(ToolUse {
                id: ToolCallId::from(value["id"].as_str().unwrap_or_default().to_owned()),
                name: value["name"].as_str().unwrap_or_default().to_owned(),
                input: value["input"].clone(),
            }),
            Some("tool_result") => Content::ToolResult(ToolResult {
                call: ToolCallId::from(
                    value["tool_use_id"].as_str().unwrap_or_default().to_owned(),
                ),
                output: value["content"].clone(),
                error: value["is_error"].as_bool().unwrap_or(false),
            }),
            // Pasted media: keep what it was, not its (base64) bytes.
            Some(kind @ ("image" | "document")) => Content::Other(serde_json::json!({
                "type": kind,
                "source": {
                    "type": value["source"]["type"],
                    "media_type": value["source"]["media_type"],
                },
            })),
            _ => Content::Other(value.clone()),
        }
    }

    /// The line's raw content: `message.content`, else the top-level `content` (system lines).
    fn raw_content(&self) -> Option<&serde_json::Value> {
        self.message.as_ref().map(|m| &m["content"]).or(self.content.as_ref())
    }

    /// The line's role as written: `message.role`, else its `type`.
    fn raw_role(&self) -> &str {
        self.message.as_ref().and_then(|m| m["role"].as_str()).unwrap_or(self.kind.as_str())
    }

    /// The text a line opens with, peeked without cloning its blocks.
    fn first_text(&self) -> Option<&str> {
        match self.raw_content()? {
            serde_json::Value::String(text) => Some(text.as_str()),
            serde_json::Value::Array(blocks) => blocks.first().and_then(|b| b["text"].as_str()),
            _ => None,
        }
    }

    /// A synthetic assistant line Claude Code wrote for a failed API call (`isApiErrorMessage`).
    fn is_api_error(&self) -> bool {
        self.is_api_error_message == Some(true)
    }

    /// Claude Code wrote this assistant line itself (an API error, a canned reply).
    fn is_synthetic(&self) -> bool {
        self.is_api_error()
            || self.message.as_ref().is_some_and(|m| m["model"].as_str() == Some(SYNTHETIC_MODEL))
    }

    /// A `queued_command` attachment: a prompt delivered while the agent was busy (the user's,
    /// or a background task's notification).
    fn queued_command(&self) -> Option<&serde_json::Value> {
        self.attachment.as_deref().filter(|a| a["type"].as_str() == Some("queued_command"))
    }

    /// Whether a queued command is one the user typed. Claude Code shows it as the user's turn
    /// when `commandMode` is `prompt`, it is not `isMeta` and its origin is human (`d8` / `Iie`
    /// in CC 2.1.281); a task notification has `commandMode: "task-notification"`.
    fn queued_by_user(queued: &serde_json::Value) -> bool {
        queued["isMeta"].as_bool() != Some(true)
            && queued["commandMode"].as_str().is_none_or(|mode| mode == "prompt")
            && human_origin(queued.get("origin"))
    }

    /// A user-role line Claude Code wrote itself rather than the user typing it.
    fn injected_user_line(&self) -> bool {
        self.is_meta == Some(true)
            || !human_origin(self.origin.as_deref())
            || self.first_text().is_some_and(|text| {
                let text = text.trim_start();
                INJECTED_PREFIXES.iter().any(|prefix| text.starts_with(prefix))
            })
    }

    /// A `local_command` system line recording the slash command the user typed (as opposed
    /// to its output, written as another `local_command` line).
    fn typed_local_command(&self) -> bool {
        self.kind == "system"
            && self.subtype.as_deref() == Some("local_command")
            && self.first_text().is_some_and(|text| text.trim_start().starts_with("<command-"))
    }

    /// Thinking tokens the call reported (`usage.output_tokens_details.thinking_tokens`).
    fn thinking_tokens(&self) -> Option<u64> {
        self.message.as_ref()?["usage"]["output_tokens_details"]["thinking_tokens"].as_u64()
    }
}

impl Message for CcodeMessage {
    fn id(&self) -> Option<MessageId> {
        self.uuid.clone().map(MessageId::from)
    }

    fn role(&self) -> Role {
        if self.kind == "attachment" {
            return match self.queued_command() {
                Some(queued) if Self::queued_by_user(queued) => Role::User,
                Some(_) => Role::System,
                None => Role::Other(self.kind.clone()),
            };
        }
        if self.is_compact_summary == Some(true) {
            return Role::System;
        }
        if self.typed_local_command() {
            return Role::User;
        }
        match self.raw_role() {
            "user" if self.injected_user_line() => Role::System,
            "user" => Role::User,
            "assistant" => Role::Assistant,
            "system" => Role::System,
            "tool" => Role::Tool,
            other => Role::Other(other.to_owned()),
        }
    }

    fn timestamp(&self) -> Option<OffsetDateTime> {
        self.timestamp.as_deref().and_then(|s| OffsetDateTime::parse(s, &Rfc3339).ok())
    }

    fn content(&self) -> Vec<Content> {
        if let Some(queued) = self.queued_command() {
            return match &queued["prompt"] {
                serde_json::Value::String(text) => {
                    typed_text(text).map(Content::Text).into_iter().collect()
                }
                serde_json::Value::Array(blocks) => blocks.iter().map(Self::block).collect(),
                _ => Vec::new(),
            };
        }
        let Some(raw) = self.raw_content() else {
            return Vec::new();
        };
        if self.is_compact_summary == Some(true) {
            return joined_text(raw).map(Content::Summary).into_iter().collect();
        }
        if self.is_api_error() {
            let text =
                joined_text(raw).or_else(|| self.error.as_ref()?.as_str().map(str::to_owned));
            return text.map(Content::Error).into_iter().collect();
        }
        // Everything but model output can hold a record of a command the user ran.
        let typed = self.raw_role() != "assistant";
        let text = |text: &str| {
            if typed {
                typed_text(text).map(Content::Text)
            } else {
                Some(Content::Text(text.to_owned()))
            }
        };
        let mut content: Vec<_> = match raw {
            serde_json::Value::String(s) => text(s).into_iter().collect(),
            serde_json::Value::Array(blocks) => blocks
                .iter()
                .filter_map(|block| match block["type"].as_str() {
                    Some("text") => text(block["text"].as_str().unwrap_or_default()),
                    _ => Some(Self::block(block)),
                })
                .collect(),
            _ => Vec::new(),
        };
        // One reasoning marker per thinking block, so a call split over several lines is
        // marked on the line that did the thinking only. The call's thinking tokens (repeated
        // on every line; the capture engine counts them once per call) ride on that marker.
        if let Some(Content::ReasoningSummary { tokens }) =
            content.iter_mut().find(|block| matches!(block, Content::ReasoningSummary { .. }))
        {
            *tokens = self.thinking_tokens();
        }
        content
    }

    fn model(&self) -> Option<String> {
        if self.is_synthetic() {
            return None;
        }
        self.message.as_ref()?.get("model")?.as_str().map(str::to_owned)
    }

    fn usage(&self) -> Option<Usage> {
        // A line Claude Code wrote itself made no API call; its zeroed usage is not one.
        if self.is_synthetic() {
            return None;
        }
        let usage = self.message.as_ref()?.get("usage")?;
        if usage.is_null() {
            return None;
        }
        let field = |name: &str| usage.get(name).and_then(serde_json::Value::as_u64);
        // The split by cache lifetime, for a writer that leaves out the total.
        let cache_write = field("cache_creation_input_tokens").or_else(|| {
            let split = usage.get("cache_creation")?.as_object()?;
            split.values().filter_map(serde_json::Value::as_u64).reduce(|a, b| a + b)
        });
        Some(Usage {
            input: field("input_tokens"),
            output: field("output_tokens"),
            cache_read: field("cache_read_input_tokens"),
            cache_write,
        })
    }

    fn stop_reason(&self) -> Option<StopReason> {
        if self.is_api_error() {
            return Some(StopReason::Error);
        }
        // An interrupt is recorded as a user line; it is the turn that it ends.
        if self.raw_role() == "user"
            && self
                .first_text()
                .is_some_and(|t| t.trim_start().starts_with("[Request interrupted by user"))
        {
            return Some(StopReason::Aborted);
        }
        Some(ccode_stop_reason(self.message.as_ref()?.get("stop_reason")?.as_str()?))
    }

    fn cwd(&self) -> Option<PathBuf> {
        self.cwd.clone()
    }

    fn git_branch(&self) -> Option<String> {
        self.git_branch.clone()
    }

    fn parent_id(&self) -> Option<MessageId> {
        self.parent_uuid.clone().or_else(|| self.logical_parent_uuid.clone()).map(MessageId::from)
    }

    /// The session a fork was copied from (`forkedFrom`), else the session the line names: a
    /// subagent's (or `/btw` side question's) lines name the session that spawned it.
    fn parent_session(&self) -> Option<SessionId> {
        self.forked_from
            .as_ref()
            .and_then(|f| f["sessionId"].as_str())
            .map(str::to_owned)
            .or_else(|| self.session_id.clone())
            .map(SessionId::from)
    }

    /// The API message id: one per model call, shared by every line the response is split
    /// into, and kept when Claude Code copies the line into a fork or a `/btw` replay.
    fn turn_id(&self) -> Option<String> {
        if self.is_synthetic() {
            return None;
        }
        self.message.as_ref()?.get("id")?.as_str().map(str::to_owned)
    }

    fn title(&self) -> Option<String> {
        let summary = || {
            (self.kind == "summary")
                .then(|| self.summary.as_ref()?.as_str().map(str::to_owned))
                .flatten()
        };
        self.custom_title.clone().or_else(|| self.ai_title.clone()).or_else(summary)
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
    use crate::harnesstools::session::{Message, Session, SessionEvent, Sessions};

    #[allow(clippy::needless_pass_by_value)]
    fn line(kind: &str, role: &str, content: serde_json::Value) -> String {
        serde_json::json!({
            "type": kind,
            "sessionId": "11111111-1111-1111-1111-111111111111",
            "uuid": "aaaa",
            "timestamp": "2026-09-18T10:00:00Z",
            "message": {"role": role, "content": content},
        })
        .to_string()
    }

    #[rstest]
    fn normalizes_a_user_string_message() {
        let raw = line("user", "user", serde_json::json!("hi there"));
        let m: CcodeMessage = serde_json::from_str(&raw).unwrap();
        assert_eq!(m.role(), Role::User);
        assert_eq!(m.content(), vec![Content::Text("hi there".into())]);
    }

    #[rstest]
    fn normalizes_assistant_tool_use_blocks() {
        let raw = line(
            "assistant",
            "assistant",
            serde_json::json!([
                {"type": "text", "text": "running"},
                {"type": "tool_use", "id": "call_1", "name": "Bash", "input": {"cmd": "ls"}},
            ]),
        );
        let m: CcodeMessage = serde_json::from_str(&raw).unwrap();
        assert_eq!(m.role(), Role::Assistant);
        let content = m.content();
        assert_eq!(content.len(), 2);
        assert!(matches!(content[1], Content::ToolUse(_)));
    }

    #[rstest]
    fn normalizes_assistant_enrichment_fields() {
        let raw = serde_json::json!({
            "type": "assistant",
            "uuid": "aaaa",
            "cwd": "/work/atuin",
            "gitBranch": "main",
            "message": {
                "role": "assistant",
                "content": [{"type": "text", "text": "done"}],
                "model": "claude-opus-4-8",
                "stop_reason": "end_turn",
                "usage": {
                    "input_tokens": 10,
                    "output_tokens": 20,
                    "cache_read_input_tokens": 5,
                    "cache_creation_input_tokens": 2,
                },
            },
        })
        .to_string();
        let m: CcodeMessage = serde_json::from_str(&raw).unwrap();
        assert_eq!(m.model(), Some("claude-opus-4-8".to_owned()));
        assert_eq!(m.stop_reason(), Some(StopReason::EndTurn));
        assert_eq!(m.cwd(), Some(PathBuf::from("/work/atuin")));
        assert_eq!(m.git_branch(), Some("main".to_owned()));
        assert_eq!(
            m.usage(),
            Some(Usage {
                input: Some(10),
                output: Some(20),
                cache_read: Some(5),
                cache_write: Some(2)
            })
        );
    }

    #[rstest]
    #[case("max_tokens", StopReason::MaxTokens)]
    #[case("tool_use", StopReason::ToolUse)]
    #[case("stop_sequence", StopReason::StopSequence)]
    #[case("refusal", StopReason::Refusal)]
    #[case("weird", StopReason::Other("weird".to_owned()))]
    fn maps_stop_reason_vocabulary(#[case] raw: &str, #[case] expected: StopReason) {
        let m: CcodeMessage = serde_json::from_str(
            &serde_json::json!({
                "type": "assistant",
                "message": {"role": "assistant", "content": [], "stop_reason": raw},
            })
            .to_string(),
        )
        .unwrap();
        assert_eq!(m.stop_reason(), Some(expected));
    }

    #[rstest]
    fn enrichment_is_none_when_the_harness_did_not_provide_it() {
        let raw = line("user", "user", serde_json::json!("hi there"));
        let m: CcodeMessage = serde_json::from_str(&raw).unwrap();
        assert_eq!(m.model(), None);
        assert_eq!(m.usage(), None);
        assert_eq!(m.stop_reason(), None);
        assert_eq!(m.cwd(), None);
        assert_eq!(m.git_branch(), None);
    }

    #[rstest]
    fn exposes_parent_line_parent_session_and_turn() {
        let m: CcodeMessage = serde_json::from_str(
            &serde_json::json!({
                "type": "assistant",
                "uuid": "bbbb",
                "parentUuid": "aaaa",
                "sessionId": "p",
                "message": {"role": "assistant", "id": "msg_01", "content": []},
            })
            .to_string(),
        )
        .unwrap();
        assert_eq!(m.parent_id(), Some(MessageId::from("aaaa".to_owned())));
        assert_eq!(m.parent_session(), Some(SessionId::from("p".to_owned())));
        assert_eq!(m.turn_id().as_deref(), Some("msg_01"));
    }

    #[rstest]
    #[case(serde_json::json!({"type": "ai-title", "aiTitle": "generated"}), "generated")]
    #[case(serde_json::json!({"type": "custom-title", "customTitle": "by hand"}), "by hand")]
    fn title_lines_expose_the_title(#[case] raw: serde_json::Value, #[case] expected: &str) {
        let m: CcodeMessage = serde_json::from_str(&raw.to_string()).unwrap();
        assert_eq!(m.title().as_deref(), Some(expected));
        assert!(m.content().is_empty());
    }

    #[rstest]
    #[case(
        serde_json::json!({"type": "user", "isCompactSummary": true,
            "message": {"role": "user", "content": "summary"}}),
    )]
    #[case(
        serde_json::json!({"type": "system", "subtype": "compact_boundary",
            "compactMetadata": {"trigger": "auto"}, "content": "boundary"}),
    )]
    fn compaction_lines_are_system(#[case] raw: serde_json::Value) {
        let m: CcodeMessage = serde_json::from_str(&raw.to_string()).unwrap();
        assert_eq!(m.role(), Role::System);
    }

    #[rstest]
    #[case("[Request interrupted by user]", Some(StopReason::Aborted))]
    #[case("[Request interrupted by user for tool use]", Some(StopReason::Aborted))]
    #[case("please continue", None)]
    fn interrupt_lines_end_the_turn(#[case] text: &str, #[case] expected: Option<StopReason>) {
        let raw = line("user", "user", serde_json::json!([{"type": "text", "text": text}]));
        let m: CcodeMessage = serde_json::from_str(&raw).unwrap();
        assert_eq!(m.stop_reason(), expected);
    }

    #[rstest]
    fn listener_reports_not_found_for_a_missing_root() {
        let sessions =
            CcodeSessions::builder().root(PathBuf::from("/no/such/claude")).pool(pool()).build();
        assert!(matches!(sessions.listener(), Err(RuntimeError::NotFound(_))));
    }

    #[rstest]
    fn listener_opens_an_existing_root() {
        let dir = tempfile::tempdir().unwrap();
        let sessions = CcodeSessions::builder().root(dir.path().to_path_buf()).pool(pool()).build();
        assert!(sessions.listener().is_ok());
    }

    #[rstest]
    #[tokio::test]
    async fn messages_streams_each_turn_of_a_session_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("session.jsonl");
        // Trailing newline required: messages() withholds an unterminated final line until a
        // later write completes it (a real session ends every record with a newline).
        let body = [
            line("user", "user", serde_json::json!("first")),
            line("assistant", "assistant", serde_json::json!([{"type": "text", "text": "second"}])),
        ]
        .join("\n")
            + "\n";
        std::fs::write(&path, body).unwrap();

        let session = CcodeSession::open(
            SessionId::from("11111111-1111-1111-1111-111111111111".to_owned()),
            path,
            pool(),
        );
        let got: Vec<Role> =
            session.messages().take(2).map_ok(|m| m.role()).try_collect().await.unwrap();
        assert_eq!(got, vec![Role::User, Role::Assistant]);
    }

    #[rstest]
    #[tokio::test]
    async fn watch_emits_sessions_as_files_appear() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("project-a");
        std::fs::create_dir_all(&sub).unwrap();
        std::fs::write(
            sub.join("22222222-2222-2222-2222-222222222222.jsonl"),
            line("user", "user", serde_json::json!("hi")),
        )
        .unwrap();

        let listener = CcodeSessions::builder()
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
        assert_eq!(seen.len(), 1);
    }

    #[rstest]
    #[tokio::test]
    async fn events_yields_messages_tagged_with_their_session() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("project-a");
        std::fs::create_dir_all(&sub).unwrap();
        // Trailing newline required: each session's messages() withholds an unterminated final
        // line until a later write completes it (a real session ends every record with a newline).
        let body = [
            line("user", "user", serde_json::json!("hi")),
            line("assistant", "assistant", serde_json::json!([{"type": "text", "text": "yo"}])),
        ]
        .join("\n")
            + "\n";
        std::fs::write(sub.join("33333333-3333-3333-3333-333333333333.jsonl"), &body).unwrap();

        let listener = CcodeSessions::builder()
            .root(dir.path().to_path_buf())
            .pool(pool())
            .build()
            .listener()
            .unwrap();
        let events: Vec<SessionEvent<CcodeMessage>> = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            listener
                .events(|_| std::future::ready(0), |_, _| std::future::ready(true))
                .take(2)
                .try_collect(),
        )
        .await
        .expect("events() did not produce within 10s")
        .unwrap();

        let sid = SessionId::from("33333333-3333-3333-3333-333333333333".to_owned());
        assert!(events.iter().all(|event| event.session == sid));
        let roles: Vec<Role> = events.iter().map(|event| event.message.role()).collect();
        assert_eq!(roles, vec![Role::User, Role::Assistant]);
        // Each event carries the offset past its line; the last one is the file's length.
        assert!(events[0].offset < events[1].offset);
        assert_eq!(events[1].offset, u64::try_from(body.len()).unwrap());
    }

    /// The offset a caller resumes from is honoured: only lines past it are yielded.
    #[rstest]
    #[tokio::test]
    async fn events_resume_each_session_from_the_given_offset() {
        let dir = tempfile::tempdir().unwrap();
        let first = line("user", "user", serde_json::json!("hi")) + "\n";
        let body = first.clone()
            + &line("assistant", "assistant", serde_json::json!([{"type": "text", "text": "yo"}]))
            + "\n";
        std::fs::write(dir.path().join("66666666-6666-6666-6666-666666666666.jsonl"), &body)
            .unwrap();

        let listener = CcodeSessions::builder()
            .root(dir.path().to_path_buf())
            .pool(pool())
            .build()
            .listener()
            .unwrap();
        let start = u64::try_from(first.len()).unwrap();
        let events: Vec<SessionEvent<CcodeMessage>> = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            listener
                .events(move |_| std::future::ready(start), |_, _| std::future::ready(true))
                .take(1)
                .try_collect(),
        )
        .await
        .expect("events() did not produce within 10s")
        .unwrap();
        assert_eq!(events[0].message.role(), Role::Assistant);
        assert_eq!(events[0].offset, u64::try_from(body.len()).unwrap());
    }

    #[rstest]
    #[tokio::test]
    async fn messages_yields_lines_appended_after_discovery() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("44444444-4444-4444-4444-444444444444.jsonl");
        std::fs::write(&path, line("user", "user", serde_json::json!("hi")) + "\n").unwrap();

        let listener = CcodeSessions::builder()
            .root(dir.path().to_path_buf())
            .pool(pool())
            .build()
            .listener()
            .unwrap();
        // The watch stream owns the watcher: it must outlive the message stream.
        let mut sessions = std::pin::pin!(listener.watch());
        let session = sessions.next().await.unwrap().unwrap();
        let mut messages = std::pin::pin!(session.messages());
        assert_eq!(messages.next().await.unwrap().unwrap().role(), Role::User);

        let mut file = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
        std::io::Write::write_all(
            &mut file,
            (line("assistant", "assistant", serde_json::json!([{"type": "text", "text": "yo"}]))
                + "\n")
                .as_bytes(),
        )
        .unwrap();
        drop(file);
        assert_eq!(messages.next().await.unwrap().unwrap().role(), Role::Assistant);
    }

    #[rstest]
    #[tokio::test]
    async fn messages_ends_when_the_session_file_is_removed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("55555555-5555-5555-5555-555555555555.jsonl");
        std::fs::write(&path, line("user", "user", serde_json::json!("hi")) + "\n").unwrap();

        let listener = CcodeSessions::builder()
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

    #[rstest]
    #[case(include_str!("../../../tests/fixtures/ccode/session1.jsonl"))]
    #[case(include_str!("../../../tests/fixtures/ccode/session2.jsonl"))]
    #[case(include_str!("../../../tests/fixtures/ccode/session3.jsonl"))]
    fn normalizes_a_real_redacted_session(#[case] jsonl: &str) {
        let msgs: Vec<CcodeMessage> = jsonl
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| serde_json::from_str::<CcodeMessage>(l).expect("fixture record parses"))
            .collect();
        assert!(msgs.len() >= 20);

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
                        assert!(!u.name.is_empty(), "tool_use normalized to an empty name");
                        assert!(!u.id.to_string().is_empty(), "tool_use normalized to an empty id");
                        tool_uses += 1;
                    }
                    Content::ToolResult(r) => {
                        assert!(!r.call.to_string().is_empty(), "tool_result lost its call id");
                        tool_results += 1;
                    }
                    _ => {}
                }
            }
        }
        assert!(saw_user && saw_assistant, "expected both user and assistant turns");
        assert!(tool_uses >= 1, "expected at least one normalized tool_use");
        assert!(tool_results >= 1, "expected at least one normalized tool_result");
    }

    fn parse(raw: &serde_json::Value) -> CcodeMessage {
        serde_json::from_str(&raw.to_string()).unwrap()
    }

    /// Legacy `summary` lines carry the session title Claude Code's `/resume` shows
    /// (`r.set(kn.leafUuid, kn.summary)` in CC 2.1.281).
    #[rstest]
    fn summary_line_exposes_its_title() {
        let m = parse(&serde_json::json!({
            "type": "summary", "summary": "Fix the flaky sync test", "leafUuid": "u9",
        }));
        assert_eq!(m.title().as_deref(), Some("Fix the flaky sync test"));
        assert!(m.content().is_empty());
    }

    /// A prompt the user types while the agent is busy is written only as a `queued_command`
    /// attachment (`attachment.prompt`); Claude Code treats it as the user's turn.
    #[rstest]
    #[case::origin_human(serde_json::json!({"origin": {"kind": "human"}}))]
    #[case::prompt_mode(serde_json::json!({"commandMode": "prompt"}))]
    fn queued_prompt_is_user_text(#[case] extra: serde_json::Value) {
        let mut attachment =
            serde_json::json!({"type": "queued_command", "prompt": "also check forks please"});
        attachment.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
        let m = parse(&serde_json::json!({
            "type": "attachment", "uuid": "a1", "parentUuid": "u0", "sessionId": "s",
            "timestamp": "2026-09-23T22:44:00Z", "attachment": attachment,
        }));
        assert_eq!(m.role(), Role::User);
        assert_eq!(m.content(), vec![Content::Text("also check forks please".into())]);
    }

    /// Background task results are delivered through the same queue; they are not the user's.
    #[rstest]
    #[case::task_notification(serde_json::json!({"commandMode": "task-notification"}))]
    #[case::peer(serde_json::json!({"origin": {"kind": "peer", "from": "a1"}}))]
    #[case::meta(serde_json::json!({"isMeta": true}))]
    fn queued_harness_message_is_system(#[case] extra: serde_json::Value) {
        let mut attachment = serde_json::json!({"type": "queued_command",
            "prompt": "<task-notification>\n<task-id>b1</task-id>\n</task-notification>"});
        attachment.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
        let m = parse(&serde_json::json!({"type": "attachment", "uuid": "a1",
            "attachment": attachment}));
        assert_eq!(m.role(), Role::System);
    }

    /// `queue-operation` lines are Claude Code's queue bookkeeping. `remove` records a command
    /// leaving the queue, whether delivered or discarded (`cr(..., commandsDiscarded)` in CC
    /// 2.1.281), and is written for task notifications too. claude-code-log renders it as
    /// steering only for Claude Code before ~2.1.101; later versions write a `queued_command`
    /// attachment for each delivery, which is where the user's text is taken from. Reading
    /// `remove` as user text would capture it twice.
    #[rstest]
    #[case("remove")]
    #[case("enqueue")]
    #[case("popAll")]
    fn queue_operation_is_not_user_text(#[case] operation: &str) {
        let m = parse(&serde_json::json!({
            "type": "queue-operation", "operation": operation, "sessionId": "s",
            "timestamp": "2026-09-23T22:44:00Z", "content": "stop and use rstest",
        }));
        assert_ne!(m.role(), Role::User);
    }

    /// `compact_boundary` lines are written with `parentUuid: null` and the real predecessor in
    /// `logicalParentUuid`.
    #[rstest]
    fn compact_boundary_keeps_its_logical_parent() {
        let m = parse(&serde_json::json!({
            "type": "system", "subtype": "compact_boundary", "uuid": "b1",
            "parentUuid": null, "logicalParentUuid": "u41",
            "content": "Conversation compacted", "compactMetadata": {"trigger": "auto"},
        }));
        assert_eq!(m.parent_id(), Some(MessageId::from("u41".to_owned())));
    }

    /// The compaction summary is the only record of the conversation it replaced.
    #[rstest]
    #[case(serde_json::json!("Summary: fixed forks"))]
    #[case(serde_json::json!([{"type": "text", "text": "Summary: fixed forks"}]))]
    fn compact_summary_is_a_summary(#[case] content: serde_json::Value) {
        let m = parse(&serde_json::json!({
            "type": "user", "uuid": "c1", "isCompactSummary": true,
            "message": {"role": "user", "content": content},
        }));
        assert_eq!(m.content(), vec![Content::Summary("Summary: fixed forks".into())]);
    }

    /// `/branch` (`--fork-session`) copies each line with `sessionId` rewritten to the new session
    /// and the origin in `forkedFrom.sessionId`.
    #[rstest]
    fn forked_line_names_the_session_it_was_forked_from() {
        let m = parse(&serde_json::json!({
            "type": "user", "uuid": "u1", "parentUuid": null, "sessionId": "new",
            "forkedFrom": {"sessionId": "old", "messageUuid": "u1"},
            "message": {"role": "user", "content": "hi"},
        }));
        assert_eq!(m.parent_session(), Some(SessionId::from("old".to_owned())));
    }

    /// Synthetic API-error assistant lines (`isApiErrorMessage`, model `<synthetic>`,
    /// `stop_reason: "stop_sequence"`, zero usage) are a failed call, not a model's reply.
    #[rstest]
    fn api_error_line_is_an_error_not_a_synthetic_model() {
        let m = parse(&serde_json::json!({
            "type": "assistant", "uuid": "e1", "isApiErrorMessage": true,
            "error": "rate_limit",
            "message": {"id": "0b5c", "role": "assistant", "model": "<synthetic>",
                "stop_reason": "stop_sequence", "stop_sequence": "",
                "content": [{"type": "text", "text": "API Error: 529 Overloaded"}],
                "usage": {"input_tokens": 0, "output_tokens": 0,
                    "cache_creation_input_tokens": 0, "cache_read_input_tokens": 0}},
        }));
        assert_eq!(m.stop_reason(), Some(StopReason::Error));
        assert_eq!(m.model(), None, "`<synthetic>` is not a model");
        assert_eq!(m.usage(), None);
        assert_eq!(m.turn_id(), None);
        assert_eq!(m.content(), vec![Content::Error("API Error: 529 Overloaded".into())]);
    }

    /// Lines Claude Code writes into the user's side of the conversation are not user text.
    #[rstest]
    #[case::meta(serde_json::json!({"isMeta": true,
        "message": {"role": "user", "content": "<local-command-caveat>Caveat</local-command-caveat>"}}))]
    #[case::task_notification(serde_json::json!({"origin": {"kind": "task-notification"},
        "message": {"role": "user", "content": "<task-notification>done</task-notification>"}}))]
    #[case::peer(serde_json::json!({"origin": {"kind": "peer"},
        "message": {"role": "user", "content": "<agent-message>report</agent-message>"}}))]
    #[case::command_output(serde_json::json!({
        "message": {"role": "user", "content": "<local-command-stdout>ok</local-command-stdout>"}}))]
    #[case::interrupt(serde_json::json!({
        "message": {"role": "user", "content": [{"type": "text", "text": "[Request interrupted by user]"}]}}))]
    fn injected_user_line_is_system(#[case] extra: serde_json::Value) {
        let mut raw = serde_json::json!({"type": "user", "uuid": "m1"});
        raw.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
        assert_eq!(parse(&raw).role(), Role::System);
    }

    /// Command output never leaves the parser; the command the user typed does, as they typed it.
    #[rstest]
    #[case::stdout("<local-command-stdout>PRIVATE</local-command-stdout>", vec![])]
    #[case::stderr("<local-command-stderr>PRIVATE</local-command-stderr>", vec![])]
    #[case::bash_output(
        "<bash-stdout>PRIVATE</bash-stdout><bash-stderr></bash-stderr><bash-exit-code>0</bash-exit-code>",
        vec![]
    )]
    #[case::slash_command(
        "<command-name>/model</command-name>\n  <command-message>model</command-message>\n  <command-args>opus</command-args>",
        vec![Content::Text("/model opus".into())]
    )]
    #[case::slash_command_without_args(
        "<command-message>login</command-message>\n<command-name>/login</command-name>\n<command-args></command-args>",
        vec![Content::Text("/login".into())]
    )]
    #[case::bash_input("<bash-input>ls -la</bash-input>", vec![Content::Text("! ls -la".into())])]
    #[case::inline_output(
        "see <local-command-stdout>PRIVATE</local-command-stdout>this",
        vec![Content::Text("see this".into())]
    )]
    #[case::unclosed_output("hi <bash-stdout>PRIVATE", vec![Content::Text("hi ".into())])]
    fn user_text_is_what_the_user_typed(#[case] text: &str, #[case] expected: Vec<Content>) {
        let m = parse(&serde_json::json!({"type": "user", "uuid": "c1",
            "message": {"role": "user", "content": text}}));
        assert_eq!(m.content(), expected);
    }

    /// Claude Code also records a typed slash command as a `local_command` system line.
    #[rstest]
    fn local_command_record_is_user_text() {
        let m = parse(&serde_json::json!({"type": "system", "subtype": "local_command",
            "uuid": "l1", "content": "<command-name>/cost</command-name><command-args></command-args>"}));
        assert_eq!(m.role(), Role::User);
        assert_eq!(m.content(), vec![Content::Text("/cost".into())]);

        let output = parse(&serde_json::json!({"type": "system", "subtype": "local_command",
            "uuid": "l2", "content": "<local-command-stdout>$0.12</local-command-stdout>"}));
        assert_eq!(output.role(), Role::System);
        assert!(output.content().is_empty());
    }

    fn assistant_line(blocks: &serde_json::Value, usage: &serde_json::Value) -> CcodeMessage {
        parse(&serde_json::json!({
            "type": "assistant", "uuid": "t1",
            "message": {"id": "msg_01", "role": "assistant", "content": blocks, "usage": usage},
        }))
    }

    /// A call split over several lines repeats its usage on each; only the line with the
    /// thinking block is marked as reasoning.
    #[rstest]
    #[case::tool_use(serde_json::json!([{"type": "tool_use", "id": "toolu_1", "name": "Bash", "input": {}}]))]
    #[case::text(serde_json::json!([{"type": "text", "text": "done"}]))]
    fn split_line_without_thinking_gains_no_reasoning_block(#[case] blocks: serde_json::Value) {
        let usage = serde_json::json!({"input_tokens": 2, "output_tokens": 206,
            "output_tokens_details": {"thinking_tokens": 21}});
        let content = assistant_line(&blocks, &usage).content();
        assert!(
            !content.iter().any(|c| matches!(c, Content::ReasoningSummary { .. })),
            "{content:?}"
        );
    }

    #[rstest]
    #[case::final_usage(serde_json::json!({"output_tokens": 206,
        "output_tokens_details": {"thinking_tokens": 21}}), Some(21))]
    // Subagent transcripts write the thinking line with the stream's opening usage.
    #[case::opening_usage(serde_json::json!({"output_tokens": 5}), None)]
    fn thinking_line_carries_the_calls_thinking_tokens(
        #[case] usage: serde_json::Value,
        #[case] expected: Option<u64>,
        #[values("thinking", "redacted_thinking")] kind: &str,
    ) {
        let blocks = serde_json::json!([{"type": kind}]);
        assert_eq!(assistant_line(&blocks, &usage).content(), vec![Content::ReasoningSummary {
            tokens: expected
        }]);
    }

    #[rstest]
    #[case::total(serde_json::json!({"cache_creation_input_tokens": 7,
        "cache_creation": {"ephemeral_5m_input_tokens": 3, "ephemeral_1h_input_tokens": 1}}), Some(7))]
    #[case::split_only(serde_json::json!({
        "cache_creation": {"ephemeral_5m_input_tokens": 3, "ephemeral_1h_input_tokens": 4}}), Some(7))]
    #[case::neither(serde_json::json!({"input_tokens": 1}), None)]
    fn cache_write_falls_back_to_the_lifetime_split(
        #[case] usage: serde_json::Value,
        #[case] expected: Option<u64>,
    ) {
        let usage = assistant_line(&serde_json::json!([]), &usage).usage().unwrap();
        assert_eq!(usage.cache_write, expected);
    }

    #[rstest]
    fn image_blocks_drop_their_bytes() {
        let m = parse(&serde_json::json!({"type": "user", "uuid": "i1",
        "message": {"role": "user", "content": [
            {"type": "image", "source": {"type": "base64", "media_type": "image/png",
                "data": "PRIVATE_BYTES"}},
            {"type": "text", "text": "what is this?"},
        ]}}));
        assert_eq!(m.role(), Role::User);
        let content = m.content();
        assert!(!format!("{content:?}").contains("PRIVATE_BYTES"));
        assert_eq!(content[1], Content::Text("what is this?".into()));
    }
}
