use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use atuin_client::ai_session::{HarnessKind, HarnessSession, NativeSessionId};
use atuin_common::harnesstools::AnyHarness;
use atuin_common::harnesstools::session::{AnyMessage, RuntimeError, SessionEvent, SessionId};
use atuin_common::sync::BlockingPool;
use futures::StreamExt;
use parking_lot::Mutex;
use tokio::task::JoinHandle;

use super::Sink;
use super::message_enricher::{MessageEnricher, SYNTHETIC};

/// Backoff bounds for retrying a harness listener whose session directory does not exist yet.
const LISTENER_RETRY_START: Duration = Duration::from_secs(2);
const LISTENER_RETRY_MAX: Duration = Duration::from_secs(60);

pub struct SessionCaptureEngine {
    listeners: Vec<JoinHandle<()>>,
}

impl SessionCaptureEngine {
    pub fn nop() -> Self {
        Self {
            listeners: Vec::new(),
        }
    }

    pub fn spawn(sink: &Arc<Sink>, pool: &BlockingPool) -> Self {
        let mut listeners = Vec::new();

        for harness in AnyHarness::all() {
            let Some(sessions) = harness.sessions(pool) else {
                continue;
            };
            let kind = HarnessKind::from(harness);
            let sink = sink.clone();

            listeners.push(tokio::spawn(async move {
                // A harness whose session directory does not exist yet (not installed, or never
                // run) must not be dropped for the life of the daemon: retry with capped backoff
                // until it appears, so capture starts without a restart.
                let mut delay = LISTENER_RETRY_START;
                let listener = loop {
                    match sessions.listener() {
                        Ok(listener) => break listener,
                        Err(RuntimeError::NotFound(_)) => {
                            tokio::time::sleep(delay).await;
                            delay = (delay * 2).min(LISTENER_RETRY_MAX);
                        }
                        Err(e) => {
                            tracing::warn!(
                                ?e,
                                ?kind,
                                "ai-session listener failed; capture disabled for this harness"
                            );
                            return;
                        }
                    }
                };

                // Transcripts `events()` has just (re)opened, and whether each resumes past its
                // start: set before the first line of the new read is yielded, so the loop below
                // sees it with that line.
                let starts: Starts = Arc::default();
                let checkpoint = {
                    let (sink, starts) = (sink.clone(), starts.clone());
                    move |id: &SessionId| {
                        let (sink, starts, id) = (sink.clone(), starts.clone(), id.clone());
                        async move {
                            let at = checkpoint_of(&sink, kind, &id).await;
                            starts.lock().insert(id, Start::Beginning);
                            at
                        }
                    }
                };
                let knows = {
                    let (sink, starts) = (sink.clone(), starts.clone());
                    move |id: SessionId, message: AnyMessage| {
                        let (sink, starts) = (sink.clone(), starts.clone());
                        async move {
                            let stored = is_stored(&sink, kind, &id, &message).await;
                            if stored {
                                starts.lock().insert(id, Start::Resumed);
                            }
                            stored
                        }
                    }
                };
                let mut events = listener.events(checkpoint, knows);
                let mut enricher = MessageEnricher::new(kind);
                // Sessions with a failed append: their checkpoint must not move past the line
                // that was lost, or a restart would never re-read it.
                let mut stuck: HashSet<SessionId> = HashSet::new();

                while let Some(ev) = events.next().await {
                    match ev {
                        Ok(SessionEvent {
                            session,
                            offset,
                            message,
                        }) => {
                            let start = starts.lock().remove(&session);
                            if let Some(start) = start {
                                // A new read retries whatever a failed append lost.
                                stuck.remove(&session);
                                warm(&sink, &mut enricher, &session, start).await;
                            }
                            let rows = enricher.capture(&session, &message);
                            if rows.is_empty() {
                                continue;
                            }
                            for msg in rows {
                                if let Err(e) = sink.append(msg).await {
                                    tracing::warn!(?e, "failed to capture ai-session message");
                                    stuck.insert(session.clone());
                                }
                            }
                            if stuck.contains(&session) {
                                continue;
                            }
                            // ponytail: one checkpoint write per row; batch per session on idle
                            // if it shows up in profiles.
                            let handle = enricher.handle(&session);
                            if let Err(e) = sink.sidecar.set_checkpoint(&handle, offset).await {
                                tracing::warn!(?e, "failed to record ai-session checkpoint");
                            }
                        }
                        Err(e) => tracing::warn!(?e, "capture error"),
                    }
                }
            }));
        }

        Self { listeners }
    }
}

/// Where a fresh read of a transcript starts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Start {
    Beginning,
    /// Past its start, from a checkpoint naming a stored message.
    Resumed,
}

type Starts = Arc<Mutex<HashMap<SessionId, Start>>>;

/// Set a session's bookkeeping up for a fresh read of its transcript: empty for one read from
/// the beginning, whose every line replays; warmed from the sidecar for one resumed past its
/// start, which replays none of the lines before it.
pub(super) async fn warm(
    sink: &Sink,
    enricher: &mut MessageEnricher,
    session: &SessionId,
    start: Start,
) {
    enricher.restart(session);
    if start == Start::Beginning {
        return;
    }
    let handle = enricher.handle(session);
    let row = sink.sidecar.get_session(&handle).await.unwrap_or_else(|e| {
        tracing::warn!(?e, %session, "failed to load the ai-session row");
        None
    });
    let last = sink.sidecar.last_message(&handle).await.unwrap_or_else(|e| {
        tracing::warn!(?e, %session, "failed to load the last ai-session message");
        None
    });
    let synthetic =
        sink.sidecar.source_ids_with_prefix(&handle, SYNTHETIC).await.unwrap_or_else(|e| {
            tracing::warn!(?e, %session, "failed to load the ai-session synthetic ids");
            Vec::new()
        });
    enricher.resume(session, row.as_ref(), last.as_ref(), &synthetic);
}

/// The resume token stored for a session, `0` for none.
async fn checkpoint_of(sink: &Sink, kind: HarnessKind, session: &SessionId) -> u64 {
    match sink.sidecar.checkpoint(&handle_of(kind, session)).await {
        Ok(Some(at)) => at,
        Ok(None) => 0,
        Err(e) => {
            tracing::warn!(?e, %session, "failed to read ai-session checkpoint; reading from the start");
            0
        }
    }
}

/// Whether the message a resume token names is one already captured, which is what makes the
/// token safe to resume from: anything else (a transcript rewritten under its offset, a row a
/// reset log dropped) leaves the session to be read from its beginning.
async fn is_stored(
    sink: &Sink,
    kind: HarnessKind,
    session: &SessionId,
    message: &AnyMessage,
) -> bool {
    let handle = handle_of(kind, session);
    let source_id = MessageEnricher::source_id(session, message);
    let stored = sink.sidecar.contains_message(&handle, &source_id).await.unwrap_or(false);
    if !stored {
        tracing::debug!(%session, "checkpoint does not name a stored message; reading from the start");
    }
    stored
}

fn handle_of(kind: HarnessKind, session: &SessionId) -> HarnessSession {
    HarnessSession {
        harness: kind,
        session: NativeSessionId::from(session.to_string()),
    }
}

impl Drop for SessionCaptureEngine {
    fn drop(&mut self) {
        for listener in &self.listeners {
            listener.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use atuin_client::ai_session::{AiSessionDatabase, AiSessionStore, SourceId};
    use atuin_client::record::sqlite_store::SqliteStore;
    use atuin_common::encryption::paseto_v4::Key;
    use atuin_common::harnesstools::ccode::session::CcodeMessage;
    use atuin_domain::record::HostId;
    use rstest::rstest;

    use super::*;

    async fn sink() -> Sink {
        let store = SqliteStore::in_memory(super::super::NOP_STORE_TIMEOUT).await.unwrap();
        let records = AiSessionStore::builder()
            .store(store)
            .host_id(HostId(atuin_common::utils::uuid_v7()))
            .key(Key::generate())
            .build();
        Sink::new(records, AiSessionDatabase::in_memory().await.unwrap())
    }

    fn line(uuid: &str) -> String {
        serde_json::json!({
            "type": "user", "uuid": uuid, "sessionId": "s1",
            "timestamp": "2026-09-18T10:00:00Z",
            "message": {"role": "user", "content": "hi"},
        })
        .to_string()
            + "\n"
    }

    /// What [`Listener::events`] composes of a session's checkpoint and the message it names,
    /// for the ccode transcripts these tests are written against.
    async fn resume_point(
        sink: &Sink,
        kind: HarnessKind,
        session: &SessionId,
        path: &std::path::Path,
    ) -> u64 {
        let at = checkpoint_of(sink, kind, session).await;
        if at == 0 {
            return 0;
        }
        let Some(m) = atuin_common::json::jsonl::value_at::<CcodeMessage>(
            path,
            at,
            &BlockingPool::new(std::num::NonZeroUsize::MIN),
        )
        .await
        else {
            return 0;
        };
        if is_stored(sink, kind, session, &AnyMessage::from(m)).await {
            at
        } else {
            0
        }
    }

    /// A checkpoint is honoured only while the line it follows is still a stored message: a
    /// transcript rewritten in place to the same length, or cut shorter, reads from the start.
    #[rstest]
    #[tokio::test]
    async fn resume_point_validates_the_checkpoint_against_the_transcript() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s1.jsonl");
        let body = line("u1") + &line("u2");
        std::fs::write(&path, &body).unwrap();

        let sink = sink().await;
        let session = SessionId::from("s1".to_owned());
        let mut enricher = MessageEnricher::new(HarnessKind::ClaudeCode);
        for raw in [line("u1"), line("u2")] {
            let m: CcodeMessage = serde_json::from_str(raw.trim_end()).unwrap();
            let msg = enricher.capture(&session, &AnyMessage::from(m)).pop().unwrap();
            sink.append(msg).await.unwrap();
        }
        let handle = enricher.handle(&session);
        let len = u64::try_from(body.len()).unwrap();
        sink.sidecar.set_checkpoint(&handle, len).await.unwrap();

        assert_eq!(resume_point(&sink, HarnessKind::ClaudeCode, &session, &path).await, len);
        assert_eq!(
            sink.sidecar.last_message(&handle).await.unwrap().unwrap().source_id,
            SourceId::from("u2".to_owned())
        );

        std::fs::write(&path, line("v1") + &line("v2")).unwrap();
        assert_eq!(
            resume_point(&sink, HarnessKind::ClaudeCode, &session, &path).await,
            0,
            "rewritten to the same length"
        );

        std::fs::write(&path, line("u1")).unwrap();
        assert_eq!(
            resume_point(&sink, HarnessKind::ClaudeCode, &session, &path).await,
            0,
            "shorter than the checkpoint"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn resume_point_starts_from_zero_without_a_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s1.jsonl");
        std::fs::write(&path, line("u1")).unwrap();
        let sink = sink().await;
        let session = SessionId::from("s1".to_owned());
        assert_eq!(resume_point(&sink, HarnessKind::ClaudeCode, &session, &path).await, 0);
    }
}
