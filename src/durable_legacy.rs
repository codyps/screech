//! Reader for the version-2 checkpoint layout, before external content references.
use super::*;
#[derive(Deserialize)]
#[cfg_attr(test, derive(Serialize))]
pub(super) struct LegacySnapshot {
    pub version: u8,
    pub state: LegacyState,
}
#[derive(Deserialize)]
#[cfg_attr(test, derive(Serialize))]
pub(super) struct LegacyState {
    streams: BTreeMap<String, LegacyStream>,
    generation: u64,
    time: u64,
}
#[derive(Clone, Deserialize)]
#[cfg_attr(test, derive(Serialize))]
struct LegacyMessage {
    sequence: u64,
    subject: String,
    headers: Bytes,
    payload: Bytes,
    time: u64,
}
#[derive(Deserialize)]
#[cfg_attr(test, derive(Serialize))]
struct LegacyStream {
    config: StreamConfig,
    generation: u64,
    created: u64,
    last_sequence: u64,
    last_msg_id: String,
    messages: OrdMap<u64, LegacyMessage>,
    bytes: u64,
    consumers: BTreeMap<String, Consumer>,
    subjects: OrdMap<String, OrdSet<u64>>,
    interest: OrdMap<u64, OrdSet<u64>>,
    dedup: OrdMap<String, (u64, u64)>,
    dedup_expiry: OrdSet<(u64, String)>,
}
impl LegacyState {
    pub fn into_current(self) -> State {
        State {
            generation: self.generation,
            time: self.time,
            streams: self
                .streams
                .into_iter()
                .map(|(name, s)| {
                    let messages = s
                        .messages
                        .into_iter()
                        .map(|(seq, m)| {
                            (
                                seq,
                                StoredMessage {
                                    sequence: m.sequence,
                                    subject: m.subject.clone(),
                                    headers: m.headers.clone(),
                                    payload: m.payload.clone(),
                                    time: m.time,
                                    content: None,
                                },
                            )
                        })
                        .collect();
                    (
                        name,
                        Stream {
                            config: s.config,
                            generation: s.generation,
                            created: s.created,
                            last_sequence: s.last_sequence,
                            last_msg_id: s.last_msg_id,
                            messages,
                            bytes: s.bytes,
                            consumers: s.consumers,
                            subjects: s.subjects,
                            interest: s.interest,
                            dedup: s.dedup,
                            dedup_expiry: s.dedup_expiry,
                        },
                    )
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::SqliteStore;
    #[test]
    fn version_two_checkpoint_imports_content_atomically() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store");
        let mut engine = Engine::recover(SqliteStore::open(&path).unwrap()).unwrap();
        let event = engine
            .state()
            .prepare_create(
                StreamConfig {
                    name: "S".into(),
                    subjects: vec!["a".into()],
                    duplicate_window: 100,
                    ..Default::default()
                },
                0,
            )
            .unwrap();
        engine.commit(event).unwrap();
        let event = engine
            .state()
            .prepare_publish(
                "S",
                "a",
                Bytes::from_static(b"NATS/1.0\r\nNats-Msg-Id: id\r\n\r\n"),
                Bytes::from_static(b"old body"),
                0,
            )
            .unwrap();
        engine.commit(event).unwrap();
        let mut state = engine.state().clone();
        let m = engine
            .read_message(&state.streams["S"].messages[&1])
            .unwrap();
        state.streams.get_mut("S").unwrap().messages.insert(1, m);
        let streams = state
            .streams
            .into_iter()
            .map(|(name, s)| {
                let messages = s
                    .messages
                    .into_iter()
                    .map(|(seq, m)| {
                        (
                            seq,
                            LegacyMessage {
                                sequence: m.sequence,
                                subject: m.subject,
                                headers: m.headers,
                                payload: m.payload,
                                time: m.time,
                            },
                        )
                    })
                    .collect();
                (
                    name,
                    LegacyStream {
                        config: s.config,
                        generation: s.generation,
                        created: s.created,
                        last_sequence: s.last_sequence,
                        last_msg_id: s.last_msg_id,
                        messages,
                        bytes: s.bytes,
                        consumers: s.consumers,
                        subjects: s.subjects,
                        interest: s.interest,
                        dedup: s.dedup,
                        dedup_expiry: s.dedup_expiry,
                    },
                )
            })
            .collect();
        let bytes = postcard::to_allocvec(&LegacySnapshot {
            version: 2,
            state: LegacyState {
                streams,
                generation: state.generation,
                time: state.time,
            },
        })
        .unwrap();
        let mut store = engine.into_store();
        store
            .checkpoint(store.last_sequence().unwrap(), &bytes)
            .unwrap();
        // Model a real old database, where there is no external body table data.
        drop(store);
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute("DELETE FROM retained_content", []).unwrap();
        drop(conn);
        let engine = Engine::recover(SqliteStore::open(&path).unwrap()).unwrap();
        let m = &engine.state().streams["S"].messages[&1];
        assert!(m.payload.is_empty() && m.content.is_some());
        assert_eq!(engine.read_message(m).unwrap().payload, b"old body"[..]);
        assert_eq!(
            engine.state().streams["S"].duplicate(b"NATS/1.0\r\nNats-Msg-Id: id\r\n\r\n", 1),
            Some(1)
        );
        drop(engine.into_store());
        let engine = Engine::recover(SqliteStore::open(&path).unwrap()).unwrap();
        assert_eq!(
            engine
                .read_message(&engine.state().streams["S"].messages[&1])
                .unwrap()
                .payload,
            b"old body"[..]
        );
    }
}
