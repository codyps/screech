use bytes::Bytes;
use proptest::prelude::*;
use screech::{
    durable::{AckAction, AckToken, ConsumerConfig, Engine, Event, Retention, StreamConfig},
    storage::{Append, Content, ContentKey, MemoryStore, Record, SqliteStore, Store, StoreError},
};
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc,
};

fn create<S: Store>(engine: &Engine<S>) -> Event {
    engine
        .state()
        .prepare_create(
            StreamConfig {
                name: "S".into(),
                subjects: vec!["events.>".into()],
                retention: Retention::Workqueue,
                ..Default::default()
            },
            0,
        )
        .unwrap()
}
fn publish<S: Store>(engine: &Engine<S>, time: u64) -> Event {
    engine
        .state()
        .prepare_publish(
            "S",
            "events.a",
            Bytes::new(),
            Bytes::from(format!("body-{time}")),
            time,
        )
        .unwrap()
}

#[test]
fn strict_mode_rejects_unsafe_and_volatile_stores_before_recovery() {
    let dir = tempfile::tempdir().unwrap();
    assert!(Engine::recover_single_voter(MemoryStore::default()).is_err());
    let store = SqliteStore::open_with_unsafe_ack_durability(dir.path().join("db")).unwrap();
    assert!(Engine::recover_single_voter(store).is_err());
    // The old opt-in mode remains available through the original entry point.
    let store = SqliteStore::open_with_unsafe_ack_durability(dir.path().join("db")).unwrap();
    let mut standalone = Engine::recover(store).unwrap();
    standalone.commit(create(&standalone)).unwrap();
    assert!(standalone.voter_progress().is_none());
}

#[test]
fn proposal_and_durable_completion_do_not_publish_state_early() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine =
        Engine::recover_single_voter(SqliteStore::open(dir.path().join("db")).unwrap()).unwrap();
    let mut plan = engine.begin_group().unwrap();
    plan.commit(create(&engine)).unwrap();
    plan.commit(
        plan.state()
            .prepare_publish("S", "events.a", Bytes::new(), "body".into(), 1)
            .unwrap(),
    )
    .unwrap();
    let task = engine.prepare_append(plan).unwrap().unwrap();
    assert_eq!(engine.voter_progress().unwrap().accepted, 2);
    assert_eq!(engine.voter_progress().unwrap().committed, 0);
    assert!(engine.state().streams.is_empty());
    assert!(engine.begin_group().is_err());
    assert!(engine.checkpoint().is_err());
    // Exercise the actual ownership boundary, on a different executor thread.
    let completion = std::thread::spawn(move || task.run()).join().unwrap();
    assert!(engine.state().streams.is_empty());
    assert_eq!(engine.voter_progress().unwrap().applied, 0);
    engine.complete_append(completion).unwrap();
    let p = engine.voter_progress().unwrap();
    assert_eq!(
        (p.accepted, p.durable, p.committed, p.applied),
        (2, 2, 2, 2)
    );
    let message = &engine.state().streams["S"].messages[&1];
    assert!(message.payload.is_empty());
    assert_eq!(engine.read_message(message).unwrap().payload, "body");
    engine.checkpoint().unwrap();
    assert_eq!(engine.voter_progress().unwrap().checkpoint, 2);
}

#[test]
fn sole_voter_recovers_completed_write_without_apply_or_reply() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut engine = Engine::recover_single_voter(SqliteStore::open(&path).unwrap()).unwrap();
    engine.commit(create(&engine)).unwrap();
    let mut plan = engine.begin_group().unwrap();
    plan.commit(publish(&engine, 1)).unwrap();
    let completion = engine.prepare_append(plan).unwrap().unwrap().run();
    assert!(engine.state().streams["S"].messages.is_empty());
    drop(completion);
    drop(engine);
    let recovered = Engine::recover_single_voter(SqliteStore::open(&path).unwrap()).unwrap();
    assert_eq!(recovered.voter_progress().unwrap().applied, 2);
    assert_eq!(
        recovered
            .read_message(&recovered.state().streams["S"].messages[&1])
            .unwrap()
            .payload,
        "body-1"
    );
    // Existing standalone readers can still recover this committed-only format.
    drop(recovered);
    assert_eq!(
        Engine::recover(SqliteStore::open(path).unwrap())
            .unwrap()
            .state()
            .streams["S"]
            .last_sequence,
        1
    );
}

#[test]
fn stale_foreign_and_empty_proposals_cannot_advance_the_voter() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine =
        Engine::recover_single_voter(SqliteStore::open(dir.path().join("a")).unwrap()).unwrap();
    let other =
        Engine::recover_single_voter(SqliteStore::open(dir.path().join("b")).unwrap()).unwrap();
    let empty = engine.begin_group().unwrap();
    assert!(engine.prepare_append(empty).unwrap().is_none());
    assert!(engine.prepare_append(other.begin_group().unwrap()).is_err());
    let mut stale = engine.begin_group().unwrap();
    stale.commit(create(&engine)).unwrap();
    engine.commit(create(&engine)).unwrap();
    assert!(engine.prepare_append(stale).is_err());
    assert_eq!(engine.voter_progress().unwrap().applied, 1);
}

/// Real SQLite plus injectable outcomes. Consumer-only appends must never use
/// the legacy buffering hook, even though the wrapper advertises strict storage.
struct FaultStore {
    inner: SqliteStore,
    fault: Arc<AtomicU8>,
}
impl Store for FaultStore {
    fn durable_appends(&self) -> bool {
        true
    }
    fn supports_batch(&self) -> bool {
        true
    }
    fn external_content(&self) -> bool {
        true
    }
    fn append(&mut self, s: &str, h: &[u8], p: &[u8]) -> Result<u64, StoreError> {
        self.inner.append(s, h, p)
    }
    fn append_with_content(
        &mut self,
        records: &[Append],
        content: &[Content],
    ) -> Result<u64, StoreError> {
        let fault = self.fault.load(Ordering::SeqCst);
        if fault == 1 {
            return Err(StoreError::Invalid);
        }
        let end = self.inner.append_with_content(records, content)?;
        match fault {
            2 => Err(StoreError::Invalid), // Durable write, lost completion.
            3 => Ok(end + 1),
            _ => Ok(end),
        }
    }
    fn append_consumer_state(&mut self, _: &[Append]) -> Result<u64, StoreError> {
        panic!("strict voter used legacy consumer buffering")
    }
    fn get(&self, seq: u64) -> Result<Option<Record>, StoreError> {
        self.inner.get(seq)
    }
    fn get_content(&self, key: ContentKey) -> Result<Option<Content>, StoreError> {
        self.inner.get_content(key)
    }
    fn last_sequence(&self) -> Result<u64, StoreError> {
        self.inner.last_sequence()
    }
    fn purge(&mut self) -> Result<(), StoreError> {
        self.inner.purge()
    }
}

#[test]
fn failed_or_invalid_completion_quarantines_without_exposing_the_proposal() {
    for fault in 1..=3 {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let setting = Arc::new(AtomicU8::new(0));
        let mut engine = Engine::recover_single_voter(FaultStore {
            inner: SqliteStore::open(&path).unwrap(),
            fault: setting.clone(),
        })
        .unwrap();
        engine.commit(create(&engine)).unwrap();
        setting.store(fault, Ordering::SeqCst);
        assert!(engine.commit(publish(&engine, 1)).is_err());
        assert!(engine.is_poisoned());
        assert!(engine.state().streams["S"].messages.is_empty());
        assert_eq!(engine.voter_progress().unwrap().applied, 1);
        assert!(engine.begin_group().is_err());
        assert!(engine.checkpoint().is_err());
        drop(engine);
        let recovered = Engine::recover_single_voter(SqliteStore::open(&path).unwrap()).unwrap();
        assert_eq!(
            recovered.state().streams["S"].messages.len(),
            usize::from(fault != 1)
        );
    }
}

#[test]
fn strict_consumer_operations_bypass_legacy_buffering() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = Engine::recover_single_voter(FaultStore {
        inner: SqliteStore::open(dir.path().join("db")).unwrap(),
        fault: Arc::new(AtomicU8::new(0)),
    })
    .unwrap();
    engine.commit(create(&engine)).unwrap();
    engine
        .commit(
            engine
                .state()
                .prepare_consumer(
                    "S",
                    ConsumerConfig {
                        name: "D".into(),
                        durable_name: Some("D".into()),
                        ..Default::default()
                    },
                    0,
                )
                .unwrap(),
        )
        .unwrap();
    engine.commit(publish(&engine, 1)).unwrap();
    engine
        .commit(
            engine
                .state()
                .prepare_delivery("S", "D", 2)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
    let c = &engine.state().streams["S"].consumers["D"];
    let token = AckToken {
        generation: c.generation,
        sequence: 1,
        consumer_sequence: c.pending[&1].consumer_sequence,
    };
    engine
        .commit(
            engine
                .state()
                .prepare_ack("S", "D", token, AckAction::Ack, 3)
                .unwrap(),
        )
        .unwrap();
    assert!(engine.state().streams["S"].messages.is_empty());
    let recovered = Engine::recover_single_voter(engine.into_store()).unwrap();
    assert_eq!(recovered.state().streams["S"].consumers["D"].ack_floor, 1);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn single_voter_refines_standalone_across_groups_checkpoints_and_restart(ops in prop::collection::vec(0u8..6, 1..50)) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("voter");
        let baseline_path = dir.path().join("standalone");
        let mut voter = Engine::recover_single_voter(SqliteStore::open(&path).unwrap()).unwrap();
        let mut baseline = Engine::recover(SqliteStore::open(&baseline_path).unwrap()).unwrap();
        voter.commit(create(&voter)).unwrap();
        baseline.commit(create(&baseline)).unwrap();
        for (time, op) in ops.into_iter().enumerate() {
            match op {
                0..=2 => {
                    let mut vp = voter.begin_group().unwrap();
                    let mut bp = baseline.begin_group().unwrap();
                    for offset in 0..=op {
                        let event = vp.state().prepare_publish("S", "events.a", Bytes::new(),
                            Bytes::from(vec![offset; 1024]), time as u64).unwrap();
                        vp.commit(event.clone()).unwrap(); bp.commit(event).unwrap();
                    }
                    voter.commit_group(vp).unwrap(); baseline.commit_group(bp).unwrap();
                }
                3 => {
                    voter.commit(voter.state().prepare_delete("S", true).unwrap()).unwrap();
                    baseline.commit(baseline.state().prepare_delete("S", true).unwrap()).unwrap();
                }
                4 => { voter.checkpoint().unwrap(); baseline.checkpoint().unwrap(); }
                _ => {
                    drop(voter); drop(baseline);
                    voter = Engine::recover_single_voter(SqliteStore::open(&path).unwrap()).unwrap();
                    baseline = Engine::recover(SqliteStore::open(&baseline_path).unwrap()).unwrap();
                }
            }
            prop_assert_eq!(voter.state(), baseline.state());
            for message in voter.state().streams["S"].messages.values() {
                prop_assert_eq!(voter.read_message(message).unwrap(), baseline.read_message(message).unwrap());
            }
        }
    }
}

#[tokio::test]
async fn worker_withholds_replies_after_ambiguous_persistence_failure() {
    use screech::{core::Message, jetstream::Handle};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let fault = Arc::new(AtomicU8::new(0));
    let handle = Handle::start_single_voter(FaultStore {
        inner: SqliteStore::open(&path).unwrap(),
        fault: fault.clone(),
    })
    .unwrap();
    let mut output = handle.take_output(Arc::new(|_, _| true)).unwrap();
    let create = Message {
        subject: "$JS.API.STREAM.CREATE.S".into(),
        reply: Some("inbox".into()),
        headers: Bytes::new(),
        payload: r#"{"name":"S","subjects":["events.>"]}"#.into(),
    };
    handle.request(create).await.unwrap();
    let reply = output.recv().await.unwrap();
    assert!(
        serde_json::from_slice::<serde_json::Value>(&reply.message.payload)
            .unwrap()
            .get("error")
            .is_none()
    );
    fault.store(2, Ordering::SeqCst);
    let publication = Message {
        subject: "events.a".into(),
        reply: Some("ack".into()),
        headers: Bytes::new(),
        payload: "body".into(),
    };
    assert!(handle.request(publication).await.is_err());
    assert!(
        output.try_recv().is_err(),
        "failed publication emitted a reply"
    );
    let info = Message {
        subject: "$JS.API.STREAM.INFO.S".into(),
        reply: Some("info".into()),
        headers: Bytes::new(),
        payload: Bytes::new(),
    };
    assert!(handle.request(info).await.is_err());
    assert!(
        output.try_recv().is_err(),
        "quarantined worker emitted a reply"
    );
    handle.shutdown().await;
    let recovered = Engine::recover_single_voter(SqliteStore::open(&path).unwrap()).unwrap();
    assert_eq!(recovered.state().streams["S"].last_sequence, 1);
}

#[test]
fn automatic_checkpoint_covers_only_completed_work() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut engine = Engine::recover_single_voter(SqliteStore::open(&path).unwrap()).unwrap();
    let mut plan = engine.begin_group().unwrap();
    for time in 1..=8192 {
        plan.commit(Event::Expire { time }).unwrap();
    }
    let completion = engine.prepare_append(plan).unwrap().unwrap().run();
    assert_eq!(engine.voter_progress().unwrap().checkpoint, 0);
    engine.complete_append(completion).unwrap();
    assert_eq!(engine.voter_progress().unwrap().checkpoint, 8192);
    drop(engine);
    let engine = Engine::recover_single_voter(SqliteStore::open(&path).unwrap()).unwrap();
    assert_eq!(engine.state().time, 8192);
    assert_eq!(engine.voter_progress().unwrap().checkpoint, 8192);
}

#[test]
fn completion_from_another_engine_cannot_replace_storage_or_state() {
    let dir = tempfile::tempdir().unwrap();
    let mut a =
        Engine::recover_single_voter(SqliteStore::open(dir.path().join("a")).unwrap()).unwrap();
    let mut b =
        Engine::recover_single_voter(SqliteStore::open(dir.path().join("b")).unwrap()).unwrap();
    let mut pa = a.begin_group().unwrap();
    let mut pb = b.begin_group().unwrap();
    pa.commit(create(&a)).unwrap();
    pb.commit(create(&b)).unwrap();
    let ca = a.prepare_append(pa).unwrap().unwrap().run();
    let cb = b.prepare_append(pb).unwrap().unwrap().run();
    // The same end index and event bytes do not establish completion ownership.
    assert!(a.complete_append(cb).is_err());
    assert!(a.state().streams.is_empty());
    assert_eq!(a.voter_progress().unwrap().applied, 0);
    a.complete_append(ca).unwrap();
    assert_eq!(a.voter_progress().unwrap().applied, 1);
}
