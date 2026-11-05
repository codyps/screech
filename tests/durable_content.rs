use bytes::Bytes;
use proptest::prelude::*;
use screech::{
    durable::*,
    storage::{MemoryStore, SqliteStore, Store},
};

fn setup<S: Store>(store: S, retention: Retention) -> Engine<S> {
    let mut engine = Engine::recover(store).unwrap();
    let event = engine
        .state()
        .prepare_create(
            StreamConfig {
                name: "S".into(),
                subjects: vec!["a.>".into()],
                retention,
                ..Default::default()
            },
            0,
        )
        .unwrap();
    engine.commit(event).unwrap();
    let event = engine
        .state()
        .prepare_consumer(
            "S",
            ConsumerConfig {
                name: "D".into(),
                durable_name: Some("D".into()),
                ack_wait: 10,
                ..Default::default()
            },
            0,
        )
        .unwrap();
    engine.commit(event).unwrap();
    engine
}
fn publish<S: Store>(engine: &mut Engine<S>, time: u64) {
    let event = engine
        .state()
        .prepare_publish(
            "S",
            "a.b",
            Bytes::from_static(b"NATS/1.0\r\nX: yes\r\n\r\n"),
            Bytes::from(vec![time as u8; 4096]),
            time,
        )
        .unwrap();
    engine.commit(event).unwrap();
}
fn equal(disk: &Engine<SqliteStore>, memory: &Engine<MemoryStore>) {
    let mut resolved = disk.state().clone();
    let stream: &mut Stream = resolved.streams.get_mut("S").unwrap();
    let selected: Vec<_> = stream.messages.values().collect();
    let messages = disk.read_messages(&selected).unwrap();
    for message in messages {
        stream.messages.insert(message.sequence, message);
    }
    assert_eq!(&resolved, memory.state());
    assert!(disk.state().streams["S"]
        .messages
        .values()
        .all(|m| m.headers.is_empty() && m.payload.is_empty() && m.content.is_some()));
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn external_content_refines_inline_model(ops in prop::collection::vec(0u8..6, 1..100), workqueue in any::<bool>()) {
        let dir = tempfile::tempdir().unwrap(); let path = dir.path().join("store");
        let retention = if workqueue { Retention::Workqueue } else { Retention::Limits };
        let mut disk = setup(SqliteStore::open(&path).unwrap(), retention.clone());
        let mut memory = setup(MemoryStore::default(), retention);
        for (time, op) in ops.into_iter().enumerate() {
            let time = time as u64 * 11;
            match op {
                0 | 1 => { publish(&mut disk, time); publish(&mut memory, time); }
                2 => {
                    let event = memory.state().prepare_delivery("S", "D", time).unwrap();
                    if let Some(event) = event { disk.commit(event.clone()).unwrap(); memory.commit(event).unwrap(); }
                }
                3 => {
                    let c = &memory.state().streams["S"].consumers["D"];
                    if let Some((&sequence, p)) = c.pending.iter().next() {
                        let token = AckToken { generation: c.generation, sequence, consumer_sequence: p.consumer_sequence };
                        let event = memory.state().prepare_ack("S", "D", token, AckAction::Ack, time).unwrap();
                        disk.commit(event.clone()).unwrap(); memory.commit(event).unwrap();
                    }
                }
                4 => { disk.checkpoint().unwrap(); memory.checkpoint().unwrap(); }
                _ => {
                    drop(disk.into_store()); disk = Engine::recover(SqliteStore::open(&path).unwrap()).unwrap();
                    memory = Engine::recover(memory.into_store()).unwrap();
                }
            }
            equal(&disk, &memory);
        }
    }
}
#[test]
fn group_reads_staged_content_and_recovery_reads_committed_content() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut disk = setup(SqliteStore::open(&path).unwrap(), Retention::Limits);
    let mut plan = disk.begin_group().unwrap();
    let event = plan
        .state()
        .prepare_publish("S", "a.b", Bytes::new(), Bytes::from_static(b"staged"), 0)
        .unwrap();
    plan.commit(event).unwrap();
    let m = &plan.state().streams["S"].messages[&1];
    assert_eq!(
        plan.read_staged_message(m).unwrap().unwrap().payload,
        b"staged"[..]
    );
    assert!(disk.state().streams["S"].messages.is_empty());
    disk.commit_group(plan).unwrap();
    disk.checkpoint().unwrap();
    drop(disk.into_store());
    let disk = Engine::recover(SqliteStore::open(&path).unwrap()).unwrap();
    assert_eq!(
        disk.read_message(&disk.state().streams["S"].messages[&1])
            .unwrap()
            .payload,
        b"staged"[..]
    );
}
#[test]
fn missing_checkpoint_content_blocks_recovery_and_corruption_poisons_reads() {
    for missing in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store");
        let mut disk = setup(SqliteStore::open(&path).unwrap(), Retention::Limits);
        publish(&mut disk, 1);
        disk.checkpoint().unwrap();
        drop(disk.into_store());
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute(
            if missing {
                "DELETE FROM retained_content"
            } else {
                "UPDATE retained_content SET payload=zeroblob(length(payload))"
            },
            [],
        )
        .unwrap();
        drop(conn);
        let recovered = Engine::recover(SqliteStore::open(&path).unwrap());
        if missing {
            assert!(recovered.is_err());
        } else {
            let mut disk = recovered.unwrap();
            assert!(disk
                .read_message(&disk.state().streams["S"].messages[&1])
                .is_err());
            assert!(disk.is_poisoned());
            assert!(disk.checkpoint().is_err());
        }
    }
}

#[test]
fn stream_recreation_does_not_alias_content_and_checkpoint_reclaims_old_generation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut disk = setup(SqliteStore::open(&path).unwrap(), Retention::Limits);
    publish(&mut disk, 1);
    let old_key = disk.state().streams["S"].messages[&1]
        .content
        .as_ref()
        .unwrap()
        .key;
    let event = disk.state().prepare_delete("S", false).unwrap();
    disk.commit(event).unwrap();
    let event = disk
        .state()
        .prepare_create(
            StreamConfig {
                name: "S".into(),
                subjects: vec!["a.>".into()],
                ..Default::default()
            },
            2,
        )
        .unwrap();
    disk.commit(event).unwrap();
    publish(&mut disk, 3);
    let new_key = disk.state().streams["S"].messages[&1]
        .content
        .as_ref()
        .unwrap()
        .key;
    assert_ne!(old_key, new_key);
    disk.checkpoint().unwrap();
    let store = disk.into_store();
    assert!(store.get_content(old_key).unwrap().is_none());
    assert_eq!(
        store.get_content(new_key).unwrap().unwrap().payload,
        vec![3; 4096]
    );
}

#[test]
fn external_bodies_do_not_count_as_event_log_bytes_after_restart() {
    for grouped in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store");
        let mut disk = setup(SqliteStore::open(&path).unwrap(), Retention::Limits);
        for _ in 0..2 {
            for _ in 0..8 {
                let body = Bytes::from(vec![42; 1024 * 1024]);
                if grouped {
                    let mut plan = disk.begin_group().unwrap();
                    let event = plan
                        .state()
                        .prepare_publish("S", "a.b", Bytes::new(), body, 0)
                        .unwrap();
                    plan.commit(event).unwrap();
                    disk.commit_group(plan).unwrap();
                } else {
                    let event = disk
                        .state()
                        .prepare_publish("S", "a.b", Bytes::new(), body, 0)
                        .unwrap();
                    disk.commit(event).unwrap();
                }
            }
            let store = disk.into_store();
            assert!(store.load_checkpoint().unwrap().is_none());
            assert!(store
                .get(store.last_sequence().unwrap())
                .unwrap()
                .unwrap()
                .payload
                .is_empty());
            drop(store);
            disk = Engine::recover(SqliteStore::open(&path).unwrap()).unwrap();
        }
        assert_eq!(disk.state().streams["S"].messages.len(), 16);
        for message in disk.state().streams["S"].messages.values() {
            assert_eq!(
                disk.read_message(message).unwrap().payload,
                vec![42; 1024 * 1024]
            );
        }
    }
}

#[test]
fn external_content_still_checkpoints_at_record_limit_after_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut disk = setup(SqliteStore::open(&path).unwrap(), Retention::Limits);
    // Two setup records plus 8189 publications leaves one record before the cap.
    for count in (0..8189).step_by(32) {
        let mut plan = disk.begin_group().unwrap();
        for _ in 0..(8189 - count).min(32) {
            let event = plan
                .state()
                .prepare_publish("S", "a.b", Bytes::new(), Bytes::from_static(b"body"), 0)
                .unwrap();
            plan.commit(event).unwrap();
        }
        disk.commit_group(plan).unwrap();
    }
    let store = disk.into_store();
    assert!(store.load_checkpoint().unwrap().is_none());
    drop(store);
    let mut disk = Engine::recover(SqliteStore::open(&path).unwrap()).unwrap();
    let mut plan = disk.begin_group().unwrap();
    for _ in 0..2 {
        let event = plan
            .state()
            .prepare_publish("S", "a.b", Bytes::new(), Bytes::from_static(b"new"), 0)
            .unwrap();
        plan.commit(event).unwrap();
    }
    disk.commit_group(plan).unwrap();
    let store = disk.into_store();
    assert_eq!(store.load_checkpoint().unwrap().unwrap().0, 8193);
    assert!(store.get(8193).unwrap().is_none());
    drop(store);
    let disk = Engine::recover(SqliteStore::open(&path).unwrap()).unwrap();
    assert_eq!(disk.state().streams["S"].messages.len(), 8191);
    assert_eq!(
        disk.read_message(&disk.state().streams["S"].messages[&8191])
            .unwrap()
            .payload,
        b"new"[..]
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn unsafe_consumer_suffix_refines_last_full_fence(ops in prop::collection::vec(0u8..6, 1..100)) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store");
        let mut disk = setup(SqliteStore::open_with_unsafe_ack_durability(&path).unwrap(), Retention::Workqueue);
        let inline = setup(MemoryStore::default(), Retention::Workqueue);
        let mut model = inline.state().clone();
        let mut durable = model.clone();
        for (time, op) in ops.into_iter().enumerate() {
            let time = time as u64 * 11;
            match op {
                0 | 1 => {
                    let event = model.prepare_publish("S", "a.b", Bytes::new(), Bytes::from(vec![time as u8;4096]), time).unwrap();
                    disk.commit(event.clone()).unwrap(); model.apply(event).unwrap();
                    durable = model.clone(); // FULL publication also fences older buffered ACKs.
                }
                2 => {
                    if let Some(event) = model.prepare_delivery("S", "D", time).unwrap() {
                        disk.commit(event.clone()).unwrap(); model.apply(event).unwrap();
                    }
                }
                3 => {
                    let c = &model.streams["S"].consumers["D"];
                    if let Some((&sequence,p)) = c.pending.iter().next() {
                        let event = model.prepare_ack("S", "D", AckToken { generation:c.generation,sequence,consumer_sequence:p.consumer_sequence }, AckAction::Ack,time).unwrap();
                        disk.commit(event.clone()).unwrap(); model.apply(event).unwrap();
                    }
                }
                4 => { disk.checkpoint().unwrap(); durable = model.clone(); }
                _ => {
                    drop(disk.into_store());
                    disk = Engine::recover(SqliteStore::open_with_unsafe_ack_durability(&path).unwrap()).unwrap();
                    model = durable.clone();
                }
            }
            let mut resolved = disk.state().clone();
            let stream: &mut Stream = resolved.streams.get_mut("S").unwrap();
            for seq in stream.messages.keys().copied().collect::<Vec<_>>() {
                stream.messages.insert(seq,disk.read_message(&stream.messages[&seq]).unwrap());
            }
            prop_assert_eq!(&resolved,&model);
        }
    }
}

#[test]
fn batch_body_read_failure_poisons_before_reservation_or_ack() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut disk = setup(SqliteStore::open(&path).unwrap(), Retention::Limits);
    for time in 0..4 {
        publish(&mut disk, time);
    }
    disk.checkpoint().unwrap();
    drop(disk.into_store());
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute(
        "UPDATE retained_content SET payload=zeroblob(length(payload)) WHERE seq=3",
        [],
    )
    .unwrap();
    drop(conn);
    let mut disk = Engine::recover(SqliteStore::open(&path).unwrap()).unwrap();
    let messages: Vec<_> = disk.state().streams["S"].messages.values().collect();
    assert!(disk.read_messages(&messages).is_err());
    assert!(disk.is_poisoned());
    assert_eq!(disk.state().streams["S"].consumers["D"].delivered, 0);
    let event = disk.state().prepare_delivery("S", "D", 5).unwrap().unwrap();
    assert!(disk.commit(event).is_err());
    drop(disk.into_store());
    let disk = Engine::recover(SqliteStore::open(&path).unwrap()).unwrap();
    assert_eq!(disk.state().streams["S"].consumers["D"].delivered, 0);
}
