use bytes::Bytes;
use proptest::prelude::*;
use screech::{
    durable::*,
    storage::{MemoryStore, SqliteStore, Store},
};
fn stream(retention: Retention) -> StreamConfig {
    StreamConfig {
        name: "S".into(),
        subjects: vec!["events.>".into()],
        retention,
        ..Default::default()
    }
}
fn consumer(name: &str) -> ConsumerConfig {
    ConsumerConfig {
        name: name.into(),
        durable_name: Some(name.into()),
        ack_wait: 10,
        ..Default::default()
    }
}
fn create<S: Store>(e: &mut Engine<S>, config: StreamConfig) {
    let event = e.state().prepare_create(config, 0).unwrap();
    e.commit(event).unwrap();
}
fn add<S: Store>(e: &mut Engine<S>, config: ConsumerConfig) {
    let event = e.state().prepare_consumer("S", config, 0).unwrap();
    e.commit(event).unwrap();
}
fn publish<S: Store>(e: &mut Engine<S>, subject: &str, headers: Bytes, time: u64) -> u64 {
    let event = e
        .state()
        .prepare_publish("S", subject, headers, Bytes::from_static(b"data"), time)
        .unwrap();
    e.commit(event).unwrap();
    e.state().streams["S"].last_sequence
}
fn take<S: Store>(e: &mut Engine<S>, name: &str, time: u64) -> Option<Delivery> {
    let ev = e.state().prepare_delivery("S", name, time).unwrap()?;
    let Event::Deliver { delivery, .. } = &ev else {
        unreachable!()
    };
    let d = delivery.clone();
    e.commit(ev).unwrap();
    Some(d)
}
fn ack<S: Store>(e: &mut Engine<S>, name: &str, d: &Delivery, action: AckAction, time: u64) {
    let generation = e.state().streams["S"].consumers[name].generation;
    let ev = e
        .state()
        .prepare_ack(
            "S",
            name,
            screech::durable::AckToken {
                generation,
                sequence: d.sequence,
                consumer_sequence: d.consumer_sequence,
            },
            action,
            time,
        )
        .unwrap();
    e.commit(ev).unwrap();
}
#[test]
fn reservations_ack_floors_redelivery_progress_nak_and_term() {
    let mut e = Engine::recover(MemoryStore::default()).unwrap();
    create(&mut e, stream(Retention::Limits));
    add(&mut e, consumer("D"));
    publish(&mut e, "events.a", Bytes::new(), 0);
    publish(&mut e, "events.b", Bytes::new(), 0);
    let a = take(&mut e, "D", 0).unwrap();
    let b = take(&mut e, "D", 0).unwrap();
    assert_eq!(a.attempt, 1);
    assert!(take(&mut e, "D", 9).is_none());
    ack(&mut e, "D", &b, AckAction::Ack, 1);
    assert_eq!(e.state().streams["S"].consumers["D"].ack_floor, 0);
    ack(&mut e, "D", &a, AckAction::Progress, 5);
    assert!(take(&mut e, "D", 10).is_none());
    let again = take(&mut e, "D", 15).unwrap();
    assert_eq!(again.sequence, a.sequence);
    assert_eq!(again.attempt, 2);
    // A delayed NAK from attempt 1 cannot postpone attempt 2.
    ack(&mut e, "D", &a, AckAction::Nak(100), 16);
    assert_eq!(
        e.state().streams["S"].consumers["D"].pending[&1].deadline,
        25
    );
    ack(&mut e, "D", &again, AckAction::Nak(2), 16);
    assert!(take(&mut e, "D", 17).is_none());
    let third = take(&mut e, "D", 18).unwrap();
    assert_eq!(third.attempt, 3);
    ack(&mut e, "D", &a, AckAction::Ack, 19);
    let c = &e.state().streams["S"].consumers["D"];
    assert!(c.pending.is_empty());
    assert_eq!(c.ack_floor, 4);
    assert_eq!(c.ack_stream, 2);
    publish(&mut e, "events.c", Bytes::new(), 20);
    let c = take(&mut e, "D", 20).unwrap();
    ack(&mut e, "D", &c, AckAction::Term, 21);
    assert!(take(&mut e, "D", 100).is_none());
}
#[test]
fn consumer_and_stream_generation_fence_old_acknowledgements() {
    let mut e = Engine::recover(MemoryStore::default()).unwrap();
    create(&mut e, stream(Retention::Workqueue));
    add(&mut e, consumer("D"));
    publish(&mut e, "events.a", Bytes::new(), 0);
    let old = take(&mut e, "D", 0).unwrap();
    let generation = e.state().streams["S"].consumers["D"].generation;
    e.commit(Event::ConsumerDelete {
        stream: "S".into(),
        name: "D".into(),
        generation,
    })
    .unwrap();
    add(&mut e, consumer("D"));
    let new = take(&mut e, "D", 0).unwrap();
    assert!(e
        .state()
        .prepare_ack(
            "S",
            "D",
            screech::durable::AckToken {
                generation,
                sequence: old.sequence,
                consumer_sequence: old.consumer_sequence
            },
            AckAction::Ack,
            0
        )
        .is_err());
    assert_eq!(e.state().streams["S"].messages.len(), 1);
    ack(&mut e, "D", &new, AckAction::Ack, 0);
    assert!(e.state().streams["S"].messages.is_empty());
    let sg = e.state().streams["S"].generation;
    e.commit(e.state().prepare_delete("S", false).unwrap())
        .unwrap();
    create(&mut e, stream(Retention::Workqueue));
    assert!(e.state().streams["S"].generation > sg);
}
#[test]
fn workqueue_interest_and_ack_all_retention() {
    for retention in [Retention::Workqueue, Retention::Interest] {
        let mut e = Engine::recover(MemoryStore::default()).unwrap();
        create(&mut e, stream(retention.clone()));
        add(&mut e, consumer("A"));
        if retention == Retention::Workqueue {
            assert!(e.state().prepare_consumer("S", consumer("B"), 0).is_err());
        } else {
            add(&mut e, consumer("B"));
        }
        publish(&mut e, "events.a", Bytes::new(), 0);
        let a = take(&mut e, "A", 0).unwrap();
        ack(&mut e, "A", &a, AckAction::Ack, 0);
        if retention == Retention::Interest {
            assert_eq!(e.state().streams["S"].messages.len(), 1);
            let b = take(&mut e, "B", 0).unwrap();
            ack(&mut e, "B", &b, AckAction::Ack, 0);
        }
        assert!(e.state().streams["S"].messages.is_empty());
    }
    let mut e = Engine::recover(MemoryStore::default()).unwrap();
    create(&mut e, stream(Retention::Limits));
    let mut c = consumer("D");
    c.ack_policy = AckPolicy::All;
    add(&mut e, c);
    for _ in 0..3 {
        publish(&mut e, "events.a", Bytes::new(), 0);
    }
    let _ = take(&mut e, "D", 0);
    let _ = take(&mut e, "D", 0);
    let third = take(&mut e, "D", 0).unwrap();
    ack(&mut e, "D", &third, AckAction::Ack, 0);
    assert!(e.state().streams["S"].consumers["D"].pending.is_empty());
    assert_eq!(e.state().streams["S"].messages.len(), 3);
}
#[test]
fn interest_without_consumers_drops_data_but_ack_sequence_and_dedup_survive() {
    let mut e = Engine::recover(MemoryStore::default()).unwrap();
    let mut cfg = stream(Retention::Interest);
    cfg.duplicate_window = 100;
    create(&mut e, cfg);
    let h = Bytes::from_static(b"NATS/1.0\r\nNats-Msg-Id: id\r\n\r\n");
    publish(&mut e, "events.a", h.clone(), 1);
    assert!(e.state().streams["S"].messages.is_empty());
    assert_eq!(e.state().streams["S"].duplicate(&h, 50), Some(1));
    e.checkpoint().unwrap();
    let e = Engine::recover(e.into_store()).unwrap();
    assert_eq!(e.state().streams["S"].duplicate(&h, 50), Some(1));
    assert_eq!(e.state().streams["S"].duplicate(&h, 101), None);
}
#[test]
fn limits_expiry_and_pending_cleanup_are_atomic() {
    let mut e = Engine::recover(MemoryStore::default()).unwrap();
    let mut cfg = stream(Retention::Limits);
    cfg.max_msgs = 2;
    cfg.max_msgs_per_subject = 1;
    cfg.max_age = 10;
    create(&mut e, cfg);
    add(&mut e, consumer("D"));
    publish(&mut e, "events.a", Bytes::new(), 0);
    let first = take(&mut e, "D", 0).unwrap();
    publish(&mut e, "events.a", Bytes::new(), 1);
    assert!(!e.state().streams["S"]
        .messages
        .contains_key(&first.sequence));
    assert!(e.state().streams["S"].consumers["D"].pending.is_empty());
    publish(&mut e, "events.b", Bytes::new(), 2);
    publish(&mut e, "events.c", Bytes::new(), 3);
    assert_eq!(
        e.state().streams["S"]
            .messages
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![3, 4]
    );
    e.commit(Event::Expire { time: 13 }).unwrap();
    assert!(e.state().streams["S"].messages.is_empty());
    assert_eq!(e.state().streams["S"].last_sequence, 4);
}
#[test]
fn discard_new_and_expectations_do_not_consume_sequence() {
    let mut e = Engine::recover(MemoryStore::default()).unwrap();
    let mut cfg = stream(Retention::Limits);
    cfg.max_msgs = 1;
    cfg.discard = Discard::New;
    create(&mut e, cfg);
    publish(&mut e, "events.a", Bytes::new(), 0);
    assert!(e
        .state()
        .prepare_publish("S", "events.a", Bytes::new(), Bytes::new(), 0)
        .is_err());
    assert_eq!(e.state().streams["S"].last_sequence, 1);
    e.commit(e.state().prepare_delete("S", true).unwrap())
        .unwrap();
    let bad = Bytes::from_static(b"NATS/1.0\r\nNats-Expected-Last-Sequence: 0\r\n\r\n");
    assert!(e
        .state()
        .prepare_publish("S", "events.a", bad, Bytes::new(), 0)
        .is_err());
    assert_eq!(publish(&mut e, "events.a", Bytes::new(), 0), 2);
}
#[test]
fn backoff_max_deliver_and_ack_pending_limits() {
    let mut e = Engine::recover(MemoryStore::default()).unwrap();
    create(&mut e, stream(Retention::Workqueue));
    let mut c = consumer("D");
    c.max_ack_pending = 1;
    c.max_deliver = 2;
    c.backoff = vec![5, 20];
    add(&mut e, c);
    publish(&mut e, "events.a", Bytes::new(), 0);
    publish(&mut e, "events.b", Bytes::new(), 0);
    let a = take(&mut e, "D", 0).unwrap();
    assert_eq!(a.deadline, 5);
    assert!(take(&mut e, "D", 4).is_none());
    let a = take(&mut e, "D", 5).unwrap();
    assert_eq!(a.deadline, 25);
    e.commit(Event::Expire { time: 25 }).unwrap();
    assert!(e.state().streams["S"].consumers["D"].exhausted.contains(&1));
    assert_eq!(take(&mut e, "D", 25).unwrap().sequence, 2);
    assert!(e.state().streams["S"].messages.contains_key(&1));
    ack(&mut e, "D", &a, AckAction::Ack, 25);
    assert!(!e.state().streams["S"].messages.contains_key(&1));
}
#[test]
fn checkpoint_recovery_preserves_delivery_dedup_and_purge() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data.db");
    let mut e = Engine::recover(SqliteStore::open(&path).unwrap()).unwrap();
    let mut cfg = stream(Retention::Limits);
    cfg.duplicate_window = 100;
    create(&mut e, cfg);
    add(&mut e, consumer("D"));
    let headers = Bytes::from_static(b"NATS/1.0\r\nNats-Msg-Id: x\r\n\r\n");
    publish(&mut e, "events.a", headers.clone(), 0);
    let d = take(&mut e, "D", 0).unwrap();
    e.checkpoint().unwrap();
    let expected = e.state().clone();
    drop(e);
    let mut e = Engine::recover(SqliteStore::open(&path).unwrap()).unwrap();
    assert_eq!(e.state(), &expected);
    assert_eq!(e.state().streams["S"].duplicate(&headers, 1), Some(1));
    ack(&mut e, "D", &d, AckAction::Ack, 1);
    e.commit(e.state().prepare_delete("S", true).unwrap())
        .unwrap();
    e.checkpoint().unwrap();
    drop(e);
    let e = Engine::recover(SqliteStore::open(path).unwrap()).unwrap();
    assert!(e.state().streams["S"].messages.is_empty());
    assert!(e.state().streams["S"].consumers["D"].pending.is_empty());
    assert_eq!(e.state().streams["S"].duplicate(&headers, 1), Some(1));
}
#[test]
fn last_per_subject_does_not_skip_publications_during_initial_delivery() {
    let mut e = Engine::recover(MemoryStore::default()).unwrap();
    create(&mut e, stream(Retention::Limits));
    publish(&mut e, "events.a", Bytes::new(), 0);
    publish(&mut e, "events.a", Bytes::new(), 0);
    publish(&mut e, "events.b", Bytes::new(), 0);
    let mut c = consumer("D");
    c.deliver_policy = DeliverPolicy::LastPerSubject;
    add(&mut e, c);
    assert_eq!(take(&mut e, "D", 0).unwrap().sequence, 2);
    publish(&mut e, "events.c", Bytes::new(), 0);
    assert_eq!(take(&mut e, "D", 0).unwrap().sequence, 3);
    assert_eq!(take(&mut e, "D", 0).unwrap().sequence, 4);
}
proptest! {
 #[test]
 fn durable_queue_refines_independent_oracle(ops in prop::collection::vec(0u8..7,0..150)){
  let mut e=Engine::recover(MemoryStore::default()).unwrap();create(&mut e,stream(Retention::Workqueue));add(&mut e,consumer("D"));
  let mut time=0;let mut last=0u64;let mut cursor=1;let mut live=std::collections::BTreeSet::new();let mut pending=std::collections::BTreeMap::<u64,(u64,u64)>::new();
  for op in ops {match op {
   0|1=>{last+=1;live.insert(last);publish(&mut e,"events.a",Bytes::new(),time);},
   2=>{let candidate=pending.iter().find(|(_,(_,due))|*due<=time).map(|(&n,(attempt,_))|(n,attempt+1)).or_else(||live.range(cursor..).next().map(|n|(*n,1)));let got=take(&mut e,"D",time);prop_assert_eq!(got.as_ref().map(|d|(d.sequence,d.attempt)),candidate);if let Some((n,attempt))=candidate{pending.insert(n,(attempt,time+10));cursor=cursor.max(n+1);}},
   3=>{if let Some((&n,_))=pending.iter().next(){let c=&e.state().streams["S"].consumers["D"];let p=&c.pending[&n];let ev=e.state().prepare_ack("S","D", screech::durable::AckToken{generation:c.generation,sequence:n,consumer_sequence:p.consumer_sequence},AckAction::Ack,time).unwrap();e.commit(ev).unwrap();pending.remove(&n);live.remove(&n);}},
   4=>time+=5,
   5=>{e.checkpoint().unwrap();e=Engine::recover(e.into_store()).unwrap();},
   _=>{let state=e.state().clone();e=Engine::recover(e.into_store()).unwrap();prop_assert_eq!(e.state(),&state);}
  }e.state().check_invariants().unwrap();prop_assert_eq!(e.state().streams["S"].messages.keys().copied().collect::<std::collections::BTreeSet<_>>(),live.clone());prop_assert_eq!(e.state().streams["S"].consumers["D"].pending.keys().copied().collect::<std::collections::BTreeSet<_>>(),pending.keys().copied().collect::<std::collections::BTreeSet<_>>());}
 }
}

#[test]
fn ack_history_is_bounded_by_outstanding_messages_not_traffic() {
    let mut e = Engine::recover(MemoryStore::default()).unwrap();
    create(&mut e, stream(Retention::Workqueue));
    add(&mut e, consumer("D"));
    publish(&mut e, "events.a", Bytes::new(), 0);
    let first = take(&mut e, "D", 0).unwrap();
    for _ in 0..3000 {
        publish(&mut e, "events.a", Bytes::new(), 0);
        let d = take(&mut e, "D", 0).unwrap();
        ack(&mut e, "D", &d, AckAction::Ack, 0);
        assert_eq!(e.state().streams["S"].consumers["D"].history.len(), 1);
    }
    assert_eq!(e.state().streams["S"].consumers["D"].num_pending, 0);
    ack(&mut e, "D", &first, AckAction::Ack, 0);
    assert_eq!(e.state().streams["S"].consumers["D"].ack_floor, 3001);
}
#[test]
fn damaged_checkpoint_is_not_ignored_or_rebuilt_as_empty() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut e = Engine::recover(SqliteStore::open(&path).unwrap()).unwrap();
    create(&mut e, stream(Retention::Workqueue));
    add(&mut e, consumer("D"));
    publish(&mut e, "events.a", Bytes::new(), 0);
    e.checkpoint().unwrap();
    drop(e);
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute("UPDATE snapshot SET data=?1", [b"corrupt".as_slice()])
        .unwrap();
    drop(conn);
    assert!(Engine::recover(SqliteStore::open(path).unwrap()).is_err());
}
#[test]
fn legacy_v1_log_upgrades_without_resetting_sequence() {
    let mut store = MemoryStore::default();
    store
        .append(
            "$SCREECH.event.v1",
            &[],
            br#"{"Create":{"config":{"name":"S","subjects":["events.>"]},"time":0}}"#,
        )
        .unwrap();
    store.append("$SCREECH.event.v1",&[],br#"{"Publish":{"stream":"S","message":{"sequence":1,"subject":"events.a","headers":[],"payload":[111,108,100],"time":0}}}"#).unwrap();
    let mut e = Engine::recover(store).unwrap();
    assert_eq!(e.state().streams["S"].messages[&1].payload, "old");
    assert_eq!(publish(&mut e, "events.a", Bytes::new(), 0), 2);
    e.checkpoint().unwrap();
    let e = Engine::recover(e.into_store()).unwrap();
    assert_eq!(e.state().streams["S"].messages.len(), 2);
}

#[test]
fn stream_update_enforces_limits_atomically_and_preserves_sequence_and_dedup() {
    let mut e = Engine::recover(MemoryStore::default()).unwrap();
    let mut cfg = stream(Retention::Limits);
    cfg.duplicate_window = 100;
    create(&mut e, cfg.clone());
    add(&mut e, consumer("D"));
    for _ in 0..5 {
        publish(&mut e, "events.a", Bytes::new(), 0);
    }
    let first = take(&mut e, "D", 0).unwrap();
    cfg.max_msgs = 2;
    e.commit(e.state().prepare_update(cfg, 0).unwrap()).unwrap();
    assert_eq!(
        e.state().streams["S"]
            .messages
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![4, 5]
    );
    assert!(!e.state().streams["S"].consumers["D"]
        .pending
        .contains_key(&first.sequence));
    assert_eq!(e.state().streams["S"].consumers["D"].num_pending, 2);
    assert_eq!(publish(&mut e, "events.a", Bytes::new(), 0), 6);
}

#[test]
fn term_on_ack_all_consumer_does_not_ack_other_pending_messages() {
    let mut e = Engine::recover(MemoryStore::default()).unwrap();
    create(&mut e, stream(Retention::Limits));
    let mut cfg = consumer("D");
    cfg.ack_policy = AckPolicy::All;
    add(&mut e, cfg);
    for _ in 0..3 {
        publish(&mut e, "events.a", Bytes::new(), 0);
    }
    take(&mut e, "D", 0);
    take(&mut e, "D", 0);
    let last = take(&mut e, "D", 0).unwrap();
    ack(&mut e, "D", &last, AckAction::Term, 0);
    ack(&mut e, "D", &last, AckAction::Term, 0);
    assert_eq!(
        e.state().streams["S"].consumers["D"]
            .pending
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(e.state().streams["S"].consumers["D"].ack_floor, 0);
}

proptest! {
 #[test]
 fn dedup_and_age_limits_refine_independent_history(ops in prop::collection::vec((0u8..5,0u8..8,0u64..8),0..200)){
  let mut e=Engine::recover(MemoryStore::default()).unwrap();let mut cfg=stream(Retention::Limits);cfg.max_msgs=5;cfg.max_age=15;cfg.duplicate_window=20;create(&mut e,cfg);
  let mut time=0u64;let mut next=0u64;let mut records=std::collections::BTreeMap::<u64,u64>::new();let mut ids=std::collections::BTreeMap::<u8,(u64,u64)>::new();
  for(op,id,advance)in ops {time+=advance;
   records.retain(|_,t|*t+15>time);ids.retain(|_,(_,t)|*t+20>time);if e.state().expiration_needed(time){e.commit(Event::Expire{time}).unwrap();}
   match op {
    0..=2=>{let h=Bytes::from(format!("NATS/1.0\r\nNats-Msg-Id: {id}\r\n\r\n"));let expected=ids.get(&id).map(|(seq,_)|*seq);prop_assert_eq!(e.state().streams["S"].duplicate(&h,time),expected);if expected.is_none(){next+=1;records.insert(next,time);ids.insert(id,(next,time));while records.len()>5{records.pop_first();}publish(&mut e,"events.a",h,time);}},
    3=>{e.checkpoint().unwrap();e=Engine::recover(e.into_store()).unwrap();},
    _=>{e.commit(e.state().prepare_delete("S",true).unwrap()).unwrap();records.clear();}
   }
   prop_assert_eq!(e.state().streams["S"].last_sequence,next);prop_assert_eq!(e.state().streams["S"].messages.keys().copied().collect::<Vec<_>>(),records.keys().copied().collect::<Vec<_>>());e.state().check_invariants().unwrap();
  }
 }
}

#[test]
fn file_byte_limits_and_last_id_expectations_survive_removal() {
    let mut e = Engine::recover(MemoryStore::default()).unwrap();
    let mut cfg = stream(Retention::Workqueue);
    cfg.max_bytes = 150;
    cfg.duplicate_window = 100;
    create(&mut e, cfg);
    add(&mut e, consumer("D"));
    let h = Bytes::from_static(b"NATS/1.0\r\nNats-Msg-Id: previous\r\n\r\n");
    publish(&mut e, "events.a", h, 0);
    let d = take(&mut e, "D", 0).unwrap();
    ack(&mut e, "D", &d, AckAction::Ack, 0);
    let condition = Bytes::from_static(b"NATS/1.0\r\nNats-Expected-Last-Msg-Id: previous\r\n\r\n");
    assert_eq!(publish(&mut e, "events.a", condition, 0), 2);
    assert!(e.state().streams["S"].bytes <= 150);
    e.state().check_invariants().unwrap();
}

#[test]
fn consumer_update_preserves_generation_and_retimes_pending_except_delayed_naks() {
    let mut e = Engine::recover(MemoryStore::default()).unwrap();
    create(&mut e, stream(Retention::Limits));
    add(&mut e, consumer("D"));
    publish(&mut e, "events.a", Bytes::new(), 0);
    publish(&mut e, "events.b", Bytes::new(), 0);
    let a = take(&mut e, "D", 0).unwrap();
    let b = take(&mut e, "D", 0).unwrap();
    ack(&mut e, "D", &b, AckAction::Nak(100), 1);
    let generation = e.state().streams["S"].consumers["D"].generation;
    let mut cfg = consumer("D");
    cfg.ack_wait = 5;
    cfg.max_ack_pending = 1;
    e.commit(e.state().prepare_consumer_update("S", cfg, 2).unwrap())
        .unwrap();
    let c = &e.state().streams["S"].consumers["D"];
    assert_eq!(c.generation, generation);
    assert_eq!(c.pending[&a.sequence].deadline, 5);
    assert_eq!(c.pending[&b.sequence].deadline, 101);
    assert_eq!(take(&mut e, "D", 5).unwrap().sequence, a.sequence);
    e.checkpoint().unwrap();
    let copy = Engine::recover(e.into_store()).unwrap();
    copy.state().check_invariants().unwrap();
}

proptest! {
    #[test]
    fn batched_reservations_and_acks_refine_serial_commits(
        count in 1usize..80, limit in 1usize..65, max_pending in 1i64..40,
        budget in 0usize..512, policy in 0u8..3, retention in 0u8..3
    ) {
        let retention = [Retention::Limits, Retention::Interest, Retention::Workqueue][retention as usize].clone();
        let mut e = Engine::recover(MemoryStore::default()).unwrap();
        create(&mut e, stream(retention.clone()));
        let mut config = consumer("D");
        config.max_ack_pending = max_pending;
        config.ack_policy = if retention == Retention::Workqueue { AckPolicy::Explicit }
            else { [AckPolicy::None, AckPolicy::All, AckPolicy::Explicit][policy as usize].clone() };
        add(&mut e, config);
        for _ in 0..count { publish(&mut e, "events.a", Bytes::new(), 0); }
        let mut serial = Engine::recover(e.into_store()).unwrap();
        let mut batched = Engine::recover(MemoryStore::default()).unwrap();
        // Build a second independent store from the same logical starting state.
        create(&mut batched, serial.state().streams["S"].config.clone());
        add(&mut batched, serial.state().streams["S"].consumers["D"].config.clone());
        for _ in 0..count { publish(&mut batched, "events.a", Bytes::new(), 0); }
        for time in [0, 10, 20] {
            let event = batched.state().prepare_delivery_batch("S", "D", time, limit, budget).unwrap();
            let mut deliveries = Vec::new(); let mut used = 0;
            for _ in 0..limit {
                let Some(ev) = serial.state().prepare_delivery("S", "D", time).unwrap() else { break };
                let Event::Deliver { delivery, .. } = &ev else { unreachable!() };
                let size = serial.state().streams["S"].messages[&delivery.sequence].size() as usize;
                if budget > 0 && size > budget.saturating_sub(used) { break; }
                used += size; deliveries.push(delivery.clone()); serial.commit(ev).unwrap();
            }
            prop_assert_eq!(event.is_some(), !deliveries.is_empty());
            if let Some(event) = event { batched.commit(event).unwrap(); }
            prop_assert_eq!(serial.state(), batched.state());
            let mut events = Vec::new();
            for d in deliveries.iter().rev() {
                let event = batched.state().prepare_ack("S", "D", AckToken {
                    generation: batched.state().streams["S"].consumers["D"].generation,
                    sequence: d.sequence, consumer_sequence: d.consumer_sequence,
                }, AckAction::Ack, time).unwrap();
                serial.commit(event.clone()).unwrap(); events.push(event);
            }
            if events.len() >= 2 { batched.commit(Event::AckBatch { events }).unwrap(); }
            else if let Some(event) = events.pop() { batched.commit(event).unwrap(); }
            prop_assert_eq!(serial.state(), batched.state());
            batched.state().check_invariants().unwrap();
            batched = Engine::recover(batched.into_store()).unwrap();
            prop_assert_eq!(serial.state(), batched.state());
        }
    }
}

#[test]
fn invalid_batches_leave_log_and_state_unchanged() {
    let mut e = Engine::recover(MemoryStore::default()).unwrap();
    create(&mut e, stream(Retention::Workqueue));
    add(&mut e, consumer("D"));
    for _ in 0..3 {
        publish(&mut e, "events.a", Bytes::new(), 0);
    }
    let before = e.state().clone();
    let mut event = e
        .state()
        .prepare_delivery_batch("S", "D", 0, 3, 0)
        .unwrap()
        .unwrap();
    if let Event::DeliverBatch { deliveries, .. } = &mut event {
        deliveries[1].sequence = 3;
    }
    assert!(e.commit(event).is_err());
    assert_eq!(e.state(), &before);
    assert!(e
        .commit(Event::AckBatch {
            events: vec![Event::Expire { time: 0 }; 2]
        })
        .is_err());
    assert_eq!(e.state(), &before);
    assert!(!e.is_poisoned());
    let store = e.into_store();
    assert_eq!(store.last_sequence().unwrap(), 5);
}

#[test]
fn ambiguous_batch_store_errors_poison_without_exposing_partial_effects() {
    struct Ambiguous {
        store: MemoryStore,
        fail: std::sync::Arc<std::sync::atomic::AtomicBool>,
        written: bool,
    }
    impl Store for Ambiguous {
        fn append(
            &mut self,
            subject: &str,
            headers: &[u8],
            payload: &[u8],
        ) -> Result<u64, screech::storage::StoreError> {
            if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
                if self.written {
                    self.store.append(subject, headers, payload)?;
                }
                return Err(screech::storage::StoreError::Invalid);
            }
            self.store.append(subject, headers, payload)
        }
        fn get(
            &self,
            seq: u64,
        ) -> Result<Option<screech::storage::Record>, screech::storage::StoreError> {
            self.store.get(seq)
        }
        fn last_sequence(&self) -> Result<u64, screech::storage::StoreError> {
            self.store.last_sequence()
        }
        fn purge(&mut self) -> Result<(), screech::storage::StoreError> {
            self.store.purge()
        }
    }
    for written in [false, true] {
        for mode in 0..3 {
            let fail = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let mut e = Engine::recover(Ambiguous {
                store: MemoryStore::default(),
                fail: fail.clone(),
                written,
            })
            .unwrap();
            create(&mut e, stream(Retention::Workqueue));
            add(&mut e, consumer("D"));
            for _ in 0..8 {
                publish(&mut e, "events.a", Bytes::new(), 0);
            }
            let mut event = e
                .state()
                .prepare_delivery_batch("S", "D", 0, 8, 0)
                .unwrap()
                .unwrap();
            if mode != 0 {
                e.commit(event).unwrap();
                let c = &e.state().streams["S"].consumers["D"];
                event = Event::AckBatch {
                    events: c
                        .pending
                        .iter()
                        .map(|(&seq, p)| {
                            e.state()
                                .prepare_ack(
                                    "S",
                                    "D",
                                    AckToken {
                                        generation: c.generation,
                                        sequence: seq,
                                        consumer_sequence: p.consumer_sequence,
                                    },
                                    AckAction::Ack,
                                    0,
                                )
                                .unwrap()
                        })
                        .collect(),
                };
                if mode == 2 {
                    let c = &e.state().streams["S"].consumers["D"];
                    let ack = e
                        .state()
                        .prepare_ack(
                            "S",
                            "D",
                            AckToken {
                                generation: c.generation,
                                sequence: 1,
                                consumer_sequence: c.pending[&1].consumer_sequence,
                            },
                            AckAction::Ack,
                            10,
                        )
                        .unwrap();
                    event = e.state().prepare_ack_next(ack, 10).unwrap().unwrap();
                }
            }
            let before = e.state().clone();
            let mut expected = before.clone();
            if written {
                expected.apply(event.clone()).unwrap();
            }
            fail.store(true, std::sync::atomic::Ordering::SeqCst);
            assert!(e.commit(event.clone()).is_err());
            assert!(e.is_poisoned());
            assert_eq!(e.state(), &before);
            assert!(e.commit(event).is_err());
            let e = Engine::recover(e.into_store().store).unwrap();
            e.state().check_invariants().unwrap();
            assert_eq!(e.state(), &expected);
        }
    }
}

#[test]
fn reservation_batch_prioritizes_due_attempts_and_respects_pending_limit() {
    let mut e = Engine::recover(MemoryStore::default()).unwrap();
    create(&mut e, stream(Retention::Workqueue));
    let mut c = consumer("D");
    c.max_ack_pending = 4;
    add(&mut e, c);
    for _ in 0..6 {
        publish(&mut e, "events.a", Bytes::new(), 0);
    }
    for _ in 0..3 {
        take(&mut e, "D", 0).unwrap();
    }
    let event = e
        .state()
        .prepare_delivery_batch("S", "D", 10, 64, 0)
        .unwrap()
        .unwrap();
    let Event::DeliverBatch { deliveries, .. } = &event else {
        unreachable!()
    };
    assert_eq!(
        deliveries
            .iter()
            .map(|d| (d.sequence, d.attempt))
            .collect::<Vec<_>>(),
        vec![(1, 2), (2, 2), (3, 2), (4, 1)]
    );
    assert_eq!(
        deliveries
            .iter()
            .map(|d| d.consumer_sequence)
            .collect::<Vec<_>>(),
        vec![4, 5, 6, 7]
    );
    e.commit(event).unwrap();
    e.state().check_invariants().unwrap();
    let c = &e.state().streams["S"].consumers["D"];
    assert_eq!(c.pending.len(), 4);
    assert_eq!(c.num_pending, 2);
    assert!(e
        .state()
        .prepare_delivery_batch("S", "D", 11, 64, 0)
        .unwrap()
        .is_none());
    let expected = e.state().clone();
    assert_eq!(Engine::recover(e.into_store()).unwrap().state(), &expected);
}

proptest! {
    #[test]
    fn ack_next_refines_serial_ack_and_delivery(
        count in 2usize..40, initial in 1usize..8, ack_all in any::<bool>(),
        retention in 0usize..3, times in prop::collection::vec(0u64..15, 1..40)
    ) {
        let retention = [Retention::Limits, Retention::Interest, Retention::Workqueue][retention].clone();
        let mut c = consumer("D");
        c.max_ack_pending = 4;
        if ack_all && retention != Retention::Workqueue { c.ack_policy = AckPolicy::All; }
        let mut fused = Engine::recover(MemoryStore::default()).unwrap();
        let mut serial = Engine::recover(MemoryStore::default()).unwrap();
        for e in [&mut fused, &mut serial] {
            create(e, stream(retention.clone())); add(e, c.clone());
            for _ in 0..count { publish(e, "events.a", Bytes::new(), 0); }
            for _ in 0..initial { take(e, "D", 0); }
        }
        let mut time = 0;
        for delta in times {
            time += delta;
            let c = &fused.state().streams["S"].consumers["D"];
            let Some((&seq, p)) = c.pending.iter().next() else { break };
            let ack = fused.state().prepare_ack("S", "D", AckToken {
                generation: c.generation, sequence: seq, consumer_sequence: p.consumer_sequence,
            }, AckAction::Ack, time).unwrap();
            let combined = fused.state().prepare_ack_next(ack.clone(), time).unwrap();
            serial.commit(ack.clone()).unwrap();
            let next = serial.state().prepare_delivery("S", "D", time).unwrap();
            prop_assert_eq!(combined.is_some(), next.is_some());
            if let Some(next) = next { serial.commit(next).unwrap(); }
            fused.commit(combined.unwrap_or(ack)).unwrap();
            prop_assert_eq!(serial.state(), fused.state());
            fused.state().check_invariants().unwrap();
            fused = Engine::recover(fused.into_store()).unwrap();
            prop_assert_eq!(serial.state(), fused.state());
        }
    }
}

#[test]
fn ack_next_rejects_forged_delivery_before_mutating_state() {
    let mut e = Engine::recover(MemoryStore::default()).unwrap();
    create(&mut e, stream(Retention::Workqueue));
    add(&mut e, consumer("D"));
    for _ in 0..3 {
        publish(&mut e, "events.a", Bytes::new(), 0);
    }
    let d = take(&mut e, "D", 0).unwrap();
    let ack = e
        .state()
        .prepare_ack(
            "S",
            "D",
            AckToken {
                generation: e.state().streams["S"].consumers["D"].generation,
                sequence: d.sequence,
                consumer_sequence: d.consumer_sequence,
            },
            AckAction::Ack,
            1,
        )
        .unwrap();
    let mut combined = e.state().prepare_ack_next(ack, 1).unwrap().unwrap();
    if let Event::AckNext { next, .. } = &mut combined {
        if let Event::Deliver { delivery, .. } = next.as_mut() {
            delivery.sequence = 3;
        }
    }
    let state = e.state().clone();
    assert!(e.commit(combined).is_err());
    assert_eq!(e.state(), &state);
    assert!(!e.is_poisoned());
}

proptest! {
    #[test]
    fn mixed_groups_refine_serial_transitions_and_recovery(
        ops in prop::collection::vec((0u8..8,0usize..3,0u64..6,0u8..5),1..120), width in 1usize..17
    ) {
        let mut grouped = Engine::recover(MemoryStore::default()).unwrap();
        let mut serial = Engine::recover(MemoryStore::default()).unwrap();
        for engine in [&mut grouped, &mut serial] {
            for i in 0..3 {
                let name=format!("S{i}");
                engine.commit(engine.state().prepare_create(StreamConfig { name:name.clone(), subjects:vec![format!("s{i}.>")],
                    retention:[Retention::Limits,Retention::Interest,Retention::Workqueue][i].clone(),
                    max_msgs:32, duplicate_window:25, ..Default::default() },0).unwrap()).unwrap();
                let mut config=consumer("D");config.max_ack_pending=8;
                engine.commit(engine.state().prepare_consumer(&name,config,0).unwrap()).unwrap();
            }
        }
        let mut time=0;
        for chunk in ops.chunks(width) {
            let before=grouped.state().clone();
            let mut plan=grouped.begin_group().unwrap();
            for &(operation,index,delta,id) in chunk {
                time+=delta;
                let name=format!("S{index}");let subject=format!("s{index}.a");
                let event=match operation {
                    0|1|7 => {
                        let headers=Bytes::from(format!("NATS/1.0\r\nNats-Msg-Id: {id}\r\n\r\n"));
                        if plan.state().streams[&name].duplicate(&headers,time).is_some() { None }
                        else { Some(plan.state().prepare_publish(&name,&subject,headers,Bytes::from_static(b"binary\0payload"),time).unwrap()) }
                    }
                    2 => plan.state().prepare_delivery(&name,"D",time).unwrap(),
                    3..=5 => {
                        let c=&plan.state().streams[&name].consumers["D"];
                        c.pending.iter().next().map(|(&seq,p)| plan.state().prepare_ack(&name,"D",AckToken {
                            generation:c.generation,sequence:seq,consumer_sequence:p.consumer_sequence,
                        },[AckAction::Ack,AckAction::Nak(3),AckAction::Progress][(operation-3) as usize].clone(),time).unwrap())
                    }
                    _ => Some(Event::Expire{time}),
                };
                if let Some(event)=event { serial.commit(event.clone()).unwrap();plan.commit(event).unwrap(); }
                prop_assert_eq!(grouped.state(),&before,"speculative state escaped");
                prop_assert_eq!(plan.state(),serial.state());
            }
            grouped.commit_group(plan).unwrap();
            prop_assert_eq!(grouped.state(),serial.state());grouped.state().check_invariants().unwrap();
            grouped.checkpoint().unwrap();
            grouped=Engine::recover(grouped.into_store()).unwrap();
            prop_assert_eq!(grouped.state(),serial.state());
        }
    }
}

#[test]
fn stale_and_foreign_plans_never_append_or_replace_committed_state() {
    let mut first = Engine::recover(MemoryStore::default()).unwrap();
    create(&mut first, stream(Retention::Limits));
    let mut second = Engine::recover(MemoryStore::default()).unwrap();
    create(&mut second, stream(Retention::Limits));
    let mut foreign = second.begin_group().unwrap();
    foreign
        .commit(
            foreign
                .state()
                .prepare_publish(
                    "S",
                    "events.a",
                    Bytes::new(),
                    Bytes::from_static(b"foreign"),
                    0,
                )
                .unwrap(),
        )
        .unwrap();
    let before = first.state().clone();
    assert!(first.commit_group(foreign).is_err());
    assert_eq!(first.state(), &before);
    let mut stale = first.begin_group().unwrap();
    stale
        .commit(
            stale
                .state()
                .prepare_publish(
                    "S",
                    "events.a",
                    Bytes::new(),
                    Bytes::from_static(b"stale"),
                    0,
                )
                .unwrap(),
        )
        .unwrap();
    publish(&mut first, "events.a", Bytes::new(), 0);
    let before = first.state().clone();
    assert!(first.commit_group(stale).is_err());
    assert_eq!(first.state(), &before);
    assert!(!first.is_poisoned());
}

#[test]
fn group_store_error_recovers_whole_group_or_none_without_exposing_projection() {
    struct Fails {
        inner: MemoryStore,
        written: bool,
        fail: bool,
    }
    impl Store for Fails {
        fn append(
            &mut self,
            s: &str,
            h: &[u8],
            p: &[u8],
        ) -> Result<u64, screech::storage::StoreError> {
            self.inner.append(s, h, p)
        }
        fn get(
            &self,
            n: u64,
        ) -> Result<Option<screech::storage::Record>, screech::storage::StoreError> {
            self.inner.get(n)
        }
        fn last_sequence(&self) -> Result<u64, screech::storage::StoreError> {
            self.inner.last_sequence()
        }
        fn purge(&mut self) -> Result<(), screech::storage::StoreError> {
            self.inner.purge()
        }
        fn supports_batch(&self) -> bool {
            true
        }
        fn append_batch(
            &mut self,
            records: &[screech::storage::Append],
        ) -> Result<u64, screech::storage::StoreError> {
            if !self.fail {
                return self.inner.append_batch(records);
            }
            if self.written {
                self.inner.append_batch(records)?;
            }
            Err(screech::storage::StoreError::Invalid)
        }
    }
    for written in [false, true] {
        let mut engine = Engine::recover(Fails {
            inner: MemoryStore::default(),
            written,
            fail: true,
        })
        .unwrap();
        create(&mut engine, stream(Retention::Workqueue));
        add(&mut engine, consumer("D"));
        // Populate several persistent-tree nodes before the failing group.
        for i in 0..160 {
            publish(
                &mut engine,
                &format!("events.s{}", i % 7),
                Bytes::from(format!("NATS/1.0\r\nNats-Msg-Id: seed{i}\r\n\r\n")),
                0,
            );
        }
        for _ in 0..80 {
            take(&mut engine, "D", 0).unwrap();
        }
        let before = engine.state().clone();
        let mut plan = engine.begin_group().unwrap();
        for sequence in [1, 31, 64, 80] {
            let c = &plan.state().streams["S"].consumers["D"];
            let event = plan
                .state()
                .prepare_ack(
                    "S",
                    "D",
                    AckToken {
                        generation: c.generation,
                        sequence,
                        consumer_sequence: c.pending[&sequence].consumer_sequence,
                    },
                    AckAction::Ack,
                    0,
                )
                .unwrap();
            plan.commit(event).unwrap();
        }
        plan.commit(
            plan.state()
                .prepare_publish(
                    "S",
                    "events.a",
                    Bytes::new(),
                    Bytes::from_static(b"value"),
                    0,
                )
                .unwrap(),
        )
        .unwrap();
        plan.commit(plan.state().prepare_delivery("S", "D", 0).unwrap().unwrap())
            .unwrap();
        let expected = plan.state().clone();
        assert!(engine.commit_group(plan).is_err());
        assert!(engine.is_poisoned());
        assert_eq!(engine.state(), &before);
        let recovered = Engine::recover(engine.into_store().inner).unwrap();
        assert_eq!(recovered.state(), if written { &expected } else { &before });
    }
}

proptest! {
    #[test]
    fn abandoned_tree_projections_leave_sibling_and_committed_state_untouched(
        count in 65usize..194, removed in prop::collection::vec(1u64..195, 1..35),
        retention in 0usize..3,
    ) {
        let mut engine = Engine::recover(MemoryStore::default()).unwrap();
        create(&mut engine, stream([Retention::Limits, Retention::Interest, Retention::Workqueue][retention].clone()));
        add(&mut engine, consumer("D"));
        for n in 0..count {
            publish(&mut engine, &format!("events.s{}", n % 5),
                Bytes::from(format!("NATS/1.0\r\nNats-Msg-Id: id{n}\r\n\r\n")), 0);
        }
        let committed = engine.state().clone();
        let mut abandoned = engine.begin_group().unwrap();
        let mut survivor = engine.begin_group().unwrap();
        for seq in removed.into_iter().filter(|n| *n <= count as u64) {
            if abandoned.state().streams["S"].messages.contains_key(&seq) {
                abandoned.commit(Event::Remove { stream: "S".into(), sequences: vec![seq] }).unwrap();
            }
            prop_assert_eq!(engine.state(), &committed);
            prop_assert_eq!(survivor.state(), &committed);
        }
        // Exercise nested pending/history/retention indexes on the surviving fork.
        for _ in 0..32 {
            let delivery = survivor.state().prepare_delivery("S", "D", 0).unwrap().unwrap();
            survivor.commit(delivery).unwrap();
        }
        let pending: Vec<_> = survivor.state().streams["S"].consumers["D"].pending.iter()
            .map(|(&sequence,p)| (sequence,p.consumer_sequence)).collect();
        for (sequence, consumer_sequence) in pending {
            let c=&survivor.state().streams["S"].consumers["D"];
            let event=survivor.state().prepare_ack("S","D",AckToken {
                generation:c.generation,sequence,consumer_sequence,
            },AckAction::Ack,0).unwrap();
            survivor.commit(event).unwrap();
        }
        let expected=survivor.state().clone();
        drop(abandoned);
        prop_assert_eq!(engine.state(), &committed);
        engine.commit_group(survivor).unwrap();
        prop_assert_eq!(engine.state(), &expected);
        engine.state().check_invariants().unwrap();
        engine.checkpoint().unwrap();
        let recovered=Engine::recover(engine.into_store()).unwrap();
        prop_assert_eq!(recovered.state(), &expected);
    }
}

proptest! {
    #[test]
    fn ack_floors_refine_history_through_redelivery_and_exhaustion(
        count in 65usize..100, ops in prop::collection::vec((0u8..7,any::<bool>(),0u64..5),30..130)
    ) {
        let mut engine=Engine::recover(MemoryStore::default()).unwrap();
        create(&mut engine,stream(Retention::Workqueue));
        let mut config=consumer("D");config.max_deliver=2;config.ack_wait=3;add(&mut engine,config);
        for _ in 0..count { publish(&mut engine,"events.a",Bytes::new(),0); }
        for _ in 0..64 { take(&mut engine,"D",0); }
        let mut time=0;
        for (op,last,delta) in ops {
            time+=delta;
            if op==0 { take(&mut engine,"D",time); }
            else if op==5 { engine.commit(Event::Expire{time}).unwrap(); }
            else if op==6 {
                engine.checkpoint().unwrap();engine=Engine::recover(engine.into_store()).unwrap();
            } else {
                let c=&engine.state().streams["S"].consumers["D"];
                let chosen=if last { c.history.iter().next_back() } else { c.history.iter().next() };
                if let Some((&original,(sequence,_)))=chosen {
                    let sequence=*sequence;
                    let consumer_sequence=c.pending.get(&sequence).map(|p|p.consumer_sequence).unwrap_or(original);
                    let event=engine.state().prepare_ack("S","D",AckToken{generation:c.generation,sequence,consumer_sequence},
                        [AckAction::Ack,AckAction::Nak(1),AckAction::Progress,AckAction::Term][(op-1) as usize].clone(),time).unwrap();
                    engine.commit(event).unwrap();
                }
            }
            let c=&engine.state().streams["S"].consumers["D"];
            prop_assert_eq!(c.ack_floor,c.history.keys().next().map(|n|n-1).unwrap_or(c.delivered));
            prop_assert_eq!(c.ack_stream,c.history.values().map(|(s,_)|s-1).min().unwrap_or(c.last_stream));
            engine.state().check_invariants().unwrap();
        }
    }
}

#[test]
fn recovery_rejects_inconsistent_ack_history_indexes() {
    for mutation in 0..6 {
        let mut engine = Engine::recover(MemoryStore::default()).unwrap();
        create(&mut engine, stream(Retention::Workqueue));
        add(&mut engine, consumer("D"));
        publish(&mut engine, "events.a", Bytes::new(), 0);
        take(&mut engine, "D", 0).unwrap();
        let mut state = engine.state().clone();
        let c = state
            .streams
            .get_mut("S")
            .unwrap()
            .consumers
            .get_mut("D")
            .unwrap();
        match mutation {
            1 => {
                c.history.clear();
            }
            2 => {
                c.delivered = 2;
                c.history.insert(2, (1, false));
            }
            3 => {
                c.delivered = 2;
                c.pending.get_mut(&1).unwrap().consumer_sequence = 2;
            }
            4 => {
                c.ack_floor = 1;
            }
            5 => {
                c.ack_stream = 1;
            }
            _ => (),
        }
        let bytes = postcard::to_allocvec(&(2u8, state)).unwrap();
        let mut store = engine.into_store();
        store
            .checkpoint(store.last_sequence().unwrap(), &bytes)
            .unwrap();
        assert_eq!(Engine::recover(store).is_ok(), mutation == 0);
    }
}

#[test]
fn batch_selection_keeps_immediately_due_saturated_deadlines() {
    let mut engine = Engine::recover(MemoryStore::default()).unwrap();
    create(&mut engine, stream(Retention::Workqueue));
    let mut config = consumer("D");
    config.max_deliver = 2;
    add(&mut engine, config);
    for _ in 0..2 {
        publish(&mut engine, "events.a", Bytes::new(), 0);
    }
    let event = engine
        .state()
        .prepare_delivery_batch("S", "D", u64::MAX, 4, 0)
        .unwrap()
        .unwrap();
    let Event::DeliverBatch { deliveries, .. } = &event else {
        panic!("expected batch")
    };
    assert_eq!(
        deliveries
            .iter()
            .map(|d| (d.sequence, d.attempt))
            .collect::<Vec<_>>(),
        vec![(1, 1), (1, 2), (2, 1), (2, 2)]
    );
    engine.commit(event).unwrap();
    engine.state().check_invariants().unwrap();
    let expected = engine.state().clone();
    engine.checkpoint().unwrap();
    assert_eq!(
        Engine::recover(engine.into_store()).unwrap().state(),
        &expected
    );
}
