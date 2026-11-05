use super::*;
use crate::storage::MemoryStore;

fn catalog() -> Engine<MemoryStore> {
    let mut engine = Engine::recover(MemoryStore::default()).unwrap();
    for n in 0..64 {
        let event = engine
            .state()
            .prepare_create(
                StreamConfig {
                    name: format!("S{n}"),
                    subjects: vec![format!("a.{n}")],
                    ..Default::default()
                },
                n,
            )
            .unwrap();
        engine.commit(event).unwrap();
    }
    let event = engine
        .state()
        .prepare_publish(
            "S0",
            "a.0",
            Bytes::from_static(b"NATS/1.0\r\nNats-Msg-Id: stable\r\n\r\n"),
            Bytes::from_static(b"payload"),
            100,
        )
        .unwrap();
    engine.commit(event).unwrap();
    let event = engine
        .state()
        .prepare_consumer(
            "S0",
            ConsumerConfig {
                name: "D".into(),
                durable_name: Some("D".into()),
                ..Default::default()
            },
            101,
        )
        .unwrap();
    engine.commit(event).unwrap();
    engine
}

#[test]
fn planned_writes_copy_only_changed_streams_and_drop_without_mutating_committed_state() {
    let engine = catalog();
    let before = postcard::to_allocvec(engine.state()).unwrap();
    let mut plan = engine.begin_group().unwrap();
    for (name, stream) in &engine.state().streams {
        assert!(std::sync::Arc::ptr_eq(
            &stream.0,
            &plan.state().streams[name].0
        ));
    }
    let event = plan
        .state()
        .prepare_publish(
            "S0",
            "a.0",
            Bytes::new(),
            Bytes::from_static(b"uncommitted"),
            102,
        )
        .unwrap();
    plan.commit(event).unwrap();
    assert_eq!(plan.state().streams["S0"].messages.len(), 2);
    assert_eq!(engine.state().streams["S0"].messages.len(), 1);
    assert!(!std::sync::Arc::ptr_eq(
        &engine.state().streams["S0"].0,
        &plan.state().streams["S0"].0
    ));
    for (name, stream) in &engine.state().streams {
        if name != "S0" {
            assert!(std::sync::Arc::ptr_eq(
                &stream.0,
                &plan.state().streams[name].0
            ));
        }
    }
    drop(plan);
    assert_eq!(postcard::to_allocvec(engine.state()).unwrap(), before);
}

#[test]
fn shared_catalog_keeps_the_previous_binary_and_json_checkpoint_layout() {
    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
    struct PreviousState {
        streams: BTreeMap<String, Stream>,
        generation: u64,
        time: u64,
    }
    let engine = catalog();
    let state = engine.state();
    let previous = PreviousState {
        streams: state
            .streams
            .iter()
            .map(|(name, stream)| (name.clone(), (**stream).clone()))
            .collect(),
        generation: state.generation,
        time: state.time,
    };
    let old = postcard::to_allocvec(&previous).unwrap();
    let new = postcard::to_allocvec(state).unwrap();
    assert_eq!(old, new);
    assert_eq!(postcard::from_bytes::<State>(&old).unwrap(), *state);
    assert_eq!(
        postcard::from_bytes::<PreviousState>(&new).unwrap(),
        previous
    );
    let old = serde_json::to_vec(&previous).unwrap();
    let new = serde_json::to_vec(state).unwrap();
    assert_eq!(old, new);
    assert_eq!(serde_json::from_slice::<State>(&old).unwrap(), *state);
    assert_eq!(
        serde_json::from_slice::<PreviousState>(&new).unwrap(),
        previous
    );
}
