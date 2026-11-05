use bytes::Bytes;
use screech::{
    durable::{Engine, StreamConfig},
    storage::{
        Append, CheckpointImage, CheckpointJob, Content, ContentKey, Record, Replication,
        SqliteStore, Store, StoreError,
    },
    storage_executor::ExecutorStore,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

#[derive(Default)]
struct Probe {
    calls: Mutex<Vec<(&'static str, std::thread::ThreadId)>>,
    fail_read: AtomicBool,
    fail_checkpoint: AtomicBool,
    fail_replication: AtomicBool,
}
impl Probe {
    fn note(&self, name: &'static str) {
        self.calls
            .lock()
            .unwrap()
            .push((name, std::thread::current().id()));
    }
}
struct Observed {
    inner: SqliteStore,
    probe: Arc<Probe>,
    replication: Option<Arc<dyn Replication>>,
}
impl Store for Observed {
    fn replication(&self) -> Option<Arc<dyn Replication>> {
        self.replication.clone()
    }
    fn replicas(&self) -> usize {
        if self.replication.is_some() {
            3
        } else {
            1
        }
    }
    fn durable_appends(&self) -> bool {
        true
    }
    fn supports_batch(&self) -> bool {
        true
    }
    fn supports_checkpoint(&self) -> bool {
        true
    }
    fn external_content(&self) -> bool {
        true
    }
    fn append(&mut self, s: &str, h: &[u8], p: &[u8]) -> Result<u64, StoreError> {
        self.probe.note("append");
        self.inner.append(s, h, p)
    }
    fn append_with_content(&mut self, r: &[Append], c: &[Content]) -> Result<u64, StoreError> {
        self.probe.note("append");
        self.inner.append_with_content(r, c)
    }
    fn get(&self, seq: u64) -> Result<Option<Record>, StoreError> {
        self.probe.note("get");
        self.inner.get(seq)
    }
    fn get_content(&self, key: ContentKey) -> Result<Option<Content>, StoreError> {
        self.probe.note("body");
        if self.probe.fail_read.load(Ordering::SeqCst) {
            return Err(StoreError::Corrupt(key.sequence));
        }
        self.inner.get_content(key)
    }
    fn last_sequence(&self) -> Result<u64, StoreError> {
        self.probe.note("last");
        self.inner.last_sequence()
    }
    fn purge(&mut self) -> Result<(), StoreError> {
        self.inner.purge()
    }
    fn load_checkpoint(&self) -> Result<Option<(u64, Bytes)>, StoreError> {
        self.probe.note("load");
        self.inner.load_checkpoint()
    }
    fn checkpoint_job(&mut self, seq: u64, job: CheckpointJob) -> Result<(), StoreError> {
        self.probe.note("serialize");
        let image = job()?;
        if self.probe.fail_checkpoint.load(Ordering::SeqCst) {
            return Err(StoreError::Invalid);
        }
        self.probe.note("install");
        self.inner.checkpoint_with_content(
            seq,
            &image.data,
            &mut image.live.into_iter(),
            &image.imports,
        )
    }
}
fn setup(path: &std::path::Path, probe: Arc<Probe>) -> Engine<ExecutorStore> {
    let store = ExecutorStore::start(Observed {
        inner: SqliteStore::open(path).unwrap(),
        probe,
        replication: None,
    })
    .unwrap();
    let mut engine = Engine::recover_single_voter(store).unwrap();
    engine
        .commit(
            engine
                .state()
                .prepare_create(
                    StreamConfig {
                        name: "S".into(),
                        subjects: vec!["a".into()],
                        ..Default::default()
                    },
                    0,
                )
                .unwrap(),
        )
        .unwrap();
    engine
        .commit(
            engine
                .state()
                .prepare_publish("S", "a", Bytes::new(), "body".into(), 1)
                .unwrap(),
        )
        .unwrap();
    engine
}
#[test]
fn reads_serialization_and_installation_share_one_storage_owner() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let probe = Arc::new(Probe::default());
    let mut engine = setup(&path, probe.clone());
    let m = &engine.state().streams["S"].messages[&1];
    assert_eq!(engine.read_message(m).unwrap().payload, "body");
    engine.checkpoint().unwrap();
    let calls = probe.calls.lock().unwrap();
    let owner = calls[0].1;
    assert_ne!(owner, std::thread::current().id());
    assert!(calls.iter().all(|(_, t)| *t == owner));
    assert!(calls.iter().any(|(n, _)| *n == "serialize"));
    assert!(calls.iter().any(|(n, _)| *n == "install"));
    drop(calls);
    drop(engine);
    // Drop must release the SQLite lock before returning, including after an
    // executor task has just returned its completion.
    let recovered = Engine::recover_single_voter(SqliteStore::open(&path).unwrap()).unwrap();
    assert_eq!(
        recovered
            .read_message(&recovered.state().streams["S"].messages[&1])
            .unwrap()
            .payload,
        "body"
    );
}
#[test]
fn failed_read_or_checkpoint_quarantines_and_preserves_durable_state() {
    for fail_read in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let probe = Arc::new(Probe::default());
        let mut engine = setup(&path, probe.clone());
        if fail_read {
            probe.fail_read.store(true, Ordering::SeqCst);
            assert!(engine
                .read_message(&engine.state().streams["S"].messages[&1])
                .is_err());
        } else {
            probe.fail_checkpoint.store(true, Ordering::SeqCst);
            assert!(engine.checkpoint().is_err());
        }
        assert!(engine.is_poisoned());
        assert!(engine.begin_group().is_err());
        assert_eq!(engine.voter_progress().unwrap().checkpoint, 0);
        drop(engine);
        let recovered = Engine::recover_single_voter(SqliteStore::open(&path).unwrap()).unwrap();
        assert_eq!(recovered.state().streams["S"].last_sequence, 1);
    }
}
#[test]
fn executor_loss_is_an_error_and_does_not_become_success() {
    for replicated in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let probe = Arc::new(Probe::default());
        let mut store = ExecutorStore::start(Observed {
            inner: SqliteStore::open(dir.path().join("db")).unwrap(),
            probe: probe.clone(),
            replication: replicated.then_some(probe),
        })
        .unwrap();
        assert!(store
            .checkpoint_job(
                0,
                Box::new(|| -> Result<CheckpointImage, StoreError> {
                    panic!("injected owner failure")
                })
            )
            .is_err());
        assert!(store.last_sequence().is_err());
        assert!(store.read_barrier().is_err());
        assert!(store.append_batch(&[]).is_err());
        assert!(store.checkpoint_needed().is_err());
    }
}

// A dispatch-only test double: no successful nonempty append is issued through
// it. Real replicated durability and epochs are checked by the cluster suites.
impl Replication for Probe {
    fn read_barrier(&self) -> Result<(), StoreError> {
        self.note("dispatch-barrier");
        if self.fail_replication.load(Ordering::Acquire) {
            Err(StoreError::Invalid)
        } else {
            Ok(())
        }
    }
    fn append_with_content(&self, records: &[Append], _: &[Content]) -> Result<u64, StoreError> {
        self.note("dispatch-append");
        if self.fail_replication.load(Ordering::Acquire) {
            Err(StoreError::Invalid)
        } else {
            assert!(records.is_empty());
            Ok(0)
        }
    }
    fn checkpoint_needed(&self) -> Result<bool, StoreError> {
        self.note("dispatch-pressure");
        Ok(false)
    }
}

#[test]
fn replication_dispatch_avoids_the_proxy_but_reads_and_checkpoints_keep_their_owner() {
    let dir = tempfile::tempdir().unwrap();
    let probe = Arc::new(Probe::default());
    let mut store = ExecutorStore::start(Observed {
        inner: SqliteStore::open(dir.path().join("db")).unwrap(),
        probe: probe.clone(),
        replication: Some(probe.clone()),
    })
    .unwrap();
    store.read_barrier().unwrap();
    assert_eq!(store.append_with_content(&[], &[]).unwrap(), 0);
    assert_eq!(store.append_batch(&[]).unwrap(), 0);
    assert!(!store.checkpoint_needed().unwrap());
    assert!(store.get(1).unwrap().is_none());
    assert_eq!(store.last_sequence().unwrap(), 0);
    assert!(store.load_checkpoint().unwrap().is_none());
    let captured = probe.clone();
    store
        .checkpoint_job(
            0,
            Box::new(move || {
                captured.note("image-job");
                Ok(CheckpointImage {
                    data: b"root".to_vec(),
                    live: vec![],
                    imports: vec![],
                })
            }),
        )
        .unwrap();
    let calls = probe.calls.lock().unwrap();
    let caller = std::thread::current().id();
    let owner = calls.iter().find(|(name, _)| *name == "get").unwrap().1;
    assert_ne!(owner, caller);
    for (name, thread) in calls.iter() {
        assert_eq!(
            *thread,
            if name.starts_with("dispatch-") {
                caller
            } else {
                owner
            },
            "{name} ran on the wrong owner"
        );
    }
}

#[test]
fn failed_replication_dispatch_cannot_advance_or_expose_the_engine_projection() {
    let dir = tempfile::tempdir().unwrap();
    let probe = Arc::new(Probe::default());
    probe.fail_replication.store(true, Ordering::Release);
    let store = ExecutorStore::start(Observed {
        inner: SqliteStore::open(dir.path().join("db")).unwrap(),
        probe: probe.clone(),
        replication: Some(probe),
    })
    .unwrap();
    let mut engine = Engine::recover_single_voter(store).unwrap();
    let event = engine
        .state()
        .prepare_create(
            StreamConfig {
                name: "S".into(),
                subjects: vec!["a".into()],
                ..Default::default()
            },
            0,
        )
        .unwrap();
    assert!(engine.commit(event).is_err());
    assert!(engine.is_poisoned());
    assert!(engine.state().streams.is_empty());
    let position = engine.voter_progress().unwrap();
    assert_eq!(
        (position.durable, position.committed, position.applied),
        (0, 0, 0)
    );
    assert!(engine.begin_group().is_err());
}

#[tokio::test]
async fn detached_async_dispatch_fences_owner_failure_before_and_after_quorum() {
    use screech::storage::ReadBarrierFuture;
    struct Paused(tokio::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>);
    impl Replication for Paused {
        fn read_barrier(&self) -> Result<(), StoreError> {
            Ok(())
        }
        fn read_barrier_async(&self) -> Option<ReadBarrierFuture> {
            let receive = self.0.try_lock().unwrap().take().unwrap();
            Some(Box::pin(async move {
                receive.await.map_err(|_| StoreError::Invalid)
            }))
        }
        fn append_with_content(&self, _: &[Append], _: &[Content]) -> Result<u64, StoreError> {
            Ok(0)
        }
        fn checkpoint_needed(&self) -> Result<bool, StoreError> {
            Ok(false)
        }
    }
    for failure_during_proof in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (send, receive) = tokio::sync::oneshot::channel();
        let mut store = ExecutorStore::start(Observed {
            inner: SqliteStore::open(dir.path().join("db")).unwrap(),
            probe: Arc::new(Probe::default()),
            replication: Some(Arc::new(Paused(tokio::sync::Mutex::new(Some(receive))))),
        })
        .unwrap();
        let dispatcher = store.replication().unwrap();
        let mut proof = dispatcher.read_barrier_async().unwrap();
        if failure_during_proof {
            tokio::select! {
                result = &mut proof => panic!("proof completed before quorum: {result:?}"),
                _ = tokio::task::yield_now() => (),
            }
        }
        assert!(store
            .checkpoint_job(0, Box::new(|| panic!("owner failure")))
            .is_err());
        send.send(()).unwrap();
        assert!(proof.await.is_err());
        assert!(dispatcher.read_barrier().is_err());
        assert!(dispatcher.append_with_content(&[], &[]).is_err());
        assert!(dispatcher.checkpoint_needed().is_err());
    }
}
