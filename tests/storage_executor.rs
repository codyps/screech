use bytes::Bytes;
use screech::{
    durable::{Engine, StreamConfig},
    storage::{
        Append, CheckpointImage, CheckpointJob, Content, ContentKey, Record, SqliteStore, Store,
        StoreError,
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
}
impl Store for Observed {
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
    let dir = tempfile::tempdir().unwrap();
    let mut store =
        ExecutorStore::start(SqliteStore::open(dir.path().join("db")).unwrap()).unwrap();
    assert!(store
        .checkpoint_job(
            0,
            Box::new(|| -> Result<CheckpointImage, StoreError> {
                panic!("injected owner failure")
            })
        )
        .is_err());
    assert!(store.last_sequence().is_err());
}
