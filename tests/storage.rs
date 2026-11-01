use bytes::Bytes;
use screech::{
    durable::{Engine, StreamConfig},
    storage::{SqliteStore, Store, StoreError},
};
#[test]
fn store_exclusive_lock_and_checksum_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("log.db");
    let mut store = SqliteStore::open(&path).unwrap();
    store.append("test", b"", b"original").unwrap();
    assert!(SqliteStore::open(&path).is_err());
    drop(store);
    // Structurally valid SQLite modification: page integrity alone cannot detect this.
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute(
        "UPDATE messages SET payload=?1 WHERE seq=1",
        [b"corrupt".as_slice()],
    )
    .unwrap();
    drop(conn);
    assert!(matches!(
        SqliteStore::open(&path),
        Err(StoreError::Corrupt(1))
    ));
}
#[test]
fn damaged_database_fails_without_reinitialization() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("log.db");
    let mut store = SqliteStore::open(&path).unwrap();
    store.append("x", &[], b"original").unwrap();
    drop(store);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[..16].fill(0x55);
    std::fs::write(&path, &bytes).unwrap();
    assert!(SqliteStore::open(&path).is_err());
    assert_eq!(std::fs::read(path).unwrap(), bytes);
}
#[test]
fn missing_committed_event_fails_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("log.db");
    let mut e = Engine::recover(SqliteStore::open(&path).unwrap()).unwrap();
    e.commit(
        e.state()
            .prepare_create(
                StreamConfig {
                    name: "S".into(),
                    subjects: vec!["s.>".into()],
                    ..Default::default()
                },
                0,
            )
            .unwrap(),
    )
    .unwrap();
    e.commit(
        e.state()
            .prepare_publish("S", "s.a", Bytes::new(), Bytes::from_static(b"data"), 0)
            .unwrap(),
    )
    .unwrap();
    drop(e);
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute("DELETE FROM messages WHERE seq=1", [])
        .unwrap();
    drop(conn);
    assert!(Engine::recover(SqliteStore::open(&path).unwrap()).is_err());
}
struct Failing;
impl Store for Failing {
    fn append(&mut self, _: &str, _: &[u8], _: &[u8]) -> Result<u64, StoreError> {
        Err(StoreError::Invalid)
    }
    fn get(&self, _: u64) -> Result<Option<screech::storage::Record>, StoreError> {
        Ok(None)
    }
    fn last_sequence(&self) -> Result<u64, StoreError> {
        Ok(0)
    }
    fn purge(&mut self) -> Result<(), StoreError> {
        Err(StoreError::Invalid)
    }
}
#[test]
fn failed_commit_has_no_visible_effect_and_poisoned_engine_rejects_writes() {
    let mut e = Engine::recover(Failing).unwrap();
    let event = e
        .state()
        .prepare_create(
            StreamConfig {
                name: "S".into(),
                subjects: vec!["s".into()],
                ..Default::default()
            },
            0,
        )
        .unwrap();
    assert!(e.commit(event.clone()).is_err());
    assert!(e.state().streams.is_empty());
    assert!(e.commit(event).is_err());
}

#[test]
fn vanished_or_empty_database_is_not_silently_reinitialized() {
    for truncate in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log.db");
        let mut store = SqliteStore::open(&path).unwrap();
        store.append("a", &[], b"data").unwrap();
        drop(store);
        if truncate {
            std::fs::write(&path, []).unwrap();
        } else {
            std::fs::remove_file(&path).unwrap();
        }
        assert!(matches!(
            SqliteStore::open(&path),
            Err(StoreError::Corrupt(0))
        ));
    }
}

proptest::proptest! {
    #[test]
    fn sqlite_wal_reuse_refines_memory_across_checkpoint_purge_and_reopen(
        ops in proptest::collection::vec((0u8..5, proptest::collection::vec(proptest::num::u8::ANY, 0..100), proptest::bool::ANY), 0..40)
    ) {
        use screech::storage::MemoryStore;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("log.db");
        let mut disk = SqliteStore::open(&path).unwrap();
        let mut memory = MemoryStore::default();
        for (operation, bytes, reopen) in ops {
            match operation {
                0..=2 => {
                    proptest::prop_assert_eq!(disk.append("a", b"headers", &bytes).unwrap(), memory.append("a", b"headers", &bytes).unwrap());
                }
                3 => {
                    let sequence = memory.last_sequence().unwrap();
                    disk.checkpoint(sequence, &bytes).unwrap(); memory.checkpoint(sequence, &bytes).unwrap();
                }
                _ => { disk.purge().unwrap(); memory.purge().unwrap(); }
            }
            if reopen { drop(disk); disk = SqliteStore::open(&path).unwrap(); }
            proptest::prop_assert_eq!(disk.last_sequence().unwrap(), memory.last_sequence().unwrap());
            proptest::prop_assert_eq!(disk.load_checkpoint().unwrap(), memory.load_checkpoint().unwrap());
            for seq in 1..=memory.last_sequence().unwrap() { proptest::prop_assert_eq!(disk.get(seq).unwrap(), memory.get(seq).unwrap()); }
        }
    }
}

#[test]
fn reused_wal_shrinks_to_limit_and_preserves_snapshot_and_highwater() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("log.db");
    let mut store = SqliteStore::open(&path).unwrap();
    store.append("a", b"", b"first").unwrap();
    let snapshot = vec![0x3a; 9 * 1024 * 1024];
    store.checkpoint(1, &snapshot).unwrap();
    assert_eq!(store.append("a", b"", b"second").unwrap(), 2);
    assert!(
        std::fs::metadata(directory.path().join("log.db-wal"))
            .unwrap()
            .len()
            <= 8 * 1024 * 1024
    );
    drop(store);
    let store = SqliteStore::open(path).unwrap();
    assert_eq!(store.last_sequence().unwrap(), 2);
    assert_eq!(
        store.load_checkpoint().unwrap().unwrap(),
        (1, Bytes::from(snapshot))
    );
    assert!(store.get(1).unwrap().is_none());
    assert_eq!(store.get(2).unwrap().unwrap().payload.as_ref(), b"second");
}

proptest::proptest! {
    #[test]
    fn atomic_appends_refine_memory_through_checkpoint_and_restart(
        ops in proptest::collection::vec((0u8..4, proptest::collection::vec(proptest::collection::vec(proptest::num::u8::ANY,0..80),1..8),proptest::bool::ANY),0..30)
    ) {
        use screech::storage::{Append,MemoryStore};
        let dir=tempfile::tempdir().unwrap();let path=dir.path().join("group.db");
        let mut disk=SqliteStore::open(&path).unwrap();let mut memory=MemoryStore::default();
        for (op,payloads,reopen) in ops {
            if op<2 {
                let records:Vec<_>=payloads.into_iter().map(|p|Append{subject:"a".into(),headers:Bytes::from_static(b"headers"),payload:p.into()}).collect();
                proptest::prop_assert_eq!(disk.append_batch(&records).unwrap(),memory.append_batch(&records).unwrap());
            } else if op==2 {
                let last=memory.last_sequence().unwrap();disk.checkpoint(last,b"snapshot").unwrap();memory.checkpoint(last,b"snapshot").unwrap();
            } else { disk.purge().unwrap();memory.purge().unwrap(); }
            if reopen {drop(disk);disk=SqliteStore::open(&path).unwrap();}
            proptest::prop_assert_eq!(disk.last_sequence().unwrap(),memory.last_sequence().unwrap());
            proptest::prop_assert_eq!(disk.load_checkpoint().unwrap(),memory.load_checkpoint().unwrap());
            for seq in 1..=memory.last_sequence().unwrap(){proptest::prop_assert_eq!(disk.get(seq).unwrap(),memory.get(seq).unwrap());}
        }
    }
}
