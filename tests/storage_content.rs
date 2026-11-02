use bytes::Bytes;
use screech::storage::{Append, Content, ContentKey, SqliteStore, Store};
fn record() -> Append {
    Append {
        subject: "event".into(),
        headers: Bytes::new(),
        payload: Bytes::new(),
    }
}
fn content(generation: u64, sequence: u64) -> Content {
    Content {
        key: ContentKey {
            generation,
            sequence,
        },
        subject: "events.a".into(),
        headers: Bytes::from_static(b"NATS/1.0\r\n\r\n"),
        payload: Bytes::from_static(b"body\0bytes"),
    }
}
#[test]
fn content_and_log_commit_atomically_and_survive_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut store = SqliteStore::open(&path).unwrap();
    let first = content(1, 1);
    let second = content(2, 1);
    assert_eq!(
        store
            .append_with_content(&[record()], std::slice::from_ref(&first))
            .unwrap(),
        1
    );
    // A duplicate content key after a fresh insert must roll the entire batch back.
    assert!(store
        .append_with_content(&[record(), record()], &[second.clone(), first.clone()])
        .is_err());
    assert_eq!(store.last_sequence().unwrap(), 1);
    assert!(store.get_content(second.key).unwrap().is_none());
    assert_eq!(
        store
            .append_with_content(&[record()], std::slice::from_ref(&second))
            .unwrap(),
        2
    );
    store
        .checkpoint_with_content(
            2,
            b"metadata",
            &mut [first.key, second.key].into_iter(),
            &[],
        )
        .unwrap();
    assert!(store.get(1).unwrap().is_none());
    drop(store);
    let mut store = SqliteStore::open(&path).unwrap();
    assert_eq!(store.get_content(first.key).unwrap(), Some(first.clone()));
    store
        .checkpoint_with_content(2, b"new metadata", &mut [second.key].into_iter(), &[])
        .unwrap();
    assert!(store.get_content(first.key).unwrap().is_none());
    assert_eq!(store.get_content(second.key).unwrap(), Some(second));
    assert!(store
        .checkpoint_with_content(2, b"bad", &mut [first.key].into_iter(), &[])
        .is_err());
    assert_eq!(
        store.load_checkpoint().unwrap().unwrap().1,
        b"new metadata".as_slice()
    );
}
#[test]
fn corrupt_content_is_rejected_on_read() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut store = SqliteStore::open(&path).unwrap();
    let body = content(1, 1);
    store.append_with_content(&[record()], &[body]).unwrap();
    drop(store);
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute(
        "UPDATE retained_content SET payload=?1",
        [b"damage".as_slice()],
    )
    .unwrap();
    drop(db);
    let store = SqliteStore::open(path).unwrap();
    assert!(store
        .get_content(ContentKey {
            generation: 1,
            sequence: 1
        })
        .is_err());
}

#[test]
fn content_batch_preserves_order_and_fails_on_missing_or_corrupt_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let mut store = SqliteStore::open(&path).unwrap();
    let contents: Vec<_> = (1..=6)
        .map(|seq| content(1, seq))
        .chain([content(2, 1)])
        .collect();
    store.append_with_content(&[record()], &contents).unwrap();
    for keys in [
        vec![],
        vec![contents[0].key],
        contents.iter().take(6).map(|c| c.key).collect(),
        vec![
            contents[4].key,
            contents[0].key,
            contents[6].key,
            contents[0].key,
        ],
    ] {
        let expected: Vec<_> = keys
            .iter()
            .map(|k| store.get_content(*k).unwrap().unwrap())
            .collect();
        assert_eq!(store.get_contents(&keys).unwrap(), expected);
    }
    assert!(store
        .get_contents(&[
            ContentKey {
                generation: 1,
                sequence: 6
            },
            ContentKey {
                generation: 1,
                sequence: 7
            }
        ])
        .is_err());
    drop(store);
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute(
        "DELETE FROM retained_content WHERE generation=1 AND seq=3",
        [],
    )
    .unwrap();
    drop(db);
    let store = SqliteStore::open(&path).unwrap();
    let keys: Vec<_> = (1..=6)
        .map(|sequence| ContentKey {
            generation: 1,
            sequence,
        })
        .collect();
    assert!(store.get_contents(&keys).is_err());
    drop(store);
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute("UPDATE retained_content SET payload=zeroblob(length(payload)) WHERE generation=1 AND seq=2",[]).unwrap();
    drop(db);
    let store = SqliteStore::open(&path).unwrap();
    assert!(store.get_contents(&keys[..2]).is_err());
}
