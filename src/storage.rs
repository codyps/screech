//! Storage contracts. `commit` returning success means durable, not merely buffered.
//! Operations on one store are serialized by its owner, independently of transport.
use bytes::Bytes;
use rusqlite::{params, Connection, OptionalExtension};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("SQLite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("corrupt stored record at sequence {0}")]
    Corrupt(u64),
    #[error("invalid sequence or store operation")]
    Invalid,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub sequence: u64,
    pub subject: String,
    pub headers: Bytes,
    pub payload: Bytes,
}
/// Input records for one atomic durable transaction. Success covers every record.
#[derive(Clone, Debug)]
pub struct Append {
    pub subject: String,
    pub headers: Bytes,
    pub payload: Bytes,
}
/// Stable content identity, independent of event-log compaction.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct ContentKey {
    pub generation: u64,
    pub sequence: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Content {
    pub key: ContentKey,
    pub subject: String,
    pub headers: Bytes,
    pub payload: Bytes,
}
fn content_checksum(content: &Content) -> u32 {
    let mut h = crc32fast::Hasher::new();
    h.update(&content.key.generation.to_le_bytes());
    h.update(
        &checksum(
            content.key.sequence,
            &content.subject,
            &content.headers,
            &content.payload,
        )
        .to_le_bytes(),
    );
    h.finalize()
}
/// An atomic durable append log. Metadata and records share one transaction domain.
pub trait Store: Send {
    fn append(&mut self, subject: &str, headers: &[u8], payload: &[u8]) -> Result<u64, StoreError>;
    /// Implementations must append all records or none. A failure may have an
    /// unknown outcome; callers must recover before issuing further durable success.
    fn append_batch(&mut self, records: &[Append]) -> Result<u64, StoreError> {
        if let [record] = records {
            self.append(&record.subject, &record.headers, &record.payload)
        } else {
            Err(StoreError::Invalid)
        }
    }
    fn supports_batch(&self) -> bool {
        false
    }
    fn external_content(&self) -> bool {
        false
    }
    /// Content and all transitions become durable in the same transaction.
    fn append_with_content(
        &mut self,
        records: &[Append],
        content: &[Content],
    ) -> Result<u64, StoreError> {
        if !content.is_empty() {
            return Err(StoreError::Invalid);
        }
        if let [record] = records {
            self.append(&record.subject, &record.headers, &record.payload)
        } else {
            self.append_batch(records)
        }
    }
    fn get_content(&self, _key: ContentKey) -> Result<Option<Content>, StoreError> {
        Err(StoreError::Invalid)
    }
    /// Validate a reference without loading its body when the backend supports it.
    fn validate_content(
        &self,
        key: ContentKey,
        subject: &str,
        headers: u64,
        payload: u64,
    ) -> Result<(), StoreError> {
        match self.get_content(key)? {
            Some(c)
                if c.subject == subject
                    && c.headers.len() as u64 == headers
                    && c.payload.len() as u64 == payload =>
            {
                Ok(())
            }
            _ => Err(StoreError::Corrupt(key.sequence)),
        }
    }
    fn checkpoint_with_content(
        &mut self,
        sequence: u64,
        state: &[u8],
        live: &mut dyn Iterator<Item = ContentKey>,
        imports: &[Content],
    ) -> Result<(), StoreError> {
        if live.next().is_some() || !imports.is_empty() {
            return Err(StoreError::Invalid);
        }
        self.checkpoint(sequence, state)
    }
    fn get(&self, sequence: u64) -> Result<Option<Record>, StoreError>;
    fn last_sequence(&self) -> Result<u64, StoreError>;
    fn purge(&mut self) -> Result<(), StoreError>;
    fn supports_checkpoint(&self) -> bool {
        false
    }
    fn load_checkpoint(&self) -> Result<Option<(u64, Bytes)>, StoreError> {
        Ok(None)
    }
    fn checkpoint(&mut self, _sequence: u64, _state: &[u8]) -> Result<(), StoreError> {
        Err(StoreError::Invalid)
    }
}
#[derive(Default)]
pub struct MemoryStore {
    last: u64,
    records: BTreeMap<u64, Record>,
    snapshot: Option<(u64, Bytes)>,
}
impl Store for MemoryStore {
    fn supports_batch(&self) -> bool {
        true
    }
    fn append_batch(&mut self, records: &[Append]) -> Result<u64, StoreError> {
        if records.is_empty() {
            return Err(StoreError::Invalid);
        }
        self.last
            .checked_add(records.len() as u64)
            .ok_or(StoreError::Invalid)?;
        for record in records {
            self.append(&record.subject, &record.headers, &record.payload)?;
        }
        Ok(self.last)
    }
    fn supports_checkpoint(&self) -> bool {
        true
    }
    fn load_checkpoint(&self) -> Result<Option<(u64, Bytes)>, StoreError> {
        Ok(self.snapshot.clone())
    }
    fn checkpoint(&mut self, sequence: u64, state: &[u8]) -> Result<(), StoreError> {
        if sequence != self.last {
            return Err(StoreError::Invalid);
        }
        self.snapshot = Some((sequence, Bytes::copy_from_slice(state)));
        self.records.retain(|seq, _| *seq > sequence);
        Ok(())
    }

    fn append(&mut self, subject: &str, headers: &[u8], payload: &[u8]) -> Result<u64, StoreError> {
        self.last = self.last.checked_add(1).ok_or(StoreError::Invalid)?;
        self.records.insert(
            self.last,
            Record {
                sequence: self.last,
                subject: subject.into(),
                headers: Bytes::copy_from_slice(headers),
                payload: Bytes::copy_from_slice(payload),
            },
        );
        Ok(self.last)
    }
    fn get(&self, sequence: u64) -> Result<Option<Record>, StoreError> {
        Ok(self.records.get(&sequence).cloned())
    }
    fn last_sequence(&self) -> Result<u64, StoreError> {
        Ok(self.last)
    }
    fn purge(&mut self) -> Result<(), StoreError> {
        self.records.clear();
        Ok(())
    }
}
pub struct SqliteStore {
    connection: Connection,
    _lock: File,
    last: u64,
}
fn checksum(sequence: u64, subject: &str, headers: &[u8], payload: &[u8]) -> u32 {
    let mut h = crc32fast::Hasher::new();
    h.update(&sequence.to_le_bytes());
    for b in [subject.as_bytes(), headers, payload] {
        h.update(&(b.len() as u64).to_le_bytes());
        h.update(b);
    }
    h.finalize()
}
impl SqliteStore {
    /// Use a pre-existing data directory. Lock prevents two servers owning one log.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref();
        let mut lock_name = path.as_os_str().to_owned();
        lock_name.push(".lock");
        let mut lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_name)?;
        fs2::FileExt::try_lock_exclusive(&lock)?;
        let mut marker = String::new();
        lock.read_to_string(&mut marker)?;
        if !marker.is_empty() && marker != "screech sqlite v1\n" {
            return Err(StoreError::Invalid);
        }
        match std::fs::metadata(path) {
            Ok(meta) if meta.len() == 0 => return Err(StoreError::Corrupt(0)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && !marker.is_empty() => {
                return Err(StoreError::Corrupt(0))
            }
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e.into()),
            _ => (),
        }
        let conn = Connection::open(path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.pragma_update(None, "foreign_keys", true)?;
        conn.pragma_update(None, "cache_size", -8192)?;
        conn.pragma_update(None, "temp_store", "FILE")?;
        conn.pragma_update(None, "mmap_size", 0)?;
        // The store already has one exclusive owner. Retain SQLite's locks and WAL
        // allocation between transactions; FULL still synchronizes every commit.
        conn.pragma_update(None, "locking_mode", "EXCLUSIVE")?;
        conn.pragma_update(None, "wal_autocheckpoint", 4096)?;
        conn.pragma_update(None, "journal_size_limit", 8 * 1024 * 1024)?;
        let mode: String = conn.pragma_query_value(None, "journal_mode", |r| r.get(0))?;
        let sync: i64 = conn.pragma_query_value(None, "synchronous", |r| r.get(0))?;
        if mode != "wal" || sync != 2 {
            return Err(StoreError::Invalid);
        }
        conn.execute_batch("CREATE TABLE IF NOT EXISTS meta (id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL, last_seq INTEGER NOT NULL CHECK(last_seq>=0)); INSERT OR IGNORE INTO meta VALUES(1,1,0); CREATE TABLE IF NOT EXISTS snapshot(id INTEGER PRIMARY KEY CHECK(id=1), seq INTEGER NOT NULL, data BLOB NOT NULL, checksum INTEGER NOT NULL); CREATE TABLE IF NOT EXISTS messages(seq INTEGER PRIMARY KEY CHECK(seq>0), subject TEXT NOT NULL, headers BLOB NOT NULL, payload BLOB NOT NULL, checksum INTEGER NOT NULL);")?;
        conn.execute_batch("CREATE TABLE IF NOT EXISTS retained_content(generation INTEGER NOT NULL CHECK(generation>0),seq INTEGER NOT NULL CHECK(seq>0),subject TEXT NOT NULL,headers BLOB NOT NULL,payload BLOB NOT NULL,checksum INTEGER NOT NULL,PRIMARY KEY(generation,seq)) WITHOUT ROWID;")?;
        let version: i64 =
            conn.query_row("SELECT version FROM meta WHERE id=1", [], |r| r.get(0))?;
        if version != 1 {
            return Err(StoreError::Invalid);
        }
        let check: String = conn.query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
        if check != "ok" {
            return Err(StoreError::Corrupt(0));
        }
        lock.seek(SeekFrom::Start(0))?;
        lock.write_all(b"screech sqlite v1\n")?;
        lock.sync_all()?;
        // SQLite FULL syncs WAL transactions; sync new directory entries before serving.
        File::open(
            path.parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new(".")),
        )?
        .sync_all()?;
        let last = u64::try_from(conn.query_row(
            "SELECT last_seq FROM meta WHERE id=1",
            [],
            |r| r.get::<_, i64>(0),
        )?)
        .map_err(|_| StoreError::Invalid)?;
        let store = Self {
            connection: conn,
            _lock: lock,
            last,
        };
        let last = store.last_sequence()?;
        let mut stmt = store
            .connection
            .prepare("SELECT seq FROM messages ORDER BY seq")?;
        let sequences = stmt.query_map([], |r| r.get::<_, i64>(0))?;
        for seq in sequences {
            let seq = u64::try_from(seq?).map_err(|_| StoreError::Invalid)?;
            if seq == 0 || seq > last {
                return Err(StoreError::Corrupt(seq));
            }
            store.get(seq)?;
        }
        drop(stmt);
        Ok(store)
    }
}
impl Store for SqliteStore {
    fn external_content(&self) -> bool {
        true
    }
    fn append_with_content(
        &mut self,
        records: &[Append],
        content: &[Content],
    ) -> Result<u64, StoreError> {
        if content.is_empty() {
            return if let [r] = records {
                self.append(&r.subject, &r.headers, &r.payload)
            } else {
                self.append_batch(records)
            };
        }
        if records.is_empty() {
            return Err(StoreError::Invalid);
        }
        let last = self.last;
        let end = last
            .checked_add(records.len() as u64)
            .filter(|n| *n <= i64::MAX as u64)
            .ok_or(StoreError::Invalid)?;
        let tx = self.connection.transaction()?;
        {
            let mut insert =
                tx.prepare_cached("INSERT INTO retained_content VALUES(?1,?2,?3,?4,?5,?6)")?;
            for c in content {
                insert.execute(params![
                    i64::try_from(c.key.generation).map_err(|_| StoreError::Invalid)?,
                    i64::try_from(c.key.sequence).map_err(|_| StoreError::Invalid)?,
                    c.subject,
                    c.headers.as_ref(),
                    c.payload.as_ref(),
                    content_checksum(c)
                ])?;
            }
            let mut insert = tx.prepare_cached("INSERT INTO messages VALUES(?1,?2,?3,?4,?5)")?;
            for (offset, r) in records.iter().enumerate() {
                let seq = last + offset as u64 + 1;
                insert.execute(params![
                    seq as i64,
                    r.subject,
                    r.headers.as_ref(),
                    r.payload.as_ref(),
                    checksum(seq, &r.subject, &r.headers, &r.payload)
                ])?;
            }
        }
        if tx
            .prepare_cached("UPDATE meta SET last_seq=?1 WHERE id=1 AND last_seq=?2")?
            .execute([end as i64, last as i64])?
            != 1
        {
            return Err(StoreError::Invalid);
        }
        if records.len() > 1 {
            crate::fault::point("before_group_store_commit");
        }
        crate::fault::point("before_commit");
        tx.commit()?;
        crate::fault::point("after_commit");
        if records.len() > 1 {
            crate::fault::point("after_group_store_commit");
        }
        self.last = end;
        Ok(end)
    }
    fn get_content(&self, key: ContentKey) -> Result<Option<Content>, StoreError> {
        let generation = i64::try_from(key.generation).map_err(|_| StoreError::Invalid)?;
        let seq = i64::try_from(key.sequence).map_err(|_| StoreError::Invalid)?;
        let row = self.connection.query_row("SELECT subject,headers,payload,checksum FROM retained_content WHERE generation=?1 AND seq=?2",[generation,seq], |r| Ok((r.get::<_,String>(0)?,r.get::<_,Vec<u8>>(1)?,r.get::<_,Vec<u8>>(2)?,r.get::<_,u32>(3)?))).optional()?;
        row.map(|(subject, headers, payload, crc)| {
            let content = Content {
                key,
                subject,
                headers: headers.into(),
                payload: payload.into(),
            };
            if content_checksum(&content) != crc {
                return Err(StoreError::Corrupt(key.sequence));
            }
            Ok(content)
        })
        .transpose()
    }
    fn validate_content(
        &self,
        key: ContentKey,
        subject: &str,
        headers: u64,
        payload: u64,
    ) -> Result<(), StoreError> {
        let row = self.connection.query_row("SELECT subject,length(headers),length(payload) FROM retained_content WHERE generation=?1 AND seq=?2", params![i64::try_from(key.generation).map_err(|_| StoreError::Invalid)?,i64::try_from(key.sequence).map_err(|_| StoreError::Invalid)?], |r| Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?,r.get::<_,i64>(2)?))).optional()?;
        if row.as_ref().is_some_and(|(s, h, p)| {
            s == subject && *h >= 0 && *p >= 0 && *h as u64 == headers && *p as u64 == payload
        }) {
            Ok(())
        } else {
            Err(StoreError::Corrupt(key.sequence))
        }
    }
    fn checkpoint_with_content(
        &mut self,
        sequence: u64,
        state: &[u8],
        live: &mut dyn Iterator<Item = ContentKey>,
        imports: &[Content],
    ) -> Result<(), StoreError> {
        if sequence != self.last_sequence()? {
            return Err(StoreError::Invalid);
        }
        let tx = self.connection.transaction()?;
        tx.execute_batch("CREATE TEMP TABLE IF NOT EXISTS live_content(generation INTEGER,seq INTEGER,PRIMARY KEY(generation,seq)) WITHOUT ROWID; DELETE FROM live_content;")?;
        {
            let mut insert =
                tx.prepare_cached("INSERT INTO retained_content VALUES(?1,?2,?3,?4,?5,?6)")?;
            for c in imports {
                insert.execute(params![
                    i64::try_from(c.key.generation).map_err(|_| StoreError::Invalid)?,
                    i64::try_from(c.key.sequence).map_err(|_| StoreError::Invalid)?,
                    c.subject,
                    c.headers.as_ref(),
                    c.payload.as_ref(),
                    content_checksum(c)
                ])?;
            }
            let mut insert = tx.prepare_cached("INSERT INTO live_content VALUES(?1,?2)")?;
            for key in live {
                insert.execute([
                    i64::try_from(key.generation).map_err(|_| StoreError::Invalid)?,
                    i64::try_from(key.sequence).map_err(|_| StoreError::Invalid)?,
                ])?;
            }
        }
        let missing:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM live_content l WHERE NOT EXISTS(SELECT 1 FROM retained_content c WHERE c.generation=l.generation AND c.seq=l.seq))",[],|r|r.get(0))?;
        if missing {
            return Err(StoreError::Corrupt(sequence));
        }
        tx.execute("INSERT INTO snapshot VALUES(1,?1,?2,?3) ON CONFLICT(id) DO UPDATE SET seq=excluded.seq,data=excluded.data,checksum=excluded.checksum",params![sequence as i64,state,checksum(sequence,"$SCREECH.snapshot.v2",&[],state)])?;
        tx.execute("DELETE FROM messages WHERE seq<=?1", [sequence as i64])?;
        tx.execute("DELETE FROM retained_content WHERE NOT EXISTS(SELECT 1 FROM live_content l WHERE l.generation=retained_content.generation AND l.seq=retained_content.seq)",[])?;
        crate::fault::point("before_checkpoint_commit");
        tx.commit()?;
        crate::fault::point("after_checkpoint_commit");
        crate::fault::point("before_wal_restart");
        let (busy, _, _): (i64, i64, i64) =
            self.connection
                .query_row("PRAGMA wal_checkpoint(RESTART)", [], |r| {
                    Ok((r.get(0)?, r.get(1)?, r.get(2)?))
                })?;
        if busy != 0 {
            return Err(StoreError::Invalid);
        }
        crate::fault::point("after_wal_restart");
        Ok(())
    }
    fn supports_batch(&self) -> bool {
        true
    }
    fn append_batch(&mut self, records: &[Append]) -> Result<u64, StoreError> {
        if records.is_empty() {
            return Err(StoreError::Invalid);
        }
        let last = self.last;
        let end = last
            .checked_add(records.len() as u64)
            .filter(|n| *n <= i64::MAX as u64)
            .ok_or(StoreError::Invalid)?;
        let tx = self.connection.transaction()?;
        {
            let mut insert = tx.prepare_cached("INSERT INTO messages VALUES(?1,?2,?3,?4,?5)")?;
            for (offset, record) in records.iter().enumerate() {
                let sequence = last + offset as u64 + 1;
                insert.execute(params![
                    sequence as i64,
                    record.subject,
                    record.headers.as_ref(),
                    record.payload.as_ref(),
                    checksum(sequence, &record.subject, &record.headers, &record.payload)
                ])?;
            }
        }
        let updated = tx
            .prepare_cached("UPDATE meta SET last_seq=?1 WHERE id=1 AND last_seq=?2")?
            .execute([end as i64, last as i64])?;
        if updated != 1 {
            return Err(StoreError::Invalid);
        }
        crate::fault::point("before_group_store_commit");
        crate::fault::point("before_commit");
        tx.commit()?;
        crate::fault::point("after_commit");
        crate::fault::point("after_group_store_commit");
        self.last = end;
        Ok(end)
    }
    fn supports_checkpoint(&self) -> bool {
        true
    }
    fn load_checkpoint(&self) -> Result<Option<(u64, Bytes)>, StoreError> {
        let row = self
            .connection
            .query_row(
                "SELECT seq,data,checksum FROM snapshot WHERE id=1",
                [],
                |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, Vec<u8>>(1)?,
                        r.get::<_, u32>(2)?,
                    ))
                },
            )
            .optional()?;
        row.map(|(seq, data, crc)| {
            let seq = u64::try_from(seq).map_err(|_| StoreError::Invalid)?;
            if seq > self.last_sequence()?
                || checksum(seq, "$SCREECH.snapshot.v2", &[], &data) != crc
            {
                return Err(StoreError::Corrupt(seq));
            }
            Ok((seq, data.into()))
        })
        .transpose()
    }
    fn checkpoint(&mut self, sequence: u64, state: &[u8]) -> Result<(), StoreError> {
        if sequence != self.last_sequence()? {
            return Err(StoreError::Invalid);
        }
        let tx = self.connection.transaction()?;
        tx.execute("INSERT INTO snapshot VALUES(1,?1,?2,?3) ON CONFLICT(id) DO UPDATE SET seq=excluded.seq,data=excluded.data,checksum=excluded.checksum",params![sequence as i64,state,checksum(sequence,"$SCREECH.snapshot.v2",&[],state)])?;
        tx.execute("DELETE FROM messages WHERE seq<=?1", [sequence as i64])?;
        crate::fault::point("before_checkpoint_commit");
        tx.commit()?;
        crate::fault::point("after_checkpoint_commit");
        crate::fault::point("before_wal_restart");
        // RESTART synchronizes the database before allowing WAL reuse. Keeping a
        // bounded allocation avoids truncate/reallocate work on the next append.
        let (busy, _, _): (i64, i64, i64) =
            self.connection
                .query_row("PRAGMA wal_checkpoint(RESTART)", [], |r| {
                    Ok((r.get(0)?, r.get(1)?, r.get(2)?))
                })?;
        if busy != 0 {
            return Err(StoreError::Invalid);
        }
        crate::fault::point("after_wal_restart");
        Ok(())
    }

    fn append(&mut self, subject: &str, headers: &[u8], payload: &[u8]) -> Result<u64, StoreError> {
        let last = self.last;
        let tx = self.connection.transaction()?;
        let seq = last
            .checked_add(1)
            .filter(|n| *n <= i64::MAX as u64)
            .ok_or(StoreError::Invalid)?;
        tx.prepare_cached("INSERT INTO messages VALUES(?1,?2,?3,?4,?5)")?
            .execute(params![
                seq as i64,
                subject,
                headers,
                payload,
                checksum(seq, subject, headers, payload)
            ])?;
        let updated = tx
            .prepare_cached("UPDATE meta SET last_seq=?1 WHERE id=1 AND last_seq=?2")?
            .execute([seq as i64, last as i64])?;
        if updated != 1 {
            return Err(StoreError::Invalid);
        }
        crate::fault::point("before_commit");
        tx.commit()?;
        crate::fault::point("after_commit");
        self.last = seq;
        Ok(seq)
    }
    fn get(&self, sequence: u64) -> Result<Option<Record>, StoreError> {
        if sequence > i64::MAX as u64 {
            return Ok(None);
        }
        let row = self
            .connection
            .query_row(
                "SELECT subject,headers,payload,checksum FROM messages WHERE seq=?1",
                [sequence as i64],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, Vec<u8>>(1)?,
                        r.get::<_, Vec<u8>>(2)?,
                        r.get::<_, u32>(3)?,
                    ))
                },
            )
            .optional()?;
        row.map(|(subject, headers, payload, crc)| {
            if checksum(sequence, &subject, &headers, &payload) != crc {
                return Err(StoreError::Corrupt(sequence));
            }
            Ok(Record {
                sequence,
                subject,
                headers: headers.into(),
                payload: payload.into(),
            })
        })
        .transpose()
    }
    fn last_sequence(&self) -> Result<u64, StoreError> {
        u64::try_from(self.connection.query_row(
            "SELECT last_seq FROM meta WHERE id=1",
            [],
            |r| r.get::<_, i64>(0),
        )?)
        .map_err(|_| StoreError::Invalid)
    }
    fn purge(&mut self) -> Result<(), StoreError> {
        let tx = self.connection.transaction()?;
        tx.execute("DELETE FROM messages", [])?;
        crate::fault::point("before_purge_commit");
        tx.commit()?;
        crate::fault::point("after_purge_commit");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cached_append_position_rejects_changed_persisted_highwater() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = SqliteStore::open(dir.path().join("log.db")).unwrap();
        store.append("a", b"", b"prior").unwrap();
        // Simulate inconsistent metadata through the connection owning the lock.
        store
            .connection
            .execute("UPDATE meta SET last_seq=7 WHERE id=1", [])
            .unwrap();
        assert!(store.append("a", b"", b"must not commit").is_err());
        assert!(store.get(2).unwrap().is_none());
        assert_eq!(store.last_sequence().unwrap(), 7);
    }

    #[test]
    fn sqlite_full_rolls_back_record_and_cached_position() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("log.db");
        let mut store = SqliteStore::open(&path).unwrap();
        store.append("a", b"", b"prior").unwrap();
        let pages: i64 = store
            .connection
            .pragma_query_value(None, "page_count", |r| r.get(0))
            .unwrap();
        store
            .connection
            .pragma_update(None, "max_page_count", pages)
            .unwrap();
        assert!(
            matches!(store.append("a", b"", &vec![0x5a; 128 * 1024]), Err(StoreError::Sqlite(rusqlite::Error::SqliteFailure(e, _))) if e.code == rusqlite::ErrorCode::DiskFull)
        );
        assert_eq!(store.last_sequence().unwrap(), 1);
        assert_eq!(store.last, 1);
        assert!(store.get(2).unwrap().is_none());
        store
            .connection
            .pragma_update(None, "max_page_count", pages + 1000)
            .unwrap();
        assert_eq!(store.append("a", b"", b"after").unwrap(), 2);
        drop(store);
        let store = SqliteStore::open(path).unwrap();
        assert_eq!(store.last_sequence().unwrap(), 2);
        assert_eq!(store.get(1).unwrap().unwrap().payload.as_ref(), b"prior");
        assert_eq!(store.get(2).unwrap().unwrap().payload.as_ref(), b"after");
    }
}

#[cfg(test)]
mod group_tests {
    use super::*;
    #[test]
    fn later_record_full_rolls_back_the_entire_group_and_position() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("group.db");
        let mut store = SqliteStore::open(&path).unwrap();
        store.append("a", b"", b"prior").unwrap();
        let pages: i64 = store
            .connection
            .pragma_query_value(None, "page_count", |r| r.get(0))
            .unwrap();
        store
            .connection
            .pragma_update(None, "max_page_count", pages)
            .unwrap();
        let records = vec![
            Append {
                subject: "a".into(),
                headers: Bytes::new(),
                payload: Bytes::from_static(b"small"),
            },
            Append {
                subject: "a".into(),
                headers: Bytes::new(),
                payload: vec![0x6a; 128 * 1024].into(),
            },
        ];
        assert!(
            matches!(store.append_batch(&records),Err(StoreError::Sqlite(rusqlite::Error::SqliteFailure(e,_))) if e.code==rusqlite::ErrorCode::DiskFull)
        );
        assert_eq!(store.last, 1);
        assert_eq!(store.last_sequence().unwrap(), 1);
        assert!(store.get(2).unwrap().is_none());
        assert!(store.get(3).unwrap().is_none());
        store
            .connection
            .pragma_update(None, "max_page_count", pages + 1000)
            .unwrap();
        assert_eq!(store.append_batch(&records).unwrap(), 3);
        drop(store);
        let store = SqliteStore::open(path).unwrap();
        assert_eq!(store.last_sequence().unwrap(), 3);
        assert_eq!(store.get(2).unwrap().unwrap().payload, records[0].payload);
        assert_eq!(store.get(3).unwrap().unwrap().payload, records[1].payload);
    }
}
