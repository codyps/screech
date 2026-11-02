//! Storage contracts. Publication appends and checkpoints return durable success.
//! Consumer transitions are durable by default; an explicit unsafe mode may buffer them.
//! Operations on one store are serialized by its owner, independently of transport.
use bytes::Bytes;
use rusqlite::{params, Connection, OptionalExtension};
use std::{
    cell::RefCell,
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
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
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
/// An immutable image built by the storage owner, then installed atomically.
pub struct CheckpointImage {
    pub data: Vec<u8>,
    pub live: Vec<ContentKey>,
    pub imports: Vec<Content>,
}
pub type CheckpointJob = Box<dyn FnOnce() -> Result<CheckpointImage, StoreError> + Send>;
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
    /// Execute serialization/reachability work in the same ordered I/O domain
    /// as installation. The default preserves synchronous standalone behavior.
    fn checkpoint_job(&mut self, sequence: u64, job: CheckpointJob) -> Result<(), StoreError> {
        let image = job()?;
        if self.external_content() {
            self.checkpoint_with_content(
                sequence,
                &image.data,
                &mut image.live.into_iter(),
                &image.imports,
            )
        } else {
            self.checkpoint(sequence, &image.data)
        }
    }
    /// Opt in only when successful appends survive process/host restart. This is
    /// independent of batching: MemoryStore is useful for models, not a voter.
    fn durable_appends(&self) -> bool {
        false
    }
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
    fn buffers_consumer_state(&self) -> bool {
        false
    }
    /// Consumer-only transitions. Backends remain strictly durable by default.
    fn append_consumer_state(&mut self, records: &[Append]) -> Result<u64, StoreError> {
        if let [record] = records {
            self.append(&record.subject, &record.headers, &record.payload)
        } else {
            self.append_batch(records)
        }
    }
    fn flush_consumer_state(&mut self, _force: bool) -> Result<(), StoreError> {
        Ok(())
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
    /// Resolve exactly these keys, in order. A missing or corrupt row fails the batch.
    fn get_contents(&self, keys: &[ContentKey]) -> Result<Vec<Content>, StoreError> {
        keys.iter()
            .map(|key| {
                self.get_content(*key)?
                    .ok_or(StoreError::Corrupt(key.sequence))
            })
            .collect()
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
const UNSAFE_CONSUMER_RECORDS: usize = 1024;
struct ConsumerBlock {
    first: u64,
    records: Vec<Append>,
}
impl ConsumerBlock {
    fn get(&self, sequence: u64) -> Option<Record> {
        let index = usize::try_from(sequence.checked_sub(self.first)?).ok()?;
        let r = self.records.get(index)?;
        Some(Record {
            sequence,
            subject: r.subject.clone(),
            headers: r.headers.clone(),
            payload: r.payload.clone(),
        })
    }
}
pub struct SqliteStore {
    connection: Connection,
    _lock: File,
    last: u64,
    unsafe_ack_durability: bool,
    buffered_since: Option<std::time::Instant>,
    buffered_consumer: Vec<Append>,
    buffered_bytes: usize,
    consumer_cache: RefCell<Option<ConsumerBlock>>,
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
// Allocation hint only: all commits still use SQLite's WAL FULL sync path.
fn reserve_wal(connection: &Connection) {
    use rusqlite::ffi;
    // SAFETY: the connection has one owner and remains alive throughout this call.
    // SQLite returns its current WAL file; no pointer is retained, and supported
    // VFS controls only extend allocation beyond EOF, preserving the valid prefix.
    unsafe {
        let mut file: *mut ffi::sqlite3_file = std::ptr::null_mut();
        let rc = ffi::sqlite3_file_control(
            connection.handle(),
            c"main".as_ptr(),
            ffi::SQLITE_FCNTL_JOURNAL_POINTER,
            std::ptr::addr_of_mut!(file).cast(),
        );
        if rc != ffi::SQLITE_OK || file.is_null() || (*file).pMethods.is_null() {
            return;
        }
        let Some(control) = (*(*file).pMethods).xFileControl else {
            return;
        };
        let mut chunk: i32 = 32 * 1024 * 1024;
        if control(
            file,
            ffi::SQLITE_FCNTL_CHUNK_SIZE,
            std::ptr::addr_of_mut!(chunk).cast(),
        ) != ffi::SQLITE_OK
        {
            return;
        }
        let mut bytes: i64 = chunk.into();
        let rc = control(
            file,
            ffi::SQLITE_FCNTL_SIZE_HINT,
            std::ptr::addr_of_mut!(bytes).cast(),
        );
        chunk = 0;
        control(
            file,
            ffi::SQLITE_FCNTL_CHUNK_SIZE,
            std::ptr::addr_of_mut!(chunk).cast(),
        );
        if rc != ffi::SQLITE_OK {
            tracing::debug!(sqlite_code = rc, "WAL allocation hint unavailable");
        }
    }
}
impl SqliteStore {
    fn append_transaction(
        &mut self,
        records: &[Append],
        content: &[Content],
    ) -> Result<u64, StoreError> {
        if records.is_empty() && self.buffered_consumer.is_empty() {
            return Err(StoreError::Invalid);
        }
        let last = self.last;
        let end = last
            .checked_add((self.buffered_consumer.len() + records.len()) as u64)
            .filter(|n| *n <= i64::MAX as u64)
            .ok_or(StoreError::Invalid)?;
        let block = if self.buffered_consumer.is_empty() {
            None
        } else {
            Some(postcard::to_allocvec(&self.buffered_consumer).map_err(|_| StoreError::Invalid)?)
        };
        let tx = self.connection.transaction()?;
        {
            if let Some(block) = &block {
                let first = last + 1;
                let through = last + self.buffered_consumer.len() as u64;
                tx.prepare_cached("INSERT INTO consumer_blocks VALUES(?1,?2,?3,?4)")?
                    .execute(params![
                        first as i64,
                        through as i64,
                        block,
                        checksum(
                            first,
                            "$SCREECH.consumer-block.v1",
                            &through.to_le_bytes(),
                            block
                        )
                    ])?;
            }
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
                let seq = last + self.buffered_consumer.len() as u64 + offset as u64 + 1;
                insert.execute(params![
                    seq as i64,
                    r.subject,
                    r.headers.as_ref(),
                    r.payload.as_ref(),
                    checksum(seq, &r.subject, &r.headers, &r.payload)
                ])?;
            }
        }
        let update = if block.is_some() {
            "UPDATE meta SET version=2,last_seq=?1 WHERE id=1 AND last_seq=?2"
        } else {
            "UPDATE meta SET last_seq=?1 WHERE id=1 AND last_seq=?2"
        };
        if tx
            .prepare_cached(update)?
            .execute([end as i64, last as i64])?
            != 1
        {
            return Err(StoreError::Invalid);
        }
        if self.buffered_consumer.len() + records.len() > 1 {
            crate::fault::point("before_group_store_commit");
        }
        crate::fault::point("before_commit");
        tx.commit()?;
        crate::fault::point("after_commit");
        if self.buffered_consumer.len() + records.len() > 1 {
            crate::fault::point("after_group_store_commit");
        }
        self.last = end;
        self.buffered_since = None;
        self.buffered_consumer.clear();
        self.buffered_bytes = 0;
        Ok(end)
    }
    fn get_consumer_record(&self, sequence: u64) -> Result<Option<Record>, StoreError> {
        if let Some(record) = self
            .consumer_cache
            .borrow()
            .as_ref()
            .and_then(|b| b.get(sequence))
        {
            return Ok(Some(record));
        }
        let row = self.connection.prepare_cached("SELECT first_seq,last_seq,data,checksum FROM consumer_blocks WHERE first_seq<=?1 ORDER BY first_seq DESC LIMIT 1")?.query_row([sequence as i64],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,i64>(1)?,r.get::<_,Vec<u8>>(2)?,r.get::<_,u32>(3)?))).optional()?;
        let Some((first, through, data, crc)) = row else {
            return Ok(None);
        };
        let first = u64::try_from(first).map_err(|_| StoreError::Corrupt(sequence))?;
        let through = u64::try_from(through).map_err(|_| StoreError::Corrupt(sequence))?;
        if through < sequence {
            return Ok(None);
        }
        if through < first
            || through > self.last
            || through - first + 1 > UNSAFE_CONSUMER_RECORDS as u64
            || data.len() > 1024 * 1024
            || checksum(
                first,
                "$SCREECH.consumer-block.v1",
                &through.to_le_bytes(),
                &data,
            ) != crc
        {
            return Err(StoreError::Corrupt(sequence));
        }
        let (count, _) =
            postcard::take_from_bytes::<usize>(&data).map_err(|_| StoreError::Corrupt(sequence))?;
        if count as u64 != through - first + 1 {
            return Err(StoreError::Corrupt(sequence));
        }
        let (records, remaining): (Vec<Append>, _) =
            postcard::take_from_bytes(&data).map_err(|_| StoreError::Corrupt(sequence))?;
        if !remaining.is_empty() || records.len() != count {
            return Err(StoreError::Corrupt(sequence));
        }
        let block = ConsumerBlock { first, records };
        let record = block.get(sequence).ok_or(StoreError::Corrupt(sequence))?;
        *self.consumer_cache.borrow_mut() = Some(block);
        Ok(Some(record))
    }
    /// Opt in to acknowledging consumer state before a durable flush.
    /// Publication/content appends and checkpoints still use FULL synchronization.
    pub fn open_with_unsafe_ack_durability(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let mut store = Self::open(path)?;
        store.unsafe_ack_durability = true;
        store.buffered_consumer = Vec::with_capacity(UNSAFE_CONSUMER_RECORDS);
        Ok(store)
    }
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
        conn.pragma_update(None, "journal_size_limit", 32 * 1024 * 1024)?;
        let mode: String = conn.pragma_query_value(None, "journal_mode", |r| r.get(0))?;
        let sync: i64 = conn.pragma_query_value(None, "synchronous", |r| r.get(0))?;
        if mode != "wal" || sync != 2 {
            return Err(StoreError::Invalid);
        }
        conn.execute_batch("CREATE TABLE IF NOT EXISTS meta (id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL, last_seq INTEGER NOT NULL CHECK(last_seq>=0)); INSERT OR IGNORE INTO meta VALUES(1,1,0); CREATE TABLE IF NOT EXISTS snapshot(id INTEGER PRIMARY KEY CHECK(id=1), seq INTEGER NOT NULL, data BLOB NOT NULL, checksum INTEGER NOT NULL); CREATE TABLE IF NOT EXISTS messages(seq INTEGER PRIMARY KEY CHECK(seq>0), subject TEXT NOT NULL, headers BLOB NOT NULL, payload BLOB NOT NULL, checksum INTEGER NOT NULL);")?;
        conn.execute_batch("CREATE TABLE IF NOT EXISTS retained_content(generation INTEGER NOT NULL CHECK(generation>0),seq INTEGER NOT NULL CHECK(seq>0),subject TEXT NOT NULL,headers BLOB NOT NULL,payload BLOB NOT NULL,checksum INTEGER NOT NULL,PRIMARY KEY(generation,seq)) WITHOUT ROWID; DROP INDEX IF EXISTS retained_content_keys;")?;
        conn.execute_batch("CREATE TABLE IF NOT EXISTS consumer_blocks(first_seq INTEGER PRIMARY KEY CHECK(first_seq>0),last_seq INTEGER NOT NULL CHECK(last_seq>=first_seq),data BLOB NOT NULL CHECK(length(data)<=1048576),checksum INTEGER NOT NULL);")?;
        let version: i64 =
            conn.query_row("SELECT version FROM meta WHERE id=1", [], |r| r.get(0))?;
        if ![1, 2].contains(&version) {
            return Err(StoreError::Invalid);
        }
        let check: String = conn.query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
        if check != "ok" {
            return Err(StoreError::Corrupt(0));
        }
        reserve_wal(&conn);
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
            unsafe_ack_durability: false,
            buffered_since: None,
            buffered_consumer: Vec::new(),
            buffered_bytes: 0,
            consumer_cache: RefCell::new(None),
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
        let mut stmt = store
            .connection
            .prepare("SELECT first_seq,last_seq FROM consumer_blocks ORDER BY first_seq")?;
        let ranges = stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)))?;
        let mut previous = 0;
        for range in ranges {
            let (first, through) = range?;
            let (first, through) = (
                u64::try_from(first).map_err(|_| StoreError::Corrupt(0))?,
                u64::try_from(through).map_err(|_| StoreError::Corrupt(0))?,
            );
            if version < 2 || first <= previous || through < first || through > last {
                return Err(StoreError::Corrupt(first));
            }
            store
                .get_consumer_record(first)?
                .ok_or(StoreError::Corrupt(first))?;
            previous = through;
        }
        let overlap:bool=store.connection.query_row("SELECT EXISTS(SELECT 1 FROM consumer_blocks b JOIN messages m ON m.seq BETWEEN b.first_seq AND b.last_seq)",[],|r|r.get(0))?;
        if overlap {
            return Err(StoreError::Corrupt(0));
        }
        drop(stmt);
        Ok(store)
    }
}
impl Store for SqliteStore {
    fn durable_appends(&self) -> bool {
        !self.unsafe_ack_durability
    }
    fn buffers_consumer_state(&self) -> bool {
        self.unsafe_ack_durability
    }
    fn append_consumer_state(&mut self, records: &[Append]) -> Result<u64, StoreError> {
        if records.is_empty() {
            return Err(StoreError::Invalid);
        }
        if !self.unsafe_ack_durability {
            return self.append_transaction(records, &[]);
        }
        let bytes = records.iter().fold(0usize, |n, r| {
            n.saturating_add(r.subject.len())
                .saturating_add(r.headers.len())
                .saturating_add(r.payload.len())
                .saturating_add(std::mem::size_of::<Append>())
        });
        if records.len() > UNSAFE_CONSUMER_RECORDS || bytes > 1024 * 1024 {
            return self.append_transaction(records, &[]);
        }
        if self.buffered_consumer.len() + records.len() > UNSAFE_CONSUMER_RECORDS
            || self.buffered_bytes.saturating_add(bytes) > 1024 * 1024
        {
            self.flush_consumer_state(true)?;
        }
        let end = self
            .last
            .checked_add(self.buffered_consumer.len() as u64)
            .and_then(|n| n.checked_add(records.len() as u64))
            .filter(|n| *n <= i64::MAX as u64)
            .ok_or(StoreError::Invalid)?;
        self.buffered_consumer.extend_from_slice(records);
        self.buffered_bytes += bytes;
        self.buffered_since
            .get_or_insert_with(std::time::Instant::now);
        Ok(end)
    }
    fn flush_consumer_state(&mut self, force: bool) -> Result<(), StoreError> {
        let Some(since) = self.buffered_since else {
            return Ok(());
        };
        if !force
            && (!crate::fault::periodic_consumer_flush_ready()
                || since.elapsed() < std::time::Duration::from_millis(50))
        {
            return Ok(());
        }
        crate::fault::point("before_unsafe_ack_flush");
        self.append_transaction(&[], &[])?;
        crate::fault::point("after_unsafe_ack_flush");
        Ok(())
    }
    fn external_content(&self) -> bool {
        true
    }
    fn append_with_content(
        &mut self,
        records: &[Append],
        content: &[Content],
    ) -> Result<u64, StoreError> {
        if records.is_empty() {
            return Err(StoreError::Invalid);
        }
        self.append_transaction(records, content)
    }
    fn get_content(&self, key: ContentKey) -> Result<Option<Content>, StoreError> {
        let generation = i64::try_from(key.generation).map_err(|_| StoreError::Invalid)?;
        let seq = i64::try_from(key.sequence).map_err(|_| StoreError::Invalid)?;
        let row = self.connection.prepare_cached("SELECT subject,headers,payload,checksum FROM retained_content WHERE generation=?1 AND seq=?2")?.query_row([generation,seq], |r| Ok((r.get::<_,String>(0)?,r.get::<_,Vec<u8>>(1)?,r.get::<_,Vec<u8>>(2)?,r.get::<_,u32>(3)?))).optional()?;
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
    fn get_contents(&self, keys: &[ContentKey]) -> Result<Vec<Content>, StoreError> {
        let Some(first) = keys.first() else {
            return Ok(Vec::new());
        };
        if keys.len() < 2
            || !keys.windows(2).all(|p| {
                p[0].generation == p[1].generation
                    && p[0].sequence.checked_add(1) == Some(p[1].sequence)
            })
        {
            return keys
                .iter()
                .map(|key| {
                    self.get_content(*key)?
                        .ok_or(StoreError::Corrupt(key.sequence))
                })
                .collect();
        }
        let mut statement = self.connection.prepare_cached("SELECT seq,subject,headers,payload,checksum FROM retained_content WHERE generation=?1 AND seq BETWEEN ?2 AND ?3 ORDER BY seq")?;
        let mut rows = statement.query(params![
            i64::try_from(first.generation).map_err(|_| StoreError::Invalid)?,
            i64::try_from(first.sequence).map_err(|_| StoreError::Invalid)?,
            i64::try_from(keys.last().unwrap().sequence).map_err(|_| StoreError::Invalid)?,
        ])?;
        let mut contents = Vec::with_capacity(keys.len());
        while let Some(row) = rows.next()? {
            let key = keys
                .get(contents.len())
                .ok_or(StoreError::Corrupt(first.sequence))?;
            if u64::try_from(row.get::<_, i64>(0)?).ok() != Some(key.sequence) {
                return Err(StoreError::Corrupt(key.sequence));
            }
            let content = Content {
                key: *key,
                subject: row.get(1)?,
                headers: row.get::<_, Vec<u8>>(2)?.into(),
                payload: row.get::<_, Vec<u8>>(3)?.into(),
            };
            if content_checksum(&content) != row.get::<_, u32>(4)? {
                return Err(StoreError::Corrupt(key.sequence));
            }
            contents.push(content);
        }
        if contents.len() != keys.len() {
            return Err(StoreError::Corrupt(keys[contents.len()].sequence));
        }
        Ok(contents)
    }
    fn validate_content(
        &self,
        key: ContentKey,
        subject: &str,
        headers: u64,
        payload: u64,
    ) -> Result<(), StoreError> {
        let row = self.connection.prepare_cached("SELECT subject,length(headers),length(payload) FROM retained_content WHERE generation=?1 AND seq=?2")?.query_row(params![i64::try_from(key.generation).map_err(|_| StoreError::Invalid)?,i64::try_from(key.sequence).map_err(|_| StoreError::Invalid)?], |r| Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?,r.get::<_,i64>(2)?))).optional()?;
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
        let diagnostic_start =
            tracing::enabled!(tracing::Level::DEBUG).then(std::time::Instant::now);
        self.flush_consumer_state(true)?;
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
        tx.execute(
            "DELETE FROM consumer_blocks WHERE last_seq<=?1",
            [sequence as i64],
        )?;
        tx.execute("DELETE FROM retained_content WHERE (generation,seq) IN (SELECT c.generation,c.seq FROM retained_content c WHERE NOT EXISTS(SELECT 1 FROM live_content l WHERE l.generation=c.generation AND l.seq=c.seq))",[])?;
        crate::fault::point("before_checkpoint_commit");
        tx.commit()?;
        self.consumer_cache.get_mut().take();
        if let Some(started) = diagnostic_start {
            tracing::debug!(
                sequence,
                snapshot_bytes = state.len(),
                store_checkpoint_commit_ms = started.elapsed().as_secs_f64() * 1000.0,
                "checkpoint committed"
            );
        }
        let restart_start = diagnostic_start.map(|_| std::time::Instant::now());
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
        if let Some(started) = restart_start {
            tracing::debug!(
                sequence,
                wal_restart_ms = started.elapsed().as_secs_f64() * 1000.0,
                "WAL restart completed"
            );
        }
        self.buffered_since = None;
        Ok(())
    }
    fn supports_batch(&self) -> bool {
        true
    }
    fn append_batch(&mut self, records: &[Append]) -> Result<u64, StoreError> {
        if records.is_empty() {
            return Err(StoreError::Invalid);
        }
        self.append_transaction(records, &[])
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
        self.flush_consumer_state(true)?;
        if sequence != self.last_sequence()? {
            return Err(StoreError::Invalid);
        }
        let tx = self.connection.transaction()?;
        tx.execute("INSERT INTO snapshot VALUES(1,?1,?2,?3) ON CONFLICT(id) DO UPDATE SET seq=excluded.seq,data=excluded.data,checksum=excluded.checksum",params![sequence as i64,state,checksum(sequence,"$SCREECH.snapshot.v2",&[],state)])?;
        tx.execute("DELETE FROM messages WHERE seq<=?1", [sequence as i64])?;
        tx.execute(
            "DELETE FROM consumer_blocks WHERE last_seq<=?1",
            [sequence as i64],
        )?;
        crate::fault::point("before_checkpoint_commit");
        tx.commit()?;
        self.consumer_cache.get_mut().take();
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
        self.buffered_since = None;
        Ok(())
    }

    fn append(&mut self, subject: &str, headers: &[u8], payload: &[u8]) -> Result<u64, StoreError> {
        if !self.buffered_consumer.is_empty() {
            return self.append_transaction(
                &[Append {
                    subject: subject.into(),
                    headers: Bytes::copy_from_slice(headers),
                    payload: Bytes::copy_from_slice(payload),
                }],
                &[],
            );
        }

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
        self.buffered_since = None;
        Ok(seq)
    }
    fn get(&self, sequence: u64) -> Result<Option<Record>, StoreError> {
        if let Some(record) = self
            .consumer_cache
            .borrow()
            .as_ref()
            .and_then(|b| b.get(sequence))
        {
            return Ok(Some(record));
        }

        if sequence > self.last {
            if let Some(r) = self
                .buffered_consumer
                .get((sequence - self.last - 1) as usize)
            {
                return Ok(Some(Record {
                    sequence,
                    subject: r.subject.clone(),
                    headers: r.headers.clone(),
                    payload: r.payload.clone(),
                }));
            }
        }

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
        let Some((subject, headers, payload, crc)) = row else {
            return self.get_consumer_record(sequence);
        };
        {
            if checksum(sequence, &subject, &headers, &payload) != crc {
                return Err(StoreError::Corrupt(sequence));
            }
            Ok(Some(Record {
                sequence,
                subject,
                headers: headers.into(),
                payload: payload.into(),
            }))
        }
    }
    fn last_sequence(&self) -> Result<u64, StoreError> {
        let persisted = u64::try_from(self.connection.query_row(
            "SELECT last_seq FROM meta WHERE id=1",
            [],
            |r| r.get::<_, i64>(0),
        )?)
        .map_err(|_| StoreError::Invalid)?;
        persisted
            .checked_add(self.buffered_consumer.len() as u64)
            .ok_or(StoreError::Invalid)
    }
    fn purge(&mut self) -> Result<(), StoreError> {
        self.flush_consumer_state(true)?;
        let tx = self.connection.transaction()?;
        tx.execute("DELETE FROM messages", [])?;
        tx.execute("DELETE FROM consumer_blocks", [])?;
        crate::fault::point("before_purge_commit");
        tx.commit()?;
        self.consumer_cache.get_mut().take();
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

#[cfg(test)]
mod unsafe_ack_tests {
    use super::*;
    #[test]
    fn buffered_consumer_commits_preserve_full_publication_fences() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store");
        let mut store = SqliteStore::open_with_unsafe_ack_durability(&path).unwrap();
        let record = Append {
            subject: "consumer".into(),
            headers: Bytes::new(),
            payload: Bytes::from_static(b"ack"),
        };
        store
            .append_consumer_state(std::slice::from_ref(&record))
            .unwrap();
        assert!(store.buffered_since.is_some());
        assert_eq!(
            store
                .connection
                .pragma_query_value(None, "synchronous", |r| r.get::<_, i64>(0))
                .unwrap(),
            2
        );
        store.flush_consumer_state(false).unwrap();
        assert!(store.buffered_since.is_some());
        store.append("publish", b"", b"body").unwrap();
        assert!(store.buffered_since.is_none());
        store
            .append_consumer_state(&[record.clone(), record.clone()])
            .unwrap();
        assert!(store.buffered_since.is_some());
        store.flush_consumer_state(true).unwrap();
        assert!(store.buffered_since.is_none());
        assert_eq!(store.last_sequence().unwrap(), 4);
        drop(store);
        let mut store = SqliteStore::open(&path).unwrap();
        assert!(!store.unsafe_ack_durability);
        store.append_consumer_state(&[record]).unwrap();
        assert!(store.buffered_since.is_none());
        assert_eq!(store.last_sequence().unwrap(), 5);
    }
    #[test]
    fn buffered_positions_are_visible_and_count_byte_and_time_limits_flush() {
        let dir = tempfile::tempdir().unwrap();
        let mut store =
            SqliteStore::open_with_unsafe_ack_durability(dir.path().join("store")).unwrap();
        let record = Append {
            subject: "consumer".into(),
            headers: Bytes::new(),
            payload: Bytes::from_static(b"ack"),
        };
        for sequence in 1..=1025 {
            assert_eq!(
                store
                    .append_consumer_state(std::slice::from_ref(&record))
                    .unwrap(),
                sequence
            );
            assert_eq!(store.last_sequence().unwrap(), sequence);
            if sequence <= 1024 {
                assert_eq!(store.last, 0);
            }
            assert_eq!(store.get(sequence).unwrap().unwrap().payload, b"ack"[..]);
        }
        assert_eq!(store.last, 1024);
        assert_eq!(store.buffered_consumer.len(), 1);
        store.buffered_since =
            Some(std::time::Instant::now() - std::time::Duration::from_millis(60));
        store.flush_consumer_state(false).unwrap();
        assert_eq!(store.last, 1025);
        assert!(store.buffered_consumer.is_empty());
        let large = Append {
            headers: Bytes::from(vec![0; 600 * 1024]),
            ..record.clone()
        };
        store
            .append_consumer_state(std::slice::from_ref(&large))
            .unwrap();
        store
            .append_consumer_state(std::slice::from_ref(&large))
            .unwrap();
        assert_eq!(store.last, 1026);
        assert_eq!(store.buffered_consumer.len(), 1);
        store.checkpoint(1027, b"state").unwrap();
        assert_eq!(store.last, 1027);
        assert!(store.buffered_consumer.is_empty());
        assert_eq!(store.load_checkpoint().unwrap().unwrap().0, 1027);
    }
    #[test]
    fn invalid_buffered_commit_does_not_change_synchronous_policy() {
        let dir = tempfile::tempdir().unwrap();
        let mut store =
            SqliteStore::open_with_unsafe_ack_durability(dir.path().join("store")).unwrap();
        assert!(store.append_consumer_state(&[]).is_err());
        assert_eq!(
            store
                .connection
                .pragma_query_value(None, "synchronous", |r| r.get::<_, i64>(0))
                .unwrap(),
            2
        );
        assert!(store.buffered_since.is_none());
        store.append("publish", b"", b"body").unwrap();
    }
}

#[cfg(test)]
mod consumer_block_tests {
    use super::*;
    fn records() -> Vec<Append> {
        (0..3)
            .map(|i| Append {
                subject: format!("consumer.{i}"),
                headers: Bytes::from(vec![i; 3]),
                payload: Bytes::from(vec![i + 1; 7]),
            })
            .collect()
    }
    #[test]
    fn blocks_preserve_logical_positions_across_publish_checkpoint_purge_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store");
        let rows = records();
        let mut store = SqliteStore::open_with_unsafe_ack_durability(&path).unwrap();
        assert_eq!(store.append_consumer_state(&rows).unwrap(), 3);
        assert_eq!(store.append("publish", b"", b"body").unwrap(), 4);
        assert_eq!(
            store
                .connection
                .query_row("SELECT version FROM meta", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            2
        );
        assert_eq!(
            store
                .connection
                .query_row("SELECT count(*) FROM consumer_blocks", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(
            store
                .connection
                .query_row("SELECT count(*) FROM messages", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            1
        );
        drop(store);
        let mut store = SqliteStore::open_with_unsafe_ack_durability(&path).unwrap();
        for sequence in [3, 1, 2] {
            let got = store.get(sequence).unwrap().unwrap();
            let expected = &rows[sequence as usize - 1];
            assert_eq!(
                (&got.subject, &got.headers, &got.payload),
                (&expected.subject, &expected.headers, &expected.payload)
            );
        }
        assert_eq!(store.get(4).unwrap().unwrap().payload, b"body"[..]);
        store.checkpoint(4, b"snapshot").unwrap();
        assert!(store.get(1).unwrap().is_none());
        assert!(store.get(4).unwrap().is_none());
        assert_eq!(store.append_consumer_state(&rows).unwrap(), 7);
        store.flush_consumer_state(true).unwrap();
        assert!(store.get(6).unwrap().is_some());
        store.purge().unwrap();
        assert!(store.get(6).unwrap().is_none());
        assert_eq!(store.last_sequence().unwrap(), 7);
        drop(store);
        assert_eq!(SqliteStore::open(path).unwrap().last_sequence().unwrap(), 7);
    }
    #[test]
    fn corrupt_or_noncanonical_blocks_fail_closed_before_allocating_unbounded_records() {
        for mutation in 0..7 {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("store");
            let rows = records();
            let mut store = SqliteStore::open_with_unsafe_ack_durability(&path).unwrap();
            store.append_consumer_state(&rows).unwrap();
            store.append("publish", b"", b"body").unwrap();
            drop(store);
            let db = Connection::open(&path).unwrap();
            match mutation {
                0 => {
                    db.execute("UPDATE consumer_blocks SET data=zeroblob(length(data))", [])
                        .unwrap();
                }
                1 => {
                    db.execute("UPDATE consumer_blocks SET last_seq=2", [])
                        .unwrap();
                }
                2 => {
                    db.execute("UPDATE consumer_blocks SET first_seq=2", [])
                        .unwrap();
                }
                3..=5 => {
                    let mut data = match mutation {
                        3 => postcard::to_allocvec(&rows[..1]).unwrap(),
                        5 => postcard::to_allocvec(&usize::MAX).unwrap(),
                        _ => postcard::to_allocvec(&rows).unwrap(),
                    };
                    if mutation == 4 {
                        data.push(0);
                    }
                    let crc = checksum(1, "$SCREECH.consumer-block.v1", &3u64.to_le_bytes(), &data);
                    db.execute(
                        "UPDATE consumer_blocks SET data=?1,checksum=?2",
                        params![data, crc],
                    )
                    .unwrap();
                }
                _ => {
                    db.execute("UPDATE meta SET version=1", []).unwrap();
                }
            }
            drop(db);
            assert!(SqliteStore::open(path).is_err(), "mutation {mutation}");
        }
    }
    #[test]
    fn a_block_cannot_overlap_ordinary_records_even_with_a_valid_checksum() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store");
        let mut rows = records();
        let mut store = SqliteStore::open_with_unsafe_ack_durability(&path).unwrap();
        store.append_consumer_state(&rows).unwrap();
        store.append("publish", b"", b"body").unwrap();
        drop(store);
        rows.push(rows[0].clone());
        let data = postcard::to_allocvec(&rows).unwrap();
        let db = Connection::open(&path).unwrap();
        db.execute(
            "UPDATE consumer_blocks SET last_seq=4,data=?1,checksum=?2",
            params![
                data,
                checksum(1, "$SCREECH.consumer-block.v1", &4u64.to_le_bytes(), &data)
            ],
        )
        .unwrap();
        drop(db);
        assert!(SqliteStore::open(path).is_err());
    }
}

#[cfg(test)]
mod retained_content_layout_tests {
    use super::*;

    #[test]
    fn legacy_content_index_migration_preserves_content_replay_and_collection() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store");
        let content = Content {
            key: ContentKey {
                generation: 1,
                sequence: 1,
            },
            subject: "events.a".into(),
            headers: Bytes::from_static(b"NATS/1.0\r\nX: y\r\n\r\n"),
            payload: vec![0x5a; 600 * 1024].into(),
        };
        let record = Append {
            subject: "event".into(),
            headers: Bytes::new(),
            payload: Bytes::from_static(b"reference"),
        };
        let mut store = SqliteStore::open(&path).unwrap();
        store
            .append_with_content(&[record], std::slice::from_ref(&content))
            .unwrap();
        drop(store);
        let old = Connection::open(&path).unwrap();
        old.execute_batch("CREATE INDEX retained_content_keys ON retained_content(generation,seq)")
            .unwrap();
        drop(old);
        let mut store = SqliteStore::open(&path).unwrap();
        assert_eq!(store.get(1).unwrap().unwrap().payload, b"reference"[..]);
        assert_eq!(
            store.get_content(content.key).unwrap(),
            Some(content.clone())
        );
        store
            .checkpoint_with_content(1, b"snapshot", &mut [content.key].into_iter(), &[])
            .unwrap();
        drop(store);
        let mut store = SqliteStore::open(&path).unwrap();
        assert_eq!(
            store.get_content(content.key).unwrap(),
            Some(content.clone())
        );
        store
            .checkpoint_with_content(1, b"empty", &mut std::iter::empty(), &[])
            .unwrap();
        drop(store);
        let store = SqliteStore::open(path).unwrap();
        assert_eq!(store.last_sequence().unwrap(), 1);
        assert!(store.get_content(content.key).unwrap().is_none());
    }
}
