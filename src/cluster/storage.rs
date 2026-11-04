//! Complete accepted entries and voting promises sync in one authenticated
//! journal. SQLite projects that history with NORMAL transactions between FULL
//! snapshot roots. Startup replays before the node can vote or expose data.
use super::{error, Batch, ClusterConfig};
mod page_checkpoint;
mod snapshots;
mod watermark;
use crate::storage::{Content, ContentKey, Record};
use fs2::FileExt;
pub(crate) use page_checkpoint::PageCheckpointer;
use protobuf::Message;
use raft::{
    eraftpb::{ConfState, Entry, HardState, Snapshot},
    GetEntriesContext, RaftState, Storage, StorageError,
};
use rusqlite::{params, Connection, OptionalExtension};
pub(crate) use snapshots::MAX_SNAPSHOT;
use std::{
    fs::{File, OpenOptions},
    io::{self, Write},
    path::Path,
    sync::{Arc, Mutex},
};

fn sql(n: u64) -> io::Result<i64> {
    i64::try_from(n).map_err(error)
}

pub(crate) struct Database {
    pub conn: Connection,
    pub failed: bool,
    pub gate: Arc<Mutex<()>>,
    pub node_id: u64,
    pub incarnation: [u8; 16],
    pub genesis: ConfState,
    pub max_log_bytes: u64,
    pub snapshot_position: (u64, u64, u64),
    entries_bytes: u64,
    projection_batch: bool,
    _lock: File,
    watermark: watermark::Watermark,
}
#[derive(Clone)]
pub(crate) struct LogStorage {
    pub db: Arc<Mutex<Database>>,
}
impl Database {
    #[cfg(test)]
    pub fn open(path: &Path, config: &ClusterConfig, bootstrap: bool) -> io::Result<Self> {
        Self::open_mode(path, config, bootstrap, false)
    }
    pub fn open_mode(
        path: &Path,
        config: &ClusterConfig,
        bootstrap: bool,
        join: bool,
    ) -> io::Result<Self> {
        Self::open_identity(path, config, bootstrap, join, None)
    }
    pub fn open_learner(path: &Path, config: &ClusterConfig, slot: u64) -> io::Result<Self> {
        let mut incarnation = [0; 16];
        getrandom::fill(&mut incarnation).map_err(error)?;
        incarnation[..8].copy_from_slice(&slot.to_le_bytes());
        Self::open_identity(path, config, false, true, Some(incarnation))
    }
    fn open_identity(
        path: &Path,
        config: &ClusterConfig,
        bootstrap: bool,
        join: bool,
        supplied_incarnation: Option<[u8; 16]>,
    ) -> io::Result<Self> {
        let marker = path.with_extension("cluster-lock");
        let existed = path.exists();
        if !existed && !bootstrap && !join {
            return Err(error("cluster storage is missing; startup must recover a fresh incarnation through the quorum"));
        }
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&marker)?;
        lock.try_lock_exclusive()?;
        let marker_bytes = std::fs::read(&marker).unwrap_or_default();
        if !existed && lock.metadata()?.len() > 0 {
            return Err(error("missing cluster database"));
        }
        let conn = Connection::open(path).map_err(error)?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(error)?;
        conn.pragma_update(None, "synchronous", "FULL")
            .map_err(error)?;
        // Match the standalone WAL budget. Snapshotting retained payloads and
        // SQLite's page checkpoint are separate work; avoid forcing both into
        // short admission bursts. The accepted journal syncs every new entry.
        conn.pragma_update(None, "wal_autocheckpoint", 4096)
            .map_err(error)?;
        conn.pragma_update(None, "journal_size_limit", 32 * 1024 * 1024)
            .map_err(error)?;
        let initialized: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='cluster_meta')",
                [],
                |r| r.get(0),
            )
            .map_err(error)?;
        if existed && !initialized {
            return Err(error("not a cluster database; offline migration required"));
        }
        conn.execute_batch("CREATE TABLE IF NOT EXISTS cluster_meta(key TEXT PRIMARY KEY,value BLOB NOT NULL);
          CREATE TABLE IF NOT EXISTS entries(idx INTEGER PRIMARY KEY,term INTEGER NOT NULL,data BLOB NOT NULL,crc INTEGER NOT NULL);
          CREATE TABLE IF NOT EXISTS event_index(seq INTEGER PRIMARY KEY,idx INTEGER NOT NULL,ordinal INTEGER NOT NULL);
          CREATE TABLE IF NOT EXISTS body_index(generation INTEGER NOT NULL,seq INTEGER NOT NULL,idx INTEGER NOT NULL,ordinal INTEGER NOT NULL,PRIMARY KEY(generation,seq));
          CREATE TABLE IF NOT EXISTS snapshot_bodies(generation INTEGER NOT NULL,seq INTEGER NOT NULL,data BLOB NOT NULL,digest BLOB NOT NULL,PRIMARY KEY(generation,seq));").map_err(error)?;
        let mut stable = config.clone();
        stable.max_log_bytes = 0;
        let identity = serde_json::to_vec(&stable).map_err(error)?;
        if initialized {
            let old: Vec<u8> = conn
                .query_row(
                    "SELECT value FROM cluster_meta WHERE key='identity'",
                    [],
                    |r| r.get(0),
                )
                .map_err(error)?;
            let old: ClusterConfig = serde_json::from_slice(&old).map_err(error)?;
            if old.node_id != config.node_id
                || old.cluster_id != config.cluster_id
                || old.secret != config.secret
                || old.voters() != config.voters()
            {
                return Err(error(
                    "cluster/node identity or genesis membership mismatch",
                ));
            }
        } else {
            conn.execute("INSERT INTO cluster_meta VALUES('identity',?1)", [identity])
                .map_err(error)?;
        }
        let incarnation = if let Some(bytes) = conn
            .query_row(
                "SELECT value FROM cluster_meta WHERE key='incarnation'",
                [],
                |r| r.get::<_, Vec<u8>>(0),
            )
            .optional()
            .map_err(error)?
        {
            bytes.try_into().map_err(|_| error("invalid incarnation"))?
        } else {
            let mut n = supplied_incarnation.unwrap_or([0; 16]);
            if join && supplied_incarnation.is_none() {
                getrandom::fill(&mut n).map_err(error)?;
            }
            conn.execute(
                "INSERT INTO cluster_meta VALUES('incarnation',?1)",
                [n.as_slice()],
            )
            .map_err(error)?;
            n
        };
        let genesis = ConfState::from((
            config.voters(),
            if incarnation != [0; 16] {
                vec![config.node_id]
            } else {
                vec![]
            },
        ));
        let mut db = Self {
            conn,
            watermark: watermark::Watermark::open(path, config, initialized, &marker_bytes)?,
            _lock: lock,
            failed: false,
            snapshot_position: (0, 0, 0),
            entries_bytes: 0,
            projection_batch: false,
            gate: Arc::new(Mutex::new(())),
            node_id: config.node_id,
            incarnation,
            genesis: genesis.clone(),
            max_log_bytes: config.max_log_bytes,
        };
        db.refresh_log_bytes()?;
        if db.meta("conf")?.is_none() {
            db.conn
                .execute(
                    "INSERT INTO cluster_meta VALUES('conf',?1)",
                    [genesis.write_to_bytes().map_err(error)?],
                )
                .map_err(error)?;
        }
        db.restore_snapshot_position()?;
        db.recover_journal()?;
        let base = db.snapshot_base()?.0;
        if base > 0 {
            // A checksum-valid but malformed model/catalog is not a safe voter.
            // Validate before RawNode can campaign or send replication messages.
            db.validate_stored_snapshot()?;
        }
        let hard = db.hard()?;
        let last = db.last()?;
        if hard.commit > last {
            return Err(error("committed Raft prefix is missing"));
        }
        for idx in base + 1..=last {
            let _ = db.entry(idx)?;
        }
        db.verify_watermark()?;
        db.recover_membership()?;
        // Rebuild indexes from the durable committed prefix, never the accepted tail.
        db.conn.execute_batch("DELETE FROM event_index;DELETE FROM body_index;DELETE FROM cluster_meta WHERE key='view';").map_err(error)?;
        db.conn
            .execute(
                "INSERT OR REPLACE INTO cluster_meta VALUES('view',?1)",
                [base.to_le_bytes().as_slice()],
            )
            .map_err(error)?;
        db.apply(hard.commit)?;
        db.seal()?;
        if marker_bytes != watermark::MARKER {
            // Once the new format is published, a restart must not tear its
            // downgrade fence by truncating and rewriting an identical marker.
            db._lock.set_len(0)?;
            db._lock.write_all(watermark::MARKER)?;
            db._lock.sync_all()?;
        }
        File::open(
            path.parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new(".")),
        )?
        .sync_all()?;
        db.conn
            .pragma_update(None, "synchronous", "NORMAL")
            .map_err(error)?;
        Ok(db)
    }
    pub fn meta(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        self.conn
            .prepare_cached("SELECT value FROM cluster_meta WHERE key=?1")
            .map_err(error)?
            .query_row([key], |r| r.get(0))
            .optional()
            .map_err(error)
    }
    pub fn hard(&self) -> io::Result<HardState> {
        self.meta("hard")?
            .map(|b| HardState::parse_from_bytes(&b).map_err(error))
            .transpose()
            .map(|h| h.unwrap_or_default())
    }
    pub fn last(&self) -> io::Result<u64> {
        self.conn
            .query_row("SELECT coalesce(max(idx),0) FROM entries", [], |r| {
                r.get::<_, i64>(0)
            })
            .map_err(error)
            .and_then(|n| u64::try_from(n).map_err(error))
            .and_then(|n| Ok(n.max(self.snapshot_base()?.0)))
    }
    pub fn end(&self) -> io::Result<u64> {
        self.conn
            .query_row("SELECT coalesce(max(seq),0) FROM event_index", [], |r| {
                r.get::<_, i64>(0)
            })
            .map_err(error)
            .and_then(|n| u64::try_from(n).map_err(error))
            .and_then(|n| Ok(n.max(self.snapshot_base()?.2)))
    }
    pub fn log_bytes(&self) -> io::Result<u64> {
        Ok(self.entries_bytes)
    }
    fn refresh_log_bytes(&mut self) -> io::Result<()> {
        self.entries_bytes = self
            .conn
            .query_row(
                "SELECT coalesce(sum(length(data)),0) FROM entries",
                [],
                |r| r.get::<_, i64>(0),
            )
            .map_err(error)
            .and_then(|n| u64::try_from(n).map_err(error))?;
        Ok(())
    }
    pub fn entry(&self, idx: u64) -> io::Result<Entry> {
        if self.failed {
            return Err(error("replica quarantined"));
        }
        let (bytes, crc): (Vec<u8>, u32) = self
            .conn
            .prepare_cached("SELECT data,crc FROM entries WHERE idx=?1")
            .map_err(error)?
            .query_row([sql(idx)?], |r| Ok((r.get(0)?, r.get(1)?)))
            .map_err(error)?;
        if crc32fast::hash(&bytes) != crc {
            return Err(error("Raft entry checksum mismatch"));
        }
        let entry = Entry::parse_from_bytes(&bytes).map_err(error)?;
        if entry.index != idx {
            return Err(error("Raft entry identity mismatch"));
        }
        Ok(entry)
    }
    pub fn persist(&mut self, entries: &[Entry], hard: Option<&HardState>) -> io::Result<()> {
        if self.failed {
            return Err(error("replica quarantined"));
        }
        if entries.is_empty() && hard.is_none() {
            return Ok(());
        }
        let old = self.hard()?;
        let durable =
            !entries.is_empty() || hard.is_some_and(|h| h.term != old.term || h.vote != old.vote);
        let last = self.last()?;
        if let Some(first) = entries.first() {
            if first.index > last + 1 || entries.windows(2).any(|p| p[1].index != p[0].index + 1) {
                return Err(error("noncontiguous accepted suffix"));
            }
            if first.index <= old.commit {
                return Err(error("cannot replace committed entries"));
            }
        }
        if let Some(h) = hard {
            if h.term < old.term
                || h.commit < old.commit
                || h.commit > entries.last().map_or(last, |e| e.index)
                || (h.term == old.term && old.vote != 0 && h.vote != old.vote)
            {
                return Err(error("invalid durable hard state"));
            }
        }
        let removed = if let Some(first) = entries.first().filter(|first| first.index <= last) {
            self.conn
                .query_row(
                    "SELECT coalesce(sum(length(data)),0) FROM entries WHERE idx>=?1",
                    [sql(first.index)?],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(error)
                .and_then(|n| u64::try_from(n).map_err(error))?
        } else {
            0
        };
        let mut added = 0u64;
        crate::fault::point("before_cluster_persist");
        if durable {
            self.accept_journal(entries, hard.unwrap_or(&old))?;
        }
        let applied = self.commit_transaction(false, |tx| {
            if let Some(first) = entries.first() {
                tx.execute("DELETE FROM entries WHERE idx>=?1", [sql(first.index)?])
                    .map_err(error)?;
                for entry in entries {
                    let bytes = entry.write_to_bytes().map_err(error)?;
                    added = added
                        .checked_add(bytes.len() as u64)
                        .ok_or_else(|| error("log size overflow"))?;
                    tx.execute(
                        "INSERT INTO entries VALUES(?1,?2,?3,?4)",
                        params![
                            sql(entry.index)?,
                            sql(entry.term)?,
                            bytes,
                            crc32fast::hash(&bytes)
                        ],
                    )
                    .map_err(error)?;
                }
            }
            if let Some(h) = hard {
                tx.execute(
                    "INSERT OR REPLACE INTO cluster_meta VALUES('hard',?1)",
                    [h.write_to_bytes().map_err(error)?],
                )
                .map_err(error)?;
            }
            Ok(())
        });
        if let Err(failure) = applied {
            self.failed = true;
            return Err(failure);
        }
        self.entries_bytes = self
            .entries_bytes
            .checked_sub(removed)
            .and_then(|n| n.checked_add(added))
            .ok_or_else(|| error("invalid cached log size"))?;
        crate::fault::point("after_cluster_persist");
        Ok(())
    }
    pub fn apply(&mut self, commit: u64) -> io::Result<Vec<(Vec<u8>, u64)>> {
        let base = self
            .meta("view")?
            .map(|b| u64::from_le_bytes(b.try_into().unwrap_or([255; 8])))
            .unwrap_or(0);
        if base > commit {
            return Err(error("view ahead of commitment"));
        }
        if base == commit {
            return Ok(Vec::new());
        }
        let mut end = self.end()?;
        let mut decoded = Vec::new();
        for idx in base + 1..=commit {
            let entry = self.entry(idx)?;
            let batch = if entry.data.is_empty()
                || entry.get_entry_type() != raft::eraftpb::EntryType::EntryNormal
            {
                None
            } else {
                Some(Batch::decode(&entry.data)?)
            };
            decoded.push((entry, batch));
        }
        self.commit_transaction(false, |tx| {
            let mut results = Vec::new();
            for (entry, batch) in decoded {
                if let Some(batch) = batch {
                    for (ordinal, _) in batch.records.iter().enumerate() {
                        end += 1;
                        tx.execute(
                            "INSERT INTO event_index VALUES(?1,?2,?3)",
                            params![sql(end)?, sql(entry.index)?, ordinal as i64],
                        )
                        .map_err(error)?;
                    }
                    for (ordinal, c) in batch.contents.iter().enumerate() {
                        tx.execute(
                            "INSERT INTO body_index VALUES(?1,?2,?3,?4)",
                            params![
                                sql(c.key.generation)?,
                                sql(c.key.sequence)?,
                                sql(entry.index)?,
                                ordinal as i64
                            ],
                        )
                        .map_err(error)?;
                    }
                }
                results.push((entry.context.to_vec(), end));
            }
            tx.execute(
                "INSERT OR REPLACE INTO cluster_meta VALUES('view',?1)",
                [commit.to_le_bytes().as_slice()],
            )
            .map_err(error)?;
            Ok(results)
        })
    }
    pub(super) fn begin_projection(&mut self) -> io::Result<()> {
        if self.projection_batch {
            return Err(error("nested projection batch"));
        }
        self.conn.execute_batch("BEGIN DEFERRED").map_err(error)?;
        self.projection_batch = true;
        Ok(())
    }
    pub(super) fn finish_projection(&mut self, commit: bool) -> io::Result<()> {
        if !self.projection_batch {
            return Ok(());
        }
        let result = self
            .conn
            .execute_batch(if commit { "COMMIT" } else { "ROLLBACK" })
            .map_err(error);
        self.projection_batch = false;
        if result.is_err() {
            self.failed = true;
            if !self.conn.is_autocommit() {
                let _ = self.conn.execute_batch("ROLLBACK");
            }
        }
        result
    }
    pub(super) fn commit_transaction<T>(
        &mut self,
        durable: bool,
        apply: impl FnOnce(&Connection) -> io::Result<T>,
    ) -> io::Result<T> {
        if !durable && self.projection_batch {
            return apply(&self.conn);
        }
        let resume = self.projection_batch;
        if resume {
            self.finish_projection(true)?;
        }
        if durable {
            self.conn
                .pragma_update(None, "synchronous", "FULL")
                .map_err(error)?;
        }
        let result = (|| {
            let tx = self.conn.transaction().map_err(error)?;
            let value = apply(&tx)?;
            tx.commit().map_err(error)?;
            Ok(value)
        })();
        // Only checkpoint roots need FULL SQLite persistence. Normal query
        // projections already have complete durable bytes in the journal.
        let restored = if durable {
            self.conn
                .pragma_update(None, "synchronous", "NORMAL")
                .map_err(error)
        } else {
            Ok(())
        };
        restored?;
        if resume && result.is_ok() {
            self.begin_projection()?;
        }
        result
    }
    pub fn record(&self, sequence: u64) -> io::Result<Option<Record>> {
        let pos: Option<(i64, i64)> = self
            .conn
            .query_row(
                "SELECT idx,ordinal FROM event_index WHERE seq=?1",
                [sql(sequence)?],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .map_err(error)?;
        pos.map(|(idx, n)| {
            let batch = Batch::decode(&self.entry(u64::try_from(idx).map_err(error)?)?.data)?;
            let r = batch
                .records
                .get(usize::try_from(n).map_err(error)?)
                .ok_or_else(|| error("invalid event index"))?;
            Ok(Record {
                sequence,
                subject: r.subject.clone(),
                headers: r.headers.clone(),
                payload: r.payload.clone(),
            })
        })
        .transpose()
    }
    pub fn content(&self, key: ContentKey) -> io::Result<Option<Content>> {
        let pos: Option<(i64, i64)> = self
            .conn
            .query_row(
                "SELECT idx,ordinal FROM body_index WHERE generation=?1 AND seq=?2",
                params![sql(key.generation)?, sql(key.sequence)?],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .map_err(error)?;
        if pos.is_none() {
            return self.snapshot_content(key);
        }
        pos.map(|(idx, n)| {
            let batch = Batch::decode(&self.entry(u64::try_from(idx).map_err(error)?)?.data)?;
            batch
                .contents
                .get(usize::try_from(n).map_err(error)?)
                .filter(|c| c.key == key)
                .cloned()
                .ok_or_else(|| error("invalid content identity"))
        })
        .transpose()
    }
}
fn raft_error(e: io::Error) -> raft::Error {
    raft::Error::Store(StorageError::Other(Box::new(e)))
}
impl Storage for LogStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        let db = self.db.lock().unwrap();
        Ok(RaftState {
            hard_state: db.hard().map_err(raft_error)?,
            conf_state: db.conf().map_err(raft_error)?,
        })
    }
    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        let db = self.db.lock().unwrap();
        let limit = max_size.into().unwrap_or(u64::MAX);
        let mut bytes = 0;
        let mut entries = Vec::new();
        if low <= db.snapshot_base().map_err(raft_error)?.0 {
            return Err(raft::Error::Store(StorageError::Compacted));
        }
        if high > db.last().map_err(raft_error)? + 1 {
            return Err(raft::Error::Store(StorageError::Unavailable));
        }
        for idx in low..high {
            let e = db.entry(idx).map_err(raft_error)?;
            bytes += u64::from(e.compute_size());
            if !entries.is_empty() && bytes > limit {
                break;
            }
            entries.push(e);
        }
        Ok(entries)
    }
    fn term(&self, idx: u64) -> raft::Result<u64> {
        let db = self.db.lock().unwrap();
        let (base, term, _) = db.snapshot_base().map_err(raft_error)?;
        if idx < base {
            return Err(raft::Error::Store(StorageError::Compacted));
        }
        if idx == base {
            return Ok(term);
        }
        db.entry(idx).map(|e| e.term).map_err(raft_error)
    }
    fn first_index(&self) -> raft::Result<u64> {
        Ok(self
            .db
            .lock()
            .unwrap()
            .snapshot_base()
            .map_err(raft_error)?
            .0
            + 1)
    }
    fn last_index(&self) -> raft::Result<u64> {
        self.db.lock().unwrap().last().map_err(raft_error)
    }
    fn snapshot(&self, index: u64, _: u64) -> raft::Result<Snapshot> {
        let db = self.db.lock().unwrap();
        if db.snapshot_base().map_err(raft_error)?.0 < index
            || db.snapshot_base().map_err(raft_error)?.0 == 0
        {
            return Err(raft::Error::Store(
                StorageError::SnapshotTemporarilyUnavailable,
            ));
        }
        db.snapshot().map_err(raft_error)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn cached_log_budget_survives_suffix_replacement_and_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let c = config();
        let mut db = Database::open(&path, &c, true).unwrap();
        let mut a = Entry {
            index: 1,
            term: 1,
            ..Default::default()
        };
        a.data = vec![7; 64].into();
        let mut b = Entry {
            index: 2,
            term: 1,
            ..Default::default()
        };
        b.data = vec![8; 128].into();
        db.persist(
            &[a.clone(), b.clone()],
            Some(&HardState {
                term: 1,
                vote: 1,
                commit: 0,
                ..Default::default()
            }),
        )
        .unwrap();
        assert_eq!(
            db.log_bytes().unwrap(),
            u64::from(a.compute_size() + b.compute_size())
        );
        b.term = 2;
        b.data = vec![9; 16].into();
        db.persist(
            &[b.clone()],
            Some(&HardState {
                term: 2,
                vote: 1,
                commit: 0,
                ..Default::default()
            }),
        )
        .unwrap();
        let expected = u64::from(a.compute_size() + b.compute_size());
        assert_eq!(db.log_bytes().unwrap(), expected);
        assert!(db
            .persist(
                &[Entry {
                    index: 4,
                    term: 2,
                    ..Default::default()
                }],
                None
            )
            .is_err());
        assert_eq!(db.log_bytes().unwrap(), expected);
        drop(db);
        assert_eq!(
            Database::open(&path, &c, false)
                .unwrap()
                .log_bytes()
                .unwrap(),
            expected
        );
    }

    use super::*;
    use crate::{
        cluster::{Batch, Peer},
        storage::{Append, Content},
    };
    use bytes::Bytes;
    pub(super) fn config() -> ClusterConfig {
        ClusterConfig {
            cluster_id: "test".into(),
            node_id: 1,
            secret: "x".repeat(32),
            max_log_bytes: 128 * 1024 * 1024,
            initial_voters: Vec::new(),
            peers: (1..=3)
                .map(|id| {
                    (
                        id,
                        Peer {
                            raft: format!("127.0.0.1:{}", 20000 + id * 3).parse().unwrap(),
                            client: format!("127.0.0.1:{}", 20001 + id * 3).parse().unwrap(),
                            service: format!("127.0.0.1:{}", 20002 + id * 3).parse().unwrap(),
                        },
                    )
                })
                .collect(),
        }
    }
    fn entry(index: u64, term: u64, body: &'static [u8]) -> Entry {
        let key = ContentKey {
            generation: 1,
            sequence: 1,
        };
        let batch = Batch {
            version: 1,
            records: vec![Append {
                subject: "event".into(),
                headers: Bytes::new(),
                payload: Bytes::new(),
            }],
            contents: vec![Content {
                key,
                subject: "subject".into(),
                headers: Bytes::new(),
                payload: Bytes::from_static(body),
            }],
        };
        Entry {
            index,
            term,
            data: postcard::to_allocvec(&batch).unwrap().into(),
            ..Default::default()
        }
    }
    #[test]
    fn conflicting_suffix_has_immutable_bytes_and_committed_only_visibility() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let c = config();
        let mut db = Database::open(&path, &c, true).unwrap();
        let mut hard = HardState {
            term: 1,
            vote: 1,
            ..Default::default()
        };
        db.persist(&[entry(1, 1, b"old")], Some(&hard)).unwrap();
        assert_eq!(db.end().unwrap(), 0);
        assert!(db
            .content(ContentKey {
                generation: 1,
                sequence: 1
            })
            .unwrap()
            .is_none());
        drop(db);
        let mut db = Database::open(&path, &c, true).unwrap();
        assert_eq!(db.end().unwrap(), 0);
        hard.term = 2;
        hard.vote = 2;
        hard.commit = 1;
        db.persist(&[entry(1, 2, b"replacement")], Some(&hard))
            .unwrap();
        db.apply(1).unwrap();
        assert_eq!(
            db.content(ContentKey {
                generation: 1,
                sequence: 1
            })
            .unwrap()
            .unwrap()
            .payload,
            "replacement"
        );
        assert!(db.persist(&[entry(1, 3, b"wrong")], None).is_err());
        drop(db);
        let db = Database::open(&path, &c, true).unwrap();
        assert_eq!(db.end().unwrap(), 1);
        assert_eq!(db.hard().unwrap(), hard);
    }
    #[test]
    fn votes_and_identity_fail_closed_while_journal_repairs_corrupt_projection() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let c = config();
        let mut db = Database::open(&path, &c, true).unwrap();
        let mut hard = HardState {
            term: 1,
            vote: 1,
            ..Default::default()
        };
        db.persist(&[entry(1, 1, b"body")], Some(&hard)).unwrap();
        hard.vote = 2;
        assert!(db.persist(&[], Some(&hard)).is_err());
        hard.vote = 1;
        hard.commit = 2;
        assert!(db.persist(&[], Some(&hard)).is_err());
        db.conn
            .execute("UPDATE entries SET data=x'000000'", [])
            .unwrap();
        drop(db);
        let repaired = Database::open(&path, &c, true).unwrap();
        assert_eq!(repaired.entry(1).unwrap(), entry(1, 1, b"body"));
        drop(repaired);
        let fresh = dir.path().join("fresh");
        drop(Database::open(&fresh, &c, true).unwrap());
        let mut other = c.clone();
        other.node_id = 2;
        assert!(Database::open(&fresh, &other, true).is_err());
        std::fs::remove_file(&fresh).unwrap();
        assert!(Database::open(&fresh, &c, true).is_err());
    }
}
