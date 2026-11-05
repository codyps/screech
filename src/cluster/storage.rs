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
    cell::Cell,
    fs::{File, OpenOptions},
    io::{self, Write},
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
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
    accepted_tip: Option<(u64, u64)>,
    // Actor-local invalidation token, never a durability or recovery witness.
    pub(super) membership_revision: u64,
    projection_batch: bool,
    projection_transaction: Cell<bool>,
    projection_eager: bool,
    pub(super) projection_changes: Arc<AtomicU64>,
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
        if !existed {
            // Raft appends and small metadata updates otherwise dirty several
            // 4 KiB WAL pages for a small publication. Existing roots retain
            // their page size; no rewrite or migration is needed to reopen.
            conn.pragma_update(None, "page_size", 1024).map_err(error)?;
        }
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(error)?;
        conn.pragma_update(None, "synchronous", "FULL")
            .map_err(error)?;
        // Bound the replayable query WAL independently of the accepted journal.
        // Amortize checkpoint/header syncs over useful projection work instead
        // of repeatedly syncing a reused WAL's retained capacity.
        conn.pragma_update(None, "wal_autocheckpoint", 32768)
            .map_err(error)?;
        let page_size: i64 = conn
            .pragma_query_value(None, "page_size", |r| r.get(0))
            .map_err(error)?;
        conn.pragma_update(None, "journal_size_limit", page_size * 32768)
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
        // These lookups are rebuilt from committed entries and the FULL
        // snapshot before voting. Keeping them out of the durable WAL avoids
        // writing several pages of disposable indexes for every publication.
        // Bound their cache; larger indexes can spill to a private temp file.
        conn.execute_batch("PRAGMA temp_store=FILE;
          PRAGMA temp.journal_mode=MEMORY;
          PRAGMA temp.cache_size=-4096;
          CREATE TEMP TABLE event_index(seq INTEGER PRIMARY KEY,idx INTEGER NOT NULL,ordinal INTEGER NOT NULL);
          CREATE TEMP TABLE body_index(generation INTEGER NOT NULL,seq INTEGER NOT NULL,idx INTEGER NOT NULL,ordinal INTEGER NOT NULL,PRIMARY KEY(generation,seq)) WITHOUT ROWID;")
            .map_err(error)?;
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
            accepted_tip: None,
            membership_revision: 0,
            projection_batch: false,
            projection_transaction: Cell::new(false),
            projection_eager: false,
            projection_changes: Arc::new(AtomicU64::new(0)),
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
        let (base, _, sequence) = db.snapshot_base()?;
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
        db.conn.execute_batch("DELETE FROM main.event_index;DELETE FROM main.body_index;
          DELETE FROM temp.event_index;DELETE FROM temp.body_index;DELETE FROM cluster_meta WHERE key='view';").map_err(error)?;
        db.conn.execute(
            "INSERT INTO cluster_meta VALUES('event_sequence',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            [sequence.to_le_bytes().as_slice()],
        ).map_err(error)?;
        db.conn
            .execute(
                "INSERT INTO cluster_meta VALUES('view',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
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
        // Populate only after validating the complete independent SQL witness
        // against the accepted journal. Verification always reads SQL directly.
        db.accepted_tip = Some((
            last,
            if last == base {
                db.snapshot_position.1
            } else {
                db.entry(last)?.term
            },
        ));
        Ok(db)
    }
    pub fn meta(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        self.projection_sql()?;
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
    pub(super) fn durable_term(&self) -> io::Result<u64> {
        if self.failed {
            return Err(error("replica quarantined"));
        }
        // This is the canonical journal promise, installed only after sync and
        // verified against the independent SQL witness before opening a voter.
        self.watermark.durable_term(self.incarnation)
    }
    pub fn last(&self) -> io::Result<u64> {
        self.projection_sql()?;
        self.conn
            .prepare_cached("SELECT coalesce(max(idx),0) FROM entries")
            .map_err(error)?
            .query_row([], |r| r.get::<_, i64>(0))
            .map_err(error)
            .and_then(|n| u64::try_from(n).map_err(error))
            .and_then(|n| Ok(n.max(self.snapshot_base()?.0)))
    }
    pub fn end(&self) -> io::Result<u64> {
        self.projection_sql()?;
        self.conn
            .prepare_cached("SELECT coalesce(max(seq),0) FROM event_index")
            .map_err(error)?
            .query_row([], |r| r.get::<_, i64>(0))
            .map_err(error)
            .and_then(|n| u64::try_from(n).map_err(error))
            .and_then(|n| Ok(n.max(self.snapshot_base()?.2)))
    }
    pub fn log_bytes(&self) -> io::Result<u64> {
        Ok(self.entries_bytes)
    }
    pub(super) fn checkpoint_pressure(&self, configuration: u64) -> io::Result<bool> {
        if self.failed {
            return Err(error("replica quarantined"));
        }
        // These verified caches are a conservative scheduling hint, not a
        // snapshot position or recovery witness. An accepted suffix may request
        // an early checkpoint; compact() still checks the actual committed SQL
        // position and retains the full accepted suffix under the storage gate.
        Ok(self.entries_bytes > self.max_log_bytes / 2
            || self.journal_bytes() > self.max_log_bytes / 2
            || self
                .accepted_tip
                .is_none_or(|(last, _)| last.saturating_sub(self.snapshot_position.0) >= 8192)
            || configuration > self.snapshot_position.0)
    }
    fn refresh_log_bytes(&mut self) -> io::Result<()> {
        self.projection_sql()?;
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
        self.projection_sql()?;
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
    fn log_term(&self, idx: u64) -> io::Result<u64> {
        if self.failed {
            return Err(error("replica quarantined"));
        }
        if let Some((last, term)) = self.accepted_tip {
            if idx == last {
                return Ok(term);
            }
        }
        self.entry(idx).map(|e| e.term)
    }
    fn log_last_index(&self) -> io::Result<u64> {
        if self.failed {
            return Err(error("replica quarantined"));
        }
        self.accepted_tip
            .map_or_else(|| self.last(), |(last, _)| Ok(last))
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
                if first.index <= last {
                    tx.prepare_cached("DELETE FROM entries WHERE idx>=?1")
                        .map_err(error)?
                        .execute([sql(first.index)?])
                        .map_err(error)?;
                }
                for entry in entries {
                    let bytes = entry.write_to_bytes().map_err(error)?;
                    added = added
                        .checked_add(bytes.len() as u64)
                        .ok_or_else(|| error("log size overflow"))?;
                    tx.prepare_cached("INSERT INTO entries VALUES(?1,?2,?3,?4)")
                        .map_err(error)?
                        .execute(params![
                            sql(entry.index)?,
                            sql(entry.term)?,
                            bytes,
                            crc32fast::hash(&bytes)
                        ])
                        .map_err(error)?;
                }
            }
            if let Some(h) = hard {
                tx.prepare_cached("INSERT INTO cluster_meta VALUES('hard',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value")
                    .map_err(error)?
                    .execute([h.write_to_bytes().map_err(error)?])
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
        if let Some(last) = entries.last() {
            self.accepted_tip = Some((last.index, last.term));
        }
        crate::fault::point("after_cluster_persist");
        Ok(())
    }
    pub fn apply(&mut self, commit: u64) -> io::Result<Vec<(Vec<u8>, u64)>> {
        let base = self.view()?;
        let entries = (base..commit)
            .map(|idx| self.entry(idx + 1))
            .collect::<io::Result<Vec<_>>>()?;
        self.apply_entries(commit, entries)
    }
    /// The owner already read and checksum-verified this exact prefix while
    /// applying configuration changes in the same gated SQL transaction.
    pub(super) fn apply_entries(
        &mut self,
        commit: u64,
        entries: Vec<Entry>,
    ) -> io::Result<Vec<(Vec<u8>, u64)>> {
        let base = self.view()?;
        if base > commit {
            return Err(error("view ahead of commitment"));
        }
        if entries.len() as u64 != commit - base
            || entries
                .iter()
                .enumerate()
                .any(|(n, e)| e.index != base + n as u64 + 1)
        {
            return Err(error("application prefix does not match committed view"));
        }
        if base == commit {
            return Ok(Vec::new());
        }
        let mut end = self.end()?;
        let mut decoded = Vec::new();
        for entry in entries {
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
                        tx.prepare_cached("INSERT INTO event_index VALUES(?1,?2,?3)")
                            .map_err(error)?
                            .execute(params![sql(end)?, sql(entry.index)?, ordinal as i64])
                            .map_err(error)?;
                    }
                    for (ordinal, c) in batch.contents.iter().enumerate() {
                        tx.prepare_cached("INSERT INTO body_index VALUES(?1,?2,?3,?4)")
                            .map_err(error)?
                            .execute(params![
                                sql(c.key.generation)?,
                                sql(c.key.sequence)?,
                                sql(entry.index)?,
                                ordinal as i64
                            ])
                            .map_err(error)?;
                    }
                }
                results.push((entry.context.to_vec(), end));
            }
            tx.prepare_cached("INSERT INTO cluster_meta VALUES('view',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value")
                .map_err(error)?
                .execute([commit.to_le_bytes().as_slice()])
                .map_err(error)?;
            tx.prepare_cached("INSERT INTO cluster_meta VALUES('event_sequence',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value")
                .map_err(error)?
                .execute([end.to_le_bytes().as_slice()])
                .map_err(error)?;
            Ok(results)
        })
    }
    pub(super) fn begin_projection(&mut self) -> io::Result<()> {
        self.begin_lazy_projection()?;
        self.projection_eager = true;
        self.projection_sql()
    }
    pub(super) fn begin_lazy_projection(&mut self) -> io::Result<()> {
        if self.projection_batch || !self.conn.is_autocommit() {
            return Err(error("nested projection batch"));
        }
        self.projection_batch = true;
        self.projection_eager = false;
        self.projection_transaction.set(false);
        Ok(())
    }
    fn projection_sql(&self) -> io::Result<()> {
        if self.projection_batch && !self.projection_transaction.get() {
            self.conn.execute_batch("BEGIN DEFERRED").map_err(error)?;
            self.projection_transaction.set(true);
        }
        Ok(())
    }
    pub(super) fn finish_projection(&mut self, commit: bool) -> io::Result<()> {
        if !self.projection_batch {
            return Ok(());
        }
        let started = self.projection_transaction.get();
        let result = if started {
            self.conn
                .execute_batch(if commit { "COMMIT" } else { "ROLLBACK" })
                .map_err(error)
        } else {
            Ok(())
        };
        self.projection_batch = false;
        self.projection_transaction.set(false);
        self.projection_eager = false;
        if !commit || result.is_err() {
            self.accepted_tip = None;
        }
        if started && commit && result.is_ok() {
            self.projection_changes
                .store(self.conn.total_changes(), Ordering::Release);
        }
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
            self.projection_sql()?;
            return apply(&self.conn);
        }
        let resume = self.projection_batch;
        let eager = self.projection_eager;
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
        if result.is_ok() {
            self.projection_changes
                .store(self.conn.total_changes(), Ordering::Release);
        }
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
            if eager {
                self.begin_projection()?;
            } else {
                self.begin_lazy_projection()?;
            }
        }
        result
    }
    pub fn record(&self, sequence: u64) -> io::Result<Option<Record>> {
        self.projection_sql()?;
        let pos: Option<(i64, i64)> = self
            .conn
            .prepare_cached("SELECT idx,ordinal FROM event_index WHERE seq=?1")
            .map_err(error)?
            .query_row([sql(sequence)?], |r| Ok((r.get(0)?, r.get(1)?)))
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
        self.projection_sql()?;
        let pos: Option<(i64, i64)> = self
            .conn
            .prepare_cached("SELECT idx,ordinal FROM body_index WHERE generation=?1 AND seq=?2")
            .map_err(error)?
            .query_row(params![sql(key.generation)?, sql(key.sequence)?], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
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
    pub fn contents(&self, keys: &[ContentKey]) -> io::Result<Vec<Content>> {
        self.projection_sql()?;
        let mut output = vec![None; keys.len()];
        let mut positions = Vec::with_capacity(keys.len());
        let mut lookup = self
            .conn
            .prepare_cached("SELECT idx,ordinal FROM body_index WHERE generation=?1 AND seq=?2")
            .map_err(error)?;
        for (at, key) in keys.iter().copied().enumerate() {
            let position: Option<(i64, i64)> = lookup
                .query_row(params![sql(key.generation)?, sql(key.sequence)?], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })
                .optional()
                .map_err(error)?;
            match position {
                Some((index, ordinal)) => positions.push((
                    u64::try_from(index).map_err(error)?,
                    usize::try_from(ordinal).map_err(error)?,
                    key,
                    at,
                )),
                None => {
                    output[at] = Some(
                        self.snapshot_content(key)?
                            .ok_or_else(|| error("missing committed content"))?,
                    )
                }
            }
        }
        // Grouped publications share an entry. Decode it once, retaining only
        // one decoded batch at a time; preserve caller order and repeated keys.
        positions.sort_unstable_by_key(|position| position.0);
        let mut cached: Option<(u64, Batch)> = None;
        for (index, ordinal, key, at) in positions {
            if cached.as_ref().is_none_or(|(cached, _)| *cached != index) {
                cached = Some((index, Batch::decode(&self.entry(index)?.data)?));
            }
            output[at] = Some(
                cached
                    .as_ref()
                    .unwrap()
                    .1
                    .contents
                    .get(ordinal)
                    .filter(|content| content.key == key)
                    .ok_or_else(|| error("invalid content identity"))?
                    .clone(),
            );
        }
        output
            .into_iter()
            .map(|content| content.ok_or_else(|| error("missing committed content")))
            .collect()
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
        db.log_term(idx).map_err(raft_error)
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
        self.db.lock().unwrap().log_last_index().map_err(raft_error)
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
    fn lazy_projection_only_opens_sql_for_mutations_and_keeps_rollback_atomic() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = Database::open(&dir.path().join("db"), &config(), true).unwrap();
        let changes = db.projection_changes.load(Ordering::Acquire);
        db.begin_lazy_projection().unwrap();
        assert!(db.conn.is_autocommit());
        db.persist(&[], None).unwrap();
        assert!(db.conn.is_autocommit());
        db.finish_projection(true).unwrap();
        assert_eq!(db.projection_changes.load(Ordering::Acquire), changes);
        db.begin_lazy_projection().unwrap();
        db.persist(
            &[entry(1, 1, b"accepted")],
            Some(&HardState {
                term: 1,
                vote: 1,
                commit: 1,
                ..Default::default()
            }),
        )
        .unwrap();
        db.apply(1).unwrap();
        assert!(!db.conn.is_autocommit());
        assert_eq!(db.end().unwrap(), 1);
        db.finish_projection(false).unwrap();
        assert!(db.conn.is_autocommit());
        assert_eq!(db.end().unwrap(), 0);
        assert_eq!(db.hard().unwrap().commit, 0);
        assert!(db
            .content(ContentKey {
                generation: 1,
                sequence: 1
            })
            .unwrap()
            .is_none());
        // SQL rollback did not roll back a voting promise already synchronized
        // in the canonical accepted journal. Recovery replays that full prefix.
        assert_eq!(db.durable_term().unwrap(), 1);
        drop(db);
        let db = Database::open(&dir.path().join("db"), &config(), false).unwrap();
        assert_eq!(db.hard().unwrap().commit, 1);
        assert_eq!(db.record(1).unwrap().unwrap().subject, "event");
    }

    #[test]
    fn full_transactions_resume_the_original_projection_scope() {
        for eager in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut db = Database::open(&dir.path().join("db"), &config(), true).unwrap();
            if eager {
                db.begin_projection().unwrap();
            } else {
                db.begin_lazy_projection().unwrap();
            }
            assert!(db.begin_lazy_projection().is_err());
            db.commit_transaction(true, |tx| {
                tx.execute("INSERT INTO cluster_meta VALUES('full-test',x'01')", [])
                    .map_err(error)?;
                Ok(())
            })
            .unwrap();
            assert!(db.projection_batch);
            assert_eq!(db.conn.is_autocommit(), !eager);
            assert!(db.begin_projection().is_err());
            db.finish_projection(true).unwrap();
            assert!(db.conn.is_autocommit());
            assert_eq!(db.meta("full-test").unwrap(), Some(vec![1]));
            db.failed = true;
            assert!(db.durable_term().is_err());
        }
    }

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
        assert_eq!(db.log_term(2).unwrap(), 1);
        assert_eq!(db.log_last_index().unwrap(), 2);
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
        assert_eq!(db.log_term(2).unwrap(), 2);
        assert_eq!(db.log_last_index().unwrap(), 2);
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
        let mut db = Database::open(&path, &c, false).unwrap();
        assert_eq!(db.log_bytes().unwrap(), expected);
        assert_eq!(db.log_term(2).unwrap(), 2);
        assert_eq!(db.log_last_index().unwrap(), 2);
        db.failed = true;
        assert!(db.log_term(2).is_err());
        assert!(db.log_last_index().is_err());
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
    fn bulk_content_preserves_order_duplicates_snapshot_bodies_and_committed_visibility() {
        use sha2::{Digest, Sha256};
        let dir = tempfile::tempdir().unwrap();
        let mut db = Database::open(&dir.path().join("db"), &config(), true).unwrap();
        let contents: Vec<_> = (1..=7)
            .map(|sequence| Content {
                key: ContentKey {
                    generation: 1,
                    sequence,
                },
                subject: format!("message.{sequence}"),
                headers: Bytes::from_static(b"binary\0header"),
                payload: Bytes::from(vec![sequence as u8; 16]),
            })
            .collect();
        let entries: Vec<_> = [0..3, 3..6, 6..7]
            .into_iter()
            .enumerate()
            .map(|(at, range)| {
                let mut record = entry(at as u64 + 1, 1, b"ignored");
                let mut batch = Batch::decode(&record.data).unwrap();
                batch.contents = contents[range].to_vec();
                record.data = postcard::to_allocvec(&batch).unwrap().into();
                record
            })
            .collect();
        db.persist(
            &entries,
            Some(&HardState {
                term: 1,
                vote: 1,
                commit: 2,
                ..Default::default()
            }),
        )
        .unwrap();
        db.apply(2).unwrap();
        let snapshot = Content {
            key: ContentKey {
                generation: 9,
                sequence: 1,
            },
            ..contents[0].clone()
        };
        let data = postcard::to_allocvec(&snapshot).unwrap();
        db.conn
            .execute(
                "INSERT INTO snapshot_bodies VALUES(?1,?2,?3,?4)",
                params![9, 1, &data, Sha256::digest(&data).as_slice()],
            )
            .unwrap();
        let expected = vec![
            contents[4].clone(),
            snapshot,
            contents[0].clone(),
            contents[4].clone(),
            contents[2].clone(),
        ];
        let keys: Vec<_> = expected.iter().map(|content| content.key).collect();
        let actual = db.contents(&keys).unwrap();
        assert_eq!(
            postcard::to_allocvec(&actual).unwrap(),
            postcard::to_allocvec(&expected).unwrap()
        );
        assert!(db.contents(&[]).unwrap().is_empty());
        assert!(
            db.contents(&[contents[6].key]).is_err(),
            "exposed accepted-only body"
        );
        db.conn
            .execute(
                "UPDATE body_index SET ordinal=1 WHERE generation=1 AND seq=1",
                [],
            )
            .unwrap();
        assert!(
            db.contents(&[contents[0].key]).is_err(),
            "accepted wrong body identity"
        );
        db.conn
            .execute("UPDATE snapshot_bodies SET digest=zeroblob(32)", [])
            .unwrap();
        assert!(
            db.contents(&[keys[1]]).is_err(),
            "accepted damaged snapshot body"
        );
    }
    #[test]
    fn existing_projection_layout_and_accepted_history_survive_smaller_page_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut db = Database::open(&path, &config(), true).unwrap();
        assert_eq!(
            db.conn
                .pragma_query_value(None, "page_size", |r| r.get::<_, i64>(0))
                .unwrap(),
            1024
        );
        db.persist(
            &[entry(1, 1, b"durable")],
            Some(&HardState {
                term: 1,
                vote: 1,
                commit: 1,
                ..Default::default()
            }),
        )
        .unwrap();
        db.apply(1).unwrap();
        drop(db);
        // Construct the legacy layout offline. The accepted journal and every
        // promise remain unchanged, so reopening must preserve this history.
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "PRAGMA synchronous=FULL; PRAGMA journal_mode=DELETE;
            PRAGMA page_size=4096; VACUUM;",
        )
        .unwrap();
        drop(conn);
        let db = Database::open(&path, &config(), false).unwrap();
        assert_eq!(
            db.conn
                .pragma_query_value(None, "page_size", |r| r.get::<_, i64>(0))
                .unwrap(),
            4096
        );
        assert_eq!(db.end().unwrap(), 1);
        assert_eq!(db.hard().unwrap().commit, 1);
        assert_eq!(
            db.content(ContentKey {
                generation: 1,
                sequence: 1
            })
            .unwrap()
            .unwrap()
            .payload,
            "durable"
        );
    }
    #[test]
    fn raft_frontier_reads_follow_shorter_replacement_suffixes_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut db = Database::open(&path, &config(), true).unwrap();
        db.persist(
            &[entry(1, 1, b"first"), entry(2, 1, b"uncommitted")],
            Some(&HardState {
                term: 1,
                vote: 1,
                ..Default::default()
            }),
        )
        .unwrap();
        assert_eq!(db.log_last_index().unwrap(), 2);
        db.persist(
            &[entry(1, 2, b"replacement")],
            Some(&HardState {
                term: 2,
                vote: 1,
                ..Default::default()
            }),
        )
        .unwrap();
        assert_eq!(db.log_last_index().unwrap(), 1);
        assert_eq!(db.log_term(1).unwrap(), 2);
        assert!(db.log_term(2).is_err());
        drop(db);
        let db = Database::open(&path, &config(), false).unwrap();
        assert_eq!(db.log_last_index().unwrap(), 1);
        assert_eq!(db.log_term(1).unwrap(), 2);
        assert!(db.log_term(2).is_err());
    }
    #[test]
    fn reused_application_prefix_cannot_skip_committed_records_or_apply_them_twice() {
        let dir = tempfile::tempdir().unwrap();
        let mut db = Database::open(&dir.path().join("db"), &config(), true).unwrap();
        let first = entry(1, 1, b"body");
        let second = Entry {
            index: 2,
            term: 1,
            ..Default::default()
        };
        db.persist(
            &[first.clone(), second.clone()],
            Some(&HardState {
                term: 1,
                vote: 1,
                commit: 2,
                ..Default::default()
            }),
        )
        .unwrap();
        assert!(db.apply_entries(2, vec![second.clone()]).is_err());
        assert!(db.apply_entries(2, vec![first.clone()]).is_err());
        assert_eq!(db.view().unwrap(), 0);
        assert!(db.record(1).unwrap().is_none());
        let verified = vec![db.entry(1).unwrap(), db.entry(2).unwrap()];
        db.apply_entries(2, verified).unwrap();
        assert_eq!(db.view().unwrap(), 2);
        assert_eq!(db.end().unwrap(), 1);
        assert!(db.apply_entries(2, vec![first, second]).is_err());
        assert_eq!(db.end().unwrap(), 1);
    }
    #[test]
    fn disposable_indexes_rebuild_only_committed_content_and_rollback_with_projection() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let c = config();
        let key = ContentKey {
            generation: 1,
            sequence: 1,
        };
        let mut db = Database::open(&path, &c, true).unwrap();
        let mut hard = HardState {
            term: 1,
            vote: 1,
            commit: 1,
            ..Default::default()
        };
        let mut accepted = entry(2, 1, b"accepted-only");
        let mut batch = Batch::decode(&accepted.data).unwrap();
        batch.contents[0].key.sequence = 2;
        accepted.data = postcard::to_allocvec(&batch).unwrap().into();
        db.persist(&[entry(1, 1, b"committed"), accepted.clone()], Some(&hard))
            .unwrap();
        db.apply(1).unwrap();
        assert_eq!(db.content(key).unwrap().unwrap().payload, "committed");
        assert!(db.record(1).unwrap().is_some());
        for table in ["event_index", "body_index"] {
            assert_eq!(
                db.conn
                    .query_row(&format!("SELECT count(*) FROM main.{table}"), [], |r| r
                        .get::<_, i64>(0))
                    .unwrap(),
                0
            );
        }
        assert_eq!(
            db.meta("event_sequence").unwrap().unwrap(),
            1u64.to_le_bytes()
        );
        // A failed owner transaction must roll back both the temporary lookups
        // and their persisted diagnostic position.
        db.begin_projection().unwrap();
        db.apply(2).unwrap();
        assert_eq!(db.end().unwrap(), 2);
        db.finish_projection(false).unwrap();
        assert_eq!(db.end().unwrap(), 1);
        assert_eq!(db.content(key).unwrap().unwrap().payload, "committed");
        assert_eq!(
            db.meta("event_sequence").unwrap().unwrap(),
            1u64.to_le_bytes()
        );
        // Old binaries left persistent lookup tables. A stale cache must not
        // expose an accepted tail or survive migration to temporary lookups.
        db.conn
            .execute_batch(
                "INSERT INTO main.event_index VALUES(999,2,0);
            INSERT INTO main.body_index VALUES(99,99,2,0);",
            )
            .unwrap();
        drop(db);
        let mut db = Database::open(&path, &c, false).unwrap();
        assert_eq!(db.end().unwrap(), 1);
        assert!(db.record(2).unwrap().is_none());
        assert_eq!(db.content(key).unwrap().unwrap().payload, "committed");
        assert_eq!(
            db.conn
                .query_row("SELECT count(*) FROM main.event_index", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            db.conn
                .query_row("SELECT count(*) FROM main.body_index", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
        // The complete accepted tail remains independently present in SQLite.
        assert_eq!(db.entry(2).unwrap(), accepted);
        hard.commit = 2;
        db.persist(&[], Some(&hard)).unwrap();
        db.apply(2).unwrap();
        assert_eq!(db.end().unwrap(), 2);
        assert_eq!(
            db.content(ContentKey {
                generation: 1,
                sequence: 2
            })
            .unwrap()
            .unwrap()
            .payload,
            "accepted-only"
        );
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
