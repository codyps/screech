//! Passive SQLite page checkpoints use their own connection. Accepted journals
//! remain synchronous; the primary connection retains its automatic checkpoint
//! limit as a fallback. No application tables are changed by this worker.
use super::*;
use crate::cluster::Input;
use rusqlite::{ErrorCode, OpenFlags};
use std::{path::PathBuf, sync::mpsc, time::Duration};
#[cfg(target_os = "linux")]
mod writeback;

// Small, regular passes avoid a large dirty WAL flush delaying the accepted
// journals on the same device. Activity still gates retained WAL capacity.
const PROJECTION_CHANGE_BUDGET: u64 = 1024;
const MINIMUM_WAL_FRAMES: i64 = 2048;

fn progress(conn: &Connection, pragma: &str) -> io::Result<Option<(i64, i64, i64)>> {
    match conn.query_row(pragma, [], |row| {
        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
    }) {
        Ok(stats) => Ok(Some(stats)),
        Err(e)
            if matches!(
                e.sqlite_error_code(),
                Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
            ) =>
        {
            Ok(None)
        }
        Err(e) => Err(error(e)),
    }
}

pub(crate) struct PageCheckpointer {
    stop: mpsc::Sender<()>,
    #[cfg(target_os = "linux")]
    _writeback: writeback::Writeback,
}
impl PageCheckpointer {
    pub fn start(
        path: PathBuf,
        changes: Arc<AtomicU64>,
        fault: mpsc::SyncSender<Input>,
    ) -> io::Result<Self> {
        let conn = Connection::open_with_flags(
            &path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(error)?;
        conn.busy_timeout(Duration::ZERO).map_err(error)?;
        conn.pragma_update(None, "synchronous", "FULL")
            .map_err(error)?;
        let page_size: u64 = conn
            .pragma_query_value(None, "page_size", |r| r.get::<_, i64>(0))
            .map_err(error)?
            .try_into()
            .map_err(error)?;
        let minimum_wal_bytes = page_size * MINIMUM_WAL_FRAMES as u64;
        let mut wal = path.into_os_string();
        wal.push("-wal");
        let wal = PathBuf::from(wal);
        #[cfg(target_os = "linux")]
        let writeback = writeback::Writeback::start(wal.clone(), fault.clone())?;
        let (stop, stopped) = mpsc::channel();
        let mut checkpointed = changes.load(Ordering::Acquire);
        let mut pending = false;
        std::thread::Builder::new()
            .name("screech-page-checkpoint".into())
            .spawn(move || {
                while let Err(mpsc::RecvTimeoutError::Timeout) =
                    stopped.recv_timeout(Duration::from_millis(100))
                {
                    let result = (|| -> io::Result<()> {
                        let current = changes.load(Ordering::Acquire);
                        if current.saturating_sub(checkpointed) < PROJECTION_CHANGE_BUDGET {
                            return Ok(());
                        }
                        if std::fs::metadata(&wal)?.len() < minimum_wal_bytes {
                            return Ok(());
                        }
                        if !pending {
                            let Some((busy, frames, backfilled)) =
                                progress(&conn, "PRAGMA wal_checkpoint(NOOP)")?
                            else {
                                return Ok(());
                            };
                            if busy != 0 {
                                return Ok(());
                            }
                            if frames == backfilled {
                                checkpointed = current;
                                return Ok(());
                            }
                            // SQLite retains WAL capacity across reuse. Temp
                            // index updates count as changes too, so neither
                            // that capacity nor the row budget measures dirty
                            // permanent pages. NOOP never performs a checkpoint.
                            if frames.saturating_sub(backfilled) < MINIMUM_WAL_FRAMES {
                                return Ok(());
                            }
                            pending = true;
                        }
                        if let Some((busy, frames, backfilled)) =
                            progress(&conn, "PRAGMA wal_checkpoint(PASSIVE)")?
                        {
                            if busy == 0 && frames == backfilled {
                                // Once started, finish partial passes even if a
                                // reader leaves fewer than the threshold dirty.
                                // Concurrent commits retain their next budget.
                                checkpointed = current;
                                pending = false;
                            }
                        }
                        Ok(())
                    })();
                    if let Err(failure) = result {
                        tracing::error!(%failure,"SQLite page checkpoint failed; fencing voter");
                        let _ = fault.send(Input::Fault);
                        break;
                    }
                }
            })?;
        Ok(Self {
            stop,
            #[cfg(target_os = "linux")]
            _writeback: writeback,
        })
    }
}

impl Drop for PageCheckpointer {
    fn drop(&mut self) {
        // Never hold runtime recovery behind a hung disk call. This worker owns
        // only its SQLite connection to the old generation; SQLite coordinates
        // page checkpoints, and generation paths are never reused for repair.
        let _ = self.stop.send(());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{io::Read, time::Instant};

    fn salt(path: &Path) -> [u8; 8] {
        let mut header = [0; 32];
        File::open(format!("{}-wal", path.display()))
            .unwrap()
            .read_exact(&mut header)
            .unwrap();
        header[16..24].try_into().unwrap()
    }

    #[test]
    fn reused_wal_capacity_does_not_flush_small_suffixes_after_temp_index_work() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "PRAGMA page_size=1024; PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL; PRAGMA wal_autocheckpoint=0;
            CREATE TABLE t(id INTEGER PRIMARY KEY, body BLOB);
            INSERT INTO t VALUES(1, zeroblob(9437184));
            PRAGMA wal_checkpoint(PASSIVE);
            INSERT INTO t VALUES(2, x'01');
            CREATE TEMP TABLE idx(id INTEGER PRIMARY KEY);
            WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<1024)
            INSERT INTO idx SELECT x FROM n;",
        )
        .unwrap();
        let progress = || {
            conn.query_row("PRAGMA wal_checkpoint(NOOP)", [], |row| {
                Ok((row.get::<_, i64>(1)?, row.get::<_, i64>(2)?))
            })
            .unwrap()
        };
        let (frames, backfilled) = progress();
        assert!(frames > backfilled && frames < 2048);
        assert!(
            std::fs::metadata(format!("{}-wal", path.display()))
                .unwrap()
                .len()
                > 8 * 1024 * 1024
        );
        let changes = Arc::new(AtomicU64::new(0));
        let (tx, rx) = mpsc::sync_channel(1);
        let _worker = PageCheckpointer::start(path.clone(), changes.clone(), tx).unwrap();
        changes.store(conn.total_changes(), Ordering::Release);
        std::thread::sleep(Duration::from_millis(500));
        let (frames, backfilled) = progress();
        assert!(
            frames > backfilled,
            "retained WAL capacity checkpointed a small suffix after temp-index work"
        );
        // Useful permanent WAL work must still trigger the background pass.
        conn.execute_batch(
            "WITH RECURSIVE n(x) AS (VALUES(3) UNION ALL SELECT x+1 FROM n WHERE x<1026)
            INSERT INTO t SELECT x, zeroblob(3072) FROM n;",
        )
        .unwrap();
        changes.store(conn.total_changes(), Ordering::Release);
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let (frames, backfilled) = progress();
            if frames > 0 && frames == backfilled {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "dirty WAL budget did not checkpoint"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
    }

    #[test]
    fn small_page_projections_checkpoint_before_eight_megabytes_accumulate() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "PRAGMA page_size=1024; PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL; PRAGMA wal_autocheckpoint=0;
            CREATE TABLE t(id INTEGER PRIMARY KEY, body BLOB);
            INSERT INTO t VALUES(1, zeroblob(2621440));",
        )
        .unwrap();
        let wal_bytes = std::fs::metadata(format!("{}-wal", path.display()))
            .unwrap()
            .len();
        assert!(wal_bytes > 2 * 1024 * 1024 && wal_bytes < 8 * 1024 * 1024);
        let changes = Arc::new(AtomicU64::new(0));
        let (tx, rx) = mpsc::sync_channel(1);
        let _worker = PageCheckpointer::start(path.clone(), changes.clone(), tx).unwrap();
        changes.store(PROJECTION_CHANGE_BUDGET, Ordering::Release);
        let deadline = Instant::now() + Duration::from_secs(5);
        while std::fs::metadata(&path).unwrap().len() < 2 * 1024 * 1024 {
            assert!(
                Instant::now() < deadline,
                "small-page checkpoint did not run"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
    }
    #[test]
    fn partial_checkpoint_finishes_after_reader_release_without_new_writes() {
        partial_checkpoint_after_reader_release(0);
    }
    #[test]
    fn partial_checkpoint_retries_a_remaining_suffix_below_the_dirty_frame_budget() {
        partial_checkpoint_after_reader_release(900);
    }
    fn partial_checkpoint_after_reader_release(pinned: i64) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "PRAGMA page_size=1024; PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL; PRAGMA wal_autocheckpoint=0;
             CREATE TABLE t(id INTEGER PRIMARY KEY, body BLOB);
             INSERT INTO t VALUES(0, x'01');",
        )
        .unwrap();
        if pinned > 0 {
            conn.execute(
                "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<?1)
                INSERT INTO t SELECT i, zeroblob(3072) FROM n",
                [pinned],
            )
            .unwrap();
        }
        let reader = Connection::open(&path).unwrap();
        reader.execute_batch("BEGIN").unwrap();
        assert_eq!(
            reader
                .query_row("SELECT count(*) FROM t", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            pinned + 1
        );
        let changes = Arc::new(AtomicU64::new(0));
        let (tx, rx) = mpsc::sync_channel(1);
        let _worker = PageCheckpointer::start(path.clone(), changes.clone(), tx).unwrap();
        conn.execute(
            "WITH RECURSIVE n(i) AS (VALUES(?1) UNION ALL SELECT i+1 FROM n WHERE i<1024)
             INSERT INTO t SELECT i, zeroblob(3072) FROM n",
            [pinned + 1],
        )
        .unwrap();
        let committed = conn.total_changes();
        changes.store(committed, Ordering::Release);
        let progress = || {
            conn.query_row("PRAGMA wal_checkpoint(NOOP)", [], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })
            .unwrap()
        };
        let until = Instant::now() + Duration::from_secs(5);
        loop {
            let (busy, frames, backfilled) = progress();
            if busy == 0 && backfilled > 0 && backfilled < frames {
                if pinned > 0 {
                    assert!(frames - backfilled < MINIMUM_WAL_FRAMES);
                }
                break;
            }
            assert!(
                Instant::now() < until,
                "reader did not hold a partial checkpoint"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        reader.execute_batch("ROLLBACK").unwrap();
        let until = Instant::now() + Duration::from_secs(5);
        loop {
            let (busy, frames, backfilled) = progress();
            if busy == 0 && frames > 0 && backfilled == frames {
                break;
            }
            assert!(
                Instant::now() < until,
                "partial checkpoint was abandoned after reader release"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(changes.load(Ordering::Acquire), committed);
        assert!(matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
    }
    #[test]
    fn retained_wal_capacity_does_not_trigger_checkpoints_without_projection_work() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;
            PRAGMA wal_autocheckpoint=0; CREATE TABLE t(id INTEGER PRIMARY KEY, body BLOB);
            INSERT INTO t VALUES(1, zeroblob(9437184));",
        )
        .unwrap();
        let changes = Arc::new(AtomicU64::new(0));
        let (tx, rx) = mpsc::sync_channel(1);
        let _worker = PageCheckpointer::start(path.clone(), changes.clone(), tx).unwrap();
        let initial = salt(&path);
        std::thread::sleep(Duration::from_millis(500));
        conn.execute("INSERT INTO t VALUES(2, x'01')", []).unwrap();
        assert_eq!(
            salt(&path),
            initial,
            "retained WAL capacity caused an unnecessary restart"
        );

        changes.store(PROJECTION_CHANGE_BUDGET, Ordering::Release);
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            // File growth precedes completion of checkpoint I/O. Observe the
            // completed backfill instead of assuming a fixed sleep is enough.
            // NOOP reports progress without checkpointing any frames itself.
            let (busy, frames, backfilled): (i64, i64, i64) = conn
                .query_row("PRAGMA wal_checkpoint(NOOP)", [], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })
                .unwrap();
            if busy == 0 && frames > 0 && backfilled == frames {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "projection budget did not trigger a checkpoint"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        conn.execute("INSERT INTO t VALUES(3, x'02')", []).unwrap();
        assert_ne!(
            salt(&path),
            initial,
            "completed checkpoint did not permit WAL reuse"
        );
        assert!(matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
    }
}
