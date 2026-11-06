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
        let minimum_wal_bytes = page_size * 2048;
        let mut wal = path.into_os_string();
        wal.push("-wal");
        let wal = PathBuf::from(wal);
        #[cfg(target_os = "linux")]
        let writeback = writeback::Writeback::start(wal.clone(), fault.clone())?;
        let (stop, stopped) = mpsc::channel();
        let mut checkpointed = changes.load(Ordering::Acquire);
        std::thread::Builder::new()
            .name("screech-page-checkpoint".into())
            .spawn(move || {
                while let Err(mpsc::RecvTimeoutError::Timeout) =
                    stopped.recv_timeout(Duration::from_millis(10))
                {
                    let result = (|| -> io::Result<()> {
                        let current = changes.load(Ordering::Acquire);
                        if current.saturating_sub(checkpointed) < PROJECTION_CHANGE_BUDGET {
                            return Ok(());
                        }
                        if std::fs::metadata(&wal)?.len() < minimum_wal_bytes {
                            return Ok(());
                        }
                        match conn.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
                            Ok((
                                row.get::<_, i64>(0)?,
                                row.get::<_, i64>(1)?,
                                row.get::<_, i64>(2)?,
                            ))
                        }) {
                            Ok((busy, _, _)) => {
                                if busy == 0 {
                                    // Count work observed before this checkpoint.
                                    // Concurrent commits remain in the next budget.
                                    checkpointed = current;
                                }
                                Ok(())
                            }
                            Err(e)
                                if matches!(
                                    e.sqlite_error_code(),
                                    Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
                                ) =>
                            {
                                Ok(())
                            }
                            Err(e) => Err(error(e)),
                        }
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
