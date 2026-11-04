//! Passive SQLite page checkpoints use their own connection. Accepted journals
//! remain synchronous; the primary connection retains its automatic checkpoint
//! limit as a fallback. No application tables are changed by this worker.
use super::*;
use crate::cluster::Input;
use rusqlite::{ErrorCode, OpenFlags};
use std::{path::PathBuf, sync::mpsc, time::Duration};

pub(crate) struct PageCheckpointer {
    stop: mpsc::Sender<()>,
}
impl PageCheckpointer {
    pub fn start(path: PathBuf, fault: mpsc::SyncSender<Input>) -> io::Result<Self> {
        let conn = Connection::open_with_flags(
            &path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(error)?;
        conn.busy_timeout(Duration::ZERO).map_err(error)?;
        conn.pragma_update(None, "synchronous", "FULL")
            .map_err(error)?;
        let mut wal = path.into_os_string();
        wal.push("-wal");
        let wal = PathBuf::from(wal);
        let (stop, stopped) = mpsc::channel();
        std::thread::Builder::new()
            .name("screech-page-checkpoint".into())
            .spawn(move || {
                while let Err(mpsc::RecvTimeoutError::Timeout) =
                    stopped.recv_timeout(Duration::from_millis(100))
                {
                    let result = (|| -> io::Result<()> {
                        if std::fs::metadata(&wal)?.len() < 8 * 1024 * 1024 {
                            return Ok(());
                        }
                        match conn.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
                            Ok((
                                row.get::<_, i64>(0)?,
                                row.get::<_, i64>(1)?,
                                row.get::<_, i64>(2)?,
                            ))
                        }) {
                            Ok(_) => Ok(()),
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
        Ok(Self { stop })
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
