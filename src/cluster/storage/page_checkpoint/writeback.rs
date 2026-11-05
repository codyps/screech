//! Start WAL writeback before SQLite's checkpoint needs to sync a large dirty
//! range. This hint proves nothing durable: accepted-journal syncs, SQLite WAL
//! checkpoint ordering and FULL snapshot-root commits remain mandatory.
use crate::cluster::Input;
use std::{fs::File, io, os::fd::AsRawFd, path::PathBuf, sync::mpsc, time::Duration};

pub(super) struct Writeback {
    stop: mpsc::Sender<()>,
}

fn queue(file: &File) -> io::Result<bool> {
    // SAFETY: the live File owns this descriptor. Zero offset/length means the
    // complete file; WRITE queues dirty pages without requesting a sync fence.
    // Never use this result to acknowledge data or advance durable positions.
    if unsafe { libc::sync_file_range(file.as_raw_fd(), 0, 0, libc::SYNC_FILE_RANGE_WRITE) } == 0 {
        return Ok(true);
    }
    let failure = io::Error::last_os_error();
    if matches!(
        failure.raw_os_error(),
        Some(libc::ENOSYS | libc::EOPNOTSUPP | libc::EINVAL | libc::ESPIPE)
    ) {
        Ok(false)
    } else {
        Err(failure)
    }
}

impl Writeback {
    pub(super) fn start(path: PathBuf, fault: mpsc::SyncSender<Input>) -> io::Result<Self> {
        let (stop, stopped) = mpsc::channel();
        std::thread::Builder::new()
            .name("screech-wal-writeback".into())
            .spawn(move || {
                let mut file = None;
                let mut written = None;
                while let Err(mpsc::RecvTimeoutError::Timeout) =
                    stopped.recv_timeout(Duration::from_millis(10))
                {
                    let result = (|| -> io::Result<bool> {
                        if file.is_none() {
                            match File::options().write(true).open(&path) {
                                Ok(opened) => file = Some(opened),
                                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                                    return Ok(true)
                                }
                                Err(error) => return Err(error),
                            }
                        }
                        // Retained WAL capacity alone is not activity. A write
                        // updates mtime even when SQLite reuses the same inode
                        // and length. Equality is just a hint, never an epoch.
                        let file = file.as_ref().unwrap();
                        let modified = file.metadata()?.modified()?;
                        if written == Some(modified) {
                            return Ok(true);
                        }
                        let supported = queue(file)?;
                        written = Some(modified);
                        Ok(supported)
                    })();
                    match result {
                        Ok(true) => (),
                        Ok(false) => break, // Unsupported optimization; keep all syncs.
                        Err(error)
                            if matches!(
                                error.kind(),
                                io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
                            ) => {}
                        Err(error) => {
                            tracing::error!(%error,"SQLite WAL writeback failed; fencing voter");
                            let _ = fault.send(Input::Fault);
                            break;
                        }
                    }
                }
            })?;
        Ok(Self { stop })
    }
}

impl Drop for Writeback {
    fn drop(&mut self) {
        // A stuck old-generation disk must not prevent quorum repair. The file
        // descriptor pins that generation; recovery never reuses its path.
        let _ = self.stop.send(());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn writeback_open_errors_fence_the_voter() {
        let directory = tempfile::tempdir().unwrap();
        let (fault, failed) = mpsc::sync_channel(1);
        let _worker = Writeback::start(directory.path().to_owned(), fault).unwrap();
        assert!(matches!(
            failed.recv_timeout(Duration::from_secs(2)).unwrap(),
            Input::Fault
        ));
    }
    #[test]
    fn missing_wal_can_appear_later_and_unsupported_files_keep_the_sync_fallback() {
        use std::io::Write;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("wal");
        let (fault, failed) = mpsc::sync_channel(1);
        let worker = Writeback::start(path.clone(), fault).unwrap();
        std::thread::sleep(Duration::from_millis(30));
        let mut file = File::create(&path).unwrap();
        file.write_all(&[7; 8192]).unwrap();
        std::thread::sleep(Duration::from_millis(30));
        assert!(matches!(failed.try_recv(), Err(mpsc::TryRecvError::Empty)));
        assert_eq!(std::fs::read(path).unwrap(), [7; 8192]);
        drop(worker);
        let unsupported = File::options().write(true).open("/dev/null").unwrap();
        assert!(!queue(&unsupported).unwrap());
    }
}
