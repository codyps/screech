//! A restart fence, never an application recovery root. SQLite remains the only
//! log/data authority. Before releasing persistence-dependent protocol messages,
//! seal the voting/log history outside the replaceable database. A stale database
//! then fails closed even when no leader is reachable. A database ahead of its
//! seal is safe: a crash may occur between its FULL commit and publishing the seal.
use super::*;
use hmac::Mac;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
const DOMAIN: u64 = 0x57415445524d4152;
const LEGACY: &[u8] = b"screech-cluster-v1\n";
pub(super) const MARKER: &[u8] = b"screech-cluster-v2\n";
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Position {
    incarnation: [u8; 16],
    term: u64,
    vote: u64,
    commit: u64,
    last: u64,
    last_term: u64,
}
#[derive(Serialize, Deserialize)]
struct Signed {
    body: Vec<u8>,
    mac: Vec<u8>,
}
pub(super) struct Watermark {
    path: PathBuf,
    cluster: String,
    secret: String,
    member: u64,
    sealed: Option<Position>,
}
impl Watermark {
    pub fn open(
        path: &Path,
        config: &ClusterConfig,
        initialized: bool,
        marker: &[u8],
    ) -> io::Result<Self> {
        let path = path.with_extension("cluster-watermark");
        let sealed = match std::fs::read(&path) {
            Ok(bytes) => {
                let signed: Signed = serde_json::from_slice(&bytes).map_err(error)?;
                auth(
                    &config.secret,
                    &config.cluster_id,
                    config.node_id,
                    &signed.body,
                )
                .verify_slice(&signed.mac)
                .map_err(error)?;
                Some(serde_json::from_slice(&signed.body).map_err(error)?)
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                // One-time migration of the earlier build's explicit marker.
                // New stores always require their seal on subsequent starts.
                let legacy = !marker.is_empty() && marker.chunks(LEGACY.len()).all(|s| s == LEGACY);
                if initialized && !legacy {
                    return Err(error("missing cluster recovery watermark"));
                }
                None
            }
            Err(e) => return Err(e),
        };
        Ok(Self {
            path,
            cluster: config.cluster_id.clone(),
            secret: config.secret.clone(),
            member: config.node_id,
            sealed,
        })
    }
}
fn auth(secret: &str, cluster: &str, member: u64, body: &[u8]) -> hmac::Hmac<sha2::Sha256> {
    let mut mac = crate::cluster::authenticate(secret, cluster, DOMAIN, body);
    mac.update(&member.to_le_bytes());
    mac
}
impl Database {
    fn position(&self) -> io::Result<Position> {
        let hard = self.hard()?;
        let last = self.last()?;
        let (base, term, _) = self.snapshot_base()?;
        let last_term = if last == base {
            term
        } else {
            self.entry(last)?.term
        };
        Ok(Position {
            incarnation: self.incarnation,
            term: hard.term,
            vote: hard.vote,
            commit: hard.commit,
            last,
            last_term,
        })
    }
    pub(super) fn verify_watermark(&self) -> io::Result<()> {
        let Some(old) = &self.watermark.sealed else {
            return Ok(());
        };
        let current = self.position()?;
        if current.incarnation != old.incarnation
            || current.term < old.term
            || current.commit < old.commit
            || (current.term == old.term && old.vote != 0 && current.vote != old.vote)
        {
            return Err(error("cluster voting history was rolled back"));
        }
        let (base, _, _) = self.snapshot_base()?;
        // A newer accepted term may legitimately replace an uncommitted suffix.
        // Otherwise the promised suffix must remain, or be covered by a snapshot.
        if base < old.last
            && current.last_term <= old.last_term
            && (current.last < old.last || self.entry(old.last)?.term != old.last_term)
        {
            return Err(error("cluster accepted history was rolled back"));
        }
        Ok(())
    }
    pub(crate) fn seal(&mut self) -> io::Result<()> {
        let position = self.position()?;
        if self.watermark.sealed.as_ref() == Some(&position) {
            return Ok(());
        }
        let body = serde_json::to_vec(&position).map_err(error)?;
        let mac = auth(
            &self.watermark.secret,
            &self.watermark.cluster,
            self.watermark.member,
            &body,
        )
        .finalize()
        .into_bytes()
        .to_vec();
        let temporary = self.watermark.path.with_extension("cluster-watermark.tmp");
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temporary)?;
        file.write_all(&serde_json::to_vec(&Signed { body, mac }).map_err(error)?)?;
        file.sync_all()?;
        crate::fault::point("before_cluster_watermark");
        std::fs::rename(temporary, &self.watermark.path)?;
        File::open(self.watermark.path.parent().unwrap())?.sync_all()?;
        crate::fault::point("after_cluster_watermark");
        self.watermark.sealed = Some(position);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn config() -> ClusterConfig {
        super::super::tests::config()
    }
    fn entry(index: u64, term: u64) -> Entry {
        Entry {
            index,
            term,
            ..Default::default()
        }
    }
    fn hard(term: u64, commit: u64) -> HardState {
        HardState {
            term,
            vote: 1,
            commit,
            ..Default::default()
        }
    }
    fn backup(db: &Database, path: &Path) {
        db.conn
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
        std::fs::copy(path, path.with_extension("backup")).unwrap();
    }
    fn restore(path: &Path) {
        for suffix in ["-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
        }
        std::fs::copy(path.with_extension("backup"), path).unwrap();
    }
    #[test]
    fn consistent_database_rollback_cannot_forget_commits_votes_or_accepted_entries() {
        for kind in ["commit", "vote", "accepted"] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("store.db");
            let mut db = Database::open_mode(&path, &config(), true, false).unwrap();
            db.persist(&[entry(1, 1)], Some(&hard(1, 1))).unwrap();
            backup(&db, &path);
            match kind {
                "commit" => db.persist(&[entry(2, 1)], Some(&hard(1, 2))),
                "vote" => db.persist(&[], Some(&hard(2, 1))),
                _ => db.persist(&[entry(2, 1)], None),
            }
            .unwrap();
            drop(db);
            restore(&path);
            let result = Database::open_mode(&path, &config(), false, false);
            assert!(result.is_err(), "{kind} rollback resumed voting");
        }
    }
    #[test]
    fn sqlite_ahead_of_the_watermark_recovers_after_interrupted_seal_publication() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store.db");
        let mut db = Database::open_mode(&path, &config(), true, false).unwrap();
        db.persist(&[entry(1, 1)], Some(&hard(1, 1))).unwrap();
        let old = std::fs::read(path.with_extension("cluster-watermark")).unwrap();
        db.persist(&[entry(2, 1)], Some(&hard(1, 2))).unwrap();
        drop(db);
        std::fs::write(path.with_extension("cluster-watermark"), old).unwrap();
        let db = Database::open_mode(&path, &config(), false, false).unwrap();
        assert_eq!(db.hard().unwrap().commit, 2);
    }
    #[test]
    fn interrupted_new_term_suffix_replacement_does_not_look_like_database_rollback() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store.db");
        let mut db = Database::open_mode(&path, &config(), true, false).unwrap();
        db.persist(&[entry(1, 1), entry(2, 1), entry(3, 1)], Some(&hard(1, 1)))
            .unwrap();
        let old = std::fs::read(path.with_extension("cluster-watermark")).unwrap();
        db.persist(&[entry(2, 2)], Some(&hard(2, 2))).unwrap();
        drop(db);
        std::fs::write(path.with_extension("cluster-watermark"), old).unwrap();
        let db = Database::open_mode(&path, &config(), false, false).unwrap();
        assert_eq!(db.last().unwrap(), 2);
        assert_eq!(db.hard().unwrap().term, 2);
    }
    #[test]
    fn missing_or_corrupt_seal_requires_quorum_recovery() {
        for missing in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("store.db");
            drop(Database::open_mode(&path, &config(), true, false).unwrap());
            if missing {
                std::fs::remove_file(path.with_extension("cluster-watermark")).unwrap();
            } else {
                std::fs::write(path.with_extension("cluster-watermark"), b"broken").unwrap();
            }
            assert!(Database::open_mode(&path, &config(), false, false).is_err());
        }
    }
}
