//! A restart fence, never an application recovery root. SQLite remains the only
//! log/data authority. Before releasing persistence-dependent protocol messages,
//! seal the voting/log history outside the replaceable database. A stale database
//! then fails closed even when no leader is reachable. A database ahead of its
//! seal is safe: a crash may occur between its FULL commit and publishing the seal.
use super::*;
use hmac::Mac;
use serde::{Deserialize, Serialize};
use std::io::Seek;
use std::path::PathBuf;
const DOMAIN: u64 = 0x57415445524d4152;
const LEGACY: &[u8] = b"screech-cluster-v1\n";
pub(super) const MARKER: &[u8] = b"screech-cluster-v2\n";
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
const SLOT_BYTES: usize = 4096;
const SEAL_BYTES: usize = 2 * SLOT_BYTES;
const MAC_AT: usize = SLOT_BYTES - 32;
const SLOT_MAGIC: &[u8; 8] = b"SCSEAL03";
pub(super) struct Watermark {
    path: PathBuf,
    cluster: String,
    secret: String,
    member: u64,
    sealed: Option<Position>,
    file: Option<File>,
    generation: u64,
}
impl Watermark {
    pub fn open(
        path: &Path,
        config: &ClusterConfig,
        initialized: bool,
        marker: &[u8],
    ) -> io::Result<Self> {
        let path = path.with_extension("cluster-watermark");
        let mut watermark = Self {
            path,
            cluster: config.cluster_id.clone(),
            secret: config.secret.clone(),
            member: config.node_id,
            sealed: None,
            file: None,
            generation: 0,
        };
        match std::fs::read(&watermark.path) {
            Ok(bytes) if bytes.len() == SEAL_BYTES => {
                // Both records must authenticate. A damaged record might contain
                // a newer promise; choosing an older valid one could double-vote
                // or lose an acknowledged prefix. Quorum recovery repairs damage.
                let (left, a) = watermark.decode(&bytes[..SLOT_BYTES], 0)?;
                let (right, b) = watermark.decode(&bytes[SLOT_BYTES..], 1)?;
                if (left == right && (left != 0 || a != b)) || left.abs_diff(right) > 1 {
                    return Err(error("invalid recovery-seal generations"));
                }
                watermark.generation = left.max(right);
                watermark.sealed = Some(if left >= right { a } else { b });
                watermark.file = Some(
                    OpenOptions::new()
                        .read(true)
                        .write(true)
                        .open(&watermark.path)?,
                );
            }
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
                watermark.sealed = Some(serde_json::from_slice(&signed.body).map_err(error)?);
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                let legacy = !marker.is_empty() && marker.chunks(LEGACY.len()).all(|s| s == LEGACY);
                if initialized && !legacy {
                    return Err(error("missing cluster recovery watermark"));
                }
            }
            Err(e) => return Err(e),
        }
        Ok(watermark)
    }
    fn encode(
        &self,
        position: &Position,
        generation: u64,
        slot: u64,
    ) -> io::Result<[u8; SLOT_BYTES]> {
        let mut record = [0; SLOT_BYTES];
        record[..8].copy_from_slice(SLOT_MAGIC);
        record[8..16].copy_from_slice(&generation.to_le_bytes());
        record[16..24].copy_from_slice(&slot.to_le_bytes());
        let body = postcard::to_allocvec(position).map_err(error)?;
        if body.len() > MAC_AT - 28 {
            return Err(error("recovery position exceeds seal record"));
        }
        record[24..28].copy_from_slice(&(body.len() as u32).to_le_bytes());
        record[28..28 + body.len()].copy_from_slice(&body);
        let mac = auth(&self.secret, &self.cluster, self.member, &record[..MAC_AT])
            .finalize()
            .into_bytes();
        record[MAC_AT..].copy_from_slice(&mac);
        Ok(record)
    }
    fn decode(&self, bytes: &[u8], slot: u64) -> io::Result<(u64, Position)> {
        auth(&self.secret, &self.cluster, self.member, &bytes[..MAC_AT])
            .verify_slice(&bytes[MAC_AT..])
            .map_err(error)?;
        if &bytes[..8] != SLOT_MAGIC
            || u64::from_le_bytes(bytes[16..24].try_into().unwrap()) != slot
        {
            return Err(error("invalid recovery-seal identity"));
        }
        let generation = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let len = u32::from_le_bytes(bytes[24..28].try_into().unwrap()) as usize;
        if len > MAC_AT - 28 || (generation != 0 && generation % 2 != slot) {
            return Err(error("invalid recovery-seal bounds"));
        }
        Ok((
            generation,
            postcard::from_bytes(&bytes[28..28 + len]).map_err(error)?,
        ))
    }
    fn publish(&mut self, position: &Position) -> io::Result<()> {
        let generation = self
            .generation
            .checked_add(1)
            .ok_or_else(|| error("recovery-seal generations exhausted"))?;
        let slot = generation % 2;
        let record = self.encode(position, generation, slot)?;
        if let Some(file) = &mut self.file {
            crate::fault::point("before_cluster_watermark");
            file.seek(std::io::SeekFrom::Start(slot * SLOT_BYTES as u64))?;
            file.write_all(&record)?;
            crate::fault::point("after_cluster_watermark_write");
            file.sync_data()?;
            self.generation = generation;
        } else {
            // Fresh stores and signed-JSON upgrades publish both initialized
            // records atomically before the first protocol promise is released.
            let temporary = self.path.with_extension("cluster-watermark.tmp");
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&temporary)?;
            file.write_all(&self.encode(position, 0, 0)?)?;
            file.write_all(&self.encode(position, 0, 1)?)?;
            file.sync_all()?;
            crate::fault::point("before_cluster_watermark");
            std::fs::rename(&temporary, &self.path)?;
            File::open(
                self.path
                    .parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or_else(|| Path::new(".")),
            )?
            .sync_all()?;
            self.file = Some(file);
            self.generation = 0;
        }
        crate::fault::point("after_cluster_watermark");
        Ok(())
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
        if self.watermark.file.is_some() && self.watermark.sealed.as_ref() == Some(&position) {
            return Ok(());
        }
        if let Err(failure) = self.watermark.publish(&position) {
            self.failed = true;
            return Err(failure);
        }
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
    fn either_torn_record_requires_quorum_repair_instead_of_an_older_valid_promise() {
        for slot in [0, 1] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            let mut db = Database::open_mode(&path, &config(), true, false).unwrap();
            db.persist(&[entry(1, 1)], Some(&hard(1, 1))).unwrap();
            db.persist(&[entry(2, 1)], Some(&hard(1, 2))).unwrap();
            drop(db);
            let seal = path.with_extension("cluster-watermark");
            let mut bytes = std::fs::read(&seal).unwrap();
            bytes[slot * SLOT_BYTES + 128] ^= 0x80;
            std::fs::write(&seal, bytes).unwrap();
            assert!(
                Database::open_mode(&path, &config(), false, false).is_err(),
                "torn slot {slot} resumed voting"
            );
        }
    }
    #[test]
    fn signed_json_fences_upgrade_automatically_without_resetting_their_promise() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let cfg = config();
        let mut db = Database::open_mode(&path, &cfg, true, false).unwrap();
        db.persist(&[entry(1, 1)], Some(&hard(1, 1))).unwrap();
        let position = db.position().unwrap();
        drop(db);
        let body = serde_json::to_vec(&position).unwrap();
        let mac = auth(&cfg.secret, &cfg.cluster_id, cfg.node_id, &body)
            .finalize()
            .into_bytes()
            .to_vec();
        std::fs::write(
            path.with_extension("cluster-watermark"),
            serde_json::to_vec(&Signed { body, mac }).unwrap(),
        )
        .unwrap();
        let mut db = Database::open_mode(&path, &cfg, false, false).unwrap();
        assert_eq!(db.position().unwrap(), position);
        assert_eq!(
            std::fs::metadata(path.with_extension("cluster-watermark"))
                .unwrap()
                .len(),
            SEAL_BYTES as u64
        );
        db.persist(&[entry(2, 1)], Some(&hard(1, 2))).unwrap();
        drop(db);
        assert_eq!(
            Database::open_mode(&path, &cfg, false, false)
                .unwrap()
                .last()
                .unwrap(),
            2
        );
    }
    #[test]
    fn unpublished_torn_standby_cannot_replace_the_latest_signed_seal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store.db");
        let mut db = Database::open_mode(&path, &config(), true, false).unwrap();
        db.persist(&[entry(1, 1)], Some(&hard(1, 1))).unwrap();
        backup(&db, &path);
        for index in 2..=16 {
            db.persist(&[entry(index, 1)], Some(&hard(1, index)))
                .unwrap();
        }
        let seal = std::fs::read(path.with_extension("cluster-watermark")).unwrap();
        drop(db);
        std::fs::write(
            path.with_extension("cluster-watermark.tmp"),
            b"torn unpublished bytes",
        )
        .unwrap();
        let db = Database::open_mode(&path, &config(), false, false).unwrap();
        assert_eq!(db.last().unwrap(), 16);
        assert_eq!(
            std::fs::read(path.with_extension("cluster-watermark")).unwrap(),
            seal
        );
        drop(db);
        restore(&path);
        assert!(
            Database::open_mode(&path, &config(), false, false).is_err(),
            "standby allowed a promised prefix to roll back"
        );
    }
    #[test]
    fn lost_commit_notification_replays_full_accepted_payload_without_weakening_sync_mode() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store.db");
        let mut db = Database::open_mode(&path, &config(), true, false).unwrap();
        let mut accepted = entry(1, 1);
        accepted.data = postcard::to_allocvec(&Batch {
            version: 1,
            records: vec![crate::storage::Append {
                subject: "event".into(),
                headers: bytes::Bytes::new(),
                payload: bytes::Bytes::from_static(b"durable"),
            }],
            contents: Vec::new(),
        })
        .unwrap()
        .into();
        db.persist(&[accepted], Some(&hard(1, 0))).unwrap();
        backup(&db, &path);
        let sealed = std::fs::read(path.with_extension("cluster-watermark")).unwrap();
        db.persist(&[], Some(&hard(1, 1))).unwrap();
        db.apply(1).unwrap();
        assert_eq!(
            std::fs::read(path.with_extension("cluster-watermark")).unwrap(),
            sealed
        );
        assert_eq!(
            db.conn
                .query_row("PRAGMA synchronous", [], |row| row.get::<_, u32>(0))
                .unwrap(),
            2
        );
        let failed: io::Result<()> =
            db.commit_transaction(false, |_| Err(error("projection failed")));
        assert!(failed.is_err());
        assert_eq!(
            db.conn
                .query_row("PRAGMA synchronous", [], |row| row.get::<_, u32>(0))
                .unwrap(),
            2
        );
        drop(db);
        // Simulate losing only NORMAL WAL frames. The accepted batch and its
        // restart fence were durable before the lost commit notification.
        restore(&path);
        let mut db = Database::open_mode(&path, &config(), false, false).unwrap();
        assert_eq!(db.hard().unwrap().commit, 0);
        assert_eq!(db.last().unwrap(), 1);
        db.persist(&[], Some(&hard(1, 1))).unwrap();
        db.apply(1).unwrap();
        assert_eq!(db.record(1).unwrap().unwrap().payload.as_ref(), b"durable");
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
