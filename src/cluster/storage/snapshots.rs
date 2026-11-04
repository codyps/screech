use super::*;
use crate::storage::CheckpointImage;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub(crate) const MAX_SNAPSHOT: usize = 512 * 1024 * 1024;
#[derive(Serialize, Deserialize)]
struct Image {
    version: u8,
    sequence: u64,
    model: Vec<u8>,
    manifest: Vec<(ContentKey, [u8; 32])>,
    contents: Vec<Content>,
    membership: Vec<u8>,
    conf_index: u64,
}
fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}
impl Database {
    pub fn snapshot_base(&self) -> io::Result<(u64, u64, u64)> {
        Ok(self.snapshot_position)
    }
    pub fn restore_snapshot_position(&mut self) -> io::Result<()> {
        if let Some(s) = self.disk_snapshot()? {
            let image: Image = postcard::from_bytes(&s.data).map_err(error)?;
            self.snapshot_position = (
                s.get_metadata().index,
                s.get_metadata().term,
                image.sequence,
            );
        }
        Ok(())
    }
    fn disk_snapshot(&self) -> io::Result<Option<Snapshot>> {
        self.meta("raft_snapshot")?
            .map(|bytes| {
                let hash = self
                    .meta("raft_snapshot_hash")?
                    .ok_or_else(|| error("missing snapshot digest"))?;
                if hash.as_slice() != digest(&bytes) {
                    return Err(error("snapshot metadata digest mismatch"));
                }
                Snapshot::parse_from_bytes(&bytes).map_err(error)
            })
            .transpose()
    }
    pub fn snapshot_conf(&self) -> io::Result<ConfState> {
        self.disk_snapshot()?
            .map(|s| s.get_metadata().get_conf_state().clone())
            .ok_or_else(|| error("missing snapshot configuration"))
    }
    pub fn snapshot_membership(&self) -> io::Result<crate::cluster::membership::Membership> {
        match self.disk_snapshot()? {
            Some(s) => {
                let image: Image = postcard::from_bytes(&s.data).map_err(error)?;
                postcard::from_bytes(&image.membership).map_err(error)
            }
            None => Ok(Default::default()),
        }
    }
    pub fn conf(&self) -> io::Result<ConfState> {
        self.meta("conf")?
            .map(|b| ConfState::parse_from_bytes(&b).map_err(error))
            .transpose()
            .map(|c| c.unwrap_or_default())
    }
    pub fn snapshot_content(&self, key: ContentKey) -> io::Result<Option<Content>> {
        let row: Option<(Vec<u8>, Vec<u8>)> = self
            .conn
            .query_row(
                "SELECT data,digest FROM snapshot_bodies WHERE generation=?1 AND seq=?2",
                params![sql(key.generation)?, sql(key.sequence)?],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .map_err(error)?;
        row.map(|(data, hash)| {
            if hash.as_slice() != digest(&data) {
                return Err(error("snapshot body digest mismatch"));
            }
            let c: Content = postcard::from_bytes(&data).map_err(error)?;
            if c.key != key {
                return Err(error("snapshot body identity mismatch"));
            }
            Ok(c)
        })
        .transpose()
    }
    pub fn snapshot(&self) -> io::Result<Snapshot> {
        let mut s = self
            .disk_snapshot()?
            .ok_or_else(|| error("snapshot unavailable"))?;
        let mut image: Image = postcard::from_bytes(&s.data).map_err(error)?;
        for (key, expected) in &image.manifest {
            let c = self
                .snapshot_content(*key)?
                .ok_or_else(|| error("snapshot body missing"))?;
            if digest(&postcard::to_allocvec(&c).map_err(error)?) != *expected {
                return Err(error("snapshot manifest mismatch"));
            }
            image.contents.push(c);
        }
        s.data = postcard::to_allocvec(&image).map_err(error)?.into();
        if s.data.len() > MAX_SNAPSHOT {
            return Err(error("snapshot exceeds transfer budget"));
        }
        Ok(s)
    }
    pub(super) fn validate_stored_snapshot(&self) -> io::Result<()> {
        let snapshot = self.snapshot()?;
        let image: Image = postcard::from_bytes(&snapshot.data).map_err(error)?;
        if image.version != 1 || image.contents.len() != image.manifest.len() {
            return Err(error("invalid stored snapshot image"));
        }
        self.validate_voter_count(snapshot.get_metadata().get_conf_state())?;
        crate::durable::validate_cluster_snapshot(&image.model, &image.contents).map_err(error)
    }
    pub fn compact(&mut self, sequence: u64, image: CheckpointImage) -> io::Result<()> {
        if self.end()? != sequence {
            return Err(error("checkpoint position mismatch"));
        }
        let index = self.view()?;
        if index == 0 {
            return Ok(());
        }
        let term = if index == self.snapshot_position.0 {
            self.snapshot_position.1
        } else {
            self.entry(index)?.term
        };
        let contents: Vec<_> = image
            .live
            .into_iter()
            .map(|key| {
                self.content(key)?
                    .ok_or_else(|| error("checkpoint content missing"))
            })
            .collect::<io::Result<_>>()?;
        crate::durable::validate_cluster_snapshot(&image.data, &contents).map_err(error)?;
        let manifest = contents
            .iter()
            .map(|c| Ok((c.key, digest(&postcard::to_allocvec(c).map_err(error)?))))
            .collect::<io::Result<_>>()?;
        let image = Image {
            version: 1,
            sequence,
            model: image.data,
            manifest,
            contents,
            membership: postcard::to_allocvec(&self.membership()?).map_err(error)?,
            conf_index: self.conf_index()?,
        };
        let mut snap = Snapshot::default();
        snap.mut_metadata().index = index;
        snap.mut_metadata().term = term;
        snap.mut_metadata().set_conf_state(self.conf()?);
        snap.data = postcard::to_allocvec(&image).map_err(error)?.into();
        let hard = self.hard()?;
        self.install_snapshot(&snap, Some(&hard), &[], true)
    }
    /// Durable activation, covered-prefix reclamation and application recovery
    /// root change together. FULL transaction failure retains the previous image.
    pub fn install_snapshot(
        &mut self,
        snapshot: &Snapshot,
        hard: Option<&HardState>,
        entries: &[Entry],
        local: bool,
    ) -> io::Result<()> {
        if self.failed {
            return Err(error("replica quarantined"));
        }
        if snapshot.data.len() > MAX_SNAPSHOT {
            return Err(error("snapshot too large"));
        }
        let mut image: Image = postcard::from_bytes(&snapshot.data).map_err(error)?;
        if image.version != 1 || image.contents.len() != image.manifest.len() {
            return Err(error("invalid snapshot image"));
        }
        crate::durable::validate_cluster_snapshot(&image.model, &image.contents).map_err(error)?;
        let mut rows = Vec::new();
        for ((key, hash), content) in image.manifest.iter().zip(&image.contents) {
            let bytes = postcard::to_allocvec(content).map_err(error)?;
            if content.key != *key || digest(&bytes) != *hash {
                return Err(error("snapshot manifest/body mismatch"));
            }
            rows.push((*key, bytes, *hash));
        }
        let members: crate::cluster::membership::Membership =
            postcard::from_bytes(&image.membership).map_err(error)?;
        let conf = snapshot.get_metadata().get_conf_state();
        self.validate_voter_count(conf)?;
        if !local {
            if !conf.voters.contains(&self.node_id)
                && !conf.learners.contains(&self.node_id)
                && !conf.voters_outgoing.contains(&self.node_id)
            {
                return Err(error("snapshot does not enroll this node"));
            }
            if members
                .identities
                .get(&self.node_id)
                .copied()
                .unwrap_or([0; 16])
                != self.incarnation
            {
                return Err(error("snapshot belongs to a different voter incarnation"));
            }
        }
        let old = self.hard()?;
        let base = self.snapshot_base()?.0;
        let index = snapshot.get_metadata().index;
        if index < base || (!local && index < old.commit) {
            return Err(error("stale snapshot"));
        }
        if local && (index != self.view()? || index > old.commit) {
            return Err(error("uncommitted checkpoint"));
        }
        if base > 0
            && index == base
            && (snapshot.get_metadata().term != self.snapshot_position.1
                || snapshot.data != self.snapshot()?.data)
        {
            return Err(error("snapshot conflicts with committed image"));
        }
        let mut next = hard.cloned().unwrap_or_else(|| old.clone());
        if next.term < old.term || (next.term == old.term && old.vote != 0 && next.vote != old.vote)
        {
            return Err(error("snapshot would roll back vote"));
        }
        next.commit = next.commit.max(index);
        if next.term < snapshot.get_metadata().term {
            return Err(error("snapshot term exceeds hard state"));
        }
        if entries.first().is_some_and(|e| e.index != index + 1)
            || entries.windows(2).any(|p| p[1].index != p[0].index + 1)
        {
            return Err(error("invalid snapshot suffix"));
        }
        if !local && next.commit > entries.last().map_or(index, |e| e.index) {
            return Err(error("snapshot committed suffix is missing"));
        }
        // Local compaction preserves the complete accepted suffix. Incoming
        // snapshots discard the old suffix; Ready supplies any valid new entries.
        image.contents.clear();
        let mut stored = snapshot.clone();
        stored.data = postcard::to_allocvec(&image).map_err(error)?.into();
        let bytes = stored.write_to_bytes().map_err(error)?;
        let conf = snapshot
            .get_metadata()
            .get_conf_state()
            .write_to_bytes()
            .map_err(error)?;
        let last = if local {
            self.last()?.max(index)
        } else {
            index
        };
        let (last, last_term) = if let Some(entry) = entries.last() {
            (entry.index, entry.term)
        } else if last == index {
            (last, snapshot.get_metadata().term)
        } else {
            (last, self.entry(last)?.term)
        };
        let root = self.watermark.next_root_bytes(
            watermark::Position {
                incarnation: self.incarnation,
                term: next.term,
                vote: next.vote,
                commit: next.commit,
                last,
                last_term,
            },
            (index, snapshot.get_metadata().term, image.sequence),
        )?;
        let tx = self.conn.transaction().map_err(error)?;
        if local {
            tx.execute("DELETE FROM entries WHERE idx<=?1", [sql(index)?])
                .map_err(error)?;
        } else {
            tx.execute("DELETE FROM entries", []).map_err(error)?;
        }
        tx.execute_batch(
            "DELETE FROM event_index;DELETE FROM body_index;DELETE FROM snapshot_bodies;",
        )
        .map_err(error)?;
        for (key, bytes, hash) in rows {
            tx.execute(
                "INSERT INTO snapshot_bodies VALUES(?1,?2,?3,?4)",
                params![
                    sql(key.generation)?,
                    sql(key.sequence)?,
                    bytes,
                    hash.as_slice()
                ],
            )
            .map_err(error)?;
        }
        for e in entries {
            let b = e.write_to_bytes().map_err(error)?;
            tx.execute(
                "INSERT INTO entries VALUES(?1,?2,?3,?4)",
                params![sql(e.index)?, sql(e.term)?, b, crc32fast::hash(&b)],
            )
            .map_err(error)?;
        }
        for (key, value) in [
            ("raft_snapshot", bytes.clone()),
            ("raft_snapshot_hash", digest(&bytes).to_vec()),
            ("hard", next.write_to_bytes().map_err(error)?),
            ("view", index.to_le_bytes().to_vec()),
            ("conf", conf),
            ("membership", image.membership.clone()),
            ("conf_index", image.conf_index.to_le_bytes().to_vec()),
            ("app_snapshot", image.model.clone()),
            ("app_sequence", image.sequence.to_le_bytes().to_vec()),
            ("journal_root", root),
        ] {
            tx.execute(
                "INSERT OR REPLACE INTO cluster_meta VALUES(?1,?2)",
                params![key, value],
            )
            .map_err(error)?;
        }
        let mut crc = crc32fast::Hasher::new();
        crc.update(&image.sequence.to_le_bytes());
        crc.update(&image.model);
        tx.execute(
            "INSERT OR REPLACE INTO cluster_meta VALUES('app_crc',?1)",
            [crc.finalize().to_le_bytes().as_slice()],
        )
        .map_err(error)?;
        crate::fault::point("before_cluster_snapshot_install");
        tx.commit().map_err(error)?;
        self.refresh_log_bytes()?;
        self.snapshot_position = (index, snapshot.get_metadata().term, image.sequence);
        self.seal()?;
        crate::fault::point("after_cluster_snapshot_install");
        Ok(())
    }
    pub fn view(&self) -> io::Result<u64> {
        self.meta("view")?
            .map(|b| {
                b.try_into()
                    .map(u64::from_le_bytes)
                    .map_err(|_| error("invalid view position"))
            })
            .transpose()
            .map(|n| n.unwrap_or(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        durable::{Engine, StreamConfig},
        storage::{Append, SqliteStore, Store},
    };
    use bytes::Bytes;
    fn config(id: u64) -> ClusterConfig {
        serde_json::from_value(serde_json::json!({"cluster_id":"snapshots","node_id":id,"secret":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx","initial_voters":[1,2,3],"peers":{
            "1":{"raft":"127.0.0.1:11001","client":"127.0.0.1:11002","service":"127.0.0.1:11003"},
            "2":{"raft":"127.0.0.1:11004","client":"127.0.0.1:11005","service":"127.0.0.1:11006"},
            "3":{"raft":"127.0.0.1:11007","client":"127.0.0.1:11008","service":"127.0.0.1:11009"},
            "4":{"raft":"127.0.0.1:11010","client":"127.0.0.1:11011","service":"127.0.0.1:11012"}
        }})).unwrap()
    }
    fn donor(root: &Path) -> (Database, Vec<u8>) {
        let mut app = Engine::recover(SqliteStore::open(root.join("app")).unwrap()).unwrap();
        app.commit(
            app.state()
                .prepare_create(
                    StreamConfig {
                        name: "S".into(),
                        subjects: vec!["a".into()],
                        ..Default::default()
                    },
                    0,
                )
                .unwrap(),
        )
        .unwrap();
        app.commit(
            app.state()
                .prepare_publish("S", "a", Bytes::new(), "retained".into(), 1)
                .unwrap(),
        )
        .unwrap();
        let key = app.state().streams["S"].messages[&1]
            .content
            .as_ref()
            .unwrap()
            .key;
        let mut store = app.into_store();
        let contents = vec![store.get_content(key).unwrap().unwrap()];
        let records = (1..=store.last_sequence().unwrap())
            .map(|n| {
                let r = store.get(n).unwrap().unwrap();
                Append {
                    subject: r.subject,
                    headers: r.headers,
                    payload: r.payload,
                }
            })
            .collect();
        let mut app = Engine::recover(store).unwrap();
        app.checkpoint().unwrap();
        store = app.into_store();
        let data = store.load_checkpoint().unwrap().unwrap().1.to_vec();
        let mut db = Database::open(&root.join("donor"), &config(1), true).unwrap();
        let entry = Entry {
            index: 1,
            term: 1,
            data: postcard::to_allocvec(&Batch {
                version: 1,
                records,
                contents,
            })
            .unwrap()
            .into(),
            ..Default::default()
        };
        let hard = HardState {
            term: 1,
            vote: 1,
            commit: 1,
            ..Default::default()
        };
        db.persist(&[entry], Some(&hard)).unwrap();
        db.apply(1).unwrap();
        (db, data)
    }
    #[test]
    fn compaction_keeps_payloads_and_accepted_tail_then_recovers_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let (mut db, data) = donor(dir.path());
        let accepted = Entry {
            index: 2,
            term: 1,
            data: Vec::new().into(),
            ..Default::default()
        };
        db.persist(&[accepted], None).unwrap();
        let key = ContentKey {
            generation: 1,
            sequence: 1,
        };
        db.compact(
            2,
            CheckpointImage {
                data: data.clone(),
                live: vec![key],
                imports: vec![],
            },
        )
        .unwrap();
        assert_eq!(db.snapshot_base().unwrap(), (1, 1, 2));
        assert_eq!(db.last().unwrap(), 2);
        assert!(db.record(1).unwrap().is_none());
        assert_eq!(db.content(key).unwrap().unwrap().payload, "retained");
        assert_eq!(db.entry(2).unwrap().index, 2);
        // Repeating a checkpoint at the same committed prefix is idempotent.
        db.compact(
            2,
            CheckpointImage {
                data,
                live: vec![key],
                imports: vec![],
            },
        )
        .unwrap();
        drop(db);
        let db = Database::open(&dir.path().join("donor"), &config(1), false).unwrap();
        assert_eq!(db.hard().unwrap().commit, 1);
        assert_eq!(db.end().unwrap(), 2);
        assert_eq!(db.last().unwrap(), 2);
        assert_eq!(db.content(key).unwrap().unwrap().payload, "retained");
    }
    #[test]
    fn full_checkpoint_before_journal_rotation_recovers_but_an_older_root_cannot_replace_it() {
        for restore_old_sqlite in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let (mut db, data) = donor(dir.path());
            let path = dir.path().join("donor");
            db.conn
                .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
                .unwrap();
            let old_sqlite = std::fs::read(&path).unwrap();
            let old_journal = std::fs::read(path.with_extension("cluster-watermark")).unwrap();
            let key = ContentKey {
                generation: 1,
                sequence: 1,
            };
            db.compact(
                2,
                CheckpointImage {
                    data,
                    live: vec![key],
                    imports: vec![],
                },
            )
            .unwrap();
            drop(db);
            if restore_old_sqlite {
                for suffix in ["-wal", "-shm"] {
                    let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
                }
                std::fs::write(&path, old_sqlite).unwrap();
                assert!(Database::open(&path, &config(1), false).is_err());
            } else {
                // FULL SQLite committed, but the old journal still names its
                // exact authenticated predecessor. Startup completes rotation.
                std::fs::write(path.with_extension("cluster-watermark"), old_journal).unwrap();
                let db = Database::open(&path, &config(1), false).unwrap();
                assert_eq!(db.snapshot_base().unwrap(), (1, 1, 2));
                assert_eq!(db.content(key).unwrap().unwrap().payload, "retained");
                assert_eq!(db.journal_bytes(), 0);
            }
        }
    }
    #[test]
    fn invalid_model_with_valid_file_digests_cannot_resume_as_a_voter() {
        let dir = tempfile::tempdir().unwrap();
        let (mut db, data) = donor(dir.path());
        db.compact(
            2,
            CheckpointImage {
                data,
                live: vec![ContentKey {
                    generation: 1,
                    sequence: 1,
                }],
                imports: vec![],
            },
        )
        .unwrap();
        let mut stored =
            Snapshot::parse_from_bytes(&db.meta("raft_snapshot").unwrap().unwrap()).unwrap();
        let mut image: Image = postcard::from_bytes(&stored.data).unwrap();
        image.model[0] = 127; // An unsupported application model, with intact payloads.
        stored.data = postcard::to_allocvec(&image).unwrap().into();
        let bytes = stored.write_to_bytes().unwrap();
        db.conn
            .execute(
                "UPDATE cluster_meta SET value=?1 WHERE key='raft_snapshot'",
                [bytes.as_slice()],
            )
            .unwrap();
        db.conn
            .execute(
                "UPDATE cluster_meta SET value=?1 WHERE key='raft_snapshot_hash'",
                [digest(&bytes).as_slice()],
            )
            .unwrap();
        drop(db);
        assert!(
            Database::open(&dir.path().join("donor"), &config(1), false).is_err(),
            "structurally invalid snapshot resumed voting before model validation"
        );
    }
    #[test]
    fn restored_snapshot_cannot_reduce_the_configured_voter_quorum() {
        let dir = tempfile::tempdir().unwrap();
        let (mut db, data) = donor(dir.path());
        db.compact(
            2,
            CheckpointImage {
                data,
                live: vec![ContentKey {
                    generation: 1,
                    sequence: 1,
                }],
                imports: vec![],
            },
        )
        .unwrap();
        let mut stored =
            Snapshot::parse_from_bytes(&db.meta("raft_snapshot").unwrap().unwrap()).unwrap();
        stored.mut_metadata().mut_conf_state().voters = vec![1];
        let bytes = stored.write_to_bytes().unwrap();
        db.conn
            .execute(
                "UPDATE cluster_meta SET value=?1 WHERE key='raft_snapshot'",
                [bytes.as_slice()],
            )
            .unwrap();
        db.conn
            .execute(
                "UPDATE cluster_meta SET value=?1 WHERE key='raft_snapshot_hash'",
                [digest(&bytes).as_slice()],
            )
            .unwrap();
        drop(db);
        assert!(
            Database::open(&dir.path().join("donor"), &config(1), false).is_err(),
            "recovery reduced the quorum to one voter"
        );
    }
    #[test]
    fn delayed_empty_snapshot_cannot_erase_newly_committed_streams() {
        let dir = tempfile::tempdir().unwrap();
        let (donor, data) = donor(dir.path());
        let mut source = Database::open(&dir.path().join("source"), &config(1), true).unwrap();
        let hard = |commit| HardState {
            term: 1,
            vote: 1,
            commit,
            ..Default::default()
        };
        source
            .persist(
                &[Entry {
                    index: 1,
                    term: 1,
                    ..Default::default()
                }],
                Some(&hard(1)),
            )
            .unwrap();
        source.apply(1).unwrap();
        let mut empty = Engine::recover(crate::storage::MemoryStore::default()).unwrap();
        empty.checkpoint().unwrap();
        let empty = empty
            .into_store()
            .load_checkpoint()
            .unwrap()
            .unwrap()
            .1
            .to_vec();
        source
            .compact(
                0,
                CheckpointImage {
                    data: empty,
                    live: vec![],
                    imports: vec![],
                },
            )
            .unwrap();
        let old = source.snapshot().unwrap();
        let mut created = donor.entry(1).unwrap();
        created.index = 2;
        source.persist(&[created], Some(&hard(2))).unwrap();
        source.apply(2).unwrap();
        let key = ContentKey {
            generation: 1,
            sequence: 1,
        };
        source
            .compact(
                2,
                CheckpointImage {
                    data: data.clone(),
                    live: vec![key],
                    imports: vec![],
                },
            )
            .unwrap();
        let new = source.snapshot().unwrap();
        let mut receiver = Database::open(&dir.path().join("receiver"), &config(2), true).unwrap();
        let mut received = hard(2);
        received.vote = 2;
        receiver
            .install_snapshot(&new, Some(&received), &[], false)
            .unwrap();
        assert!(receiver.install_snapshot(&old, None, &[], false).is_err());
        assert_eq!(receiver.snapshot_base().unwrap(), (2, 1, 2));
        assert_eq!(receiver.meta("app_snapshot").unwrap().unwrap(), data);
        assert_eq!(receiver.content(key).unwrap().unwrap().payload, "retained");
        drop(receiver);
        let receiver = Database::open(&dir.path().join("receiver"), &config(2), false).unwrap();
        assert_eq!(receiver.end().unwrap(), 2);
        assert_eq!(receiver.content(key).unwrap().unwrap().payload, "retained");
    }
    #[test]
    fn snapshot_decode_failure_never_advances_applied_or_releases_protocol_responses() {
        let dir = tempfile::tempdir().unwrap();
        let (mut donor, data) = donor(dir.path());
        donor
            .compact(
                2,
                CheckpointImage {
                    data,
                    live: vec![ContentKey {
                        generation: 1,
                        sequence: 1,
                    }],
                    imports: vec![],
                },
            )
            .unwrap();
        let mut snapshot = donor.snapshot().unwrap();
        snapshot.data = vec![255].into();
        let db = std::sync::Arc::new(std::sync::Mutex::new(
            Database::open(&dir.path().join("receiver"), &config(2), true).unwrap(),
        ));
        let log = crate::cluster::LogStorage { db: db.clone() };
        let mut node = raft::RawNode::new(
            &raft::Config {
                id: 2,
                ..Default::default()
            },
            log,
            &slog::Logger::root(slog::Discard, slog::o!()),
        )
        .unwrap();
        node.step(raft::eraftpb::Message {
            msg_type: raft::eraftpb::MessageType::MsgSnapshot,
            from: 1,
            to: 2,
            term: 1,
            snapshot: Some(snapshot).into(),
            ..Default::default()
        })
        .unwrap();
        assert!(node.has_ready());
        let (send, mut receive) = tokio::sync::mpsc::channel(4);
        let outgoing = std::collections::BTreeMap::from([(1, send)]);
        assert!(crate::cluster::pump(
            &mut node,
            &mut std::collections::BTreeMap::new(),
            &outgoing,
            &std::collections::BTreeMap::new()
        )
        .is_err());
        assert!(receive.try_recv().is_err());
        assert_eq!(node.raft.raft_log.applied, 0);
        let db = db.lock().unwrap();
        assert_eq!(db.hard().unwrap().commit, 0);
        assert_eq!(db.view().unwrap(), 0);
        assert_eq!(db.last().unwrap(), 0);
    }
    #[test]
    fn malformed_snapshot_or_wrong_incarnation_cannot_replace_the_old_root() {
        let dir = tempfile::tempdir().unwrap();
        let (mut donor, data) = donor(dir.path());
        let key = ContentKey {
            generation: 1,
            sequence: 1,
        };
        donor
            .compact(
                2,
                CheckpointImage {
                    data,
                    live: vec![key],
                    imports: vec![],
                },
            )
            .unwrap();
        let snapshot = donor.snapshot().unwrap();
        let mut receiver = Database::open(&dir.path().join("receiver"), &config(2), true).unwrap();
        let hard = HardState {
            term: 1,
            vote: 2,
            commit: 1,
            ..Default::default()
        };
        let mut bad = snapshot.clone();
        let mut image: Image = postcard::from_bytes(&bad.data).unwrap();
        image.contents[0].payload = "tampered".into();
        bad.data = postcard::to_allocvec(&image).unwrap().into();
        assert!(receiver
            .install_snapshot(&bad, Some(&hard), &[], false)
            .is_err());
        assert_eq!(receiver.last().unwrap(), 0);
        receiver.incarnation = [9; 16];
        assert!(receiver
            .install_snapshot(&snapshot, Some(&hard), &[], false)
            .is_err());
        receiver.incarnation = [0; 16];
        receiver
            .install_snapshot(&snapshot, Some(&hard), &[], false)
            .unwrap();
        assert_eq!(receiver.hard().unwrap().vote, 2);
        assert_eq!(receiver.content(key).unwrap().unwrap().payload, "retained");
        let mut vote_rollback = hard.clone();
        vote_rollback.vote = 1;
        assert!(receiver
            .install_snapshot(&snapshot, Some(&vote_rollback), &[], false)
            .is_err());
        let mut learner =
            Database::open_mode(&dir.path().join("learner"), &config(4), false, true).unwrap();
        assert!(learner
            .install_snapshot(&snapshot, Some(&hard), &[], false)
            .is_err());
        assert_eq!(learner.last().unwrap(), 0);
        receiver
            .conn
            .execute("DELETE FROM snapshot_bodies", [])
            .unwrap();
        drop(receiver);
        assert!(Database::open(&dir.path().join("receiver"), &config(2), false).is_err());
    }
}
