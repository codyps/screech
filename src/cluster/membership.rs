use super::*;
use raft::eraftpb::{
    ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, Entry, EntryType,
};
#[derive(Clone, Default, Serialize, Deserialize)]
pub(crate) struct Membership {
    pub identities: BTreeMap<u64, [u8; 16]>,
    pub retired: BTreeMap<u64, u64>,
    pub replacing: Option<Replacement>,
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Replacement {
    pub old: u64,
    pub new: u64,
    pub nonce: [u8; 16],
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Change {
    pub replacement: Replacement,
    pub stage: u8,
}
impl Database {
    pub(crate) fn validate_voter_count(&self, conf: &raft::eraftpb::ConfState) -> io::Result<()> {
        // This implementation replaces voters one-for-one. General resizing is
        // not supported; replay may never silently shrink the required quorum.
        let expected = self.genesis.voters.len();
        let distinct = |ids: &[u64]| {
            ids.iter()
                .copied()
                .filter(|id| *id != 0)
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                == expected
        };
        if conf.voters.len() != expected
            || !distinct(&conf.voters)
            || (!conf.voters_outgoing.is_empty()
                && (conf.voters_outgoing.len() != expected || !distinct(&conf.voters_outgoing)))
        {
            return Err(error(
                "recovered membership changes the configured voter quorum",
            ));
        }
        Ok(())
    }
    pub fn accepts_sender(&self, message: &Message, nonce: [u8; 16]) -> io::Result<bool> {
        // Check both records in one SQLite read snapshot and lock acquisition.
        let (state, conf) = self
            .conn
            .prepare_cached(
                "SELECT
          (SELECT value FROM cluster_meta WHERE key='membership'),
          (SELECT value FROM cluster_meta WHERE key='conf')",
            )
            .map_err(error)?
            .query_row([], |row| {
                Ok((
                    row.get::<_, Option<Vec<u8>>>(0)?,
                    row.get::<_, Option<Vec<u8>>>(1)?,
                ))
            })
            .map_err(error)?;
        let state: Membership = state
            .map(|b| postcard::from_bytes(&b).map_err(error))
            .transpose()?
            .unwrap_or_default();
        let conf = conf
            .map(|b| raft::eraftpb::ConfState::parse_from_bytes(&b).map_err(error))
            .transpose()?
            .unwrap_or_default();
        let expected = state
            .identities
            .get(&message.from)
            .copied()
            .unwrap_or([0; 16]);
        let joining = self.incarnation != [0; 16] && !state.identities.contains_key(&self.node_id);
        let enrolled = conf.voters.contains(&message.from)
            || conf.voters_outgoing.contains(&message.from)
            || conf.learners.contains(&message.from)
            || conf.learners_next.contains(&message.from);
        let replication = matches!(
            message.get_msg_type(),
            raft::eraftpb::MessageType::MsgAppend
                | raft::eraftpb::MessageType::MsgHeartbeat
                | raft::eraftpb::MessageType::MsgSnapshot
        );
        Ok(!state.retired.contains_key(&message.from)
            && ((enrolled && expected == nonce) || (joining && replication)))
    }
    pub fn membership(&self) -> io::Result<Membership> {
        self.meta("membership")?
            .map(|b| postcard::from_bytes(&b).map_err(error))
            .transpose()
            .map(|m| m.unwrap_or_default())
    }
    pub fn conf_index(&self) -> io::Result<u64> {
        self.meta("conf_index")?
            .map(|b| {
                b.try_into()
                    .map(u64::from_le_bytes)
                    .map_err(|_| error("invalid configuration index"))
            })
            .transpose()
            .map(|n| n.unwrap_or(0))
    }
    pub fn note_change(state: &mut Membership, entry: &Entry) -> io::Result<()> {
        if entry.context.is_empty() {
            return Ok(());
        }
        let change: Change = postcard::from_bytes(&entry.context).map_err(error)?;
        match change.stage {
            1 => {
                state
                    .identities
                    .insert(change.replacement.new, change.replacement.nonce);
                state.replacing = Some(change.replacement);
            }
            2 => {}
            3 => {
                state
                    .retired
                    .insert(change.replacement.old, change.replacement.new);
                state.replacing = None;
            }
            4 => {
                state.retired.insert(change.replacement.new, 0);
                state.replacing = None;
            }
            5 => {
                state
                    .identities
                    .insert(change.replacement.new, change.replacement.nonce);
            }
            6 => {
                state.retired.insert(change.replacement.new, 0);
            }
            _ => return Err(error("unknown replacement stage")),
        }
        Ok(())
    }
    /// Reconstruct configuration from the snapshot and committed changes. This
    /// also closes crashes between hard-state persistence and apply bookkeeping.
    pub fn recover_membership(&mut self) -> io::Result<()> {
        let base = self.snapshot_base()?.0;
        let mut state = self.snapshot_membership()?;
        let conf = if base > 0 {
            self.snapshot_conf()?
        } else {
            self.genesis.clone()
        };
        let storage = raft::storage::MemStorage::new_with_conf_state(conf);
        let config = raft::Config {
            id: self.node_id,
            ..Default::default()
        };
        let mut node = RawNode::new(
            &config,
            storage,
            &slog::Logger::root(slog::Discard, slog::o!()),
        )
        .map_err(error)?;
        let mut index = if base > 0 { self.conf_index()? } else { 0 };
        if base == 0 {
            state = Membership::default();
        }
        for n in base + 1..=self.hard()?.commit {
            let e = self.entry(n)?;
            if e.get_entry_type() == EntryType::EntryConfChangeV2 {
                let cc = ConfChangeV2::parse_from_bytes(&e.data).map_err(error)?;
                node.apply_conf_change(&cc).map_err(error)?;
                Self::note_change(&mut state, &e)?;
                index = n;
            } else if e.get_entry_type() != EntryType::EntryNormal {
                return Err(error("unsupported configuration entry"));
            }
        }
        let conf = node.raft.prs().conf().to_conf_state();
        self.validate_voter_count(&conf)?;
        if (conf.voters.contains(&self.node_id) || state.identities.contains_key(&self.node_id))
            && state
                .identities
                .get(&self.node_id)
                .copied()
                .unwrap_or([0; 16])
                != self.incarnation
        {
            return Err(error(
                "voter incarnation does not match committed enrollment",
            ));
        }
        self.commit_transaction(false, |tx| {
            tx.execute(
                "INSERT OR REPLACE INTO cluster_meta VALUES('conf',?1)",
                [conf.write_to_bytes().map_err(error)?],
            )
            .map_err(error)?;
            tx.execute(
                "INSERT OR REPLACE INTO cluster_meta VALUES('conf_index',?1)",
                [index.to_le_bytes().as_slice()],
            )
            .map_err(error)?;
            tx.execute(
                "INSERT OR REPLACE INTO cluster_meta VALUES('membership',?1)",
                [postcard::to_allocvec(&state).map_err(error)?],
            )
            .map_err(error)?;
            Ok(())
        })
    }
}
pub(crate) fn apply_configuration(node: &mut RawNode<LogStorage>, commit: u64) -> io::Result<()> {
    let entries = {
        let db = node.store().db.lock().unwrap();
        (db.view()? + 1..=commit)
            .map(|i| db.entry(i))
            .collect::<io::Result<Vec<_>>>()?
    };
    for e in entries {
        if e.get_entry_type() == EntryType::EntryConfChangeV2 {
            let cc = ConfChangeV2::parse_from_bytes(&e.data).map_err(error)?;
            // The committed configuration must survive losing the SQLite
            // projection, including a crash before the next ordinary append.
            // Seal that commit before the new voter set can take effect.
            node.store().db.lock().unwrap().seal()?;
            let conf = node.apply_conf_change(&cc).map_err(error)?;
            if step_down_removed(node) {
                // Configuration callbacks can queue leader-only probes. A
                // removed leader must not carry them into follower LightReady.
                node.raft.msgs.clear();
            }
            let mut db = node.store().db.lock().unwrap();
            db.validate_voter_count(&conf)?;
            let mut state = db.membership()?;
            Database::note_change(&mut state, &e)?;
            db.commit_transaction(false, |tx| {
                tx.execute(
                    "INSERT OR REPLACE INTO cluster_meta VALUES('conf',?1)",
                    [conf.write_to_bytes().map_err(error)?],
                )
                .map_err(error)?;
                tx.execute(
                    "INSERT OR REPLACE INTO cluster_meta VALUES('conf_index',?1)",
                    [e.index.to_le_bytes().as_slice()],
                )
                .map_err(error)?;
                tx.execute(
                    "INSERT OR REPLACE INTO cluster_meta VALUES('membership',?1)",
                    [postcard::to_allocvec(&state).map_err(error)?],
                )
                .map_err(error)?;
                crate::fault::point("before_cluster_membership_apply");
                Ok(())
            })?;
            crate::fault::point("after_cluster_membership_apply");
        }
    }
    Ok(())
}
fn single(id: u64, kind: ConfChangeType) -> ConfChangeSingle {
    ConfChangeSingle {
        node_id: id,
        change_type: kind,
        ..Default::default()
    }
}
pub(crate) fn start_replacement(
    node: &mut RawNode<LogStorage>,
    replacement: Replacement,
) -> io::Result<()> {
    if node.raft.state != StateRole::Leader {
        return Err(error("not leader"));
    }
    let db = node.store().db.lock().unwrap();
    let conf = db.conf()?;
    let state = db.membership()?;
    if state.retired.get(&replacement.old) == Some(&replacement.new)
        && conf.voters.contains(&replacement.new)
    {
        return Ok(());
    }
    if state.replacing.as_ref().is_some_and(|r| {
        r.old == replacement.old && r.new == replacement.new && r.nonce == replacement.nonce
    }) {
        return Ok(());
    }
    if !conf.voters_outgoing.is_empty()
        || node.raft.pending_conf_index > node.raft.raft_log.applied
        || !conf.voters.contains(&replacement.old)
        || conf.voters.contains(&replacement.new)
        || conf.learners.contains(&replacement.new)
        || state.identities.contains_key(&replacement.new)
        || state.retired.contains_key(&replacement.new)
        || replacement.nonce == [0; 16]
    {
        return Err(error("replacement needs a new unused identity and one healthy quorum; another change may be pending"));
    }
    if let Some(pending) = state.replacing {
        if pending.old != replacement.old || !conf.learners.contains(&pending.new) {
            return Err(error("a different replacement is pending"));
        }
        drop(db);
        let cc = ConfChangeV2 {
            changes: vec![single(pending.new, ConfChangeType::RemoveNode)].into(),
            ..Default::default()
        };
        return node
            .propose_conf_change(
                postcard::to_allocvec(&Change {
                    replacement: pending,
                    stage: 4,
                })
                .map_err(error)?,
                cc,
            )
            .map_err(error);
    }
    drop(db);
    let cc = ConfChangeV2 {
        changes: vec![single(replacement.new, ConfChangeType::AddLearnerNode)].into(),
        ..Default::default()
    };
    let context = postcard::to_allocvec(&Change {
        replacement,
        stage: 1,
    })
    .map_err(error)?;
    node.propose_conf_change(context, cc).map_err(error)
}
pub(crate) fn advance_replacement(node: &mut RawNode<LogStorage>) -> io::Result<()> {
    if node.raft.state != StateRole::Leader
        || node.raft.pending_conf_index > node.raft.raft_log.applied
    {
        return Ok(());
    }
    let db = node.store().db.lock().unwrap();
    let state = db.membership()?;
    let Some(replacement) = state.replacing else {
        return Ok(());
    };
    let conf = db.conf()?;
    let commit = db.hard()?.commit;
    drop(db);
    let (cc, stage) = if !conf.voters_outgoing.is_empty() {
        (ConfChangeV2::default(), 3)
    } else {
        let Some(progress) = node.raft.prs().get(replacement.new) else {
            return Ok(());
        };
        if progress.matched < commit || !progress.recent_active {
            return Ok(());
        }
        (
            ConfChangeV2 {
                transition: ConfChangeTransition::Explicit,
                changes: vec![
                    single(replacement.new, ConfChangeType::AddNode),
                    single(replacement.old, ConfChangeType::RemoveNode),
                ]
                .into(),
                ..Default::default()
            },
            2,
        )
    };
    node.propose_conf_change(
        postcard::to_allocvec(&Change { replacement, stage }).map_err(error)?,
        cc,
    )
    .map_err(error)
}

/// raft-rs 0.7 marks a removed leader non-promotable but leaves step-down to
/// its caller. Step down immediately after applying committed removal, before
/// advance can try to update the removed leader's progress.
pub(crate) fn step_down_removed(node: &mut RawNode<LogStorage>) -> bool {
    if node.raft.state == StateRole::Leader
        && !node.raft.prs().conf().voters().contains(node.raft.id)
    {
        node.raft.become_follower(node.raft.term, 0);
        true
    } else {
        false
    }
}

/// Non-voting slots can start as standbys without changing the voter count.
/// Losing a standby's store retires its old incarnation before enrollment.
pub(crate) fn enroll_standby(
    node: &mut RawNode<LogStorage>,
    new: u64,
    nonce: [u8; 16],
    old: Option<u64>,
) -> io::Result<()> {
    if node.raft.pending_conf_index > node.raft.raft_log.applied {
        return Err(error("configuration change pending"));
    }
    let db = node.store().db.lock().unwrap();
    let state = db.membership()?;
    if state.identities.contains_key(&new) || state.retired.contains_key(&new) {
        return Err(error("recovery identity already used"));
    }
    let (target, kind, stage, nonce) = if let Some(old) = old {
        (
            old,
            ConfChangeType::RemoveNode,
            6,
            state.identities.get(&old).copied().unwrap_or([0; 16]),
        )
    } else {
        (new, ConfChangeType::AddLearnerNode, 5, nonce)
    };
    drop(db);
    node.propose_conf_change(
        postcard::to_allocvec(&Change {
            replacement: Replacement {
                old: 0,
                new: target,
                nonce,
            },
            stage,
        })
        .map_err(error)?,
        ConfChangeV2 {
            changes: vec![single(target, kind)].into(),
            ..Default::default()
        },
    )
    .map_err(error)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn config() -> ClusterConfig {
        ClusterConfig {
            cluster_id: "members".into(),
            node_id: 1,
            secret: "s".repeat(32),
            max_log_bytes: 512 * 1024 * 1024,
            initial_voters: vec![1, 2, 3],
            peers: (1..=4)
                .map(|id| {
                    (
                        id,
                        Peer {
                            raft: format!("127.0.0.1:{}", 32000 + id * 3).parse().unwrap(),
                            client: format!("127.0.0.1:{}", 32001 + id * 3).parse().unwrap(),
                            service: format!("127.0.0.1:{}", 32002 + id * 3).parse().unwrap(),
                        },
                    )
                })
                .collect(),
        }
    }
    #[test]
    fn replay_recovers_membership_after_every_persist_before_apply_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let config = config();
        let replacement = Replacement {
            old: 3,
            new: 4,
            nonce: [7; 16],
        };
        let changes = [
            ConfChangeV2 {
                changes: vec![single(4, ConfChangeType::AddLearnerNode)].into(),
                ..Default::default()
            },
            ConfChangeV2 {
                transition: ConfChangeTransition::Explicit,
                changes: vec![
                    single(4, ConfChangeType::AddNode),
                    single(3, ConfChangeType::RemoveNode),
                ]
                .into(),
                ..Default::default()
            },
            ConfChangeV2::default(),
        ];
        let mut db = Database::open(&path, &config, true).unwrap();
        for (n, change) in changes.into_iter().enumerate() {
            let index = n as u64 + 1;
            let e = Entry {
                index,
                term: 1,
                entry_type: EntryType::EntryConfChangeV2,
                data: change.write_to_bytes().unwrap().into(),
                context: postcard::to_allocvec(&Change {
                    replacement: replacement.clone(),
                    stage: index as u8,
                })
                .unwrap()
                .into(),
                ..Default::default()
            };
            db.persist(
                &[e],
                Some(&raft::eraftpb::HardState {
                    term: 1,
                    vote: 1,
                    commit: index,
                    ..Default::default()
                }),
            )
            .unwrap();
            // Kill after the authoritative commit reaches disk, before any
            // application/configuration bookkeeping. Recovery must derive it.
            drop(db);
            db = Database::open(&path, &config, false).unwrap();
            let mut conf = db.conf().unwrap();
            conf.voters.sort_unstable();
            conf.voters_outgoing.sort_unstable();
            conf.learners.sort_unstable();
            let state = db.membership().unwrap();
            assert_eq!(db.conf_index().unwrap(), index);
            assert_eq!(db.end().unwrap(), 0);
            assert_eq!(state.identities[&4], [7; 16]);
            match index {
                1 => {
                    assert_eq!(conf.learners, vec![4]);
                    assert!(state.replacing.is_some());
                }
                2 => {
                    assert_eq!(conf.voters_outgoing, vec![1, 2, 3]);
                    assert_eq!(conf.voters, vec![1, 2, 4]);
                }
                3 => {
                    assert!(conf.voters_outgoing.is_empty());
                    assert_eq!(conf.voters, vec![1, 2, 4]);
                    assert_eq!(state.retired[&3], 4);
                    assert!(state.replacing.is_none());
                }
                _ => unreachable!(),
            }
        }
    }
    #[test]
    fn retired_unenrolled_and_reincarnated_senders_cannot_vote() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::open(&dir.path().join("db"), &config(), true).unwrap();
        let state = Membership {
            identities: BTreeMap::from([(4, [7; 16])]),
            retired: BTreeMap::from([(3, 4)]),
            replacing: None,
        };
        let conf = raft::eraftpb::ConfState::from((vec![1, 2, 4], vec![]));
        db.conn
            .execute(
                "INSERT OR REPLACE INTO cluster_meta VALUES('membership',?1)",
                [postcard::to_allocvec(&state).unwrap()],
            )
            .unwrap();
        db.conn
            .execute(
                "INSERT OR REPLACE INTO cluster_meta VALUES('conf',?1)",
                [conf.write_to_bytes().unwrap()],
            )
            .unwrap();
        let mut vote = Message {
            from: 4,
            to: 1,
            msg_type: raft::eraftpb::MessageType::MsgRequestVote,
            ..Default::default()
        };
        assert!(db.accepts_sender(&vote, [7; 16]).unwrap());
        assert!(!db.accepts_sender(&vote, [8; 16]).unwrap());
        vote.from = 3;
        assert!(!db.accepts_sender(&vote, [0; 16]).unwrap());
        vote.from = 5;
        assert!(!db.accepts_sender(&vote, [0; 16]).unwrap());
    }
}

#[cfg(test)]
mod abort_tests {
    use super::*;
    #[test]
    fn abandoned_learner_identity_stays_retired_after_compaction_and_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let config:ClusterConfig=serde_json::from_value(serde_json::json!({"cluster_id":"abort","node_id":1,"secret":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx","initial_voters":[1,2,3],"peers":{
            "1":{"raft":"127.0.0.1:12001","client":"127.0.0.1:12002","service":"127.0.0.1:12003"},
            "2":{"raft":"127.0.0.1:12004","client":"127.0.0.1:12005","service":"127.0.0.1:12006"},
            "3":{"raft":"127.0.0.1:12007","client":"127.0.0.1:12008","service":"127.0.0.1:12009"},
            "4":{"raft":"127.0.0.1:12010","client":"127.0.0.1:12011","service":"127.0.0.1:12012"}
        }})).unwrap();
        let replacement = Replacement {
            old: 3,
            new: 4,
            nonce: [7; 16],
        };
        let mut db = Database::open(&path, &config, true).unwrap();
        for (index, kind, stage) in [
            (1, ConfChangeType::AddLearnerNode, 1),
            (2, ConfChangeType::RemoveNode, 4),
        ] {
            let change = ConfChangeV2 {
                changes: vec![single(4, kind)].into(),
                ..Default::default()
            };
            let entry = Entry {
                index,
                term: 1,
                entry_type: EntryType::EntryConfChangeV2,
                data: change.write_to_bytes().unwrap().into(),
                context: postcard::to_allocvec(&Change {
                    replacement: replacement.clone(),
                    stage,
                })
                .unwrap()
                .into(),
                ..Default::default()
            };
            db.persist(
                &[entry],
                Some(&raft::eraftpb::HardState {
                    term: 1,
                    vote: 1,
                    commit: index,
                    ..Default::default()
                }),
            )
            .unwrap();
            drop(db);
            db = Database::open(&path, &config, false).unwrap();
        }
        let mut engine =
            crate::durable::Engine::recover(crate::storage::MemoryStore::default()).unwrap();
        engine.checkpoint().unwrap();
        let data = engine
            .into_store()
            .load_checkpoint()
            .unwrap()
            .unwrap()
            .1
            .to_vec();
        db.compact(
            0,
            crate::storage::CheckpointImage {
                data,
                live: vec![],
                imports: vec![],
            },
        )
        .unwrap();
        drop(db);
        let db = Database::open(&path, &config, false).unwrap();
        let state = db.membership().unwrap();
        assert_eq!(state.retired[&4], 0);
        assert_eq!(state.identities[&4], [7; 16]);
        assert!(state.replacing.is_none());
        let mut voters = db.conf().unwrap().voters;
        voters.sort_unstable();
        assert_eq!(voters, vec![1, 2, 3]);
    }
}
