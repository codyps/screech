#[test]
fn a_briefly_partitioned_voter_catches_up_from_recent_committed_entries_after_compaction() {
    use crate::storage::{CheckpointImage, MemoryStore, Store};
    use raft::Storage as _;
    for voters in [3, 5] {
        let dir = tempfile::tempdir().unwrap();
        let mut sim = Simulation::with_voters(dir.path(), voters);
        sim.nodes[0].campaign().unwrap();
        sim.settle(None);
        for _ in 0..10 {
            sim.nodes[0].propose(vec![], vec![]).unwrap();
            sim.settle(Some(voters));
        }
        let committed = sim.nodes[0].raft.raft_log.committed;
        assert!(sim.nodes[(voters - 1) as usize].raft.raft_log.committed < committed);
        let mut app = crate::durable::Engine::recover(MemoryStore::default()).unwrap();
        app.checkpoint().unwrap();
        let data = app.into_store().load_checkpoint().unwrap().unwrap().1.to_vec();
        sim.nodes[0].store().db.lock().unwrap().compact(0, CheckpointImage {
            data,
            live: vec![],
            imports: vec![],
        }).unwrap();
        assert_eq!(sim.nodes[0].store().snapshot(0, voters).unwrap().get_metadata().index, committed);
        // Reopen the follower's append window just as a real heartbeat reply
        // does, then inspect the actual Raft output before delivering it.
        let term = sim.nodes[0].raft.term;
        sim.nodes[0].step(Message {
            from: voters,
            to: 1,
            term,
            msg_type: raft::eraftpb::MessageType::MsgHeartbeatResponse,
            ..Default::default()
        }).unwrap();
        pump(&mut sim.nodes[0], &mut sim.pending[0], &sim.send, &BTreeMap::new()).unwrap();
        let mut sent = Vec::new();
        for receive in &mut sim.receive {
            while let Ok(message) = receive.try_recv() { sent.push(message); }
        }
        assert!(sent.iter().any(|m| m.to == voters
            && m.get_msg_type() == raft::eraftpb::MessageType::MsgAppend
            && !m.entries.is_empty()));
        assert!(sent.iter().all(|m| m.get_msg_type() != raft::eraftpb::MessageType::MsgSnapshot));
        for message in sent {
            sim.nodes[(message.to - 1) as usize].step(message).unwrap();
        }
        sim.settle(None);
        assert!(sim.nodes.iter().all(|node| node.raft.raft_log.committed == committed));
    }
}

#[test]
fn replication_prefers_a_real_caught_up_quorum_and_keeps_outstanding_appends_on_window_changes() {
    let dir = tempfile::tempdir().unwrap();
    let mut sim = Simulation::with_voters(dir.path(), 5);
    sim.nodes[0].campaign().unwrap();
    sim.settle(None);
    let node = &mut sim.nodes[0];
    assert_eq!(
        replication::foreground(node, &[]).unwrap(),
        [2, 3].into_iter().collect()
    );
    let lagging = node.raft.mut_prs().get_mut(4).unwrap();
    assert_eq!(lagging.ins.count(), 0);
    lagging.ins.add(100);
    assert!(lagging.ins.full());
    // A different caught-up voter must take over without forgetting an already
    // outstanding append on the newly selected peer.
    node.raft.mut_prs().get_mut(2).unwrap().matched = 0;
    assert_eq!(
        replication::foreground(node, &[]).unwrap(),
        [3, 4].into_iter().collect()
    );
    let promoted = node.raft.prs().get(4).unwrap();
    assert_eq!(promoted.ins.count(), 1);
    assert!(!promoted.ins.full());
    node.raft.mut_prs().get_mut(3).unwrap().matched = 0;
    node.raft.mut_prs().get_mut(4).unwrap().matched = 0;
    assert!(
        replication::foreground(node, &[]).is_none(),
        "insufficient caught-up voters were paced"
    );
    assert_eq!(node.raft.prs().get(4).unwrap().ins.count(), 1);
}

#[test]
fn replication_pacing_is_disabled_for_learners_membership_entries_snapshots_and_followers() {
    use raft::eraftpb::{
        ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, Entry, EntryType,
        MessageType,
    };
    let dir = tempfile::tempdir().unwrap();
    let mut sim = Simulation::new(dir.path());
    sim.nodes[0].campaign().unwrap();
    sim.settle(None);
    assert!(replication::foreground(&mut sim.nodes[0], &[]).is_some());
    let configuration = Message {
        msg_type: MessageType::MsgAppend,
        entries: vec![Entry {
            entry_type: EntryType::EntryConfChangeV2,
            ..Default::default()
        }]
        .into(),
        ..Default::default()
    };
    assert!(replication::foreground(&mut sim.nodes[0], &[configuration]).is_none());
    assert!(replication::foreground(
        &mut sim.nodes[0],
        &[Message {
            msg_type: MessageType::MsgSnapshot,
            ..Default::default()
        }]
    )
    .is_none());
    assert!(replication::foreground(&mut sim.nodes[1], &[]).is_none());
    sim.nodes[0]
        .apply_conf_change(&ConfChangeV2 {
            changes: vec![ConfChangeSingle {
                node_id: 4,
                change_type: ConfChangeType::AddLearnerNode,
                ..Default::default()
            }]
            .into(),
            ..Default::default()
        })
        .unwrap();
    assert!(replication::foreground(&mut sim.nodes[0], &[]).is_none());
    sim.nodes[0]
        .apply_conf_change(&ConfChangeV2 {
            transition: ConfChangeTransition::Explicit,
            changes: vec![
                ConfChangeSingle {
                    node_id: 4,
                    change_type: ConfChangeType::AddNode,
                    ..Default::default()
                },
                ConfChangeSingle {
                    node_id: 3,
                    change_type: ConfChangeType::RemoveNode,
                    ..Default::default()
                },
            ]
            .into(),
            ..Default::default()
        })
        .unwrap();
    assert!(!sim.nodes[0]
        .raft
        .prs()
        .conf()
        .to_conf_state()
        .voters_outgoing
        .is_empty());
    assert!(replication::foreground(&mut sim.nodes[0], &[]).is_none());
}
