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
