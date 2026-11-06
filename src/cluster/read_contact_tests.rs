fn cache_contact_policy(node: &RawNode<LogStorage>) -> Role {
    let mut db = node.store().db.lock().unwrap();
    let policy = Arc::new(db.sender_policy().unwrap());
    let configuration = db.conf_index().unwrap();
    db.cache_heartbeat_policy(policy, configuration);
    Role {
        leader: node.raft.leader_id,
        term: node.raft.term,
        configuration,
    }
}

fn contact_message(node: &RawNode<LogStorage>) -> Message {
    Message {
        from: node.raft.leader_id,
        to: node.raft.id,
        term: node.raft.term,
        commit: node.raft.raft_log.committed,
        msg_type: raft::eraftpb::MessageType::MsgHeartbeat,
        context: vec![7; 24].into(),
        ..Default::default()
    }
}

#[test]
fn read_contacts_require_recorded_term_commit_and_current_healthy_policy() {
    let dir = tempfile::tempdir().unwrap();
    let mut sim = Simulation::new(dir.path());
    sim.nodes[0].campaign().unwrap();
    sim.settle(None);
    let role = cache_contact_policy(&sim.nodes[1]);
    let message = contact_message(&sim.nodes[1]);
    let mut db = sim.nodes[1].store().db.lock().unwrap();
    assert!(db.can_reply_to_read_heartbeat(role, &message, [0; 16]));
    // No SQL transaction may be opened by contact validation, even while the
    // actor owns a lazy projection. A newer commit must go through that actor.
    db.begin_lazy_projection().unwrap();
    assert!(db.can_reply_to_read_heartbeat(role, &message, [0; 16]));
    assert!(db.conn.is_autocommit());
    for kind in 0..7 {
        let mut invalid = message.clone();
        match kind {
            0 => invalid.commit += 1,
            1 => invalid.term += 1,
            2 => invalid.to = 3,
            3 => invalid.context.clear(),
            4 => invalid.from = 3,
            5 => invalid.entries.push(raft::eraftpb::Entry::default()),
            _ => invalid.msg_type = raft::eraftpb::MessageType::MsgAppend,
        }
        assert!(
            !db.can_reply_to_read_heartbeat(role, &invalid, [0; 16]),
            "case {kind}"
        );
    }
    assert!(!db.can_reply_to_read_heartbeat(role, &message, [1; 16]));
    assert!(!db.can_reply_to_read_heartbeat(
        Role {
            configuration: role.configuration + 1,
            ..role
        },
        &message,
        [0; 16]
    ));
    db.failed = true;
    assert!(!db.can_reply_to_read_heartbeat(role, &message, [0; 16]));
    db.failed = false;
    db.membership_revision += 1;
    assert!(!db.can_reply_to_read_heartbeat(role, &message, [0; 16]));
    db.membership_revision -= 1;
    db.finish_projection(false).unwrap();
    assert!(
        !db.can_reply_to_read_heartbeat(role, &message, [0; 16]),
        "rolled back scope must invalidate accepted frontier"
    );
    drop(db);
    drop(sim);

    // Restart reconstructs the contact frontier from authenticated storage;
    // persisting a newer term fences the old role before role publication.
    let mut reopened = Database::open(&dir.path().join("2"), &config(2), true).unwrap();
    let policy = Arc::new(reopened.sender_policy().unwrap());
    reopened.cache_heartbeat_policy(policy, role.configuration);
    assert!(reopened.can_reply_to_read_heartbeat(role, &message, [0; 16]));
    let mut hard = reopened.hard().unwrap();
    hard.term += 1;
    hard.vote = 3;
    reopened.persist(&[], Some(&hard)).unwrap();
    assert!(!reopened.can_reply_to_read_heartbeat(role, &message, [0; 16]));
}

#[test]
fn early_read_contacts_still_require_distinct_fresh_quorum_responses() {
    use raft::eraftpb::MessageType;
    for voters in [3, 5] {
        let dir = tempfile::tempdir().unwrap();
        let mut sim = Simulation::with_voters(dir.path(), voters);
        sim.nodes[0].campaign().unwrap();
        sim.settle(None);
        let rx = sim.quorum_read(0, vec![8; 24]);
        let heartbeats = std::mem::take(&mut sim.nodes[0].raft.msgs);
        assert_eq!(heartbeats.len(), voters as usize / 2);
        let mut responses = Vec::new();
        for heartbeat in heartbeats {
            let follower = &mut sim.nodes[(heartbeat.to - 1) as usize];
            let role = cache_contact_policy(follower);
            assert!(follower
                .store()
                .db
                .lock()
                .unwrap()
                .can_reply_to_read_heartbeat(role, &heartbeat, [0; 16]));
            let response = Message {
                from: heartbeat.to,
                to: 1,
                term: heartbeat.term,
                commit: heartbeat.commit,
                msg_type: MessageType::MsgHeartbeatResponse,
                context: heartbeat.context.clone(),
                ..Default::default()
            };
            step_peer_message(follower, heartbeat, true).unwrap();
            assert!(
                follower.raft.msgs.is_empty(),
                "actor emitted a duplicate contact response"
            );
            responses.push(response);
        }
        for (n, response) in responses.iter().enumerate() {
            // Duplicating a real voter response cannot substitute for another
            // voter in the five-voter quorum.
            sim.nodes[0].step(response.clone()).unwrap();
            sim.nodes[0].step(response.clone()).unwrap();
            pump(
                &mut sim.nodes[0],
                &mut sim.pending[0],
                &sim.send,
                &BTreeMap::new(),
            )
            .unwrap();
            if n + 1 < responses.len() {
                assert!(matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
            }
        }
        rx.try_recv().unwrap().unwrap();
        sim.settle(None);
        let next = sim.quorum_read(0, vec![9; 24]);
        sim.nodes[0].raft.msgs.clear();
        for response in responses {
            sim.nodes[0].step(response).unwrap();
        }
        pump(
            &mut sim.nodes[0],
            &mut sim.pending[0],
            &sim.send,
            &BTreeMap::new(),
        )
        .unwrap();
        assert!(
            matches!(next.try_recv(), Err(mpsc::TryRecvError::Empty)),
            "old round released a new read"
        );
    }
}

#[test]
fn actor_keeps_responses_when_contact_has_become_a_term_or_commit_transition() {
    use raft::eraftpb::MessageType;
    let dir = tempfile::tempdir().unwrap();
    let mut sim = Simulation::new(dir.path());
    sim.nodes[0].campaign().unwrap();
    sim.settle(None);
    let mut message = contact_message(&sim.nodes[1]);
    message.term += 1;
    step_peer_message(&mut sim.nodes[1], message, true).unwrap();
    assert!(
        sim.nodes[1]
            .raft
            .msgs
            .iter()
            .any(|m| m.get_msg_type() == MessageType::MsgHeartbeatResponse),
        "higher term response was suppressed"
    );
    // A commit transition is also processed by the ordinary Ready pipeline.
    let last = sim.nodes[2].raft.raft_log.last_index();
    let term = sim.nodes[2].raft.term;
    sim.nodes[2]
        .step(Message {
            from: 1,
            to: 3,
            term,
            log_term: term,
            index: last,
            commit: last,
            msg_type: MessageType::MsgAppend,
            entries: vec![raft::eraftpb::Entry {
                index: last + 1,
                term,
                ..Default::default()
            }]
            .into(),
            ..Default::default()
        })
        .unwrap();
    sim.nodes[2].raft.msgs.clear();
    let mut message = contact_message(&sim.nodes[2]);
    message.commit += 1;
    step_peer_message(&mut sim.nodes[2], message, true).unwrap();
    assert!(sim.nodes[2]
        .raft
        .msgs
        .iter()
        .any(|m| m.get_msg_type() == MessageType::MsgHeartbeatResponse));
}

#[tokio::test]
async fn read_contact_network_path_is_nonblocking_and_preserves_authenticated_context() {
    for voters in [3, 5] {
        let dir = tempfile::tempdir().unwrap();
        let mut sim = Simulation::with_voters(dir.path(), voters);
        sim.nodes[0].campaign().unwrap();
        sim.settle(None);
        let role = cache_contact_policy(&sim.nodes[1]);
        let message = contact_message(&sim.nodes[1]);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (tx, _rx) = mpsc::sync_channel(8);
        let (stop, stop_rx) = watch::channel(false);
        let (role_tx, role_rx) = watch::channel(role);
        let sender = transport::PeerSender::start(
            config(2),
            listener.local_addr().unwrap(),
            tx.clone(),
            stop_rx,
            [0; 16],
        );
        let db = sim.nodes[1].store().db.clone();
        let active = startup::Active {
            config: config(2),
            tx,
            role: role_rx,
            db: db.clone(),
            incarnation: [0; 16],
            session: Arc::new(AtomicU64::new(1)),
            peer_senders: Arc::new([(1, sender)].into_iter().collect()),
        };
        {
            let _busy = db.lock().unwrap();
            assert!(
                !reply_to_read_heartbeat(&active, &message, [0; 16]),
                "socket path blocked on the owner"
            );
        }
        let gate = db.lock().unwrap().gate.clone();
        {
            let _projection = gate.lock().unwrap();
            assert!(reply_to_read_heartbeat(&active, &message, [0; 16]));
        }
        let (mut stream, _) = tokio::time::timeout(Duration::from_secs(2), listener.accept())
            .await
            .unwrap()
            .unwrap();
        let bytes = Arc::new(Semaphore::new(64 * 1024 * 1024));
        let first = stream.read_u32().await.unwrap();
        let (response, nonce, _permit) =
            receive_message(&config(1), &mut stream, bytes, Some(first))
                .await
                .unwrap();
        assert_eq!(nonce, [0; 16]);
        assert!(message.commit > 0);
        assert_eq!(response.commit, message.commit);
        sim.nodes[0]
            .raft
            .mut_prs()
            .get_mut(2)
            .unwrap()
            .committed_index = 0;
        sim.nodes[0].step(response.clone()).unwrap();
        assert_eq!(
            sim.nodes[0].raft.prs().get(2).unwrap().committed_index,
            message.commit
        );
        assert_eq!(response.context, message.context);
        assert_eq!(
            (response.from, response.to, response.term),
            (2, 1, role.term)
        );
        assert_eq!(
            response.get_msg_type(),
            raft::eraftpb::MessageType::MsgHeartbeatResponse
        );
        drop(role_tx);
        assert!(!reply_to_read_heartbeat(&active, &message, [0; 16]));
        stop.send(true).unwrap();
    }
}
