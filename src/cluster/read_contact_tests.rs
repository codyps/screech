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
fn read_contacts_require_recorded_term_accepted_prefix_and_current_healthy_policy() {
    let dir = tempfile::tempdir().unwrap();
    let mut sim = Simulation::new(dir.path());
    sim.nodes[0].campaign().unwrap();
    sim.settle(None);
    let role = cache_contact_policy(&sim.nodes[1]);
    let message = contact_message(&sim.nodes[1]);
    let mut db = sim.nodes[1].store().db.lock().unwrap();
    assert!(db.can_reply_to_read_heartbeat(role, &message, [0; 16]));
    // No SQL transaction may be opened by contact validation, even while the
    // actor owns a lazy projection. An unaccepted prefix still needs that actor.
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

#[test]
fn fresh_read_contacts_can_precede_follower_commit_projection_and_survive_cold_leader_loss() {
    use raft::eraftpb::MessageType;
    for voters in [3, 5] {
        let dir = tempfile::tempdir().unwrap();
        let mut sim = Simulation::with_voters(dir.path(), voters);
        sim.nodes[0].campaign().unwrap();
        sim.settle(None);
        let previous = sim.nodes[0].raft.raft_log.committed;
        sim.propose(0, 1, b"read-contact");
        pump(
            &mut sim.nodes[0],
            &mut sim.pending[0],
            &sim.send,
            &BTreeMap::new(),
        )
        .unwrap();
        // Accept the actual publication on every voter, but retain all later
        // leader commit notifications in the transport queues.
        for follower in 1..voters as usize {
            while let Ok(message) = sim.receive[follower].try_recv() {
                sim.nodes[follower].step(message).unwrap();
            }
            pump(
                &mut sim.nodes[follower],
                &mut sim.pending[follower],
                &sim.send,
                &BTreeMap::new(),
            )
            .unwrap();
        }
        while let Ok(message) = sim.receive[0].try_recv() {
            sim.nodes[0].step(message).unwrap();
            pump(
                &mut sim.nodes[0],
                &mut sim.pending[0],
                &sim.send,
                &BTreeMap::new(),
            )
            .unwrap();
        }
        let committed = sim.nodes[0].raft.raft_log.committed;
        assert!(committed > previous);
        assert_eq!(
            sim.nodes[0]
                .store()
                .db
                .lock()
                .unwrap()
                .hard()
                .unwrap()
                .commit,
            committed
        );
        let read = sim.quorum_read(0, vec![6; 24]);
        let contacts = std::mem::take(&mut sim.nodes[0].raft.msgs);
        assert_eq!(contacts.len(), voters as usize / 2);
        for (n, contact) in contacts.into_iter().enumerate() {
            let follower = &mut sim.nodes[(contact.to - 1) as usize];
            let role = cache_contact_policy(follower);
            let db = follower.store().db.lock().unwrap();
            assert_eq!(db.hard().unwrap().commit, previous);
            assert_eq!(db.last().unwrap(), committed);
            assert_eq!(contact.commit, committed);
            assert!(db.can_reply_to_read_heartbeat(role, &contact, [0; 16]),
                "a durable accepted suffix must not require commit projection for a leadership contact");
            let mut beyond = contact.clone();
            beyond.commit += 1;
            assert!(!db.can_reply_to_read_heartbeat(role, &beyond, [0; 16]));
            let response = Message {
                from: contact.to,
                to: contact.from,
                term: contact.term,
                commit: previous,
                msg_type: MessageType::MsgHeartbeatResponse,
                context: contact.context,
                ..Default::default()
            };
            drop(db);
            // The actual Raft leader counts distinct fresh responders even
            // though each reports its older, truthful commit frontier.
            sim.nodes[0].step(response.clone()).unwrap();
            sim.nodes[0].step(response).unwrap();
            pump(
                &mut sim.nodes[0],
                &mut sim.pending[0],
                &sim.send,
                &BTreeMap::new(),
            )
            .unwrap();
            if n + 1 < voters as usize / 2 {
                assert!(matches!(read.try_recv(), Err(mpsc::TryRecvError::Empty)));
            }
        }
        read.try_recv().unwrap().unwrap();
        for follower in &sim.nodes[1..] {
            assert_eq!(
                follower.store().db.lock().unwrap().hard().unwrap().commit,
                previous
            );
        }
        // Discard every process's volatile state and undelivered commit notice.
        // The former leader stays isolated; accepted bytes on the survivors
        // must elect a leader and recover the exact publication without it.
        drop(sim);
        let mut reopened = Simulation::with_voters(dir.path(), voters);
        reopened.ticks(30, Some(1));
        let leader = reopened
            .nodes
            .iter()
            .skip(1)
            .find(|node| node.raft.state == StateRole::Leader)
            .unwrap();
        let db = leader.store().db.lock().unwrap();
        assert!(db.hard().unwrap().commit >= committed);
        assert_eq!(
            db.content(ContentKey {
                generation: 1,
                sequence: 1
            })
            .unwrap()
            .unwrap()
            .payload
            .as_ref(),
            b"read-contact"
        );
    }
}

#[tokio::test]
async fn read_contact_network_path_is_nonblocking_and_preserves_authenticated_context() {
    for (voters, accepted_ahead) in [(3, false), (3, true), (5, false), (5, true)] {
        let dir = tempfile::tempdir().unwrap();
        let mut sim = Simulation::with_voters(dir.path(), voters);
        sim.nodes[0].campaign().unwrap();
        sim.settle(None);
        let role = cache_contact_policy(&sim.nodes[1]);
        let mut message = contact_message(&sim.nodes[1]);
        let recorded_commit = message.commit;
        if accepted_ahead {
            let mut db = sim.nodes[1].store().db.lock().unwrap();
            db.persist(
                &[raft::eraftpb::Entry {
                    index: recorded_commit + 1,
                    term: role.term,
                    ..Default::default()
                }],
                None,
            )
            .unwrap();
            message.commit += 1;
        }
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
        assert_eq!(response.commit, recorded_commit);
        assert_eq!(db.lock().unwrap().hard().unwrap().commit, recorded_commit);
        sim.nodes[0]
            .raft
            .mut_prs()
            .get_mut(2)
            .unwrap()
            .committed_index = 0;
        sim.nodes[0].step(response.clone()).unwrap();
        assert_eq!(
            sim.nodes[0].raft.prs().get(2).unwrap().committed_index,
            recorded_commit
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
