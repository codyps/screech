//! Keep a caught-up quorum prompt while other voters batch their accepted suffix.
use super::*;
use std::collections::BTreeSet;

pub(super) fn foreground(
    node: &mut RawNode<LogStorage>,
    messages: &[Message],
) -> Option<BTreeSet<u64>> {
    let conf = node.raft.prs().conf().to_conf_state();
    let stable = node.raft.state == StateRole::Leader
        && conf.voters.len() >= 3
        && conf.voters.contains(&node.raft.id)
        && conf.voters_outgoing.is_empty()
        && conf.learners.is_empty()
        && conf.learners_next.is_empty()
        && messages.iter().all(|m| {
            m.get_msg_type() != raft::eraftpb::MessageType::MsgSnapshot
                && m.entries
                    .iter()
                    .all(|entry| entry.get_entry_type() == raft::eraftpb::EntryType::EntryNormal)
        });
    let mut peers: Vec<_> = conf
        .voters
        .iter()
        .filter_map(|&id| {
            let progress = node.raft.prs().get(id)?;
            (id != node.raft.id
                && progress.recent_active
                && progress.matched >= node.raft.raft_log.committed)
                .then_some((id, progress.matched))
        })
        .collect();
    peers.sort_unstable_by_key(|&(id, matched)| (std::cmp::Reverse(matched), id));
    let needed = conf.voters.len() / 2;
    let foreground = (stable && peers.len() >= needed).then(|| {
        peers
            .into_iter()
            .take(needed)
            .map(|(id, _)| id)
            .collect::<BTreeSet<_>>()
    });
    let leader = node.raft.id;
    for (&id, progress) in node.raft.mut_prs().iter_mut() {
        // Raft safely defers a smaller capacity until existing appends drain.
        // The next ACK then sends the whole pending suffix in one append.
        progress.ins.set_cap(
            if id != leader
                && foreground
                    .as_ref()
                    .is_some_and(|peers| !peers.contains(&id))
            {
                1
            } else {
                REPLICATION_WINDOW
            },
        );
    }
    foreground
}
