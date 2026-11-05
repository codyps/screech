//! Raft cluster with snapshot catch-up and learner-based voter replacement.
//! Clients at every node reach the elected
//! leader's NATS service. Connections end on a view change and clients reconnect.
//! All durable operations and current reads cross the same consensus boundary.
mod membership;
mod replication;
mod startup;
mod storage;
mod transport;
use crate::{
    server::{Config as ServerConfig, Server},
    storage::{Append, CheckpointJob, Content, ContentKey, Record, Replication, Store, StoreError},
};
use bytes::Bytes;
use hmac::{Hmac, Mac};
use protobuf::Message as ProtoMessage;
use raft::{eraftpb::Message, RawNode, StateRole};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
pub use startup::run;
use std::{
    collections::BTreeMap,
    io,
    net::SocketAddr,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc, Arc, Mutex,
    },
    time::{Duration, Instant},
};
use storage::{Database, LogStorage};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::{watch, Semaphore},
};
const MAX_FRAME: usize = 40 * 1024 * 1024;
const RPC_TIMEOUT: Duration = Duration::from_secs(4);
pub(crate) fn error(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(e.to_string())
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Peer {
    pub raft: SocketAddr,
    pub client: SocketAddr,
    pub service: SocketAddr,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    pub cluster_id: String,
    pub node_id: u64,
    pub secret: String,
    pub peers: BTreeMap<u64, Peer>,
    #[serde(default = "log_budget")]
    pub max_log_bytes: u64,
    #[serde(default)]
    pub initial_voters: Vec<u64>,
}
fn log_budget() -> u64 {
    512 * 1024 * 1024
}
impl ClusterConfig {
    pub fn voters(&self) -> Vec<u64> {
        if self.initial_voters.is_empty() {
            self.peers.keys().copied().collect()
        } else {
            self.initial_voters.clone()
        }
    }
    fn slot_for(&self, member: u64, identities: &BTreeMap<u64, [u8; 16]>) -> Option<u64> {
        if self.peers.contains_key(&member) {
            Some(member)
        } else {
            identities
                .get(&member)
                .map(|n| u64::from_le_bytes(n[..8].try_into().unwrap()))
                .filter(|slot| self.peers.contains_key(slot))
        }
    }
    pub fn validate(&self) -> io::Result<()> {
        if self.max_log_bytes < 2 * MAX_FRAME as u64 {
            return Err(error(
                "cluster log budget must be at least 80 MiB for checkpoint headroom",
            ));
        }
        if self.cluster_id.is_empty()
            || self.secret.len() < 32
            || !self.peers.contains_key(&self.node_id)
            || self.peers.keys().any(|id| *id == 0)
            || (!matches!(self.voters().len(), 3 | 5)
                || self.voters().iter().any(|id| !self.peers.contains_key(id))
                || self
                    .voters()
                    .iter()
                    .collect::<std::collections::BTreeSet<_>>()
                    .len()
                    != self.voters().len())
        {
            return Err(error("cluster requires an identity, secret of at least 32 bytes, and three or five distinct nonzero voter IDs"));
        }
        let mut addresses = std::collections::BTreeSet::new();
        for peer in self.peers.values() {
            for addr in [peer.raft, peer.client, peer.service] {
                if addr.port() == 0 || !addresses.insert(addr) {
                    return Err(error("cluster endpoints must be distinct fixed addresses"));
                }
            }
        }
        Ok(())
    }
}
#[derive(Serialize, Deserialize)]
pub(crate) struct Batch {
    version: u8,
    records: Vec<Append>,
    contents: Vec<Content>,
}
impl Batch {
    fn encode(records: &[Append], contents: &[Content]) -> io::Result<Vec<u8>> {
        #[derive(Serialize)]
        struct Borrowed<'a> {
            version: u8,
            records: &'a [Append],
            contents: &'a [Content],
        }
        postcard::to_allocvec(&Borrowed {
            version: 1,
            records,
            contents,
        })
        .map_err(error)
    }
    fn decode(data: &[u8]) -> io::Result<Self> {
        let b: Self = postcard::from_bytes(data).map_err(error)?;
        if b.version != 1 {
            return Err(error("unknown consensus batch version"));
        }
        Ok(b)
    }
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Role {
    leader: u64,
    term: u64,
    configuration: u64,
}
enum ConnectionRoute {
    Local(tokio::sync::mpsc::Sender<TcpStream>),
    Remote(TcpStream),
}
type Reply = mpsc::SyncSender<io::Result<u64>>;
struct Pending {
    started: Instant,
    reply: Reply,
    read_index: Option<u64>,
}
struct ReadProbe {
    deadline: Instant,
    messages: Vec<Message>,
}
const REPLICATION_WINDOW: usize = 8;
enum Input {
    Message(Message, [u8; 16], tokio::sync::OwnedSemaphorePermit),
    ReadHeartbeat(Message, [u8; 16], tokio::sync::OwnedSemaphorePermit),
    Propose {
        term: u64,
        configuration: u64,
        session: u64,
        activate: bool,
        data: Vec<u8>,
        reply: Reply,
    },
    Read {
        term: u64,
        configuration: u64,
        session: u64,
        reply: Reply,
    },
    Stop,
    Fault,
    Replace {
        term: u64,
        configuration: u64,
        session: u64,
        replacement: membership::Replacement,
        reply: Reply,
    },
    SnapshotSent(u64, bool),
    Enroll {
        slot: u64,
        member: u64,
        nonce: [u8; 16],
        term: u64,
        reply: Reply,
    },
}
#[derive(Clone)]
struct Client {
    tx: mpsc::SyncSender<Input>,
    role: watch::Receiver<Role>,
    db: Arc<Mutex<Database>>,
    id: u64,
    term: u64,
    replicas: usize,
    configuration: u64,
    session: u64,
    active_session: Arc<AtomicU64>,
}
impl Client {
    fn barrier(&self) -> io::Result<()> {
        self.current()?;
        let (reply, rx) = mpsc::sync_channel(1);
        self.tx
            .try_send(Input::Read {
                term: self.term,
                configuration: self.configuration,
                session: self.session,
                reply,
            })
            .map_err(error)?;
        rx.recv_timeout(RPC_TIMEOUT)
            .map_err(|_| error("quorum read timeout"))??;
        // The owner checks its durable epoch and publishes the committed role
        // before releasing ReadIndex replies. Reject a replaced service without
        // another SQL round trip on the application worker.
        self.current()
    }
    fn propose(&self, data: Vec<u8>) -> io::Result<u64> {
        self.request(data, false)
    }
    fn activate(&self) -> io::Result<u64> {
        self.request(Vec::new(), true)
    }
    fn request(&self, data: Vec<u8>, activate: bool) -> io::Result<u64> {
        let role = *self.role.borrow();
        if role.leader != self.id
            || role.term != self.term
            || role.configuration != self.configuration
            || (!activate && self.active_session.load(Ordering::Acquire) != self.session)
        {
            return Err(error("leadership changed"));
        }
        let (reply, rx) = mpsc::sync_channel(1);
        self.tx
            .try_send(Input::Propose {
                term: self.term,
                configuration: self.configuration,
                session: self.session,
                activate,
                data,
                reply,
            })
            .map_err(error)?;
        rx.recv_timeout(RPC_TIMEOUT)
            .map_err(|_| error("consensus timeout; outcome unknown"))?
    }
    fn read<R>(&self, f: impl FnOnce(&mut Database) -> io::Result<R>) -> io::Result<R> {
        self.current()?;
        let gate = self.db.lock().unwrap().gate.clone();
        let _gate = gate.lock().unwrap();
        let mut db = self.db.lock().unwrap();
        // A configuration/term can become durable before the watch is published,
        // or while this caller waits for storage. This is stale work, not damage.
        self.current()?;
        if db.failed {
            return Err(error("replica quarantined"));
        }
        // Epoch checks and all content lookups share a SQLite read snapshot.
        // The gate excludes the owner's uncommitted projection and checkpoints;
        // FULL checkpoint transactions can still suspend and resume this scope.
        let mut stale = false;
        let result = (|| {
            db.begin_projection()?;
            if db.hard()?.term != self.term || db.conf_index()? != self.configuration {
                stale = true;
                return Err(error("application epoch changed"));
            }
            f(&mut db)
        })();
        let finished = db.finish_projection(result.is_ok());
        if (result.is_err() && !stale) || finished.is_err() {
            db.failed = true;
            let _ = self.tx.try_send(Input::Fault);
        }
        finished.and(result)
    }
    fn recovery_failed(&self, failure: &crate::durable::Error) {
        use crate::durable::Error;
        if matches!(
            failure,
            Error::Invalid(_)
                | Error::Json(_)
                | Error::Codec(_)
                | Error::Store(StoreError::Corrupt(_) | StoreError::Invalid)
        ) {
            // A malformed model/replay must not leave an unavailable leader
            // heartbeating forever. Epoch checks prevent a retired service from
            // quarantining a healthy successor. Quorum/timeouts are retried.
            let _ = self.read::<()>(|_| Err(error(failure)));
        }
    }
    fn current(&self) -> io::Result<()> {
        if self.active_session.load(Ordering::Acquire) != self.session
            || *self.role.borrow()
                != (Role {
                    leader: self.id,
                    term: self.term,
                    configuration: self.configuration,
                })
        {
            Err(error("leadership changed"))
        } else {
            Ok(())
        }
    }
}
struct ClusterStore {
    client: Client,
}
impl Replication for Client {
    fn read_barrier(&self) -> Result<(), StoreError> {
        self.barrier().map_err(StoreError::from)
    }
    fn append_with_content(
        &self,
        records: &[Append],
        contents: &[Content],
    ) -> Result<u64, StoreError> {
        let data = Batch::encode(records, contents)?;
        if data.len() > 32 * 1024 * 1024 {
            return Err(StoreError::Invalid);
        }
        self.propose(data).map_err(StoreError::from)
    }
    fn checkpoint_needed(&self) -> Result<bool, StoreError> {
        self.current()?;
        let db = self.db.lock().unwrap();
        self.current()?;
        db.checkpoint_pressure(self.configuration)
            .map_err(StoreError::from)
    }
}
impl Store for ClusterStore {
    fn replication(&self) -> Option<Arc<dyn Replication>> {
        Some(Arc::new(self.client.clone()))
    }
    fn replace_voter(&self, old: u64, new: u64, nonce: [u8; 16]) -> Result<(), StoreError> {
        self.client.current()?;
        let (reply, rx) = mpsc::sync_channel(1);
        self.client
            .tx
            .try_send(Input::Replace {
                term: self.client.term,
                configuration: self.client.configuration,
                session: self.client.session,
                replacement: membership::Replacement { old, new, nonce },
                reply,
            })
            .map_err(|e| StoreError::Io(error(e)))?;
        rx.recv_timeout(RPC_TIMEOUT)
            .map_err(|e| StoreError::Io(error(e)))?
            .map(|_| ())
            .map_err(StoreError::from)
    }
    fn cluster_status(&self) -> Result<serde_json::Value, StoreError> {
        self.client.read(|db|{
            let conf=db.conf()?;let membership=db.membership()?;
            Ok(serde_json::json!({"voters":conf.voters,"outgoing":conf.voters_outgoing,"learners":conf.learners,"retired":membership.retired,"replacement":membership.replacing,"configuration_index":db.conf_index()?,"snapshot_index":db.snapshot_base()?.0,"commit_index":db.hard()?.commit,"event_sequence":db.end()?}))
        }).map_err(StoreError::from)
    }
    fn checkpoint_needed(&self) -> Result<bool, StoreError> {
        self.client
            .read(|db| {
                Ok(db.log_bytes()? > db.max_log_bytes / 2
                    || db.journal_bytes() > db.max_log_bytes / 2
                    || db.view()?.saturating_sub(db.snapshot_base()?.0) >= 8192
                    || db.conf_index()? > db.snapshot_base()?.0)
            })
            .map_err(StoreError::from)
    }
    fn replicas(&self) -> usize {
        self.client.replicas
    }
    fn durable_appends(&self) -> bool {
        true
    }
    fn supports_batch(&self) -> bool {
        true
    }
    fn external_content(&self) -> bool {
        true
    }
    // A complete durable snapshot replaces the covered committed prefix. The
    // accepted suffix remains authoritative until consensus commits or replaces it.
    fn read_barrier(&self) -> Result<(), StoreError> {
        self.client.barrier().map_err(StoreError::from)
    }
    fn append(&mut self, s: &str, h: &[u8], p: &[u8]) -> Result<u64, StoreError> {
        self.append_with_content(
            &[Append {
                subject: s.into(),
                headers: Bytes::copy_from_slice(h),
                payload: Bytes::copy_from_slice(p),
            }],
            &[],
        )
    }
    fn append_batch(&mut self, r: &[Append]) -> Result<u64, StoreError> {
        self.append_with_content(r, &[])
    }
    fn append_with_content(&mut self, r: &[Append], c: &[Content]) -> Result<u64, StoreError> {
        Replication::append_with_content(&self.client, r, c)
    }
    fn get(&self, seq: u64) -> Result<Option<Record>, StoreError> {
        self.client
            .read(|db| db.record(seq))
            .map_err(StoreError::from)
    }
    fn last_sequence(&self) -> Result<u64, StoreError> {
        self.client.read(|db| db.end()).map_err(StoreError::from)
    }
    fn get_content(&self, key: ContentKey) -> Result<Option<Content>, StoreError> {
        // This private adapter resolves references from the committed model.
        // Absence is corruption, not an optional lookup: report it within read()
        // so the Raft replica is fenced as well as the application worker.
        self.client
            .read(|db| {
                db.content(key)?
                    .map(Some)
                    .ok_or_else(|| error("missing committed content"))
            })
            .map_err(StoreError::from)
    }
    fn get_contents(&self, keys: &[ContentKey]) -> Result<Vec<Content>, StoreError> {
        self.client
            .read(|db| db.contents(keys))
            .map_err(StoreError::from)
    }
    fn validate_content(
        &self,
        key: ContentKey,
        subject: &str,
        headers: u64,
        payload: u64,
    ) -> Result<(), StoreError> {
        self.client
            .read(|db| {
                let content = db
                    .content(key)?
                    .ok_or_else(|| error("missing committed content"))?;
                if content.subject != subject
                    || content.headers.len() as u64 != headers
                    || content.payload.len() as u64 != payload
                {
                    return Err(error("committed content reference mismatch"));
                }
                Ok(())
            })
            .map_err(StoreError::from)
    }
    fn purge(&mut self) -> Result<(), StoreError> {
        Err(StoreError::Invalid)
    }
    fn supports_checkpoint(&self) -> bool {
        true
    }
    fn checkpoint_job(&mut self, seq: u64, job: CheckpointJob) -> Result<(), StoreError> {
        self.client.current()?;
        let image = job()?;
        self.client
            .read(move |db| db.compact(seq, image))
            .map_err(StoreError::from)
    }
    fn load_checkpoint(&self) -> Result<Option<(u64, Bytes)>, StoreError> {
        self.client
            .read(
                |db| match (db.meta("app_sequence")?, db.meta("app_snapshot")?) {
                    (Some(seq), Some(data)) => {
                        let n = u64::from_le_bytes(
                            seq.try_into()
                                .map_err(|_| error("invalid checkpoint position"))?,
                        );
                        if n > db.end()? {
                            return Err(error("checkpoint ahead of committed events"));
                        }
                        let expected = db
                            .meta("app_crc")?
                            .ok_or_else(|| error("missing checkpoint checksum"))?;
                        let mut checksum = crc32fast::Hasher::new();
                        checksum.update(&n.to_le_bytes());
                        checksum.update(&data);
                        if expected.as_slice() != checksum.finalize().to_le_bytes() {
                            return Err(error("checkpoint checksum mismatch"));
                        }
                        Ok(Some((n, data.into())))
                    }
                    (None, None) => Ok(None),
                    _ => Err(error("incomplete checkpoint")),
                },
            )
            .map_err(StoreError::from)
    }
}
#[cfg(test)]
fn pump(
    node: &mut RawNode<LogStorage>,
    pending: &mut BTreeMap<Vec<u8>, Pending>,
    out: &BTreeMap<u64, tokio::sync::mpsc::Sender<Message>>,
    routes: &BTreeMap<u64, u64>,
) -> io::Result<()> {
    if !node.has_ready() {
        return Ok(());
    }
    project(
        node,
        |node, completed| pump_ready(node, pending, out, routes, completed),
        |_| {},
    )
}
fn project<R>(
    node: &mut RawNode<LogStorage>,
    step: impl FnOnce(&mut RawNode<LogStorage>, &mut Vec<(Reply, u64)>) -> io::Result<R>,
    publish: impl FnOnce(&R),
) -> io::Result<R> {
    let gate = node.store().db.lock().unwrap().gate.clone();
    let _gate = gate.lock().unwrap();
    {
        let mut db = node.store().db.lock().unwrap();
        if db.failed {
            return Err(error("replica quarantined"));
        }
        db.begin_lazy_projection()?;
    }
    let mut completed = Vec::new();
    let result = step(node, &mut completed);
    let finished = node
        .store()
        .db
        .lock()
        .unwrap()
        .finish_projection(result.is_ok());
    let result = finished.and(result);
    if let Err(failure) = &result {
        node.store().db.lock().unwrap().failed = true;
        for (reply, _) in completed {
            let _ = reply.send(Err(error(failure)));
        }
    } else {
        publish(result.as_ref().unwrap());
        // Complete the query projection before exposing replies. The accepted
        // payload and promises synced independently before replication ACKs.
        for (reply, end) in completed {
            let _ = reply.send(Ok(end));
        }
    }
    result
}
fn next_batch_input(
    rx: &mpsc::Receiver<Input>,
    deferred: &mut Option<Input>,
    over_budget: bool,
    term: u64,
) -> Option<Input> {
    let input = rx.try_recv().ok()?;
    // A synchronous accepted-journal flush can consume the time budget while
    // quorum replies or the leader's commit notification arrive. Drain those
    // already queued completions before COMMIT so accepted entries and their
    // projection can share one transaction, on followers as well as the leader.
    // Never wait, skip an input, accept new entries past the time budget, or
    // carry a new term across this boundary. The eight-input cap still applies.
    if over_budget && !completes_projection(&input, term) {
        *deferred = Some(input);
        None
    } else {
        Some(input)
    }
}
fn completes_projection(input: &Input, term: u64) -> bool {
    matches!(input, Input::Message(message, _, _) | Input::ReadHeartbeat(message, _, _)
        if message.term == term && (matches!(message.get_msg_type(),
            raft::eraftpb::MessageType::MsgAppendResponse |
            raft::eraftpb::MessageType::MsgHeartbeatResponse |
            raft::eraftpb::MessageType::MsgHeartbeat)
            || (message.get_msg_type() == raft::eraftpb::MessageType::MsgAppend
                && message.entries.is_empty())))
}
fn step_peer_message(
    node: &mut RawNode<LogStorage>,
    message: Message,
    read_replied: bool,
) -> raft::Result<()> {
    let acknowledged = (read_replied
        && message.get_msg_type() == raft::eraftpb::MessageType::MsgHeartbeat
        && message.term == node.raft.term
        && message.from == node.raft.leader_id
        && message.commit <= node.raft.raft_log.committed)
        .then(|| (message.from, message.term, message.context.clone()));
    node.step(message)?;
    if let Some((to, term, context)) = acknowledged {
        // The socket path already sent this exact contact response. Still step
        // the heartbeat for election timers and normal Raft bookkeeping, keeping
        // any higher-term response and every persistence-dependent message.
        node.raft.msgs.retain(|message| {
            !(message.get_msg_type() == raft::eraftpb::MessageType::MsgHeartbeatResponse
                && message.to == to
                && message.term == term
                && message.context == context)
        });
    }
    Ok(())
}
fn reply_to_read_heartbeat(active: &startup::Active, message: &Message, nonce: [u8; 16]) -> bool {
    use transport::MessageSender;
    if active.role.has_changed().is_err() {
        return false;
    }
    let role = *active.role.borrow();
    {
        let db = match active.db.try_lock() {
            Ok(db) => db,
            Err(_) => return false,
        };
        if !db.can_reply_to_read_heartbeat(role, message, nonce) {
            return false;
        }
    }
    let slot = if active.config.peers.contains_key(&message.from) {
        message.from
    } else {
        u64::from_le_bytes(nonce[..8].try_into().unwrap())
    };
    let Some(sender) = active.peer_senders.get(&slot) else {
        return false;
    };
    sender
        .try_send(Message {
            from: active.config.node_id,
            to: message.from,
            term: message.term,
            msg_type: raft::eraftpb::MessageType::MsgHeartbeatResponse,
            context: message.context.clone(),
            ..Default::default()
        })
        .is_ok()
}
fn follower_commit_input(
    rx: &mpsc::Receiver<Input>,
    deferred: &mut Option<Input>,
    term: u64,
    deadline: Instant,
) -> Option<Input> {
    let input = rx
        .recv_timeout(deadline.saturating_duration_since(Instant::now()))
        .ok()?;
    // Holding a follower's query transaction never delays a newly queued
    // acceptance, epoch change or control request behind the grace period.
    if !completes_projection(&input, term) {
        *deferred = Some(input);
        None
    } else {
        Some(input)
    }
}
fn start_read(node: &mut RawNode<LogStorage>, context: &[u8], round: usize) -> Vec<Message> {
    node.read_index(context.to_vec());
    let conf = node.raft.prs().conf().to_conf_state();
    // During membership changes leave Raft's full joint-quorum fanout intact.
    if !conf.voters_outgoing.is_empty()
        || !conf.learners.is_empty()
        || !conf.learners_next.is_empty()
        || !conf.voters.contains(&node.raft.id)
    {
        return Vec::new();
    }
    let needed = conf.voters.len() / 2;
    let committed = node.raft.raft_log.committed;
    let mut peers = conf.voters;
    // A just-committed publication can leave other followers still persisting
    // its entry. Their earlier replication acknowledgements only help select
    // a fast initial quorum; this read still needs new context-bound replies.
    peers.retain(|id| {
        *id != node.raft.id
            && node
                .raft
                .prs()
                .get(*id)
                .is_some_and(|progress| progress.recent_active && progress.matched >= committed)
    });
    peers.sort_unstable();
    if peers.len() < needed || peers.is_empty() {
        return Vec::new();
    }
    let start = round % peers.len();
    // Suppress only this newly created round's redundant initial probes.
    // Periodic bcast_heartbeat reissues the pending context to ALL voters,
    // so an unavailable selected peer cannot strand a read. Raft still counts
    // distinct current-voter acknowledgements for every fresh read context.
    let mut deferred = Vec::new();
    node.raft.msgs.retain_mut(|message| {
        let selected = message.get_msg_type() != raft::eraftpb::MessageType::MsgHeartbeat
            || message.get_context() != context
            || (0..needed).any(|n| peers[(start + n) % peers.len()] == message.to);
        if !selected {
            deferred.push(std::mem::take(message));
        }
        selected
    });
    deferred
}
fn hedge_reads(
    node: &mut RawNode<LogStorage>,
    pending: &BTreeMap<Vec<u8>, Pending>,
    probes: &mut BTreeMap<Vec<u8>, ReadProbe>,
    now: Instant,
) {
    probes.retain(|context, probe| {
        if node.raft.state != StateRole::Leader
            || pending
                .get(context)
                .is_none_or(|request| request.read_index.is_some())
            || probe
                .messages
                .iter()
                .any(|message| message.term != node.raft.term)
        {
            return false;
        }
        if now < probe.deadline {
            return true;
        }
        // These are the original context-bound probes, not cached responses.
        // Raft still requires a fresh quorum and verifies its current membership.
        node.raft.msgs.append(&mut probe.messages);
        false
    });
}
fn pump_ready<S: transport::MessageSender>(
    node: &mut RawNode<LogStorage>,
    pending: &mut BTreeMap<Vec<u8>, Pending>,
    out: &BTreeMap<u64, S>,
    routes: &BTreeMap<u64, u64>,
    completed: &mut Vec<(Reply, u64)>,
) -> io::Result<()> {
    while node.has_ready() {
        let mut ready = node.ready();
        let read_states = ready.take_read_states();
        let epoch_change = !ready.snapshot().is_empty()
            || if let Some(hard) = ready.hs() {
                let old = node.store().db.lock().unwrap().recorded_hard()?;
                hard.term != old.term || hard.vote != old.vote
            } else {
                false
            };
        let mut early = ready.take_messages();
        if !epoch_change {
            // Raft marks leader replication messages as independent of local
            // entry persistence. Followers can flush in parallel with us; our
            // own durable acceptance and every follower ACK remain fenced.
            dispatch_messages(node, std::mem::take(&mut early), out, routes)?;
        }
        {
            let mut db = node.store().db.lock().unwrap();
            if ready.snapshot().is_empty() {
                db.persist(ready.entries(), ready.hs())?;
            } else {
                db.install_snapshot(ready.snapshot(), ready.hs(), ready.entries(), false)?;
            }
        }
        early.extend(ready.take_persisted_messages());
        dispatch_messages(node, early, out, routes)?;
        // Applying the durable hard-state prefix builds the application indexes.
        // Raft's committed entry lists may be split across Ready and LightReady.
        let commit = node.raft.raft_log.committed;
        if commit > node.raft.raft_log.applied {
            let entries = membership::apply_configuration(node, commit)?;
            let replies = node
                .store()
                .db
                .lock()
                .unwrap()
                .apply_entries(commit, entries)?;
            for (key, end) in replies {
                if let Some(request) = pending.remove(&key) {
                    completed.push((request.reply, end));
                }
            }
        }
        let mut light = node.advance(ready);
        if let Some(commit) = light.commit_index() {
            let mut hard = node.store().db.lock().unwrap().recorded_hard()?;
            hard.commit = commit;
            node.store().db.lock().unwrap().persist(&[], Some(&hard))?;
            let entries = membership::apply_configuration(node, commit)?;
            let mut db = node.store().db.lock().unwrap();
            for (key, end) in db.apply_entries(commit, entries)? {
                if let Some(request) = pending.remove(&key) {
                    completed.push((request.reply, end));
                }
            }
        }
        dispatch_messages(node, light.take_messages(), out, routes)?;
        // The application is represented by its committed indexes, advanced above.
        node.advance_apply();
        for state in read_states {
            if let Some(request) = pending.get_mut(&state.request_ctx) {
                request.read_index = Some(state.index);
            }
        }
        // advance_apply follows completed projection writes above. Heartbeats
        // and ReadIndex responses do not need to reread an unchanged SQL view.
        let view = node.raft.raft_log.applied;
        if pending
            .values()
            .any(|request| request.read_index.is_some_and(|index| index <= view))
            && node.store().db.lock().unwrap().durable_term()? != node.raft.term
        {
            return Err(error("durable read epoch does not match Raft"));
        }
        pending.retain(|_, request| {
            if request.read_index.is_some_and(|index| index <= view) {
                completed.push((request.reply.clone(), view));
                false
            } else {
                true
            }
        });
    }
    Ok(())
}
fn dispatch_messages<S: transport::MessageSender>(
    node: &mut RawNode<LogStorage>,
    messages: Vec<Message>,
    out: &BTreeMap<u64, S>,
    routes: &BTreeMap<u64, u64>,
) -> io::Result<()> {
    if !messages.is_empty() {
        let foreground = replication::foreground(node, &messages);
        let mut identities = None;
        for message in messages {
            let background = message.get_msg_type() == raft::eraftpb::MessageType::MsgAppend
                && foreground
                    .as_ref()
                    .is_some_and(|peers| !peers.contains(&message.to));
            let route = if out.contains_key(&message.to) {
                message.to
            } else {
                if identities.is_none() {
                    identities = Some(node.store().db.lock().unwrap().membership()?.identities);
                }
                identities
                    .as_ref()
                    .unwrap()
                    .get(&message.to)
                    .map(|n| u64::from_le_bytes(n[..8].try_into().unwrap()))
                    .or_else(|| routes.get(&message.to).copied())
                    .unwrap_or(message.to)
            };
            if let Some(tx) = out.get(&route) {
                let _ = if background {
                    tx.try_send_background(message)
                } else {
                    tx.try_send(message)
                };
            }
        }
    }
    Ok(())
}
#[derive(Serialize, Deserialize)]
struct Envelope<'a> {
    cluster: &'a str,
    from: u64,
    body: &'a [u8],
    mac: &'a [u8],
    incarnation: [u8; 16],
}
fn authenticate(secret: &str, cluster: &str, from: u64, body: &[u8]) -> Hmac<Sha256> {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(&(cluster.len() as u64).to_le_bytes());
    mac.update(cluster.as_bytes());
    mac.update(&from.to_le_bytes());
    mac.update(body);
    mac
}
#[derive(Serialize, Deserialize)]
struct Chunk<'a> {
    nonce: [u8; 16],
    offset: u64,
    total: u64,
    hash: [u8; 32],
    data: &'a [u8],
    snapshot: bool,
}
fn envelope_frame(envelope: &Envelope<'_>) -> io::Result<Vec<u8>> {
    let mut frame = Vec::with_capacity(envelope.body.len() + envelope.cluster.len() + 128);
    frame.extend_from_slice(&[0; 4]);
    let mut frame = postcard::to_extend(envelope, frame).map_err(error)?;
    let size = u32::try_from(frame.len() - 4).map_err(error)?;
    frame[..4].copy_from_slice(&size.to_be_bytes());
    Ok(frame)
}
fn message_chunk(
    config: &ClusterConfig,
    chunk: &Chunk<'_>,
    incarnation: [u8; 16],
) -> io::Result<Vec<u8>> {
    let body = postcard::to_allocvec(chunk).map_err(error)?;
    let mut auth = authenticate(&config.secret, &config.cluster_id, config.node_id, &body);
    auth.update(&incarnation);
    let mac = auth.finalize().into_bytes();
    envelope_frame(&Envelope {
        cluster: &config.cluster_id,
        from: config.node_id,
        body: &body,
        mac: &mac,
        incarnation,
    })
}
async fn write_peer_frame(stream: &TcpStream, mut frame: &[u8]) -> io::Result<()> {
    while !frame.is_empty() {
        stream.writable().await?;
        match stream.try_write(frame) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(n) => frame = &frame[n..],
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => (),
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
async fn send_message(
    config: &ClusterConfig,
    stream: &TcpStream,
    message: &Message,
    incarnation: [u8; 16],
) -> io::Result<()> {
    let body = message.write_to_bytes().map_err(error)?;
    if body.len() > storage::MAX_SNAPSHOT + MAX_FRAME {
        return Err(error("message exceeds transfer budget"));
    }
    let hash: [u8; 32] = Sha256::digest(&body).into();
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).map_err(error)?;
    for (n, data) in body.chunks(1024 * 1024).enumerate() {
        let frame = message_chunk(
            config,
            &Chunk {
                nonce,
                offset: (n * 1024 * 1024) as u64,
                total: body.len() as u64,
                hash,
                data,
                snapshot: message.get_msg_type() == raft::eraftpb::MessageType::MsgSnapshot,
            },
            incarnation,
        )?;
        write_peer_frame(stream, &frame).await?;
    }
    Ok(())
}
async fn receive_message<R: AsyncRead + Unpin>(
    config: &ClusterConfig,
    stream: &mut R,
    bytes: Arc<Semaphore>,
    mut first: Option<u32>,
) -> io::Result<(Message, [u8; 16], tokio::sync::OwnedSemaphorePermit)> {
    let mut data = Vec::new();
    let mut identity = None;
    let mut permit = None;
    loop {
        let size = if let Some(n) = first.take() {
            n as usize
        } else {
            stream.read_u32().await? as usize
        };
        if size > 2 * 1024 * 1024 {
            return Err(error("invalid transfer chunk size"));
        }
        let mut frame = vec![0; size];
        stream.read_exact(&mut frame).await?;
        let envelope: Envelope = postcard::from_bytes(&frame).map_err(error)?;
        if envelope.cluster != config.cluster_id || envelope.from == 0 {
            return Err(error("unknown peer"));
        }
        let mut auth = authenticate(
            &config.secret,
            envelope.cluster,
            envelope.from,
            envelope.body,
        );
        auth.update(&envelope.incarnation);
        auth.verify_slice(envelope.mac).map_err(error)?;
        let chunk: Chunk = postcard::from_bytes(envelope.body).map_err(error)?;
        if chunk.total > (storage::MAX_SNAPSHOT + MAX_FRAME) as u64
            || chunk.total == 0
            || chunk.data.is_empty()
            || chunk.offset != data.len() as u64
            || chunk.offset + chunk.data.len() as u64 > chunk.total
        {
            return Err(error("invalid transfer bounds"));
        }
        let expected = (envelope.from, chunk.nonce, chunk.total, chunk.hash);
        if identity.is_none() {
            permit = Some(
                bytes
                    .clone()
                    .try_acquire_many_owned(chunk.total as u32)
                    .map_err(error)?,
            );
            identity = Some(expected);
        }
        if identity != Some(expected) {
            return Err(error("transfer identity changed"));
        }
        // Most Raft messages fit one authenticated chunk. Parse that borrowed
        // payload directly instead of allocating and copying an assembly buffer.
        let complete = if data.is_empty() && chunk.data.len() as u64 == chunk.total {
            Some(chunk.data)
        } else {
            data.extend_from_slice(chunk.data);
            (data.len() as u64 == chunk.total).then_some(data.as_slice())
        };
        if chunk.snapshot {
            crate::fault::point("after_cluster_snapshot_chunk");
        }
        if let Some(encoded) = complete {
            let actual: [u8; 32] = Sha256::digest(encoded).into();
            if actual != chunk.hash {
                return Err(error("transfer digest mismatch"));
            }
            let message = Message::parse_from_bytes(encoded).map_err(error)?;
            if message.from != envelope.from || message.to != config.node_id {
                return Err(error("peer identity mismatch"));
            }
            return Ok((
                message,
                envelope.incarnation,
                permit.expect("nonempty authenticated transfer"),
            ));
        }
    }
}
async fn network(
    config: &ClusterConfig,
    tx: mpsc::SyncSender<Input>,
    stop: watch::Receiver<bool>,
    incarnation: [u8; 16],
) -> BTreeMap<u64, transport::PeerSender> {
    config
        .peers
        .iter()
        .filter(|(id, _)| **id != config.node_id)
        .map(|(&id, peer)| {
            (
                id,
                transport::PeerSender::start(
                    config.clone(),
                    peer.raft,
                    tx.clone(),
                    stop.clone(),
                    incarnation,
                ),
            )
        })
        .collect()
}

/// Run a cluster node until shutdown. Store paths and node incarnations are
/// immutable; damaged members are replaced through enrolled learner identities.
async fn run_runtime(
    config: ClusterConfig,
    slot: u64,
    database: Database,
    mut server_config: ServerConfig,
    mut shutdown: watch::Receiver<bool>,
    hub: startup::Hub,
) -> io::Result<()> {
    let peer = &config.peers[&slot];
    let clients = TcpListener::bind(peer.client).await?;
    let db = Arc::new(Mutex::new(database));
    let (tx, rx) = mpsc::sync_channel(256);
    let (checkpoint_path, changes) = {
        let database = db.lock().unwrap();
        (
            database.conn.path().map(std::path::PathBuf::from),
            database.projection_changes.clone(),
        )
    };
    let _page_checkpointer = if let Some(path) = checkpoint_path {
        let fault = tx.clone();
        Some(
            tokio::task::spawn_blocking(move || {
                storage::PageCheckpointer::start(path, changes, fault)
            })
            .await
            .map_err(error)??,
        )
    } else {
        None
    };
    let (role_tx, role) = watch::channel(Role::default());
    let (stop, stop_rx) = watch::channel(false);
    let incarnation = db.lock().unwrap().incarnation;
    let out = Arc::new(network(&config, tx.clone(), stop_rx.clone(), incarnation).await);
    let log = LogStorage { db: db.clone() };
    let member = config.node_id;
    let mut node = tokio::task::spawn_blocking(move || {
        let raft_config = raft::Config {
            id: member,
            election_tick: 15,
            heartbeat_tick: 2,
            check_quorum: true,
            pre_vote: true,
            max_size_per_msg: 1024 * 1024,
            max_inflight_msgs: REPLICATION_WINDOW,
            applied: log.db.lock().unwrap().hard()?.commit,
            ..Default::default()
        };
        let logger = slog::Logger::root(slog::Discard, slog::o!());
        RawNode::new(&raft_config, log, &logger).map_err(error)
    })
    .await
    .map_err(error)??;
    let active_session = Arc::new(AtomicU64::new(0));
    hub.install(startup::Active {
        config: config.clone(),
        tx: tx.clone(),
        role: role.clone(),
        db: db.clone(),
        incarnation,
        session: active_session.clone(),
        peer_senders: out.clone(),
    });
    let actor_session = active_session.clone();
    let actor_config = config.clone();
    let actor = std::thread::Builder::new()
        .name("screech-raft".into())
        .spawn(move || -> io::Result<()> {
            let mut pending: BTreeMap<Vec<u8>, Pending> = BTreeMap::new();
            let mut nonce = [0u8; 16];
            getrandom::fill(&mut nonce).map_err(error)?;
            let mut serial = 0u64;
            let mut read_round = 0usize;
            let mut read_probes = BTreeMap::<Vec<u8>, ReadProbe>::new();
            // New learners must reply to replacement leaders before receiving
            // a snapshot containing the committed placement map. These routes
            // are transport hints only; membership still gates every message.
            let mut routes = BTreeMap::new();
            let mut tick = Instant::now();
            let (mut policy, mut policy_index, mut policy_revision) = {
                let mut db = node.store().db.lock().unwrap();
                let policy = Arc::new(db.sender_policy()?);
                let index = db.conf_index()?;
                db.cache_heartbeat_policy(policy.clone(), index);
                (policy, index, db.membership_revision)
            };
            let mut deferred_input = None;
            loop {
                let wait = read_probes.values().map(|probe|
                    probe.deadline.saturating_duration_since(Instant::now()))
                    .min().unwrap_or(Duration::from_millis(20))
                    .min(Duration::from_millis(20));
                let mut input = deferred_input.take().map(Ok)
                    .unwrap_or_else(|| rx.recv_timeout(wait));
                if matches!(&input, Ok(Input::Stop) | Err(mpsc::RecvTimeoutError::Disconnected)) {
                    break;
                }
                if matches!(&input, Ok(Input::Fault)) {
                    return Err(error("replica storage failed; quarantined"));
                }
                let mut incoming_lease = None;
                let mut stopped = false;
                // The gate orders input validation and Ready work with content
                // readers. Start SQL lazily when this turn actually mutates it.
                project(&mut node, |node, completed| {
                let started = Instant::now();
                let mut processed = 0;
                let mut commit_deadline = None;
                loop {
                let read_replied = matches!(&input, Ok(Input::ReadHeartbeat(..)));
                let accepted_input = matches!(&input, Ok(Input::Message(message, _, _))
                    if message.get_msg_type() == raft::eraftpb::MessageType::MsgAppend
                        && !message.entries.is_empty());
                match input {
                    Ok(Input::Stop) | Err(mpsc::RecvTimeoutError::Disconnected) => { stopped = true; break; },
                    Ok(Input::SnapshotSent(id, ok)) => node.report_snapshot(
                        id,
                        if ok {
                            raft::SnapshotStatus::Finish
                        } else {
                            raft::SnapshotStatus::Failure
                        },
                    ),
                    Ok(Input::Fault) => return Err(error("replica storage failed; quarantined")),
                    Ok(Input::Message(message, nonce, permit) | Input::ReadHeartbeat(message, nonce, permit)) => {
                        incoming_lease = Some(permit);
                        if policy.accepts(&message, nonce) {
                            tracing::trace!(from=message.from,to=message.to,kind=?message.get_msg_type(),index=message.index,commit=message.commit,"authenticated peer message");
                            if let Some(slot) = actor_config.slot_for(message.from, &BTreeMap::from([(message.from,nonce)])) {
                                routes.insert(message.from,slot);
                            }
                            if message.get_msg_type() == raft::eraftpb::MessageType::MsgSnapshot {
                                tracing::debug!(from=message.from,to=message.to,index=message.get_snapshot().get_metadata().index, conf=?message.get_snapshot().get_metadata().get_conf_state(),"received snapshot");
                            }
                            if let Err(error)=step_peer_message(node, message, read_replied) {tracing::debug!(%error,"peer Raft step rejected");}
                        } else {
                            tracing::debug!(from=message.from,to=message.to,kind=?message.get_msg_type(),"fenced peer message");
                        }
                    }
                    Ok(Input::Enroll {
                        slot,
                        member,
                        nonce,
                        term,
                        reply,
                    }) => {
                        let result = (|| -> io::Result<u64> {
                            if node.raft.state != StateRole::Leader || term != node.raft.term {
                                return Err(error("leadership changed"));
                            }
                            if member == 0 || actor_config.peers.contains_key(&member)
                                || actor_config.slot_for(member, &BTreeMap::from([(member,nonce)])) != Some(slot)
                            {
                                return Err(error("invalid recovery placement"));
                            }
                            let db = node.store().db.lock().unwrap();
                            let conf = db.conf()?;
                            let state = db.membership()?;
                            if conf
                                .voters
                                .iter()
                                .chain(&conf.learners)
                                .chain(&conf.voters_outgoing)
                                .any(|id| *id == member && state.identities.get(id) == Some(&nonce))
                            {
                                return Ok(0);
                            }
                            if !conf.voters_outgoing.is_empty() {
                                return Err(error("joint change in progress"));
                            }
                            let old = conf
                                .voters
                                .iter()
                                .find(|id| {
                                    actor_config.slot_for(**id, &state.identities) == Some(slot)
                                })
                                .copied();
                            let learner = conf
                                .learners
                                .iter()
                                .find(|id| {
                                    actor_config.slot_for(**id, &state.identities) == Some(slot)
                                })
                                .copied();
                            drop(db);
                            if let Some(old) = old {
                                membership::start_replacement(
                                    node,
                                    membership::Replacement {
                                        old,
                                        new: member,
                                        nonce,
                                    },
                                )?;
                            } else {
                                membership::enroll_standby(node, member, nonce, learner)?;
                            }
                            Ok(0)
                        })();
                        let _ = reply.send(result);
                    }
                    Ok(Input::Replace {
                        term,
                        configuration,
                        session,
                        replacement,
                        reply,
                    }) => {
                        if term != node.raft.term
                            || configuration != policy_index
                            || session != actor_session.load(Ordering::Acquire)
                            || actor_config
                                .slot_for(
                                    replacement.new,
                                    &BTreeMap::from([(replacement.new, replacement.nonce)]),
                                )
                                .is_none()
                        {
                            let _ = reply
                                .send(Err(error("invalid replacement target or leader epoch")));
                        } else {
                            let result = membership::start_replacement(node, replacement);
                            let _ = reply.send(result.map(|_| 0));
                        }
                    }
                    Ok(Input::Read {term,configuration,session,reply}) => {
                        if node.raft.state != StateRole::Leader
                            || node.raft.term != term
                            || configuration != policy_index
                            || session != actor_session.load(Ordering::Acquire)
                        {
                            let _ = reply.send(Err(error("not current leader")));
                        } else {
                            serial = serial.checked_add(1).ok_or_else(|| error("proposal IDs exhausted"))?;
                            let mut context = nonce.to_vec();
                            context.extend_from_slice(&serial.to_le_bytes());
                            let messages = start_read(node, &context, read_round);
                            if !messages.is_empty() {
                                read_probes.insert(context.clone(), ReadProbe {
                                    deadline: Instant::now() + Duration::from_micros(250),
                                    messages,
                                });
                            }
                            read_round = read_round.wrapping_add(1);
                            pending.insert(context, Pending {started: Instant::now(),reply,read_index:None});
                        }
                    }
                    Ok(Input::Propose {
                        term,
                        configuration,
                        session,
                        activate,
                        data,
                        reply,
                    }) => {
                        if node.raft.state != StateRole::Leader
                            || node.raft.term != term
                            || configuration != policy_index
                            || (!activate && session != actor_session.load(Ordering::Acquire))
                            || (activate && !data.is_empty())
                        {
                            let _ = reply.send(Err(error("not current leader")));
                        } else {
                            let used = {
                                let db = node.store().db.lock().unwrap();
                                db.log_bytes()?.max(db.journal_bytes())
                            };
                            if used + data.len() as u64 + 256 > actor_config.max_log_bytes {
                                let _ = reply
                                    .send(Err(error("cluster log budget reached; writes paused")));
                            } else {
                                serial = serial
                                    .checked_add(1)
                                    .ok_or_else(|| error("proposal IDs exhausted"))?;
                                let mut context = nonce.to_vec();
                                context.extend_from_slice(&serial.to_le_bytes());
                                match node.propose(context.clone(), data) {
                                    Ok(()) => {
                                        if activate {
                                            actor_session.store(session, Ordering::Release);
                                        }
                                        pending.insert(context, Pending {started: Instant::now(),reply,read_index:None});
                                    }
                                    Err(e) => {
                                        let _ = reply.send(Err(error(e)));
                                    }
                                }
                            }
                        }
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                }
                if tick.elapsed() >= Duration::from_millis(100) {
                    node.tick();
                    tick = Instant::now();
                }
                pump_ready(node, &mut pending, &out, &routes, completed)?;
                hedge_reads(node, &pending, &mut read_probes, Instant::now());
                pump_ready(node, &mut pending, &out, &routes, completed)?;
                if accepted_input && node.raft.state == StateRole::Follower
                    && node.raft.raft_log.applied < node.raft.raft_log.last_index()
                    && !node.store().db.lock().unwrap().conn.is_autocommit()
                {
                    // The complete accepted journal and replication ACK are
                    // already durable/dispatched by pump_ready. Only closing this
                    // follower's replayable SQL transaction waits for the commit
                    // notification, bounded to one millisecond of idle time.
                    commit_deadline = Some(Instant::now() + Duration::from_millis(1));
                    crate::fault::point("after_cluster_accept_before_projection_commit");
                }
                {
                    let mut db = node.store().db.lock().unwrap();
                    // Only configuration application and snapshot installation
                    // can change this policy. Check their in-memory token on
                    // ordinary heartbeats, avoiding a SQLite read transaction.
                    // Refresh after EACH input, before authenticating the next.
                    if db.membership_revision != policy_revision {
                        policy = Arc::new(db.sender_policy()?);
                        policy_index = db.conf_index()?;
                        policy_revision = db.membership_revision;
                        db.cache_heartbeat_policy(policy.clone(), policy_index);
                    }
                }
                if policy.replacement_pending() {
                    membership::advance_replacement(node)?;
                }
                processed += 1;
                if processed >= 8 { break; }
                match next_batch_input(&rx, &mut deferred_input,
                    started.elapsed() >= Duration::from_micros(100), node.raft.term) {
                    Some(next) => input = Ok(next),
                    None => {
                        let next = commit_deadline.filter(|deadline| {
                            deferred_input.is_none() && *deadline > Instant::now()
                                && node.raft.state == StateRole::Follower
                                && node.raft.raft_log.applied < node.raft.raft_log.last_index()
                        }).and_then(|deadline| follower_commit_input(
                            &rx, &mut deferred_input, node.raft.term, deadline));
                        if let Some(next) = next { input = Ok(next); } else { break; }
                    },
                }
                }
                Ok(Role { leader: node.raft.leader_id, term: node.raft.term, configuration: policy_index })
                }, |role| {
                    role_tx.send_if_modified(|old| {
                        if *old != *role {
                            *old = *role;
                            true
                        } else {
                            false
                        }
                    });
                })?;
                drop(incoming_lease);
                if stopped { break; }
                pending.retain(|_, request| {
                    if node.raft.state != StateRole::Leader || request.started.elapsed() > RPC_TIMEOUT {
                        let _ = request.reply.send(Err(error(
                            "leadership lost or quorum timeout; outcome unknown",
                        )));
                        false
                    } else {
                        true
                    }
                });
            }
            Ok(())
        })?;
    // All protocols, including Core subscriptions and pull inboxes, share this
    // leader-local owner. Local public connections hand off their actual socket;
    // followers proxy to its service listener over the network.
    let connection_limit = server_config.max_connections;
    let local_connections = Arc::new(Mutex::new(None));
    let service_connections = local_connections.clone();
    server_config.listen = peer.service;
    server_config.connect_urls = config
        .peers
        .values()
        .map(|p| p.client.to_string())
        .collect();
    let service_config = config.clone();
    let service_tx = tx.clone();
    let service_db = db.clone();
    let mut service_role = role.clone();
    let mut service_stop = stop_rx.clone();
    let service = tokio::spawn(async move {
        let mut session = 0u64;
        let mut running: Option<(
            watch::Sender<bool>,
            tokio::task::JoinHandle<io::Result<()>>,
            Role,
        )> = None;
        loop {
            let current = *service_role.borrow_and_update();
            if running.as_ref().is_some_and(|(_, _, term)| {
                current.leader != service_config.node_id || current != *term
            }) {
                service_connections.lock().unwrap().take();
                let (stop, task, _) = running.take().unwrap();
                stop.send_replace(true);
                let _ = task.await;
            }
            if current.leader == service_config.node_id && running.is_none() {
                session = session
                    .checked_add(1)
                    .expect("application sessions exhausted");
                let client = Client {
                    tx: service_tx.clone(),
                    role: service_role.clone(),
                    db: service_db.clone(),
                    id: service_config.node_id,
                    term: current.term,
                    replicas: service_config.voters().len(),
                    configuration: current.configuration,
                    session,
                    active_session: active_session.clone(),
                };
                // Establish a current-term committed observation before recovery.
                let prepared = tokio::task::spawn_blocking(move || {
                    client.activate()?;
                    let guard = client.clone();
                    crate::jetstream::Handle::start_single_voter(ClusterStore { client }).map_err(
                        |failure| {
                            guard.recovery_failed(&failure);
                            error(failure)
                        },
                    )
                })
                .await;
                match prepared {
                    Ok(Ok(handle)) => {
                        let mut local = server_config.clone();
                        let discovery = {
                            let db = service_db.lock().unwrap();
                            db.conf().map(|c| {
                                (c, db.membership().map(|m| m.identities).unwrap_or_default())
                            })
                        };
                        if let Ok((conf, identities)) = discovery {
                            local.connect_urls = conf
                                .voters
                                .iter()
                                .chain(&conf.voters_outgoing)
                                .filter_map(|id| service_config.slot_for(*id, &identities))
                                .filter_map(|slot| service_config.peers.get(&slot))
                                .map(|p| p.client.to_string())
                                .collect::<std::collections::BTreeSet<_>>()
                                .into_iter()
                                .collect();
                        }
                        local.jetstream = Some(handle.clone());
                        match Server::bind(local).await {
                            Ok(server) => {
                                let (stop, rx) = watch::channel(false);
                                let (send, incoming) =
                                    tokio::sync::mpsc::channel(connection_limit.clamp(1, 256));
                                *service_connections.lock().unwrap() = Some((current, send));
                                running = Some((
                                    stop,
                                    tokio::spawn(server.run_with_incoming(rx, Some(incoming))),
                                    current,
                                ));
                            }
                            Err(e) => {
                                handle.shutdown().await;
                                tracing::error!(%e,"leader service bind failed");
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        tracing::error!(%e, "leader recovery failed");
                        // A network/leadership error during recovery is retried;
                        // only an observed storage fault quarantines the replica.
                        if service_db.lock().unwrap().failed {
                            let _ = service_tx.try_send(Input::Fault);
                        }
                    }
                    Err(e) => tracing::error!(%e, "leader recovery task failed"),
                }
            }
            tokio::select! {_=service_stop.changed()=>break,result=service_role.changed()=>{if result.is_err(){break;}},_=tokio::time::sleep(Duration::from_millis(200))=>{}}
        }
        service_connections.lock().unwrap().take();
        if let Some((stop, task, _)) = running {
            stop.send_replace(true);
            let _ = task.await;
        }
    });
    println!("LISTEN {}", peer.client);
    let slots = Arc::new(Semaphore::new(connection_limit));
    let mut actor_status = role.clone();
    loop {
        if *shutdown.borrow() {
            break;
        }
        tokio::select! {
            _=shutdown.changed()=>break,
            result=actor_status.changed()=>{if result.is_err(){break;}},
            accepted=clients.accept()=>{
                let (mut incoming,_)=accepted?;incoming.set_nodelay(true)?;let Ok(slot)=slots.clone().try_acquire_owned() else{continue};
                let mut role=role.clone();let peers=config.peers.clone();let route_db=db.clone();let mut stop=stop_rx.clone();
                let local_connections=local_connections.clone();let local_id=config.node_id;
                tokio::spawn(async move {
                    let _slot=slot;
                    let selected=tokio::time::timeout(Duration::from_secs(6),async{
                        loop {
                            let current=*role.borrow_and_update();
                            if current.leader == local_id {
                                let local = local_connections.lock().unwrap().as_ref()
                                    .filter(|(epoch,_)| *epoch == current).map(|(_,send)|send.clone());
                                if let Some(send) = local {return Ok::<_,io::Error>((current,ConnectionRoute::Local(send)));}
                            } else {
                            let leader_slot=if peers.contains_key(&current.leader){current.leader}else{route_db.lock().unwrap().membership().ok().and_then(|m|m.identities.get(&current.leader).map(|n|u64::from_le_bytes(n[..8].try_into().unwrap()))).unwrap_or(0)};
                            if let Some(peer)=peers.get(&leader_slot) {if let Ok(stream)=TcpStream::connect(peer.service).await{stream.set_nodelay(true)?;return Ok::<_,io::Error>((current,ConnectionRoute::Remote(stream)));}}
                            }
                            tokio::select!{_=role.changed()=>{},_=tokio::time::sleep(Duration::from_millis(50))=>{}}
                        }
                    }).await;
                    if let Ok(Ok((epoch,route)))=selected {
                        let mut outgoing=match route {
                            ConnectionRoute::Local(send)=>{let _=send.try_send(incoming);return;},
                            ConnectionRoute::Remote(stream)=>stream,
                        };
                        tokio::select!{
                            _=tokio::io::copy_bidirectional(&mut incoming,&mut outgoing)=>{},
                            _=stop.changed()=>{},
                            _=async {loop{if role.changed().await.is_err() || *role.borrow()!=epoch{break;}}}=>{}
                        }
                    }
                });
            }
        }
    }
    stop.send_replace(true);
    let _ = service.await;
    let _ = tx.send(Input::Stop);
    tokio::task::spawn_blocking(move || actor.join().map_err(|_| error("Raft actor panicked"))?)
        .await
        .map_err(error)??;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Append;
    include!("read_contact_tests.rs");
    include!("replication_tests.rs");
    #[test]
    fn borrowed_replication_batch_preserves_the_accepted_entry_wire_format() {
        let records = vec![Append {
            subject: "event".into(),
            headers: Bytes::from_static(b"\0header"),
            payload: Bytes::from_static(b"record\0"),
        }];
        let contents = vec![Content {
            key: ContentKey {
                generation: 12,
                sequence: 37,
            },
            subject: "subject".into(),
            headers: Bytes::new(),
            payload: Bytes::from_static(b"payload\0"),
        }];
        let encoded = Batch::encode(&records, &contents).unwrap();
        assert_eq!(
            encoded,
            postcard::to_allocvec(&Batch {
                version: 1,
                records,
                contents: contents.clone()
            })
            .unwrap()
        );
        assert_eq!(Batch::decode(&encoded).unwrap().contents, contents);
    }
    #[test]
    fn queued_quorum_completions_cross_the_time_budget_without_skipping_inputs_or_terms() {
        use raft::eraftpb::MessageType;
        let message = |kind, term| {
            Input::Message(
                Message {
                    from: 2,
                    to: 1,
                    msg_type: kind,
                    term,
                    ..Default::default()
                },
                [0; 16],
                Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap(),
            )
        };
        let (tx, rx) = mpsc::sync_channel(8);
        let mut deferred = None;
        tx.send(message(MessageType::MsgAppendResponse, 3)).unwrap();
        tx.send(message(MessageType::MsgHeartbeatResponse, 3))
            .unwrap();
        tx.send(message(MessageType::MsgHeartbeat, 3)).unwrap();
        tx.send(message(MessageType::MsgAppend, 3)).unwrap();
        let Input::Message(mut append, nonce, permit) = message(MessageType::MsgAppend, 3) else {
            unreachable!()
        };
        append.entries.push(raft::eraftpb::Entry {
            index: 7,
            term: 3,
            ..Default::default()
        });
        tx.send(Input::Message(append, nonce, permit)).unwrap();
        tx.send(message(MessageType::MsgAppendResponse, 3)).unwrap();
        for _ in 0..4 {
            assert!(next_batch_input(&rx, &mut deferred, true, 3).is_some());
            assert!(deferred.is_none());
        }
        assert!(next_batch_input(&rx, &mut deferred, true, 3).is_none());
        assert!(matches!(deferred.take(), Some(Input::Message(m,_,_)) if !m.entries.is_empty()));
        assert!(next_batch_input(&rx, &mut deferred, true, 3).is_some());
        for kind in [
            MessageType::MsgAppendResponse,
            MessageType::MsgHeartbeat,
            MessageType::MsgAppend,
            MessageType::MsgSnapshot,
        ] {
            tx.send(message(kind, 4)).unwrap();
            assert!(next_batch_input(&rx, &mut deferred, true, 3).is_none());
            assert!(matches!(deferred.take(), Some(Input::Message(m,_,_)) if m.term==4));
        }
        tx.send(message(MessageType::MsgSnapshot, 3)).unwrap();
        assert!(next_batch_input(&rx, &mut deferred, true, 3).is_none());
        assert!(
            matches!(deferred.take(), Some(Input::Message(m,_,_)) if m.get_msg_type()==MessageType::MsgSnapshot)
        );
        tx.send(Input::Stop).unwrap();
        assert!(next_batch_input(&rx, &mut deferred, false, 3).is_some());
        assert!(next_batch_input(&rx, &mut deferred, true, 3).is_none());
        assert!(
            deferred.is_none(),
            "empty queue must not create deferred work"
        );
    }
    #[test]
    fn follower_commit_wait_defers_new_entries_and_terms_and_respects_its_deadline() {
        use raft::eraftpb::{Entry, MessageType};
        let (tx, rx) = mpsc::sync_channel(4);
        let mut deferred = None;
        let input = |kind, term, entries: Vec<Entry>| {
            Input::Message(
                Message {
                    from: 1,
                    to: 2,
                    term,
                    msg_type: kind,
                    entries: entries.into(),
                    ..Default::default()
                },
                [0; 16],
                Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap(),
            )
        };
        tx.send(input(MessageType::MsgHeartbeat, 3, vec![]))
            .unwrap();
        assert!(follower_commit_input(&rx, &mut deferred, 3, Instant::now()).is_some());
        tx.send(input(
            MessageType::MsgAppend,
            3,
            vec![Entry {
                index: 7,
                term: 3,
                ..Default::default()
            }],
        ))
        .unwrap();
        tx.send(input(MessageType::MsgHeartbeat, 3, vec![]))
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        assert!(follower_commit_input(&rx, &mut deferred, 3, deadline).is_none());
        assert!(matches!(deferred.take(), Some(Input::Message(m,_,_)) if !m.entries.is_empty()));
        assert!(follower_commit_input(&rx, &mut deferred, 3, deadline).is_some());
        tx.send(input(MessageType::MsgHeartbeat, 4, vec![]))
            .unwrap();
        assert!(follower_commit_input(&rx, &mut deferred, 3, deadline).is_none());
        assert!(matches!(deferred.take(), Some(Input::Message(m,_,_)) if m.term==4));
        assert!(follower_commit_input(&rx, &mut deferred, 3, Instant::now()).is_none());
        assert!(deferred.is_none());
        drop(tx);
        assert!(follower_commit_input(&rx, &mut deferred, 3, deadline).is_none());
    }
    #[test]
    fn borrowed_peer_frames_remain_compatible_with_owned_wire_format() {
        #[derive(Serialize, Deserialize)]
        struct OwnedEnvelope {
            cluster: String,
            from: u64,
            body: Vec<u8>,
            mac: Vec<u8>,
            incarnation: [u8; 16],
        }
        #[derive(Serialize, Deserialize)]
        struct OwnedChunk {
            nonce: [u8; 16],
            offset: u64,
            total: u64,
            hash: [u8; 32],
            data: Vec<u8>,
            snapshot: bool,
        }
        for size in [1, 127, 128, 1024 * 1024] {
            let chunk = OwnedChunk {
                nonce: [3; 16],
                offset: 128,
                total: size as u64 + 128,
                hash: [4; 32],
                data: vec![255; size],
                snapshot: size > 128,
            };
            let old_chunk = postcard::to_allocvec(&chunk).unwrap();
            let borrowed: Chunk = postcard::from_bytes(&old_chunk).unwrap();
            assert_eq!(postcard::to_allocvec(&borrowed).unwrap(), old_chunk);
            let envelope = OwnedEnvelope {
                cluster: "wire-compatibility".into(),
                from: 300,
                body: old_chunk,
                mac: vec![5; 32],
                incarnation: [6; 16],
            };
            let old_frame = postcard::to_allocvec(&envelope).unwrap();
            let borrowed: Envelope = postcard::from_bytes(&old_frame).unwrap();
            assert_eq!(postcard::to_allocvec(&borrowed).unwrap(), old_frame);
            let new_frame = postcard::to_allocvec(&Envelope {
                cluster: &envelope.cluster,
                from: envelope.from,
                body: &envelope.body,
                mac: &envelope.mac,
                incarnation: envelope.incarnation,
            })
            .unwrap();
            let decoded: OwnedEnvelope = postcard::from_bytes(&new_frame).unwrap();
            assert_eq!(decoded.body, envelope.body);
            assert_eq!(decoded.mac, envelope.mac);
            let framed = envelope_frame(&borrowed).unwrap();
            assert_eq!(&framed[..4], &(old_frame.len() as u32).to_be_bytes());
            assert_eq!(&framed[4..], old_frame);
        }
    }
    pub(super) fn config(id: u64) -> ClusterConfig {
        config_size(id, 3)
    }
    fn config_size(id: u64, voters: u64) -> ClusterConfig {
        ClusterConfig {
            cluster_id: "simulation".into(),
            node_id: id,
            secret: "s".repeat(32),
            max_log_bytes: log_budget(),
            initial_voters: Vec::new(),
            peers: (1..=voters)
                .map(|n| {
                    (
                        n,
                        Peer {
                            raft: format!("127.0.0.1:{}", 30000 + n * 3).parse().unwrap(),
                            client: format!("127.0.0.1:{}", 30001 + n * 3).parse().unwrap(),
                            service: format!("127.0.0.1:{}", 30002 + n * 3).parse().unwrap(),
                        },
                    )
                })
                .collect(),
        }
    }
    struct Simulation {
        nodes: Vec<RawNode<LogStorage>>,
        pending: Vec<BTreeMap<Vec<u8>, Pending>>,
        receive: Vec<tokio::sync::mpsc::Receiver<Message>>,
        send: BTreeMap<u64, tokio::sync::mpsc::Sender<Message>>,
    }
    impl Simulation {
        fn new(root: &Path) -> Self {
            Self::with_voters(root, 3)
        }
        fn with_voters(root: &Path, voters: u64) -> Self {
            let mut send = BTreeMap::new();
            let mut receive = Vec::new();
            let mut nodes = Vec::new();
            for id in 1..=voters {
                let (tx, rx) = tokio::sync::mpsc::channel(1024);
                send.insert(id, tx);
                receive.push(rx);
                let db = Arc::new(Mutex::new(
                    Database::open(&root.join(id.to_string()), &config_size(id, voters), true)
                        .unwrap(),
                ));
                let applied = db.lock().unwrap().hard().unwrap().commit;
                let log = LogStorage { db };
                let c = raft::Config {
                    id,
                    election_tick: 10,
                    heartbeat_tick: 1,
                    check_quorum: true,
                    pre_vote: true,
                    applied,
                    max_inflight_msgs: REPLICATION_WINDOW,
                    max_size_per_msg: 1024 * 1024,
                    ..Default::default()
                };
                nodes.push(
                    RawNode::new(&c, log, &slog::Logger::root(slog::Discard, slog::o!())).unwrap(),
                );
            }
            Self {
                pending: (0..voters).map(|_| BTreeMap::new()).collect(),
                nodes,
                receive,
                send,
            }
        }
        fn settle(&mut self, isolated: Option<u64>) {
            // Reverse and duplicate messages to exercise the real implementation's
            // ordering/idempotency rather than simulating an ideal transport.
            for _ in 0..100 {
                for (node, pending) in self.nodes.iter_mut().zip(&mut self.pending) {
                    pump(node, pending, &self.send, &BTreeMap::new()).unwrap();
                }
                let mut messages = Vec::new();
                for rx in &mut self.receive {
                    while let Ok(m) = rx.try_recv() {
                        messages.push(m);
                    }
                }
                if messages.is_empty() {
                    return;
                }
                for m in messages.into_iter().rev() {
                    if isolated.is_some_and(|id| m.to == id || m.from == id) {
                        continue;
                    }
                    let to = (m.to - 1) as usize;
                    let _ = self.nodes[to].step(m.clone());
                    let _ = self.nodes[to].step(m);
                }
            }
            panic!("simulation failed to settle");
        }
        fn ticks(&mut self, n: usize, isolated: Option<u64>) {
            for _ in 0..n {
                for node in &mut self.nodes {
                    node.tick();
                }
                self.settle(isolated);
            }
        }
        fn quorum_read(
            &mut self,
            leader: usize,
            context: Vec<u8>,
        ) -> mpsc::Receiver<io::Result<u64>> {
            let (reply, rx) = mpsc::sync_channel(1);
            start_read(&mut self.nodes[leader], &context, 0);
            self.pending[leader].insert(
                context,
                Pending {
                    started: Instant::now(),
                    reply,
                    read_index: None,
                },
            );
            rx
        }
        fn propose(&mut self, node: usize, key: u64, body: &'static [u8]) {
            let batch = Batch {
                version: 1,
                records: vec![Append {
                    subject: "event".into(),
                    headers: Bytes::new(),
                    payload: Bytes::new(),
                }],
                contents: vec![Content {
                    key: ContentKey {
                        generation: 1,
                        sequence: key,
                    },
                    subject: "subject".into(),
                    headers: Bytes::new(),
                    payload: Bytes::from_static(body),
                }],
            };
            self.nodes[node]
                .propose(vec![], postcard::to_allocvec(&batch).unwrap())
                .unwrap();
        }
    }
    #[test]
    fn bounded_replication_window_batches_a_lagging_voter_without_committing_a_minority() {
        use raft::eraftpb::MessageType;
        let dir = tempfile::tempdir().unwrap();
        let mut sim = Simulation::with_voters(dir.path(), 5);
        sim.nodes[0].campaign().unwrap();
        sim.settle(None);
        sim.nodes[0].raft.mut_prs().get_mut(5).unwrap().ins = raft::Inflights::new(1);
        let committed = sim.nodes[0].raft.raft_log.committed;
        sim.propose(0, 1, b"one");
        pump(
            &mut sim.nodes[0],
            &mut sim.pending[0],
            &sim.send,
            &BTreeMap::new(),
        )
        .unwrap();
        let first = sim.receive[4].try_recv().unwrap();
        assert_eq!(first.entries.len(), 1);
        for key in 2..=9 {
            sim.propose(0, key, b"next");
            pump(
                &mut sim.nodes[0],
                &mut sim.pending[0],
                &sim.send,
                &BTreeMap::new(),
            )
            .unwrap();
        }
        assert!(
            sim.receive[4].try_recv().is_err(),
            "lagging voter received another outstanding append"
        );
        assert_eq!(sim.nodes[0].raft.raft_log.committed, committed);
        sim.nodes[4].step(first).unwrap();
        pump(
            &mut sim.nodes[4],
            &mut sim.pending[4],
            &sim.send,
            &BTreeMap::new(),
        )
        .unwrap();
        let response = sim.receive[0].try_recv().unwrap();
        assert_eq!(response.get_msg_type(), MessageType::MsgAppendResponse);
        sim.nodes[0].step(response).unwrap();
        pump(
            &mut sim.nodes[0],
            &mut sim.pending[0],
            &sim.send,
            &BTreeMap::new(),
        )
        .unwrap();
        assert_eq!(
            sim.nodes[0].raft.raft_log.committed, committed,
            "two voters formed a five-voter quorum"
        );
        let batch = sim.receive[4].try_recv().unwrap();
        assert_eq!(
            batch.entries.len(),
            8,
            "pending suffix was not sent as one batch"
        );
        sim.nodes[4].step(batch).unwrap();
        sim.settle(None);
        for node in &sim.nodes {
            assert_eq!(node.raft.raft_log.applied, committed + 9);
            assert_eq!(node.store().db.lock().unwrap().end().unwrap(), 9);
        }
    }
    #[test]
    fn follower_acceptance_ack_and_commit_projection_share_a_sql_transaction() {
        use raft::eraftpb::MessageType;
        let dir = tempfile::tempdir().unwrap();
        let mut sim = Simulation::new(dir.path());
        sim.nodes[0].campaign().unwrap();
        sim.settle(None);
        sim.propose(0, 1, b"confirmed");
        pump(
            &mut sim.nodes[0],
            &mut sim.pending[0],
            &sim.send,
            &BTreeMap::new(),
        )
        .unwrap();
        let append = sim.receive[1].try_recv().unwrap();
        assert_eq!(append.get_msg_type(), MessageType::MsgAppend);
        let accepted = append.entries.last().unwrap().index;
        let initial = sim.nodes[1]
            .store()
            .db
            .lock()
            .unwrap()
            .projection_changes
            .load(Ordering::Acquire);
        let (leader, rest) = sim.nodes.split_at_mut(1);
        let follower = &mut rest[0];
        project(
            follower,
            |follower, completed| {
                follower.step(append).unwrap();
                pump_ready(
                    follower,
                    &mut sim.pending[1],
                    &sim.send,
                    &BTreeMap::new(),
                    completed,
                )?;
                {
                    let db = follower.store().db.lock().unwrap();
                    assert!(!db.conn.is_autocommit());
                    assert_eq!(db.projection_changes.load(Ordering::Acquire), initial);
                    assert_eq!(db.durable_term()?, follower.raft.term);
                }
                // The normal pump released a durable acceptance response before
                // closing the follower's query transaction. Obtain a real quorum
                // commit, then project its notification in that same transaction.
                let ack = sim.receive[0].try_recv().unwrap();
                assert_eq!(ack.get_msg_type(), MessageType::MsgAppendResponse);
                assert!(!ack.reject);
                assert_eq!(ack.index, accepted);
                leader[0].step(ack).unwrap();
                pump(
                    &mut leader[0],
                    &mut sim.pending[0],
                    &sim.send,
                    &BTreeMap::new(),
                )?;
                let notification = sim.receive[1].try_recv().unwrap();
                assert!(notification.entries.is_empty());
                assert_eq!(notification.commit, accepted);
                follower.step(notification).unwrap();
                pump_ready(
                    follower,
                    &mut sim.pending[1],
                    &sim.send,
                    &BTreeMap::new(),
                    completed,
                )?;
                let db = follower.store().db.lock().unwrap();
                assert_eq!(db.view()?, accepted);
                assert!(!db.conn.is_autocommit());
                assert_eq!(db.projection_changes.load(Ordering::Acquire), initial);
                Ok(())
            },
            |_| {},
        )
        .unwrap();
        let db = follower.store().db.lock().unwrap();
        assert!(db.conn.is_autocommit());
        assert!(db.projection_changes.load(Ordering::Acquire) > initial);
        assert_eq!(
            db.content(ContentKey {
                generation: 1,
                sequence: 1
            })
            .unwrap()
            .unwrap()
            .payload,
            "confirmed"
        );
    }
    #[test]
    fn partition_replaces_uncommitted_content_and_restart_recovers_only_committed_history() {
        let dir = tempfile::tempdir().unwrap();
        let mut sim = Simulation::new(dir.path());
        sim.nodes[0].campaign().unwrap();
        sim.settle(None);
        assert_eq!(sim.nodes[0].raft.state, StateRole::Leader);
        sim.propose(0, 1, b"committed");
        sim.settle(None);
        sim.propose(0, 2, b"discarded");
        sim.settle(Some(1));
        assert_eq!(sim.nodes[0].store().db.lock().unwrap().end().unwrap(), 1);
        sim.nodes[1].campaign().unwrap();
        sim.settle(Some(1));
        // Election timeouts are randomized; the first round can legitimately
        // split the two surviving votes. Require bounded recovery, not a lucky
        // timeout draw in exactly twenty ticks.
        let leader = (0..100)
            .find_map(|_| {
                sim.ticks(1, Some(1));
                (1..3).find(|&n| sim.nodes[n].raft.state == StateRole::Leader)
            })
            .expect("surviving quorum failed to elect a leader");
        sim.propose(leader, 2, b"replacement");
        sim.settle(Some(1));
        sim.ticks(30, None);
        for node in &sim.nodes {
            let db = node.store().db.lock().unwrap();
            assert_eq!(db.end().unwrap(), 2);
            assert_eq!(
                db.content(ContentKey {
                    generation: 1,
                    sequence: 2
                })
                .unwrap()
                .unwrap()
                .payload,
                "replacement"
            );
        }
        drop(sim);
        let sim = Simulation::new(dir.path());
        for node in &sim.nodes {
            assert_eq!(node.store().db.lock().unwrap().end().unwrap(), 2);
        }
    }
    #[test]
    fn removed_leader_steps_down_after_leave_joint_is_committed() {
        use raft::eraftpb::{ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2};
        let dir = tempfile::tempdir().unwrap();
        let mut sim = Simulation::new(dir.path());
        sim.nodes[0].campaign().unwrap();
        sim.settle(None);
        let path = dir.path().join("1");
        let initial_revision = sim.nodes[0].store().db.lock().unwrap().membership_revision;
        {
            let db = sim.nodes[0].store().db.lock().unwrap();
            db.conn
                .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
                .unwrap();
            std::fs::copy(&path, path.with_extension("before-membership")).unwrap();
        }
        let change = |id, kind| ConfChangeSingle {
            node_id: id,
            change_type: kind,
            ..Default::default()
        };
        sim.nodes[0]
            .propose_conf_change(
                vec![],
                ConfChangeV2 {
                    changes: vec![change(4, ConfChangeType::AddLearnerNode)].into(),
                    ..Default::default()
                },
            )
            .unwrap();
        sim.settle(None);
        assert_eq!(
            sim.nodes[0].store().db.lock().unwrap().membership_revision,
            initial_revision + 1
        );
        sim.nodes[0]
            .propose_conf_change(
                vec![],
                ConfChangeV2 {
                    transition: ConfChangeTransition::Explicit,
                    changes: vec![
                        change(4, ConfChangeType::AddNode),
                        change(1, ConfChangeType::RemoveNode),
                    ]
                    .into(),
                    ..Default::default()
                },
            )
            .unwrap();
        sim.settle(None);
        assert_eq!(
            sim.nodes[0].store().db.lock().unwrap().membership_revision,
            initial_revision + 2
        );
        assert!(!membership::step_down_removed(&mut sim.nodes[0]));
        sim.nodes[0]
            .propose_conf_change(vec![], ConfChangeV2::default())
            .unwrap();
        sim.settle(None);
        assert_eq!(
            sim.nodes[0].store().db.lock().unwrap().membership_revision,
            initial_revision + 3
        );
        assert_eq!(sim.nodes[0].raft.state, StateRole::Follower);
        assert!(!membership::step_down_removed(&mut sim.nodes[0]));
        let index = sim.nodes[0]
            .store()
            .db
            .lock()
            .unwrap()
            .conf_index()
            .unwrap();
        drop(sim);
        for suffix in ["-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
        }
        std::fs::copy(path.with_extension("before-membership"), &path).unwrap();
        let db = Database::open(&path, &config(1), false).unwrap();
        assert_eq!(
            db.conf_index().unwrap(),
            index,
            "forgot committed membership"
        );
        let conf = db.conf().unwrap();
        assert!(conf.voters_outgoing.is_empty());
        assert!(!conf.voters.contains(&1));
        assert!(conf.voters.contains(&4));
    }
    #[test]
    fn quorum_reads_wait_for_majority_without_appending_or_advancing_durable_history() {
        for count in [3, 5] {
            let dir = tempfile::tempdir().unwrap();
            let mut sim = Simulation::with_voters(dir.path(), count);
            sim.nodes[0].campaign().unwrap();
            sim.settle(None);
            let before = {
                let db = sim.nodes[0].store().db.lock().unwrap();
                (db.last().unwrap(), db.hard().unwrap(), db.view().unwrap())
            };
            let revision = sim.nodes[0].store().db.lock().unwrap().membership_revision;
            let rx = sim.quorum_read(0, b"partitioned-read".to_vec());
            sim.settle(Some(1));
            assert!(matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
            sim.ticks(3, None);
            assert_eq!(rx.try_recv().unwrap().unwrap(), before.2);
            let db = sim.nodes[0].store().db.lock().unwrap();
            assert_eq!(
                db.membership_revision, revision,
                "quorum reads changed membership policy"
            );
            assert_eq!(
                (db.last().unwrap(), db.hard().unwrap(), db.view().unwrap()),
                before
            );
        }
    }
    #[test]
    fn quorum_read_falls_back_to_all_voters_when_an_initial_target_is_unavailable() {
        for count in [3, 5] {
            let dir = tempfile::tempdir().unwrap();
            let mut sim = Simulation::with_voters(dir.path(), count);
            sim.nodes[0].campaign().unwrap();
            sim.settle(None);
            let before = {
                let db = sim.nodes[0].store().db.lock().unwrap();
                (
                    db.last().unwrap(),
                    db.hard().unwrap(),
                    db.view().unwrap(),
                    db.journal_bytes(),
                )
            };
            let rx = sim.quorum_read(0, b"fresh-fallback".to_vec());
            let targets: Vec<_> = sim.nodes[0]
                .raft
                .msgs
                .iter()
                .filter(|m| {
                    m.get_msg_type() == raft::eraftpb::MessageType::MsgHeartbeat
                        && m.get_context() == b"fresh-fallback"
                })
                .map(|m| m.to)
                .collect();
            assert_eq!(targets.len(), count as usize / 2);
            let unavailable = targets[0];
            sim.settle(Some(unavailable));
            assert!(matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
            // Keep the selected peer unavailable. The ordinary heartbeat must
            // reach the other healthy quorum without another client request.
            sim.ticks(3, Some(unavailable));
            assert_eq!(rx.try_recv().unwrap().unwrap(), before.2);
            let db = sim.nodes[0].store().db.lock().unwrap();
            assert_eq!(
                (
                    db.last().unwrap(),
                    db.hard().unwrap(),
                    db.view().unwrap(),
                    db.journal_bytes()
                ),
                before
            );
        }
    }
    #[test]
    fn initial_read_quorums_rotate_without_starving_voters() {
        for count in [3, 5] {
            let dir = tempfile::tempdir().unwrap();
            let mut sim = Simulation::with_voters(dir.path(), count);
            sim.nodes[0].campaign().unwrap();
            sim.settle(None);
            let mut probes = BTreeMap::<u64, usize>::new();
            for round in 0..count as usize - 1 {
                let context = format!("round-{round}").into_bytes();
                start_read(&mut sim.nodes[0], &context, round);
                let mut selected = 0;
                for message in sim.nodes[0].raft.msgs.drain(..) {
                    if message.get_msg_type() == raft::eraftpb::MessageType::MsgHeartbeat
                        && message.get_context() == context.as_slice()
                    {
                        *probes.entry(message.to).or_default() += 1;
                        selected += 1;
                    }
                }
                assert_eq!(selected, count as usize / 2);
            }
            assert_eq!(probes.len(), count as usize - 1);
            assert!(probes.values().all(|n| *n == count as usize / 2));
        }
    }
    #[test]
    fn delayed_read_probes_reach_another_quorum_without_a_tick_or_log_write() {
        for count in [3, 5] {
            let dir = tempfile::tempdir().unwrap();
            let mut sim = Simulation::with_voters(dir.path(), count);
            sim.nodes[0].campaign().unwrap();
            sim.settle(None);
            let before = {
                let db = sim.nodes[0].store().db.lock().unwrap();
                (
                    db.last().unwrap(),
                    db.hard().unwrap(),
                    db.view().unwrap(),
                    db.journal_bytes(),
                )
            };
            let context = b"delayed-fresh-read".to_vec();
            let messages = start_read(&mut sim.nodes[0], &context, 0);
            assert_eq!(messages.len(), count as usize - 1 - count as usize / 2);
            let unavailable = sim.nodes[0]
                .raft
                .msgs
                .iter()
                .find(|message| message.get_context() == context.as_slice())
                .unwrap()
                .to;
            let (reply, rx) = mpsc::sync_channel(1);
            sim.pending[0].insert(
                context.clone(),
                Pending {
                    started: Instant::now(),
                    reply,
                    read_index: None,
                },
            );
            let deadline = Instant::now() + Duration::from_secs(1);
            let mut probes = BTreeMap::from([(context, ReadProbe { deadline, messages })]);
            sim.settle(Some(unavailable));
            assert!(matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
            hedge_reads(
                &mut sim.nodes[0],
                &sim.pending[0],
                &mut probes,
                deadline - Duration::from_micros(1),
            );
            assert_eq!(probes.len(), 1);
            assert!(sim.nodes[0].raft.msgs.is_empty());
            hedge_reads(&mut sim.nodes[0], &sim.pending[0], &mut probes, deadline);
            assert!(probes.is_empty());
            // Sending more probes is not itself a quorum observation.
            assert!(matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
            sim.settle(Some(unavailable));
            assert_eq!(rx.try_recv().unwrap().unwrap(), before.2);
            let db = sim.nodes[0].store().db.lock().unwrap();
            assert_eq!(
                (
                    db.last().unwrap(),
                    db.hard().unwrap(),
                    db.view().unwrap(),
                    db.journal_bytes()
                ),
                before
            );
        }
    }

    #[test]
    fn delayed_read_probes_discard_completed_reads_and_old_leader_epochs() {
        for obsolete in ["completed", "follower", "new-term"] {
            let dir = tempfile::tempdir().unwrap();
            let mut sim = Simulation::with_voters(dir.path(), 5);
            sim.nodes[0].campaign().unwrap();
            sim.settle(None);
            let context = b"obsolete-read".to_vec();
            let messages = start_read(&mut sim.nodes[0], &context, 0);
            let (reply, _rx) = mpsc::sync_channel(1);
            sim.pending[0].insert(
                context.clone(),
                Pending {
                    started: Instant::now(),
                    reply,
                    read_index: None,
                },
            );
            let deadline = Instant::now();
            let mut probes = BTreeMap::from([(context, ReadProbe { deadline, messages })]);
            sim.nodes[0].raft.msgs.clear();
            match obsolete {
                "completed" => sim.pending[0].clear(),
                "follower" => {
                    let term = sim.nodes[0].raft.term;
                    sim.nodes[0].raft.become_follower(term + 1, 0);
                }
                "new-term" => sim.nodes[0].raft.term += 1,
                _ => unreachable!(),
            }
            hedge_reads(&mut sim.nodes[0], &sim.pending[0], &mut probes, deadline);
            assert!(probes.is_empty());
            assert!(sim.nodes[0].raft.msgs.is_empty());
        }
    }
    #[test]
    fn initial_read_probes_prefer_caught_up_voters_without_reusing_their_acks() {
        let dir = tempfile::tempdir().unwrap();
        let mut sim = Simulation::with_voters(dir.path(), 5);
        sim.nodes[0].campaign().unwrap();
        sim.settle(None);
        // Model delayed append acknowledgements from two followers. Rotating
        // to those peers would wait behind their outstanding persistence work.
        for id in [4, 5] {
            sim.nodes[0].raft.mut_prs().get_mut(id).unwrap().matched = 0;
        }
        start_read(&mut sim.nodes[0], b"caught-up-read", 2);
        let targets: std::collections::BTreeSet<_> = sim.nodes[0]
            .raft
            .msgs
            .drain(..)
            .filter(|m| {
                m.get_msg_type() == raft::eraftpb::MessageType::MsgHeartbeat
                    && m.get_context() == b"caught-up-read"
            })
            .map(|m| m.to)
            .collect();
        assert_eq!(targets, [2, 3].into_iter().collect());
        // Even though these voters already acknowledged the committed prefix,
        // a newly arriving request cannot succeed before its fresh replies.
        assert!(sim.nodes[0].raft.read_states.is_empty());
    }
    #[test]
    fn initial_read_broadcasts_when_too_few_voters_are_caught_up() {
        let dir = tempfile::tempdir().unwrap();
        let mut sim = Simulation::with_voters(dir.path(), 5);
        sim.nodes[0].campaign().unwrap();
        sim.settle(None);
        for id in [3, 4, 5] {
            sim.nodes[0].raft.mut_prs().get_mut(id).unwrap().matched = 0;
        }
        start_read(&mut sim.nodes[0], b"broadcast-read", 0);
        let targets: std::collections::BTreeSet<_> = sim.nodes[0]
            .raft
            .msgs
            .drain(..)
            .filter(|m| {
                m.get_msg_type() == raft::eraftpb::MessageType::MsgHeartbeat
                    && m.get_context() == b"broadcast-read"
            })
            .map(|m| m.to)
            .collect();
        assert_eq!(targets, [2, 3, 4, 5].into_iter().collect());
    }
    #[test]
    fn read_probes_keep_full_fanout_for_learners_and_joint_voters() {
        use raft::eraftpb::{
            ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2, MessageType,
        };
        let dir = tempfile::tempdir().unwrap();
        let mut sim = Simulation::new(dir.path());
        sim.nodes[0].campaign().unwrap();
        sim.settle(None);
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
        start_read(&mut sim.nodes[0], b"learner-read", 0);
        let targets: std::collections::BTreeSet<_> = sim.nodes[0]
            .raft
            .msgs
            .drain(..)
            .filter(|m| {
                m.get_msg_type() == MessageType::MsgHeartbeat && m.get_context() == b"learner-read"
            })
            .map(|m| m.to)
            .collect();
        assert_eq!(targets, [2, 3, 4].into_iter().collect());
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
        start_read(&mut sim.nodes[0], b"joint-read", 1);
        let targets: std::collections::BTreeSet<_> = sim.nodes[0]
            .raft
            .msgs
            .drain(..)
            .filter(|m| {
                m.get_msg_type() == MessageType::MsgHeartbeat && m.get_context() == b"joint-read"
            })
            .map(|m| m.to)
            .collect();
        assert_eq!(targets, [2, 3, 4].into_iter().collect());
    }
    #[test]
    fn failed_projection_commit_cannot_release_a_staged_success_reply() {
        let dir = tempfile::tempdir().unwrap();
        let mut sim = Simulation::new(dir.path());
        sim.nodes[0].campaign().unwrap();
        sim.settle(None);
        let before = {
            let db = sim.nodes[0].store().db.lock().unwrap();
            // A deferred constraint fails at COMMIT, after all projection
            // statements and reply staging have succeeded.
            db.conn.execute_batch("PRAGMA foreign_keys=ON;
              CREATE TABLE projection_parent(id INTEGER PRIMARY KEY);
              CREATE TABLE projection_guard(id INTEGER REFERENCES projection_parent(id) DEFERRABLE INITIALLY DEFERRED);
              CREATE TRIGGER fail_projection AFTER INSERT ON cluster_meta WHEN NEW.key='view'
              BEGIN INSERT INTO projection_guard VALUES(99); END;
              CREATE TRIGGER fail_projection_update AFTER UPDATE ON cluster_meta WHEN NEW.key='view'
              BEGIN INSERT INTO projection_guard VALUES(99); END;").unwrap();
            db.view().unwrap()
        };
        let context = b"fail-only-at-commit".to_vec();
        let (reply, rx) = mpsc::sync_channel(1);
        sim.pending[0].insert(
            context.clone(),
            Pending {
                started: Instant::now(),
                reply,
                read_index: None,
            },
        );
        sim.nodes[0].propose(context, Vec::new()).unwrap();
        pump(
            &mut sim.nodes[0],
            &mut sim.pending[0],
            &sim.send,
            &BTreeMap::new(),
        )
        .unwrap();
        // Obtain an actual majority's durable acceptance before attempting to
        // apply the leader's committed prefix.
        while let Ok(message) = sim.receive[1].try_recv() {
            sim.nodes[1].step(message).unwrap();
            pump(
                &mut sim.nodes[1],
                &mut sim.pending[1],
                &sim.send,
                &BTreeMap::new(),
            )
            .unwrap();
        }
        while let Ok(message) = sim.receive[0].try_recv() {
            sim.nodes[0].step(message).unwrap();
        }
        let (second, second_rx) = mpsc::sync_channel(1);
        let mut published = false;
        assert!(project(
            &mut sim.nodes[0],
            |node, completed| {
                pump_ready(
                    node,
                    &mut sim.pending[0],
                    &sim.send,
                    &BTreeMap::new(),
                    completed,
                )?;
                completed.push((second, before));
                Ok(())
            },
            |_| published = true,
        )
        .is_err());
        assert!(!published, "failed SQL commit published a new epoch");
        assert!(
            second_rx.try_recv().unwrap().is_err(),
            "late failure released another success"
        );
        assert!(
            rx.try_recv().unwrap().is_err(),
            "COMMIT failure produced success"
        );
        let db = sim.nodes[0].store().db.lock().unwrap();
        assert!(db.failed);
        assert_eq!(db.view().unwrap(), before);
    }
    #[test]
    fn old_term_majority_cannot_commit_before_a_current_term_entry() {
        use raft::eraftpb::{HardState, MessageType};
        let dir = tempfile::tempdir().unwrap();
        let mut sim = Simulation::with_voters(dir.path(), 5);
        sim.nodes[0].campaign().unwrap();
        sim.settle(None);
        let initial = sim.nodes[0]
            .store()
            .db
            .lock()
            .unwrap()
            .hard()
            .unwrap()
            .commit;
        // Accept a prior-term application entry only on the old leader. No ACK
        // was released. Elections may leave this entry on several replicas.
        sim.propose(0, 1, b"prior-term");
        pump(
            &mut sim.nodes[0],
            &mut BTreeMap::new(),
            &sim.send,
            &BTreeMap::new(),
        )
        .unwrap();
        let entry = sim.nodes[0]
            .store()
            .db
            .lock()
            .unwrap()
            .entry(initial + 1)
            .unwrap();
        for rx in &mut sim.receive {
            while rx.try_recv().is_ok() {}
        }
        sim.nodes[0].raft.become_follower(2, 0);
        sim.nodes[0].raft.become_candidate();
        sim.nodes[0].raft.become_leader();
        pump(
            &mut sim.nodes[0],
            &mut BTreeMap::new(),
            &sim.send,
            &BTreeMap::new(),
        )
        .unwrap();
        let term = sim.nodes[0].raft.term;
        let current = sim.nodes[0]
            .store()
            .db
            .lock()
            .unwrap()
            .entry(entry.index + 1)
            .unwrap();
        assert_eq!(current.term, term);
        for rx in &mut sim.receive {
            while rx.try_recv().is_ok() {}
        }
        // Deliver real AppendEntries to two followers, withholding the new
        // term's entry. Persist their responses through the production pump.
        for (previous, record) in [(initial, entry.clone()), (entry.index, current.clone())] {
            let previous_term = if previous == initial { 1 } else { entry.term };
            for follower in [1, 2] {
                sim.nodes[follower]
                    .step(Message {
                        msg_type: MessageType::MsgAppend,
                        from: 1,
                        to: follower as u64 + 1,
                        term,
                        index: previous,
                        log_term: previous_term,
                        commit: initial,
                        entries: vec![record.clone()].into(),
                        ..Default::default()
                    })
                    .unwrap();
                pump(
                    &mut sim.nodes[follower],
                    &mut BTreeMap::new(),
                    &sim.send,
                    &BTreeMap::new(),
                )
                .unwrap();
            }
            while let Ok(message) = sim.receive[0].try_recv() {
                sim.nodes[0].step(message).unwrap();
            }
            pump(
                &mut sim.nodes[0],
                &mut BTreeMap::new(),
                &sim.send,
                &BTreeMap::new(),
            )
            .unwrap();
            let db = sim.nodes[0].store().db.lock().unwrap();
            if record.term < term {
                assert_eq!(db.hard().unwrap().commit, initial);
                assert_eq!(
                    db.end().unwrap(),
                    0,
                    "old-term quorum released application state"
                );
            } else {
                assert_eq!(db.hard().unwrap().commit, current.index);
                assert_eq!(db.end().unwrap(), 1);
                assert_eq!(
                    db.content(ContentKey {
                        generation: 1,
                        sequence: 1
                    })
                    .unwrap()
                    .unwrap()
                    .payload,
                    b"prior-term".as_slice()
                );
            }
        }
        // The persisted state, rather than an in-memory commit hint, survives.
        let expected = HardState {
            term,
            vote: 1,
            commit: current.index,
            ..Default::default()
        };
        assert_eq!(
            sim.nodes[0].store().db.lock().unwrap().hard().unwrap(),
            expected
        );
    }
    #[test]
    fn campaign_persists_its_own_vote_before_sending_vote_requests_and_restart() {
        use raft::eraftpb::MessageType;
        let dir = tempfile::tempdir().unwrap();
        let mut sim = Simulation::new(dir.path());
        sim.nodes[0].raft.become_follower(1, 0);
        sim.nodes[0].raft.become_candidate();
        // Use the normal campaign path after a previous term/vote, as in #7956.
        sim.nodes[0].raft.become_follower(2, 0);
        sim.nodes[0].campaign().unwrap();
        sim.settle(Some(1));
        // Pre-vote cannot change durable term/vote without a quorum.
        assert_eq!(
            sim.nodes[0].store().db.lock().unwrap().hard().unwrap().term,
            2
        );
        sim.nodes[0].raft.pre_vote = false;
        sim.nodes[0].campaign().unwrap();
        let term = sim.nodes[0].raft.term;
        // Pump persists Ready hard state before exposing any protocol output.
        pump(
            &mut sim.nodes[0],
            &mut BTreeMap::new(),
            &sim.send,
            &BTreeMap::new(),
        )
        .unwrap();
        let hard = sim.nodes[0].store().db.lock().unwrap().hard().unwrap();
        assert_eq!((hard.term, hard.vote), (term, 1));
        let mut requests = 0;
        for rx in &mut sim.receive {
            while let Ok(message) = rx.try_recv() {
                if message.get_msg_type() == MessageType::MsgRequestVote {
                    assert_eq!(message.term, hard.term);
                    requests += 1;
                }
            }
        }
        assert_eq!(requests, 2);
        drop(sim);
        let sim = Simulation::new(dir.path());
        assert_eq!(
            sim.nodes[0].store().db.lock().unwrap().hard().unwrap(),
            hard
        );
        assert_eq!(
            sim.nodes[0].raft.prs().conf().to_conf_state().voters.len(),
            3
        );
    }
    #[test]
    fn authentication_binds_cluster_sender_and_complete_payload() {
        let mac = authenticate("secret", "cluster", 1, b"entry")
            .finalize()
            .into_bytes();
        assert!(authenticate("secret", "cluster", 1, b"entry")
            .verify_slice(&mac)
            .is_ok());
        for (cluster, id, body) in [
            ("other", 1, b"entry".as_slice()),
            ("cluster", 2, b"entry"),
            ("cluster", 1, b"wrong"),
        ] {
            assert!(authenticate("secret", cluster, id, body)
                .verify_slice(&mac)
                .is_err());
        }
        assert!(authenticate("wrong", "cluster", 1, b"entry")
            .verify_slice(&mac)
            .is_err());
    }
    #[tokio::test]
    async fn peer_stream_frames_multiple_messages_and_chunked_snapshots_without_eof() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let mut snapshot = Message {
            from: 1,
            to: 2,
            msg_type: raft::eraftpb::MessageType::MsgSnapshot,
            ..Default::default()
        };
        snapshot.mut_snapshot().data = vec![42; 2 * 1024 * 1024 + 13].into();
        let messages = vec![
            Message {
                from: 1,
                to: 2,
                msg_type: raft::eraftpb::MessageType::MsgHeartbeat,
                ..Default::default()
            },
            snapshot,
            Message {
                from: 1,
                to: 2,
                msg_type: raft::eraftpb::MessageType::MsgHeartbeatResponse,
                ..Default::default()
            },
        ];
        let expected = messages.clone();
        let receiving = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = BufReader::new(stream);
            for message in expected {
                let (actual, nonce, _permit) = receive_message(
                    &config(2),
                    &mut stream,
                    Arc::new(Semaphore::new(MAX_FRAME)),
                    None,
                )
                .await
                .unwrap();
                assert_eq!(actual, message);
                assert_eq!(nonce, [7; 16]);
            }
        });
        let stream = TcpStream::connect(address).await.unwrap();
        stream.set_nodelay(true).unwrap();
        for message in messages {
            send_message(&config(1), &stream, &message, [7; 16])
                .await
                .unwrap();
        }
        receiving.await.unwrap();
    }
    #[tokio::test]
    async fn single_chunk_messages_keep_authentication_identity_and_memory_limits() {
        for kind in [
            "valid",
            "mac",
            "digest",
            "source",
            "destination",
            "budget",
            "bounds",
        ] {
            let source = config(1);
            let mut message = Message {
                from: 1,
                to: 2,
                term: 1,
                msg_type: raft::eraftpb::MessageType::MsgHeartbeat,
                context: vec![7; 32].into(),
                ..Default::default()
            };
            if kind == "source" {
                message.from = 3;
            }
            if kind == "destination" {
                message.to = 3;
            }
            let body = message.write_to_bytes().unwrap();
            let hash = if kind == "digest" {
                [0; 32]
            } else {
                Sha256::digest(&body).into()
            };
            let chunk = postcard::to_allocvec(&Chunk {
                nonce: [4; 16],
                offset: 0,
                total: if kind == "bounds" {
                    0
                } else {
                    body.len() as u64
                },
                hash,
                data: &body,
                snapshot: false,
            })
            .unwrap();
            let mut auth = authenticate(&source.secret, &source.cluster_id, 1, &chunk);
            auth.update(&[8; 16]);
            let mut mac = auth.finalize().into_bytes().to_vec();
            if kind == "mac" {
                mac[0] ^= 1;
            }
            let frame = envelope_frame(&Envelope {
                cluster: &source.cluster_id,
                from: 1,
                body: &chunk,
                mac: &mac,
                incarnation: [8; 16],
            })
            .unwrap();
            let (mut output, mut input) = tokio::io::duplex(4096);
            output.write_all(&frame).await.unwrap();
            drop(output);
            let budget = if kind == "budget" { 1 } else { 4096 };
            let bytes = Arc::new(Semaphore::new(budget));
            let result = receive_message(&config(2), &mut input, bytes.clone(), None).await;
            if kind == "valid" {
                let (actual, nonce, permit) = result.unwrap();
                assert_eq!(actual, message);
                assert_eq!(nonce, [8; 16]);
                assert_eq!(bytes.available_permits(), budget - body.len());
                drop(permit);
            } else {
                assert!(result.is_err(), "accepted {kind} message");
                if kind == "digest" {
                    assert!(result.unwrap_err().to_string().contains("digest mismatch"));
                }
            }
            assert_eq!(bytes.available_permits(), budget);
        }
    }
}

#[cfg(test)]
mod checkpoint_tests {
    use super::*;
    #[test]
    fn checkpoint_cache_is_bound_to_position_and_checksum() {
        let dir = tempfile::tempdir().unwrap();
        let config:ClusterConfig=serde_json::from_value(serde_json::json!({
            "cluster_id":"checkpoint", "node_id":1, "secret":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            "peers":{
                "1":{"raft":"127.0.0.1:41001","client":"127.0.0.1:41002","service":"127.0.0.1:41003"},
                "2":{"raft":"127.0.0.1:41004","client":"127.0.0.1:41005","service":"127.0.0.1:41006"},
                "3":{"raft":"127.0.0.1:41007","client":"127.0.0.1:41008","service":"127.0.0.1:41009"}
            }
        })).unwrap();
        let db = Arc::new(Mutex::new(
            Database::open(&dir.path().join("db"), &config, true).unwrap(),
        ));
        let (_role, rx) = watch::channel(Role {
            leader: 1,
            term: 1,
            configuration: 0,
        });
        let (tx, _requests) = mpsc::sync_channel(1);
        let mut store = ClusterStore {
            client: Client {
                tx,
                role: rx,
                db: db.clone(),
                id: 1,
                term: 1,
                replicas: 3,
                configuration: 0,
                session: 0,
                active_session: Arc::new(AtomicU64::new(0)),
            },
        };
        let mut source =
            crate::durable::Engine::recover(crate::storage::MemoryStore::default()).unwrap();
        source.checkpoint().unwrap();
        let data = source
            .into_store()
            .load_checkpoint()
            .unwrap()
            .unwrap()
            .1
            .to_vec();
        let expected = data.clone();
        {
            let mut db = db.lock().unwrap();
            let entry = raft::eraftpb::Entry {
                index: 1,
                term: 1,
                ..Default::default()
            };
            let hard = raft::eraftpb::HardState {
                term: 1,
                vote: 1,
                commit: 1,
                ..Default::default()
            };
            db.persist(&[entry], Some(&hard)).unwrap();
            db.apply(1).unwrap();
        }
        store
            .checkpoint_job(
                0,
                Box::new(move || {
                    Ok(crate::storage::CheckpointImage {
                        data,
                        live: Vec::new(),
                        imports: Vec::new(),
                    })
                }),
            )
            .unwrap();
        assert_eq!(
            store.load_checkpoint().unwrap().unwrap().1.as_ref(),
            expected.as_slice()
        );
        // A new service in the same Raft term/configuration fences the old
        // worker before it can enqueue any stale prepared operation.
        store.client.active_session.store(1, Ordering::Release);
        assert!(store.client.propose(Vec::new()).is_err());
        assert!(_requests.try_recv().is_err());
        assert!(!db.lock().unwrap().failed);
        store.client.active_session.store(0, Ordering::Release);
        db.lock()
            .unwrap()
            .persist(
                &[],
                Some(&raft::eraftpb::HardState {
                    term: 2,
                    vote: 0,
                    commit: 1,
                    ..Default::default()
                }),
            )
            .unwrap();
        assert!(store.client.read(|_| Ok(())).is_err());
        assert!(
            !db.lock().unwrap().failed,
            "stale watched epoch must not quarantine healthy storage"
        );
        store.client.term = 2;
        _role.send_replace(Role {
            leader: 1,
            term: 2,
            configuration: 0,
        });
        db.lock()
            .unwrap()
            .conn
            .execute(
                "UPDATE cluster_meta SET value=?1 WHERE key='app_snapshot'",
                [b"SNAPSHOT".as_slice()],
            )
            .unwrap();
        assert!(store.load_checkpoint().is_err());
    }
}

#[cfg(test)]
mod application_failure_tests {
    use super::*;
    #[test]
    fn read_reply_observes_the_committed_epoch_before_returning_to_an_old_service() {
        let dir = tempfile::tempdir().unwrap();
        let (mut client, requests) = client(dir.path());
        let database = client.db.clone();
        let (roles, role) = watch::channel(Role {
            leader: 1,
            term: 1,
            configuration: 0,
        });
        client.role = role;
        let (done, result) = mpsc::channel();
        let caller = std::thread::spawn(move || done.send(client.barrier()).unwrap());
        let Input::Read { reply, .. } = requests.recv_timeout(Duration::from_secs(1)).unwrap()
        else {
            panic!("expected a fresh quorum read");
        };
        let mut node = RawNode::new(
            &raft::Config {
                id: 1,
                applied: 1,
                ..Default::default()
            },
            LogStorage {
                db: database.clone(),
            },
            &slog::Logger::root(slog::Discard, slog::o!()),
        )
        .unwrap();
        project(
            &mut node,
            |node, completed| {
                let mut db = node.store().db.lock().unwrap();
                let mut hard = db.hard()?;
                hard.term = 2;
                hard.vote = 2;
                db.persist(&[], Some(&hard))?;
                completed.push((reply, 0));
                Ok(Role {
                    leader: 2,
                    term: 2,
                    configuration: 0,
                })
            },
            |epoch| {
                let db = database.lock().unwrap();
                assert!(db.conn.is_autocommit(), "epoch published before SQL commit");
                assert_eq!(db.hard().unwrap().term, 2);
                drop(db);
                assert!(
                    matches!(
                        result.recv_timeout(Duration::from_millis(100)),
                        Err(mpsc::RecvTimeoutError::Timeout)
                    ),
                    "read reply escaped before epoch publication"
                );
                roles.send_replace(*epoch);
            },
        )
        .unwrap();
        assert!(result
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .is_err());
        caller.join().unwrap();
        assert!(
            !database.lock().unwrap().failed,
            "legitimate epoch change quarantined the voter"
        );
    }
    #[test]
    fn content_reader_waits_for_projection_commit_and_cannot_see_a_rolled_back_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let (client, _requests) = client(dir.path());
        let database = client.db.clone();
        let gate = database.lock().unwrap().gate.clone();
        let owner = gate.lock().unwrap();
        {
            let mut db = database.lock().unwrap();
            db.begin_projection().unwrap();
            db.conn
                .execute("INSERT INTO event_index VALUES(1,1,0)", [])
                .unwrap();
        }
        let (started, waiting) = mpsc::channel();
        let (done, result) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            started.send(()).unwrap();
            done.send(client.read(|db| db.end())).unwrap();
        });
        waiting.recv().unwrap();
        let early = result.recv_timeout(Duration::from_millis(100));
        database.lock().unwrap().finish_projection(false).unwrap();
        drop(owner);
        reader.join().unwrap();
        assert!(
            matches!(early, Err(mpsc::RecvTimeoutError::Timeout)),
            "reader exposed an uncommitted projection: {early:?}"
        );
        assert_eq!(result.recv().unwrap().unwrap(), 0);
    }
    fn client(root: &Path) -> (Client, mpsc::Receiver<Input>) {
        let config = super::tests::config(1);
        let mut db = Database::open(&root.join("db"), &config, true).unwrap();
        let key = ContentKey {
            generation: 1,
            sequence: 1,
        };
        let entry = raft::eraftpb::Entry {
            index: 1,
            term: 1,
            data: postcard::to_allocvec(&Batch {
                version: 1,
                records: vec![],
                contents: vec![Content {
                    key,
                    subject: "data".into(),
                    headers: Bytes::new(),
                    payload: Bytes::from_static(b"body"),
                }],
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
                commit: 1,
                ..Default::default()
            }),
        )
        .unwrap();
        db.apply(1).unwrap();
        let (_, role) = watch::channel(Role {
            leader: 1,
            term: 1,
            configuration: 0,
        });
        let (tx, rx) = mpsc::sync_channel(4);
        (
            Client {
                tx,
                role,
                db: Arc::new(Mutex::new(db)),
                id: 1,
                term: 1,
                configuration: 0,
                replicas: 3,
                session: 0,
                active_session: Arc::new(AtomicU64::new(0)),
            },
            rx,
        )
    }
    #[test]
    fn missing_required_content_and_invalid_references_fence_consensus() {
        for kind in [
            "single",
            "batch",
            "batch-identity",
            "batch-entry",
            "reference",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let (client, rx) = client(dir.path());
            let db = client.db.clone();
            let store = ClusterStore { client };
            let key = ContentKey {
                generation: 1,
                sequence: 1,
            };
            let result = if kind == "reference" {
                store.validate_content(key, "wrong-subject", 0, 4)
            } else {
                let query = match kind {
                    "batch-identity" => "UPDATE body_index SET ordinal=1",
                    "batch-entry" => "UPDATE entries SET data=x'000000'",
                    _ => "DELETE FROM body_index",
                };
                db.lock().unwrap().conn.execute(query, []).unwrap();
                if kind == "single" {
                    store.get_content(key).map(|_| ())
                } else {
                    store.get_contents(&[key]).map(|_| ())
                }
            };
            assert!(result.is_err());
            assert!(db.lock().unwrap().failed);
            assert!(matches!(rx.try_recv(), Ok(Input::Fault)));
        }
    }
    #[test]
    fn malformed_recovery_fences_current_replica_but_epoch_changes_and_network_errors_do_not() {
        use crate::durable::Error;
        for kind in [
            "invalid",
            "json",
            "codec",
            "missing",
            "network",
            "session",
            "term",
            "configuration",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let (client, rx) = client(dir.path());
            let failure = match kind {
                "json" => Error::Json(serde_json::from_str::<serde_json::Value>("{").unwrap_err()),
                "codec" => Error::Codec(postcard::Error::DeserializeUnexpectedEnd),
                "missing" => Error::Store(StoreError::Corrupt(1)),
                "network" => Error::Store(
                    io::Error::new(io::ErrorKind::TimedOut, "quorum unavailable").into(),
                ),
                _ => Error::Invalid("bad durable model"),
            };
            if kind == "session" {
                client.active_session.store(1, Ordering::Release);
            }
            if kind == "term" {
                client
                    .db
                    .lock()
                    .unwrap()
                    .persist(
                        &[],
                        Some(&raft::eraftpb::HardState {
                            term: 2,
                            commit: 1,
                            ..Default::default()
                        }),
                    )
                    .unwrap();
            }
            if kind == "configuration" {
                client
                    .db
                    .lock()
                    .unwrap()
                    .conn
                    .execute(
                        "INSERT OR REPLACE INTO cluster_meta VALUES('conf_index',?1)",
                        [1u64.to_le_bytes().as_slice()],
                    )
                    .unwrap();
            }
            client.recovery_failed(&failure);
            let corrupt = matches!(kind, "invalid" | "json" | "codec" | "missing");
            assert_eq!(client.db.lock().unwrap().failed, corrupt, "{kind}");
            assert_eq!(matches!(rx.try_recv(), Ok(Input::Fault)), corrupt, "{kind}");
        }
    }
}
