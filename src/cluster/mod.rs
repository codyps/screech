//! Raft cluster with snapshot catch-up and learner-based voter replacement.
//! Clients at every node reach the elected
//! leader's NATS service. Connections end on a view change and clients reconnect.
//! All durable operations and current reads cross the same consensus boundary.
mod membership;
mod startup;
mod storage;
use crate::{
    server::{Config as ServerConfig, Server},
    storage::{Append, CheckpointJob, Content, ContentKey, Record, Store, StoreError},
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
    io::{AsyncReadExt, AsyncWriteExt},
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
type Reply = mpsc::SyncSender<io::Result<u64>>;
struct Pending {
    started: Instant,
    reply: Reply,
    read_index: Option<u64>,
}
enum Input {
    Message(Message, [u8; 16], tokio::sync::OwnedSemaphorePermit),
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
        // A service can be replaced while ReadIndex is in flight, including in
        // the same Raft term. Check both the published and durable epochs.
        self.read(|_| Ok(()))
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
        let mut db = self.db.lock().unwrap();
        // A configuration/term can become durable before the watch is published,
        // or while this caller waits for storage. This is stale work, not damage.
        self.current()?;
        if db.hard()?.term != self.term || db.conf_index()? != self.configuration {
            return Err(error("application epoch changed"));
        }
        let result = f(&mut db);
        if result.is_err() {
            db.failed = true;
            let _ = self.tx.try_send(Input::Fault);
        }
        result
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
impl Store for ClusterStore {
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
        let data = postcard::to_allocvec(&Batch {
            version: 1,
            records: r.to_vec(),
            contents: c.to_vec(),
        })
        .map_err(|e| StoreError::Io(error(e)))?;
        if data.len() > 32 * 1024 * 1024 {
            return Err(StoreError::Invalid);
        }
        self.client.propose(data).map_err(StoreError::from)
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
            .read(|db| {
                keys.iter()
                    .map(|key| {
                        db.content(*key)?
                            .ok_or_else(|| error("missing committed content"))
                    })
                    .collect()
            })
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
        let gate = self.client.db.lock().unwrap().gate.clone();
        let _gate = gate.lock().unwrap();
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
fn pump(
    node: &mut RawNode<LogStorage>,
    pending: &mut BTreeMap<Vec<u8>, Pending>,
    out: &BTreeMap<u64, tokio::sync::mpsc::Sender<Message>>,
    routes: &BTreeMap<u64, u64>,
) -> io::Result<()> {
    while node.has_ready() {
        let gate = node.store().db.lock().unwrap().gate.clone();
        let _gate = gate.lock().unwrap();
        let mut ready = node.ready();
        let read_states = ready.take_read_states();
        let old = node.store().db.lock().unwrap().hard()?;
        let epoch_change = !ready.snapshot().is_empty()
            || ready
                .hs()
                .is_some_and(|hard| hard.term != old.term || hard.vote != old.vote);
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
        let commit = node.store().db.lock().unwrap().hard()?.commit;
        membership::apply_configuration(node, commit)?;
        let replies = {
            let mut db = node.store().db.lock().unwrap();
            let commit = db.hard()?.commit;
            db.apply(commit)?
        };
        for (key, end) in replies {
            if let Some(request) = pending.remove(&key) {
                let _ = request.reply.send(Ok(end));
            }
        }
        let mut light = node.advance(ready);
        if let Some(commit) = light.commit_index() {
            let mut hard = node.store().db.lock().unwrap().hard()?;
            hard.commit = commit;
            node.store().db.lock().unwrap().persist(&[], Some(&hard))?;
            membership::apply_configuration(node, commit)?;
            let mut db = node.store().db.lock().unwrap();
            for (key, end) in db.apply(commit)? {
                if let Some(request) = pending.remove(&key) {
                    let _ = request.reply.send(Ok(end));
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
        let view = node.store().db.lock().unwrap().view()?;
        pending.retain(|_, request| {
            if request.read_index.is_some_and(|index| index <= view) {
                let _ = request.reply.send(Ok(view));
                false
            } else {
                true
            }
        });
    }
    Ok(())
}
fn dispatch_messages(
    node: &RawNode<LogStorage>,
    messages: Vec<Message>,
    out: &BTreeMap<u64, tokio::sync::mpsc::Sender<Message>>,
    routes: &BTreeMap<u64, u64>,
) -> io::Result<()> {
    if !messages.is_empty() {
        let identities = node.store().db.lock().unwrap().membership()?.identities;
        for message in messages {
            let route = if out.contains_key(&message.to) {
                message.to
            } else {
                identities
                    .get(&message.to)
                    .map(|n| u64::from_le_bytes(n[..8].try_into().unwrap()))
                    .or_else(|| routes.get(&message.to).copied())
                    .unwrap_or(message.to)
            };
            if let Some(tx) = out.get(&route) {
                let _ = tx.try_send(message);
            }
        }
    }
    Ok(())
}
#[derive(Serialize, Deserialize)]
struct Envelope {
    cluster: String,
    from: u64,
    body: Vec<u8>,
    mac: Vec<u8>,
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
struct Chunk {
    nonce: [u8; 16],
    offset: u64,
    total: u64,
    hash: [u8; 32],
    data: Vec<u8>,
    snapshot: bool,
}
async fn send_message(
    config: &ClusterConfig,
    stream: &mut TcpStream,
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
        let chunk = Chunk {
            nonce,
            offset: (n * 1024 * 1024) as u64,
            total: body.len() as u64,
            hash,
            data: data.to_vec(),
            snapshot: message.get_msg_type() == raft::eraftpb::MessageType::MsgSnapshot,
        };
        let body = postcard::to_allocvec(&chunk).map_err(error)?;
        let mut auth = authenticate(&config.secret, &config.cluster_id, config.node_id, &body);
        auth.update(&incarnation);
        let mac = auth.finalize().into_bytes().to_vec();
        let envelope = postcard::to_allocvec(&Envelope {
            cluster: config.cluster_id.clone(),
            from: config.node_id,
            body,
            mac,
            incarnation,
        })
        .map_err(error)?;
        let mut frame = Vec::with_capacity(4 + envelope.len());
        frame.extend_from_slice(&(envelope.len() as u32).to_be_bytes());
        frame.extend_from_slice(&envelope);
        stream.write_all(&frame).await?;
    }
    Ok(())
}
async fn receive_message(
    config: &ClusterConfig,
    stream: &mut TcpStream,
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
            &envelope.cluster,
            envelope.from,
            &envelope.body,
        );
        auth.update(&envelope.incarnation);
        auth.verify_slice(&envelope.mac).map_err(error)?;
        let chunk: Chunk = postcard::from_bytes(&envelope.body).map_err(error)?;
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
        data.extend_from_slice(&chunk.data);
        if chunk.snapshot {
            crate::fault::point("after_cluster_snapshot_chunk");
        }
        if data.len() as u64 == chunk.total {
            let actual: [u8; 32] = Sha256::digest(&data).into();
            if actual != chunk.hash {
                return Err(error("transfer digest mismatch"));
            }
            let message = Message::parse_from_bytes(&data).map_err(error)?;
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
) -> BTreeMap<u64, tokio::sync::mpsc::Sender<Message>> {
    let mut out = BTreeMap::new();
    for (&id, peer) in &config.peers {
        if id != config.node_id {
            let (send, mut receive) = tokio::sync::mpsc::channel::<Message>(4);
            out.insert(id, send);
            let config = config.clone();
            let address = peer.raft;
            let mut stop = stop.clone();
            let tx = tx.clone();
            tokio::spawn(async move {
                // A replacement and its retiring voter share one physical
                // address. Their authenticated destinations need independent
                // streams: rejecting an old ID must not drop the new ID's next
                // frame. Bound the cache across repeated replacements.
                let mut streams: BTreeMap<u64, (TcpStream, Instant)> = BTreeMap::new();
                loop {
                    tokio::select! {
                        _=stop.changed()=>break,
                        message=receive.recv()=>{let Some(message)=message else{break};
                            let snapshot=message.get_msg_type()==raft::eraftpb::MessageType::MsgSnapshot;
                            let result=tokio::select! {
                                _=stop.changed()=>break,
                                result=tokio::time::timeout(Duration::from_secs(30),async {
                                    if !streams.contains_key(&message.to) {
                                        if streams.len()>=4 {
                                            if let Some(oldest)=streams.iter().min_by_key(|(_,(_,used))|*used).map(|(id,_)|*id) {streams.remove(&oldest);}
                                        }
                                        let connected=TcpStream::connect(address).await?;
                                        connected.set_nodelay(true)?;
                                        streams.insert(message.to,(connected,Instant::now()));
                                    }
                                    let (stream,used)=streams.get_mut(&message.to).unwrap();
                                    *used=Instant::now();
                                    send_message(&config,stream,&message,incarnation).await
                                })=>result,
                            };
                            if !matches!(result,Ok(Ok(()))) { streams.remove(&message.to); }
                            if snapshot {let _=tx.try_send(Input::SnapshotSent(message.to,matches!(result,Ok(Ok(())))));}
                        }
                    }
                }
            });
        }
    }
    out
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
    let (role_tx, role) = watch::channel(Role::default());
    let (stop, stop_rx) = watch::channel(false);
    let incarnation = db.lock().unwrap().incarnation;
    let out = network(&config, tx.clone(), stop_rx.clone(), incarnation).await;
    let log = LogStorage { db: db.clone() };
    let raft_config = raft::Config {
        id: config.node_id,
        election_tick: 15,
        heartbeat_tick: 2,
        check_quorum: true,
        pre_vote: true,
        max_size_per_msg: 1024 * 1024,
        max_inflight_msgs: 8,
        applied: db.lock().unwrap().hard()?.commit,
        ..Default::default()
    };
    let logger = slog::Logger::root(slog::Discard, slog::o!());
    let mut node = RawNode::new(&raft_config, log, &logger).map_err(error)?;
    let active_session = Arc::new(AtomicU64::new(0));
    hub.install(startup::Active {
        config: config.clone(),
        tx: tx.clone(),
        role: role.clone(),
        db: db.clone(),
        session: active_session.clone(),
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
            // New learners must reply to replacement leaders before receiving
            // a snapshot containing the committed placement map. These routes
            // are transport hints only; membership still gates every message.
            let mut routes = BTreeMap::new();
            let mut tick = Instant::now();
            loop {
                let mut incoming_lease = None;
                match rx.recv_timeout(Duration::from_millis(20)) {
                    Ok(Input::Stop) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    Ok(Input::SnapshotSent(id, ok)) => node.report_snapshot(
                        id,
                        if ok {
                            raft::SnapshotStatus::Finish
                        } else {
                            raft::SnapshotStatus::Failure
                        },
                    ),
                    Ok(Input::Fault) => return Err(error("replica storage failed; quarantined")),
                    Ok(Input::Message(message, nonce, permit)) => {
                        incoming_lease = Some(permit);
                        let db = node.store().db.lock().unwrap();
                        let allowed = db.accepts_sender(&message, nonce)?;
                        drop(db);
                        if allowed {
                            tracing::trace!(from=message.from,to=message.to,kind=?message.get_msg_type(),index=message.index,commit=message.commit,"authenticated peer message");
                            if let Some(slot) = actor_config.slot_for(message.from, &BTreeMap::from([(message.from,nonce)])) {
                                routes.insert(message.from,slot);
                            }
                            if message.get_msg_type() == raft::eraftpb::MessageType::MsgSnapshot {
                                tracing::debug!(from=message.from,to=message.to,index=message.get_snapshot().get_metadata().index, conf=?message.get_snapshot().get_metadata().get_conf_state(),"received snapshot");
                            }
                            if let Err(error)=node.step(message) {tracing::debug!(%error,"peer Raft step rejected");}
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
                                    &mut node,
                                    membership::Replacement {
                                        old,
                                        new: member,
                                        nonce,
                                    },
                                )?;
                            } else {
                                membership::enroll_standby(&mut node, member, nonce, learner)?;
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
                            || configuration != node.store().db.lock().unwrap().conf_index()?
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
                            let result = membership::start_replacement(&mut node, replacement);
                            let _ = reply.send(result.map(|_| 0));
                        }
                    }
                    Ok(Input::Read {term,configuration,session,reply}) => {
                        if node.raft.state != StateRole::Leader
                            || node.raft.term != term
                            || configuration != node.store().db.lock().unwrap().conf_index()?
                            || session != actor_session.load(Ordering::Acquire)
                        {
                            let _ = reply.send(Err(error("not current leader")));
                        } else {
                            serial = serial.checked_add(1).ok_or_else(|| error("proposal IDs exhausted"))?;
                            let mut context = nonce.to_vec();
                            context.extend_from_slice(&serial.to_le_bytes());
                            node.read_index(context.clone());
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
                            || configuration != node.store().db.lock().unwrap().conf_index()?
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
                pump(&mut node, &mut pending, &out, &routes)?;
                drop(incoming_lease);
                membership::advance_replacement(&mut node)?;
                let configuration = node.store().db.lock().unwrap().conf_index()?;
                let role = Role {
                    leader: node.raft.leader_id,
                    term: node.raft.term,
                    configuration,
                };
                role_tx.send_if_modified(|old| {
                    if *old != role {
                        *old = role;
                        true
                    } else {
                        false
                    }
                });
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
    // Leader-local service lifecycle. The public endpoints proxy all protocols,
    // including Core subscriptions and pull inboxes, through this one owner.
    let connection_limit = server_config.max_connections;
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
                                running = Some((stop, tokio::spawn(server.run(rx)), current));
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
                tokio::spawn(async move {
                    let _slot=slot;
                    let selected=tokio::time::timeout(Duration::from_secs(6),async{
                        loop {
                            let current=*role.borrow_and_update();
                            let leader_slot=if peers.contains_key(&current.leader){current.leader}else{route_db.lock().unwrap().membership().ok().and_then(|m|m.identities.get(&current.leader).map(|n|u64::from_le_bytes(n[..8].try_into().unwrap()))).unwrap_or(0)};
                            if let Some(peer)=peers.get(&leader_slot) {if let Ok(stream)=TcpStream::connect(peer.service).await{stream.set_nodelay(true)?;return Ok::<_,io::Error>((current,stream));}}
                            tokio::select!{_=role.changed()=>{},_=tokio::time::sleep(Duration::from_millis(50))=>{}}
                        }
                    }).await;
                    if let Ok(Ok((epoch,mut outgoing)))=selected {
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
            self.nodes[leader].read_index(context.clone());
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
        assert!(!membership::step_down_removed(&mut sim.nodes[0]));
        sim.nodes[0]
            .propose_conf_change(vec![], ConfChangeV2::default())
            .unwrap();
        sim.settle(None);
        assert_eq!(sim.nodes[0].raft.state, StateRole::Follower);
        assert!(!membership::step_down_removed(&mut sim.nodes[0]));
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
            let rx = sim.quorum_read(0, b"partitioned-read".to_vec());
            sim.settle(Some(1));
            assert!(matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
            sim.ticks(3, None);
            assert_eq!(rx.try_recv().unwrap().unwrap(), before.2);
            let db = sim.nodes[0].store().db.lock().unwrap();
            assert_eq!(
                (db.last().unwrap(), db.hard().unwrap(), db.view().unwrap()),
                before
            );
        }
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
            let (mut stream, _) = listener.accept().await.unwrap();
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
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream.set_nodelay(true).unwrap();
        for message in messages {
            send_message(&config(1), &mut stream, &message, [7; 16])
                .await
                .unwrap();
        }
        receiving.await.unwrap();
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
        for kind in ["single", "batch", "reference"] {
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
                db.lock()
                    .unwrap()
                    .conn
                    .execute("DELETE FROM body_index", [])
                    .unwrap();
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
