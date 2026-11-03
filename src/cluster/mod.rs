//! Raft cluster with snapshot catch-up and learner-based voter replacement.
//! Clients at every node reach the elected
//! leader's NATS service. Connections end on a view change and clients reconnect.
//! All durable operations and current reads cross the same consensus boundary.
mod membership;
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
                    || db.view()?.saturating_sub(db.snapshot_base()?.0) >= 256
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
        self.client
            .propose(Vec::new())
            .map(|_| ())
            .map_err(StoreError::from)
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
        self.client
            .read(|db| db.content(key))
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
    pending: &mut BTreeMap<Vec<u8>, (Instant, Reply)>,
    out: &BTreeMap<u64, tokio::sync::mpsc::Sender<Message>>,
) -> io::Result<()> {
    while node.has_ready() {
        let gate = node.store().db.lock().unwrap().gate.clone();
        let _gate = gate.lock().unwrap();
        let mut ready = node.ready();
        {
            let mut db = node.store().db.lock().unwrap();
            if ready.snapshot().is_empty() {
                db.persist(ready.entries(), ready.hs())?;
            } else {
                db.install_snapshot(ready.snapshot(), ready.hs(), ready.entries(), false)?;
            }
        }
        let mut messages = ready.take_messages();
        messages.extend(ready.take_persisted_messages());
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
            if let Some((_, reply)) = pending.remove(&key) {
                let _ = reply.send(Ok(end));
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
                if let Some((_, reply)) = pending.remove(&key) {
                    let _ = reply.send(Ok(end));
                }
            }
        }
        messages.extend(light.take_messages());
        // The application is represented by its committed indexes, advanced above.
        node.advance_apply();
        for message in messages {
            if let Some(tx) = out.get(&message.to) {
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
    address: SocketAddr,
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
    let mut stream = TcpStream::connect(address).await?;
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
        stream.write_u32(envelope.len() as u32).await?;
        stream.write_all(&envelope).await?;
    }
    Ok(())
}
async fn receive_message(
    config: &ClusterConfig,
    stream: &mut TcpStream,
    bytes: Arc<Semaphore>,
) -> io::Result<(Message, [u8; 16], tokio::sync::OwnedSemaphorePermit)> {
    let mut data = Vec::new();
    let mut identity = None;
    let mut permit = None;
    loop {
        let size = stream.read_u32().await? as usize;
        if size > 2 * 1024 * 1024 {
            return Err(error("invalid transfer chunk size"));
        }
        let mut frame = vec![0; size];
        stream.read_exact(&mut frame).await?;
        let envelope: Envelope = postcard::from_bytes(&frame).map_err(error)?;
        if envelope.cluster != config.cluster_id || !config.peers.contains_key(&envelope.from) {
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
    listener: TcpListener,
    mut stop: watch::Receiver<bool>,
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
                loop {
                    tokio::select! {
                        _=stop.changed()=>break,
                        message=receive.recv()=>{let Some(message)=message else{break};
                            let snapshot=message.get_msg_type()==raft::eraftpb::MessageType::MsgSnapshot;
                            let result=tokio::time::timeout(Duration::from_secs(30),send_message(&config,address,&message,incarnation)).await;
                            if snapshot {let _=tx.try_send(Input::SnapshotSent(id,matches!(result,Ok(Ok(())))));}
                        }
                    }
                }
            });
        }
    }
    let config = config.clone();
    tokio::spawn(async move {
        let slots = Arc::new(Semaphore::new(16));
        let bytes = Arc::new(Semaphore::new(2 * (storage::MAX_SNAPSHOT + MAX_FRAME)));
        loop {
            tokio::select! {
                _=stop.changed()=>break,
                accepted=listener.accept()=>{
                    let Ok((mut stream,_))=accepted else{break};let Ok(slot)=slots.clone().try_acquire_owned() else{continue};
                    let config=config.clone();let tx=tx.clone();let bytes=bytes.clone();
                    tokio::spawn(async move{let _slot=slot;
                        if let Ok(Ok((message,nonce,permit)))=tokio::time::timeout(Duration::from_secs(30),receive_message(&config,&mut stream,bytes)).await {let _=tx.try_send(Input::Message(message,nonce,permit));}
                    });
                }
            }
        }
    });
    out
}

/// Run a cluster node until shutdown. Store paths and node incarnations are
/// immutable; damaged members are replaced through enrolled learner identities.
pub async fn run(
    config: ClusterConfig,
    path: &Path,
    bootstrap: bool,
    join: bool,
    mut server_config: ServerConfig,
    mut shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    config.validate()?;
    if join && config.voters().contains(&config.node_id) {
        return Err(error(
            "a replacement must use a new node ID outside the original voters",
        ));
    }
    let peer = &config.peers[&config.node_id];
    let listener = TcpListener::bind(peer.raft).await?;
    let clients = TcpListener::bind(peer.client).await?;
    let db = Arc::new(Mutex::new(Database::open_mode(
        path, &config, bootstrap, join,
    )?));
    let (tx, rx) = mpsc::sync_channel(256);
    let (role_tx, role) = watch::channel(Role::default());
    let (stop, stop_rx) = watch::channel(false);
    let incarnation = db.lock().unwrap().incarnation;
    if join {
        println!(
            "JOIN {} {}",
            config.node_id,
            incarnation
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        );
    }
    let out = network(&config, tx.clone(), listener, stop_rx.clone(), incarnation).await;
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
    let actor_session = active_session.clone();
    let actor_config = config.clone();
    let actor = std::thread::Builder::new()
        .name("screech-raft".into())
        .spawn(move || -> io::Result<()> {
            let mut pending: BTreeMap<Vec<u8>, (Instant, Reply)> = BTreeMap::new();
            let mut nonce = [0u8; 16];
            getrandom::fill(&mut nonce).map_err(error)?;
            let mut serial = 0u64;
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
                            let _ = node.step(message);
                        }
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
                            || !actor_config.peers.contains_key(&replacement.new)
                        {
                            let _ = reply
                                .send(Err(error("invalid replacement target or leader epoch")));
                        } else {
                            let result = membership::start_replacement(&mut node, replacement);
                            let _ = reply.send(result.map(|_| 0));
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
                            let used: i64 = node
                                .store()
                                .db
                                .lock()
                                .unwrap()
                                .conn
                                .query_row(
                                    "SELECT coalesce(sum(length(data)),0) FROM entries",
                                    [],
                                    |r| r.get(0),
                                )
                                .map_err(error)?;
                            if used as u64 + data.len() as u64 + 256 > actor_config.max_log_bytes {
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
                                        pending.insert(context, (Instant::now(), reply));
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
                pump(&mut node, &mut pending, &out)?;
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
                pending.retain(|_, (start, reply)| {
                    if node.raft.state != StateRole::Leader || start.elapsed() > RPC_TIMEOUT {
                        let _ = reply.send(Err(error(
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
                    crate::jetstream::Handle::start_single_voter(ClusterStore { client })
                        .map_err(error)
                })
                .await;
                match prepared {
                    Ok(Ok(handle)) => {
                        let mut local = server_config.clone();
                        if let Ok(conf) = service_db.lock().unwrap().conf() {
                            local.connect_urls = service_config
                                .peers
                                .iter()
                                .filter(|(id, _)| {
                                    conf.voters.contains(id) || conf.voters_outgoing.contains(id)
                                })
                                .map(|(_, p)| p.client.to_string())
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
                let (mut incoming,_)=accepted?;let Ok(slot)=slots.clone().try_acquire_owned() else{continue};
                let mut role=role.clone();let peers=config.peers.clone();let mut stop=stop_rx.clone();
                tokio::spawn(async move {
                    let _slot=slot;
                    let selected=tokio::time::timeout(Duration::from_secs(6),async{
                        loop {
                            let current=*role.borrow_and_update();
                            if let Some(peer)=peers.get(&current.leader) {if let Ok(stream)=TcpStream::connect(peer.service).await{return Ok::<_,io::Error>((current,stream));}}
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
    fn config(id: u64) -> ClusterConfig {
        ClusterConfig {
            cluster_id: "simulation".into(),
            node_id: id,
            secret: "s".repeat(32),
            max_log_bytes: log_budget(),
            initial_voters: Vec::new(),
            peers: (1..=3)
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
        receive: Vec<tokio::sync::mpsc::Receiver<Message>>,
        send: BTreeMap<u64, tokio::sync::mpsc::Sender<Message>>,
    }
    impl Simulation {
        fn new(root: &Path) -> Self {
            let mut send = BTreeMap::new();
            let mut receive = Vec::new();
            let mut nodes = Vec::new();
            for id in 1..=3 {
                let (tx, rx) = tokio::sync::mpsc::channel(1024);
                send.insert(id, tx);
                receive.push(rx);
                let db = Arc::new(Mutex::new(
                    Database::open(&root.join(id.to_string()), &config(id), true).unwrap(),
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
                nodes,
                receive,
                send,
            }
        }
        fn settle(&mut self, isolated: Option<u64>) {
            // Reverse and duplicate messages to exercise the real implementation's
            // ordering/idempotency rather than simulating an ideal transport.
            for _ in 0..100 {
                for node in &mut self.nodes {
                    pump(node, &mut BTreeMap::new(), &self.send).unwrap();
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
        sim.ticks(20, Some(1));
        let leader = (1..3)
            .find(|&n| sim.nodes[n].raft.state == StateRole::Leader)
            .unwrap();
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
