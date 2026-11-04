//! Startup is the recovery protocol: fresh peers agree on genesis, intact stores
//! resume, and unusable stores enroll fresh voting identities through the quorum.
//! The configured slot/endpoints never change. No operator bootstrap/join action
//! is needed, and a missing store never silently reuses lost vote history.
use super::*;
use fs2::FileExt;
use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::PathBuf,
    sync::RwLock,
};
const MAGIC: u32 = 0x53434833;
const CONTROL_LIMIT: usize = 4 * 1024 * 1024;
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct Claim {
    slot: u64,
    nonce: [u8; 16],
    signature: Vec<u8>,
}
#[derive(Clone, Serialize, Deserialize)]
struct Instance {
    member: u64,
    path: PathBuf,
}
#[derive(Clone, Serialize, Deserialize)]
struct Record {
    cluster: String,
    slot: u64,
    nonce: [u8; 16],
    virgin: bool,
    certificate: Option<Vec<Claim>>,
    instance: Option<Instance>,
}
#[derive(Clone)]
pub(super) struct Active {
    pub config: ClusterConfig,
    pub tx: mpsc::SyncSender<Input>,
    pub role: watch::Receiver<Role>,
    pub db: Arc<Mutex<Database>>,
    pub session: Arc<AtomicU64>,
}
#[derive(Clone)]
pub(super) struct Hub(Arc<RwLock<Shared>>);
struct Shared {
    record: Record,
    active: Option<Active>,
}
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct View {
    leader: u64,
    term: u64,
    voters: Vec<u64>,
    learners: Vec<u64>,
    outgoing: Vec<u64>,
    identities: BTreeMap<u64, [u8; 16]>,
    retired: BTreeMap<u64, u64>,
}
#[derive(Clone, Serialize, Deserialize)]
struct Status {
    slot: u64,
    fresh: Option<Claim>,
    certificate: Option<Vec<Claim>>,
    leader: Option<View>,
}
#[derive(Serialize, Deserialize)]
enum Action {
    Inspect,
    Enroll { member: u64, nonce: [u8; 16] },
}
#[derive(Serialize, Deserialize)]
struct Request {
    cluster: String,
    slot: u64,
    challenge: [u8; 16],
    action: Action,
}
#[derive(Serialize, Deserialize)]
struct Response {
    challenge: [u8; 16],
    result: Result<Status, String>,
}
#[derive(Serialize, Deserialize)]
struct Signed {
    body: Vec<u8>,
    mac: Vec<u8>,
}
fn sign(config: &ClusterConfig, domain: u64, body: &[u8]) -> Vec<u8> {
    authenticate(&config.secret, &config.cluster_id, domain, body)
        .finalize()
        .into_bytes()
        .to_vec()
}
fn encode<T: Serialize>(config: &ClusterConfig, domain: u64, value: &T) -> io::Result<Vec<u8>> {
    let body = serde_json::to_vec(value).map_err(error)?;
    serde_json::to_vec(&Signed {
        mac: sign(config, domain, &body),
        body,
    })
    .map_err(error)
}
fn decode<T: serde::de::DeserializeOwned>(
    config: &ClusterConfig,
    domain: u64,
    bytes: &[u8],
) -> io::Result<T> {
    let signed: Signed = serde_json::from_slice(bytes).map_err(error)?;
    authenticate(&config.secret, &config.cluster_id, domain, &signed.body)
        .verify_slice(&signed.mac)
        .map_err(error)?;
    serde_json::from_slice(&signed.body).map_err(error)
}
fn fresh_claim(config: &ClusterConfig, record: &Record) -> io::Result<Claim> {
    let bytes = serde_json::to_vec(&(record.slot, record.nonce, config.voters())).map_err(error)?;
    Ok(Claim {
        slot: record.slot,
        nonce: record.nonce,
        signature: sign(config, 0x47454e, &bytes),
    })
}
fn valid_certificate(config: &ClusterConfig, record: &Record, claims: &[Claim]) -> bool {
    let mut ids = std::collections::BTreeSet::new();
    for c in claims {
        let Ok(bytes) = serde_json::to_vec(&(c.slot, c.nonce, config.voters())) else {
            return false;
        };
        if !ids.insert(c.slot)
            || authenticate(&config.secret, &config.cluster_id, 0x47454e, &bytes)
                .verify_slice(&c.signature)
                .is_err()
        {
            return false;
        }
    }
    ids == config.voters().into_iter().collect()
        && claims
            .iter()
            .any(|c| c.slot == record.slot && c.nonce == record.nonce)
}
fn save(config: &ClusterConfig, path: &Path, record: &Record) -> io::Result<()> {
    let temporary = path.with_extension("node-state.tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(&encode(config, 0x4e4f4445, record)?)?;
    file.sync_all()?;
    crate::fault::point("before_cluster_startup_manifest");
    std::fs::rename(&temporary, path)?;
    File::open(path.parent().unwrap_or_else(|| Path::new(".")))?.sync_all()?;
    crate::fault::point("after_cluster_startup_manifest");
    Ok(())
}
impl Hub {
    pub fn install(&self, active: Active) {
        self.0.write().unwrap().active = Some(active);
    }
    pub fn clear(&self) {
        self.0.write().unwrap().active = None;
    }
    fn update(&self, record: &Record) {
        self.0.write().unwrap().record = record.clone();
    }
    async fn answer(&self, config: &ClusterConfig, request: &Request) -> io::Result<Status> {
        if request.cluster != config.cluster_id || !config.peers.contains_key(&request.slot) {
            return Err(error("unknown startup peer"));
        }
        let (record, active) = {
            let shared = self.0.read().unwrap();
            (shared.record.clone(), shared.active.clone())
        };
        let mut status = Status {
            slot: record.slot,
            fresh: if record.virgin {
                Some(fresh_claim(config, &record)?)
            } else {
                None
            },
            certificate: record.certificate.clone(),
            leader: None,
        };
        if let Some(active) = active {
            let role = *active.role.borrow();
            if role.leader == active.config.node_id {
                let action = match request.action {
                    Action::Inspect => None,
                    Action::Enroll { member, nonce } => Some((request.slot, member, nonce)),
                };
                let active_config = active.config.clone();
                let result = tokio::task::spawn_blocking(move || -> io::Result<View> {
                    let client = Client {
                        tx: active.tx,
                        role: active.role,
                        db: active.db,
                        id: active.config.node_id,
                        term: role.term,
                        configuration: role.configuration,
                        replicas: active.config.voters().len(),
                        session: active.session.load(Ordering::Acquire),
                        active_session: active.session,
                    };
                    // This is a logged observation, never a cached leader hint.
                    client.propose(Vec::new())?;
                    if let Some((slot, member, nonce)) = action {
                        if active_config.peers.contains_key(&member)
                            || u64::from_le_bytes(nonce[..8].try_into().unwrap()) != slot
                            || !active_config.peers.contains_key(&slot)
                        {
                            return Err(error("invalid fresh recovery identity"));
                        }
                        let (reply, rx) = mpsc::sync_channel(1);
                        client
                            .tx
                            .try_send(Input::Enroll {
                                slot,
                                member,
                                nonce,
                                term: client.term,
                                reply,
                            })
                            .map_err(error)?;
                        rx.recv_timeout(RPC_TIMEOUT).map_err(error)??;
                    }
                    client.read(|db| {
                        let conf = db.conf()?;
                        let m = db.membership()?;
                        Ok(View {
                            leader: client.id,
                            term: client.term,
                            voters: conf.voters,
                            learners: conf.learners,
                            outgoing: conf.voters_outgoing,
                            identities: m.identities,
                            retired: m.retired,
                        })
                    })
                })
                .await
                .map_err(error)?;
                if let Ok(view) = result {
                    status.leader = Some(view);
                }
            }
        }
        Ok(status)
    }
}
async fn write_frame(stream: &mut TcpStream, data: &[u8]) -> io::Result<()> {
    if data.len() > CONTROL_LIMIT {
        return Err(error("startup response too large"));
    }
    stream.write_u32(data.len() as u32).await?;
    stream.write_all(data).await
}
async fn read_frame(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let len = stream.read_u32().await? as usize;
    if len > CONTROL_LIMIT {
        return Err(error("startup frame too large"));
    }
    let mut bytes = vec![0; len];
    stream.read_exact(&mut bytes).await?;
    Ok(bytes)
}
async fn rpc(config: &ClusterConfig, peer: &Peer, action: Action) -> io::Result<Status> {
    let mut challenge = [0; 16];
    getrandom::fill(&mut challenge).map_err(error)?;
    let request = Request {
        cluster: config.cluster_id.clone(),
        slot: config.node_id,
        challenge,
        action,
    };
    let bytes = encode(config, 0x52514553, &request)?;
    tokio::time::timeout(Duration::from_secs(6), async {
        let mut stream = TcpStream::connect(peer.raft).await?;
        stream.write_u32(MAGIC).await?;
        write_frame(&mut stream, &bytes).await?;
        let reply: Response = decode(config, 0x5253504e, &read_frame(&mut stream).await?)?;
        if reply.challenge != challenge {
            return Err(error("replayed startup response"));
        }
        reply.result.map_err(error)
    })
    .await
    .map_err(error)?
}
async fn inspect(config: &ClusterConfig) -> Vec<Status> {
    let mut tasks = tokio::task::JoinSet::new();
    for peer in config.peers.values() {
        let config = config.clone();
        let peer = peer.clone();
        tasks.spawn(async move { rpc(&config, &peer, Action::Inspect).await });
    }
    let mut statuses = Vec::new();
    while let Some(result) = tasks.join_next().await {
        if let Ok(Ok(status)) = result {
            statuses.push(status);
        }
    }
    statuses
}
async fn listener(
    config: ClusterConfig,
    hub: Hub,
    listener: TcpListener,
    mut stop: watch::Receiver<bool>,
) {
    let slots = Arc::new(Semaphore::new(32));
    let bytes = Arc::new(Semaphore::new(2 * (storage::MAX_SNAPSHOT + MAX_FRAME)));
    loop {
        tokio::select! {
            _=stop.changed()=>break,
            accepted=listener.accept()=>{
                let Ok((mut stream,_))=accepted else{break};let Ok(slot)=slots.clone().try_acquire_owned() else{continue};
                let config=config.clone();let hub=hub.clone();let bytes=bytes.clone();
                tokio::spawn(async move{let _slot=slot;let result=async {
                    stream.set_nodelay(true)?;
                    let first=tokio::time::timeout(Duration::from_secs(30),stream.read_u32()).await??;
                    if first==MAGIC {
                        tokio::time::timeout(Duration::from_secs(30),async {
                        let request:Request=decode(&config,0x52514553,&read_frame(&mut stream).await?)?;
                        let result=hub.answer(&config,&request).await.map_err(|e|e.to_string());
                        write_frame(&mut stream,&encode(&config,0x5253504e,&Response{challenge:request.challenge,result})?).await?;
                        Ok::<_,io::Error>(())
                        }).await??;
                    }else{
                        let mut first=first;
                        loop {
                        let active=hub.0.read().unwrap().active.clone();
                        let Some(active)=active else {return Ok(());};
                            let (message,nonce,permit)=tokio::time::timeout(Duration::from_secs(30),receive_message(&active.config,&mut stream,bytes.clone(),Some(first))).await??;
                            active.tx.try_send(Input::Message(message,nonce,permit)).map_err(error)?;
                            first=tokio::time::timeout(Duration::from_secs(30),stream.read_u32()).await??;
                        }
                    }
                    Ok::<_,io::Error>(())
                }.await;if let Err(error)=result {tracing::debug!(%error,"cluster peer connection ended");}});
            }
        }
    }
}
fn record_for(config: &ClusterConfig, store: &Path, manifest: &Path) -> io::Result<Record> {
    if let Ok(bytes) = std::fs::read(manifest) {
        if let Ok(record) = decode::<Record>(config, 0x4e4f4445, &bytes) {
            if record.cluster != config.cluster_id || record.slot != config.node_id {
                return Err(error("startup identity mismatch"));
            }
            return Ok(record);
        }
        // Preserve corrupt metadata, never reinterpret it as unused genesis.
        let mut suffix = [0; 8];
        getrandom::fill(&mut suffix).map_err(error)?;
        std::fs::copy(
            manifest,
            manifest.with_extension(format!("damaged-{:016x}", u64::from_le_bytes(suffix))),
        )?;
    }
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).map_err(error)?;
    let evidence = manifest.exists()
        || store.with_extension("cluster-lock").exists()
        || store.with_extension("recovery").exists()
        || store.with_extension("cluster-watermark").exists()
        || std::fs::metadata(store).is_ok_and(|m| m.len() > 0);
    Ok(Record {
        cluster: config.cluster_id.clone(),
        slot: config.node_id,
        nonce,
        virgin: !evidence,
        certificate: None,
        instance: if store.exists() {
            Some(Instance {
                member: config.node_id,
                path: store.to_path_buf(),
            })
        } else {
            None
        },
    })
}
async fn open_existing(config: ClusterConfig, instance: Instance) -> io::Result<Database> {
    tokio::task::spawn_blocking(move || {
        let mut config = config;
        config.node_id = instance.member;
        Database::open_mode(&instance.path, &config, false, false)
    })
    .await
    .map_err(error)?
}
fn new_instance(config: &ClusterConfig, store: &Path) -> io::Result<(Instance, Database)> {
    let root = store.with_extension("recovery");
    std::fs::create_dir_all(&root)?;
    let (member, directory) = loop {
        let mut random = [0; 8];
        getrandom::fill(&mut random).map_err(error)?;
        let member = u64::from_le_bytes(random) | (1 << 63);
        if config.peers.contains_key(&member) {
            continue;
        }
        let directory = root.join(format!("{member:016x}"));
        match std::fs::create_dir(&directory) {
            Ok(()) => break (member, directory),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
    };
    // Publish every parent directory before a manifest can select this path.
    File::open(&root)?.sync_all()?;
    File::open(store.parent().expect("absolute store path"))?.sync_all()?;
    let path = directory.join("store.db");
    let mut runtime = config.clone();
    runtime.node_id = member;
    // Bind the physical slot before the first durable root is created. An
    // identity cannot change inside an accepted journal's history.
    let db = Database::open_learner(&path, &runtime, config.node_id)?;
    Ok((Instance { member, path }, db))
}
// The signed virgin record guarantees no actor has ever been allowed to vote.
// A crash during first database creation can leave incomplete files; archive
// them and finish the already-agreed genesis, before clearing that virgin bit.
fn open_genesis(config: &ClusterConfig, store: &Path) -> io::Result<Database> {
    match Database::open_mode(store, config, true, false) {
        Ok(db) => Ok(db),
        Err(e) => {
            tracing::warn!(%e,"resuming interrupted initial database creation");
            let mut random = [0; 8];
            getrandom::fill(&mut random).map_err(error)?;
            let archive = store
                .with_extension("recovery")
                .join(format!("initializing-{:016x}", u64::from_le_bytes(random)));
            std::fs::create_dir_all(&archive)?;
            for source in [
                store.to_path_buf(),
                PathBuf::from(format!("{}-wal", store.display())),
                PathBuf::from(format!("{}-shm", store.display())),
                store.with_extension("cluster-lock"),
                store.with_extension("cluster-watermark"),
                store.with_extension("cluster-watermark.tmp"),
            ] {
                if source.exists() {
                    std::fs::rename(&source, archive.join(source.file_name().unwrap()))?;
                }
            }
            File::open(&archive)?.sync_all()?;
            File::open(store.parent().unwrap())?.sync_all()?;
            Database::open_mode(store, config, true, false)
        }
    }
}
/// One command for initial formation, ordinary restart and repair. Slot locking
/// spans every generation; archived databases are never reset or used as new voters.
pub async fn run(
    config: ClusterConfig,
    store: &Path,
    server: ServerConfig,
    mut shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    config.validate()?;
    let parent = std::fs::canonicalize(
        store
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new(".")),
    )?;
    let store = parent.join(
        store
            .file_name()
            .ok_or_else(|| error("store filename required"))?,
    );
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(store.with_extension("startup-lock"))?;
    lock.try_lock_exclusive()?;
    let manifest = store.with_extension("node-state");
    let mut record = record_for(&config, &store, &manifest)?;
    save(&config, &manifest, &record)?;
    let hub = Hub(Arc::new(RwLock::new(Shared {
        record: record.clone(),
        active: None,
    })));
    let socket = TcpListener::bind(config.peers[&config.node_id].raft).await?;
    let (stop, stop_rx) = watch::channel(false);
    let listening = tokio::spawn(listener(config.clone(), hub.clone(), socket, stop_rx));
    let mut existing = match record.instance.clone() {
        Some(i) => match open_existing(config.clone(), i).await {
            Ok(db) => Some(db),
            Err(e) => {
                tracing::warn!(%e,slot=config.node_id,"stored generation requires quorum recovery");
                None
            }
        },
        None => None,
    };
    loop {
        if *shutdown.borrow() {
            break;
        }
        let statuses = inspect(&config).await;
        let leader = statuses
            .iter()
            .find_map(|s| s.leader.as_ref().map(|v| (s.slot, v.clone())));
        let mut database = None;
        if leader.is_none() {
            if let Some(db) = existing.take() {
                database = Some(db);
            } else if record.virgin && config.voters().contains(&config.node_id) {
                let proof = statuses
                    .iter()
                    .filter_map(|s| s.certificate.as_ref())
                    .find(|p| valid_certificate(&config, &record, p))
                    .cloned()
                    .or_else(|| {
                        let claims: Vec<_> = statuses
                            .iter()
                            .filter_map(|s| s.fresh.clone())
                            .filter(|c| config.voters().contains(&c.slot))
                            .collect();
                        valid_certificate(&config, &record, &claims).then_some(claims)
                    });
                if let Some(proof) = proof {
                    // Persist the formation agreement before any Raft voting.
                    record.certificate = Some(proof);
                    save(&config, &manifest, &record)?;
                    hub.update(&record);
                    let db = open_genesis(&config, &store)?;
                    record.instance = Some(Instance {
                        member: config.node_id,
                        path: store.clone(),
                    });
                    record.virgin = false;
                    save(&config, &manifest, &record)?;
                    hub.update(&record);
                    database = Some(db);
                }
            }
        } else {
            // A live quorum authorizes a new incarnation, even for a valid but
            // stale local image. Never vote using state that the quorum supersedes.
            existing.take();
            let (instance, db) = new_instance(&config, &store)?;
            record.instance = Some(instance);
            record.virgin = false;
            save(&config, &manifest, &record)?;
            hub.update(&record);
            database = Some(db);
        }
        if let Some(db) = database {
            let instance = record.instance.clone().expect("selected generation");
            tracing::info!(slot=config.node_id,member=instance.member,path=%instance.path.display(),"starting cluster generation");
            let mut runtime = config.clone();
            runtime.node_id = instance.member;
            let (halt, halt_rx) = watch::channel(false);
            let hub_runtime = hub.clone();
            let runtime_server = server.clone();
            let slot = config.node_id;
            let mut task = tokio::spawn(async move {
                super::run_runtime(runtime, slot, db, runtime_server, halt_rx, hub_runtime).await
            });
            loop {
                tokio::select! {
                    _=shutdown.changed()=>{halt.send_replace(true);let _=task.await;break;}
                    result=&mut task=>{hub.clear();if let Ok(Err(e))=result{tracing::error!(%e,"node restarting in recovery mode");}break;}
                    _=tokio::time::sleep(Duration::from_millis(300))=>{
                        let active = hub.0.read().unwrap().active.clone();
                        let enrolled = active.as_ref().is_some_and(|a| {
                            if a.role.borrow().leader == 0 { return false; }
                            let db = a.db.lock().unwrap();
                            let Ok(conf) = db.conf() else { return false; };
                            let Ok(state) = db.membership() else { return false; };
                            conf.voters.iter().chain(&conf.learners).chain(&conf.voters_outgoing).any(|id| *id == instance.member)
                                && state.identities.get(&instance.member).copied().unwrap_or([0;16]) == db.incarnation
                        });
                        // Healthy members already exchange Raft heartbeats. Only
                        // formation/recovery needs the logged discovery barrier.
                        if enrolled { continue; }
                        let statuses=inspect(&config).await;
                        if let Some((leader_slot,view))=statuses.iter().find_map(|s|s.leader.as_ref().map(|v|(s.slot,v))) {
                            let nonce=hub.0.read().unwrap().active.as_ref().map(|a|a.db.lock().unwrap().incarnation);
                            let active_here=view.voters.iter().chain(&view.learners).chain(&view.outgoing).any(|id|*id==instance.member && view.identities.get(id).copied().unwrap_or([0;16])==nonce.unwrap_or([255;16]));
                            if !active_here {
                                if config.peers.contains_key(&instance.member) || view.retired.contains_key(&instance.member) {
                                    // The quorum superseded this persisted generation while
                                    // we were offline. Fence it before requesting a new one.
                                    halt.send_replace(true); let _=task.await; break;
                                }
                                if let Some(nonce)=nonce {
                                    if let Err(e)=rpc(&config,&config.peers[&leader_slot],Action::Enroll{member:instance.member,nonce}).await {tracing::debug!(%e,"automatic enrollment retry");}
                                }
                            }
                        }
                    }
                }
                if *shutdown.borrow() {
                    break;
                }
            }
            hub.clear();
            existing = None;
            // A terminated runtime (including disk failure) never silently resets
            // its generation. The next pass asks the live quorum for enrollment.
        } else {
            tokio::select! {_=shutdown.changed()=>break,_=tokio::time::sleep(Duration::from_millis(200))=>{}}
        }
    }
    stop.send_replace(true);
    let _ = listening.await;
    drop(lock);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn config(slot: u64) -> ClusterConfig {
        super::super::tests::config(slot)
    }
    #[test]
    fn formation_requires_every_original_claim_and_rejects_replayed_local_identity() {
        let dir = tempfile::tempdir().unwrap();
        let mut records = Vec::new();
        let mut claims = Vec::new();
        for slot in 1..=3 {
            let path = dir.path().join(format!("{slot}.db"));
            let record =
                record_for(&config(slot), &path, &path.with_extension("node-state")).unwrap();
            claims.push(fresh_claim(&config(slot), &record).unwrap());
            records.push(record);
        }
        assert!(valid_certificate(&config(1), &records[0], &claims));
        assert!(!valid_certificate(&config(1), &records[0], &claims[..2]));
        let mut duplicate = claims.clone();
        duplicate[2] = claims[1].clone();
        assert!(!valid_certificate(&config(1), &records[0], &duplicate));
        records[0].nonce[0] ^= 1;
        assert!(!valid_certificate(&config(1), &records[0], &claims));
        claims[0].signature[0] ^= 1;
        assert!(!valid_certificate(&config(2), &records[1], &claims));
    }
    #[test]
    fn damaged_or_missing_store_with_history_cannot_claim_fresh_genesis() {
        let dir = tempfile::tempdir().unwrap();
        let config = config(1);
        let store = dir.path().join("db");
        let manifest = store.with_extension("node-state");
        let record = record_for(&config, &store, &manifest).unwrap();
        assert!(record.virgin);
        save(
            &config,
            &manifest,
            &Record {
                virgin: false,
                ..record
            },
        )
        .unwrap();
        assert!(!record_for(&config, &store, &manifest).unwrap().virgin);
        std::fs::write(&manifest, b"corrupt identity").unwrap();
        assert!(!record_for(&config, &store, &manifest).unwrap().virgin);
        std::fs::remove_file(&manifest).unwrap();
        std::fs::write(store.with_extension("cluster-lock"), b"history").unwrap();
        assert!(!record_for(&config, &store, &manifest).unwrap().virgin);
        std::fs::remove_file(store.with_extension("cluster-lock")).unwrap();
        std::fs::create_dir(store.with_extension("recovery")).unwrap();
        assert!(!record_for(&config, &store, &manifest).unwrap().virgin);
    }
    #[test]
    fn fresh_generations_keep_stable_placement_and_never_copy_old_voting_state() {
        let dir = tempfile::tempdir().unwrap();
        let config = config(1);
        let store = dir.path().join("db");
        let original = Database::open_mode(&store, &config, true, false).unwrap();
        original
            .conn
            .execute("INSERT INTO cluster_meta VALUES('sentinel',x'01')", [])
            .unwrap();
        let (first, db) = new_instance(&config, &store).unwrap();
        let (second, other) = new_instance(&config, &store).unwrap();
        assert_ne!(first.member, second.member);
        assert_ne!(db.incarnation, other.incarnation);
        assert_ne!(first.path, second.path);
        assert!(!config.peers.contains_key(&first.member));
        assert_eq!(
            config.slot_for(
                first.member,
                &BTreeMap::from([(first.member, db.incarnation)])
            ),
            Some(1)
        );
        assert_eq!(db.hard().unwrap().term, 0);
        assert_eq!(db.hard().unwrap().vote, 0);
        assert!(db.meta("sentinel").unwrap().is_none());
        assert!(db.conf().unwrap().learners.contains(&first.member));
        assert!(!db.conf().unwrap().voters.contains(&first.member));
        assert_eq!(original.meta("sentinel").unwrap().unwrap(), vec![1]);
        let incarnation = db.incarnation;
        drop(db);
        let mut runtime = config.clone();
        runtime.node_id = first.member;
        // A crash before enrollment/snapshot transfer must preserve the same
        // nonvoting identity and a readable, authenticated initial journal.
        let db = Database::open_mode(&first.path, &runtime, false, false).unwrap();
        assert_eq!(db.incarnation, incarnation);
        assert_eq!(db.hard().unwrap().term, 0);
        assert!(db.conf().unwrap().learners.contains(&first.member));
    }
    #[test]
    fn startup_messages_and_manifests_are_bound_to_cluster_secret_and_domain() {
        let config = config(1);
        let bytes = encode(&config, 1, &42u64).unwrap();
        assert_eq!(decode::<u64>(&config, 1, &bytes).unwrap(), 42);
        assert!(decode::<u64>(&config, 2, &bytes).is_err());
        let mut foreign = config.clone();
        foreign.cluster_id.push('x');
        assert!(decode::<u64>(&foreign, 1, &bytes).is_err());
        foreign = config.clone();
        foreign.secret.push('x');
        assert!(decode::<u64>(&foreign, 1, &bytes).is_err());
        let mut signed: Signed = serde_json::from_slice(&bytes).unwrap();
        signed.body[0] ^= 1;
        assert!(decode::<u64>(&config, 1, &serde_json::to_vec(&signed).unwrap()).is_err());
    }
}
