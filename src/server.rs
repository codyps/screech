//! TCP adapter. Routing and enqueue are one linearization point; writers never hold it.
use crate::{
    core::{Core, Message},
    msg::NatsMessage,
    parser::{parse, Limits},
};
use bytes::{Buf, Bytes, BytesMut};
use std::{
    collections::{HashMap, VecDeque},
    io,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{watch, Notify, Semaphore},
    task::JoinSet,
    time::{timeout, Instant},
};

#[derive(Clone)]
pub struct Config {
    pub listen: SocketAddr,
    pub limits: Limits,
    pub max_pending: usize,
    pub max_connections: usize,
    pub max_subscriptions: usize,
    pub write_deadline: Duration,
    pub storage_deadline: Duration,
    pub ping_interval: Duration,
    pub max_pings: usize,
    pub token: Option<String>,
    pub jetstream: Option<crate::jetstream::Handle>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:4222".parse().unwrap(),
            limits: Limits::default(),
            max_pending: 8 * 1024 * 1024,
            max_connections: 4096,
            max_subscriptions: 4096,
            write_deadline: Duration::from_secs(2),
            storage_deadline: Duration::from_secs(5),
            ping_interval: Duration::from_secs(120),
            max_pings: 2,
            token: None,
            jetstream: None,
        }
    }
}
enum FrameData {
    Raw(Bytes),
    Delivery {
        message: Arc<Message>,
        sid: Arc<str>,
        headers: bool,
    },
}
struct Queued {
    size: usize,
    data: FrameData,
}
#[derive(Default)]
struct Outbox {
    frames: VecDeque<Queued>,
    bytes: usize,
    count: usize,
    closed: bool,
}
#[derive(Clone)]
struct Peer {
    out: Arc<Mutex<Outbox>>,
    ready: Arc<Notify>,
    max_pending: usize,
    cancel: watch::Sender<bool>,
    headers: bool,
}
struct WriterGuard {
    task: tokio::task::JoinHandle<()>,
    peer: Peer,
}
impl Drop for WriterGuard {
    fn drop(&mut self) {
        self.peer.close();
        self.task.abort();
    }
}
struct Hub {
    core: Core,
    peers: HashMap<u64, Peer>,
}
impl Peer {
    fn send(&self, bytes: Bytes) -> bool {
        self.enqueue(Queued {
            size: bytes.len(),
            data: FrameData::Raw(bytes),
        })
    }
    fn send_delivery(&self, message: Arc<Message>, sid: Arc<str>) -> bool {
        let size = wire_size(&message, &sid, self.headers);
        self.enqueue(Queued {
            size,
            data: FrameData::Delivery {
                message,
                sid,
                headers: self.headers,
            },
        })
    }
    fn enqueue(&self, frame: Queued) -> bool {
        let mut out = self.out.lock().unwrap();
        if out.closed {
            return false;
        }
        if out.bytes.saturating_add(frame.size) > self.max_pending || out.count >= 4096 {
            drop(out);
            self.cancel.send_replace(true);
            return false;
        }
        let wake = out.frames.is_empty();
        out.bytes += frame.size;
        out.count += 1;
        out.frames.push_back(frame);
        drop(out);
        if wake {
            self.ready.notify_one();
        }
        true
    }
    fn close(&self) {
        self.out.lock().unwrap().closed = true;
        self.ready.notify_one();
    }
}
impl Hub {
    fn publish(&mut self, publisher: u64, msg: &Message) -> usize {
        self.publish_to(publisher, &msg.subject, msg)
    }
    fn publish_to(&mut self, publisher: u64, target: &str, msg: &Message) -> usize {
        self.publish_to_queue(publisher, target, None, msg)
    }
    fn publish_to_queue(
        &mut self,
        publisher: u64,
        target: &str,
        queue: Option<&str>,
        msg: &Message,
    ) -> usize {
        let mut dead = Vec::new();
        let mut shared = None;
        let Hub { core, peers } = self;
        let count = core.route_with(publisher, target, queue, |client, sid| {
            if let Some(peer) = peers.get(&client) {
                let message = shared.get_or_insert_with(|| Arc::new(msg.clone()));
                if !peer.send_delivery(message.clone(), sid.clone()) {
                    dead.push(client);
                }
            }
        });
        for id in dead {
            self.core.disconnect(id);
            self.peers.remove(&id);
        }
        count
    }
}
fn digits(n: usize) -> usize {
    if n == 0 {
        1
    } else {
        n.ilog10() as usize + 1
    }
}
fn wire_size(msg: &Message, sid: &str, headers: bool) -> usize {
    let reply = msg.reply.as_ref().map(|r| r.len() + 1).unwrap_or(0);
    if headers && !msg.headers.is_empty() {
        let total = msg.headers.len() + msg.payload.len();
        12 + msg.subject.len()
            + sid.len()
            + reply
            + digits(msg.headers.len())
            + digits(total)
            + total
    } else {
        10 + msg.subject.len() + sid.len() + reply + digits(msg.payload.len()) + msg.payload.len()
    }
}
fn encode_into(out: &mut Vec<u8>, msg: &Message, sid: &str, headers: bool) {
    let with_headers = headers && !msg.headers.is_empty();
    let mut number = itoa::Buffer::new();
    out.extend_from_slice(if with_headers { b"HMSG " } else { b"MSG " });
    out.extend_from_slice(msg.subject.as_bytes());
    out.push(b' ');
    out.extend_from_slice(sid.as_bytes());
    if let Some(reply) = &msg.reply {
        out.push(b' ');
        out.extend_from_slice(reply.as_bytes());
    }
    out.push(b' ');
    if with_headers {
        out.extend_from_slice(number.format(msg.headers.len()).as_bytes());
        out.push(b' ');
        out.extend_from_slice(
            number
                .format(msg.headers.len() + msg.payload.len())
                .as_bytes(),
        );
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(&msg.headers);
    } else {
        out.extend_from_slice(number.format(msg.payload.len()).as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(&msg.payload);
    out.extend_from_slice(b"\r\n");
}
fn protocol_error(peer: &Peer, msg: &str) {
    peer.send(Bytes::from(format!("-ERR '{msg}'\r\n")));
}
/// Bind before spawning: tests use port 0 without a free-port race.
pub struct Server {
    listener: TcpListener,
    config: Config,
    identity: Arc<str>,
}
impl Server {
    pub async fn bind(config: Config) -> io::Result<Self> {
        if config.max_pending == 0
            || config.max_pending > u32::MAX as usize
            || config.limits.payload == 0
            || config.limits.control_line == 0
            || config.write_deadline.is_zero()
            || config.storage_deadline.is_zero()
            || config.ping_interval.is_zero()
            || config.max_pings == 0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid server limits",
            ));
        }
        let listener = TcpListener::bind(config.listen).await?;
        let mut random = [0u8; 16];
        getrandom::fill(&mut random).map_err(|e| io::Error::other(e.to_string()))?;
        let identity: Arc<str> = format!(
            "SCREECH{}",
            random
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        )
        .into();
        Ok(Self {
            listener,
            config,
            identity,
        })
    }
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }
    pub async fn run(self, mut shutdown: watch::Receiver<bool>) -> io::Result<()> {
        let hub = Arc::new(Mutex::new(Hub {
            core: Core::default(),
            peers: HashMap::new(),
        }));
        let mut output_task = None;
        if let Some(js) = &self.config.jetstream {
            let weak = Arc::downgrade(&hub);
            let mut output = js
                .take_output(Arc::new(move |target, queue| {
                    weak.upgrade().is_some_and(|h| {
                        h.lock().unwrap().core.has_interest_in_queue(target, queue)
                    })
                }))
                .map_err(io::Error::other)?;
            let output_hub = hub.clone();
            let mut stop = shutdown.clone();
            output_task = Some(tokio::spawn(async move {
                loop {
                    tokio::select! {biased;_=stop.changed()=>break,frame=output.recv()=>{let Some(frame)=frame else{break;};output_hub.lock().unwrap().publish_to_queue(0,&frame.target,frame.queue.as_deref(),&frame.message);}}
                }
            }));
        }
        let slots = Arc::new(Semaphore::new(self.config.max_connections));
        let config = Arc::new(self.config);
        let mut tasks = JoinSet::new();
        let mut id = 0u64;
        loop {
            if *shutdown.borrow() {
                break;
            }
            tokio::select! {
                biased;
                _=shutdown.changed()=>{break;}
                Some(result)=tasks.join_next(),if !tasks.is_empty()=>{if let Err(e)=result {tracing::error!(error=%e,"connection task failed");}}
                result=self.listener.accept()=>{
                    let (socket,addr)=result?;
                    let Ok(slot)=slots.clone().try_acquire_owned() else {drop(socket);continue;};
                    id=id.checked_add(1).ok_or_else(||io::Error::other("client id exhausted"))?;
                    let hub=hub.clone();let config=config.clone();let stop=shutdown.clone();let identity=self.identity.clone();
                    tasks.spawn(async move {let _slot=slot;if let Err(e)=connection(id,socket,hub,config,stop,identity).await {tracing::debug!(client=id,peer=%addr,error=%e,"connection closed");}});
                }
            }
            if *shutdown.borrow() {
                break;
            }
        }
        // Signal every writer, then wait for bounded flush/connection shutdown.
        {
            let h = hub.lock().unwrap();
            for p in h.peers.values() {
                p.cancel.send_replace(true);
            }
        }
        while tasks.join_next().await.is_some() {}
        if let Some(task) = output_task {
            task.abort();
            let _ = task.await;
        }
        if let Some(js) = &config.jetstream {
            js.shutdown().await;
        }
        Ok(())
    }
}
struct Registration {
    id: u64,
    hub: Arc<Mutex<Hub>>,
}
impl Drop for Registration {
    fn drop(&mut self) {
        let mut h = self.hub.lock().unwrap();
        h.core.disconnect(self.id);
        h.peers.remove(&self.id);
    }
}
type DurableCompletion = (io::Result<()>, Arc<tokio::sync::OwnedSemaphorePermit>);
fn finish_durable(result: Result<DurableCompletion, tokio::task::JoinError>) -> io::Result<()> {
    result.map_err(io::Error::other)?.0
}
async fn connection(
    id: u64,
    mut socket: TcpStream,
    hub: Arc<Mutex<Hub>>,
    config: Arc<Config>,
    mut shutdown: watch::Receiver<bool>,
    identity: Arc<str>,
) -> io::Result<()> {
    socket.set_nodelay(true)?;
    let addr = socket.local_addr()?;
    let info = serde_json::json!({"server_id":identity.to_string(),"server_name":"screech","version":env!("CARGO_PKG_VERSION"),"go":"rust","host":addr.ip().to_string(),"port":addr.port(),"headers":true,"max_payload":config.limits.payload,"proto":1,"client_id":id,"auth_required":config.token.is_some(),"tls_required":false,"jetstream":config.jetstream.is_some()});
    timeout(
        config.write_deadline,
        socket.write_all(format!("INFO {info}\r\n").as_bytes()),
    )
    .await??;
    let (read, mut write) = socket.into_split();
    let mut read = read;

    let (cancel, mut canceled) = watch::channel(false);
    let peer = Peer {
        out: Arc::new(Mutex::new(Outbox::default())),
        ready: Arc::new(Notify::new()),
        max_pending: config.max_pending,
        cancel,
        headers: false,
    };
    {
        let mut h = hub.lock().unwrap();
        h.core.connect(id, true);
        h.peers.insert(id, peer.clone());
    }
    let registration = Registration {
        id,
        hub: hub.clone(),
    };
    // A separate writer ensures stalled recipients cannot block a publisher's read loop.
    let deadline = config.write_deadline;
    let writer_peer = peer.clone();
    let writer = tokio::spawn(async move {
        let mut data = Vec::with_capacity(64 * 1024);
        let mut batch = Vec::with_capacity(64);
        loop {
            let notified = writer_peer.ready.notified();
            let mut bytes = 0;
            let closed = {
                let mut out = writer_peer.out.lock().unwrap();
                while bytes < 64 * 1024 {
                    let Some(frame) = out.frames.pop_front() else {
                        break;
                    };
                    bytes += frame.size;
                    batch.push(frame);
                }
                out.closed
            };
            if batch.is_empty() {
                if closed {
                    break;
                }
                notified.await;
                continue;
            }
            for frame in &batch {
                let start = data.len();
                match &frame.data {
                    FrameData::Raw(bytes) => data.extend_from_slice(bytes),
                    FrameData::Delivery {
                        message,
                        sid,
                        headers,
                    } => encode_into(&mut data, message, sid, *headers),
                }
                debug_assert_eq!(data.len() - start, frame.size);
            }

            let count = batch.len();
            batch.clear();
            match timeout(deadline, write.write_all(&data)).await {
                Ok(Ok(())) => {
                    let mut out = writer_peer.out.lock().unwrap();
                    out.bytes -= bytes;
                    out.count -= count;
                }
                _ => {
                    writer_peer.close();
                    writer_peer.cancel.send_replace(true);
                    break;
                }
            }
            data.clear();
        }
        let _ = write.shutdown().await;
    });
    let mut writer = WriterGuard {
        task: writer,
        peer: peer.clone(),
    };
    let mut buf = BytesMut::with_capacity(8192);
    let mut read_chunk = vec![0u8; 64 * 1024];
    let mut connected = false;
    let mut verbose = true;
    let mut no_responders = false;
    let mut pings = 0usize;
    let mut next_ping = Instant::now() + config.ping_interval;
    let auth_deadline = Instant::now() + Duration::from_secs(5);
    let mut durable = JoinSet::<DurableCompletion>::new();
    let result=async {
        loop {
            while let Some(result) = durable.try_join_next() { finish_durable(result)?; }
            // Limit work per poll to give readers, writers, and shutdown a turn.
            let mut processed=0;
            while processed<256 {
                let parsed=match parse(&buf,config.limits) {Ok(v)=>v,Err(e)=>{protocol_error(&peer,&e.0);return Ok(());}};
                let Some((cmd,remaining))=parsed else {break;};
                let consumed=buf.len()-remaining.len();
                let mut advanced=false;
                if !connected && config.token.is_some() && !matches!(cmd,NatsMessage::Connect(_)) {protocol_error(&peer,"Authorization Violation");return Ok(());}
                match cmd {
                    NatsMessage::Connect(c)=>{
                        if connected {protocol_error(&peer,"Duplicate CONNECT");return Ok(());}
                        if c.protocol.is_some_and(|v| !(0..=1).contains(&v)) || c.tls_required || (c.no_responders==Some(true) && c.headers!=Some(true)) {protocol_error(&peer,"Unsupported CONNECT options");return Ok(());}
                        if config.token.as_deref().is_some_and(|token| c.auth_token.as_deref()!=Some(token)) {protocol_error(&peer,"Authorization Violation");return Ok(());}
                        connected=true;verbose=c.verbose;no_responders=c.no_responders.unwrap_or(false);
                        let mut h=hub.lock().unwrap();h.core.connect(id,c.echo.unwrap_or(true));if let Some(p)=h.peers.get_mut(&id){p.headers=c.headers.unwrap_or(false);}
                        if verbose{peer.send(Bytes::from_static(b"+OK\r\n"));}
                    }
                    NatsMessage::Ping=>{if !peer.send(Bytes::from_static(b"PONG\r\n")){return Ok(());}}
                    NatsMessage::Pong=>{pings=0;}
                    NatsMessage::Sub(s)=>{
                        let mut h=hub.lock().unwrap();
                        if h.core.subscription_count(id)>=config.max_subscriptions && !h.core.has_subscription(id,&s.sid) {protocol_error(&peer,"Maximum Subscriptions Exceeded");return Ok(());}
                        if let Err(e)=h.core.subscribe(id,s.sid.into_owned(),s.subject.into_owned(),s.queue_group.map(|s|s.into_owned())){protocol_error(&peer,e);return Ok(());}
                        if verbose{peer.send(Bytes::from_static(b"+OK\r\n"));}
                    }
                    NatsMessage::Unsub(s)=>{hub.lock().unwrap().core.unsubscribe(id,&s.sid,s.max_msgs);if verbose{peer.send(Bytes::from_static(b"+OK\r\n"));}}
                    NatsMessage::Pub(p)=>{
                        let subject=String::from_utf8_lossy(&p.subject).into_owned();let reply=p.reply_to.as_deref().map(|v|String::from_utf8_lossy(v).into_owned());let plen=p.payload.len();drop(p);
                        let frame=buf.split_to(consumed).freeze();advanced=true;
                        let msg=Message{subject,reply,headers:Bytes::new(),payload:frame.slice(consumed-plen-2..consumed-2)};publish(&hub,id,&msg,no_responders,config.jetstream.as_ref(),&mut durable,config.storage_deadline)?;
                        if verbose{peer.send(Bytes::from_static(b"+OK\r\n"));}}
                    NatsMessage::HPub(p)=>{
                        let subject=String::from_utf8_lossy(&p.subject).into_owned();let reply=p.reply_to.as_deref().map(|v|String::from_utf8_lossy(v).into_owned());let plen=p.payload.len();let hlen=p.headers.len();drop(p);
                        let frame=buf.split_to(consumed).freeze();advanced=true;
                        let msg=Message{subject,reply,headers:frame.slice(consumed-plen-hlen-2..consumed-plen-2),payload:frame.slice(consumed-plen-2..consumed-2)};publish(&hub,id,&msg,no_responders,config.jetstream.as_ref(),&mut durable,config.storage_deadline)?;
                        if verbose{peer.send(Bytes::from_static(b"+OK\r\n"));}}
                    _=>{protocol_error(&peer,"Unexpected server operation");return Ok(());}
                }
                if !advanced{buf.advance(consumed);}processed+=1;
            }
            if processed==256 {tokio::task::yield_now().await;}
            if *canceled.borrow() || *shutdown.borrow(){return Ok(());}
            if processed==256 {continue;}
            // The parser checks advertised sizes before accepting the payload.
            let max=config.limits.payload.saturating_add(config.limits.control_line).saturating_add(4);
            let allowance=max.saturating_sub(buf.len()).min(64*1024);
            if allowance==0 {protocol_error(&peer,"Maximum buffer exceeded");return Ok(());}

            tokio::select! {
                biased;
                _=canceled.changed()=>return Ok(()),
                _=shutdown.changed()=>return Ok(()),
                Some(result)=durable.join_next(),if !durable.is_empty()=>{finish_durable(result)?;}
                _=tokio::time::sleep_until(auth_deadline),if config.token.is_some() && !connected=>{protocol_error(&peer,"Authorization Timeout");return Ok(());}
                _=tokio::time::sleep_until(next_ping)=>{
                    if pings>=config.max_pings {protocol_error(&peer,"Stale Connection");return Ok(());}
                    if !peer.send(Bytes::from_static(b"PING\r\n")){return Ok(());}
                    pings+=1;next_ping=Instant::now()+config.ping_interval;
                }
                result=read.read(&mut read_chunk[..allowance])=>{let n=result?;if n==0{return Ok(());}buf.extend_from_slice(&read_chunk[..n]);}
            }
        }
    }.await;
    durable.abort_all();
    while durable.join_next().await.is_some() {}
    drop(registration);
    peer.close();
    drop(peer);
    // Flush accepted frames on a normal close; stalled writes have a deadline.
    if timeout(config.write_deadline * 2, &mut writer.task)
        .await
        .is_err()
    {
        writer.task.abort();
        let _ = (&mut writer.task).await;
    }
    result
}
fn publish(
    hub: &Arc<Mutex<Hub>>,
    id: u64,
    msg: &Message,
    no_responders: bool,
    js: Option<&crate::jetstream::Handle>,
    durable: &mut JoinSet<DurableCompletion>,
    deadline: Duration,
) -> io::Result<()> {
    // Core routing is immediate, including publications captured by a stream.
    // Storage admission/completion never holds the routing lock or read loop.
    let recipients = hub.lock().unwrap().publish(id, msg);
    let Some(js) = js.filter(|js| js.captures(&msg.subject)) else {
        if recipients == 0 && no_responders {
            no_response(hub, msg.reply.as_deref());
        }
        return Ok(());
    };
    while let Some(result) = durable.try_join_next() {
        finish_durable(result)?;
    }
    let pending = js.try_request(msg.clone()).map_err(io::Error::other)?;
    let permit = pending.lease();
    let expires = Instant::now() + deadline;
    let hub = hub.clone();
    let reply = if recipients == 0 && no_responders {
        msg.reply.clone()
    } else {
        None
    };
    durable.spawn(async move {
        let result = async {
            let handled = tokio::time::timeout_at(expires, pending.wait())
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Storage deadline exceeded"))?
                .map_err(io::Error::other)?;
            if !handled {
                no_response(&hub, reply.as_deref());
            }
            Ok(())
        }
        .await;
        // Retain the global admission slot until completion is collected too.
        (result, permit)
    });
    Ok(())
}
fn no_response(hub: &Arc<Mutex<Hub>>, reply: Option<&str>) {
    if let Some(reply) = reply {
        hub.lock().unwrap().publish(
            0,
            &Message {
                subject: reply.into(),
                reply: None,
                headers: Bytes::from_static(b"NATS/1.0 503\r\n\r\n"),
                payload: Bytes::new(),
            },
        );
    }
}
