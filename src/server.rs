//! TCP adapter. Routing and enqueue are one linearization point; writers never hold it.
use crate::{
    core::{Core, Message},
    msg::NatsMessage,
    parser::{parse, Limits},
};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::{
    collections::{HashMap, VecDeque},
    io::{self, IoSlice},
    net::SocketAddr,
    ops::Range,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt},
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
// Parser-validated fields retain one immutable wire allocation through forwarding.
struct WireMessage {
    frame: Bytes,
    subject: Range<usize>,
    reply: Option<Range<usize>>,
    body: usize,
    header_len: usize,
}
struct MessageParts<'a> {
    subject: &'a [u8],
    reply: Option<&'a [u8]>,
    headers: &'a [u8],
    payload: &'a [u8],
}
enum OutboundMessage {
    Owned(Message),
    Wire(WireMessage),
}
impl OutboundMessage {
    fn parts(&self) -> MessageParts<'_> {
        match self {
            Self::Owned(m) => MessageParts {
                subject: m.subject.as_bytes(),
                reply: m.reply.as_deref().map(str::as_bytes),
                headers: &m.headers,
                payload: &m.payload,
            },
            Self::Wire(m) => MessageParts {
                subject: &m.frame[m.subject.clone()],
                reply: m.reply.as_ref().map(|r| &m.frame[r.clone()]),
                headers: &m.frame[m.body..m.body + m.header_len],
                payload: &m.frame[m.body + m.header_len..m.frame.len() - 2],
            },
        }
    }
}
impl WireMessage {
    fn to_owned_message(&self) -> Message {
        Message {
            subject: std::str::from_utf8(&self.frame[self.subject.clone()])
                .expect("validated subject")
                .into(),
            reply: self.reply.as_ref().map(|r| {
                std::str::from_utf8(&self.frame[r.clone()])
                    .expect("validated reply")
                    .into()
            }),
            headers: self.frame.slice(self.body..self.body + self.header_len),
            payload: self
                .frame
                .slice(self.body + self.header_len..self.frame.len() - 2),
        }
    }
}
// parse() returns borrowed PUB/HPUB arguments within this receive buffer.
fn field_range(buffer: &[u8], field: &[u8]) -> Range<usize> {
    let start = (field.as_ptr() as usize)
        .checked_sub(buffer.as_ptr() as usize)
        .expect("borrowed parser field");
    let end = start.checked_add(field.len()).expect("parser field length");
    assert!(end <= buffer.len(), "borrowed parser field bounds");
    start..end
}
fn reserve_input(buf: &mut BytesMut, allowance: usize) {
    // Retained frames can share this allocation. Reuse its tail until growing
    // is necessary, rather than reserving an entire read window each time.
    if buf.capacity() - buf.len() < allowance.min(8192) {
        buf.reserve(allowance);
    }
}
enum FrameData {
    Raw(Bytes),
    Delivery {
        message: Arc<OutboundMessage>,
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
// Wake receivers after routing the available input burst. Dropping on any
// early return still wakes every accepted delivery before connection cleanup.
#[derive(Default)]
struct DeliveryWakes(Vec<Arc<Notify>>);
impl Drop for DeliveryWakes {
    fn drop(&mut self) {
        for ready in self.0.drain(..) {
            ready.notify_one();
        }
    }
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
    fn send_delivery(
        &self,
        message: Arc<OutboundMessage>,
        sid: Arc<str>,
        wakes: Option<&mut DeliveryWakes>,
    ) -> bool {
        let size = wire_size(&message, &sid, self.headers);
        self.enqueue_with_wakes(
            Queued {
                size,
                data: FrameData::Delivery {
                    message,
                    sid,
                    headers: self.headers,
                },
            },
            wakes,
        )
    }
    fn enqueue(&self, frame: Queued) -> bool {
        self.enqueue_with_wakes(frame, None)
    }
    fn enqueue_with_wakes(&self, frame: Queued, wakes: Option<&mut DeliveryWakes>) -> bool {
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
            if let Some(wakes) = wakes {
                wakes.0.push(self.ready.clone());
            } else {
                self.ready.notify_one();
            }
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
        let mut shared = None;
        self.route(publisher, target, queue, None, || {
            shared
                .get_or_insert_with(|| Arc::new(OutboundMessage::Owned(msg.clone())))
                .clone()
        })
    }
    fn route(
        &mut self,
        publisher: u64,
        target: &str,
        queue: Option<&str>,
        mut wakes: Option<&mut DeliveryWakes>,
        mut message: impl FnMut() -> Arc<OutboundMessage>,
    ) -> usize {
        let mut dead = Vec::new();
        let Hub { core, peers } = self;
        let count = core.route_with(publisher, target, queue, |client, sid| {
            if let Some(peer) = peers.get(&client) {
                if !peer.send_delivery(message(), sid.clone(), wakes.as_deref_mut()) {
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
fn wire_size(message: &OutboundMessage, sid: &str, headers: bool) -> usize {
    let msg = message.parts();
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
fn encode_header(out: &mut Vec<u8>, msg: &MessageParts<'_>, sid: &str, headers: bool) {
    let with_headers = headers && !msg.headers.is_empty();
    let mut number = itoa::Buffer::new();
    out.extend_from_slice(if with_headers { b"HMSG " } else { b"MSG " });
    out.extend_from_slice(msg.subject);
    out.push(b' ');
    out.extend_from_slice(sid.as_bytes());
    if let Some(reply) = &msg.reply {
        out.push(b' ');
        out.extend_from_slice(reply);
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
    } else {
        out.extend_from_slice(number.format(msg.payload.len()).as_bytes());
        out.extend_from_slice(b"\r\n");
    }
}
fn encode_into(out: &mut Vec<u8>, msg: &MessageParts<'_>, sid: &str, headers: bool) {
    encode_header(out, msg, sid, headers);
    if headers && !msg.headers.is_empty() {
        out.extend_from_slice(msg.headers);
    }
    out.extend_from_slice(msg.payload);
    out.extend_from_slice(b"\r\n");
}

#[derive(Debug)]
enum WritePart {
    Buffered(Range<usize>),
    Headers(usize),
    Payload(usize),
}
impl WritePart {
    fn bytes<'a>(&self, data: &'a [u8], frames: &'a [Queued]) -> &'a [u8] {
        match self {
            Self::Buffered(range) => &data[range.clone()],
            Self::Headers(index) | Self::Payload(index) => {
                let FrameData::Delivery { message, .. } = &frames[*index].data else {
                    unreachable!("borrowed delivery")
                };
                let msg = message.parts();
                if matches!(self, Self::Headers(_)) {
                    msg.headers
                } else {
                    msg.payload
                }
            }
        }
    }
}
// Keep small messages coalesced. Large bodies use scatter/gather while their
// original frames remain alive; the buffer owns only their headers/trailers.
fn prepare_batch(data: &mut Vec<u8>, parts: &mut Vec<WritePart>, frames: &mut [Queued]) {
    data.clear();
    parts.clear();
    let mut buffered = 0;
    for (index, frame) in frames.iter_mut().enumerate() {
        let mut borrowed = false;
        match &frame.data {
            FrameData::Raw(bytes) => data.extend_from_slice(bytes),
            FrameData::Delivery {
                message,
                sid,
                headers,
            } => {
                let msg = message.parts();
                let with_headers = *headers && !msg.headers.is_empty();
                if msg.payload.len().saturating_add(if with_headers {
                    msg.headers.len()
                } else {
                    0
                }) >= 1024
                {
                    encode_header(data, &msg, sid, *headers);
                    if data.len() > buffered {
                        parts.push(WritePart::Buffered(buffered..data.len()));
                    }
                    if with_headers {
                        parts.push(WritePart::Headers(index));
                    }
                    if !msg.payload.is_empty() {
                        parts.push(WritePart::Payload(index));
                    }
                    buffered = data.len();
                    data.extend_from_slice(b"\r\n");
                    borrowed = true;
                } else {
                    encode_into(data, &msg, sid, *headers);
                }
            }
        }
        // Copied frames need not pin receive allocations during a stalled write.
        if !borrowed {
            frame.data = FrameData::Raw(Bytes::new());
        }
    }
    if data.len() > buffered {
        parts.push(WritePart::Buffered(buffered..data.len()));
    }
}
async fn write_batch<W: AsyncWrite + Unpin>(
    writer: &mut W,
    data: &[u8],
    parts: &[WritePart],
    frames: &[Queued],
) -> io::Result<()> {
    if parts.len() <= 1 {
        let mut remaining = data;
        while !remaining.is_empty() {
            match writer.write(remaining).await {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(n) => remaining = &remaining[n..],
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {
                    tokio::task::yield_now().await
                }
                Err(error) => return Err(error),
            }
        }
        return Ok(());
    }
    let mut at = 0;
    let mut offset = 0;
    while at < parts.len() {
        // Bound each vector; partial writes and non-vectored backends are supported.
        let mut slices: [IoSlice<'_>; 128] = std::array::from_fn(|_| IoSlice::new(&[]));
        let mut count = 0;
        for (n, part) in parts[at..].iter().take(slices.len()).enumerate() {
            let bytes = part.bytes(data, frames);
            slices[count] = IoSlice::new(if n == 0 { &bytes[offset..] } else { bytes });
            count += 1;
        }
        let result = if writer.is_write_vectored() {
            writer.write_vectored(&slices[..count]).await
        } else {
            writer.write(&slices[0]).await
        };
        let mut n = match result {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {
                tokio::task::yield_now().await;
                continue;
            }
            other => other?,
        };
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "socket stopped accepting output",
            ));
        }
        while n > 0 {
            let remaining = parts[at].bytes(data, frames).len() - offset;
            if n < remaining {
                offset += n;
                break;
            }
            n -= remaining;
            at += 1;
            offset = 0;
        }
    }
    Ok(())
}
async fn writer_loop<W: AsyncWrite + Unpin>(mut write: W, peer: Peer, deadline: Duration) {
    let mut data = Vec::with_capacity(64 * 1024);
    let mut batch = Vec::with_capacity(64);
    let mut parts = Vec::with_capacity(16);
    loop {
        let notified = peer.ready.notified();
        let mut bytes = 0;
        let closed = {
            let mut out = peer.out.lock().unwrap();
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
        let count = batch.len();
        prepare_batch(&mut data, &mut parts, &mut batch);
        debug_assert_eq!(
            parts
                .iter()
                .map(|p| p.bytes(&data, &batch).len())
                .sum::<usize>(),
            bytes
        );
        let result = timeout(deadline, write_batch(&mut write, &data, &parts, &batch)).await;
        batch.clear();
        match result {
            Ok(Ok(())) => {
                let mut out = peer.out.lock().unwrap();
                out.bytes -= bytes;
                out.count -= count;
            }
            _ => {
                peer.close();
                peer.cancel.send_replace(true);
                break;
            }
        }
        data.clear();
    }
    let _ = write.shutdown().await;
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
    let (read, write) = socket.into_split();
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
    let writer = tokio::spawn(writer_loop(write, writer_peer, deadline));
    let mut writer = WriterGuard {
        task: writer,
        peer: peer.clone(),
    };
    let mut buf = BytesMut::with_capacity(64 * 1024);
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
            let mut wakes=DeliveryWakes::default();
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
                        let subject=field_range(&buf,&p.subject);let reply=p.reply_to.as_deref().map(|v|field_range(&buf,v));let plen=p.payload.len();drop(p);
                        let frame=buf.split_to(consumed).freeze();advanced=true;
                        let msg=WireMessage{frame,subject,reply,body:consumed-plen-2,header_len:0};publish(&hub,id,msg,&mut wakes,no_responders,&config,&mut durable)?;
                        if verbose{peer.send(Bytes::from_static(b"+OK\r\n"));}}
                    NatsMessage::HPub(p)=>{
                        let subject=field_range(&buf,&p.subject);let reply=p.reply_to.as_deref().map(|v|field_range(&buf,v));let plen=p.payload.len();let hlen=p.headers.len();drop(p);
                        let frame=buf.split_to(consumed).freeze();advanced=true;
                        let msg=WireMessage{frame,subject,reply,body:consumed-plen-hlen-2,header_len:hlen};publish(&hub,id,msg,&mut wakes,no_responders,&config,&mut durable)?;
                        if verbose{peer.send(Bytes::from_static(b"+OK\r\n"));}}
                    _=>{protocol_error(&peer,"Unexpected server operation");return Ok(());}
                }
                if !advanced{buf.advance(consumed);}processed+=1;
            }
            drop(wakes);
            if processed==256 {tokio::task::yield_now().await;}
            if *canceled.borrow() || *shutdown.borrow(){return Ok(());}
            if processed==256 {continue;}
            // The parser checks advertised sizes before accepting the payload.
            let max=config.limits.payload.saturating_add(config.limits.control_line).saturating_add(4);
            let allowance=max.saturating_sub(buf.len()).min(64*1024);
            if allowance==0 {protocol_error(&peer,"Maximum buffer exceeded");return Ok(());}

            reserve_input(&mut buf, allowance);
            let mut destination = (&mut buf).limit(allowance);
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
                result=read.read_buf(&mut destination)=>{let n=result?;if n==0{return Ok(());}}
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
    wire: WireMessage,
    wakes: &mut DeliveryWakes,
    no_responders: bool,
    config: &Config,
    durable: &mut JoinSet<DurableCompletion>,
) -> io::Result<()> {
    // Core routing is immediate, including publications captured by a stream.
    // Storage admission/completion never holds the routing lock or read loop.
    let shared = Arc::new(OutboundMessage::Wire(wire));
    let parts = shared.parts();
    let subject = std::str::from_utf8(parts.subject).expect("validated subject");
    let recipients = hub
        .lock()
        .unwrap()
        .route(id, subject, None, Some(wakes), || shared.clone());
    let Some(js) = config.jetstream.as_ref().filter(|js| js.captures(subject)) else {
        if recipients == 0 && no_responders {
            no_response(
                hub,
                parts
                    .reply
                    .map(|r| std::str::from_utf8(r).expect("validated reply")),
            );
        }
        return Ok(());
    };
    let OutboundMessage::Wire(wire) = shared.as_ref() else {
        unreachable!()
    };
    let msg = wire.to_owned_message();
    while let Some(result) = durable.try_join_next() {
        finish_durable(result)?;
    }
    let reply = if recipients == 0 && no_responders {
        msg.reply.clone()
    } else {
        None
    };
    let pending = js.try_request(msg).map_err(io::Error::other)?;
    let permit = pending.lease();
    let expires = Instant::now() + config.storage_deadline;
    let hub = hub.clone();
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

#[cfg(test)]
#[path = "server_output_tests.rs"]
mod output_tests;
