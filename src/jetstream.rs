//! Serialized durable worker with bounded ingress/egress and asynchronous pull completion.
use crate::{
    core::Message,
    durable::{self, AckAction, AckPolicy, ConsumerConfig, Engine, Event, StreamConfig},
    storage::Store,
};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use bytes::Bytes;
use serde_json::{json, Value};
use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{mpsc as std_mpsc, Arc, Mutex, RwLock},
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};
#[path = "jetstream_direct_reads.rs"]
mod direct_reads;
use direct_reads::DirectReads;
const REQUEST_SLOTS: usize = 256;

#[derive(Clone)]
pub struct Outbound {
    pub target: String,
    pub protocol_reply: bool,
    pub queue: Option<String>,
    pub message: Message,
}
type OutputSink = Arc<dyn Fn(Outbound) -> Result<(), String> + Send + Sync>;
type Interest = Arc<dyn Fn(&str, Option<&str>) -> bool + Send + Sync>;
#[derive(Clone)]
pub struct Handle {
    tx: std_mpsc::Sender<Request>,
    slots: Arc<Semaphore>,
    catalog_pending: Arc<std::sync::atomic::AtomicUsize>,
    interests: Arc<RwLock<Vec<String>>>,
    output: Arc<Mutex<Option<mpsc::Receiver<Outbound>>>>,
    sink: Arc<RwLock<Option<OutputSink>>>,
    observer: Arc<RwLock<Option<Interest>>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
    done: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
    direct_output: bool,
    direct_reads: Option<Arc<DirectReads>>,
}
struct Request {
    message: Message,
    response: oneshot::Sender<Result<bool, String>>,
    _permit: Arc<OwnedSemaphorePermit>,
    _catalog: Option<CatalogFence>,
}

// A concurrent burst can arrive in several network-runtime turns. After a
// multi-request round, briefly allow the next burst to catch up before starting
// its durability boundary. Sequential traffic and standalone stores never wait.
fn collect_group<T>(
    requests: &mut Vec<T>,
    ingress: &std_mpsc::Receiver<T>,
    deferred: &mut Option<T>,
    previous: usize,
    wait: Duration,
    size: impl Fn(&T) -> Option<usize>,
) {
    let deadline = std::time::Instant::now() + wait;
    let mut bytes = size(&requests[0]).unwrap();
    while requests.len() < 32 {
        let next = match ingress.try_recv() {
            Ok(request) => request,
            Err(std_mpsc::TryRecvError::Empty) if requests.len() < previous => {
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                if remaining.is_zero() {
                    break;
                }
                match ingress.recv_timeout(remaining) {
                    Ok(request) => request,
                    Err(_) => break,
                }
            }
            Err(_) => break,
        };
        match size(&next) {
            Some(len) if bytes + len <= 1024 * 1024 => {
                bytes += len;
                requests.push(next);
            }
            _ => {
                *deferred = Some(next);
                break;
            }
        }
    }
}

#[cfg(test)]
mod admission_tests {
    use super::*;

    #[test]
    fn concurrent_admission_preserves_catalog_fence_and_following_request() {
        let (tx, rx) = std_mpsc::channel();
        for request in [Some(20), None, Some(30)] {
            tx.send(request).unwrap();
        }
        let mut requests = vec![Some(10)];
        let mut deferred = None;
        collect_group(
            &mut requests,
            &rx,
            &mut deferred,
            16,
            Duration::from_secs(1),
            |r| *r,
        );
        assert_eq!(requests, [Some(10), Some(20)]);
        assert_eq!(deferred, Some(None));
        assert_eq!(rx.try_recv().unwrap(), Some(30));
    }

    #[test]
    fn concurrent_admission_keeps_count_and_byte_bounds() {
        let (tx, rx) = std_mpsc::channel();
        for _ in 0..40 {
            tx.send(1).unwrap();
        }
        let mut requests = vec![1];
        let mut deferred = None;
        collect_group(&mut requests, &rx, &mut deferred, 32, Duration::ZERO, |r| {
            Some(*r)
        });
        assert_eq!(requests.len(), 32);
        assert_eq!(rx.try_iter().count(), 9);
        tx.send(1024 * 1024).unwrap();
        tx.send(2).unwrap();
        requests.clear();
        requests.push(1);
        collect_group(&mut requests, &rx, &mut deferred, 32, Duration::ZERO, |r| {
            Some(*r)
        });
        assert_eq!(requests, [1]);
        assert_eq!(deferred, Some(1024 * 1024));
        assert_eq!(rx.try_recv().unwrap(), 2);
    }

    #[test]
    fn sequential_admission_does_not_wait_for_another_request() {
        let (_tx, rx) = std_mpsc::channel();
        let mut requests = vec![1];
        let mut deferred = None;
        // Use a budget far above scheduler jitter to distinguish an immediate
        // return from incorrectly waiting for the next sequential request.
        let started = std::time::Instant::now();
        collect_group(
            &mut requests,
            &rx,
            &mut deferred,
            1,
            Duration::from_secs(5),
            |r| Some(*r),
        );
        assert_eq!(requests, [1]);
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn burst_admission_accepts_arrivals_then_returns_at_previous_size() {
        let (tx, rx) = std_mpsc::channel();
        let (done, wait) = std_mpsc::channel();
        let producer = std::thread::spawn(move || {
            tx.send(2).unwrap();
            tx.send(3).unwrap();
            // Keep the channel connected until collection has returned.
            wait.recv().unwrap();
            drop(tx);
        });
        let mut requests = vec![1];
        let mut deferred = None;
        collect_group(
            &mut requests,
            &rx,
            &mut deferred,
            3,
            Duration::from_secs(1),
            |r| Some(*r),
        );
        assert_eq!(requests, [1, 2, 3]);
        done.send(()).unwrap();
        producer.join().unwrap();
    }
}
struct CatalogFence(Arc<std::sync::atomic::AtomicUsize>);
impl Drop for CatalogFence {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}
pub struct PendingRequest {
    response: Completion,
    permit: Arc<OwnedSemaphorePermit>,
}
impl PendingRequest {
    pub(crate) fn lease(&self) -> Arc<OwnedSemaphorePermit> {
        self.permit.clone()
    }
    pub async fn wait(self) -> Result<bool, String> {
        let Self { response, permit } = self;
        let result = match response {
            Completion::Worker(receiver) => receiver.await.map_err(|_| "Storage worker stopped")?,
            Completion::Direct(completion) => completion.await,
        };
        drop(permit);
        result
    }
}
enum Completion {
    Worker(oneshot::Receiver<Result<bool, String>>),
    Direct(DetachedCompletion),
}
type CompletionFuture = Pin<Box<dyn Future<Output = Result<bool, String>> + Send>>;
// Drive direct reads in the caller's existing completion task. If it is
// cancelled, finish the admitted proof on the runtime, retaining its lease
// until the proof resolves. Cancellation must not create unbounded Raft reads.
struct DetachedCompletion {
    future: Option<CompletionFuture>,
    runtime: tokio::runtime::Handle,
}
impl Future for DetachedCompletion {
    type Output = Result<bool, String>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = self.future.as_mut().unwrap().as_mut().poll(cx);
        if result.is_ready() {
            self.future = None;
        }
        result
    }
}
impl Drop for DetachedCompletion {
    fn drop(&mut self) {
        if let Some(future) = self.future.take() {
            self.runtime.spawn(future);
        }
    }
}
#[derive(Clone)]
struct Pull {
    stream: String,
    consumer: String,
    stream_generation: u64,
    generation: u64,
    inbox: String,
    remaining: usize,
    bytes: usize,
    max_bytes: usize,
    expires: u64,
    heartbeat: u64,
    next_heartbeat: u64,
    no_wait: bool,
}
enum Effect {
    Message(Outbound),
    #[cfg(feature = "fault-injection")]
    Point(&'static str),
}
struct Worker<S> {
    clock_base: u64,
    clock_start: std::time::Instant,
    engine: Engine<S>,
    plan: Option<durable::GroupPlan>,
    effects: RefCell<Vec<Effect>>,
    staged_count: Cell<usize>,
    staged_bytes: Cell<usize>,
    output: mpsc::Sender<Outbound>,
    sink: Arc<RwLock<Option<OutputSink>>>,
    pulls: VecDeque<Pull>,
    observer: Arc<RwLock<Option<Interest>>>,
    interests: Arc<RwLock<Vec<String>>>,
    quorum_observed: bool,
    checkpoint_checked: Option<u64>,
    direct_reads: Option<Arc<DirectReads>>,
}
impl Handle {
    pub fn start<S: Store + 'static>(store: S) -> Result<Self, durable::Error> {
        Self::start_engine(Engine::recover(store)?)
    }
    /// Experimental fixed one-voter driver; unsafe buffering and volatile stores
    /// are rejected before recovery. Existing start() keeps standalone behavior.
    pub fn start_single_voter<S: Store + 'static>(store: S) -> Result<Self, durable::Error> {
        let store = crate::storage_executor::ExecutorStore::start(store)?;
        Self::start_engine(Engine::recover_single_voter(store)?)
    }
    fn start_engine<S: Store + 'static>(engine: Engine<S>) -> Result<Self, durable::Error> {
        // Cluster persistence has its own Raft owner. Its application worker
        // can use the existing synchronous Core adapter without blocking that
        // owner. Keep the fixed single-voter driver's output isolation.
        let direct_output = engine.voter_progress().is_none() || engine.replicas() > 1;
        let direct_reads = engine.replication().map(|replication| {
            Arc::new(DirectReads::new(
                engine.state(),
                replication,
                engine.failure_flag(),
            ))
        });
        let worker_reads = direct_reads.clone();
        let interests = Arc::new(RwLock::new(
            engine
                .state()
                .streams
                .values()
                .flat_map(|s| s.config.subjects.clone())
                .collect::<Vec<_>>(),
        ));
        let worker_interests = interests.clone();
        let (tx, rx) = std_mpsc::channel::<Request>();
        let (out, output) = mpsc::channel(1024);
        let sink = Arc::new(RwLock::new(None));
        let worker_sink = sink.clone();
        let observer = Arc::new(RwLock::new(None));
        let worker_observer = observer.clone();
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let worker_stop = stop.clone();
        let (done_tx, done_rx) = oneshot::channel();
        let clock_base = now().max(engine.state().time);
        tokio::task::spawn_blocking(move || {
            let clock_start = std::time::Instant::now();
            if let Some(reads) = &worker_reads {
                reads.clock(clock_base, clock_start);
            }
            let mut worker = Worker {
                clock_base,
                clock_start,
                engine,
                plan: None,
                effects: RefCell::new(Vec::with_capacity(64)),
                staged_count: Cell::new(0),
                staged_bytes: Cell::new(0),
                output: out,
                sink: worker_sink,
                pulls: VecDeque::new(),
                observer: worker_observer,
                interests: worker_interests,
                quorum_observed: false,
                checkpoint_checked: None,
                direct_reads: worker_reads,
            };
            let mut deferred = None;
            let mut idle_wait = Duration::from_millis(10);
            let mut poll_after_reply = false;
            // ACK-only rounds return an empty vector for the next admission burst.
            let mut request_scratch = Vec::with_capacity(32);
            let mut previous_group = 1;
            loop {
                if worker_stop.load(std::sync::atomic::Ordering::Acquire) {
                    break;
                }
                let next = deferred.take().map(Ok).unwrap_or_else(|| {
                    if poll_after_reply {
                        // Reply-driven clients often issue their next request at once.
                        // Briefly poll to avoid a sleep/wakeup on that round trip;
                        // idle connections and strictly durable stores still sleep.
                        let until = std::time::Instant::now() + Duration::from_micros(128);
                        loop {
                            match rx.try_recv() {
                                Ok(request) => return Ok(request),
                                Err(std_mpsc::TryRecvError::Disconnected) => {
                                    return Err(std_mpsc::RecvTimeoutError::Disconnected)
                                }
                                Err(std_mpsc::TryRecvError::Empty) => (),
                            }
                            if std::time::Instant::now() >= until {
                                break;
                            }
                            std::hint::spin_loop();
                        }
                    }
                    rx.recv_timeout(idle_wait)
                });
                poll_after_reply = false;
                // A fresh client request must obtain its own observation. Only
                // requests already in flight in this turn can share a commit.
                worker.quorum_observed = false;
                match next {
                    Ok(req) if worker.groupable(&req.message) => {
                        crate::fault::group_gate();
                        let mut requests = std::mem::take(&mut request_scratch);
                        requests.reserve(32);
                        requests.push(req);
                        collect_group(
                            &mut requests,
                            &rx,
                            &mut deferred,
                            previous_group,
                            if worker.engine.replicas() > 1 {
                                Duration::from_micros(64)
                            } else {
                                Duration::ZERO
                            },
                            |req| {
                                worker.groupable(&req.message).then_some(
                                    req.message.payload.len() + req.message.headers.len(),
                                )
                            },
                        );
                        previous_group = requests.len();
                        let active_consumers = !worker.pulls.is_empty() || worker.has_push();
                        if requests.iter().all(|req| simple_ack(&req.message)) && !active_consumers
                        {
                            let (replied, scratch) = worker.ack_requests(requests);
                            request_scratch = scratch;
                            poll_after_reply = replied && worker.engine.buffers_consumer_state();
                        } else if worker.engine.supports_groups()
                            && (requests.len() > 1
                                || active_consumers
                                || (worker.engine.replicas() > 1
                                    && !worker.duplicate_publication(&requests[0].message)))
                        {
                            previous_group = worker.group_requests(requests, &rx, &mut deferred);
                        } else {
                            let req = requests.pop().unwrap();
                            let has_reply = req.message.reply.is_some();
                            let result = worker.handle(req.message);
                            // Keep grouped and active-consumer scheduling unchanged.
                            // Publication success still follows the FULL store commit.
                            poll_after_reply = has_reply
                                && result.as_ref().is_ok_and(|handled| *handled)
                                && worker.engine.buffers_consumer_state();
                            let _ = req.response.send(result);
                        }
                    }
                    Ok(req) => {
                        previous_group = 1;
                        let result = worker.handle(req.message);
                        let _ = req.response.send(result);
                    }
                    Err(std_mpsc::RecvTimeoutError::Disconnected) => break,
                    Err(std_mpsc::RecvTimeoutError::Timeout) => previous_group = 1,
                }
                idle_wait = match worker.maintain() {
                    Ok(true) => Duration::ZERO,
                    Ok(false) => Duration::from_millis(10),
                    Err(error) => {
                        tracing::error!(%error,"durable worker maintenance failed");
                        Duration::from_millis(10)
                    }
                };
            }
            if let Err(error) = worker.engine.flush_consumer_state(true) {
                tracing::error!(%error, "consumer-state flush failed during shutdown");
            }
            drop(worker);
            let _ = done_tx.send(());
        });
        Ok(Self {
            tx,
            slots: Arc::new(Semaphore::new(REQUEST_SLOTS)),
            catalog_pending: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            interests,
            output: Arc::new(Mutex::new(Some(output))),
            sink,
            observer,
            stop,
            done: Arc::new(Mutex::new(Some(done_rx))),
            direct_output,
            direct_reads,
        })
    }
    pub fn take_output(&self, observer: Interest) -> Result<mpsc::Receiver<Outbound>, String> {
        let output = self
            .output
            .lock()
            .unwrap()
            .take()
            .ok_or("Storage worker already attached")?;
        *self.observer.write().unwrap() = Some(observer);
        Ok(output)
    }
    pub(crate) fn attach_output(&self, observer: Interest, sink: OutputSink) -> Result<(), String> {
        // Exclude concurrent sends while draining startup output and installing
        // the synchronous adapter, so no output may overtake an earlier frame.
        let mut destination = self.sink.write().unwrap();
        let mut output = self
            .output
            .lock()
            .unwrap()
            .take()
            .ok_or("Storage worker already attached")?;
        *self.observer.write().unwrap() = Some(observer);
        if !self.direct_output {
            // Keep routing/peer locks off the strict driver. The existing bounded
            // channel preserves order, including startup frames, and backpressure.
            tokio::task::spawn_blocking(move || {
                while let Some(frame) = output.blocking_recv() {
                    if let Err(error) = sink(frame) {
                        tracing::error!(%error, "single-voter output adapter stopped");
                        break;
                    }
                }
            });
            return Ok(());
        }
        while let Ok(frame) = output.try_recv() {
            sink(frame)?;
        }
        *destination = Some(sink);
        Ok(())
    }
    pub async fn shutdown(&self) {
        self.stop.store(true, std::sync::atomic::Ordering::Release);
        let done = self.done.lock().unwrap().take();
        if let Some(done) = done {
            let _ = tokio::time::timeout(Duration::from_secs(2), done).await;
        }
    }
    pub fn captures(&self, subject: impl AsRef<[u8]>) -> bool {
        let subject = subject.as_ref();
        self.catalog_pending
            .load(std::sync::atomic::Ordering::Acquire)
            > 0
            || subject.starts_with(b"$JS.API.")
            || subject.starts_with(b"$JS.ACK.")
            || self
                .interests
                .read()
                .unwrap()
                .iter()
                .any(|f| crate::core::matches(f, subject))
    }
    fn submit(
        &self,
        message: Message,
        permit: OwnedSemaphorePermit,
    ) -> Result<PendingRequest, String> {
        if self.stop.load(std::sync::atomic::Ordering::Acquire) {
            return Err("Storage worker stopped".into());
        }
        // Preserve queued-command and catalog ordering. The immutable model is
        // published only after successful commit and before its outputs escape.
        // Standalone stores have no replication dispatcher and use the worker.
        if self.slots.available_permits() == REQUEST_SLOTS - 1
            && self
                .catalog_pending
                .load(std::sync::atomic::Ordering::Acquire)
                == 0
        {
            if let (Some(reads), Ok(runtime), Some(sink)) = (
                self.direct_reads.as_ref(),
                tokio::runtime::Handle::try_current(),
                self.sink.read().unwrap().clone(),
            ) {
                if let Some((outbound, proof)) = reads.prepare(&message) {
                    let reads = reads.clone();
                    let permit = Arc::new(permit);
                    let work_permit = permit.clone();
                    let stop = self.stop.clone();
                    let future: CompletionFuture = Box::pin(async move {
                        crate::fault::group_gate_async().await;
                        let proof = proof.await.map_err(|e| {
                            reads.fail();
                            e.to_string()
                        });
                        let result = proof.and_then(|()| {
                            if stop.load(std::sync::atomic::Ordering::Acquire) {
                                return Err("Storage worker stopped".into());
                            }
                            if !reads.healthy() {
                                return Err("Storage failed: restart required".into());
                            }
                            crate::fault::point("before_async_duplicate_reply");
                            sink(outbound).map(|()| true)
                        });
                        drop(work_permit);
                        result
                    });
                    crate::fault::group_ingress();
                    return Ok(PendingRequest {
                        response: Completion::Direct(DetachedCompletion {
                            future: Some(future),
                            runtime,
                        }),
                        permit,
                    });
                }
            }
        }
        let catalog = [
            "$JS.API.STREAM.CREATE.",
            "$JS.API.STREAM.UPDATE.",
            "$JS.API.STREAM.DELETE.",
        ]
        .iter()
        .any(|p| message.subject.starts_with(p));
        let fence = if catalog {
            self.catalog_pending
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            Some(CatalogFence(self.catalog_pending.clone()))
        } else {
            None
        };
        let permit = Arc::new(permit);
        let (response, receiver) = oneshot::channel();
        self.tx
            .send(Request {
                message,
                response,
                _permit: permit.clone(),
                _catalog: fence,
            })
            .map_err(|_| "Storage worker stopped")?;
        crate::fault::group_ingress();
        Ok(PendingRequest {
            response: Completion::Worker(receiver),
            permit,
        })
    }
    /// Bounded admission in wire order, without waiting for a storage completion.
    pub fn try_request(&self, message: Message) -> Result<PendingRequest, String> {
        let permit = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| "Durable backlog full")?;
        self.submit(message, permit)
    }
    pub async fn request(&self, message: Message) -> Result<bool, String> {
        if self.stop.load(std::sync::atomic::Ordering::Acquire) {
            return Err("Storage worker stopped".into());
        }
        let permit = self
            .slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| "Storage worker stopped")?;
        self.submit(message, permit)?.wait().await
    }
}
fn simple_ack(message: &Message) -> bool {
    message.subject.starts_with("$JS.ACK.")
        && (message.payload.is_empty() || message.payload.as_ref() == b"+ACK")
}
fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u64::MAX as u128) as u64
}
fn timestamp(t: u64) -> String {
    time::OffsetDateTime::from_unix_timestamp_nanos(t as i128)
        .unwrap()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap()
}
fn response(subject: String, value: Value) -> Message {
    Message {
        subject,
        reply: None,
        headers: Bytes::new(),
        payload: serde_json::to_vec(&value).unwrap().into(),
    }
}
fn err(kind: &str, description: &str) -> Value {
    json!({"type":format!("io.nats.jetstream.api.v1.{kind}_response"),"error":{"code":400,"err_code":10052,"description":description}})
}
fn not_found(kind: &str, consumer: bool) -> Value {
    json!({"type":format!("io.nats.jetstream.api.v1.{kind}_response"),"error":{"code":404,"err_code":if consumer{10014}else{10059},"description":if consumer{"Consumer not found"}else{"Stream not found"}}})
}
fn stream_info(s: &durable::Stream, replicas: usize) -> Value {
    let mut config = serde_json::to_value(&s.config).unwrap();
    config["storage"] = json!("file");
    config["num_replicas"] = json!(replicas);
    config["allow_direct"] = json!(false);
    json!({"type":"io.nats.jetstream.api.v1.stream_info_response","config":config,"created":timestamp(s.created),"state":{"messages":s.messages.len(),"bytes":s.bytes,"first_seq":s.messages.keys().next().copied().unwrap_or(s.last_sequence.saturating_add(1)),"first_ts":timestamp(s.messages.values().next().map(|m|m.time).unwrap_or(0)),"last_seq":s.last_sequence,"last_ts":timestamp(s.messages.values().next_back().map(|m|m.time).unwrap_or(0)),"consumer_count":s.consumers.len()}})
}
fn consumer_info(
    s: &durable::Stream,
    c: &durable::Consumer,
    waiting: usize,
    replicas: usize,
) -> Value {
    let mut config = serde_json::to_value(&c.config).unwrap();
    config["replay_policy"] = json!("instant");
    config["num_replicas"] = json!(replicas);
    json!({"type":"io.nats.jetstream.api.v1.consumer_info_response","stream_name":s.config.name,"name":c.config.name,"created":timestamp(c.created),"config":config,"delivered":{"consumer_seq":c.delivered,"stream_seq":c.last_stream},"ack_floor":{"consumer_seq":c.ack_floor,"stream_seq":c.ack_stream},"num_ack_pending":c.pending.len(),"num_exhausted":c.exhausted.len(),"num_redelivered":c.pending.values().filter(|p|p.attempt>1).count(),"num_waiting":waiting,"num_pending":c.ready_count(s),"push_bound":c.config.deliver_subject.is_some()})
}
fn raw_message(m: &durable::StoredMessage) -> Value {
    let mut v = json!({"type":"io.nats.jetstream.api.v1.stream_msg_get_response","message":{"subject":m.subject,"seq":m.sequence,"data":STANDARD.encode(&m.payload),"time":timestamp(m.time)}});
    if !m.headers.is_empty() {
        v["message"]["hdrs"] = json!(STANDARD.encode(&m.headers));
    }
    v
}
fn default_option(v: &Value) -> bool {
    v.is_null()
        || v == false
        || v == ""
        || v.as_i64() == Some(0)
        || v.as_array().is_some_and(|a| a.is_empty())
        || v.as_object().is_some_and(|o| o.is_empty())
}
fn configuration(v: &Value, name: &str, replicas: usize) -> Result<StreamConfig, String> {
    let mut object = v
        .as_object()
        .ok_or("Expected stream configuration")?
        .clone();
    for (key, v) in &object {
        let allowed = match key.as_str() {
            "name"
            | "subjects"
            | "retention"
            | "discard"
            | "max_msgs"
            | "max_bytes"
            | "max_msgs_per_subject"
            | "max_msg_size"
            | "max_age"
            | "duplicate_window"
            | "max_consumers" => true,
            "storage" => v == "file",
            "num_replicas" => v.as_u64().is_some_and(|n| n <= 1 || n == replicas as u64),
            "compression" => v == "none",
            "consumer_limits" => {
                v.is_null()
                    || v.as_object()
                        .is_some_and(|o| o.values().all(default_option))
            }
            _ => default_option(v),
        };
        if !allowed {
            return Err(format!("Unsupported stream option: {key}"));
        }
    }
    if object.get("name").and_then(Value::as_str) != Some(name) {
        return Err("Stream name does not match API subject".into());
    }
    let age = object.get("max_age").and_then(Value::as_u64).unwrap_or(0);
    if age > 0 && age < 100_000_000 {
        return Err("Max age needs to be at least 100ms".into());
    }
    if object
        .get("duplicate_window")
        .and_then(Value::as_u64)
        .unwrap_or(0)
        == 0
    {
        object.insert(
            "duplicate_window".into(),
            json!(if age > 0 {
                age.min(120_000_000_000)
            } else {
                120_000_000_000u64
            }),
        );
    }
    let window = object
        .get("duplicate_window")
        .and_then(Value::as_u64)
        .ok_or("Invalid duplicate window")?;
    if window < 100_000_000 || (age > 0 && window > age) {
        return Err("Invalid duplicate window for stream age".into());
    }
    for key in [
        "max_msgs",
        "max_bytes",
        "max_msgs_per_subject",
        "max_msg_size",
        "max_consumers",
    ] {
        if object.get(key).and_then(Value::as_i64) == Some(0) {
            object.insert(key.into(), json!(-1));
        }
    }
    serde_json::from_value(Value::Object(object)).map_err(|e| e.to_string())
}
fn consumer_configuration(
    v: &Value,
    path_name: Option<&str>,
    replicas: usize,
) -> Result<ConsumerConfig, String> {
    let mut obj = v
        .as_object()
        .ok_or("Expected consumer configuration")?
        .clone();
    for (key, v) in &obj {
        let allowed = match key.as_str() {
            "name" | "durable_name" | "description" | "deliver_subject" | "deliver_group"
            | "ack_policy" | "ack_wait" | "max_deliver" | "backoff" | "filter_subject"
            | "filter_subjects" | "deliver_policy" | "opt_start_seq" | "opt_start_time"
            | "max_ack_pending" | "max_waiting" | "max_batch" | "max_expires" | "max_bytes" => true,
            "replay_policy" => v == "instant",
            "num_replicas" => v.as_u64().is_some_and(|n| n <= 1 || n == replicas as u64),
            _ => default_option(v),
        };
        if !allowed {
            return Err(format!("Unsupported consumer option: {key}"));
        }
    }
    for key in [
        "name",
        "description",
        "deliver_subject",
        "deliver_group",
        "durable_name",
        "opt_start_time",
    ] {
        if obj.get(key).is_some_and(Value::is_null) {
            obj.remove(key);
        }
    }
    if let Some(name) = path_name {
        for key in ["name", "durable_name"] {
            if obj
                .get(key)
                .and_then(Value::as_str)
                .is_some_and(|n| n != name)
            {
                return Err("Consumer name does not match API subject".into());
            }
        }
        obj.insert("name".into(), json!(name));
    }
    for key in [
        "ack_wait",
        "max_deliver",
        "max_ack_pending",
        "max_waiting",
        "max_batch",
    ] {
        if obj.get(key).and_then(Value::as_i64) == Some(0) {
            obj.remove(key);
        }
    }
    let mut config: ConsumerConfig =
        serde_json::from_value(Value::Object(obj)).map_err(|e| e.to_string())?;
    if config.name.is_empty() {
        if let Some(name) = &config.durable_name {
            config.name = name.clone();
        }
    }
    Ok(config)
}
impl<S: Store> Worker<S> {
    fn state(&self) -> &durable::State {
        self.plan
            .as_ref()
            .map(|p| p.state())
            .unwrap_or_else(|| self.engine.state())
    }
    fn read_message(
        &self,
        message: &durable::StoredMessage,
    ) -> Result<durable::StoredMessage, String> {
        if let Some(result) = self
            .plan
            .as_ref()
            .and_then(|p| p.read_staged_message(message))
        {
            return result.map_err(|e| e.to_string());
        }
        self.engine.read_message(message).map_err(|e| e.to_string())
    }
    fn read_messages(
        &self,
        messages: &[&durable::StoredMessage],
    ) -> Result<Vec<durable::StoredMessage>, String> {
        if self.plan.as_ref().is_some_and(|p| p.has_staged_content()) {
            messages.iter().map(|m| self.read_message(m)).collect()
        } else {
            self.engine
                .read_messages(messages)
                .map_err(|e| e.to_string())
        }
    }
    fn has_push(&self) -> bool {
        self.state().streams.values().any(|s| {
            s.consumers.values().any(|c| {
                c.config.deliver_subject.as_ref().is_some_and(|target| {
                    self.interested(target, c.config.deliver_group.as_deref())
                })
            })
        })
    }
    fn groupable(&self, msg: &Message) -> bool {
        self.engine.supports_groups()
            && (msg.subject.starts_with("$JS.ACK.")
                || msg.subject.starts_with("$JS.API.CONSUMER.MSG.NEXT.")
                || !msg.subject.starts_with("$JS.API."))
            && msg.payload.len().saturating_add(msg.headers.len()) <= 1024 * 1024
            && self
                .state()
                .streams
                .values()
                .all(|s| s.config.name.len() <= 256 && s.consumers.keys().all(|n| n.len() <= 256))
    }
    fn duplicate_publication(&self, message: &Message) -> bool {
        // A lone duplicate needs no speculative model or staged mutation.
        // handle() rechecks the ID/window and obtains a fresh ReadIndex before
        // replying; this selection hint never supplies a quorum observation.
        !message.headers.is_empty()
            && !message.subject.starts_with("$JS.API.")
            && self.state().streams.values().any(|stream| {
                stream
                    .config
                    .subjects
                    .iter()
                    .any(|filter| crate::core::matches(filter, &message.subject))
                    && stream.duplicate(&message.headers, self.time()).is_some()
            })
    }
    fn output_capacity(&self) -> usize {
        if self.plan.is_none() {
            return self.available_output();
        }
        if self.staged_bytes.get() >= 8 * 1024 * 1024 {
            0
        } else {
            self.available_output()
                .saturating_sub(self.staged_count.get())
        }
    }

    fn available_output(&self) -> usize {
        if self.sink.read().unwrap().is_some() {
            1024
        } else {
            self.output.capacity()
        }
    }
    fn dispatch(&self, message: Outbound) -> Result<(), String> {
        if self.engine.voter_progress().is_some() && !self.engine.can_release_output() {
            return Err("Single-voter output is not committed or storage failed".into());
        }
        let destination = self.sink.read().unwrap();
        if let Some(sink) = destination.as_ref() {
            sink(message)
        } else {
            self.output
                .try_send(message)
                .map_err(|_| "Durable output queue full or closed".into())
        }
    }
    fn send(&self, message: Outbound) -> Result<(), String> {
        if self.plan.is_some() {
            if self.output_capacity() == 0 {
                return Err("Durable output queue full or closed".into());
            }
            self.staged_count.set(self.staged_count.get() + 1);
            self.staged_bytes.set(
                self.staged_bytes.get().saturating_add(
                    message
                        .message
                        .payload
                        .len()
                        .saturating_add(message.message.headers.len())
                        .saturating_add(message.message.subject.len()),
                ),
            );
            self.effects.borrow_mut().push(Effect::Message(message));
            Ok(())
        } else {
            self.dispatch(message)
        }
    }
    #[cfg(feature = "fault-injection")]
    fn after_commit_point(&self, name: &'static str) {
        if self.plan.is_some() {
            self.effects.borrow_mut().push(Effect::Point(name));
        } else {
            crate::fault::point(name);
        }
    }
    #[cfg(not(feature = "fault-injection"))]
    #[inline(always)]
    fn after_commit_point(&self, _: &'static str) {}
    fn group_requests(
        &mut self,
        requests: Vec<Request>,
        ingress: &std_mpsc::Receiver<Request>,
        deferred: &mut Option<Request>,
    ) -> usize {
        if self.engine.replicas() == 1 {
            if let Err(error) = self.engine.read_barrier() {
                for request in requests {
                    let _ = request.response.send(Err(error.to_string()));
                }
                return 1;
            }
        }
        let pulls_before = self.engine.voter_progress().map(|_| self.pulls.clone());
        match self.engine.begin_group() {
            Ok(plan) => self.plan = Some(plan),
            Err(error) => {
                for request in requests {
                    let _ = request.response.send(Err(error.to_string()));
                }
                return 1;
            }
        }
        let mut bytes: usize = requests
            .iter()
            .map(|r| r.message.payload.len() + r.message.headers.len())
            .sum();
        let mut count = requests.len();
        let mut requests = requests.into_iter();
        let mut completed = Vec::with_capacity(32);
        loop {
            let request = if let Some(request) = requests.next() {
                request
            } else {
                // Admit arrivals during projection work, without a timer or wait.
                // Never cross a previously encountered fence or the group bounds.
                if deferred.is_some() || count >= 32 {
                    break;
                }
                crate::fault::group_extend_gate();
                let Ok(request) = ingress.try_recv() else {
                    break;
                };
                let size = request.message.payload.len() + request.message.headers.len();
                if !self.groupable(&request.message) || bytes + size > 1024 * 1024 {
                    *deferred = Some(request);
                    break;
                }
                bytes += size;
                count += 1;
                request
            };
            let result = self.handle(request.message);
            completed.push((request.response, result, request._permit));
        }
        // Previously waiting pulls and push consumers join the same durability
        // boundary as the publications/ACKs that made their deliveries eligible.
        if let Err(error) = self.maintain() {
            tracing::warn!(%error, "group maintenance stopped");
        }
        let plan = self.plan.take().unwrap();
        let multiple = plan.len() > 1;
        let mut committed = if plan.is_empty() && self.engine.replicas() > 1 {
            self.barrier().and_then(|()| self.commit_plan(plan))
        } else {
            self.commit_plan(plan)
        };
        if committed.is_err() {
            if let Some(pulls) = pulls_before {
                self.pulls = pulls;
            }
        }
        let mut effects = std::mem::take(self.effects.get_mut());
        self.staged_count.set(0);
        self.staged_bytes.set(0);
        if committed.is_ok() {
            if multiple {
                crate::fault::point("before_group_replies");
            }
            for effect in effects.drain(..) {
                match effect {
                    #[cfg(feature = "fault-injection")]
                    Effect::Point(name) => crate::fault::point(name),
                    Effect::Message(out) => {
                        if self.dispatch(out).is_err() {
                            committed = Err("Durable output queue full or closed".into());
                            break;
                        }
                    }
                }
            }
        }
        effects.clear();
        *self.effects.get_mut() = effects;
        for (response, result, _permit) in completed {
            let _ = response.send(committed.clone().and(result));
        }
        count
    }
    fn time(&self) -> u64 {
        self.clock_base
            .saturating_add(self.clock_start.elapsed().as_nanos().min(u64::MAX as u128) as u64)
            .max(self.state().time)
    }
    fn emit(&self, message: Message) -> Result<(), String> {
        self.emit_to(message.subject.clone(), message)
    }
    fn emit_to(&self, target: String, message: Message) -> Result<(), String> {
        self.send(Outbound {
            target,
            protocol_reply: true,
            queue: None,
            message,
        })
    }
    fn interested(&self, target: &str, queue: Option<&str>) -> bool {
        self.observer
            .read()
            .unwrap()
            .as_ref()
            .is_some_and(|f| f(target, queue))
    }
    fn status(
        &self,
        inbox: &str,
        code: u16,
        description: &str,
        pending: usize,
    ) -> Result<(), String> {
        self.emit(Message {
            subject: inbox.into(),
            reply: None,
            headers: Bytes::from(format!(
                "NATS/1.0 {code} {description}\r\nNats-Pending-Messages: {pending}\r\n\r\n"
            )),
            payload: Bytes::new(),
        })
    }
    fn commit(&mut self, event: Event) -> Result<(), String> {
        if let Some(plan) = self.plan.as_mut() {
            plan.commit(event).map_err(|e| e.to_string())
        } else if self.engine.voter_progress().is_some() {
            let mut plan = self.engine.begin_group().map_err(|e| e.to_string())?;
            plan.commit(event).map_err(|e| e.to_string())?;
            self.commit_plan(plan)
        } else {
            self.engine.commit(event).map_err(|e| e.to_string())?;
            if let Some(reads) = &self.direct_reads {
                reads.publish(self.engine.state());
            }
            self.quorum_observed = true;
            Ok(())
        }
    }
    fn commit_plan(&mut self, plan: durable::GroupPlan) -> Result<(), String> {
        let changed = !plan.is_empty();
        self.engine.commit_group(plan).map_err(|e| e.to_string())?;
        if let Some(reads) = &self.direct_reads {
            reads.publish(self.engine.state());
        }
        self.quorum_observed |= changed;
        Ok(())
    }
    fn barrier(&mut self) -> Result<(), String> {
        self.engine.read_barrier().map_err(|e| e.to_string())?;
        self.quorum_observed = true;
        Ok(())
    }
    fn expire(&mut self) -> Result<(), String> {
        let time = self.time();
        if self.state().expiration_needed(time) {
            let exhausted: Vec<_> = self
                .state()
                .streams
                .values()
                .flat_map(|s| {
                    s.consumers.values().flat_map(move |c| {
                        c.pending
                            .iter()
                            .filter(move |(n, p)| {
                                c.config.max_deliver > 0
                                    && p.attempt >= c.config.max_deliver as u64
                                    && p.deadline <= time
                                    && s.messages.get(*n).is_some_and(|m| {
                                        s.config.max_age == 0
                                            || m.time.saturating_add(s.config.max_age) > time
                                    })
                            })
                            .map(move |(&seq, p)| {
                                (
                                    s.config.name.clone(),
                                    c.config.name.clone(),
                                    c.generation,
                                    seq,
                                    p.attempt,
                                )
                            })
                    })
                })
                .collect();
            self.commit(Event::Expire { time })?;
            for (stream, consumer, generation, sequence, attempts) in exhausted {
                let advisory = response(
                    format!("$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.{stream}.{consumer}"),
                    json!({"type":"io.nats.jetstream.advisory.v1.max_deliver","id":format!("{generation}-{sequence}"),"timestamp":timestamp(time),"stream":stream,"consumer":consumer,"stream_seq":sequence,"deliveries":attempts}),
                );
                if let Err(error) = self.send(Outbound {
                    target: advisory.subject.clone(),
                    protocol_reply: false,
                    queue: None,
                    message: advisory,
                }) {
                    tracing::warn!(%error,"consumer delivery-limit advisory dropped");
                }
            }
        }
        Ok(())
    }
    fn handle(&mut self, msg: Message) -> Result<bool, String> {
        if self.plan.is_none()
            && (self.engine.replicas() == 1 || msg.subject.starts_with("$JS.API."))
        {
            self.barrier()?;
        }
        if self.engine.is_poisoned() {
            return Err("Storage failed: restart required".into());
        }
        self.expire()?;
        if msg.subject.starts_with("$JS.ACK.") {
            return self.ack(msg);
        }
        if let Some(api) = msg.subject.strip_prefix("$JS.API.") {
            let Some(reply) = msg.reply.as_ref() else {
                return Ok(true);
            };
            let parts: Vec<_> = api.split('.').collect();
            let body = if msg.payload.is_empty() {
                Ok(json!({}))
            } else {
                serde_json::from_slice::<Value>(&msg.payload)
            };
            let body = match body {
                Ok(b) => b,
                Err(_) => {
                    self.emit(response(reply.clone(), err("error", "Invalid JSON")))?;
                    return Ok(true);
                }
            };
            if let ["CONSUMER", "MSG", "NEXT", stream, name] = parts.as_slice() {
                self.pull(stream, name, reply, &body)?;
                return Ok(true);
            }
            let result = self.api(&parts, &body);
            let value = match result {
                Ok(v) => v,
                Err(e) if self.engine.is_poisoned() => return Err(e),
                Err(e) => err("error", &e),
            };
            if parts.first() == Some(&"STREAM") {
                *self.interests.write().unwrap() = self
                    .engine
                    .state()
                    .streams
                    .values()
                    .flat_map(|s| s.config.subjects.clone())
                    .collect();
            }
            self.emit(response(reply.clone(), value))?;
            return Ok(true);
        }
        let stream = self
            .state()
            .streams
            .values()
            .find(|s| {
                s.config
                    .subjects
                    .iter()
                    .any(|f| crate::core::matches(f, &msg.subject))
            })
            .map(|s| s.config.name.clone());
        let Some(stream) = stream else {
            return Ok(false);
        };
        if let Some(seq) = self.state().streams[&stream].duplicate(&msg.headers, self.time()) {
            if self.plan.is_none() && self.engine.replicas() > 1 {
                self.barrier()?;
            }
            if let Some(reply) = msg.reply {
                self.emit(response(
                    reply,
                    json!({"stream":stream,"seq":seq,"duplicate":true}),
                ))?;
            }
            return Ok(true);
        }
        let forbidden = std::str::from_utf8(&msg.headers)
            .unwrap_or("")
            .lines()
            .filter_map(|line| line.split_once(':'))
            .any(|(k, _)| {
                k.to_ascii_lowercase().starts_with("nats-")
                    && !matches!(
                        k.to_ascii_lowercase().as_str(),
                        "nats-msg-id"
                            | "nats-expected-stream"
                            | "nats-expected-last-sequence"
                            | "nats-expected-last-subject-sequence"
                            | "nats-expected-last-msg-id"
                    )
            });
        let prepared = if forbidden {
            Err(durable::Error::Invalid(
                "Unsupported NATS publish control header",
            ))
        } else {
            self.state().prepare_publish(
                &stream,
                &msg.subject,
                Bytes::copy_from_slice(&msg.headers),
                Bytes::copy_from_slice(&msg.payload),
                self.time(),
            )
        };
        match prepared {
            Ok(event) => {
                self.commit(event)?;
                self.after_commit_point("before_publish_ack");
                if let Some(reply) = msg.reply {
                    self.emit(response(reply,json!({"stream":stream,"seq":self.state().streams[&stream].last_sequence,"duplicate":false})))?;
                }
            }
            Err(e) => {
                if self.plan.is_none() && self.engine.replicas() > 1 {
                    self.barrier()?;
                }
                if let Some(reply) = msg.reply {
                    self.emit(response(reply, err("pub_ack", &e.to_string())))?;
                }
            }
        }
        Ok(true)
    }
    fn api(&mut self, parts: &[&str], body: &Value) -> Result<Value, String> {
        let time = self.time();
        match parts {
            ["SCREECH", "CLUSTER", "STATUS"] => {
                self.engine.cluster_status().map_err(|e| e.to_string())
            }
            ["SCREECH", "CLUSTER", "REPLACE"] => {
                let old = body["old"].as_u64().ok_or("old voter required")?;
                let new = body["new"].as_u64().ok_or("new voter required")?;
                let hex = body["nonce"]
                    .as_str()
                    .ok_or("new member incarnation nonce required")?;
                if hex.len() != 32 || !hex.is_ascii() {
                    return Err("invalid incarnation nonce".into());
                }
                let mut nonce = [0; 16];
                for (i, b) in nonce.iter_mut().enumerate() {
                    *b = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
                        .map_err(|_| "invalid incarnation nonce")?;
                }
                self.engine
                    .replace_voter(old, new, nonce)
                    .map_err(|e| e.to_string())?;
                Ok(json!({"accepted":true,"note":"poll cluster status for committed replacement"}))
            }
            ["SCREECH", "CLUSTER", "COMPACT"] if self.engine.replicas() > 1 => {
                self.engine.checkpoint().map_err(|e| e.to_string())?;
                Ok(json!({"success":true}))
            }
            #[cfg(feature = "fault-injection")]
            ["SCREECH", "GROUPS"] => Ok(json!({"committed_groups":self.engine.committed_groups()})),
            #[cfg(feature = "fault-injection")]
            ["SCREECH", "CHECKPOINT"] => {
                self.engine.checkpoint().map_err(|e| e.to_string())?;
                Ok(json!({"success":true}))
            }
            ["INFO"] => Ok(
                json!({"type":"io.nats.jetstream.api.v1.account_info_response","memory":0,"storage":self.state().streams.values().map(|s|s.bytes).sum::<u64>(),"streams":self.state().streams.len(),"consumers":self.state().streams.values().map(|s|s.consumers.len()).sum::<usize>(),"limits":{"max_memory":-1,"max_storage":-1,"max_streams":-1,"max_consumers":-1},"api":{"total":0,"errors":0}}),
            ),
            ["STREAM", "CREATE", name] => {
                let config = configuration(body, name, self.engine.replicas())?;
                if let Some(s) = self.state().streams.get(*name) {
                    if s.config != config {
                        return Err("Stream already exists with a different configuration".into());
                    }
                } else {
                    let event = self
                        .state()
                        .prepare_create(config, time)
                        .map_err(|e| e.to_string())?;
                    self.commit(event)?;
                }
                let mut v = stream_info(&self.state().streams[*name], self.engine.replicas());
                v["type"] = json!("io.nats.jetstream.api.v1.stream_create_response");
                Ok(v)
            }
            ["STREAM", "UPDATE", name] => {
                let config = configuration(body, name, self.engine.replicas())?;
                let event = self
                    .state()
                    .prepare_update(config, time)
                    .map_err(|e| e.to_string())?;
                self.commit(event)?;
                let mut v = stream_info(&self.state().streams[*name], self.engine.replicas());
                v["type"] = json!("io.nats.jetstream.api.v1.stream_update_response");
                Ok(v)
            }
            ["STREAM", "INFO", name] => Ok(self
                .state()
                .streams
                .get(*name)
                .map(|s| stream_info(s, self.engine.replicas()))
                .unwrap_or_else(|| not_found("stream_info", false))),
            ["STREAM", "NAMES"] | ["STREAM", "LIST"] => {
                let offset = body.get("offset").and_then(Value::as_u64).unwrap_or(0) as usize;
                let filter = body.get("subject").and_then(Value::as_str);
                let values: Vec<_> = self
                    .state()
                    .streams
                    .values()
                    .filter(|s| {
                        filter.is_none_or(|f| {
                            s.config.subjects.iter().any(|s| durable::overlap(s, f))
                        })
                    })
                    .collect();
                let total = values.len();
                let names = parts[1] == "NAMES";
                let values: Vec<_> = values
                    .into_iter()
                    .skip(offset)
                    .take(256)
                    .map(|s| {
                        if names {
                            json!(s.config.name)
                        } else {
                            stream_info(s, self.engine.replicas())
                        }
                    })
                    .collect();
                Ok(
                    json!({"type":format!("io.nats.jetstream.api.v1.stream_{}_response",if names{"names"}else{"list"}),"total":total,"offset":offset,"limit":256,"streams":values}),
                )
            }
            ["STREAM", "DELETE", name] => {
                self.commit(
                    self.state()
                        .prepare_delete(name, false)
                        .map_err(|e| e.to_string())?,
                )?;
                Ok(json!({"type":"io.nats.jetstream.api.v1.stream_delete_response","success":true}))
            }
            ["STREAM", "PURGE", name] => {
                let obj = body.as_object().ok_or("Expected purge request")?;
                if obj
                    .keys()
                    .any(|k| !matches!(k.as_str(), "filter" | "seq" | "keep"))
                {
                    return Err("Unsupported purge option".into());
                }
                let filter = body.get("filter").and_then(Value::as_str).unwrap_or("");
                let seq = body.get("seq").and_then(Value::as_u64).unwrap_or(0);
                let keep = body.get("keep").and_then(Value::as_u64).unwrap_or(0);
                if (!filter.is_empty() && !crate::parser::subject(filter, true))
                    || (seq > 0 && keep > 0)
                {
                    return Err("Invalid purge filter/sequence/keep".into());
                }
                let s = self.state().streams.get(*name).ok_or("Stream not found")?;
                let mut ids: Vec<_> = s
                    .messages
                    .iter()
                    .filter(|(n, m)| {
                        (filter.is_empty() || crate::core::matches(filter, &m.subject))
                            && (seq == 0 || **n < seq)
                    })
                    .map(|(&n, _)| n)
                    .collect();
                ids.truncate(ids.len().saturating_sub(keep as usize));
                let count = ids.len();
                self.commit(Event::Remove {
                    stream: name.to_string(),
                    sequences: ids,
                })?;
                Ok(
                    json!({"type":"io.nats.jetstream.api.v1.stream_purge_response","success":true,"purged":count}),
                )
            }
            ["STREAM", "MSG", "DELETE", name] => {
                let seq = body
                    .get("seq")
                    .and_then(Value::as_u64)
                    .ok_or("Sequence required")?;
                let s = self.state().streams.get(*name).ok_or("Stream not found")?;
                if !s.messages.contains_key(&seq) {
                    return Ok(
                        json!({"type":"io.nats.jetstream.api.v1.stream_msg_delete_response","error":{"code":404,"err_code":10037,"description":"Message not found"}}),
                    );
                }
                self.commit(Event::Remove {
                    stream: name.to_string(),
                    sequences: vec![seq],
                })?;
                Ok(
                    json!({"type":"io.nats.jetstream.api.v1.stream_msg_delete_response","success":true}),
                )
            }
            ["STREAM", "MSG", "GET", name] => {
                let s = self.state().streams.get(*name).ok_or("Stream not found")?;
                let m = if let Some(subject) = body.get("last_by_subj").and_then(Value::as_str) {
                    s.messages
                        .values()
                        .rev()
                        .find(|m| crate::core::matches(subject, &m.subject))
                } else if let Some(subject) = body.get("next_by_subj").and_then(Value::as_str) {
                    let seq = body.get("seq").and_then(Value::as_u64).unwrap_or(1);
                    s.messages
                        .range(seq..)
                        .map(|(_, m)| m)
                        .find(|m| crate::core::matches(subject, &m.subject))
                } else {
                    body.get("seq")
                        .and_then(Value::as_u64)
                        .and_then(|n| s.messages.get(&n))
                };
                let m = m.map(|m| self.read_message(m)).transpose()?;
                Ok(m.as_ref().map(raw_message).unwrap_or_else(||json!({"type":"io.nats.jetstream.api.v1.stream_msg_get_response","error":{"code":404,"err_code":10037,"description":"Message not found"}})))
            }
            ["CONSUMER", "CREATE", stream]
            | ["CONSUMER", "CREATE", stream, _]
            | ["CONSUMER", "CREATE", stream, _, _]
            | ["CONSUMER", "DURABLE", "CREATE", stream, _] => {
                if body
                    .get("stream_name")
                    .and_then(Value::as_str)
                    .is_some_and(|s| s != *stream)
                {
                    return Err("Stream name mismatch".into());
                }
                let name = if parts[1] == "DURABLE" {
                    parts.get(4).copied()
                } else {
                    parts.get(3).copied()
                };
                let config = consumer_configuration(
                    body.get("config").ok_or("Consumer config required")?,
                    name,
                    self.engine.replicas(),
                )?;
                let action = body.get("action").and_then(Value::as_str).unwrap_or("");
                if !matches!(action, "create" | "update" | "") {
                    return Err("Unknown consumer action".into());
                }
                let s = self
                    .state()
                    .streams
                    .get(*stream)
                    .ok_or("Stream not found")?;
                let name = if config.name.is_empty() {
                    format!("C{}", self.state().generation + 1)
                } else {
                    config.name.clone()
                };
                if let Some(c) = s.consumers.get(&name) {
                    let mut wanted = config.clone();
                    wanted.name = name.clone();
                    if wanted != c.config {
                        if action == "create" {
                            return Err(
                                "Consumer already exists with different configuration".into()
                            );
                        }
                        let event = self
                            .state()
                            .prepare_consumer_update(stream, wanted, time)
                            .map_err(|e| e.to_string())?;
                        self.commit(event)?;
                    }
                } else {
                    if action == "update" {
                        return Err("Consumer not found".into());
                    }
                    let event = self
                        .state()
                        .prepare_consumer(stream, config, time)
                        .map_err(|e| e.to_string())?;
                    self.commit(event)?;
                }
                let s = &self.state().streams[*stream];
                let mut v = consumer_info(
                    s,
                    &s.consumers[&name],
                    self.waiting(stream, &name),
                    self.engine.replicas(),
                );
                v["type"] = json!("io.nats.jetstream.api.v1.consumer_create_response");
                Ok(v)
            }
            ["CONSUMER", "INFO", stream, name] => Ok(self
                .state()
                .streams
                .get(*stream)
                .and_then(|s| {
                    s.consumers.get(*name).map(|c| {
                        consumer_info(s, c, self.waiting(stream, name), self.engine.replicas())
                    })
                })
                .unwrap_or_else(|| not_found("consumer_info", true))),
            ["CONSUMER", "DELETE", stream, name] => {
                let c = self
                    .state()
                    .streams
                    .get(*stream)
                    .and_then(|s| s.consumers.get(*name))
                    .ok_or("Consumer not found")?;
                self.commit(Event::ConsumerDelete {
                    stream: stream.to_string(),
                    name: name.to_string(),
                    generation: c.generation,
                })?;
                Ok(
                    json!({"type":"io.nats.jetstream.api.v1.consumer_delete_response","success":true}),
                )
            }
            ["CONSUMER", "NAMES", stream] | ["CONSUMER", "LIST", stream] => {
                let s = self
                    .state()
                    .streams
                    .get(*stream)
                    .ok_or("Stream not found")?;
                let offset = body.get("offset").and_then(Value::as_u64).unwrap_or(0) as usize;
                let names = parts[1] == "NAMES";
                let cs: Vec<_> = s
                    .consumers
                    .values()
                    .skip(offset)
                    .take(256)
                    .map(|c| {
                        if names {
                            json!(c.config.name)
                        } else {
                            consumer_info(
                                s,
                                c,
                                self.waiting(stream, &c.config.name),
                                self.engine.replicas(),
                            )
                        }
                    })
                    .collect();
                Ok(
                    json!({"type":format!("io.nats.jetstream.api.v1.consumer_{}_response",if names{"names"}else{"list"}),"total":s.consumers.len(),"offset":offset,"limit":256,"consumers":cs}),
                )
            }
            _ => Err("Unsupported JetStream operation".into()),
        }
    }
    fn waiting(&self, stream: &str, name: &str) -> usize {
        self.pulls
            .iter()
            .filter(|p| p.stream == stream && p.consumer == name)
            .count()
    }
    fn prepare_pull(
        &self,
        stream: &str,
        name: &str,
        inbox: &str,
        body: &Value,
    ) -> Result<Pull, (u16, &'static str, usize)> {
        let Some(s) = self.state().streams.get(stream) else {
            return Err((404, "Stream Not Found", 0));
        };
        let Some(c) = s.consumers.get(name) else {
            return Err((404, "Consumer Not Found", 0));
        };
        if c.config.deliver_subject.is_some() {
            return Err((409, "Consumer Is Push Based", 0));
        }
        if !body.is_object() && !body.is_u64() {
            return Err((400, "Invalid Pull Request", 0));
        }
        for key in ["batch", "expires", "max_bytes", "idle_heartbeat"] {
            if body.get(key).is_some_and(|v| !v.is_u64()) {
                return Err((400, "Invalid Pull Request", 0));
            }
        }
        let batch = body
            .as_u64()
            .or_else(|| body.get("batch").and_then(Value::as_u64))
            .unwrap_or(1);
        let expires = body
            .get("expires")
            .and_then(Value::as_u64)
            .unwrap_or(30_000_000_000);
        let max_bytes = body.get("max_bytes").and_then(Value::as_u64).unwrap_or(0);
        let heartbeat = body
            .get("idle_heartbeat")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        if batch == 0
            || batch > c.config.max_batch as u64
            || batch > 10000
            || expires > 3_600_000_000_000
            || (c.config.max_expires > 0 && expires > c.config.max_expires)
            || (c.config.max_bytes > 0 && max_bytes > c.config.max_bytes as u64)
            || heartbeat > 0 && (expires == 0 || heartbeat > expires / 2)
        {
            return Err((409, "Invalid Pull Limits", batch as usize));
        }
        if self.waiting(stream, name) >= c.config.max_waiting || self.pulls.len() >= 4096 {
            return Err((409, "Exceeded MaxWaiting", batch as usize));
        }
        let time = self.time();
        Ok(Pull {
            stream: stream.into(),
            consumer: name.into(),
            stream_generation: s.generation,
            generation: c.generation,
            inbox: inbox.into(),
            remaining: batch as usize,
            bytes: 0,
            max_bytes: max_bytes as usize,
            expires: time.saturating_add(expires),
            heartbeat,
            next_heartbeat: time.saturating_add(heartbeat),
            no_wait: body.get("no_wait") == Some(&Value::Bool(true)),
        })
    }
    fn pull(&mut self, stream: &str, name: &str, inbox: &str, body: &Value) -> Result<(), String> {
        let mut pull = match self.prepare_pull(stream, name, inbox, body) {
            Ok(pull) => pull,
            Err((code, description, pending)) => {
                return self.status(inbox, code, description, pending)
            }
        };
        self.fill(&mut pull)?;
        if pull.remaining > 0 {
            if pull.no_wait
                && self
                    .state()
                    .prepare_delivery(stream, name, self.time())
                    .map_err(|e| e.to_string())?
                    .is_none()
            {
                self.no_messages(&pull)?;
            } else {
                self.pulls.push_back(pull);
            }
        }
        Ok(())
    }
    fn no_messages(&self, p: &Pull) -> Result<(), String> {
        let c = &self.state().streams[&p.stream].consumers[&p.consumer];
        if c.config.max_ack_pending > 0 && c.pending.len() as i64 >= c.config.max_ack_pending {
            self.status(&p.inbox, 409, "Exceeded MaxAckPending", p.remaining)
        } else {
            self.status(&p.inbox, 404, "No Messages", p.remaining)
        }
    }
    fn fill(&mut self, p: &mut Pull) -> Result<(), String> {
        let limit = p.remaining.min(64).min(self.output_capacity());
        if limit == 0 {
            return Ok(());
        }
        if limit == 1 {
            let Some(event) = self
                .state()
                .prepare_delivery(&p.stream, &p.consumer, self.time())
                .map_err(|e| e.to_string())?
            else {
                return Ok(());
            };
            let Event::Deliver { delivery, .. } = &event else {
                unreachable!()
            };
            let stream = &self.state().streams[&p.stream];
            let message = self.read_message(&stream.messages[&delivery.sequence])?;
            let size = message.size() as usize;
            if p.max_bytes > 0 && size > p.max_bytes.saturating_sub(p.bytes) {
                self.status(&p.inbox, 409, "Batch Completed", p.remaining)?;
                p.remaining = 0;
                return Ok(());
            }
            let ready = stream.consumers[&p.consumer]
                .ready_count(stream)
                .saturating_sub(usize::from(delivery.attempt == 1));
            let delivery = delivery.clone();
            self.commit(event)?;
            self.after_commit_point("before_delivery_send");
            self.deliver(&p.stream, &p.consumer, &p.inbox, message, &delivery, ready)?;
            p.remaining -= 1;
            p.bytes += size;
            return Ok(());
        }
        let available = p.max_bytes.saturating_sub(p.bytes);
        let event = if p.max_bytes > 0 && available == 0 {
            None
        } else {
            self.state()
                .prepare_delivery_batch(
                    &p.stream,
                    &p.consumer,
                    self.time(),
                    limit,
                    if p.max_bytes == 0 {
                        8 * 1024 * 1024
                    } else {
                        available.min(8 * 1024 * 1024)
                    },
                )
                .map_err(|e| e.to_string())?
        };
        let Some(event) = event else {
            if p.max_bytes > 0
                && self
                    .state()
                    .prepare_delivery(&p.stream, &p.consumer, self.time())
                    .map_err(|e| e.to_string())?
                    .is_some()
            {
                self.status(&p.inbox, 409, "Batch Completed", p.remaining)?;
                p.remaining = 0;
            }
            return Ok(());
        };
        let Event::DeliverBatch { deliveries, .. } = &event else {
            unreachable!()
        };
        let stream = &self.state().streams[&p.stream];
        let mut ready = stream.consumers[&p.consumer].ready_count(stream);
        let selected: Vec<_> = deliveries
            .iter()
            .map(|d| &stream.messages[&d.sequence])
            .collect();
        let bodies = self.read_messages(&selected)?;
        let messages: Vec<_> = bodies
            .into_iter()
            .zip(deliveries)
            .map(|(message, d)| {
                if d.attempt == 1 {
                    ready = ready.saturating_sub(1);
                }
                (message, d.clone(), ready)
            })
            .collect();
        self.commit(event)?;
        for (message, delivery, ready) in messages {
            let size = message.size() as usize;
            self.after_commit_point("before_delivery_send");
            self.deliver(&p.stream, &p.consumer, &p.inbox, message, &delivery, ready)?;
            p.remaining -= 1;
            p.bytes += size;
        }
        Ok(())
    }
    fn deliver(
        &self,
        stream: &str,
        name: &str,
        target: &str,
        m: durable::StoredMessage,
        d: &durable::Delivery,
        ready: usize,
    ) -> Result<(), String> {
        let s = &self.state().streams[stream];
        let c = &s.consumers[name];
        let reply = if c.config.ack_policy == AckPolicy::None {
            None
        } else {
            Some(format!(
                "$JS.ACK._.{}.{}.{}.{}.{}.{}.{}.{}.{}",
                s.generation,
                stream,
                name,
                d.attempt,
                d.sequence,
                d.consumer_sequence,
                m.time,
                ready,
                c.generation
            ))
        };
        self.send(Outbound {
            target: target.into(),
            protocol_reply: false,
            queue: c.config.deliver_group.clone(),
            message: Message {
                subject: m.subject,
                reply,
                headers: m.headers,
                payload: m.payload,
            },
        })
    }

    fn prepare_ack_message(&self, msg: &Message) -> Option<(Event, Option<Value>)> {
        let mut parts = msg.subject.split('.');
        let t: [&str; 12] = std::array::from_fn(|_| parts.next().unwrap_or(""));
        if parts.next().is_some() {
            return None;
        }
        let (Ok(sgen), Ok(generation), Ok(sequence), Ok(cseq)) = (
            t[3].parse::<u64>(),
            t[11].parse::<u64>(),
            t[7].parse::<u64>(),
            t[8].parse::<u64>(),
        ) else {
            return None;
        };
        let stream = t[4];
        let name = t[5];
        if self
            .state()
            .streams
            .get(stream)
            .is_none_or(|s| s.generation != sgen)
        {
            return None;
        }
        let text = std::str::from_utf8(&msg.payload).unwrap_or("").trim();
        let next = if let Some(body) = text.strip_prefix("+NXT") {
            let body = body.trim();
            match if body.is_empty() {
                Ok(json!({"batch":1}))
            } else {
                serde_json::from_str::<Value>(body)
            } {
                Ok(body) => Some(body),
                Err(_) => return None,
            }
        } else {
            None
        };
        let action = if text.is_empty() || text == "+ACK" || next.is_some() {
            AckAction::Ack
        } else if text == "+WPI" {
            AckAction::Progress
        } else if text == "+TERM" || text.starts_with("+TERM ") {
            AckAction::Term
        } else if text == "-NAK" {
            AckAction::Nak(0)
        } else {
            let json = text.strip_prefix("-NAK ")?;
            let delay = serde_json::from_str::<Value>(json)
                .ok()
                .and_then(|v| v.get("delay").and_then(Value::as_u64))?;
            AckAction::Nak(delay)
        };
        let event = self
            .state()
            .prepare_ack(
                stream,
                name,
                crate::durable::AckToken {
                    generation,
                    sequence,
                    consumer_sequence: cseq,
                },
                action,
                self.time(),
            )
            .ok()?;
        Some((event, next))
    }
    fn ack(&mut self, msg: Message) -> Result<bool, String> {
        let Some((event, next)) = self.prepare_ack_message(&msg) else {
            return Ok(true);
        };
        let t: Vec<_> = msg.subject.split('.').collect();
        let stream = t[4];
        let name = t[5];
        if let (Some(reply), Some(body)) = (&msg.reply, &next) {
            if self.output_capacity() > 0 {
                if let Ok(pull) = self.prepare_pull(stream, name, reply, body) {
                    if pull.remaining == 1 {
                        if let Some(combined) = self
                            .state()
                            .prepare_ack_next(event.clone(), self.time())
                            .map_err(|e| e.to_string())?
                        {
                            let Event::AckNext { next, .. } = &combined else {
                                unreachable!()
                            };
                            let Event::Deliver { delivery, .. } = next.as_ref() else {
                                unreachable!()
                            };
                            let state = &self.state().streams[stream];
                            let message = self.read_message(&state.messages[&delivery.sequence])?;
                            if pull.max_bytes == 0 || message.size() as usize <= pull.max_bytes {
                                let delivery = delivery.clone();
                                self.commit(combined)?;
                                self.after_commit_point("before_ack_reply");
                                self.after_commit_point("before_delivery_send");
                                let state = &self.state().streams[stream];
                                let ready = state.consumers[name].ready_count(state);
                                self.deliver(stream, name, reply, message, &delivery, ready)?;
                                return Ok(true);
                            }
                        }
                    }
                }
            }
        }
        self.commit(event)?;
        self.after_commit_point("before_ack_reply");
        if let Some(reply) = msg.reply {
            if let Some(body) = next {
                self.pull(stream, name, &reply, &body)?;
                return Ok(true);
            }
            self.emit(Message {
                subject: reply,
                reply: None,
                headers: Bytes::new(),
                payload: Bytes::new(),
            })?;
        }
        Ok(true)
    }
    fn finish_ack_request(&self, request: Request, committed: Result<(), String>) -> bool {
        let has_reply = request.message.reply.is_some();
        let result = committed.and_then(|()| {
            self.after_commit_point("before_ack_reply");
            if let Some(reply) = request.message.reply {
                self.emit(Message {
                    subject: reply,
                    reply: None,
                    headers: Bytes::new(),
                    payload: Bytes::new(),
                })?;
            }
            Ok(true)
        });
        let replied = has_reply && result.is_ok();
        let _ = request.response.send(result);
        replied
    }
    fn ack_requests(&mut self, mut requests: Vec<Request>) -> (bool, Vec<Request>) {
        if self.engine.replicas() == 1 {
            if let Err(error) = self.engine.read_barrier() {
                for request in requests.drain(..) {
                    let _ = request.response.send(Err(error.to_string()));
                }
                return (false, requests);
            }
        }
        if requests.len() == 1 {
            // A standalone ACK needs neither an event list nor a valid-request list.
            let request = requests.pop().unwrap();
            let Some((event, _)) = self.prepare_ack_message(&request.message) else {
                let _ = request.response.send(Ok(true));
                return (false, requests);
            };
            let result = self.commit(event);
            return (self.finish_ack_request(request, result), requests);
        }
        let mut events = Vec::with_capacity(requests.len());
        let mut valid = Vec::with_capacity(requests.len());
        for request in requests.drain(..) {
            if let Some((event, _)) = self.prepare_ack_message(&request.message) {
                events.push(event);
                valid.push(request);
            } else {
                let _ = request.response.send(Ok(true));
            }
        }
        if events.is_empty() {
            return (false, requests);
        }
        let event = if events.len() == 1 {
            events.pop().unwrap()
        } else {
            Event::AckBatch { events }
        };
        let result = self.commit(event);
        let mut replied = false;
        for request in valid {
            replied |= self.finish_ack_request(request, result.clone());
        }
        (replied, requests)
    }
    fn maintain(&mut self) -> Result<bool, String> {
        if self.engine.is_poisoned() {
            return Ok(false);
        }
        let applied = self.engine.voter_progress().map(|p| p.applied);
        if self.plan.is_none()
            && (self.engine.replicas() == 1 || self.checkpoint_checked != applied)
        {
            // Pure reads cannot add accepted bytes. A new application position
            // needs a pressure check; a new leader/configuration gets a fresh
            // worker and an initial check. Keep standalone polling unchanged.
            if self.engine.checkpoint_needed().map_err(|e| e.to_string())? {
                self.engine.checkpoint().map_err(|e| e.to_string())?;
            }
            self.checkpoint_checked = applied;
        }
        if self.plan.is_none()
            && (self.engine.replicas() == 1 || !self.quorum_observed)
            && (!self.pulls.is_empty()
                || self.has_push()
                || self.state().expiration_needed(self.time()))
        {
            self.barrier()?;
        }
        self.engine
            .flush_consumer_state(false)
            .map_err(|e| e.to_string())?;
        self.expire()?;
        let mut progressed = false;
        let n = self.pulls.len();
        for _ in 0..n {
            let mut p = self.pulls.pop_front().unwrap();
            let time = self.time();
            let valid = self
                .state()
                .streams
                .get(&p.stream)
                .filter(|s| s.generation == p.stream_generation)
                .and_then(|s| s.consumers.get(&p.consumer))
                .is_some_and(|c| c.generation == p.generation);
            if !valid {
                self.status(&p.inbox, 409, "Consumer Deleted", p.remaining)?;
                continue;
            }
            if !self.interested(&p.inbox, None) {
                continue;
            }
            if time >= p.expires {
                self.status(&p.inbox, 408, "Request Timeout", p.remaining)?;
                continue;
            }
            let before = p.remaining;
            self.fill(&mut p)?;
            progressed |= p.remaining < before;
            if p.remaining > 0 {
                if p.no_wait
                    && self
                        .state()
                        .prepare_delivery(&p.stream, &p.consumer, self.time())
                        .map_err(|e| e.to_string())?
                        .is_none()
                {
                    self.no_messages(&p)?;
                    continue;
                }
                if p.heartbeat > 0 && time >= p.next_heartbeat {
                    self.status(&p.inbox, 100, "Idle Heartbeat", p.remaining)?;
                    p.next_heartbeat = time.saturating_add(p.heartbeat);
                }
                self.pulls.push_back(p);
            }
        }
        let push: Vec<_> = self
            .state()
            .streams
            .iter()
            .flat_map(|(s, state)| {
                state.consumers.iter().filter_map(move |(name, c)| {
                    c.config.deliver_subject.as_ref().map(|target| {
                        (
                            s.clone(),
                            name.clone(),
                            target.clone(),
                            c.config.deliver_group.clone(),
                            state.generation,
                            c.generation,
                        )
                    })
                })
            })
            .collect();
        for (stream, name, target, queue, sgen, cgen) in push {
            if !self.interested(&target, queue.as_deref()) {
                continue;
            }
            let mut p = Pull {
                stream,
                consumer: name,
                stream_generation: sgen,
                generation: cgen,
                inbox: target,
                remaining: 64,
                bytes: 0,
                max_bytes: 0,
                expires: u64::MAX,
                heartbeat: 0,
                next_heartbeat: 0,
                no_wait: false,
            };
            let before = p.remaining;
            self.fill(&mut p)?;
            progressed |= p.remaining < before;
        }
        Ok(progressed)
    }
}

#[cfg(test)]
mod output_adapter_tests {
    use super::*;

    fn info(inbox: &str) -> Message {
        Message {
            subject: "$JS.API.INFO".into(),
            reply: Some(inbox.into()),
            headers: Bytes::new(),
            payload: Bytes::from_static(b"{}"),
        }
    }

    #[tokio::test]
    async fn direct_adapter_drains_startup_output_before_live_replies_and_attaches_once() {
        let handle = Handle::start(crate::storage::MemoryStore::default()).unwrap();
        for i in 0..8 {
            assert!(handle.request(info(&format!("_INBOX.{i}"))).await.unwrap());
        }
        let pending: Vec<_> = (8..32)
            .map(|i| handle.try_request(info(&format!("_INBOX.{i}"))).unwrap())
            .collect();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let captured = seen.clone();
        handle
            .attach_output(
                Arc::new(|_, _| true),
                Arc::new(move |out| {
                    captured
                        .lock()
                        .unwrap()
                        .push((out.target, out.protocol_reply));
                    Ok(())
                }),
            )
            .unwrap();
        for request in pending {
            assert!(request.wait().await.unwrap());
        }
        assert_eq!(
            *seen.lock().unwrap(),
            (0..32)
                .map(|i| (format!("_INBOX.{i}"), true))
                .collect::<Vec<_>>()
        );
        assert!(handle.take_output(Arc::new(|_, _| true)).is_err());
        assert!(handle
            .attach_output(Arc::new(|_, _| true), Arc::new(|_| Ok(())))
            .is_err());
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn direct_adapter_propagates_dispatch_failure_and_cannot_replace_a_receiver() {
        let handle = Handle::start(crate::storage::MemoryStore::default()).unwrap();
        handle
            .attach_output(
                Arc::new(|_, _| true),
                Arc::new(|_| Err("adapter stopped".into())),
            )
            .unwrap();
        assert!(handle.request(info("_INBOX.failure")).await.is_err());
        handle.shutdown().await;
        let handle = Handle::start(crate::storage::MemoryStore::default()).unwrap();
        let _receiver = handle.take_output(Arc::new(|_, _| true)).unwrap();
        assert!(handle
            .attach_output(Arc::new(|_, _| true), Arc::new(|_| Ok(())))
            .is_err());
        handle.shutdown().await;
    }
    #[tokio::test]
    async fn ignored_ack_bursts_release_admission_and_do_not_emit_confirmations() {
        let handle = Handle::start(crate::storage::MemoryStore::default()).unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let captured = seen.clone();
        handle
            .attach_output(
                Arc::new(|_, _| true),
                Arc::new(move |out| {
                    captured.lock().unwrap().push(out.target);
                    Ok(())
                }),
            )
            .unwrap();
        // Exceed the entire admission budget repeatedly, with both singleton
        // and queued ACK bursts, including an unrelated request between rounds.
        for width in [1, 8, 32] {
            for round in 0..32 {
                let pending: Vec<_> = (0..width)
                    .map(|i| {
                        handle
                            .try_request(Message {
                                subject: "$JS.ACK._.1.missing.D.1.1.1.1.0.1".into(),
                                reply: Some(format!("_INBOX.ignored.{width}.{round}.{i}")),
                                headers: Bytes::new(),
                                payload: Bytes::new(),
                            })
                            .unwrap()
                    })
                    .collect();
                for request in pending {
                    assert!(tokio::time::timeout(Duration::from_secs(2), request.wait())
                        .await
                        .unwrap()
                        .unwrap());
                }
                if round % 8 == 0 {
                    assert!(handle.request(info("_INBOX.info")).await.unwrap());
                }
            }
        }
        assert_eq!(*seen.lock().unwrap(), vec!["_INBOX.info"; 12]);
        handle.shutdown().await;
    }
}
