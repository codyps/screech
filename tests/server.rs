use futures_util::StreamExt;
use screech::server::{Config, Server};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    sync::watch,
    time::{timeout, Duration},
};
struct Running {
    addr: std::net::SocketAddr,
    stop: watch::Sender<bool>,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}
impl Running {
    async fn start(mut config: Config) -> Self {
        config.listen = "127.0.0.1:0".parse().unwrap();
        let server = Server::bind(config).await.unwrap();
        let addr = server.local_addr().unwrap();
        let (stop, rx) = watch::channel(false);
        let task = tokio::spawn(server.run(rx));
        Self { addr, stop, task }
    }
    async fn stop(self) {
        self.stop.send_replace(true);
        timeout(Duration::from_secs(5), self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    async fn raw(&self) -> BufReader<TcpStream> {
        let mut socket = BufReader::new(TcpStream::connect(self.addr).await.unwrap());
        let mut info = String::new();
        socket.read_line(&mut info).await.unwrap();
        assert!(info.starts_with("INFO "));
        socket
            .get_mut()
            .write_all(b"CONNECT {\"verbose\":false,\"headers\":true}\r\n")
            .await
            .unwrap();
        socket
    }
}
async fn line(s: &mut BufReader<TcpStream>) -> String {
    let mut out = String::new();
    timeout(Duration::from_secs(3), s.read_line(&mut out))
        .await
        .unwrap()
        .unwrap();
    out
}
#[tokio::test]
async fn official_rust_client_pub_sub_headers_request_and_no_responders() {
    let server = Running::start(Config::default()).await;
    let client = async_nats::connect(server.addr.to_string()).await.unwrap();
    let mut sub = client.subscribe("foo.*").await.unwrap();
    client.flush().await.unwrap();
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("X-Test", "yes");
    client
        .publish_with_headers("foo.x", headers, bytes::Bytes::from_static(b"a\0b\r\n"))
        .await
        .unwrap();
    client.flush().await.unwrap();
    let msg = timeout(Duration::from_secs(3), sub.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&msg.payload[..], b"a\0b\r\n");
    assert_eq!(msg.headers.unwrap().get("X-Test").unwrap().as_str(), "yes");
    let mut responder = client.subscribe("service").await.unwrap();
    client.flush().await.unwrap();
    let rc = client.clone();
    let task = tokio::spawn(async move {
        let m = responder.next().await.unwrap();
        rc.publish(m.reply.unwrap(), "ok".into()).await.unwrap();
    });
    assert_eq!(
        client
            .request("service", "request".into())
            .await
            .unwrap()
            .payload,
        "ok"
    );
    task.await.unwrap();
    let error = client
        .request("missing", "request".into())
        .await
        .unwrap_err();
    assert_eq!(error.kind(), async_nats::RequestErrorKind::NoResponders);
    drop(client);
    server.stop().await;
}
#[tokio::test]
async fn unsubscribe_total_limit_and_queue_handover() {
    let server = Running::start(Config::default()).await;
    let mut old = server.raw().await;
    let mut new = server.raw().await;
    let mut publisher = server.raw().await;
    old.get_mut()
        .write_all(b"SUB foo q old\r\nUNSUB old 1\r\nPING\r\n")
        .await
        .unwrap();
    assert_eq!(line(&mut old).await, "PONG\r\n");
    publisher
        .get_mut()
        .write_all(b"PUB foo 1\r\nx\r\nPING\r\n")
        .await
        .unwrap();
    assert_eq!(line(&mut publisher).await, "PONG\r\n");
    new.get_mut()
        .write_all(b"SUB foo q new\r\nPING\r\n")
        .await
        .unwrap();
    assert_eq!(line(&mut new).await, "PONG\r\n");
    old.get_mut()
        .write_all(b"UNSUB old\r\nPING\r\n")
        .await
        .unwrap();
    assert_eq!(line(&mut old).await, "MSG foo old 1\r\n");
    assert_eq!(line(&mut old).await, "x\r\n");
    assert_eq!(line(&mut old).await, "PONG\r\n");
    publisher
        .get_mut()
        .write_all(b"PUB foo 1\r\ny\r\n")
        .await
        .unwrap();
    assert_eq!(line(&mut new).await, "MSG foo new 1\r\n");
    assert_eq!(line(&mut new).await, "y\r\n");
    server.stop().await;
}
#[tokio::test]
async fn echo_false_and_protocol_error_isolation() {
    let server = Running::start(Config::default()).await;
    let mut a = BufReader::new(TcpStream::connect(server.addr).await.unwrap());
    line(&mut a).await;
    a.get_mut().write_all(b"CONNECT {\"verbose\":false,\"echo\":false}\r\nSUB foo 1\r\nPUB foo 1\r\nx\r\nPING\r\n").await.unwrap();
    assert_eq!(line(&mut a).await, "PONG\r\n");
    a.get_mut()
        .write_all(b"PUB foo 999999999\r\n")
        .await
        .unwrap();
    assert!(line(&mut a).await.starts_with("-ERR"));
    let mut healthy = server.raw().await;
    healthy.get_mut().write_all(b"PING\r\n").await.unwrap();
    assert_eq!(line(&mut healthy).await, "PONG\r\n");
    server.stop().await;
}
#[tokio::test]
async fn token_authentication() {
    let server = Running::start(Config {
        token: Some("secret".into()),
        ..Config::default()
    })
    .await;
    let mut s = BufReader::new(TcpStream::connect(server.addr).await.unwrap());
    line(&mut s).await;
    s.get_mut()
        .write_all(b"CONNECT {\"auth_token\":\"wrong\"}\r\n")
        .await
        .unwrap();
    assert!(line(&mut s).await.contains("Authorization Violation"));
    let client = async_nats::ConnectOptions::with_token("secret".into())
        .connect(server.addr.to_string())
        .await
        .unwrap();
    client.flush().await.unwrap();
    drop(client);
    server.stop().await;
}
#[tokio::test]
async fn stalled_subscriber_does_not_block_a_healthy_connection() {
    let server = Running::start(Config {
        max_pending: 1024,
        write_deadline: Duration::from_millis(100),
        ..Config::default()
    })
    .await;
    let mut slow = server.raw().await;
    slow.get_mut()
        .write_all(b"SUB flood 1\r\nPING\r\n")
        .await
        .unwrap();
    assert_eq!(line(&mut slow).await, "PONG\r\n");
    let mut publisher = server.raw().await;
    // A single frame exceeds this subscriber's byte budget, forcing deterministic eviction.
    let payload = vec![b'x'; 2048];
    publisher
        .get_mut()
        .write_all(b"PUB flood 2048\r\n")
        .await
        .unwrap();
    publisher.get_mut().write_all(&payload).await.unwrap();
    publisher
        .get_mut()
        .write_all(b"\r\nPING\r\n")
        .await
        .unwrap();
    assert_eq!(line(&mut publisher).await, "PONG\r\n");
    let mut healthy = server.raw().await;
    healthy.get_mut().write_all(b"PING\r\n").await.unwrap();
    assert_eq!(line(&mut healthy).await, "PONG\r\n");
    let mut byte = [0u8];
    assert_eq!(
        timeout(Duration::from_secs(2), slow.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    server.stop().await;
}
#[tokio::test(start_paused = true)]
async fn heartbeat_disconnects_unresponsive_client() {
    let server = Running::start(Config {
        ping_interval: Duration::from_secs(1),
        max_pings: 1,
        ..Config::default()
    })
    .await;
    let mut s = server.raw().await;
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(line(&mut s).await, "PING\r\n");
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(line(&mut s).await.contains("Stale Connection"));
    server.stop().await;
}

#[tokio::test]
async fn official_client_stream_api_and_explicit_unsupported_configuration() {
    let dir = tempfile::tempdir().unwrap();
    let store = screech::storage::SqliteStore::open(dir.path().join("data.db")).unwrap();
    let handle = screech::jetstream::Handle::start(store).unwrap();
    let server = Running::start(Config {
        jetstream: Some(handle),
        ..Config::default()
    })
    .await;
    let client = async_nats::connect(server.addr.to_string()).await.unwrap();
    let js = async_nats::jetstream::new(client.clone());
    let stream = js
        .create_stream(async_nats::jetstream::stream::Config {
            name: "T".into(),
            subjects: vec!["events.>".into()],
            duplicate_window: Duration::ZERO,
            ..Default::default()
        })
        .await
        .unwrap();
    let ack = js
        .publish("events.a", bytes::Bytes::from_static(b"hello"))
        .await
        .unwrap()
        .await
        .unwrap();
    assert_eq!(ack.sequence, 1);
    let msg = stream.get_raw_message(1).await.unwrap();
    assert_eq!(msg.payload, "hello");
    assert!(js
        .create_stream(async_nats::jetstream::stream::Config {
            name: "BAD".into(),
            subjects: vec!["bad.>".into()],
            num_replicas: 3,
            ..Default::default()
        })
        .await
        .is_err());
    stream.purge().await.unwrap();
    assert!(stream.get_raw_message(1).await.is_err());
    assert_eq!(
        js.publish("events.b", "new".into())
            .await
            .unwrap()
            .await
            .unwrap()
            .sequence,
        2
    );
    js.delete_stream("T").await.unwrap();
    drop(client);
    server.stop().await;
}

#[tokio::test]
async fn durable_pull_ack_redelivery_and_same_connection_waiting_pull() {
    use async_nats::jetstream::{
        consumer::{pull, AckPolicy},
        AckKind,
    };
    let dir = tempfile::tempdir().unwrap();
    let handle = screech::jetstream::Handle::start(
        screech::storage::SqliteStore::open(dir.path().join("data.db")).unwrap(),
    )
    .unwrap();
    let server = Running::start(Config {
        jetstream: Some(handle),
        ..Default::default()
    })
    .await;
    let client = async_nats::connect(server.addr.to_string()).await.unwrap();
    let js = async_nats::jetstream::new(client.clone());
    let stream = js
        .create_stream(async_nats::jetstream::stream::Config {
            name: "S".into(),
            subjects: vec!["events.>".into()],
            ..Default::default()
        })
        .await
        .unwrap();
    let consumer = stream
        .create_consumer(pull::Config {
            durable_name: Some("D".into()),
            ack_policy: AckPolicy::Explicit,
            ack_wait: Duration::from_secs(5),
            max_ack_pending: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    for data in ["one", "two"] {
        js.publish("events.a", data.into())
            .await
            .unwrap()
            .await
            .unwrap();
    }
    let mut first = consumer.fetch().max_messages(1).messages().await.unwrap();
    let first = timeout(Duration::from_secs(2), first.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(first.payload, "one");
    assert_eq!(first.info().unwrap().delivered, 1);
    assert_eq!(first.subject.as_str(), "events.a");
    let mut waiting = consumer
        .batch()
        .max_messages(1)
        .expires(Duration::from_secs(2))
        .messages()
        .await
        .unwrap();
    // Progress must extend the deadline, while the same connection also has a waiting fetch.
    first.ack_with(AckKind::Progress).await.unwrap();
    client.flush().await.unwrap();
    first.double_ack().await.unwrap();
    let second = timeout(Duration::from_secs(2), waiting.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(second.payload, "two");
    second
        .ack_with(AckKind::Nak(Some(Duration::from_millis(20))))
        .await
        .unwrap();
    let mut redelivery = consumer
        .batch()
        .max_messages(1)
        .expires(Duration::from_secs(2))
        .messages()
        .await
        .unwrap();
    let again = timeout(Duration::from_secs(3), redelivery.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(again.payload, "two");
    assert_eq!(again.info().unwrap().delivered, 2);
    again.double_ack().await.unwrap();
    let mut consumer = consumer;
    let info = consumer.info().await.unwrap();
    assert_eq!(info.num_ack_pending, 0);
    assert_eq!(info.ack_floor.consumer_sequence, 3);
    drop(client);
    server.stop().await;
}

#[tokio::test]
async fn official_client_workqueue_retention_and_deduplication() {
    use async_nats::jetstream::{
        consumer::{pull, AckPolicy},
        stream::RetentionPolicy,
    };
    let dir = tempfile::tempdir().unwrap();
    let handle = screech::jetstream::Handle::start(
        screech::storage::SqliteStore::open(dir.path().join("data.db")).unwrap(),
    )
    .unwrap();
    let server = Running::start(Config {
        jetstream: Some(handle),
        ..Default::default()
    })
    .await;
    let client = async_nats::connect(server.addr.to_string()).await.unwrap();
    let js = async_nats::jetstream::new(client.clone());
    let stream = js
        .create_stream(async_nats::jetstream::stream::Config {
            name: "S".into(),
            subjects: vec!["jobs.>".into()],
            retention: RetentionPolicy::WorkQueue,
            duplicate_window: Duration::from_secs(120),
            ..Default::default()
        })
        .await
        .unwrap();
    let consumer = stream
        .create_consumer(pull::Config {
            durable_name: Some("D".into()),
            ack_policy: AckPolicy::Explicit,
            ..Default::default()
        })
        .await
        .unwrap();
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("Nats-Msg-Id", "stable-id");
    let a = js
        .publish_with_headers("jobs.a", headers.clone(), "one".into())
        .await
        .unwrap()
        .await
        .unwrap();
    let b = js
        .publish_with_headers("jobs.a", headers.clone(), "different payload".into())
        .await
        .unwrap()
        .await
        .unwrap();
    assert_eq!(a.sequence, b.sequence);
    assert!(b.duplicate);
    let mut messages = consumer.fetch().max_messages(1).messages().await.unwrap();
    let m = messages.next().await.unwrap().unwrap();
    m.double_ack().await.unwrap();
    assert!(stream.get_raw_message(1).await.is_err());
    let duplicate = js
        .publish_with_headers("jobs.a", headers, "one".into())
        .await
        .unwrap()
        .await
        .unwrap();
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.sequence, 1);
    drop(client);
    server.stop().await;
}

#[tokio::test]
async fn push_consumer_uses_original_subject_and_ack_metadata() {
    use async_nats::jetstream::consumer::{push, AckPolicy};
    let dir = tempfile::tempdir().unwrap();
    let handle = screech::jetstream::Handle::start(
        screech::storage::SqliteStore::open(dir.path().join("data.db")).unwrap(),
    )
    .unwrap();
    let server = Running::start(Config {
        jetstream: Some(handle),
        ..Default::default()
    })
    .await;
    let client = async_nats::connect(server.addr.to_string()).await.unwrap();
    let js = async_nats::jetstream::new(client.clone());
    let stream = js
        .create_stream(async_nats::jetstream::stream::Config {
            name: "S".into(),
            subjects: vec!["events.>".into()],
            ..Default::default()
        })
        .await
        .unwrap();
    let consumer = stream
        .create_consumer(push::Config {
            durable_name: Some("D".into()),
            deliver_subject: "deliver.push".into(),
            ack_policy: AckPolicy::Explicit,
            ..Default::default()
        })
        .await
        .unwrap();
    let mut messages = consumer.messages().await.unwrap();
    js.publish("events.real", "value".into())
        .await
        .unwrap()
        .await
        .unwrap();
    let m = timeout(Duration::from_secs(2), messages.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(m.subject.as_str(), "events.real");
    let info = m.info().unwrap();
    assert_eq!(info.stream, "S");
    assert_eq!(info.consumer, "D");
    assert_eq!(info.stream_sequence, 1);
    m.double_ack().await.unwrap();
    drop(messages);
    drop(client);
    server.stop().await;
}

#[tokio::test]
async fn queue_push_does_not_leak_to_plain_or_other_queue_subscriptions() {
    use async_nats::jetstream::consumer::{push, AckPolicy};
    let dir = tempfile::tempdir().unwrap();
    let handle = screech::jetstream::Handle::start(
        screech::storage::SqliteStore::open(dir.path().join("db")).unwrap(),
    )
    .unwrap();
    let server = Running::start(Config {
        jetstream: Some(handle),
        ..Default::default()
    })
    .await;
    let client = async_nats::connect(server.addr.to_string()).await.unwrap();
    let js = async_nats::jetstream::new(client.clone());
    let stream = js
        .create_stream(async_nats::jetstream::stream::Config {
            name: "S".into(),
            subjects: vec!["events.>".into()],
            ..Default::default()
        })
        .await
        .unwrap();
    let mut plain = client.subscribe("delivery.queue").await.unwrap();
    let mut other = client
        .queue_subscribe("delivery.queue", "other".into())
        .await
        .unwrap();
    let consumer = stream
        .create_consumer(push::Config {
            durable_name: Some("D".into()),
            deliver_subject: "delivery.queue".into(),
            deliver_group: Some("workers".into()),
            ack_policy: AckPolicy::Explicit,
            ..Default::default()
        })
        .await
        .unwrap();
    let mut messages = consumer.messages().await.unwrap();
    client.flush().await.unwrap();
    js.publish("events.a", "one".into())
        .await
        .unwrap()
        .await
        .unwrap();
    let message = timeout(Duration::from_secs(2), messages.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    message.double_ack().await.unwrap();
    assert!(timeout(Duration::from_millis(30), plain.next())
        .await
        .is_err());
    assert!(timeout(Duration::from_millis(30), other.next())
        .await
        .is_err());
    drop(messages);
    drop(client);
    server.stop().await;
}

#[tokio::test]
async fn no_wait_large_batch_and_filtered_retention_purge() {
    use async_nats::jetstream::consumer::{pull, AckPolicy};
    let dir = tempfile::tempdir().unwrap();
    let handle = screech::jetstream::Handle::start(
        screech::storage::SqliteStore::open(dir.path().join("db")).unwrap(),
    )
    .unwrap();
    let server = Running::start(Config {
        jetstream: Some(handle),
        ..Default::default()
    })
    .await;
    let client = async_nats::connect(server.addr.to_string()).await.unwrap();
    let js = async_nats::jetstream::new(client.clone());
    let stream = js
        .create_stream(async_nats::jetstream::stream::Config {
            name: "S".into(),
            subjects: vec!["events.>".into()],
            ..Default::default()
        })
        .await
        .unwrap();
    let consumer = stream
        .create_consumer(pull::Config {
            durable_name: Some("D".into()),
            ack_policy: AckPolicy::Explicit,
            ..Default::default()
        })
        .await
        .unwrap();
    for n in 0..100 {
        js.publish("events.a", n.to_string().into())
            .await
            .unwrap()
            .await
            .unwrap();
    }
    let mut batch = consumer.fetch().max_messages(100).messages().await.unwrap();
    let mut received = 0;
    while let Some(message) = timeout(Duration::from_secs(3), batch.next()).await.unwrap() {
        let message = message.unwrap();
        assert_eq!(message.payload, received.to_string());
        received += 1;
        message.double_ack().await.unwrap();
    }
    assert_eq!(received, 100);
    let request = serde_json::json!({"filter":"events.a","keep":1});
    let reply = client
        .request(
            "$JS.API.STREAM.PURGE.S",
            serde_json::to_vec(&request).unwrap().into(),
        )
        .await
        .unwrap();
    let result: serde_json::Value = serde_json::from_slice(&reply.payload).unwrap();
    assert_eq!(result["purged"], 99);
    assert!(stream.get_raw_message(1).await.is_err());
    assert_eq!(stream.get_raw_message(100).await.unwrap().payload, "99");
    drop(client);
    server.stop().await;
}

#[tokio::test]
async fn durable_consumer_updates_and_delivery_limit_advisories() {
    use async_nats::jetstream::consumer::{pull, AckPolicy};
    let dir = tempfile::tempdir().unwrap();
    let handle = screech::jetstream::Handle::start(
        screech::storage::SqliteStore::open(dir.path().join("db")).unwrap(),
    )
    .unwrap();
    let server = Running::start(Config {
        jetstream: Some(handle),
        ..Default::default()
    })
    .await;
    let client = async_nats::connect(server.addr.to_string()).await.unwrap();
    let js = async_nats::jetstream::new(client.clone());
    let stream = js
        .create_stream(async_nats::jetstream::stream::Config {
            name: "S".into(),
            subjects: vec!["events.>".into()],
            retention: async_nats::jetstream::stream::RetentionPolicy::WorkQueue,
            ..Default::default()
        })
        .await
        .unwrap();
    let mut consumer = stream
        .create_consumer(pull::Config {
            durable_name: Some("D".into()),
            ack_policy: AckPolicy::Explicit,
            ack_wait: Duration::from_secs(30),
            max_deliver: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    let mut config = serde_json::to_value(&consumer.info().await.unwrap().config).unwrap();
    config["ack_wait"] = serde_json::json!(20_000_000u64);
    let body = serde_json::json!({"stream_name":"S","action":"update","config":config});
    let reply = client
        .request(
            "$JS.API.CONSUMER.CREATE.S.D",
            serde_json::to_vec(&body).unwrap().into(),
        )
        .await
        .unwrap();
    let updated: serde_json::Value = serde_json::from_slice(&reply.payload).unwrap();
    assert!(updated.get("error").is_none(), "{updated}");
    assert_eq!(
        consumer.info().await.unwrap().config.ack_wait,
        Duration::from_millis(20)
    );
    let mut advisories = client
        .subscribe("$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.S.D")
        .await
        .unwrap();
    client.flush().await.unwrap();
    js.publish("events.a", "unacked".into())
        .await
        .unwrap()
        .await
        .unwrap();
    let mut batch = consumer.fetch().max_messages(1).messages().await.unwrap();
    let _message = batch.next().await.unwrap().unwrap();
    let advisory = timeout(Duration::from_secs(3), advisories.next())
        .await
        .unwrap()
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&advisory.payload).unwrap();
    assert_eq!(value["stream_seq"], 1);
    assert_eq!(value["deliveries"], 1);
    assert_eq!(stream.get_raw_message(1).await.unwrap().payload, "unacked");
    let info = consumer.info().await.unwrap();
    assert_eq!(info.num_ack_pending, 0);
    assert_eq!(info.num_pending, 0);
    drop(client);
    server.stop().await;
}

type GateState = (std::sync::Mutex<(bool, bool, bool)>, std::sync::Condvar);
#[derive(Clone, Default)]
struct StorageGate(std::sync::Arc<GateState>);
impl StorageGate {
    fn arm(&self) {
        *self.0 .0.lock().unwrap() = (true, false, false);
    }
    fn release(&self) {
        self.0 .0.lock().unwrap().2 = true;
        self.0 .1.notify_all();
    }
    fn block(&self) {
        let mut state = self.0 .0.lock().unwrap();
        if state.0 {
            state.1 = true;
            while !state.2 {
                state = self.0 .1.wait(state).unwrap();
            }
        }
    }
    async fn blocked(&self) {
        timeout(Duration::from_secs(3), async {
            while !self.0 .0.lock().unwrap().1 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
    }
}
struct ReleaseStorage(StorageGate);
impl Drop for ReleaseStorage {
    fn drop(&mut self) {
        self.0.release();
    }
}
struct GatedStore {
    inner: screech::storage::MemoryStore,
    gate: StorageGate,
}
impl screech::storage::Store for GatedStore {
    fn append(&mut self, s: &str, h: &[u8], p: &[u8]) -> Result<u64, screech::storage::StoreError> {
        self.gate.block();
        screech::storage::Store::append(&mut self.inner, s, h, p)
    }
    fn get(
        &self,
        n: u64,
    ) -> Result<Option<screech::storage::Record>, screech::storage::StoreError> {
        screech::storage::Store::get(&self.inner, n)
    }
    fn last_sequence(&self) -> Result<u64, screech::storage::StoreError> {
        screech::storage::Store::last_sequence(&self.inner)
    }
    fn purge(&mut self) -> Result<(), screech::storage::StoreError> {
        screech::storage::Store::purge(&mut self.inner)
    }
}

#[tokio::test]
async fn captured_core_and_same_connection_ping_progress_before_storage_commit() {
    let gate = StorageGate::default();
    let _release = ReleaseStorage(gate.clone());
    let handle = screech::jetstream::Handle::start(GatedStore {
        inner: Default::default(),
        gate: gate.clone(),
    })
    .unwrap();
    let server = Running::start(Config {
        jetstream: Some(handle),
        storage_deadline: Duration::from_secs(10),
        ..Default::default()
    })
    .await;
    let client = async_nats::connect(server.addr.to_string()).await.unwrap();
    let js = async_nats::jetstream::new(client.clone());
    let stream = js
        .create_stream(async_nats::jetstream::stream::Config {
            name: "S".into(),
            subjects: vec!["events.>".into()],
            ..Default::default()
        })
        .await
        .unwrap();
    let mut live = client.subscribe("events.>").await.unwrap();
    let mut plain = client.subscribe("plain").await.unwrap();
    client.flush().await.unwrap();
    gate.arm();
    let mut publisher = server.raw().await;
    publisher
        .get_mut()
        .write_all(b"SUB _ACK 9\r\nPUB events.a _ACK 1\r\na\r\n")
        .await
        .unwrap();
    gate.blocked().await;
    publisher
        .get_mut()
        .write_all(b"PUB events.b _ACK 1\r\nb\r\n")
        .await
        .unwrap();
    let headers = b"NATS/1.0\r\nX-Test: yes\r\n\r\n";
    let payload = b"c\0d";
    let mut packet = format!(
        "HPUB events.c _ACK {} {}\r\n",
        headers.len(),
        headers.len() + payload.len()
    )
    .into_bytes();
    packet.extend_from_slice(headers);
    packet.extend_from_slice(payload);
    packet.extend_from_slice(b"\r\nPUB plain 1\r\nx\r\nPING\r\n");
    publisher.get_mut().write_all(&packet).await.unwrap();
    for expected in [b"a".as_slice(), b"b", payload] {
        let message = timeout(Duration::from_secs(3), live.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(message.payload.as_ref(), expected);
        if expected == payload {
            assert_eq!(
                message.headers.unwrap().get("X-Test").unwrap().as_str(),
                "yes"
            );
        }
    }
    assert_eq!(
        timeout(Duration::from_secs(3), plain.next())
            .await
            .unwrap()
            .unwrap()
            .payload,
        "x"
    );
    assert_eq!(line(&mut publisher).await, "PONG\r\n");
    let mut unexpected = String::new();
    assert!(
        timeout(
            Duration::from_millis(30),
            publisher.read_line(&mut unexpected)
        )
        .await
        .is_err(),
        "early durable ACK: {unexpected}"
    );
    gate.release();
    for seq in 1..=3 {
        let header = line(&mut publisher).await;
        assert!(header.starts_with("MSG _ACK 9 "), "{header}");
        let size: usize = header.split_whitespace().last().unwrap().parse().unwrap();
        let mut body = vec![0; size + 2];
        publisher.read_exact(&mut body).await.unwrap();
        let ack: serde_json::Value = serde_json::from_slice(&body[..size]).unwrap();
        assert_eq!(ack["seq"], seq);
    }
    assert_eq!(
        stream.get_raw_message(3).await.unwrap().payload.as_ref(),
        payload
    );
    assert!(
        timeout(Duration::from_millis(30), live.next())
            .await
            .is_err(),
        "Core forwarded again after commit"
    );
    drop(publisher);
    drop(client);
    server.stop().await;
}

#[tokio::test]
async fn pipelined_catalog_create_and_publication_keep_storage_order() {
    let handle =
        screech::jetstream::Handle::start(screech::storage::MemoryStore::default()).unwrap();
    let server = Running::start(Config {
        jetstream: Some(handle),
        ..Default::default()
    })
    .await;
    let mut publisher = server.raw().await;
    let body = br#"{"name":"S","subjects":["future.>"]}"#;
    let mut packet = format!(
        "SUB _CAT 1\r\nSUB _ACK 2\r\nPUB $JS.API.STREAM.CREATE.S _CAT {}\r\n",
        body.len()
    )
    .into_bytes();
    packet.extend_from_slice(body);
    packet.extend_from_slice(b"\r\nPUB future.a _ACK 1\r\np\r\nPING\r\n");
    publisher.get_mut().write_all(&packet).await.unwrap();
    let mut count = 0;
    let mut pong = false;
    let mut ack = false;
    while count < 2 || !pong {
        let header = line(&mut publisher).await;
        if header == "PONG\r\n" {
            pong = true;
            continue;
        }
        assert!(header.starts_with("MSG "), "{header}");
        let size: usize = header.split_whitespace().last().unwrap().parse().unwrap();
        let mut body = vec![0; size + 2];
        publisher.read_exact(&mut body).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body[..size]).unwrap();
        assert!(value.get("error").is_none(), "{value}");
        if header.starts_with("MSG _ACK ") {
            assert_eq!(value["seq"], 1);
            ack = true;
        }
        count += 1;
    }
    assert!(ack);
    drop(publisher);
    server.stop().await;
}

#[tokio::test]
async fn cancelled_durable_waiters_do_not_release_queued_admission_slots() {
    let gate = StorageGate::default();
    let _release = ReleaseStorage(gate.clone());
    let handle = screech::jetstream::Handle::start(GatedStore {
        inner: Default::default(),
        gate: gate.clone(),
    })
    .unwrap();
    let create = screech::core::Message {
        subject: "$JS.API.STREAM.CREATE.S".into(),
        reply: Some("_CAT".into()),
        headers: bytes::Bytes::new(),
        payload: bytes::Bytes::from_static(br#"{"name":"S","subjects":["events.>"]}"#),
    };
    handle.request(create).await.unwrap();
    gate.arm();
    let message = screech::core::Message {
        subject: "events.a".into(),
        reply: None,
        headers: bytes::Bytes::new(),
        payload: bytes::Bytes::new(),
    };
    let mut pending = Vec::new();
    pending.push(handle.try_request(message.clone()).unwrap());
    gate.blocked().await;
    for _ in 1..256 {
        pending.push(handle.try_request(message.clone()).unwrap());
    }
    assert!(handle.try_request(message.clone()).is_err());
    drop(pending);
    assert!(handle.try_request(message).is_err());
    gate.release();
    let unmatched = screech::core::Message {
        subject: "unmatched".into(),
        reply: None,
        headers: bytes::Bytes::new(),
        payload: bytes::Bytes::new(),
    };
    assert!(!timeout(Duration::from_secs(3), handle.request(unmatched))
        .await
        .unwrap()
        .unwrap());
    handle.shutdown().await;
}
