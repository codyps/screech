use bytes::Bytes;
use screech::cluster::{ClusterConfig, Peer};
use std::{
    collections::BTreeMap,
    net::TcpListener,
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::TcpStream,
    time::{sleep, timeout},
};
struct Processes(Vec<Child>);
impl Drop for Processes {
    fn drop(&mut self) {
        for p in &mut self.0 {
            let _ = p.kill();
            let _ = p.wait();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn official_client_discovers_peers_and_reconnects_after_leader_process_loss() {
    let dir = tempfile::tempdir().unwrap();
    let held: Vec<_> = (0..9)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let addrs: Vec<_> = held.iter().map(|s| s.local_addr().unwrap()).collect();
    let peers: BTreeMap<_, _> = (1..=3)
        .map(|id| {
            (
                id,
                Peer {
                    raft: addrs[(id as usize - 1) * 3],
                    client: addrs[(id as usize - 1) * 3 + 1],
                    service: addrs[(id as usize - 1) * 3 + 2],
                },
            )
        })
        .collect();
    drop(held);
    let mut processes = Processes(Vec::new());
    for id in 1..=3 {
        let node = dir.path().join(id.to_string());
        std::fs::create_dir(&node).unwrap();
        let config = ClusterConfig {
            cluster_id: "official-client".into(),
            node_id: id,
            secret: "x".repeat(32),
            peers: peers.clone(),
            max_log_bytes: 128 * 1024 * 1024,
            initial_voters: Vec::new(),
        };
        let file = node.join("config.json");
        std::fs::write(&file, serde_json::to_vec(&config).unwrap()).unwrap();
        let log = std::fs::File::create(node.join("server.log")).unwrap();
        processes.0.push(
            Command::new(env!("CARGO_BIN_EXE_screech-server"))
                .args(["--cluster-config"])
                .arg(file)
                .arg("--store")
                .arg(node.join("db"))
                .stdout(Stdio::null())
                .stderr(log)
                .spawn()
                .unwrap(),
        );
    }
    let leader = timeout(Duration::from_secs(15), async {
        loop {
            for (&id, peer) in &peers {
                if let Ok(Ok(stream)) =
                    timeout(Duration::from_millis(100), TcpStream::connect(peer.service)).await
                {
                    let mut reader = BufReader::new(stream);
                    let mut info = String::new();
                    if matches!(
                        timeout(Duration::from_millis(100), reader.read_line(&mut info)).await,
                        Ok(Ok(_))
                    ) && info.starts_with("INFO ")
                    {
                        return id;
                    }
                }
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    // Supply one public address. INFO must teach the official client the other
    // public endpoints; the internal service addresses must not leak into discovery.
    let client = async_nats::ConnectOptions::new()
        .connection_timeout(Duration::from_secs(2))
        .connect(peers[&leader].client.to_string())
        .await
        .unwrap();
    let js = async_nats::jetstream::new(client.clone());
    let stream = js
        .create_stream(async_nats::jetstream::stream::Config {
            name: "T".into(),
            subjects: vec!["events.>".into()],
            num_replicas: 3,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(stream.cached_info().config.num_replicas, 3);
    assert_eq!(
        js.publish("events.a", Bytes::from_static(b"before"))
            .await
            .unwrap()
            .await
            .unwrap()
            .sequence,
        1
    );
    processes.0[(leader - 1) as usize].kill().unwrap();
    processes.0[(leader - 1) as usize].wait().unwrap();
    timeout(Duration::from_secs(20), async {
        loop {
            let mut headers = async_nats::HeaderMap::new();
            headers.insert("Nats-Msg-Id", "after-leader-loss");
            if let Ok(ack) = js
                .publish_with_headers("events.a", headers, Bytes::from_static(b"after"))
                .await
            {
                if let Ok(ack) = ack.await {
                    assert_eq!(ack.sequence, 2);
                    break;
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    let stream = js.get_stream("T").await.unwrap();
    assert_eq!(stream.get_raw_message(1).await.unwrap().payload, "before");
    assert_eq!(stream.get_raw_message(2).await.unwrap().payload, "after");
}
