use clap::Parser;
use screech::{
    parser::Limits,
    server::{Config, Server},
};
use std::{
    net::{IpAddr, SocketAddr},
    time::Duration,
};
#[derive(Parser)]
#[command(version, about = "Standalone NATS server")]
struct Args {
    #[arg(short = 'a', long, default_value = "127.0.0.1")]
    addr: IpAddr,
    #[arg(short = 'p', long, default_value_t = 4222)]
    port: u16,
    #[arg(long, default_value_t = 1048576)]
    max_payload: usize,
    #[arg(long, default_value_t = 8388608)]
    max_pending: usize,
    #[arg(long, default_value_t = 4096)]
    max_connections: usize,
    #[arg(long, default_value_t = 4096)]
    max_subscriptions: usize,
    #[arg(long, default_value_t = 2000)]
    write_deadline_ms: u64,
    #[arg(long, default_value_t = 120000)]
    ping_interval_ms: u64,
    #[arg(long)]
    token: Option<String>,
    #[arg(long, default_value_t = 5000)]
    storage_deadline_ms: u64,
    /// Enable the experimental stream API with a SQLite event log.
    #[arg(long)]
    store: Option<std::path::PathBuf>,
    /// UNSAFE: consumer reservations/ACKs may be lost after process or host failure; flush target 50 ms.
    #[arg(long, requires = "store")]
    unsafe_ack_durability: bool,
    /// Experimental strict fixed single-voter engine (no peer replication yet).
    #[arg(long, requires = "store", conflicts_with = "unsafe_ack_durability")]
    single_voter: bool,
    /// Fixed-membership Raft cluster configuration (JSON).
    #[arg(long, requires = "store", conflicts_with_all = ["single_voter", "unsafe_ack_durability"])]
    cluster_config: Option<std::path::PathBuf>,
    /// Initialize a fresh voter for the first cluster bootstrap only.
    #[arg(long, requires = "cluster_config")]
    cluster_bootstrap: bool,
}
fn main() -> std::io::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(run());
    // Blocking disk operations cannot be cancelled. Do not let a wedged storage
    // thread prevent process shutdown; unacknowledged outcomes recover on restart.
    runtime.shutdown_timeout(Duration::from_secs(2));
    result
}
async fn run() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    let a = Args::parse();
    if let Some(path) = &a.cluster_config {
        let config: screech::cluster::ClusterConfig =
            serde_json::from_slice(&std::fs::read(path)?).map_err(std::io::Error::other)?;
        let (stop, rx) = tokio::sync::watch::channel(false);
        spawn_signals(stop);
        return screech::cluster::run(
            config,
            a.store.as_ref().expect("clap requires store"),
            a.cluster_bootstrap,
            Config {
                limits: Limits {
                    payload: a.max_payload,
                    ..Limits::default()
                },
                max_pending: a.max_pending,
                max_connections: a.max_connections,
                max_subscriptions: a.max_subscriptions,
                write_deadline: Duration::from_millis(a.write_deadline_ms),
                storage_deadline: Duration::from_millis(a.storage_deadline_ms),
                ping_interval: Duration::from_millis(a.ping_interval_ms),
                token: a.token,
                ..Config::default()
            },
            rx,
        )
        .await;
    }
    if a.unsafe_ack_durability {
        eprintln!("UNSAFE ACK DURABILITY ENABLED: consumer reservations and confirmed ACKs may be lost after process or host failure. Consumer-state flush target: 50 ms. Publication ACKs remain durable.");
    }
    let jetstream = a
        .store
        .map(|path| {
            (if a.unsafe_ack_durability {
                screech::storage::SqliteStore::open_with_unsafe_ack_durability(path)
            } else {
                screech::storage::SqliteStore::open(path)
            })
            .map_err(std::io::Error::other)
            .and_then(|store| {
                (if a.single_voter {
                    screech::jetstream::Handle::start_single_voter(store)
                } else {
                    screech::jetstream::Handle::start(store)
                })
                .map_err(std::io::Error::other)
            })
        })
        .transpose()?;
    let server = Server::bind(Config {
        listen: SocketAddr::new(a.addr, a.port),
        limits: Limits {
            payload: a.max_payload,
            ..Limits::default()
        },
        max_pending: a.max_pending,
        max_connections: a.max_connections,
        max_subscriptions: a.max_subscriptions,
        write_deadline: Duration::from_millis(a.write_deadline_ms),
        storage_deadline: Duration::from_millis(a.storage_deadline_ms),
        ping_interval: Duration::from_millis(a.ping_interval_ms),
        token: a.token,
        jetstream,
        ..Config::default()
    })
    .await?;
    println!("LISTEN {}", server.local_addr()?);
    let (stop, rx) = tokio::sync::watch::channel(false);
    spawn_signals(stop);
    server.run(rx).await
}

fn spawn_signals(stop: tokio::sync::watch::Sender<bool>) {
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            let mut term =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("SIGTERM handler");
            tokio::select! {_=tokio::signal::ctrl_c()=>(),_=term.recv()=>()};
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
        stop.send_replace(true);
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_flags_require_explicit_storage_and_exclude_standalone_modes() {
        assert!(Args::try_parse_from(["screech", "--cluster-config", "cluster.json"]).is_err());
        assert!(Args::try_parse_from(["screech", "--cluster-bootstrap"]).is_err());
        for flag in ["--single-voter", "--unsafe-ack-durability"] {
            assert!(Args::try_parse_from([
                "screech",
                "--store",
                "db",
                "--cluster-config",
                "cluster.json",
                flag
            ])
            .is_err());
        }
        let args = Args::try_parse_from([
            "screech",
            "--store",
            "db",
            "--cluster-config",
            "cluster.json",
            "--cluster-bootstrap",
        ])
        .unwrap();
        assert!(args.cluster_bootstrap);
    }

    #[test]
    fn single_voter_requires_store_and_forbids_unsafe_ack() {
        assert!(Args::try_parse_from(["screech", "--single-voter"]).is_err());
        assert!(Args::try_parse_from([
            "screech",
            "--store",
            "db",
            "--single-voter",
            "--unsafe-ack-durability"
        ])
        .is_err());
        let strict = Args::try_parse_from(["screech", "--store", "db", "--single-voter"]).unwrap();
        assert!(strict.single_voter && !strict.unsafe_ack_durability);
        let old =
            Args::try_parse_from(["screech", "--store", "db", "--unsafe-ack-durability"]).unwrap();
        assert!(!old.single_voter && old.unsafe_ack_durability);
        assert!(
            !Args::try_parse_from(["screech", "--store", "db"])
                .unwrap()
                .single_voter
        );
    }
}
