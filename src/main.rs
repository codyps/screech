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
    let jetstream = a
        .store
        .map(|path| {
            screech::storage::SqliteStore::open(path)
                .map_err(std::io::Error::other)
                .and_then(|store| {
                    screech::jetstream::Handle::start(store).map_err(std::io::Error::other)
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
    server.run(rx).await
}
