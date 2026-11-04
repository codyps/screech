use super::*;
use tokio::io::{AsyncBufReadExt, BufReader};

async fn line(socket: &mut BufReader<TcpStream>) -> String {
    let mut line = String::new();
    timeout(Duration::from_secs(3), socket.read_line(&mut line))
        .await
        .unwrap()
        .unwrap();
    line
}

async fn ready(socket: TcpStream) -> BufReader<TcpStream> {
    let mut socket = BufReader::new(socket);
    assert!(line(&mut socket).await.starts_with("INFO "));
    socket
        .get_mut()
        .write_all(b"CONNECT {\"verbose\":false}\r\nPING\r\n")
        .await
        .unwrap();
    assert_eq!(line(&mut socket).await, "PONG\r\n");
    socket
}

async fn handoff(send: &mpsc::Sender<TcpStream>) -> TcpStream {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    send.send(listener.accept().await.unwrap().0).await.unwrap();
    client
}

#[tokio::test]
async fn handed_off_and_bound_sockets_share_routing_and_shutdown() {
    let server = Server::bind(Config {
        listen: "127.0.0.1:0".parse().unwrap(),
        ..Config::default()
    })
    .await
    .unwrap();
    let address = server.local_addr().unwrap();
    let (send, incoming) = mpsc::channel(2);
    let (stop, shutdown) = watch::channel(false);
    let task = tokio::spawn(server.run_with_incoming(shutdown, Some(incoming)));
    let mut local = ready(handoff(&send).await).await;
    let mut bound = ready(TcpStream::connect(address).await.unwrap()).await;
    for socket in [&mut local, &mut bound] {
        socket
            .get_mut()
            .write_all(b"SUB subject 1\r\nPING\r\n")
            .await
            .unwrap();
        assert_eq!(line(socket).await, "PONG\r\n");
    }
    for from_local in [false, true] {
        let publisher = if from_local { &mut local } else { &mut bound };
        publisher
            .get_mut()
            .write_all(b"PUB subject 1\r\nx\r\n")
            .await
            .unwrap();
        for socket in [&mut local, &mut bound] {
            assert_eq!(line(socket).await, "MSG subject 1 1\r\n");
            assert_eq!(line(socket).await, "x\r\n");
        }
    }
    drop(send);
    let mut extra = ready(TcpStream::connect(address).await.unwrap()).await;
    stop.send_replace(true);
    timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    for socket in [&mut local, &mut bound, &mut extra] {
        assert!(
            line(socket).await.is_empty(),
            "shutdown left a client connected"
        );
    }
}

#[tokio::test]
async fn handed_off_sockets_use_the_existing_connection_limit() {
    let server = Server::bind(Config {
        listen: "127.0.0.1:0".parse().unwrap(),
        max_connections: 1,
        ..Config::default()
    })
    .await
    .unwrap();
    let address = server.local_addr().unwrap();
    let (send, incoming) = mpsc::channel(2);
    let (stop, shutdown) = watch::channel(false);
    let task = tokio::spawn(server.run_with_incoming(shutdown, Some(incoming)));
    let mut local = ready(handoff(&send).await).await;
    let mut excess = BufReader::new(TcpStream::connect(address).await.unwrap());
    assert!(
        line(&mut excess).await.is_empty(),
        "bound socket bypassed handoff admission"
    );
    let mut excess = BufReader::new(handoff(&send).await);
    assert!(
        line(&mut excess).await.is_empty(),
        "handoff socket bypassed admission"
    );
    local.get_mut().write_all(b"PING\r\n").await.unwrap();
    assert_eq!(line(&mut local).await, "PONG\r\n");
    stop.send_replace(true);
    timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
