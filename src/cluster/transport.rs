//! Ordered, bounded peer writes with a nonblocking path for connected peers.
//! A partial frame owns the socket until the async writer sends its remainder.
use super::*;

const FAST_MESSAGE_BYTES: u32 = 32 * 1024;
const BACKGROUND_APPEND_DELAY: Duration = Duration::from_millis(4);

pub(super) trait MessageSender {
    fn try_send(&self, message: Message) -> Result<(), ()>;
    fn try_send_background(&self, message: Message) -> Result<(), ()> {
        self.try_send(message)
    }
}
#[cfg(test)]
impl MessageSender for tokio::sync::mpsc::Sender<Message> {
    fn try_send(&self, message: Message) -> Result<(), ()> {
        self.try_send(message).map_err(|_| ())
    }
}

struct Connection {
    stream: Arc<TcpStream>,
    used: Instant,
}
#[derive(Default)]
struct State {
    pending: usize,
    urgent: u64,
    streams: BTreeMap<u64, Connection>,
}
enum Write {
    Message(Message),
    Background {
        message: Message,
        deadline: Instant,
        urgent: u64,
    },
    Frame {
        to: u64,
        stream: Arc<TcpStream>,
        frame: Vec<u8>,
        written: usize,
    },
}
impl Write {
    fn to(&self) -> u64 {
        match self {
            Self::Message(message) | Self::Background { message, .. } => message.to,
            Self::Frame { to, .. } => *to,
        }
    }
}
pub(super) struct PeerSender {
    config: ClusterConfig,
    incarnation: [u8; 16],
    state: Arc<Mutex<State>>,
    send: tokio::sync::mpsc::Sender<Write>,
    urgent: Arc<tokio::sync::Notify>,
}
impl MessageSender for PeerSender {
    fn try_send(&self, message: Message) -> Result<(), ()> {
        let mut state = self.state.lock().unwrap();
        // A live writer and a reserved queue slot must precede any socket I/O:
        // if the write is partial, its remainder always has an ordered home.
        let permit = self.send.try_reserve().map_err(|_| ())?;
        let mut queued = Write::Message(message);
        if state.pending == 0 {
            if let Write::Message(message) = &queued {
                if message.get_msg_type() != raft::eraftpb::MessageType::MsgSnapshot
                    && message.compute_size() <= FAST_MESSAGE_BYTES
                {
                    if let Some(connection) = state.streams.get_mut(&message.to) {
                        let frame =
                            small_frame(&self.config, message, self.incarnation).map_err(|_| ())?;
                        match connection.stream.try_write(&frame) {
                            Ok(n) if n == frame.len() => {
                                connection.used = Instant::now();
                                return Ok(());
                            }
                            Ok(0) => {
                                state.streams.remove(&message.to);
                            }
                            Ok(written) => {
                                queued = Write::Frame {
                                    to: message.to,
                                    stream: connection.stream.clone(),
                                    frame,
                                    written,
                                };
                            }
                            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                                queued = Write::Frame {
                                    to: message.to,
                                    stream: connection.stream.clone(),
                                    frame,
                                    written: 0,
                                };
                            }
                            Err(_) => {
                                state.streams.remove(&message.to);
                            }
                        }
                    }
                }
            }
        }
        state.pending += 1;
        state.urgent = state.urgent.wrapping_add(1);
        permit.send(queued);
        drop(state);
        self.urgent.notify_one();
        Ok(())
    }
    fn try_send_background(&self, message: Message) -> Result<(), ()> {
        self.background(message, Instant::now() + BACKGROUND_APPEND_DELAY)
    }
}

fn small_frame(
    config: &ClusterConfig,
    message: &Message,
    incarnation: [u8; 16],
) -> io::Result<Vec<u8>> {
    let body = message.write_to_bytes().map_err(error)?;
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).map_err(error)?;
    message_chunk(
        config,
        &Chunk {
            nonce,
            offset: 0,
            total: body.len() as u64,
            hash: Sha256::digest(&body).into(),
            data: &body,
            snapshot: false,
        },
        incarnation,
    )
}
impl PeerSender {
    fn background(&self, message: Message, deadline: Instant) -> Result<(), ()> {
        let mut state = self.state.lock().unwrap();
        let permit = self.send.try_reserve().map_err(|_| ())?;
        let urgent = state.urgent;
        state.pending += 1;
        permit.send(Write::Background {
            message,
            deadline,
            urgent,
        });
        Ok(())
    }
    pub(super) fn start(
        config: ClusterConfig,
        address: SocketAddr,
        tx: mpsc::SyncSender<Input>,
        mut stop: watch::Receiver<bool>,
        incarnation: [u8; 16],
    ) -> Self {
        let (send, mut receive) = tokio::sync::mpsc::channel::<Write>(4);
        let state = Arc::new(Mutex::new(State::default()));
        let writer_state = state.clone();
        let writer_config = config.clone();
        let urgent = Arc::new(tokio::sync::Notify::new());
        let writer_urgent = urgent.clone();
        tokio::spawn(async move {
            loop {
                let write = tokio::select! {
                    _ = stop.changed() => break,
                    next = receive.recv() => { let Some(next) = next else { break }; next }
                };
                let write = if let Write::Background {
                    message,
                    deadline,
                    urgent,
                } = write
                {
                    loop {
                        let notified = writer_urgent.notified();
                        if writer_state.lock().unwrap().urgent != urgent {
                            break;
                        }
                        tokio::select! {
                            _ = stop.changed() => return,
                            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => break,
                            _ = notified => (),
                        }
                    }
                    Write::Message(message)
                } else {
                    write
                };
                let to = write.to();
                let snapshot = matches!(&write, Write::Message(message)
                    if message.get_msg_type() == raft::eraftpb::MessageType::MsgSnapshot);
                let result = tokio::select! {
                    _ = stop.changed() => break,
                    result = tokio::time::timeout(Duration::from_secs(30), async {
                        match write {
                            Write::Background {..} => unreachable!("background delay is resolved before writing"),
                            Write::Frame { stream, frame, written, .. } => {
                                write_peer_frame(&stream, &frame[written..]).await
                            }
                            Write::Message(message) => {
                                let existing = writer_state.lock().unwrap().streams.get(&to)
                                    .map(|connection| connection.stream.clone());
                                let stream = if let Some(stream) = existing { stream } else {
                                    let stream = Arc::new(TcpStream::connect(address).await?);
                                    stream.set_nodelay(true)?;
                                    let mut state = writer_state.lock().unwrap();
                                    // Replacement identities at the same physical address
                                    // retain separate authenticated streams, bounded to four.
                                    if state.streams.len() >= 4 {
                                        if let Some(oldest) = state.streams.iter()
                                            .min_by_key(|(_, connection)| connection.used)
                                            .map(|(id, _)| *id) {
                                            state.streams.remove(&oldest);
                                        }
                                    }
                                    state.streams.insert(to, Connection { stream: stream.clone(), used: Instant::now() });
                                    stream
                                };
                                send_message(&writer_config, &stream, &message, incarnation).await
                            }
                        }
                    }) => result,
                };
                let ok = matches!(result, Ok(Ok(())));
                {
                    let mut state = writer_state.lock().unwrap();
                    if ok {
                        if let Some(connection) = state.streams.get_mut(&to) {
                            connection.used = Instant::now();
                        }
                    } else {
                        // Never send a new frame after an incomplete failed one.
                        state.streams.remove(&to);
                    }
                    state.pending -= 1;
                }
                if snapshot {
                    let _ = tx.try_send(Input::SnapshotSent(to, ok));
                }
            }
        });
        Self {
            config,
            incarnation,
            state,
            send,
            urgent,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::tests::config;
    use raft::eraftpb::MessageType;

    fn heartbeat(index: u64) -> Message {
        Message {
            from: 1,
            to: 2,
            term: 1,
            index,
            msg_type: MessageType::MsgHeartbeat,
            ..Default::default()
        }
    }
    #[tokio::test]
    async fn urgent_traffic_releases_background_appends_without_reordering_or_lost_wakeups() {
        for (wait_for_writer, urgent_kind) in [
            (false, MessageType::MsgHeartbeat),
            (true, MessageType::MsgHeartbeat),
            (false, MessageType::MsgAppend),
            (true, MessageType::MsgAppend),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let (tx, _rx) = mpsc::sync_channel(4);
            let (stop, stopping) = watch::channel(false);
            let sender = PeerSender::start(
                config(1),
                listener.local_addr().unwrap(),
                tx,
                stopping,
                [7; 16],
            );
            let mut append = heartbeat(1);
            append.msg_type = MessageType::MsgAppend;
            append.entries.push(raft::eraftpb::Entry {
                index: 1,
                term: 1,
                ..Default::default()
            });
            sender
                .background(append, Instant::now() + Duration::from_secs(30))
                .unwrap();
            if wait_for_writer {
                assert!(
                    tokio::time::timeout(Duration::from_millis(20), listener.accept())
                        .await
                        .is_err()
                );
            }
            // Exercise both a pre-existing wakeup and a writer already waiting.
            let mut urgent = heartbeat(2);
            urgent.msg_type = urgent_kind;
            sender.try_send(urgent).unwrap();
            let (mut input, _) = tokio::time::timeout(Duration::from_secs(2), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let budget = Arc::new(Semaphore::new(MAX_FRAME));
            let (first, nonce, _) = receive_message(&config(2), &mut input, budget.clone(), None)
                .await
                .unwrap();
            assert_eq!(
                (first.get_msg_type(), first.index, nonce),
                (MessageType::MsgAppend, 1, [7; 16])
            );
            let (next, _, _) = receive_message(&config(2), &mut input, budget, None)
                .await
                .unwrap();
            assert_eq!((next.get_msg_type(), next.index), (urgent_kind, 2));
            stop.send_replace(true);
        }
    }
    #[tokio::test]
    async fn background_append_deadline_sends_without_any_urgent_message() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (tx, _rx) = mpsc::sync_channel(4);
        let (stop, stopping) = watch::channel(false);
        let sender = PeerSender::start(
            config(1),
            listener.local_addr().unwrap(),
            tx,
            stopping,
            [0; 16],
        );
        let mut append = heartbeat(1);
        append.msg_type = MessageType::MsgAppend;
        sender.try_send_background(append).unwrap();
        let (mut input, _) = tokio::time::timeout(Duration::from_secs(2), listener.accept())
            .await
            .unwrap()
            .unwrap();
        let (message, _, _) = receive_message(
            &config(2),
            &mut input,
            Arc::new(Semaphore::new(MAX_FRAME)),
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            (message.get_msg_type(), message.index),
            (MessageType::MsgAppend, 1)
        );
        stop.send_replace(true);
    }
    #[tokio::test]
    async fn partial_frame_finishes_before_queued_snapshot_and_small_message() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let stream = Arc::new(TcpStream::connect(address).await.unwrap());
        let (input, _) = listener.accept().await.unwrap();
        let (tx, _rx) = mpsc::sync_channel(4);
        let (stop, stopping) = watch::channel(false);
        let sender = PeerSender::start(config(1), address, tx, stopping, [7; 16]);
        let first = heartbeat(1);
        let frame = small_frame(&config(1), &first, [7; 16]).unwrap();
        let written = 13;
        write_peer_frame(&stream, &frame[..written]).await.unwrap();
        {
            let mut state = sender.state.lock().unwrap();
            state.streams.insert(
                2,
                Connection {
                    stream: stream.clone(),
                    used: Instant::now(),
                },
            );
            state.pending = 1;
            sender
                .send
                .try_send(Write::Frame {
                    to: 2,
                    stream,
                    frame,
                    written,
                })
                .unwrap();
        }
        let mut snapshot = Message {
            msg_type: MessageType::MsgSnapshot,
            ..heartbeat(2)
        };
        snapshot
            .mut_snapshot()
            .set_data(vec![3; 2 * 1024 * 1024].into());
        sender.try_send(snapshot.clone()).unwrap();
        let last = heartbeat(3);
        sender.try_send(last.clone()).unwrap();
        let mut input = BufReader::new(input);
        let budget = Arc::new(Semaphore::new(4 * 1024 * 1024));
        for expected in [first, snapshot, last] {
            let (message, nonce, _permit) = tokio::time::timeout(
                Duration::from_secs(5),
                receive_message(&config(2), &mut input, budget.clone(), None),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(message, expected);
            assert_eq!(nonce, [7; 16]);
        }
        while sender.state.lock().unwrap().pending != 0 {
            tokio::task::yield_now().await;
        }
        // An established idle stream sends synchronously without queueing work.
        sender.try_send(heartbeat(4)).unwrap();
        assert_eq!(sender.state.lock().unwrap().pending, 0);
        let (message, _, _) = receive_message(&config(2), &mut input, budget, None)
            .await
            .unwrap();
        assert_eq!(message.index, 4);
        stop.send_replace(true);
    }
    #[tokio::test]
    async fn bounded_queue_and_closed_writer_prevent_socket_bypass() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let stream = Arc::new(
            TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap(),
        );
        let (mut input, _) = listener.accept().await.unwrap();
        let (send, mut receive) = tokio::sync::mpsc::channel(4);
        let state = Arc::new(Mutex::new(State {
            pending: 1,
            ..Default::default()
        }));
        let sender = PeerSender {
            config: config(1),
            incarnation: [7; 16],
            state,
            send,
            urgent: Arc::new(tokio::sync::Notify::new()),
        };
        // Model a writer already holding the previous frame's remainder.
        sender.state.lock().unwrap().streams.insert(
            2,
            Connection {
                stream,
                used: Instant::now(),
            },
        );
        for n in 1..=4 {
            if n % 2 == 0 {
                sender.try_send_background(heartbeat(n)).unwrap();
            } else {
                sender.try_send(heartbeat(n)).unwrap();
            }
        }
        assert!(sender.try_send(heartbeat(5)).is_err());
        assert!(sender.try_send_background(heartbeat(5)).is_err());
        assert_eq!(sender.state.lock().unwrap().pending, 5);
        for n in 1..=4 {
            let (Write::Message(message) | Write::Background { message, .. }) =
                receive.try_recv().unwrap()
            else {
                panic!("out-of-order frame")
            };
            assert_eq!(message.index, n);
        }
        sender.state.lock().unwrap().pending = 0;
        drop(receive);
        assert!(sender.try_send(heartbeat(6)).is_err());
        assert!(sender.try_send_background(heartbeat(6)).is_err());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), input.read_u8())
                .await
                .is_err()
        );
    }
}
