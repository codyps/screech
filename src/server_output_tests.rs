use super::*;
use proptest::prelude::*;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

fn expected(
    subject: &str,
    reply: Option<&str>,
    headers: &[u8],
    body: &[u8],
    sid: &str,
    capable: bool,
) -> Vec<u8> {
    let reply = reply.map(|r| format!(" {r}")).unwrap_or_default();
    let mut wire = if capable && !headers.is_empty() {
        format!(
            "HMSG {subject} {sid}{reply} {} {}\r\n",
            headers.len(),
            headers.len() + body.len()
        )
        .into_bytes()
    } else {
        format!("MSG {subject} {sid}{reply} {}\r\n", body.len()).into_bytes()
    };
    if capable {
        wire.extend_from_slice(headers);
    }
    wire.extend_from_slice(body);
    wire.extend_from_slice(b"\r\n");
    wire
}
fn message(
    subject: &str,
    reply: Option<&str>,
    headers: &[u8],
    payload: &[u8],
    wire: bool,
) -> Arc<OutboundMessage> {
    if !wire {
        return Arc::new(OutboundMessage::Owned(Message {
            subject: subject.into(),
            reply: reply.map(str::to_owned),
            headers: Bytes::copy_from_slice(headers),
            payload: Bytes::copy_from_slice(payload),
        }));
    }
    let reply = reply.map(|r| format!(" {r}")).unwrap_or_default();
    let mut packet = if headers.is_empty() {
        format!("pUb  {subject}\t{reply} {}\r\n", payload.len()).into_bytes()
    } else {
        format!(
            "hPuB  {subject}\t{reply} {} {}\r\n",
            headers.len(),
            headers.len() + payload.len()
        )
        .into_bytes()
    };
    packet.extend_from_slice(headers);
    packet.extend_from_slice(payload);
    packet.extend_from_slice(b"\r\nPING\r\n");
    let mut input = BytesMut::from(packet.as_slice());
    let (command, tail) = parse(&input, Limits::default()).unwrap().unwrap();
    let consumed = input.len() - tail.len();
    let (subject, reply, hlen, plen) = match command {
        NatsMessage::Pub(p) => (
            field_range(&input, &p.subject),
            p.reply_to.as_deref().map(|r| field_range(&input, r)),
            0,
            p.payload.len(),
        ),
        NatsMessage::HPub(p) => (
            field_range(&input, &p.subject),
            p.reply_to.as_deref().map(|r| field_range(&input, r)),
            p.headers.len(),
            p.payload.len(),
        ),
        _ => unreachable!(),
    };
    let frame = input.split_to(consumed).freeze();
    // Reuse and grow the source buffer before any delivery is encoded.
    input.clear();
    input.extend_from_slice(&vec![0x5a; 131072]);
    Arc::new(OutboundMessage::Wire(WireMessage {
        frame,
        subject,
        reply,
        body: consumed - hlen - plen - 2,
        header_len: hlen,
    }))
}
fn frame(message: Arc<OutboundMessage>, sid: &str, headers: bool) -> Queued {
    Queued {
        size: wire_size(&message, sid, headers),
        data: FrameData::Delivery {
            message,
            sid: Arc::from(sid),
            headers,
        },
    }
}
fn compact_frame(message: Arc<OutboundMessage>, sid: &str, headers: bool) -> Queued {
    if let OutboundMessage::Wire(wire) = message.as_ref() {
        if let Ok(message) = CompactWire::new(wire.clone()) {
            return Queued {
                size: wire_size_parts(&message.parts(), sid, headers),
                data: FrameData::Wire {
                    message,
                    sid: Arc::from(sid),
                    headers,
                },
            };
        }
    }
    frame(message, sid, headers)
}
struct ShortWriter {
    output: Vec<u8>,
    maximum: usize,
    vectored: bool,
    interrupt: bool,
    interrupt_after_progress: bool,
    zero: bool,
    calls: usize,
}
impl ShortWriter {
    fn accept(&mut self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        self.calls += 1;
        if self.interrupt {
            self.interrupt = false;
            return Err(io::ErrorKind::Interrupted.into());
        }
        if self.interrupt_after_progress && !self.output.is_empty() {
            self.interrupt_after_progress = false;
            return Err(io::ErrorKind::Interrupted.into());
        }
        if self.zero {
            return Ok(0);
        }
        let mut left = self.maximum;
        for buf in bufs {
            let n = left.min(buf.len());
            self.output.extend_from_slice(&buf[..n]);
            left -= n;
            if left == 0 {
                break;
            }
        }
        Ok(self.maximum - left)
    }
}
impl AsyncWrite for ShortWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(self.accept(&[IoSlice::new(bytes)]))
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(self.accept(bufs))
    }
    fn is_write_vectored(&self) -> bool {
        self.vectored
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
fn writer(maximum: usize, vectored: bool) -> ShortWriter {
    ShortWriter {
        output: Vec::new(),
        maximum,
        vectored,
        interrupt: true,
        interrupt_after_progress: true,
        zero: false,
        calls: 0,
    }
}
proptest! {
    #[test]
    fn partial_output_refines_complete_wire_encoding(
        specs in prop::collection::vec((prop::collection::vec(any::<u8>(),0..4096),any::<bool>(),any::<bool>(),any::<bool>(),any::<bool>(),any::<bool>()),1..9),
        maximum in 1usize..2048, vectored in any::<bool>()
    ) {
        let runtime=tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let mut frames=Vec::new();let mut want=Vec::new();
            for (n,(payload,with_headers,capable,reply,wire,compact)) in specs.into_iter().enumerate() {
                let subject=if n%2==0 {"café.x"}else{"消息.test"};let sid=format!("consumer{n}");
                let headers=if with_headers {b"NATS/1.0\r\nX-Test: yes\r\n\r\n".as_slice()}else{b""};
                let reply=reply.then_some("inbox.réply");
                let msg=message(subject,reply,headers,&payload,wire);
                let next=expected(subject,reply,headers,&payload,&sid,capable);
                prop_assert_eq!(wire_size(&msg,&sid,capable),next.len());want.extend(next);
                frames.push(if compact {compact_frame(msg,&sid,capable)} else {frame(msg,&sid,capable)});
                frames.push(Queued{size:6,data:FrameData::Raw(Bytes::from_static(b"PONG\r\n"))});want.extend_from_slice(b"PONG\r\n");
            }
            let mut data=Vec::new();let mut parts=Vec::new();prepare_batch(&mut data,&mut parts,&mut frames);
            let mut output=writer(maximum,vectored);write_batch(&mut output,&data,&parts,&frames).await.unwrap();
            prop_assert_eq!(output.output,want);Ok(())
        })?;
    }
}
#[tokio::test]
async fn large_headers_empty_bodies_and_many_vectors_preserve_framing() {
    for capable in [false, true] {
        let mut frames = Vec::new();
        let mut want = Vec::new();
        let headers = [
            b"NATS/1.0\r\nX: ".as_slice(),
            &vec![b'x'; 2048],
            b"\r\n\r\n",
        ]
        .concat();
        for n in 0..320 {
            let payload = vec![n as u8; if n % 2 == 0 { 0 } else { 65537 }];
            let msg = message("subject", Some("reply"), &headers, &payload, n % 2 == 0);
            want.extend(expected(
                "subject",
                Some("reply"),
                &headers,
                &payload,
                "1",
                capable,
            ));
            frames.push(if n % 3 == 0 {
                compact_frame(msg, "1", capable)
            } else {
                frame(msg, "1", capable)
            });
        }
        let mut data = Vec::new();
        let mut parts = Vec::new();
        prepare_batch(&mut data, &mut parts, &mut frames);
        assert!(parts.len() > WRITE_IOV_LIMIT);
        assert!(data.len() < want.len() / 2);
        let mut output = writer(100000, true);
        write_batch(&mut output, &data, &parts, &frames)
            .await
            .unwrap();
        assert_eq!(output.output, want);
    }
}
#[tokio::test]
async fn zero_writes_fail_and_empty_batches_succeed() {
    let mut frames = vec![frame(
        message("s", None, b"", &vec![0; 1024], true),
        "1",
        true,
    )];
    let mut data = Vec::new();
    let mut parts = Vec::new();
    prepare_batch(&mut data, &mut parts, &mut frames);
    let mut output = writer(100, true);
    output.zero = true;
    assert_eq!(
        write_batch(&mut output, &data, &parts, &frames)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::WriteZero
    );
    write_batch(&mut output, &[], &[], &[]).await.unwrap();
}
#[tokio::test]
async fn borrowed_body_keeps_inflight_budget_until_write_completes() {
    let body = vec![0xa5; 65537];
    let msg = message("s", Some("r"), b"", &body, true);
    let want = expected("s", Some("r"), b"", &body, "1", true);
    let limit = want.len();
    let (cancel, _) = watch::channel(false);
    let peer = Peer {
        out: Arc::new(Mutex::new(Outbox::default())),
        ready: Arc::new(Notify::new()),
        max_pending: limit,
        cancel,
        headers: true,
        socket: None,
    };
    let wire = match Arc::try_unwrap(msg) {
        Ok(OutboundMessage::Wire(wire)) => wire,
        _ => panic!("exclusive wire"),
    };
    let mut wakes = DeliveryWakes::default();
    assert!(peer.send_wire(wire, Arc::from("1"), &mut wakes));
    drop(wakes);
    let (write, mut read) = tokio::io::duplex(17);
    let task = tokio::spawn(writer_loop(write, peer.clone(), Duration::from_secs(5)));
    timeout(Duration::from_secs(1), async {
        loop {
            if peer.out.lock().unwrap().frames.is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    {
        let out = peer.out.lock().unwrap();
        assert_eq!(out.bytes, limit);
        assert_eq!(out.count, 1);
    }
    assert!(!peer.send(Bytes::from_static(b"PONG\r\n")));
    peer.close();
    let mut got = Vec::new();
    read.read_to_end(&mut got).await.unwrap();
    task.await.unwrap();
    assert_eq!(got, want);
    let out = peer.out.lock().unwrap();
    assert_eq!(out.bytes, 0);
    assert_eq!(out.count, 0);
}
#[tokio::test]
async fn stalled_partial_write_closes_without_releasing_inflight_credit() {
    let msg = message("s", None, b"", &vec![0; 4096], true);
    let limit = wire_size(&msg, "1", true);
    let (cancel, mut canceled) = watch::channel(false);
    let peer = Peer {
        out: Arc::new(Mutex::new(Outbox::default())),
        ready: Arc::new(Notify::new()),
        max_pending: limit,
        cancel,
        headers: true,
        socket: None,
    };
    let wire = match Arc::try_unwrap(msg) {
        Ok(OutboundMessage::Wire(wire)) => wire,
        _ => panic!("exclusive wire"),
    };
    let mut wakes = DeliveryWakes::default();
    assert!(peer.send_wire(wire, Arc::from("1"), &mut wakes));
    drop(wakes);
    let (write, _unread) = tokio::io::duplex(17);
    writer_loop(write, peer.clone(), Duration::from_millis(20)).await;
    canceled.changed().await.unwrap();
    assert!(*canceled.borrow());
    let out = peer.out.lock().unwrap();
    assert!(out.closed);
    assert_eq!(out.bytes, limit);
    assert_eq!(out.count, 1);
}

#[test]
fn retained_small_frames_reuse_available_receive_storage() {
    let mut input = BytesMut::with_capacity(64 * 1024);
    let mut retained = Vec::new();
    for n in 0..200 {
        let tail = input.as_ptr();
        reserve_input(&mut input, 64 * 1024);
        assert_eq!(
            input.as_ptr(),
            tail,
            "adequate shared tail space was reallocated"
        );
        input.extend_from_slice(&[n as u8; 128]);
        retained.push(input.split_to(128).freeze());
    }
    for (n, frame) in retained.iter().enumerate() {
        assert!(frame.iter().all(|b| *b == n as u8));
    }
}

#[tokio::test]
async fn deferred_wakes_keep_admission_limits_and_wake_on_early_return() {
    let (cancel, _) = watch::channel(false);
    let peer = Peer {
        out: Arc::new(Mutex::new(Outbox::default())),
        ready: Arc::new(Notify::new()),
        max_pending: 4096,
        cancel,
        headers: true,
        socket: None,
    };
    // Dropping the burst guard is also the error/return path. No write task is
    // notified while routing; accepted frames and their budget remain intact.
    {
        let mut wakes = DeliveryWakes::default();
        let msg = message("s", None, b"", &vec![0xab; 1024], true);
        assert!(peer.send_delivery(msg.clone(), Arc::from("1"), Some(&mut wakes)));
        assert!(peer.send_delivery(msg.clone(), Arc::from("1"), Some(&mut wakes)));
        assert_eq!(wakes.0.len(), 1);
        assert!(timeout(Duration::from_millis(5), peer.ready.notified())
            .await
            .is_err());
        assert!(peer.send_delivery(msg.clone(), Arc::from("1"), Some(&mut wakes)));
        assert!(!peer.send_delivery(msg, Arc::from("1"), Some(&mut wakes)));
        assert_eq!(peer.out.lock().unwrap().count, 3);
    }
    timeout(Duration::from_millis(100), peer.ready.notified())
        .await
        .unwrap();
    assert_eq!(peer.out.lock().unwrap().count, 3);
}

#[test]
fn jetstream_text_boundary_rejects_binary_subjects_and_replies_without_panicking() {
    for (subject, reply) in [
        (b"bad.\xff".as_slice(), b"reply".as_slice()),
        (b"subject", b"bad.\xfe"),
    ] {
        let mut input = BytesMut::new();
        input.extend_from_slice(b"PUB ");
        input.extend_from_slice(subject);
        input.extend_from_slice(b" ");
        input.extend_from_slice(reply);
        input.extend_from_slice(b" 0\r\n\r\n");
        let (msg, tail) = parse(&input, Limits::default()).unwrap().unwrap();
        assert!(tail.is_empty());
        let NatsMessage::Pub(p) = msg else {
            panic!("PUB")
        };
        let subject = field_range(&input, &p.subject);
        let reply = p.reply_to.as_deref().map(|r| field_range(&input, r));
        let body = input.len() - 2;
        let wire = WireMessage {
            frame: input.freeze(),
            subject,
            reply,
            body,
            header_len: 0,
        };
        assert_eq!(
            wire.to_owned_message().unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }
}

#[tokio::test]
async fn oversized_compact_offsets_fall_back_without_truncating_fields() {
    for large_reply in [false, true] {
        let subject = if large_reply {
            "s".into()
        } else {
            "s".repeat(65536)
        };
        let reply = if large_reply {
            "r".repeat(65536)
        } else {
            "r".into()
        };
        let mut input = BytesMut::from(format!("PUB {subject} {reply} 0\r\n\r\n").as_bytes());
        let (msg, tail) = parse(
            &input,
            Limits {
                control_line: 131072,
                payload: 0,
            },
        )
        .unwrap()
        .unwrap();
        assert!(tail.is_empty());
        let NatsMessage::Pub(p) = msg else {
            panic!("PUB")
        };
        let subject_range = field_range(&input, &p.subject);
        let reply_range = p.reply_to.as_deref().map(|r| field_range(&input, r));
        let body = input.len() - 2;
        let wire = WireMessage {
            frame: input.split().freeze(),
            subject: subject_range,
            reply: reply_range,
            body,
            header_len: 0,
        };
        let (cancel, _) = watch::channel(false);
        let peer = Peer {
            out: Arc::new(Mutex::new(Outbox::default())),
            ready: Arc::new(Notify::new()),
            max_pending: 262144,
            cancel,
            headers: true,
            socket: None,
        };
        let mut wakes = DeliveryWakes::default();
        assert!(peer.send_wire(wire, Arc::from("1"), &mut wakes));
        let mut frames: Vec<_> = peer.out.lock().unwrap().frames.drain(..).collect();
        let mut data = Vec::new();
        let mut parts = Vec::new();
        prepare_batch(&mut data, &mut parts, &mut frames);
        let mut output = writer(137, true);
        write_batch(&mut output, &data, &parts, &frames)
            .await
            .unwrap();
        assert_eq!(
            output.output,
            expected(&subject, Some(&reply), b"", b"", "1", true)
        );
    }
}

fn test_wire(subject: &str, headers: &[u8], payload: &[u8]) -> WireMessage {
    match Arc::try_unwrap(message(subject, None, headers, payload, true)) {
        Ok(OutboundMessage::Wire(wire)) => wire,
        _ => panic!("exclusive wire message"),
    }
}
#[derive(Debug, PartialEq, Eq)]
struct PeerSnapshot {
    bytes: usize,
    count: usize,
    closed: bool,
    canceled: bool,
    frames: Vec<Vec<u8>>,
}
fn peer_snapshots(peers: &[Peer]) -> Vec<PeerSnapshot> {
    peers
        .iter()
        .map(|peer| {
            let out = peer.out.lock().unwrap();
            let frames = out
                .frames
                .iter()
                .map(|frame| {
                    let mut bytes = Vec::new();
                    match &frame.data {
                        FrameData::Raw(raw) => bytes.extend_from_slice(raw),
                        FrameData::Wire { sid, headers, .. }
                        | FrameData::Delivery { sid, headers, .. } => {
                            encode_into(&mut bytes, &frame.data.parts(), sid, *headers)
                        }
                    }
                    bytes
                })
                .collect();
            PeerSnapshot {
                bytes: out.bytes,
                count: out.count,
                closed: out.closed,
                canceled: *peer.cancel.borrow(),
                frames,
            }
        })
        .collect()
}
fn burst_hub(
    specs: &[(bool, bool, u8, bool)],
    subscriptions: &[u8],
    echo: bool,
) -> (Hub, Vec<Peer>) {
    let mut hub = Hub {
        core: Core::default(),
        peers: HashMap::new(),
    };
    let mut peers = Vec::new();
    for (client, &(headers, closed, budget, inflight)) in specs.iter().enumerate() {
        let (cancel, _) = watch::channel(false);
        let count = if inflight { 4095 } else { 0 };
        let peer = Peer {
            out: Arc::new(Mutex::new(Outbox {
                count,
                bytes: count * 6,
                closed,
                ..Outbox::default()
            })),
            ready: Arc::new(Notify::new()),
            max_pending: if inflight {
                65536
            } else {
                [0, 1024, 4096, 65536, 262144][usize::from(budget) % 5]
            },
            cancel,
            headers,
            socket: None,
        };
        hub.core.connect(client as u64, echo || client != 0);
        if budget != 4 || !closed {
            hub.peers.insert(client as u64, peer.clone());
        }
        peers.push(peer);
    }
    for (n, &code) in subscriptions.iter().enumerate() {
        let client = (n / 2) as u64;
        let sid = (n % 2).to_string();
        let filter =
            ["a", "a.*", "a.>", ">", "*.*", "b.>", "café.*", "消息.>"][usize::from(code) % 8];
        let queue = (code & 8 != 0).then_some(if code & 16 != 0 { "q" } else { "other" });
        hub.core
            .subscribe(client, sid.clone(), filter.into(), queue.map(str::to_owned))
            .unwrap();
        if code & 32 != 0 {
            hub.core
                .unsubscribe(client, &sid, Some(u64::from(code % 7 + 1)));
        }
    }
    (hub, peers)
}
proptest! {
    #[test]
    fn core_burst_refines_serial_publications_through_admission_and_handover(
        peers in prop::collection::vec((any::<bool>(),any::<bool>(),0u8..5,any::<bool>()),4),
        subscriptions in prop::collection::vec(any::<u8>(),8),
        specs in prop::collection::vec((0usize..6,prop::collection::vec(any::<u8>(),0..4096),any::<bool>()),0..80),
        echo in any::<bool>(),
    ) {
        let (mut serial, serial_peers) = burst_hub(&peers, &subscriptions, echo);
        let (mut batched, batch_peers) = burst_hub(&peers, &subscriptions, echo);
        let mut wires = VecDeque::new();
        let mut serial_wakes = DeliveryWakes::default();
        for (subject, payload, headers) in specs {
            let subject = ["a","a.x","a.x.y","b.y","café.x","消息.x"][subject];
            let headers = if headers {b"NATS/1.0\r\n\r\n".as_slice()} else {b""};
            let wire = test_wire(subject, headers, &payload);
            serial.forward_wire(0, subject.as_bytes(), wire.clone(), &mut serial_wakes);
            wires.push_back(wire);
        }
        let mut batch_wakes = DeliveryWakes::default();
        while !wires.is_empty() { batched.forward_wire_burst(0, &mut wires, &mut batch_wakes); }
        prop_assert!(wires.is_empty());
        prop_assert_eq!(peer_snapshots(&serial_peers), peer_snapshots(&batch_peers));
        for client in 0..4 {
            prop_assert_eq!(serial.core.subscription_count(client), batched.core.subscription_count(client));
        }
        // Future routes expose cursor and auto-unsubscribe differences as well.
        for subject in ["a", "a.x", "a.x.y", "b.y", "café.x", "消息.x"] {
            prop_assert_eq!(serial.core.publish(0, subject), batched.core.publish(0, subject));
        }
    }
}

#[test]
fn collector_preserves_control_boundaries_partial_frames_and_budget() {
    let first = test_wire("a", b"", b"x");
    let following = b"PUB a 1\r\ny\r\nHPUB a 12 13\r\nNATS/1.0\r\n\r\nz\r\n";
    for boundary in [
        b"PING\r\n".as_slice(),
        b"PUB a reply 0\r\n\r\n",
        b"PUB a 2\r\nx",
        b"PUB a..b 0\r\n\r\n",
        b"INVALID\r\n",
    ] {
        let mut input = BytesMut::from([following.as_slice(), boundary].concat().as_slice());
        let mut wires = VecDeque::from([first.clone()]);
        collect_core_burst(&mut input, Limits::default(), 256, usize::MAX, &mut wires);
        assert_eq!(wires.len(), 3);
        assert_eq!(&input[..], boundary);
        assert_eq!(
            &wires[1].frame[wires[1].body..wires[1].frame.len() - 2],
            b"y"
        );
        assert_eq!(wires[2].header_len, 12);
    }
    let mut input = BytesMut::from(following.as_slice());
    let mut wires = VecDeque::from([first.clone()]);
    collect_core_burst(&mut input, Limits::default(), 2, usize::MAX, &mut wires);
    assert_eq!(wires.len(), 2);
    assert!(input.starts_with(b"HPUB"));
    let mut input = BytesMut::from(following.as_slice());
    let mut wires = VecDeque::from([first]);
    collect_core_burst(&mut input, Limits::default(), 256, 4096, &mut wires);
    assert_eq!(wires.len(), 1);
    assert_eq!(&input[..], following);
    let mut input = BytesMut::from(b"PUB a.\xff 0\r\n\r\n".as_slice());
    let mut wires = VecDeque::from([test_wire("a", b"", b"")]);
    collect_core_burst(&mut input, Limits::default(), 256, usize::MAX, &mut wires);
    assert_eq!(wires.len(), 2);
    assert!(input.is_empty());
    assert_eq!(&wires[1].frame[wires[1].subject.clone()], b"a.\xff");
}

fn protocol_reply(body: &[u8], headers: bool) -> Queued {
    queued_delivery(
        Arc::new(OutboundMessage::Reply(Message {
            subject: "_INBOX.reply".into(),
            reply: Some("next".into()),
            headers: Bytes::from_static(b"NATS/1.0 200 OK\r\n\r\n"),
            payload: Bytes::copy_from_slice(body),
        })),
        Arc::from("19"),
        headers,
    )
}

proptest! {
    #[test]
    fn immediate_reply_partial_writes_preserve_every_wire_byte(
        body in prop::collection::vec(any::<u8>(), 0..129), headers in any::<bool>(),
    ) {
        let complete = expected("_INBOX.reply", Some("next"), b"NATS/1.0 200 OK\r\n\r\n", &body, "19", headers);
        for split in 1..=complete.len() {
            let mut frame = protocol_reply(&body, headers);
            let mut scratch = Vec::new();
            let done = try_small_reply(&mut frame, &mut scratch, |wire| {
                assert_eq!(wire, complete);
                Ok(split)
            }).unwrap();
            prop_assert_eq!(done, split == complete.len());
            if !done {
                let FrameData::Raw(suffix) = &frame.data else { panic!("raw suffix"); };
                prop_assert_eq!(frame.size, complete.len() - split);
                prop_assert_eq!(suffix.as_ref(), &complete[split..]);
            }
        }
    }
}

#[test]
fn immediate_reply_retry_errors_and_delivery_coalescing() {
    let mut scratch = Vec::new();
    for kind in [io::ErrorKind::WouldBlock, io::ErrorKind::Interrupted] {
        let mut frame = protocol_reply(b"ok", true);
        let original_size = frame.size;
        assert!(!try_small_reply(&mut frame, &mut scratch, |_| Err(kind.into())).unwrap());
        assert_eq!(frame.size, original_size);
        assert!(matches!(frame.data, FrameData::Delivery { .. }));
    }
    for (result, kind) in [
        (Ok::<usize, io::Error>(0), io::ErrorKind::WriteZero),
        (Ok(usize::MAX), io::ErrorKind::InvalidData),
        (
            Err(io::ErrorKind::BrokenPipe.into()),
            io::ErrorKind::BrokenPipe,
        ),
    ] {
        assert_eq!(
            try_small_reply(&mut protocol_reply(b"ok", true), &mut scratch, |_| {
                match &result {
                    Ok(n) => Ok(*n),
                    Err(e) => Err(e.kind().into()),
                }
            })
            .unwrap_err()
            .kind(),
            kind
        );
    }
    for mut frame in [
        protocol_reply(&[0; 513], true),
        queued_delivery(
            message("subject", None, &[], b"body", false),
            Arc::from("1"),
            false,
        ),
        Queued {
            size: 4,
            data: FrameData::Raw(Bytes::from_static(b"PING")),
        },
    ] {
        assert!(
            !try_small_reply(&mut frame, &mut scratch, |_| panic!("must stay queued")).unwrap()
        );
    }
}

#[tokio::test]
async fn immediate_reply_respects_inflight_credit_closed_state_and_budget() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (socket, _) = listener.accept().await.unwrap();
    let (_, write) = socket.into_split();
    write.writable().await.unwrap();
    let (cancel, mut canceled) = watch::channel(false);
    let peer = Peer {
        out: Arc::new(Mutex::new(Outbox::default())),
        ready: Arc::new(Notify::new()),
        max_pending: 1024,
        cancel,
        headers: true,
        socket: Some(SharedSocketWrite(Arc::new(Mutex::new(write)))),
    };
    // A batch removed from frames remains in flight until its complete write.
    peer.out.lock().unwrap().count = 1;
    assert!(peer.enqueue(protocol_reply(b"queued", true)));
    assert_eq!(peer.out.lock().unwrap().count, 2);
    let mut data = [0; 512];
    assert_eq!(
        client.try_read(&mut data).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    *peer.out.lock().unwrap() = Outbox::default();
    assert!(peer.enqueue(protocol_reply(b"direct", true)));
    assert_eq!(peer.out.lock().unwrap().count, 0);
    client.readable().await.unwrap();
    let n = client.try_read(&mut data).unwrap();
    assert_eq!(
        &data[..n],
        expected(
            "_INBOX.reply",
            Some("next"),
            b"NATS/1.0 200 OK\r\n\r\n",
            b"direct",
            "19",
            true
        )
    );
    let mut limited = peer.clone();
    limited.max_pending = 1;
    assert!(!limited.enqueue(protocol_reply(b"over budget", true)));
    assert!(*canceled.borrow_and_update());
    peer.close();
    assert!(!peer.enqueue(protocol_reply(b"closed", true)));
    assert_eq!(peer.out.lock().unwrap().count, 0);
    assert_eq!(
        client.try_read(&mut data).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
}
