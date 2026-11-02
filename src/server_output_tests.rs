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
        specs in prop::collection::vec((prop::collection::vec(any::<u8>(),0..4096),any::<bool>(),any::<bool>(),any::<bool>(),any::<bool>()),1..9),
        maximum in 1usize..2048, vectored in any::<bool>()
    ) {
        let runtime=tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let mut frames=Vec::new();let mut want=Vec::new();
            for (n,(payload,with_headers,capable,reply,wire)) in specs.into_iter().enumerate() {
                let subject=if n%2==0 {"café.x"}else{"消息.test"};let sid=format!("consumer{n}");
                let headers=if with_headers {b"NATS/1.0\r\nX-Test: yes\r\n\r\n".as_slice()}else{b""};
                let reply=reply.then_some("inbox.réply");
                let msg=message(subject,reply,headers,&payload,wire);
                let next=expected(subject,reply,headers,&payload,&sid,capable);
                prop_assert_eq!(wire_size(&msg,&sid,capable),next.len());want.extend(next);
                frames.push(frame(msg,&sid,capable));
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
        for n in 0..160 {
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
            frames.push(frame(msg, "1", capable));
        }
        let mut data = Vec::new();
        let mut parts = Vec::new();
        prepare_batch(&mut data, &mut parts, &mut frames);
        assert!(parts.len() > 128);
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
    };
    assert!(peer.send_delivery(msg, Arc::from("1"), None));
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
    };
    assert!(peer.send_delivery(msg, Arc::from("1"), None));
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
