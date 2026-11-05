//! Read-only duplicate acknowledgements from the last published committed model.
//! Every reply still obtains a new context-bound quorum observation.
use super::*;
use crate::storage::{ReadBarrierFuture, Replication};
use std::{sync::OnceLock, time::Instant};

pub(super) struct DirectReads {
    state: RwLock<durable::State>,
    clock: OnceLock<(u64, Instant)>,
    replication: Arc<dyn Replication>,
    failed: Arc<std::sync::atomic::AtomicBool>,
}
impl DirectReads {
    pub(super) fn new(
        state: &durable::State,
        replication: Arc<dyn Replication>,
        failed: Arc<std::sync::atomic::AtomicBool>,
    ) -> Self {
        Self {
            state: RwLock::new(state.clone()),
            clock: OnceLock::new(),
            replication,
            failed,
        }
    }
    pub(super) fn healthy(&self) -> bool {
        !self.failed.load(std::sync::atomic::Ordering::Acquire)
    }
    pub(super) fn fail(&self) {
        self.failed
            .store(true, std::sync::atomic::Ordering::Release);
    }
    pub(super) fn clock(&self, base: u64, start: Instant) {
        let _ = self.clock.set((base, start));
    }
    pub(super) fn publish(&self, state: &durable::State) {
        *self.state.write().unwrap() = state.clone();
    }
    pub(super) fn prepare(&self, message: &Message) -> Option<(Outbound, ReadBarrierFuture)> {
        if !self.healthy()
            || message.headers.is_empty()
            || message.subject.starts_with("$JS.API.")
            || message.subject.starts_with("$JS.ACK.")
        {
            return None;
        }
        let reply = message.reply.as_ref()?;
        let (base, start) = self.clock.get()?;
        let state = self.state.read().unwrap();
        let time = base
            .saturating_add(start.elapsed().as_nanos().min(u64::MAX as u128) as u64)
            .max(state.time);
        // Expiration is a mutation and continues through the ordered worker.
        if state.expiration_needed(time) {
            return None;
        }
        let stream = state.streams.values().find(|stream| {
            stream
                .config
                .subjects
                .iter()
                .any(|filter| crate::core::matches(filter, &message.subject))
        })?;
        let sequence = stream.duplicate(&message.headers, time)?;
        let message = response(
            reply.clone(),
            json!({"stream":stream.config.name,"seq":sequence,"duplicate":true}),
        );
        let future = self.replication.read_barrier_async()?;
        Some((
            Outbound {
                target: reply.clone(),
                protocol_reply: true,
                queue: None,
                message,
            },
            future,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{Append, Content, MemoryStore, StoreError};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Proofs {
        calls: AtomicUsize,
        replies: Mutex<VecDeque<oneshot::Receiver<Result<(), StoreError>>>>,
    }
    type ProofReply = oneshot::Sender<Result<(), StoreError>>;
    type Setup = (
        Handle,
        Arc<Proofs>,
        Vec<ProofReply>,
        std_mpsc::Receiver<Outbound>,
    );
    impl Replication for Proofs {
        fn read_barrier(&self) -> Result<(), StoreError> {
            panic!("blocking read was used")
        }
        fn read_barrier_async(&self) -> Option<ReadBarrierFuture> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let receive = self.replies.lock().unwrap().pop_front().unwrap();
            Some(Box::pin(async move {
                receive.await.map_err(|_| StoreError::Invalid)?
            }))
        }
        fn append_with_content(&self, _: &[Append], _: &[Content]) -> Result<u64, StoreError> {
            Err(StoreError::Invalid)
        }
        fn checkpoint_needed(&self) -> Result<bool, StoreError> {
            Ok(false)
        }
    }
    fn model() -> durable::State {
        let mut state = durable::State::default();
        state
            .apply(
                state
                    .prepare_create(
                        StreamConfig {
                            name: "S".into(),
                            subjects: vec!["events".into()],
                            duplicate_window: 120_000_000_000,
                            ..Default::default()
                        },
                        0,
                    )
                    .unwrap(),
            )
            .unwrap();
        state
            .apply(
                state
                    .prepare_publish(
                        "S",
                        "events",
                        Bytes::from_static(b"NATS/1.0\r\nNats-Msg-Id: id\r\n\r\n"),
                        Bytes::from_static(b"committed"),
                        1,
                    )
                    .unwrap(),
            )
            .unwrap();
        state
    }
    fn duplicate() -> Message {
        Message {
            subject: "events".into(),
            reply: Some("reply".into()),
            headers: Bytes::from_static(b"NATS/1.0\r\nNats-Msg-Id: id\r\n\r\n"),
            payload: "ignored".into(),
        }
    }
    fn setup(count: usize) -> Setup {
        let (sends, receives): (Vec<_>, VecDeque<_>) =
            (0..count).map(|_| oneshot::channel()).unzip();
        let proofs = Arc::new(Proofs {
            calls: AtomicUsize::new(0),
            replies: Mutex::new(receives),
        });
        let mut handle = Handle::start(MemoryStore::default()).unwrap();
        assert!(
            handle.direct_reads.is_none(),
            "standalone enabled the cluster dispatcher"
        );
        let reads = Arc::new(DirectReads::new(
            &model(),
            proofs.clone(),
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
        ));
        reads.clock(1, Instant::now());
        handle.direct_reads = Some(reads);
        let (send, receive) = std_mpsc::channel();
        handle
            .attach_output(
                Arc::new(|_, _| true),
                Arc::new(move |out| send.send(out).map_err(|_| "test sink closed".into())),
            )
            .unwrap();
        (handle, proofs, sends, receive)
    }

    #[tokio::test]
    async fn each_duplicate_waits_for_its_own_successful_quorum_observation() {
        let (handle, proofs, sends, output) = setup(2);
        for (index, send) in sends.into_iter().enumerate() {
            let mut pending = handle.try_request(duplicate()).unwrap();
            assert_eq!(proofs.calls.load(Ordering::SeqCst), index + 1);
            assert!(matches!(
                pending.response.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
            assert!(matches!(
                output.try_recv(),
                Err(std_mpsc::TryRecvError::Empty)
            ));
            send.send(Ok(())).unwrap();
            assert!(pending.wait().await.unwrap());
            let result: Value =
                serde_json::from_slice(&output.try_recv().unwrap().message.payload).unwrap();
            assert_eq!(result, json!({"stream":"S","seq":1,"duplicate":true}));
        }
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn failed_quorum_never_releases_a_duplicate_ack() {
        let (handle, _, mut sends, output) = setup(1);
        let pending = handle.try_request(duplicate()).unwrap();
        sends.pop().unwrap().send(Err(StoreError::Invalid)).unwrap();
        assert!(pending.wait().await.is_err());
        assert!(matches!(
            output.try_recv(),
            Err(std_mpsc::TryRecvError::Empty)
        ));
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn storage_failure_during_a_read_fences_even_a_successful_quorum_reply() {
        let (handle, _, mut sends, output) = setup(1);
        let pending = handle.try_request(duplicate()).unwrap();
        handle.direct_reads.as_ref().unwrap().fail();
        sends.pop().unwrap().send(Ok(())).unwrap();
        assert!(pending.wait().await.is_err());
        assert!(matches!(
            output.try_recv(),
            Err(std_mpsc::TryRecvError::Empty)
        ));
        assert!(handle
            .direct_reads
            .as_ref()
            .unwrap()
            .prepare(&duplicate())
            .is_none());
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn queued_work_and_catalog_fences_keep_requests_on_the_ordered_worker() {
        let (handle, proofs, mut sends, output) = setup(1);
        handle.catalog_pending.store(1, Ordering::SeqCst);
        assert!(!handle
            .try_request(duplicate())
            .unwrap()
            .wait()
            .await
            .unwrap());
        handle.catalog_pending.store(0, Ordering::SeqCst);
        let preceding = handle.slots.clone().acquire_owned().await.unwrap();
        assert!(!handle
            .try_request(duplicate())
            .unwrap()
            .wait()
            .await
            .unwrap());
        drop(preceding);
        // A response can wake its caller before the worker drops Request's
        // lease. The next request may correctly take the conservative fallback
        // until that lease is released; synchronize before testing fast dispatch.
        timeout_for_lease(&handle).await;
        assert_eq!(proofs.calls.load(Ordering::SeqCst), 0);
        assert!(matches!(
            output.try_recv(),
            Err(std_mpsc::TryRecvError::Empty)
        ));
        let pending = handle.try_request(duplicate()).unwrap();
        sends.pop().unwrap().send(Ok(())).unwrap();
        assert!(pending.wait().await.unwrap());
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn disconnected_callers_keep_the_admission_lease_until_the_proof_finishes() {
        let (handle, proofs, mut sends, _) = setup(1);
        drop(handle.try_request(duplicate()).unwrap());
        assert_eq!(proofs.calls.load(Ordering::SeqCst), 1);
        assert_eq!(handle.slots.available_permits(), REQUEST_SLOTS - 1);
        sends.pop().unwrap().send(Ok(())).unwrap();
        timeout_for_lease(&handle).await;
        handle.shutdown().await;
    }
    async fn timeout_for_lease(handle: &Handle) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while handle.slots.available_permits() != REQUEST_SLOTS {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[test]
    fn unpublished_catalog_changes_and_expired_ids_cannot_authorize_a_reply() {
        let proofs = Arc::new(Proofs {
            calls: AtomicUsize::new(0),
            replies: Mutex::new(VecDeque::new()),
        });
        let mut state = model();
        let reads = DirectReads::new(
            &state,
            proofs.clone(),
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
        );
        assert!(
            reads.prepare(&duplicate()).is_none(),
            "clock was not initialized"
        );
        reads.clock(1, Instant::now());
        let generation = state.streams["S"].generation;
        state
            .apply(state.prepare_delete("S", false).unwrap())
            .unwrap();
        assert_eq!(
            reads.state.read().unwrap().streams["S"].generation,
            generation
        );
        reads.publish(&state);
        assert!(
            reads.prepare(&duplicate()).is_none(),
            "deleted stream supplied an ACK"
        );
        let expired = DirectReads::new(
            &model(),
            proofs.clone(),
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
        );
        expired.clock(u64::MAX, Instant::now());
        assert!(expired.prepare(&duplicate()).is_none());
        assert_eq!(proofs.calls.load(Ordering::SeqCst), 0);
    }
}
