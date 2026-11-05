//! One bounded, permanent owner for strict-mode storage. Requests carry owned
//! inputs and private completion channels. Physical I/O stays on this owner;
//! replication dispatch can go straight to its independently serialized owner.
//! A disconnected executor is an ambiguous outcome, never durable success.
use crate::storage::{
    Append, CheckpointJob, Content, ContentKey, ReadBarrierFuture, Record, Replication, Store,
    StoreError,
};
use bytes::Bytes;
use std::{
    io,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc,
    },
};

type Job = Box<dyn FnOnce(&mut dyn Store) + Send>;
pub struct ExecutorStore {
    jobs: Option<mpsc::SyncSender<Job>>,
    owner: Option<std::thread::JoinHandle<()>>,
    external: bool,
    checkpoints: bool,
    replicas: usize,
    replication: Option<Arc<dyn Replication>>,
    failed: Arc<AtomicBool>,
}
// A detached read dispatcher must retain the same owner-loss fence as calls
// through ExecutorStore. The guard runs on normal exit and panic alike.
struct OwnerFailure(Arc<AtomicBool>);
impl Drop for OwnerFailure {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
struct GuardedReplication {
    inner: Arc<dyn Replication>,
    failed: Arc<AtomicBool>,
}
impl GuardedReplication {
    fn ready(&self) -> Result<(), StoreError> {
        if self.failed.load(Ordering::Acquire) {
            Err(stopped())
        } else {
            Ok(())
        }
    }
}
impl Replication for GuardedReplication {
    fn read_barrier(&self) -> Result<(), StoreError> {
        self.ready()?;
        self.inner.read_barrier()?;
        self.ready()
    }
    fn read_barrier_async(&self) -> Option<ReadBarrierFuture> {
        let proof = self.inner.read_barrier_async()?;
        let failed = self.failed.clone();
        Some(Box::pin(async move {
            if failed.load(Ordering::Acquire) {
                return Err(stopped());
            }
            proof.await?;
            if failed.load(Ordering::Acquire) {
                return Err(stopped());
            }
            Ok(())
        }))
    }
    fn append_with_content(
        &self,
        records: &[Append],
        content: &[Content],
    ) -> Result<u64, StoreError> {
        self.ready()?;
        let result = self.inner.append_with_content(records, content)?;
        self.ready()?;
        Ok(result)
    }
    fn checkpoint_needed(&self) -> Result<bool, StoreError> {
        self.ready()?;
        let result = self.inner.checkpoint_needed()?;
        self.ready()?;
        Ok(result)
    }
}
impl ExecutorStore {
    pub fn start<S: Store + 'static>(mut store: S) -> Result<Self, StoreError> {
        if !store.durable_appends() || store.buffers_consumer_state() || !store.supports_batch() {
            return Err(StoreError::Invalid);
        }
        let external = store.external_content();
        let checkpoints = store.supports_checkpoint();
        let replicas = store.replicas();
        // Single-voter and standalone backends keep their existing owner path.
        let replication = if replicas > 1 {
            store.replication()
        } else {
            None
        };
        let failed = Arc::new(AtomicBool::new(false));
        let owner_failed = OwnerFailure(failed.clone());
        let (jobs, incoming) = mpsc::sync_channel::<Job>(1);
        let owner = std::thread::Builder::new()
            .name("screech-storage-owner".into())
            .spawn(move || {
                let _failure = owner_failed;
                while let Ok(job) = incoming.recv() {
                    job(&mut store);
                }
            })?;
        Ok(Self {
            jobs: Some(jobs),
            owner: Some(owner),
            external,
            checkpoints,
            replicas,
            replication,
            failed,
        })
    }
    fn call<R: Send + 'static>(
        &self,
        job: impl FnOnce(&mut dyn Store) -> Result<R, StoreError> + Send + 'static,
    ) -> Result<R, StoreError> {
        let lost = || {
            self.failed.store(true, Ordering::Release);
            stopped()
        };
        let (reply, completion) = mpsc::sync_channel(1);
        self.jobs
            .as_ref()
            .ok_or_else(lost)?
            .send(Box::new(move |store| {
                let _ = reply.send(job(store));
            }))
            .map_err(|_| lost())?;
        completion.recv().map_err(|_| lost())?
    }
    fn owner_ready(&self) -> Result<(), StoreError> {
        if self.failed.load(Ordering::Acquire)
            || self
                .owner
                .as_ref()
                .is_none_or(std::thread::JoinHandle::is_finished)
        {
            self.failed.store(true, Ordering::Release);
            Err(stopped())
        } else {
            Ok(())
        }
    }
    fn replicated<R>(
        &self,
        dispatch: impl FnOnce(&dyn Replication) -> Result<R, StoreError>,
    ) -> Option<Result<R, StoreError>> {
        self.replication.as_ref().map(|replication| {
            self.owner_ready()?;
            let result = dispatch(replication.as_ref());
            self.owner_ready()?;
            result
        })
    }
}
impl Drop for ExecutorStore {
    fn drop(&mut self) {
        // No call can still borrow this value. Close the inbox and wait for the
        // backend to release its exclusive file lock before reporting shutdown.
        self.jobs.take();
        if let Some(owner) = self.owner.take() {
            let _ = owner.join();
        }
    }
}
fn stopped() -> StoreError {
    io::Error::new(
        io::ErrorKind::BrokenPipe,
        "storage executor stopped; recovery required",
    )
    .into()
}
impl Store for ExecutorStore {
    fn replication(&self) -> Option<Arc<dyn Replication>> {
        Some(Arc::new(GuardedReplication {
            inner: self.replication.as_ref()?.clone(),
            failed: self.failed.clone(),
        }))
    }
    fn cluster_status(&self) -> Result<serde_json::Value, StoreError> {
        self.call(|s| s.cluster_status())
    }
    fn replace_voter(&self, old: u64, new: u64, nonce: [u8; 16]) -> Result<(), StoreError> {
        self.call(move |s| s.replace_voter(old, new, nonce))
    }
    fn checkpoint_needed(&self) -> Result<bool, StoreError> {
        self.replicated(|r| r.checkpoint_needed())
            .unwrap_or_else(|| self.call(|s| s.checkpoint_needed()))
    }
    fn replicas(&self) -> usize {
        self.replicas
    }
    fn read_barrier(&self) -> Result<(), StoreError> {
        self.replicated(|r| r.read_barrier())
            .unwrap_or_else(|| self.call(|s| s.read_barrier()))
    }
    fn durable_appends(&self) -> bool {
        true
    }
    fn supports_batch(&self) -> bool {
        true
    }
    fn external_content(&self) -> bool {
        self.external
    }
    fn supports_checkpoint(&self) -> bool {
        self.checkpoints
    }
    fn append(&mut self, subject: &str, headers: &[u8], payload: &[u8]) -> Result<u64, StoreError> {
        let record = Append {
            subject: subject.into(),
            headers: Bytes::copy_from_slice(headers),
            payload: Bytes::copy_from_slice(payload),
        };
        if let Some(result) =
            self.replicated(|r| r.append_with_content(std::slice::from_ref(&record), &[]))
        {
            return result;
        }
        self.call(move |s| s.append(&record.subject, &record.headers, &record.payload))
    }
    fn append_batch(&mut self, records: &[Append]) -> Result<u64, StoreError> {
        if let Some(result) = self.replicated(|r| r.append_with_content(records, &[])) {
            return result;
        }
        let records = records.to_vec();
        self.call(move |s| s.append_batch(&records))
    }
    fn append_with_content(
        &mut self,
        records: &[Append],
        content: &[Content],
    ) -> Result<u64, StoreError> {
        if let Some(result) = self.replicated(|r| r.append_with_content(records, content)) {
            return result;
        }
        let records = records.to_vec();
        let content = content.to_vec();
        self.call(move |s| s.append_with_content(&records, &content))
    }
    fn get(&self, sequence: u64) -> Result<Option<Record>, StoreError> {
        self.call(move |s| s.get(sequence))
    }
    fn last_sequence(&self) -> Result<u64, StoreError> {
        self.call(|s| s.last_sequence())
    }
    fn get_content(&self, key: ContentKey) -> Result<Option<Content>, StoreError> {
        self.call(move |s| s.get_content(key))
    }
    fn get_contents(&self, keys: &[ContentKey]) -> Result<Vec<Content>, StoreError> {
        let keys = keys.to_vec();
        self.call(move |s| s.get_contents(&keys))
    }
    fn validate_content(
        &self,
        key: ContentKey,
        subject: &str,
        headers: u64,
        payload: u64,
    ) -> Result<(), StoreError> {
        let subject = subject.to_owned();
        self.call(move |s| s.validate_content(key, &subject, headers, payload))
    }
    fn purge(&mut self) -> Result<(), StoreError> {
        self.call(|s| s.purge())
    }
    fn load_checkpoint(&self) -> Result<Option<(u64, Bytes)>, StoreError> {
        self.call(|s| s.load_checkpoint())
    }
    fn checkpoint(&mut self, sequence: u64, state: &[u8]) -> Result<(), StoreError> {
        let state = state.to_vec();
        self.call(move |s| s.checkpoint(sequence, &state))
    }
    fn checkpoint_with_content(
        &mut self,
        sequence: u64,
        state: &[u8],
        live: &mut dyn Iterator<Item = ContentKey>,
        imports: &[Content],
    ) -> Result<(), StoreError> {
        let state = state.to_vec();
        let live: Vec<_> = live.collect();
        let imports = imports.to_vec();
        self.call(move |s| {
            s.checkpoint_with_content(sequence, &state, &mut live.into_iter(), &imports)
        })
    }
    fn checkpoint_job(&mut self, sequence: u64, job: CheckpointJob) -> Result<(), StoreError> {
        self.call(move |s| s.checkpoint_job(sequence, job))
    }
}
