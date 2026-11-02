//! One bounded, permanent owner for strict-mode storage. Requests carry owned
//! inputs and private completion channels; the worker never runs backend code.
//! Waiting adapters are deliberately serialized until the consensus event pump
//! exists. A disconnected executor is an ambiguous outcome, never durable success.
use crate::storage::{Append, CheckpointJob, Content, ContentKey, Record, Store, StoreError};
use bytes::Bytes;
use std::{io, sync::mpsc};

type Job = Box<dyn FnOnce(&mut dyn Store) + Send>;
pub struct ExecutorStore {
    jobs: Option<mpsc::SyncSender<Job>>,
    owner: Option<std::thread::JoinHandle<()>>,
    external: bool,
    checkpoints: bool,
}
impl ExecutorStore {
    pub fn start<S: Store + 'static>(mut store: S) -> Result<Self, StoreError> {
        if !store.durable_appends() || store.buffers_consumer_state() || !store.supports_batch() {
            return Err(StoreError::Invalid);
        }
        let external = store.external_content();
        let checkpoints = store.supports_checkpoint();
        let (jobs, incoming) = mpsc::sync_channel::<Job>(1);
        let owner = std::thread::Builder::new()
            .name("screech-storage-owner".into())
            .spawn(move || {
                while let Ok(job) = incoming.recv() {
                    job(&mut store);
                }
            })?;
        Ok(Self {
            jobs: Some(jobs),
            owner: Some(owner),
            external,
            checkpoints,
        })
    }
    fn call<R: Send + 'static>(
        &self,
        job: impl FnOnce(&mut dyn Store) -> Result<R, StoreError> + Send + 'static,
    ) -> Result<R, StoreError> {
        let (reply, completion) = mpsc::sync_channel(1);
        self.jobs
            .as_ref()
            .ok_or_else(stopped)?
            .send(Box::new(move |store| {
                let _ = reply.send(job(store));
            }))
            .map_err(|_| stopped())?;
        completion.recv().map_err(|_| stopped())?
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
        self.call(move |s| s.append(&record.subject, &record.headers, &record.payload))
    }
    fn append_batch(&mut self, records: &[Append]) -> Result<u64, StoreError> {
        let records = records.to_vec();
        self.call(move |s| s.append_batch(&records))
    }
    fn append_with_content(
        &mut self,
        records: &[Append],
        content: &[Content],
    ) -> Result<u64, StoreError> {
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
