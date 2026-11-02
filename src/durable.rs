//! Deterministic, versioned durable transitions. No clock, socket, runtime or SQLite.
use crate::{
    core::matches,
    parser::subject,
    storage::{Content, ContentKey, Store, StoreError},
};
use bytes::Bytes;
use imbl::{OrdMap, OrdSet};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum Retention {
    #[default]
    Limits,
    Interest,
    Workqueue,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum Discard {
    #[default]
    Old,
    New,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct StreamConfig {
    pub name: String,
    pub subjects: Vec<String>,
    pub retention: Retention,
    pub discard: Discard,
    pub max_msgs: i64,
    pub max_bytes: i64,
    pub max_msgs_per_subject: i64,
    pub max_msg_size: i64,
    pub max_age: u64,
    pub duplicate_window: u64,
    pub max_consumers: i64,
}
impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            subjects: vec![],
            retention: Retention::Limits,
            discard: Discard::Old,
            max_msgs: -1,
            max_bytes: -1,
            max_msgs_per_subject: -1,
            max_msg_size: -1,
            max_age: 0,
            duplicate_window: 0,
            max_consumers: -1,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum AckPolicy {
    None,
    All,
    #[default]
    Explicit,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum DeliverPolicy {
    #[default]
    All,
    New,
    Last,
    ByStartSequence,
    ByStartTime,
    LastPerSubject,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ConsumerConfig {
    pub name: String,
    pub durable_name: Option<String>,
    pub description: String,
    pub deliver_subject: Option<String>,
    pub deliver_group: Option<String>,
    pub ack_policy: AckPolicy,
    pub ack_wait: u64,
    pub max_deliver: i64,
    pub backoff: Vec<u64>,
    pub filter_subject: String,
    pub filter_subjects: Vec<String>,
    pub deliver_policy: DeliverPolicy,
    pub opt_start_seq: u64,
    pub opt_start_time: Option<String>,
    pub max_ack_pending: i64,
    pub max_waiting: usize,
    pub max_batch: usize,
    pub max_expires: u64,
    pub max_bytes: usize,
}
impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            durable_name: None,
            description: String::new(),
            deliver_subject: None,
            deliver_group: None,
            ack_policy: AckPolicy::Explicit,
            ack_wait: 30_000_000_000,
            max_deliver: -1,
            backoff: vec![],
            filter_subject: String::new(),
            filter_subjects: vec![],
            deliver_policy: DeliverPolicy::All,
            opt_start_seq: 0,
            opt_start_time: None,
            max_ack_pending: 1000,
            max_waiting: 512,
            max_batch: 1000,
            max_expires: 0,
            max_bytes: 0,
        }
    }
}
impl ConsumerConfig {
    pub fn accepts(&self, s: &str) -> bool {
        if !self.filter_subjects.is_empty() {
            self.filter_subjects.iter().any(|f| matches(f, s))
        } else {
            self.filter_subject.is_empty() || matches(&self.filter_subject, s)
        }
    }
    pub fn wait(&self, attempt: u64) -> u64 {
        if self.backoff.is_empty() {
            self.ack_wait
        } else {
            self.backoff[(attempt.saturating_sub(1) as usize).min(self.backoff.len() - 1)]
        }
    }
    fn filters(&self) -> Vec<&str> {
        if !self.filter_subjects.is_empty() {
            self.filter_subjects.iter().map(String::as_str).collect()
        } else {
            vec![if self.filter_subject.is_empty() {
                ">"
            } else {
                &self.filter_subject
            }]
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContentRef {
    pub key: ContentKey,
    pub headers_len: u64,
    pub payload_len: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredMessage {
    pub sequence: u64,
    pub subject: String,
    pub headers: Bytes,
    pub payload: Bytes,
    pub time: u64,
    #[serde(default)]
    pub content: Option<ContentRef>,
}
impl StoredMessage {
    pub fn size(&self) -> u64 {
        let (headers, payload) = self
            .content
            .as_ref()
            .map(|r| (r.headers_len, r.payload_len))
            .unwrap_or((self.headers.len() as u64, self.payload.len() as u64));
        self.subject.len() as u64 + headers + payload + 30 + if headers == 0 { 0 } else { 4 }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Delivery {
    pub sequence: u64,
    pub consumer_sequence: u64,
    pub attempt: u64,
    pub deadline: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AckToken {
    pub generation: u64,
    pub sequence: u64,
    pub consumer_sequence: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum AckAction {
    Ack,
    Nak(u64),
    Progress,
    Term,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Event {
    ConsumerUpdate {
        stream: String,
        config: ConsumerConfig,
        generation: u64,
        time: u64,
    },
    Update {
        config: StreamConfig,
        time: u64,
    },
    Remove {
        stream: String,
        sequences: Vec<u64>,
    },
    Create {
        config: StreamConfig,
        time: u64,
    },
    Delete {
        name: String,
    },
    Purge {
        name: String,
    },
    Publish {
        stream: String,
        message: StoredMessage,
    },
    ConsumerCreate {
        stream: String,
        config: ConsumerConfig,
        time: u64,
        generation: u64,
    },
    ConsumerDelete {
        stream: String,
        name: String,
        generation: u64,
    },
    Deliver {
        stream: String,
        consumer: String,
        generation: u64,
        time: u64,
        delivery: Delivery,
    },
    Ack {
        stream: String,
        consumer: String,
        generation: u64,
        sequence: u64,
        consumer_sequence: u64,
        time: u64,
        action: AckAction,
    },
    Expire {
        time: u64,
    },
    /// One durable reservation boundary for at most 64 deliveries to one consumer.
    DeliverBatch {
        stream: String,
        consumer: String,
        generation: u64,
        time: u64,
        deliveries: Vec<Delivery>,
    },
    /// Only ACK events, in ingress order; no lifecycle or delivery changes inside.
    AckBatch {
        events: Vec<Event>,
    },
    /// A client's ACK-and-next operation, with both effects in one durable record.
    AckNext {
        ack: Box<Event>,
        next: Box<Event>,
    },
}
#[derive(Default, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
// Catalogs are small mutable maps; growing indexes below are persistent trees.
// Cloned projections share tree nodes and copy only paths changed by a transition.
pub struct State {
    pub streams: BTreeMap<String, Stream>,
    pub generation: u64,
    pub time: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pending {
    pub last_activity: u64,
    pub nak: bool,
    pub consumer_sequence: u64,
    pub attempt: u64,
    pub deadline: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Consumer {
    pub config: ConsumerConfig,
    pub generation: u64,
    pub created: u64,
    pub cursor: u64,
    pub delivered: u64,
    pub last_stream: u64,
    pub ack_floor: u64,
    pub ack_stream: u64,
    pub pending: OrdMap<u64, Pending>,
    pub exhausted: OrdSet<u64>,
    pub history: OrdMap<u64, (u64, bool)>,
    pub initial: Option<OrdSet<u64>>,
    pub initial_cutoff: u64,
    pub num_pending: usize,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stream {
    pub config: StreamConfig,
    pub generation: u64,
    pub created: u64,
    pub last_sequence: u64,
    pub last_msg_id: String,
    pub messages: OrdMap<u64, StoredMessage>,
    pub bytes: u64,
    pub consumers: BTreeMap<String, Consumer>,
    pub subjects: OrdMap<String, OrdSet<u64>>,
    pub interest: OrdMap<u64, OrdSet<u64>>,
    pub dedup: OrdMap<String, (u64, u64)>,
    pub dedup_expiry: OrdSet<(u64, String)>,
}
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Store(#[from] StoreError),
    #[error("{0}")]
    Json(#[from] serde_json::Error),
    #[error("snapshot codec: {0}")]
    Codec(#[from] postcard::Error),
    #[error("{0}")]
    Invalid(&'static str),
}
fn invalid(s: &'static str) -> Error {
    Error::Invalid(s)
}
pub fn name_valid(s: &str) -> bool {
    !s.is_empty() && !s.bytes().any(|c| c <= b' ' || b".*>/\\".contains(&c))
}
pub fn header<'a>(headers: &'a [u8], name: &str) -> Option<&'a str> {
    std::str::from_utf8(headers)
        .ok()?
        .split("\r\n")
        .skip(1)
        .filter_map(|s| s.split_once(':'))
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.trim())
}
pub fn overlap(a: &str, b: &str) -> bool {
    let mut a = a.split('.');
    let mut b = b.split('.');
    loop {
        match (a.next(), b.next()) {
            (None, None) => return true,
            (None, _) | (_, None) => return false,
            (Some(">"), Some(_)) | (Some(_), Some(">")) => return true,
            (Some(x), Some(y)) if x == y || x == "*" || y == "*" => (),
            _ => return false,
        }
    }
}
enum Acknowledged {
    None,
    Single(u64),
    Multiple(OrdSet<u64>),
}
impl Acknowledged {
    fn for_each(self, mut apply: impl FnMut(u64)) {
        match self {
            Self::None => (),
            Self::Single(sequence) => apply(sequence),
            Self::Multiple(sequences) => {
                for sequence in sequences {
                    apply(sequence);
                }
            }
        }
    }
}
impl Consumer {
    fn acknowledge(
        &mut self,
        sequence: u64,
        consumer_sequence: u64,
        time: u64,
        action: AckAction,
    ) -> Acknowledged {
        match action {
            AckAction::Nak(delay) => {
                if let Some(p) = self.pending.get_mut(&sequence) {
                    if p.consumer_sequence == consumer_sequence {
                        p.last_activity = time;
                        p.nak = true;
                        p.deadline = time.saturating_add(delay);
                    }
                }
            }
            AckAction::Progress => {
                if let Some(p) = self.pending.get_mut(&sequence) {
                    if p.consumer_sequence == consumer_sequence {
                        p.last_activity = time;
                        p.nak = false;
                        p.deadline = time.saturating_add(self.config.wait(p.attempt));
                    }
                }
            }
            kind @ (AckAction::Ack | AckAction::Term) => {
                if kind == AckAction::Ack && self.config.ack_policy == AckPolicy::All {
                    let ids = self
                        .history
                        .range(..=consumer_sequence)
                        .map(|(_, (s, _))| *s)
                        .chain(std::iter::once(sequence))
                        .collect::<OrdSet<_>>();
                    for n in &ids {
                        self.finish(*n);
                    }
                    return Acknowledged::Multiple(ids);
                }
                self.finish(sequence);
                return Acknowledged::Single(sequence);
            }
        }
        Acknowledged::None
    }
    fn next_delivery(&self, s: &Stream, time: u64) -> Option<Delivery> {
        self.select_delivery(s, time, true)
    }
    fn select_delivery(&self, s: &Stream, time: u64, check_due: bool) -> Option<Delivery> {
        let due = if check_due {
            self.pending
                .iter()
                .find(|(seq, p)| {
                    p.deadline <= time
                        && s.messages.get(*seq).is_some_and(|m| {
                            s.config.max_age == 0 || m.time.saturating_add(s.config.max_age) > time
                        })
                        && (self.config.max_deliver < 0
                            || p.attempt < self.config.max_deliver as u64)
                })
                .map(|(&seq, p)| (seq, p.attempt + 1))
        } else {
            None
        };
        let next = if let Some(x) = due {
            Some(x)
        } else if self.config.max_ack_pending > 0
            && self.pending.len() as i64 >= self.config.max_ack_pending
        {
            None
        } else {
            s.messages
                .range(self.cursor..)
                .find(|(seq, m)| {
                    self.config.accepts(&m.subject)
                        && self
                            .initial
                            .as_ref()
                            .is_none_or(|ids| ids.contains(*seq) || **seq > self.initial_cutoff)
                        && (s.config.max_age == 0 || m.time.saturating_add(s.config.max_age) > time)
                })
                .map(|(&seq, _)| (seq, 1))
        };
        next.map(|(sequence, attempt)| Delivery {
            sequence,
            consumer_sequence: self.delivered + 1,
            attempt,
            deadline: time.saturating_add(self.config.wait(attempt)),
        })
    }
    fn reserve(&mut self, d: &Delivery, time: u64) {
        self.delivered = d.consumer_sequence;
        self.last_stream = self.last_stream.max(d.sequence);
        self.cursor = self.cursor.max(d.sequence + 1);
        if let Some(initial) = &mut self.initial {
            initial.remove(&d.sequence);
            if initial.is_empty() {
                self.initial = None;
            }
        }
        if !self.pending.contains_key(&d.sequence) {
            self.num_pending = self.num_pending.saturating_sub(1);
            self.history
                .insert(d.consumer_sequence, (d.sequence, false));
        }
        if self.config.ack_policy == AckPolicy::None {
            self.finish(d.sequence);
        } else {
            self.pending.insert(
                d.sequence,
                Pending {
                    last_activity: time,
                    nak: false,
                    consumer_sequence: d.consumer_sequence,
                    attempt: d.attempt,
                    deadline: d.deadline,
                },
            );
            self.advance();
        }
    }
    fn finish(&mut self, seq: u64) {
        let pending = self.pending.remove(&seq);
        let exhausted = self.exhausted.remove(&seq);
        // Retention can finish the same sequence after its ACK already did so.
        if pending.is_none() && exhausted.is_none() && self.config.ack_policy != AckPolicy::None {
            return;
        }
        if let Some(p) = pending.filter(|p| p.attempt == 1) {
            self.history.remove(&p.consumer_sequence);
        } else {
            // A redelivery retains its original history key. ACK-none also passes
            // here before a pending entry is installed.
            let finished: Vec<_> = self
                .history
                .iter()
                .filter(|(_, (s, _))| *s == seq)
                .map(|(n, _)| *n)
                .collect();
            for n in finished {
                self.history.remove(&n);
            }
        }
        self.advance();
    }
    fn advance(&mut self) {
        self.ack_floor = self
            .history
            .get_min()
            .map(|(n, _)| n - 1)
            .unwrap_or(self.delivered)
            .max(self.ack_floor);
        // Every history sequence has one pending or exhausted entry. Both indexes
        // are ordered by stream sequence; no history scan is needed for this floor.
        let first = self
            .pending
            .get_min()
            .map(|(n, _)| *n)
            .into_iter()
            .chain(self.exhausted.get_min().copied())
            .min();
        self.ack_stream = first
            .map(|n| n - 1)
            .unwrap_or(self.last_stream)
            .max(self.ack_stream);
    }
    pub fn ready_count(&self, _: &Stream) -> usize {
        self.num_pending
    }
    fn eligible(&self, seq: u64, subject: &str) -> bool {
        seq >= self.cursor
            && self.config.accepts(subject)
            && self
                .initial
                .as_ref()
                .is_none_or(|ids| ids.contains(&seq) || seq > self.initial_cutoff)
    }
}

impl Stream {
    fn remove(&mut self, seq: u64) {
        if let Some(m) = self.messages.remove(&seq) {
            self.bytes -= m.size();
            if let Some(ids) = self.subjects.get_mut(&m.subject) {
                ids.remove(&seq);
                if ids.is_empty() {
                    self.subjects.remove(&m.subject);
                }
            }
            self.interest.remove(&seq);
            for c in self.consumers.values_mut() {
                if c.eligible(seq, &m.subject) {
                    c.num_pending = c.num_pending.saturating_sub(1);
                }
                if let Some(ids) = &mut c.initial {
                    ids.remove(&seq);
                }
                c.finish(seq);
            }
        }
    }
    fn reserve_delivery(
        &mut self,
        consumer: &str,
        generation: u64,
        delivery: &Delivery,
        time: u64,
    ) {
        let c = self.consumers.get_mut(consumer).unwrap();
        c.reserve(delivery, time);
        if c.config.ack_policy == AckPolicy::None {
            self.acknowledge(delivery.sequence, generation);
        }
    }
    fn acknowledge(&mut self, seq: u64, generation: u64) {
        match self.config.retention {
            Retention::Limits => (),
            Retention::Workqueue => self.remove(seq),
            Retention::Interest => {
                if let Some(ids) = self.interest.get_mut(&seq) {
                    ids.remove(&generation);
                    if ids.is_empty() {
                        self.remove(seq);
                    }
                }
            }
        }
    }
    fn expire(&mut self, time: u64) {
        if self.config.max_age > 0 {
            while let Some((&seq, m)) = self.messages.iter().next() {
                if m.time.saturating_add(self.config.max_age) > time {
                    break;
                }
                self.remove(seq);
            }
        }
        while let Some((expires, id)) = self.dedup_expiry.get_min().cloned() {
            if expires > time {
                break;
            }
            self.dedup_expiry.remove(&(expires, id.clone()));
            if self
                .dedup
                .get(&id)
                .is_some_and(|(_, t)| t.saturating_add(self.config.duplicate_window) <= time)
            {
                self.dedup.remove(&id);
            }
        }
        for c in self.consumers.values_mut() {
            let ids: Vec<_> = c
                .pending
                .iter()
                .filter(|(_, p)| {
                    p.deadline <= time
                        && c.config.max_deliver > 0
                        && p.attempt >= c.config.max_deliver as u64
                })
                .map(|(&n, _)| n)
                .collect();
            for n in ids {
                c.pending.remove(&n);
                c.exhausted.insert(n);
            }
        }
    }
    pub fn duplicate(&self, headers: &[u8], time: u64) -> Option<u64> {
        let id = header(headers, "Nats-Msg-Id")?;
        let &(seq, created) = self.dedup.get(id)?;
        (self.config.duplicate_window > 0
            && created.saturating_add(self.config.duplicate_window) > time)
            .then_some(seq)
    }
}
impl State {
    pub fn prepare_create(&self, config: StreamConfig, time: u64) -> Result<Event, Error> {
        if !name_valid(&config.name)
            || config.subjects.is_empty()
            || config
                .subjects
                .iter()
                .any(|s| !subject(s, true) || s.starts_with("$JS.") || s.starts_with("$SCREECH."))
            || [
                config.max_msgs,
                config.max_bytes,
                config.max_msgs_per_subject,
                config.max_msg_size,
                config.max_consumers,
            ]
            .iter()
            .any(|n| *n < -1)
        {
            return Err(invalid("Invalid stream configuration"));
        }
        if self.streams.contains_key(&config.name) {
            return Err(invalid("Stream already exists"));
        }
        if self.streams.values().any(|s| {
            s.config
                .subjects
                .iter()
                .any(|a| config.subjects.iter().any(|b| overlap(a, b)))
        }) {
            return Err(invalid("Stream subjects overlap"));
        }
        self.generation
            .checked_add(1)
            .ok_or(invalid("Generation exhausted"))?;
        Ok(Event::Create {
            config,
            time: time.max(self.time),
        })
    }
    pub fn prepare_update(&self, config: StreamConfig, time: u64) -> Result<Event, Error> {
        let s = self
            .streams
            .get(&config.name)
            .ok_or(invalid("Stream not found"))?;
        if config.subjects.is_empty()
            || config
                .subjects
                .iter()
                .any(|f| !subject(f, true) || f.starts_with("$JS.") || f.starts_with("$SCREECH."))
            || [
                config.max_msgs,
                config.max_bytes,
                config.max_msgs_per_subject,
                config.max_msg_size,
                config.max_consumers,
            ]
            .iter()
            .any(|n| *n < -1)
        {
            return Err(invalid("Invalid stream configuration"));
        }
        if config.retention != s.config.retention
            && (!s.messages.is_empty() || !s.consumers.is_empty())
        {
            return Err(invalid("Cannot change active retention policy"));
        }
        if config.max_consumers > 0 && config.max_consumers < s.consumers.len() as i64 {
            return Err(invalid("Consumer limit below current count"));
        }
        if self
            .streams
            .values()
            .filter(|s| s.config.name != config.name)
            .any(|s| {
                s.config
                    .subjects
                    .iter()
                    .any(|a| config.subjects.iter().any(|b| overlap(a, b)))
            })
        {
            return Err(invalid("Stream subjects overlap"));
        }
        Ok(Event::Update {
            config,
            time: time.max(self.time),
        })
    }
    pub fn prepare_publish(
        &self,
        stream: &str,
        subj: &str,
        headers: Bytes,
        payload: Bytes,
        time: u64,
    ) -> Result<Event, Error> {
        let s = self
            .streams
            .get(stream)
            .ok_or(invalid("Stream not found"))?;
        let time = time.max(self.time);
        if !subject(subj, false) || !s.config.subjects.iter().any(|f| matches(f, subj)) {
            return Err(invalid("Subject does not match stream"));
        }
        if s.duplicate(&headers, time).is_some() {
            return Err(invalid("Duplicate publication"));
        }
        for (key, expected) in [
            ("Nats-Expected-Stream", s.config.name.clone()),
            ("Nats-Expected-Last-Sequence", s.last_sequence.to_string()),
            (
                "Nats-Expected-Last-Subject-Sequence",
                s.subjects
                    .get(subj)
                    .and_then(|ids| ids.get_max())
                    .copied()
                    .unwrap_or(0)
                    .to_string(),
            ),
        ] {
            if header(&headers, key).is_some_and(|v| v != expected) {
                return Err(invalid("Publication expectation failed"));
            }
        }
        if let Some(id) = header(&headers, "Nats-Expected-Last-Msg-Id") {
            let last = s.last_msg_id.as_str();
            if id != last {
                return Err(invalid("Last message ID expectation failed"));
            }
        }
        let size = (subj.len()
            + headers.len()
            + payload.len()
            + 30
            + if headers.is_empty() { 0 } else { 4 }) as u64;
        if s.config.max_msg_size > 0
            && (headers.len() + payload.len()) as i64 > s.config.max_msg_size
        {
            return Err(invalid("Maximum message size exceeded"));
        }
        if s.config.max_bytes > 0 && size > s.config.max_bytes as u64 {
            return Err(invalid("Message exceeds stream byte limit"));
        }
        if s.config.discard == Discard::New {
            let live = s.messages.values().filter(|m| {
                s.config.max_age == 0 || m.time.saturating_add(s.config.max_age) > time
            });
            let (count, bytes, per) = live.fold((0i64, 0u64, 0i64), |(count, bytes, per), m| {
                (
                    count + 1,
                    bytes + m.size(),
                    per + i64::from(m.subject == subj),
                )
            });
            if (s.config.max_msgs > 0 && count >= s.config.max_msgs)
                || (s.config.max_bytes > 0 && bytes + size > s.config.max_bytes as u64)
                || (s.config.max_msgs_per_subject > 0 && per >= s.config.max_msgs_per_subject)
            {
                return Err(invalid("Stream limit exceeded"));
            }
        }
        let sequence = s
            .last_sequence
            .checked_add(1)
            .ok_or(invalid("Sequence exhausted"))?;
        Ok(Event::Publish {
            stream: stream.into(),
            message: StoredMessage {
                sequence,
                subject: subj.into(),
                headers,
                payload,
                time,
                content: None,
            },
        })
    }
    pub fn prepare_delete(&self, name: &str, purge: bool) -> Result<Event, Error> {
        if !self.streams.contains_key(name) {
            return Err(invalid("Stream not found"));
        }
        Ok(if purge {
            Event::Purge { name: name.into() }
        } else {
            Event::Delete { name: name.into() }
        })
    }
    pub fn prepare_consumer(
        &self,
        stream: &str,
        mut config: ConsumerConfig,
        time: u64,
    ) -> Result<Event, Error> {
        let s = self
            .streams
            .get(stream)
            .ok_or(invalid("Stream not found"))?;
        if config.name.is_empty() {
            config.name = config
                .durable_name
                .clone()
                .unwrap_or_else(|| format!("C{}", self.generation + 1));
        }
        if !name_valid(&config.name)
            || config
                .durable_name
                .as_ref()
                .is_some_and(|n| n != &config.name)
            || config.ack_wait == 0
            || config.max_deliver == 0
            || config.max_deliver < -1
            || config.max_ack_pending == 0
            || config.max_ack_pending < -1
            || config.max_waiting == 0
            || config.max_batch == 0
            || config.backoff.contains(&0)
            || (!config.backoff.is_empty()
                && config.max_deliver > 0
                && config.backoff.len() as i64 > config.max_deliver)
        {
            return Err(invalid("Invalid consumer configuration"));
        }
        if !config.filter_subject.is_empty() && !config.filter_subjects.is_empty()
            || config.filters().iter().any(|s| !subject(s, true))
            || config
                .deliver_subject
                .as_ref()
                .is_some_and(|s| !subject(s, false))
            || config
                .deliver_group
                .as_ref()
                .is_some_and(|q| !crate::parser::token(q))
            || config.deliver_group.is_some() && config.deliver_subject.is_none()
        {
            return Err(invalid("Invalid consumer filter or delivery subject"));
        }
        if config.deliver_policy == DeliverPolicy::ByStartSequence && config.opt_start_seq == 0 {
            return Err(invalid("Start sequence required"));
        }
        if config.deliver_policy == DeliverPolicy::ByStartTime
            && config
                .opt_start_time
                .as_ref()
                .and_then(|t| time_parse(t).ok())
                .is_none()
        {
            return Err(invalid("Start time required"));
        }
        if s.consumers.contains_key(&config.name) {
            return Err(invalid("Consumer already exists"));
        }
        if s.config.max_consumers > 0 && s.consumers.len() as i64 >= s.config.max_consumers {
            return Err(invalid("Maximum consumers exceeded"));
        }
        if s.config.retention == Retention::Workqueue
            && (config.ack_policy != AckPolicy::Explicit
                || s.consumers.values().any(|c| {
                    c.config
                        .filters()
                        .iter()
                        .any(|a| config.filters().iter().any(|b| overlap(a, b)))
                }))
        {
            return Err(invalid(
                "Work queue requires explicit ACK and non-overlapping consumers",
            ));
        }
        Ok(Event::ConsumerCreate {
            stream: stream.into(),
            config,
            time: time.max(self.time),
            generation: self
                .generation
                .checked_add(1)
                .ok_or(invalid("Generation exhausted"))?,
        })
    }
    pub fn prepare_consumer_update(
        &self,
        stream: &str,
        config: ConsumerConfig,
        time: u64,
    ) -> Result<Event, Error> {
        let c = self
            .streams
            .get(stream)
            .and_then(|s| s.consumers.get(&config.name))
            .ok_or(invalid("Consumer not found"))?;
        let mut expected = c.config.clone();
        expected.description = config.description.clone();
        expected.ack_wait = config.ack_wait;
        expected.max_deliver = config.max_deliver;
        expected.backoff = config.backoff.clone();
        expected.max_ack_pending = config.max_ack_pending;
        expected.max_batch = config.max_batch;
        expected.max_expires = config.max_expires;
        expected.max_bytes = config.max_bytes;
        if expected != config {
            return Err(invalid(
                "Consumer identity, filters, delivery mode and policy are immutable",
            ));
        }
        if config.ack_wait == 0
            || config.max_deliver == 0
            || config.max_deliver < -1
            || config.max_ack_pending == 0
            || config.max_ack_pending < -1
            || config.max_batch == 0
            || config.backoff.contains(&0)
            || config.max_deliver > 0 && config.backoff.len() as i64 > config.max_deliver
        {
            return Err(invalid("Invalid consumer limits"));
        }
        Ok(Event::ConsumerUpdate {
            stream: stream.into(),
            config,
            generation: c.generation,
            time: time.max(self.time),
        })
    }
    pub fn prepare_delivery(
        &self,
        stream: &str,
        name: &str,
        time: u64,
    ) -> Result<Option<Event>, Error> {
        let s = self
            .streams
            .get(stream)
            .ok_or(invalid("Stream not found"))?;
        let c = s.consumers.get(name).ok_or(invalid("Consumer not found"))?;
        let time = time.max(self.time);
        Ok(c.next_delivery(s, time).map(|delivery| Event::Deliver {
            stream: stream.into(),
            consumer: name.into(),
            generation: c.generation,
            time,
            delivery,
        }))
    }
    /// Preview only the consumer state, without copying retained stream payloads.
    pub fn prepare_delivery_batch(
        &self,
        stream: &str,
        name: &str,
        time: u64,
        limit: usize,
        max_bytes: usize,
    ) -> Result<Option<Event>, Error> {
        if limit == 0 || limit > 64 {
            return Err(invalid("Invalid reservation batch size"));
        }
        let s = self
            .streams
            .get(stream)
            .ok_or(invalid("Stream not found"))?;
        let mut c = s
            .consumers
            .get(name)
            .ok_or(invalid("Consumer not found"))?
            .clone();
        let time = time.max(self.time);
        let mut deliveries = Vec::with_capacity(limit);
        let mut bytes = 0usize;
        let mut check_due = true;
        for _ in 0..limit {
            let Some(d) = c.select_delivery(s, time, check_due) else {
                break;
            };
            let size = s.messages[&d.sequence].size() as usize;
            if max_bytes > 0 && size > max_bytes.saturating_sub(bytes) {
                break;
            }
            bytes = bytes.saturating_add(size);
            // Once selection reaches new data, no original reservation is due.
            // New entries cannot become due at this fixed time unless addition
            // saturated (including time == u64::MAX).
            if d.attempt == 1 && d.deadline > time {
                check_due = false;
            }
            c.reserve(&d, time);
            deliveries.push(d);
        }
        Ok((!deliveries.is_empty()).then(|| Event::DeliverBatch {
            stream: stream.into(),
            consumer: name.into(),
            generation: c.generation,
            time,
            deliveries,
        }))
    }

    pub fn prepare_ack(
        &self,
        stream: &str,
        name: &str,
        token: AckToken,
        action: AckAction,
        time: u64,
    ) -> Result<Event, Error> {
        let AckToken {
            generation,
            sequence,
            consumer_sequence,
        } = token;
        let s = self
            .streams
            .get(stream)
            .ok_or(invalid("Stream not found"))?;
        let c = s.consumers.get(name).ok_or(invalid("Consumer not found"))?;
        if c.generation != generation
            || consumer_sequence == 0
            || consumer_sequence > c.delivered
            || sequence > c.last_stream
            || c.history
                .get(&consumer_sequence)
                .is_some_and(|(seq, _)| *seq != sequence)
        {
            return Err(invalid("Stale or invalid acknowledgement"));
        }
        Ok(Event::Ack {
            stream: stream.into(),
            consumer: name.into(),
            generation,
            sequence,
            consumer_sequence,
            time: time.max(self.time),
            action,
        })
    }
    /// Preview one consumer only; ACKed messages are behind its cursor, so its
    /// next selection is identical against the original immutable stream index.
    pub fn prepare_ack_next(&self, ack: Event, time: u64) -> Result<Option<Event>, Error> {
        self.validate(&ack)?;
        let Event::Ack {
            stream,
            consumer,
            sequence,
            consumer_sequence,
            time: ack_time,
            action: AckAction::Ack,
            ..
        } = &ack
        else {
            return Err(invalid("Expected plain ACK"));
        };
        let s = &self.streams[stream];
        let mut c = s.consumers[consumer].clone();
        c.acknowledge(*sequence, *consumer_sequence, *ack_time, AckAction::Ack);
        let time = time.max(self.time).max(*ack_time);
        let next = c.next_delivery(s, time).map(|delivery| Event::Deliver {
            stream: stream.clone(),
            consumer: consumer.clone(),
            generation: c.generation,
            time,
            delivery,
        });
        Ok(next.map(|next| Event::AckNext {
            ack: Box::new(ack),
            next: Box::new(next),
        }))
    }

    pub fn check_invariants(&self) -> Result<(), Error> {
        let mut generations = OrdSet::new();
        for (name, s) in &self.streams {
            if name != &s.config.name
                || s.generation == 0
                || s.generation > self.generation
                || generations.insert(s.generation).is_some()
                || s.bytes != s.messages.values().map(StoredMessage::size).sum::<u64>()
            {
                return Err(invalid("Invalid stream snapshot"));
            }
            let mut subjects = OrdMap::<String, OrdSet<u64>>::new();
            for (&seq, m) in &s.messages {
                if seq == 0
                    || seq != m.sequence
                    || seq > s.last_sequence
                    || !subject(&m.subject, false)
                    || m.time > self.time
                {
                    return Err(invalid("Invalid message snapshot"));
                }
                if let Some(r) = &m.content {
                    if r.key.generation != s.generation
                        || r.key.sequence != seq
                        || !m.headers.is_empty()
                        || !m.payload.is_empty()
                    {
                        return Err(invalid("Invalid content reference"));
                    }
                }
                subjects.entry(m.subject.clone()).or_default().insert(seq);
            }
            if subjects != s.subjects {
                return Err(invalid("Invalid subject index"));
            }
            for (name, c) in &s.consumers {
                if name != &c.config.name
                    || c.generation == 0
                    || c.generation > self.generation
                    || generations.insert(c.generation).is_some()
                    || c.ack_floor > c.delivered
                    || c.ack_stream > c.last_stream
                    || c.last_stream > s.last_sequence
                    || c.num_pending
                        != s.messages
                            .iter()
                            .filter(|(seq, m)| c.eligible(**seq, &m.subject))
                            .count()
                {
                    return Err(invalid("Invalid consumer snapshot"));
                }
                for (&seq, p) in &c.pending {
                    if !s.messages.contains_key(&seq)
                        || p.attempt == 0
                        || p.consumer_sequence > c.delivered
                        || seq >= c.cursor
                        || c.exhausted.contains(&seq)
                    {
                        return Err(invalid("Invalid pending delivery snapshot"));
                    }
                }
                let mut history_sequences = OrdSet::new();
                if c.exhausted.iter().any(|n| !s.messages.contains_key(n))
                    || c.history.iter().any(|(n, (seq, _))| {
                        *n == 0
                            || *n > c.delivered
                            || (!c.pending.contains_key(seq) && !c.exhausted.contains(seq))
                            || history_sequences.insert(*seq).is_some()
                    })
                    || history_sequences.len() != c.pending.len() + c.exhausted.len()
                    || c.ack_floor
                        != c.history
                            .get_min()
                            .map(|(n, _)| n - 1)
                            .unwrap_or(c.delivered)
                    || c.ack_stream
                        != c.history
                            .values()
                            .map(|(seq, _)| seq - 1)
                            .min()
                            .unwrap_or(c.last_stream)
                    || c.pending.iter().any(|(seq, p)| {
                        p.attempt == 1
                            && c.history
                                .get(&p.consumer_sequence)
                                .is_none_or(|(s, _)| s != seq)
                    })
                {
                    return Err(invalid("Invalid ACK history snapshot"));
                }
            }
            if s.dedup.iter().any(|(id, (seq, t))| {
                id.is_empty() || *seq == 0 || *seq > s.last_sequence || *t > self.time
            }) {
                return Err(invalid("Invalid dedup snapshot"));
            }
            for (&seq, ids) in &s.interest {
                if !s.messages.contains_key(&seq)
                    || ids
                        .iter()
                        .any(|id| !s.consumers.values().any(|c| c.generation == *id))
                {
                    return Err(invalid("Invalid interest snapshot"));
                }
            }
        }
        Ok(())
    }
    pub fn expiration_needed(&self, time: u64) -> bool {
        self.streams.values().any(|s| {
            (s.config.max_age > 0
                && s.messages
                    .iter()
                    .next()
                    .is_some_and(|(_, m)| m.time.saturating_add(s.config.max_age) <= time))
                || s.dedup_expiry.get_min().is_some_and(|(t, _)| *t <= time)
                || s.consumers.values().any(|c| {
                    c.config.max_deliver > 0
                        && c.pending
                            .values()
                            .any(|p| p.deadline <= time && p.attempt >= c.config.max_deliver as u64)
                })
        })
    }
    fn validate(&self, e: &Event) -> Result<(), Error> {
        let prepared = match e {
            Event::AckNext { ack, next } => {
                let Event::Deliver { time, .. } = next.as_ref() else {
                    return Err(invalid("Expected next delivery"));
                };
                self.prepare_ack_next(ack.as_ref().clone(), *time)?
                    .ok_or(invalid("No next delivery"))?
            }
            Event::DeliverBatch {
                stream,
                consumer,
                time,
                deliveries,
                ..
            } => self
                .prepare_delivery_batch(stream, consumer, *time, deliveries.len(), 0)?
                .ok_or(invalid("No eligible delivery"))?,
            Event::AckBatch { events } => {
                if events.len() < 2 || events.len() > 64 {
                    return Err(invalid("Invalid ACK batch size"));
                }
                let mut previous = self.time;
                for event in events {
                    let Event::Ack { time, .. } = event else {
                        return Err(invalid("Non-ACK in ACK batch"));
                    };
                    if *time < previous {
                        return Err(invalid("Unordered ACK batch"));
                    }
                    self.validate(event)?;
                    previous = *time;
                }
                e.clone()
            }
            Event::ConsumerUpdate {
                stream,
                config,
                time,
                ..
            } => self.prepare_consumer_update(stream, config.clone(), *time)?,
            Event::Update { config, time } => self.prepare_update(config.clone(), *time)?,
            Event::Remove { stream, sequences } => {
                let s = self
                    .streams
                    .get(stream)
                    .ok_or(invalid("Stream not found"))?;
                if sequences.windows(2).any(|p| p[0] >= p[1])
                    || sequences.iter().any(|n| !s.messages.contains_key(n))
                {
                    return Err(invalid("Invalid removal set"));
                }
                e.clone()
            }
            Event::Create { config, time } => self.prepare_create(config.clone(), *time)?,
            Event::Delete { name } => self.prepare_delete(name, false)?,
            Event::Purge { name } => self.prepare_delete(name, true)?,
            Event::Publish { stream, message } => self.prepare_publish(
                stream,
                &message.subject,
                message.headers.clone(),
                message.payload.clone(),
                message.time,
            )?,
            Event::ConsumerCreate {
                stream,
                config,
                time,
                ..
            } => self.prepare_consumer(stream, config.clone(), *time)?,
            Event::ConsumerDelete {
                stream,
                name,
                generation,
            } => {
                let c = self
                    .streams
                    .get(stream)
                    .and_then(|s| s.consumers.get(name))
                    .ok_or(invalid("Consumer not found"))?;
                if c.generation != *generation {
                    return Err(invalid("Stale consumer generation"));
                }
                e.clone()
            }
            Event::Deliver {
                stream,
                consumer,
                time,
                ..
            } => self
                .prepare_delivery(stream, consumer, *time)?
                .ok_or(invalid("No eligible delivery"))?,
            Event::Ack {
                stream,
                consumer,
                generation,
                sequence,
                consumer_sequence,
                time,
                action,
            } => self.prepare_ack(
                stream,
                consumer,
                crate::durable::AckToken {
                    generation: *generation,
                    sequence: *sequence,
                    consumer_sequence: *consumer_sequence,
                },
                action.clone(),
                *time,
            )?,
            Event::Expire { time } => {
                if *time < self.time {
                    return Err(invalid("Clock moved backwards"));
                }
                e.clone()
            }
        };
        if &prepared != e {
            return Err(invalid("Stale prepared event"));
        }
        Ok(())
    }
    pub fn apply(&mut self, event: Event) -> Result<(), Error> {
        self.validate(&event)?;
        self.apply_validated(event)
    }
    // Engine calls this only after validation followed by a successful append.
    fn apply_validated(&mut self, event: Event) -> Result<(), Error> {
        match event {
            Event::AckNext { ack, next } => {
                self.apply_validated(*ack)?;
                self.apply(*next)?;
            }
            Event::DeliverBatch {
                stream,
                consumer,
                generation,
                time,
                deliveries,
            } => {
                self.time = self.time.max(time);
                let s = self.streams.get_mut(&stream).unwrap();
                for delivery in &deliveries {
                    s.reserve_delivery(&consumer, generation, delivery, time);
                }
            }
            Event::AckBatch { events } => {
                // Validation already checked every ACK and its timestamp order.
                // ACKs only remove history or adjust activity; they cannot replace
                // the sequence mapping used to validate a later ACK in this batch.
                for event in events {
                    self.apply_validated(event)?;
                }
            }
            Event::ConsumerUpdate {
                stream,
                config,
                time,
                ..
            } => {
                self.time = self.time.max(time);
                let c = self
                    .streams
                    .get_mut(&stream)
                    .unwrap()
                    .consumers
                    .get_mut(&config.name)
                    .unwrap();
                let timing =
                    c.config.ack_wait != config.ack_wait || c.config.backoff != config.backoff;
                c.config = config;
                if timing {
                    let keys: Vec<_> = c.pending.keys().copied().collect();
                    for key in keys {
                        let p = c.pending.get_mut(&key).unwrap();
                        if !p.nak {
                            p.deadline = p.last_activity.saturating_add(c.config.wait(p.attempt));
                        }
                    }
                }
            }
            Event::Update { config, time } => {
                self.time = self.time.max(time);
                let s = self.streams.get_mut(&config.name).unwrap();
                s.config = config;
                s.dedup_expiry = s
                    .dedup
                    .iter()
                    .map(|(id, (_, t))| (t.saturating_add(s.config.duplicate_window), id.clone()))
                    .collect();
                s.expire(time);
                let subjects: Vec<_> = s.subjects.keys().cloned().collect();
                for subject in subjects {
                    while s.config.max_msgs_per_subject > 0
                        && s.subjects
                            .get(&subject)
                            .is_some_and(|ids| ids.len() as i64 > s.config.max_msgs_per_subject)
                    {
                        let n = *s.subjects[&subject].get_min().unwrap();
                        s.remove(n);
                    }
                }
                while (s.config.max_msgs > 0 && s.messages.len() as i64 > s.config.max_msgs)
                    || (s.config.max_bytes > 0 && s.bytes > s.config.max_bytes as u64)
                {
                    s.remove(*s.messages.iter().next().unwrap().0);
                }
            }
            Event::Remove { stream, sequences } => {
                let s = self.streams.get_mut(&stream).unwrap();
                for n in sequences {
                    s.remove(n);
                }
            }
            Event::Create { config, time } => {
                self.generation += 1;
                self.time = self.time.max(time);
                self.streams.insert(
                    config.name.clone(),
                    Stream {
                        config,
                        generation: self.generation,
                        created: time,
                        last_sequence: 0,
                        last_msg_id: String::new(),
                        messages: OrdMap::new(),
                        bytes: 0,
                        consumers: BTreeMap::new(),
                        subjects: OrdMap::new(),
                        interest: OrdMap::new(),
                        dedup: OrdMap::new(),
                        dedup_expiry: OrdSet::new(),
                    },
                );
            }
            Event::Delete { name } => {
                self.streams.remove(&name);
            }
            Event::Purge { name } => {
                let s = self.streams.get_mut(&name).unwrap();
                let ids: Vec<_> = s.messages.keys().copied().collect();
                for n in ids {
                    s.remove(n);
                }
            }
            Event::Publish { stream, message } => {
                self.time = self.time.max(message.time);
                let s = self.streams.get_mut(&stream).unwrap();
                s.expire(message.time);
                s.last_sequence = message.sequence;
                s.last_msg_id = header(&message.headers, "Nats-Msg-Id").unwrap_or("").into();
                if s.config.duplicate_window > 0 {
                    if let Some(id) =
                        header(&message.headers, "Nats-Msg-Id").filter(|s| !s.is_empty())
                    {
                        s.dedup.insert(id.into(), (message.sequence, message.time));
                        s.dedup_expiry.insert((
                            message.time.saturating_add(s.config.duplicate_window),
                            id.into(),
                        ));
                    }
                }
                let interest: OrdSet<_> = if s.config.retention == Retention::Interest {
                    s.consumers
                        .values()
                        .filter(|c| c.config.accepts(&message.subject))
                        .map(|c| c.generation)
                        .collect()
                } else {
                    OrdSet::new()
                };
                if s.config.retention != Retention::Interest || !interest.is_empty() {
                    for c in s.consumers.values_mut() {
                        if c.eligible(message.sequence, &message.subject) {
                            c.num_pending += 1;
                        }
                    }
                    s.bytes += message.size();
                    s.subjects
                        .entry(message.subject.clone())
                        .or_default()
                        .insert(message.sequence);
                    if s.config.retention == Retention::Interest {
                        s.interest.insert(message.sequence, interest);
                    }
                    let subject = message.subject.clone();
                    s.messages.insert(message.sequence, message);
                    while s.config.max_msgs_per_subject > 0
                        && s.subjects
                            .get(&subject)
                            .is_some_and(|ids| ids.len() as i64 > s.config.max_msgs_per_subject)
                    {
                        let n = *s.subjects[&subject].get_min().unwrap();
                        s.remove(n);
                    }
                    while (s.config.max_msgs > 0 && s.messages.len() as i64 > s.config.max_msgs)
                        || (s.config.max_bytes > 0 && s.bytes > s.config.max_bytes as u64)
                    {
                        s.remove(*s.messages.iter().next().unwrap().0);
                    }
                }
            }
            Event::ConsumerCreate {
                stream,
                config,
                time,
                generation,
            } => {
                self.time = self.time.max(time);
                self.generation = generation;
                let s = self.streams.get_mut(&stream).unwrap();
                let eligible: Vec<_> = s
                    .messages
                    .values()
                    .filter(|m| config.accepts(&m.subject))
                    .collect();
                let initial = if config.deliver_policy == DeliverPolicy::LastPerSubject {
                    Some(
                        s.subjects
                            .iter()
                            .filter(|(subject, _)| config.accepts(subject))
                            .filter_map(|(_, ids)| ids.get_max().copied())
                            .collect::<OrdSet<_>>(),
                    )
                } else {
                    None
                };
                let start = match config.deliver_policy {
                    DeliverPolicy::New => s.last_sequence + 1,
                    DeliverPolicy::Last => eligible
                        .last()
                        .map(|m| m.sequence)
                        .unwrap_or(s.last_sequence + 1),
                    DeliverPolicy::ByStartSequence => config.opt_start_seq,
                    DeliverPolicy::ByStartTime => {
                        let t = time_parse(config.opt_start_time.as_ref().unwrap())?;
                        eligible
                            .iter()
                            .find(|m| m.time >= t)
                            .map(|m| m.sequence)
                            .unwrap_or(s.last_sequence + 1)
                    }
                    _ => 1,
                };
                if s.config.retention == Retention::Interest {
                    for m in eligible.iter().filter(|m| {
                        m.sequence >= start
                            && initial.as_ref().is_none_or(|ids| ids.contains(&m.sequence))
                    }) {
                        s.interest.entry(m.sequence).or_default().insert(generation);
                    }
                }
                s.consumers.insert(
                    config.name.clone(),
                    Consumer {
                        config,
                        generation,
                        created: time,
                        cursor: start,
                        delivered: 0,
                        last_stream: 0,
                        ack_floor: 0,
                        ack_stream: 0,
                        pending: OrdMap::new(),
                        exhausted: OrdSet::new(),
                        history: OrdMap::new(),
                        num_pending: eligible
                            .iter()
                            .filter(|m| {
                                m.sequence >= start
                                    && initial.as_ref().is_none_or(|ids| ids.contains(&m.sequence))
                            })
                            .count(),
                        initial_cutoff: s.last_sequence,
                        initial,
                    },
                );
            }
            Event::ConsumerDelete {
                stream,
                name,
                generation,
            } => {
                let s = self.streams.get_mut(&stream).unwrap();
                s.consumers.remove(&name);
                if s.config.retention == Retention::Interest {
                    let ids: Vec<_> = s
                        .interest
                        .iter()
                        .filter(|(_, ids)| ids.contains(&generation))
                        .map(|(&n, _)| n)
                        .collect();
                    for n in ids {
                        s.acknowledge(n, generation);
                    }
                }
            }
            Event::Deliver {
                stream,
                consumer,
                generation,
                time,
                delivery: d,
            } => {
                self.time = self.time.max(time);
                let s = self.streams.get_mut(&stream).unwrap();
                s.reserve_delivery(&consumer, generation, &d, time);
            }

            Event::Ack {
                stream,
                consumer,
                generation,
                sequence,
                consumer_sequence,
                time,
                action,
            } => {
                self.time = self.time.max(time);
                let s = self.streams.get_mut(&stream).unwrap();
                let c = s.consumers.get_mut(&consumer).unwrap();
                c.acknowledge(sequence, consumer_sequence, time, action)
                    .for_each(|n| s.acknowledge(n, generation));
            }

            Event::Expire { time } => {
                self.time = time;
                for s in self.streams.values_mut() {
                    s.expire(time);
                }
            }
        }
        Ok(())
    }
}
pub fn time_parse(s: &str) -> Result<u64, Error> {
    time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
        .ok()
        .and_then(|t| u64::try_from(t.unix_timestamp_nanos()).ok())
        .ok_or(invalid("Invalid timestamp"))
}
// Checkpoints bound replay work; every preceding append is already durable.
const CHECKPOINT_RECORDS: usize = 8192;
const CHECKPOINT_BYTES: usize = 8 * 1024 * 1024;

/// Positions in the strict single-voter event log (not future Raft indexes).
/// Accepted work is private until its complete durable append has finished.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VoterProgress {
    pub accepted: u64,
    pub durable: u64,
    pub committed: u64,
    pub applied: u64,
    pub checkpoint: u64,
}

/// Owned persistence effect. It can run on a blocking I/O executor while the
/// engine retains its old committed projection. Dropping it leaves the engine
/// unavailable; an unknown write outcome must be recovered, never cancelled.
pub struct AppendTask<S> {
    store: S,
    plan: GroupPlan,
    token: std::sync::Arc<()>,
    expected: u64,
}

/// Only executing an AppendTask can construct this completion. Both successful
/// and failed completions return ownership of the same storage instance.
pub struct AppendCompletion<S> {
    task: AppendTask<S>,
    result: Result<u64, crate::storage::StoreError>,
}

impl<S: Store> AppendTask<S> {
    pub fn run(mut self) -> AppendCompletion<S> {
        // Strict mode never calls append_consumer_state, even for ACK-only work.
        let result = self
            .store
            .append_with_content(&self.plan.records, &self.plan.contents);
        AppendCompletion { task: self, result }
    }
}

pub struct Engine<S> {
    state: State,
    store: Option<S>,
    voter: Option<VoterProgress>,
    pending: Option<std::sync::Arc<()>>,
    checkpoint_sequence: u64,
    poisoned: std::cell::Cell<bool>,
    log_sequence: u64,
    since_checkpoint: usize,
    log_bytes: usize,
    identity: std::sync::Arc<()>,
    #[cfg(feature = "fault-injection")]
    committed_groups: u64,
}
impl<S: Store> Engine<S> {
    fn ensure_ready(&self) -> Result<(), Error> {
        if self.poisoned.get() {
            Err(invalid("Storage failed: restart required"))
        } else if self.pending.is_some() {
            Err(invalid("Persistence in flight"))
        } else {
            Ok(())
        }
    }
    fn storage(&self) -> Result<&S, Error> {
        self.store.as_ref().ok_or(invalid("Persistence in flight"))
    }
    pub fn voter_progress(&self) -> Option<VoterProgress> {
        self.voter
    }
    pub(crate) fn can_release_output(&self) -> bool {
        !self.poisoned.get()
            && self.pending.is_none()
            && self.voter.is_none_or(|p| p.applied == p.committed)
    }
    /// Strict, fixed one-voter mode. Every durable append is committed by the
    /// sole voter. This format must not be opened as a multi-voter accepted log.
    pub fn recover_single_voter(store: S) -> Result<Self, Error> {
        if store.buffers_consumer_state() || !store.durable_appends() || !store.supports_batch() {
            return Err(invalid("Single-voter mode requires durable atomic appends and forbids unsafe ACK buffering"));
        }
        let mut engine = Self::recover(store)?;
        let end = engine.log_sequence;
        engine.voter = Some(VoterProgress {
            accepted: end,
            durable: end,
            committed: end,
            applied: end,
            checkpoint: engine.checkpoint_sequence,
        });
        Ok(engine)
    }
    pub fn state(&self) -> &State {
        &self.state
    }
    /// Resolve a selected message without retaining its body in the model.
    pub fn read_message(&self, message: &StoredMessage) -> Result<StoredMessage, Error> {
        self.ensure_ready()?;
        let result = hydrate_message(self.storage()?, message);
        if result.is_err() {
            self.poisoned.set(true);
        }
        result
    }
    /// Resolve a bounded selection before committing its reservation/retention effects.
    pub fn read_messages(&self, messages: &[&StoredMessage]) -> Result<Vec<StoredMessage>, Error> {
        self.ensure_ready()?;
        let result = (|| {
            if let Some(keys) = messages
                .iter()
                .map(|m| m.content.as_ref().map(|r| r.key))
                .collect::<Option<Vec<_>>>()
            {
                let contents = self.storage()?.get_contents(&keys)?;
                if contents.len() != messages.len() {
                    return Err(invalid("Invalid content batch length"));
                }
                messages
                    .iter()
                    .zip(contents)
                    .map(|(m, c)| hydrate_content(m, c))
                    .collect()
            } else {
                messages
                    .iter()
                    .map(|m| hydrate_message(self.storage()?, m))
                    .collect()
            }
        })();
        if result.is_err() {
            self.poisoned.set(true);
        }
        result
    }
    pub fn is_poisoned(&self) -> bool {
        self.poisoned.get()
    }
    pub(crate) fn buffers_consumer_state(&self) -> bool {
        self.store
            .as_ref()
            .is_some_and(Store::buffers_consumer_state)
    }
    pub fn flush_consumer_state(&mut self, force: bool) -> Result<(), Error> {
        self.ensure_ready()?;
        if let Err(error) = self
            .store
            .as_mut()
            .expect("storage available")
            .flush_consumer_state(force)
        {
            self.poisoned.set(true);
            return Err(error.into());
        }
        Ok(())
    }
    /// Return storage after completing any outstanding append.
    ///
    /// # Panics
    /// Panics if an AppendTask or its completion still owns the storage.
    pub fn into_store(self) -> S {
        self.store
            .expect("append in flight: complete it before taking storage")
    }
    pub fn recover(store: S) -> Result<Self, Error> {
        let last = store.last_sequence()?;
        let (base, mut state) = if let Some((seq, bytes)) = store.load_checkpoint()? {
            let snapshot = decode_snapshot(&bytes)?;
            if snapshot.version != 3 || seq > last {
                return Err(invalid("Invalid snapshot version or position"));
            }
            snapshot.state.check_invariants()?;
            (seq, snapshot.state)
        } else {
            (0, State::default())
        };
        let mut log_bytes = 0usize;
        for seq in base + 1..=last {
            let record = store
                .get(seq)?
                .ok_or(invalid("Missing committed log record"))?;
            log_bytes = log_bytes
                .saturating_add(record.headers.len())
                .saturating_add(record.payload.len());
            let event = match record.subject.as_str() {
                "$SCREECH.event.v1" => serde_json::from_slice(&record.payload)?,
                "$SCREECH.event.v2" | "$SCREECH.event.v3" => {
                    let mut event: Event = serde_json::from_slice(&record.headers)?;
                    if let Event::Publish { message, .. } = &mut event {
                        message.payload = record.payload;
                    } else if !record.payload.is_empty() {
                        return Err(invalid("Unexpected event payload"));
                    }
                    event
                }
                _ => return Err(invalid("Unknown event version")),
            };
            let mut event = event;
            let detached = if let Event::Publish { stream, message } = &mut event {
                if let Some(reference) = message.content.clone() {
                    if state
                        .streams
                        .get(stream)
                        .is_none_or(|s| s.generation != reference.key.generation)
                    {
                        return Err(invalid("Invalid content generation"));
                    }
                    *message = hydrate_message(&store, message)?;
                    Some((stream.clone(), message.sequence, reference))
                } else {
                    None
                }
            } else {
                None
            };
            state.apply(event)?;
            if let Some((stream, sequence, reference)) = detached {
                detach_message(&mut state, &stream, sequence, reference);
            }
        }
        for message in state.streams.values().flat_map(|s| s.messages.values()) {
            if let Some(reference) = &message.content {
                store.validate_content(
                    reference.key,
                    &message.subject,
                    reference.headers_len,
                    reference.payload_len,
                )?;
            }
        }
        let migrate = store.external_content()
            && state
                .streams
                .values()
                .any(|s| s.messages.values().any(|m| m.content.is_none()));
        let mut engine = Self {
            state,
            store: Some(store),
            voter: None,
            pending: None,
            checkpoint_sequence: base,
            poisoned: std::cell::Cell::new(false),
            log_sequence: last,
            since_checkpoint: (last - base) as usize,
            log_bytes,
            identity: std::sync::Arc::new(()),
            #[cfg(feature = "fault-injection")]
            committed_groups: 0,
        };
        if migrate {
            engine.checkpoint()?;
        }
        Ok(engine)
    }
    pub fn checkpoint(&mut self) -> Result<(), Error> {
        self.ensure_ready()?;
        if let Some(voter) = &mut self.voter {
            let state = self.state.clone();
            let result = self
                .store
                .as_mut()
                .expect("storage available")
                .checkpoint_job(
                    self.log_sequence,
                    Box::new(move || {
                        let live = state
                            .streams
                            .values()
                            .flat_map(|s| s.messages.values())
                            .filter_map(|m| m.content.as_ref().map(|r| r.key))
                            .collect();
                        let data = postcard::to_allocvec(&SnapshotRef {
                            version: 3,
                            state: &state,
                        })
                        .map_err(|e| crate::storage::StoreError::Io(std::io::Error::other(e)))?;
                        Ok(crate::storage::CheckpointImage {
                            data,
                            live,
                            imports: Vec::new(),
                        })
                    }),
                );
            if let Err(error) = result {
                self.poisoned.set(true);
                return Err(error.into());
            }
            self.checkpoint_sequence = self.log_sequence;
            voter.checkpoint = self.log_sequence;
            self.since_checkpoint = 0;
            self.log_bytes = 0;
            return Ok(());
        }
        let mut imports = Vec::new();
        if self.storage()?.external_content() {
            for stream in self.state.streams.values_mut() {
                let sequences: Vec<_> = stream
                    .messages
                    .iter()
                    .filter_map(|(&sequence, message)| {
                        message.content.is_none().then_some(sequence)
                    })
                    .collect();
                for sequence in sequences {
                    let message = stream.messages.get_mut(&sequence).unwrap();
                    if message.content.is_none() {
                        let (reference, content) = content_parts(stream.generation, message);
                        imports.push(content);
                        message.headers = Bytes::new();
                        message.payload = Bytes::new();
                        message.content = Some(reference);
                    }
                }
            }
        }
        let data = postcard::to_allocvec(&SnapshotRef {
            version: 3,
            state: &self.state,
        })?;
        let result = if self.storage()?.external_content() {
            let mut live = self
                .state
                .streams
                .values()
                .flat_map(|s| s.messages.values())
                .filter_map(|m| m.content.as_ref().map(|r| r.key));
            self.store
                .as_mut()
                .expect("storage available")
                .checkpoint_with_content(self.log_sequence, &data, &mut live, &imports)
        } else {
            self.store
                .as_mut()
                .expect("storage available")
                .checkpoint(self.log_sequence, &data)
        };
        if let Err(e) = result {
            self.poisoned.set(true);
            return Err(e.into());
        }
        self.checkpoint_sequence = self.log_sequence;
        if let Some(voter) = &mut self.voter {
            voter.checkpoint = self.log_sequence;
        }
        self.since_checkpoint = 0;
        self.log_bytes = 0;
        Ok(())
    }
    #[cfg(feature = "fault-injection")]
    pub fn committed_groups(&self) -> u64 {
        self.committed_groups
    }
    pub fn supports_groups(&self) -> bool {
        self.store.as_ref().is_some_and(Store::supports_batch)
    }
    pub fn begin_group(&self) -> Result<GroupPlan, Error> {
        if self.poisoned.get() || self.pending.is_some() || !self.storage()?.supports_batch() {
            return Err(invalid("Storage cannot plan a group"));
        }
        Ok(GroupPlan {
            state: self.state.clone(),
            identity: self.identity.clone(),
            base: self.log_sequence,
            #[cfg(feature = "fault-injection")]
            events: Vec::with_capacity(32),
            records: Vec::with_capacity(32),
            contents: Vec::new(),
            external_content: self.storage()?.external_content(),
            bytes: 0,
            log_bytes: 0,
            consumer_only: true,
            failed: false,
        })
    }
    /// Admit one prepared proposal and hand storage ownership to the executor.
    /// No model changes or outputs are published here. Empty read-only groups
    /// need no persistence and are observed against the current committed state.
    pub fn prepare_append(&mut self, plan: GroupPlan) -> Result<Option<AppendTask<S>>, Error> {
        if self.voter.is_none()
            || self.poisoned.get()
            || self.pending.is_some()
            || plan.failed
            || !std::sync::Arc::ptr_eq(&self.identity, &plan.identity)
            || plan.base != self.log_sequence
        {
            return Err(invalid("Stale, foreign or failed single-voter proposal"));
        }
        if plan.is_empty() {
            return Ok(None);
        }
        let expected = self
            .log_sequence
            .checked_add(plan.len() as u64)
            .ok_or(invalid("Log exhausted"))?;
        if plan.len() > 1 {
            #[cfg(feature = "fault-injection")]
            crate::fault::group_context(self.log_sequence, &plan.records, &plan.events);
            crate::fault::point("before_group_commit");
        }
        let token = std::sync::Arc::new(());
        self.pending = Some(token.clone());
        self.voter.as_mut().unwrap().accepted = expected;
        Ok(Some(AppendTask {
            store: self.store.take().expect("no other persistence in flight"),
            plan,
            token,
            expected,
        }))
    }

    /// Release a proposal only after its matching durable completion. A failed
    /// append is ambiguous: retain the old projection and require recovery.
    pub fn complete_append(&mut self, completion: AppendCompletion<S>) -> Result<(), Error> {
        let AppendCompletion { task, result } = completion;
        if !self
            .pending
            .as_ref()
            .is_some_and(|t| std::sync::Arc::ptr_eq(t, &task.token))
        {
            return Err(invalid("Foreign persistence completion"));
        }
        self.store = Some(task.store);
        self.pending = None;
        if self.poisoned.get() {
            return Err(invalid("Storage failed: restart required"));
        }
        match result {
            Ok(end) if end == task.expected => {
                self.log_sequence = end;
                let voter = self.voter.as_mut().unwrap();
                voter.durable = end;
                voter.committed = end;
            }
            Ok(_) => {
                self.poisoned.set(true);
                return Err(invalid("Invalid single-voter append position"));
            }
            Err(error) => {
                self.poisoned.set(true);
                return Err(error.into());
            }
        }
        self.install_group(task.plan)
    }

    pub fn commit_group(&mut self, plan: GroupPlan) -> Result<(), Error> {
        if self.voter.is_some() {
            if let Some(task) = self.prepare_append(plan)? {
                self.complete_append(task.run())?;
            }
            return Ok(());
        }
        if self.poisoned.get()
            || self.pending.is_some()
            || plan.failed
            || !std::sync::Arc::ptr_eq(&self.identity, &plan.identity)
            || plan.base != self.log_sequence
        {
            return Err(invalid("Stale, foreign or failed group"));
        }
        if plan.records.is_empty() {
            return Ok(());
        }
        if plan.records.len() > 1 {
            #[cfg(feature = "fault-injection")]
            crate::fault::group_context(self.log_sequence, &plan.records, &plan.events);
            crate::fault::point("before_group_commit");
        }
        let expected = self
            .log_sequence
            .checked_add(plan.records.len() as u64)
            .ok_or(invalid("Log exhausted"))?;
        let store = self
            .store
            .as_mut()
            .ok_or(invalid("Persistence in flight"))?;
        let result = if plan.consumer_only {
            store.append_consumer_state(&plan.records)
        } else if !plan.contents.is_empty() {
            store.append_with_content(&plan.records, &plan.contents)
        } else if plan.records.len() == 1 {
            let r = &plan.records[0];
            store.append(&r.subject, &r.headers, &r.payload)
        } else {
            store.append_batch(&plan.records)
        };
        match result {
            Ok(end) if end == expected => self.log_sequence = end,
            Ok(_) => {
                self.poisoned.set(true);
                return Err(invalid("Invalid grouped append position"));
            }
            Err(error) => {
                self.poisoned.set(true);
                return Err(error.into());
            }
        }
        self.install_group(plan)
    }
    fn install_group(&mut self, plan: GroupPlan) -> Result<(), Error> {
        if plan.records.len() > 1 {
            #[cfg(feature = "fault-injection")]
            {
                self.committed_groups += 1;
            }
            crate::fault::point("after_group_commit");
        }
        #[cfg(feature = "fault-injection")]
        for event in &plan.events {
            crate::fault::operation("after", operation(event));
            crate::fault::batch_point("after", event);
        }
        self.state = plan.state;
        if let Some(voter) = &mut self.voter {
            voter.applied = self.log_sequence;
        }
        self.since_checkpoint += plan.records.len();
        self.log_bytes += plan.log_bytes;
        if self.storage()?.supports_checkpoint()
            && (self.since_checkpoint >= CHECKPOINT_RECORDS || self.log_bytes >= CHECKPOINT_BYTES)
        {
            self.checkpoint()?;
        }
        Ok(())
    }
    pub fn commit(&mut self, event: Event) -> Result<(), Error> {
        if self.voter.is_some() {
            let mut plan = self.begin_group()?;
            plan.commit(event)?;
            return self.commit_group(plan);
        }
        self.ensure_ready()?;
        self.state.validate(&event)?;
        let operation = operation(&event);
        crate::fault::operation("before", operation);
        crate::fault::batch_point("before", &event);
        let (record, content, detached) =
            encode_transition(&self.state, &event, self.storage()?.external_content())?;
        let encoded_bytes = record.headers.len() + record.payload.len();
        let store = self
            .store
            .as_mut()
            .ok_or(invalid("Persistence in flight"))?;
        let result = if consumer_state_event(&event) {
            store.append_consumer_state(std::slice::from_ref(&record))
        } else if let Some(content) = content {
            store.append_with_content(std::slice::from_ref(&record), &[content])
        } else {
            store.append(&record.subject, &record.headers, &record.payload)
        };
        match result {
            Ok(seq) => self.log_sequence = seq,
            Err(e) => {
                self.poisoned.set(true);
                return Err(e.into());
            }
        }
        crate::fault::operation("after", operation);
        crate::fault::batch_point("after", &event);
        if let Err(e) = self.state.apply_validated(event) {
            self.poisoned.set(true);
            return Err(e);
        }
        if let Some((stream, sequence, reference)) = detached {
            detach_message(&mut self.state, &stream, sequence, reference);
        }
        self.since_checkpoint += 1;
        self.log_bytes += encoded_bytes;
        if self.storage()?.supports_checkpoint()
            && (self.since_checkpoint >= CHECKPOINT_RECORDS || self.log_bytes >= CHECKPOINT_BYTES)
        {
            self.checkpoint()?;
        }
        Ok(())
    }
}
/// A private, disposable projection. The engine's committed model is untouched
/// until commit_group succeeds; callers must defer every observable side effect.
pub struct GroupPlan {
    state: State,
    identity: std::sync::Arc<()>,
    base: u64,
    #[cfg(feature = "fault-injection")]
    events: Vec<Event>,
    records: Vec<crate::storage::Append>,
    contents: Vec<Content>,
    external_content: bool,
    bytes: usize,
    log_bytes: usize,
    consumer_only: bool,
    failed: bool,
}
impl GroupPlan {
    pub fn state(&self) -> &State {
        &self.state
    }
    pub fn len(&self) -> usize {
        self.records.len()
    }
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
    pub fn has_staged_content(&self) -> bool {
        !self.contents.is_empty()
    }
    pub fn read_staged_message(
        &self,
        message: &StoredMessage,
    ) -> Option<Result<StoredMessage, Error>> {
        let key = message.content.as_ref()?.key;
        self.contents
            .iter()
            .rev()
            .find(|c| c.key == key)
            .map(|c| hydrate_content(message, c.clone()))
    }
    pub fn commit(&mut self, event: Event) -> Result<(), Error> {
        if self.failed {
            return Err(invalid("Failed group"));
        }
        self.state.validate(&event)?;
        let (record, content, detached) =
            encode_transition(&self.state, &event, self.external_content)?;
        let bytes = record
            .headers
            .len()
            .saturating_add(record.payload.len())
            .saturating_add(
                content
                    .as_ref()
                    .map(|c| c.headers.len() + c.payload.len())
                    .unwrap_or(0),
            );
        if self.records.len() >= 8192 || bytes > (32 * 1024 * 1024usize).saturating_sub(self.bytes)
        {
            self.failed = true;
            return Err(invalid("Group capacity exceeded"));
        }
        crate::fault::operation("before", operation(&event));
        crate::fault::batch_point("before", &event);
        self.consumer_only &= consumer_state_event(&event);
        #[cfg(feature = "fault-injection")]
        self.events.push(event.clone());
        if let Err(error) = self.state.apply_validated(event) {
            self.failed = true;
            return Err(error);
        }
        if let Some((stream, sequence, reference)) = detached {
            detach_message(&mut self.state, &stream, sequence, reference);
        }
        if let Some(content) = content {
            self.contents.push(content);
        }
        self.bytes += bytes;
        self.log_bytes += record.headers.len() + record.payload.len();
        self.records.push(record);
        Ok(())
    }
}
fn operation(event: &Event) -> &'static str {
    match event {
        Event::ConsumerCreate { .. }
        | Event::ConsumerDelete { .. }
        | Event::ConsumerUpdate { .. } => "consumer",
        Event::Deliver { .. } | Event::DeliverBatch { .. } => "delivery",
        Event::Ack { .. } | Event::AckBatch { .. } => "ack",
        Event::AckNext { .. } => "ack_next",
        Event::Expire { .. } => "expiration",
        _ => "publish",
    }
}
fn consumer_state_event(event: &Event) -> bool {
    matches!(
        event,
        Event::Deliver { .. }
            | Event::DeliverBatch { .. }
            | Event::Ack { .. }
            | Event::AckBatch { .. }
            | Event::AckNext { .. }
    )
}
fn content_parts(generation: u64, message: &StoredMessage) -> (ContentRef, Content) {
    let key = ContentKey {
        generation,
        sequence: message.sequence,
    };
    (
        ContentRef {
            key,
            headers_len: message.headers.len() as u64,
            payload_len: message.payload.len() as u64,
        },
        Content {
            key,
            subject: message.subject.clone(),
            headers: message.headers.clone(),
            payload: message.payload.clone(),
        },
    )
}
fn detach_message(state: &mut State, stream: &str, sequence: u64, reference: ContentRef) {
    if let Some(message) = state
        .streams
        .get_mut(stream)
        .and_then(|s| s.messages.get_mut(&sequence))
    {
        message.headers = Bytes::new();
        message.payload = Bytes::new();
        message.content = Some(reference);
    }
}
fn hydrate_content(message: &StoredMessage, content: Content) -> Result<StoredMessage, Error> {
    let reference = message
        .content
        .as_ref()
        .ok_or(invalid("Missing content reference"))?;
    if content.key != reference.key
        || content.subject != message.subject
        || content.headers.len() as u64 != reference.headers_len
        || content.payload.len() as u64 != reference.payload_len
    {
        return Err(StoreError::Corrupt(reference.key.sequence).into());
    }
    Ok(StoredMessage {
        sequence: message.sequence,
        subject: content.subject,
        headers: content.headers,
        payload: content.payload,
        time: message.time,
        content: None,
    })
}
fn hydrate_message(store: &impl Store, message: &StoredMessage) -> Result<StoredMessage, Error> {
    let Some(reference) = &message.content else {
        return Ok(message.clone());
    };
    let content = store
        .get_content(reference.key)?
        .ok_or(StoreError::Corrupt(reference.key.sequence))?;
    hydrate_content(message, content)
}
type DetachedMessage = (String, u64, ContentRef);
fn encode_transition(
    state: &State,
    event: &Event,
    external: bool,
) -> Result<
    (
        crate::storage::Append,
        Option<Content>,
        Option<DetachedMessage>,
    ),
    Error,
> {
    if external {
        if let Event::Publish { stream, message } = event {
            let (reference, content) = content_parts(state.streams[stream].generation, message);
            let mut metadata = message.clone();
            metadata.headers = Bytes::new();
            metadata.payload = Bytes::new();
            metadata.content = Some(reference.clone());
            return Ok((
                crate::storage::Append {
                    subject: "$SCREECH.event.v3".into(),
                    headers: serde_json::to_vec(&Event::Publish {
                        stream: stream.clone(),
                        message: metadata,
                    })?
                    .into(),
                    payload: Bytes::new(),
                },
                Some(content),
                Some((stream.clone(), message.sequence, reference)),
            ));
        }
    }
    Ok((encode_event(event)?, None, None))
}
fn encode_event(event: &Event) -> Result<crate::storage::Append, Error> {
    let (bytes, payload) = if let Event::Publish { stream, message } = event {
        let mut metadata = message.clone();
        metadata.payload = Bytes::new();
        (
            serde_json::to_vec(&Event::Publish {
                stream: stream.clone(),
                message: metadata,
            })?,
            message.payload.clone(),
        )
    } else {
        let mut bytes = Vec::with_capacity(256);
        serde_json::to_writer(&mut bytes, &event)?;
        (bytes, Bytes::new())
    };
    Ok(crate::storage::Append {
        subject: "$SCREECH.event.v2".into(),
        headers: bytes.into(),
        payload,
    })
}
#[derive(Serialize)]
struct SnapshotRef<'a> {
    version: u8,
    state: &'a State,
}
#[derive(Deserialize)]
struct Snapshot {
    version: u8,
    state: State,
}

#[path = "durable_legacy.rs"]
mod legacy;
fn decode_snapshot(bytes: &[u8]) -> Result<Snapshot, Error> {
    match bytes.first() {
        Some(3) => Ok(postcard::from_bytes(bytes)?),
        Some(2) => {
            let old: legacy::LegacySnapshot = postcard::from_bytes(bytes)?;
            if old.version != 2 {
                return Err(invalid("Invalid snapshot version"));
            }
            Ok(Snapshot {
                version: 3,
                state: old.state.into_current(),
            })
        }
        _ => Err(invalid("Invalid snapshot version")),
    }
}
