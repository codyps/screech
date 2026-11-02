//! Pure Core NATS routing model: no I/O, locks, clocks, runtime, or storage.
use crate::parser::{subject, token};
use bytes::Bytes;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};
pub type ClientId = u64;
type Key = (ClientId, Arc<str>);
#[derive(Clone, Debug)]
pub struct Message {
    pub subject: String,
    pub reply: Option<String>,
    pub headers: Bytes,
    pub payload: Bytes,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Delivery {
    pub client: ClientId,
    pub sid: String,
}
#[derive(Default)]
struct Subscription {
    subject: String,
    queue: Option<Arc<str>>,
    delivered: u64,
    maximum: Option<u64>,
}
#[derive(Default)]
struct Routes {
    direct: Vec<Key>,
    groups: Vec<(Arc<str>, Vec<Key>)>,
}
// Arena edges are indices: matching, pruning and dropping deep filters never recurse.
#[derive(Default)]
struct TokenNode {
    literals: HashMap<String, usize>,
    star: Option<usize>,
    terminal: BTreeSet<Key>,
    tail: BTreeSet<Key>,
}
impl TokenNode {
    fn is_empty(&self) -> bool {
        self.literals.is_empty()
            && self.star.is_none()
            && self.terminal.is_empty()
            && self.tail.is_empty()
    }
}
struct WildcardIndex {
    nodes: Vec<Option<TokenNode>>,
    free: Vec<usize>,
    count: usize,
}
impl Default for WildcardIndex {
    fn default() -> Self {
        Self {
            nodes: vec![Some(TokenNode::default())],
            free: Vec::new(),
            count: 0,
        }
    }
}
impl WildcardIndex {
    fn is_empty(&self) -> bool {
        self.count == 0
    }
    fn allocate(&mut self) -> usize {
        if let Some(index) = self.free.pop() {
            self.nodes[index] = Some(TokenNode::default());
            index
        } else {
            self.nodes.push(Some(TokenNode::default()));
            self.nodes.len() - 1
        }
    }
    fn insert(&mut self, filter: &str, key: Key) {
        let mut at = 0;
        for token in filter.split('.') {
            if token == ">" {
                self.count += usize::from(self.nodes[at].as_mut().unwrap().tail.insert(key));
                return;
            }
            let node = self.nodes[at].as_ref().unwrap();
            let next = if token == "*" {
                node.star
            } else {
                node.literals.get(token).copied()
            };
            at = match next {
                Some(next) => next,
                None => {
                    let next = self.allocate();
                    let node = self.nodes[at].as_mut().unwrap();
                    if token == "*" {
                        node.star = Some(next);
                    } else {
                        node.literals.insert(token.into(), next);
                    }
                    next
                }
            };
        }
        self.count += usize::from(self.nodes[at].as_mut().unwrap().terminal.insert(key));
    }
    fn remove(&mut self, filter: &str, key: &Key) {
        let mut at = 0;
        let mut path = Vec::new();
        let mut tail = false;
        for token in filter.split('.') {
            if token == ">" {
                tail = true;
                break;
            }
            let node = self.nodes[at].as_ref().unwrap();
            let next = if token == "*" {
                node.star
            } else {
                node.literals.get(token).copied()
            };
            let Some(next) = next else {
                return;
            };
            path.push((at, token, next));
            at = next;
        }
        let node = self.nodes[at].as_mut().unwrap();
        let removed = if tail {
            node.tail.remove(key)
        } else {
            node.terminal.remove(key)
        };
        if !removed {
            return;
        }
        self.count -= 1;
        for (parent, token, child) in path.into_iter().rev() {
            if !self.nodes[child].as_ref().unwrap().is_empty() {
                break;
            }
            let node = self.nodes[parent].as_mut().unwrap();
            if token == "*" {
                node.star = None;
            } else {
                node.literals.remove(token);
            }
            self.nodes[child] = None;
            self.free.push(child);
        }
    }
    // Early-exit interest checks need no result vector. The branch stack allocates
    // only when both a literal and '*' edge match the same subject token.
    fn any_matching(&self, subject: &str, mut test: impl FnMut(&Key) -> bool) -> bool {
        if self.is_empty() {
            return false;
        }
        let mut pending = Vec::new();
        let mut branch = (0, subject.split('.'));
        loop {
            let (at, mut tokens) = branch;
            let node = self.nodes[at].as_ref().unwrap();
            if let Some(token) = tokens.next() {
                // '>' consumes at least one remaining token.
                if node.tail.iter().any(&mut test) {
                    return true;
                }
                let literal = node.literals.get(token).copied();
                match (literal, node.star) {
                    (Some(next), Some(star)) => {
                        pending.push((star, tokens.clone()));
                        branch = (next, tokens);
                        continue;
                    }
                    (Some(next), None) | (None, Some(next)) => {
                        branch = (next, tokens);
                        continue;
                    }
                    (None, None) => (),
                }
            } else if node.terminal.iter().any(&mut test) {
                return true;
            }
            let Some(next) = pending.pop() else {
                return false;
            };
            branch = next;
        }
    }
}
fn wildcard(filter: &str) -> bool {
    filter.split('.').any(|t| t == "*" || t == ">")
}

#[derive(Default)]
pub struct Core {
    clients: BTreeMap<ClientId, bool>,
    subs: BTreeMap<Key, Subscription>,
    exact: HashMap<String, BTreeSet<Key>>,
    wild: WildcardIndex,
    queues: HashMap<Arc<str>, usize>,
    cache: HashMap<String, Arc<Routes>>,
    cache_bytes: usize,
}
impl Core {
    pub fn has_interest(&self, subject: &str) -> bool {
        self.has_interest_in_queue(subject, None)
    }
    pub fn has_interest_in_queue(&self, subject: &str, queue: Option<&str>) -> bool {
        let eligible = |k: &Key| queue.is_none_or(|q| self.subs[k].queue.as_deref() == Some(q));
        self.exact
            .get(subject)
            .is_some_and(|keys| keys.iter().any(eligible))
            || self.wild.any_matching(subject, eligible)
    }
    pub fn connect(&mut self, client: ClientId, echo: bool) {
        self.clients.insert(client, echo);
    }
    pub fn disconnect(&mut self, client: ClientId) {
        let keys: Vec<_> = self
            .subs
            .keys()
            .filter(|(id, _)| *id == client)
            .cloned()
            .collect();
        for key in keys {
            self.remove(&key);
        }
        self.clients.remove(&client);
    }
    pub fn has_subscription(&self, client: ClientId, sid: &str) -> bool {
        self.subs.contains_key(&(client, Arc::from(sid)))
    }
    pub fn subscription_count(&self, client: ClientId) -> usize {
        self.subs.keys().filter(|(id, _)| *id == client).count()
    }
    pub fn subscribe(
        &mut self,
        client: ClientId,
        sid: String,
        filter: String,
        queue: Option<String>,
    ) -> Result<(), &'static str> {
        if !self.clients.contains_key(&client)
            || !token(&sid)
            || !subject(&filter, true)
            || queue.as_ref().is_some_and(|q| !token(q))
        {
            return Err("Invalid subscription");
        }
        let key = (client, Arc::from(sid));
        // NATS ignores a duplicate SID, preserving the existing subscription.
        if self.subs.contains_key(&key) {
            return Ok(());
        }
        self.cache.clear();
        self.cache_bytes = 0;
        if wildcard(&filter) {
            self.wild.insert(&filter, key.clone());
        } else {
            self.exact
                .entry(filter.clone())
                .or_default()
                .insert(key.clone());
        }
        self.subs.insert(
            key,
            Subscription {
                subject: filter,
                queue: queue.map(Arc::from),
                ..Default::default()
            },
        );
        Ok(())
    }
    fn remove(&mut self, key: &Key) {
        if let Some(sub) = self.subs.remove(key) {
            self.cache.clear();
            self.cache_bytes = 0;
            if wildcard(&sub.subject) {
                self.wild.remove(&sub.subject, key);
            } else if let Some(keys) = self.exact.get_mut(&sub.subject) {
                keys.remove(key);
                if keys.is_empty() {
                    self.exact.remove(&sub.subject);
                }
            }
            if let Some(q) = sub.queue {
                if !self.subs.values().any(|s| s.queue.as_ref() == Some(&q)) {
                    self.queues.remove(&q);
                }
            }
        }
    }
    pub fn unsubscribe(&mut self, client: ClientId, sid: &str, maximum: Option<u64>) {
        let key = (client, Arc::from(sid));
        if let Some(sub) = self.subs.get_mut(&key) {
            if maximum.is_some_and(|n| n > sub.delivered) {
                sub.maximum = maximum;
                return;
            }
        }
        self.remove(&key);
    }
    pub fn publish(&mut self, publisher: ClientId, subject: &str) -> Vec<Delivery> {
        self.publish_in_queue(publisher, subject, None)
    }
    pub fn publish_in_queue(
        &mut self,
        publisher: ClientId,
        subject: &str,
        queue: Option<&str>,
    ) -> Vec<Delivery> {
        let mut result = Vec::new();
        self.route_with(publisher, subject, queue, |client, sid| {
            result.push(Delivery {
                client,
                sid: sid.to_string(),
            })
        });
        result
    }
    fn routes(&mut self, subject: &str) -> Arc<Routes> {
        if let Some(routes) = self.cache.get(subject) {
            return routes.clone();
        }
        let mut direct = Vec::new();
        let mut groups = BTreeMap::<Arc<str>, Vec<Key>>::new();
        for key in self.exact.get(subject).into_iter().flatten() {
            if let Some(queue) = &self.subs[key].queue {
                groups.entry(queue.clone()).or_default().push(key.clone());
            } else {
                direct.push(key.clone());
            }
        }
        let exact_direct = direct.len();
        self.wild.any_matching(subject, |key| {
            if let Some(queue) = &self.subs[key].queue {
                groups.entry(queue.clone()).or_default().push(key.clone());
            } else {
                direct.push(key.clone());
            }
            false
        });
        // Preserve exact-first direct delivery and stable wildcard/queue order,
        // independently of the token traversal's branch order.
        direct[exact_direct..].sort_unstable();
        let mut groups: Vec<_> = groups.into_iter().collect();
        for (_, keys) in &mut groups {
            keys.sort();
        }
        let size = 128
            + groups.capacity() * std::mem::size_of::<(Arc<str>, Vec<Key>)>()
            + subject.len()
            + (direct.capacity()
                + groups
                    .iter()
                    .map(|(_, keys)| keys.capacity())
                    .sum::<usize>())
                * std::mem::size_of::<Key>();
        let routes = Arc::new(Routes { direct, groups });
        if size <= 4 * 1024 * 1024 {
            if self.cache.len() >= 4096 || self.cache_bytes + size > 4 * 1024 * 1024 {
                self.cache.clear();
                self.cache_bytes = 0;
            }
            self.cache_bytes += size;
            self.cache.insert(subject.into(), routes.clone());
        }
        routes
    }
    pub fn route_with(
        &mut self,
        publisher: ClientId,
        subject: &str,
        queue: Option<&str>,
        mut deliver: impl FnMut(ClientId, &Arc<str>),
    ) -> usize {
        if self.wild.is_empty() && !self.exact.contains_key(subject) {
            return 0;
        }
        let routes = self.routes(subject);
        let skip_echo = self.clients.get(&publisher) == Some(&false);
        let mut count = 0;
        if queue.is_none() {
            for key in &routes.direct {
                if skip_echo && key.0 == publisher {
                    continue;
                }
                self.deliver(key, &mut deliver);
                count += 1;
            }
        }
        for (group, keys) in &routes.groups {
            if queue.is_some_and(|q| q != group.as_ref()) {
                continue;
            }
            let eligible = |key: &&Key| !skip_echo || key.0 != publisher;
            let n = keys.iter().filter(eligible).count();
            if n == 0 {
                continue;
            }
            let cursor = self.queues.entry(group.clone()).or_default();
            let index = *cursor % n;
            *cursor = cursor.wrapping_add(1);
            let key = keys
                .iter()
                .filter(eligible)
                .nth(index)
                .expect("eligible queue member");
            self.deliver(key, &mut deliver);
            count += 1;
        }
        count
    }
    fn deliver(&mut self, key: &Key, callback: &mut impl FnMut(ClientId, &Arc<str>)) {
        let sub = self.subs.get_mut(key).expect("indexed subscription");
        sub.delivered += 1;
        let remove = sub.maximum.is_some_and(|n| sub.delivered >= n);
        callback(key.0, &key.1);
        if remove {
            self.remove(key);
        }
    }
    pub fn routing_cache_usage(&self) -> (usize, usize) {
        (self.cache.len(), self.cache_bytes)
    }
}
/// `>` consumes at least one token; wildcards are whole tokens.
pub fn matches(filter: &str, subject: &str) -> bool {
    let mut input = subject.split('.');
    for token in filter.split('.') {
        match (token, input.next()) {
            (">", Some(_)) => return true,
            ("*", Some(_)) => (),
            (a, Some(b)) if a == b => (),
            _ => return false,
        }
    }
    input.next().is_none()
}

#[cfg(test)]
mod index_tests {
    use super::*;
    #[test]
    fn removed_prefixes_are_pruned_and_arena_slots_reused() {
        let mut index = WildcardIndex::default();
        let key = (1, Arc::from("s"));
        for n in 0..10000 {
            let filter = format!("root.{n}.*.>");
            index.insert(&filter, key.clone());
            assert!(index.any_matching(&format!("root.{n}.a.b"), |_| true));
            index.remove(&filter, &key);
            assert!(index.is_empty());
            assert!(index.nodes[0].as_ref().unwrap().is_empty());
            assert_eq!(index.nodes.len(), 4);
            assert_eq!(index.free.len(), 3);
        }
    }
}
