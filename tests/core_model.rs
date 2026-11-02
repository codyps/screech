//! Scan-based oracle: no routing index, route cache, or production matcher.
use proptest::prelude::*;
use screech::core::Core;
use std::collections::BTreeMap;

type Key = (u64, String);
struct Sub {
    filter: String,
    queue: Option<String>,
    delivered: u64,
    maximum: Option<u64>,
}
#[derive(Default)]
struct Reference {
    clients: BTreeMap<u64, bool>,
    subs: BTreeMap<Key, Sub>,
    cursors: BTreeMap<String, usize>,
}
fn hit(filter: &str, subject: &str) -> bool {
    let f: Vec<_> = filter.split('.').collect();
    let s: Vec<_> = subject.split('.').collect();
    let tail = f.last() == Some(&">");
    let n = f.len() - usize::from(tail);
    (if tail { s.len() > n } else { s.len() == n })
        && f[..n].iter().zip(&s).all(|(a, b)| *a == "*" || a == b)
}
impl Reference {
    fn remove(&mut self, key: &Key) {
        if let Some(sub) = self.subs.remove(key) {
            if let Some(queue) = sub.queue {
                if !self.subs.values().any(|s| s.queue.as_ref() == Some(&queue)) {
                    self.cursors.remove(&queue);
                }
            }
        }
    }
    fn unsubscribe(&mut self, key: &Key, maximum: Option<u64>) {
        if let Some(sub) = self.subs.get_mut(key) {
            if maximum.is_some_and(|n| n > sub.delivered) {
                sub.maximum = maximum;
                return;
            }
        }
        self.remove(key);
    }
    fn deliver(&mut self, key: &Key) {
        let sub = self.subs.get_mut(key).unwrap();
        sub.delivered += 1;
        if sub.maximum.is_some_and(|n| sub.delivered >= n) {
            self.remove(key);
        }
    }
    fn publish(&mut self, publisher: u64, subject: &str, queue: Option<&str>) -> Vec<Key> {
        let mut direct = Vec::new();
        let mut groups = BTreeMap::<String, Vec<Key>>::new();
        for (key, sub) in &self.subs {
            if !hit(&sub.filter, subject) {
                continue;
            }
            if let Some(q) = &sub.queue {
                groups.entry(q.clone()).or_default().push(key.clone());
            } else {
                direct.push((
                    sub.filter.split('.').any(|t| t == "*" || t == ">"),
                    key.clone(),
                ));
            }
        }
        direct.sort(); // Exact matches first, then wildcard matches, each by key.
        let skip_echo = self.clients.get(&publisher) == Some(&false);
        let mut result = Vec::new();
        if queue.is_none() {
            for (_, key) in direct {
                if skip_echo && key.0 == publisher {
                    continue;
                }
                self.deliver(&key);
                result.push(key);
            }
        }
        for (q, keys) in groups {
            if queue.is_some_and(|wanted| wanted != q) {
                continue;
            }
            let keys: Vec<_> = keys
                .into_iter()
                .filter(|key| !skip_echo || key.0 != publisher)
                .collect();
            if keys.is_empty() {
                continue;
            }
            let cursor = self.cursors.entry(q).or_default();
            let key = keys[*cursor % keys.len()].clone();
            *cursor = cursor.wrapping_add(1);
            self.deliver(&key);
            result.push(key);
        }
        result
    }
    fn interested(&self, subject: &str, queue: Option<&str>) -> bool {
        self.subs
            .values()
            .any(|s| hit(&s.filter, subject) && queue.is_none_or(|q| s.queue.as_deref() == Some(q)))
    }
}
fn filter() -> impl Strategy<Value = String> {
    prop_oneof![
        prop::sample::select(vec![
            ">", "a", "a.*", "a.>", "*.a", "*.*", "*.a.>", "café.*", "消息.>"
        ])
        .prop_map(str::to_owned),
        (
            prop::collection::vec(prop::sample::select(vec!["a", "b", "c", "*", "café"]), 0..8),
            any::<bool>()
        )
            .prop_map(|(mut tokens, tail)| {
                if tail || tokens.is_empty() {
                    tokens.push(">");
                }
                tokens.join(".")
            })
    ]
}
fn subject() -> impl Strategy<Value = String> {
    prop_oneof![
        prop::sample::select(vec!["a", "a.b", "a.b.c", "b.a", "café.a", "消息.x"])
            .prop_map(str::to_owned),
        prop::collection::vec(prop::sample::select(vec!["a", "b", "c", "z", "café"]), 1..9)
            .prop_map(|tokens| tokens.join("."))
    ]
}
proptest! {
    #[test]
    fn token_index_refines_scan_model_through_mutations(
        ops in prop::collection::vec((0u8..10, 0u64..6, 0u8..8, filter(), subject(), 0u8..4, 0u64..8, any::<bool>()), 0..500)
    ) {
        let mut core = Core::default(); let mut reference = Reference::default();
        for id in 0..5 { core.connect(id, true); reference.clients.insert(id, true); }
        for (op, client, sid, filter, subject, queue, maximum, echo) in ops {
            let key = (client, sid.to_string());
            let q = match queue { 0 => None, 1 => Some("q"), 2 => Some("other"), _ => Some("absent") };
            match op {
                0..=2 => {
                    let result = core.subscribe(client, key.1.clone(), filter.clone(), q.map(str::to_owned));
                    prop_assert_eq!(result.is_ok(), reference.clients.contains_key(&client));
                    if result.is_ok() { reference.subs.entry(key.clone()).or_insert(Sub { filter, queue: q.map(str::to_owned), delivered: 0, maximum: None }); }
                }
                3 => { let max = echo.then_some(maximum); core.unsubscribe(client, &key.1, max); reference.unsubscribe(&key, max); }
                4 => {
                    core.disconnect(client); reference.clients.remove(&client);
                    let keys: Vec<_> = reference.subs.keys().filter(|(id,_)| *id == client).cloned().collect();
                    for key in keys { reference.remove(&key); }
                }
                5 => { core.connect(client, echo); reference.clients.insert(client, echo); }
                _ => {
                    let expected = reference.publish(client, &subject, q);
                    let actual: Vec<_> = core.publish_in_queue(client, &subject, q).into_iter().map(|d| (d.client,d.sid)).collect();
                    prop_assert_eq!(actual, expected);
                    // Reuse the cached route after auto-unsubscribe and cursor changes.
                    let expected = reference.publish(client, &subject, q);
                    let actual: Vec<_> = core.publish_in_queue(client, &subject, q).into_iter().map(|d| (d.client,d.sid)).collect();
                    prop_assert_eq!(actual, expected);
                }
            }
            for probe in [&*subject, "a.b", "消息.x"] {
                for q in [None, Some("q"), Some("other"), Some("absent")] {
                    prop_assert_eq!(core.has_interest_in_queue(probe, q), reference.interested(probe, q));
                }
            }
            for id in 0..6 {
                prop_assert_eq!(core.subscription_count(id), reference.subs.keys().filter(|(client,_)| *client == id).count());
                for sid in 0..8 { prop_assert_eq!(core.has_subscription(id, &sid.to_string()), reference.subs.contains_key(&(id,sid.to_string()))); }
            }
        }
    }
}

#[test]
fn token_branches_preserve_direct_order_and_tail_requires_a_token() {
    let mut core = Core::default();
    for id in 0..5 {
        core.connect(id, true);
    }
    for (id, filter) in [(0, "a.>"), (1, "a.*"), (2, "*.b"), (3, ">"), (4, "a.b")] {
        core.subscribe(id, "s".into(), filter.into(), None).unwrap();
    }
    assert_eq!(
        core.publish(9, "a.b")
            .into_iter()
            .map(|d| d.client)
            .collect::<Vec<_>>(),
        vec![4, 0, 1, 2, 3]
    );
    assert_eq!(
        core.publish(9, "a")
            .into_iter()
            .map(|d| d.client)
            .collect::<Vec<_>>(),
        vec![3]
    );
    assert_eq!(
        core.publish(9, "a.b.c")
            .into_iter()
            .map(|d| d.client)
            .collect::<Vec<_>>(),
        vec![0, 3]
    );
}

#[test]
fn cache_eviction_and_subscription_churn_preserve_interest() {
    let mut core = Core::default();
    core.connect(1, true);
    core.connect(2, false);
    for n in 0..512 {
        core.subscribe(1, format!("unused{n}"), format!("unused.{n}.*"), None)
            .unwrap();
    }
    for n in 0..512 {
        assert!(core.has_interest(&format!("unused.{n}.probe")));
    }
    core.subscribe(1, "active".into(), "a.>".into(), Some("q".into()))
        .unwrap();
    core.subscribe(2, "self".into(), "*.x".into(), Some("q".into()))
        .unwrap();
    for n in 0..9000 {
        let subject = format!("a.{n}.x");
        assert!(core.has_interest_in_queue(&subject, Some("q")));
        assert_eq!(core.publish(2, &subject)[0].sid, "active");
    }
    core.disconnect(1);
    assert!(!core.has_interest("a.0.x"));
    assert!(core.has_interest("a.x"));
    assert!(core.publish(2, "a.x").is_empty());
    core.unsubscribe(2, "self", None);
    assert!(!core.has_interest("a.x"));
    core.subscribe(2, "self".into(), "a.x".into(), None)
        .unwrap();
    assert_eq!(core.publish(9, "a.x")[0].sid, "self");
}

#[test]
fn deeply_nested_filters_match_prune_and_drop_iteratively() {
    let mut core = Core::default();
    core.connect(1, true);
    let prefix = std::iter::repeat_n("a", 20000)
        .collect::<Vec<_>>()
        .join(".");
    let filter = format!("{prefix}.*.>");
    let subject = format!("{prefix}.b.c");
    core.subscribe(1, "deep".into(), filter.clone(), None)
        .unwrap();
    assert!(core.has_interest(&subject));
    assert_eq!(core.publish(9, &subject).len(), 1);
    assert!(!core.has_interest(&format!("{prefix}.b")));
    core.unsubscribe(1, "deep", None);
    assert!(!core.has_interest(&subject));
    core.subscribe(1, "deep".into(), filter, None).unwrap();
    assert_eq!(core.publish(9, &subject).len(), 1);
    drop(core);
}

#[test]
fn all_matching_literal_and_star_branches_deliver_once_and_prune() {
    let mut core = Core::default();
    core.connect(1, true);
    for bits in 0u32..64 {
        let filter = (0..6)
            .map(|n| if bits & (1 << n) == 0 { "a" } else { "*" })
            .collect::<Vec<_>>()
            .join(".");
        let sid = format!("{bits:02}");
        core.subscribe(1, sid.clone(), filter, None).unwrap();
        if bits % 2 == 1 {
            core.unsubscribe(1, &sid, Some(1));
        }
    }
    let subject = "a.a.a.a.a.a";
    assert!(core.has_interest(subject));
    assert!(!core.has_interest_in_queue(subject, Some("q")));
    assert_eq!(
        core.publish(9, subject)
            .into_iter()
            .map(|d| d.sid)
            .collect::<Vec<_>>(),
        (0..64).map(|n| format!("{n:02}")).collect::<Vec<_>>()
    );
    assert_eq!(
        core.publish(9, subject)
            .into_iter()
            .map(|d| d.sid)
            .collect::<Vec<_>>(),
        (0..64)
            .step_by(2)
            .map(|n| format!("{n:02}"))
            .collect::<Vec<_>>()
    );
    core.disconnect(1);
    assert!(!core.has_interest(subject));
    assert!(core.publish(9, subject).is_empty());
}
