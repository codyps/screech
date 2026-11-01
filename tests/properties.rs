use bytes::Bytes;
use proptest::prelude::*;
use screech::{
    core::{matches, Core},
    durable::{Engine, StreamConfig},
    msg::NatsMessage,
    parser::{parse, Limits},
    storage::{MemoryStore, SqliteStore, Store},
};

proptest! {
    #[test]
    fn fragmented_binary_frames(payload in prop::collection::vec(any::<u8>(),0..2048),split in any::<usize>(),reply in any::<bool>()) {
        let head=format!("pUb foo{} {}\r\n",if reply{" inbox"}else{""},payload.len());
        let mut wire=head.into_bytes();wire.extend_from_slice(&payload);wire.extend_from_slice(b"\r\n");
        let cut=split%(wire.len()+1);
        if cut<wire.len(){prop_assert!(parse(&wire[..cut],Limits::default()).unwrap().is_none());}
        wire.extend_from_slice(b"PING\r\n");
        let (msg,tail)=parse(&wire,Limits::default()).unwrap().unwrap();
        let NatsMessage::Pub(p)=msg else {panic!("not PUB")};
        prop_assert_eq!(&*p.payload,&payload);prop_assert_eq!(tail,b"PING\r\n");
    }
    #[test]
    fn arbitrary_bytes_never_panic_or_overconsume(input in prop::collection::vec(any::<u8>(),0..8192)) {
        if let Ok(Some((_,tail)))=parse(&input,Limits {control_line:64,payload:128}) {prop_assert!(tail.len()<input.len());}
    }
    #[test]
    fn routing_matches_independent_reference(ops in prop::collection::vec((0u8..5,0u8..5,any::<bool>()),0..300)) {
        let mut core=Core::default();for id in 0..5 {core.connect(id,true);}
        let filters=["a","a.*","a.>","b.>",">"];
        let subjects=["a","a.x","a.x.y","b.x","z"];
        let mut reference=std::collections::BTreeMap::<(u64,String),(String,u64,Option<u64>)>::new();
        for (op,n,flag) in ops {
            let id=u64::from(n);let sid=n.to_string();
            match op {
                0=>{let f=filters[n as usize];core.subscribe(id,sid.clone(),f.into(),None).unwrap();reference.entry((id,sid)).or_insert((f.into(),0,None));},
                1=>{let key=(id,sid.clone());let max=if flag {Some(2)}else{None};core.unsubscribe(id,&sid,max);if let Some(s)=reference.get_mut(&key){if max.is_some_and(|n|n>s.1){s.2=max;}else{reference.remove(&key);}}},
                2=>{core.disconnect(id);core.connect(id,true);reference.retain(|(c,_),_|*c!=id);},
                _=>{
                    let subject=subjects[n as usize];
                    let mut expected=Vec::new();let mut remove=Vec::new();
                    for (key,(filter,delivered,maximum)) in &mut reference {
                        // Independent wildcard oracle uses token counts and prefix matching.
                        let f:Vec<_>=filter.split('.').collect();let s:Vec<_>=subject.split('.').collect();
                        let tail=f.last()==Some(&">");let fixed=if tail {f.len()-1}else{f.len()};
                        let hit=(if tail{s.len()>fixed}else{s.len()==fixed}) && f[..fixed].iter().zip(&s).all(|(a,b)|*a=="*"||a==b);
                        if hit{expected.push(key.clone());*delivered+=1;if maximum.is_some_and(|n|*delivered>=n){remove.push(key.clone());}}
                    }
                    for key in remove{reference.remove(&key);}
                    let mut actual:Vec<_>=core.publish(99,subject).into_iter().map(|d|(d.client,d.sid)).collect();actual.sort();
                    prop_assert_eq!(actual,expected);
                }
            }
        }
    }
    #[test]
    fn queue_exactly_one_and_auto_unsubscribe(count in 1usize..200,limit in 1u64..100) {
        let mut core=Core::default();for id in 1..=3 {core.connect(id,true);core.subscribe(id,"s".into(),"foo.>".into(),Some("workers".into())).unwrap();}
        core.unsubscribe(1,"s",Some(limit));
        let mut delivered=[0u64;4];
        for _ in 0..count {let ds=core.publish(9,"foo.x");prop_assert_eq!(ds.len(),1);delivered[ds[0].client as usize]+=1;}
        prop_assert!(delivered[1]<=limit);prop_assert_eq!(delivered.iter().sum::<u64>(),count as u64);
    }
    #[test]
    fn sqlite_and_memory_logs_refine_same_model(ops in prop::collection::vec((any::<bool>(),prop::collection::vec(any::<u8>(),0..128)),0..60)) {
        let dir=tempfile::tempdir().unwrap();let path=dir.path().join("log.db");
        let mut disk=SqliteStore::open(&path).unwrap();let mut memory=MemoryStore::default();
        for (purge,payload) in ops {
            if purge {disk.purge().unwrap();memory.purge().unwrap();}else{prop_assert_eq!(disk.append("a.x",&[],&payload).unwrap(),memory.append("a.x",&[],&payload).unwrap());}
            prop_assert_eq!(disk.last_sequence().unwrap(),memory.last_sequence().unwrap());
            for seq in 1..=memory.last_sequence().unwrap(){prop_assert_eq!(disk.get(seq).unwrap(),memory.get(seq).unwrap());}
            drop(disk);disk=SqliteStore::open(&path).unwrap();
        }
    }
    #[test]
    fn stream_events_replay_to_same_state(ops in prop::collection::vec(0u8..5,0..100)) {
        let mut engine=Engine::recover(MemoryStore::default()).unwrap();let mut replay=screech::durable::State::default();
        let mut exists=false;let mut last=0;let mut live=std::collections::BTreeSet::new();
        for op in ops {
            let event=if !exists {exists=true;last=0;engine.state().prepare_create(StreamConfig{name:"S".into(),subjects:vec!["a.>".into()], ..Default::default()},0).unwrap()}
                else if op==0 {exists=false;live.clear();engine.state().prepare_delete("S",false).unwrap()}
                else if op==1 {live.clear();engine.state().prepare_delete("S",true).unwrap()}
                else {last+=1;live.insert(last);engine.state().prepare_publish("S","a.x",Bytes::new(),Bytes::from_static(b"data"),0).unwrap()};
            replay.apply(event.clone()).unwrap();engine.commit(event).unwrap();prop_assert_eq!(engine.state(),&replay);
            if exists{let s=&engine.state().streams["S"];prop_assert_eq!(s.last_sequence,last);prop_assert_eq!(s.messages.keys().copied().collect::<std::collections::BTreeSet<_>>(),live.clone());}
        }
    }
}
#[test]
fn greater_requires_a_token() {
    assert!(!matches("foo.>", "foo"));
    assert!(matches("foo.>", "foo.bar"));
}
#[test]
fn queue_handover_preserves_already_selected_delivery() {
    let mut core = Core::default();
    core.connect(1, true);
    core.connect(2, true);
    core.subscribe(1, "old".into(), "foo".into(), Some("q".into()))
        .unwrap();
    let selected = core.publish(9, "foo");
    core.subscribe(2, "new".into(), "foo".into(), Some("q".into()))
        .unwrap();
    core.unsubscribe(1, "old", None);
    assert_eq!(selected[0].sid, "old");
    assert_eq!(core.publish(9, "foo")[0].sid, "new");
}

#[test]
fn routing_cache_is_bounded_and_invalidated_on_subscription_changes() {
    let mut core = Core::default();
    core.connect(1, true);
    core.subscribe(1, "old".into(), ">".into(), None).unwrap();
    for n in 0..10000 {
        assert_eq!(core.publish(2, &format!("subject.{n}"))[0].sid, "old");
        let (entries, bytes) = core.routing_cache_usage();
        assert!(entries <= 4096 && bytes <= 4 * 1024 * 1024);
    }
    core.unsubscribe(1, "old", None);
    core.subscribe(1, "new".into(), "subject.*".into(), None)
        .unwrap();
    assert_eq!(core.publish(2, "subject.1")[0].sid, "new");
    core.unsubscribe(1, "new", Some(2));
    assert_eq!(core.publish(2, "subject.1")[0].sid, "new");
    assert!(core.publish(2, "subject.1").is_empty());
}
