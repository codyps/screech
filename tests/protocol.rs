use screech::{
    parser::{parse, Limits},
    NatsMessage,
};
#[test]
fn every_frame_prefix_is_incomplete() {
    let wires: [&[u8]; 8] = [
        b"PING\r\n",
        b"PONG\r\n",
        b"CONNECT {}\r\n",
        b"SUB foo.* group 7\r\n",
        b"UNSUB 7 22\r\n",
        b"PUB a reply 3\r\na\0b\r\n",
        b"HPUB a 12 15\r\nNATS/1.0\r\n\r\nabc\r\n",
        b"PUB a 0\r\n\r\n",
    ];
    for wire in wires {
        for i in 0..wire.len() {
            assert!(
                parse(&wire[..i], Limits::default()).unwrap().is_none(),
                "prefix {i} of {wire:?}"
            );
        }
        assert!(parse(wire, Limits::default()).unwrap().is_some());
    }
}
#[test]
fn malformed_frames_fail_early() {
    for wire in [
        b"PUB a -1\r\n".as_slice(),
        b"PUB a 999999999999999999999999999999999\r\n",
        b"PUB a 1048577\r\n",
        b"PUB a 1\r\nx!\n",
        b"HPUB a 16 12\r\n",
        b"SUB a..b 1\r\n",
        b"SUB a.>.b 1\r\n",
        b"PING x\r\n",
        b"CONNECT []\r\n",
    ] {
        assert!(parse(wire, Limits::default()).is_err(), "{wire:?}");
    }
    assert!(parse(&[b'a'; 4097], Limits::default()).is_err());
}
#[test]
fn tabs_case_and_pipeline() {
    let (msg, tail) = parse(b"sUb\tfoo\tq\t7\r\nPING\r\n", Limits::default())
        .unwrap()
        .unwrap();
    assert!(matches!(msg, NatsMessage::Sub(_)));
    assert_eq!(tail, b"PING\r\n");
}

#[test]
fn embedded_wildcard_characters_are_literals() {
    for subject in ["a*", "*a", "**", "a>", ">a"] {
        let wire = format!("PUB {subject} 0\r\n\r\n");
        assert!(parse(wire.as_bytes(), Limits::default()).unwrap().is_some());
        assert!(!screech::core::matches(subject, "a"));
        assert!(screech::core::matches(subject, subject));
    }
}
