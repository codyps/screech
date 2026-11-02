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

#[test]
fn boxed_handshakes_preserve_fields_and_pipelined_binary_publications() {
    let connect = r#"cOnNeCt {"verbose":false,"headers":true,"echo":false,"name":"λ","auth_token":"secret","no_responders":true}"#.as_bytes();
    let mut wire = connect.to_vec();
    wire.extend_from_slice(b"\r\nPUB a reply 3\r\na\0b\r\nPING\r\n");
    for end in 0..connect.len() + 2 {
        assert!(parse(&wire[..end], Limits::default()).unwrap().is_none());
    }
    let (command, tail) = parse(&wire, Limits::default()).unwrap().unwrap();
    let NatsMessage::Connect(c) = command else {
        panic!("CONNECT")
    };
    assert!(!c.verbose);
    assert_eq!(c.headers, Some(true));
    assert_eq!(c.echo, Some(false));
    assert_eq!(c.name.as_deref(), Some("λ"));
    assert_eq!(c.auth_token.as_deref(), Some("secret"));
    assert_eq!(c.no_responders, Some(true));
    let (command, tail) = parse(tail, Limits::default()).unwrap().unwrap();
    let NatsMessage::Pub(p) = command else {
        panic!("PUB")
    };
    assert_eq!(p.payload.as_ref(), b"a\0b");
    assert_eq!(p.reply_to.as_deref(), Some(b"reply".as_slice()));
    assert_eq!(tail, b"PING\r\n");
}

proptest::proptest! {
    #[test]
    fn subject_validation_refines_token_oracle(
        chars in proptest::collection::vec(proptest::prop_oneof![
            proptest::prelude::any::<char>(),
            proptest::sample::select(vec!['.', '*', '>', ' ', '\t', '\r', '\n', '\0', '\u{7f}', 'a']),
        ], 0..128),
        wildcard in proptest::prelude::any::<bool>(),
    ) {
        let text: String = chars.into_iter().collect();
        let tokens: Vec<_> = text.split('.').collect();
        let expected = !text.is_empty()
            && !text.bytes().any(|b| b <= b' ' || b == 127)
            && tokens.iter().enumerate().all(|(i, t)| !t.is_empty() && match *t {
                ">" => wildcard && i + 1 == tokens.len(),
                "*" => wildcard,
                _ => true,
            });
        proptest::prop_assert_eq!(screech::parser::subject(&text, wildcard), expected);
    }
}

proptest::proptest! {
    #[test]
    fn byte_subject_validation_refines_token_oracle(
        input in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..256),
        wildcard in proptest::prelude::any::<bool>(),
    ) {
        let tokens: Vec<_> = input.split(|b| *b == b'.').collect();
        let expected = !input.is_empty() && !input.iter().any(|b| *b <= b' ' || *b == 127)
            && tokens.iter().enumerate().all(|(i, t)| !t.is_empty() && match *t {
                b">" => wildcard && i + 1 == tokens.len(),
                b"*" => wildcard,
                _ => true,
            });
        proptest::prop_assert_eq!(screech::parser::subject(&input, wildcard), expected);
    }
}
