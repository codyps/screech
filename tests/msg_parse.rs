use screech::{msg, NatsMessage};
use tracing::info;

pub fn with_tracing<T>(f: impl FnOnce() -> T) -> T {
    f()
}

#[test]
fn msg_parse_info() {
    with_tracing(|| {
        let info = msg::Info {
            server_id: "1".to_string().into(),
            server_name: "2".to_string().into(),
            version: "3".to_string().into(),
            go: "4".to_string().into(),
            host: "5".to_string().into(),
            port: 6,
            headers: true,
            max_payload: 7,
            proto: 8,

            client_id: None,
            auth_required: None,
            tls_required: None,
            tls_verify: None,
            tls_available: None,
            connect_urls: None,
            ws_connect_urls: None,
            ldm: None,
            git_commit: None,
            jetstream: None,
            ip: None,
            client_ip: None,
            nonce: None,
            cluster: None,
            domain: None,
        };

        let s = format!("INFO {}\r\n", serde_json::to_string(&info).unwrap());
        info!("parsing: {:?}", s);

        let ks = "INFO {\"server_id\":\"1\",\"server_name\":\"2\",\"version\":\"3\",\"go\":\"4\",\"host\":\"5\",\"port\":6,\"headers\":true,\"max_payload\":7,\"proto\":8}\r\n";

        assert_eq!(s, ks);

        // only mandatory fields
        let (msg, rem) = NatsMessage::try_from_bytes(s.as_bytes())
            .expect("error parsing")
            .expect("parse incomplete");

        assert_eq!(rem, b"");

        assert_eq!(
            msg,
            NatsMessage::Info(msg::Info {
                server_id: "1".to_string().into(),
                server_name: "2".to_string().into(),
                version: "3".to_string().into(),
                go: "4".to_string().into(),
                host: "5".to_string().into(),
                port: 6,
                headers: true,
                max_payload: 7,
                proto: 8,

                client_id: None,
                auth_required: None,
                tls_required: None,
                tls_verify: None,
                tls_available: None,
                connect_urls: None,
                ws_connect_urls: None,
                ldm: None,
                git_commit: None,
                jetstream: None,
                ip: None,
                client_ip: None,
                nonce: None,
                cluster: None,
                domain: None,
            })
        );
    });
}
