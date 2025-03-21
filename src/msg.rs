use serde::{Deserialize, Serialize};
use std::borrow::Cow;

#[derive(Debug)]
pub enum NatsMessage<'a> {
    /// Sent by server on accept of new connections. json payload
    Info(Info<'a>),
    /// Sent by client after handling INFO. json payload
    Connect(Connect<'a>),
    /// Publish a payload to a subject. 2 line format with byte count for 2nd line.
    /// `PUB <subject> [reply-to] <#bytes>\r\n[payload]\r\n`
    Pub(Pub<'a>),
    /// `HPUB <subject> [reply-to] <#header bytes> <#total bytes>\r\n[headers]\r\n\r\n[payload]\r\n`
    HPub(HPub<'a>),
    Sub(Sub<'a>),
    Unsub(Unsub<'a>),
    Msg,
    HMsg,
    Ping,
    Pong,
    Ok,
    Err,
}

impl<'a> NatsMessage<'a> {
    pub fn from_str(s: &'a str) -> Result<Self, serde_json::Error> {
        // split on the first space (' ')
        let (cmd, rest) = s.split_once(' ').unwrap();

        match cmd {
            "INFO" => serde_json::from_str(rest).map(NatsMessage::Info),
            "CONNECT" => serde_json::from_str(rest).map(NatsMessage::Connect),
            "PUB" => Pub::try_from_str(rest).map(NatsMessage::Pub),
            "HPUB" => HPub::try_from_str(rest).map(NatsMessage::HPub),
            "SUB" => Sub::try_from_str(rest).map(NatsMessage::Sub),
            "UNSUB" => Unsub::try_from_str(rest).map(NatsMessage::Unsub),
            "MSG" => Ok(NatsMessage::Msg),
            "HMSG" => Ok(NatsMessage::HMsg),
            "PING" => Ok(NatsMessage::Ping),
            "PONG" => Ok(NatsMessage::Pong),
            "OK" => Ok(NatsMessage::Ok),
            "ERR" => Ok(NatsMessage::Err),
            _ => Err(serde_json::Error::custom("Unknown command")),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct Connect<'a> {
    verbose: bool,
    pedantic: bool,
    tls_required: bool,
    auth_token: Option<Cow<'a, str>>,
    user: Option<Cow<'a, str>>,
    pass: Option<Cow<'a, str>>,
    name: Option<Cow<'a, str>>,
    lang: Cow<'a, str>,
    version: Cow<'a, str>,
    protocol: Option<i32>,
    echo: Option<bool>,
    sig: Option<Cow<'a, str>>,
    jwt: Option<Cow<'a, str>>,
    no_responders: Option<bool>,
    headers: Option<bool>,
    nk: Option<Cow<'a, str>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Info<'a> {
    server_id: Cow<'a, str>,
    server_name: Cow<'a, str>,
    version: Cow<'a, str>,
    go: Cow<'a, str>,
    host: Cow<'a, str>,
    port: i32,
    headers: bool,
    max_payload: i32,
    proto: i32,
    client_id: Option<u64>,
    auth_required: Option<bool>,
    tls_required: Option<bool>,
    tls_verify: Option<bool>,
    tls_available: Option<bool>,
    connect_urls: Option<Vec<Cow<'a, str>>>,
    ws_connect_urls: Option<Vec<Cow<'a, str>>>,
    ldm: Option<bool>,
    git_commit: Option<Cow<'a, str>>,
    jetstream: Option<bool>,
    ip: Option<Cow<'a, str>>,
    client_ip: Option<Cow<'a, str>>,
    nonce: Option<Cow<'a, str>>,
    cluster: Option<Cow<'a, str>>,
    domain: Option<Cow<'a, str>>,
}

/// Publish a payload to a subject. 2 line format with byte count for 2nd line.
/// `PUB <subject> [reply-to] <#bytes>\r\n[payload]\r\n`
#[derive(Debug)]
pub struct Pub<'a> {
    subject: Cow<'a, str>,
    reply_to: Option<Cow<'a, str>>,
    payload: Cow<'a, [u8]>,
}

impl<'a> Pub<'a> {
    /// Parse a string into a `Pub` message. Expects the `PUB ` to be trimmed already.
    /// Returns remainder of string that was unconsumed.
    ///
    /// If we don't have enough bytes yet, returns Ok(None).
    fn try_from_str(s: &'a str) -> Result<Option<(Self, &'a str)>, eyre::Report> {
        // check subject
        let Some((subject, rem)) = s.split_once(' ') else {
            // We don't have a subject terminator.
            // NOTE: we _could_ check if subject is valid so far to emit an early error here.
            return Ok(None);
        };

        // check reply-to OR bytes
        // look for the first space (' ') or '\r' (end of line)
        let Some(i) = memchr::memchr2(' ', '\r', rem.as_bytes()) else {
            // We don't have a reply-to or byte count terminator.
            // NOTE: we _could_ check if reply-to is valid so far to emit an early error here.
            return Ok(None);
        };

        let (reply_to_or_bytes, rem) = rem.split_at(i);
        let sep = rem.as_bytes()[0];
        let rem = &rem[1..];
        let (reply_to, bytes_s, rem) = if sep == b' ' {
            // we have a reply-to
            let reply_to = reply_to_or_bytes;
            let Some((bytes_s, rem)) = rem.split_once('\r') else {
                // We don't have a byte count terminator.
                // NOTE: we _could_ check if reply-to is valid so far to emit an early error here.
                return Ok(None);
            };

            (Some(reply_to), bytes_s, rem)
        } else if sep == b'\r' {
            let bytes_s = reply_to_or_bytes;
            // we have a byte count
            let (bytes_s, rem) = rem.split_once('\r').unwrap();
            let bytes = bytes_s
                .parse::<usize>()
                .wrap_err("Failed to parse byte count")?;

            (None, bytes_s, rem)
        };

        let bytes = bytes_s
            .parse::<usize>()
            .wrap_err("Failed to parse byte count")?;

        if rem.len() < bytes {
            return Ok(None);
        }

        let (payload, rem) = rem.split_at(bytes);

        Ok(Some((
            Self {
                subject: subject.into(),
                reply_to: Some(reply_to.into()),
                payload: payload.into(),
            },
            rem,
        )))
    }
}

/// Pub with headers
/// `HPUB <subject> [reply-to] <#header bytes> <#total bytes>\r\n[headers]\r\n\r\n[payload]\r\n`
#[derive(Debug)]
pub struct HPub<'a> {
    subject: Cow<'a, str>,
    reply_to: Option<Cow<'a, str>>,
    headers: Vec<Cow<'a, str>>,
    payload: Cow<'a, [u8]>,
}
