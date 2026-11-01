use core::str;
use eyre::{eyre, WrapErr};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::fmt;
use tracing::{info, trace};

use super::Error;

#[derive(Debug, PartialEq, Eq, Clone)]
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
    pub fn try_from_bytes(s: &'a [u8]) -> Result<Option<(Self, &'a [u8])>, Error> {
        info!("try_from_bytes: {}", AsciiStr(s));

        // split on the first space (' ')
        let Some(cmd_end) = memchr::memchr(b' ', s) else {
            return Err(eyre!("No command found").into());
        };

        info!("cmd_end: {}", cmd_end);
        let (cmd, rest) = s.split_at(cmd_end);
        let rest = &rest[1..];

        info!("rest: {}", AsciiStr(rest));

        let upper_cmd = cmd.to_ascii_uppercase();

        match &upper_cmd[..] {
            b"INFO" => Ok(Info::try_from_bytes(rest)
                .map(|v| v.map(|(i, rest)| (NatsMessage::Info(i), rest)))?),
            b"CONNECT" => Ok(Connect::try_from_bytes(rest)
                .map(|v| v.map(|(c, rest)| (NatsMessage::Connect(c), rest)))?),
            b"PUB" => Ok(Pub::try_from_bytes(rest)
                .map(|v| v.map(|(p, rest)| (NatsMessage::Pub(p), rest)))?),
            /*
            "HPUB" => HPub::try_from_bytes(rest).map(NatsMessage::HPub),
            "SUB" => Sub::try_from_bytes(rest).map(NatsMessage::Sub),
            "UNSUB" => Unsub::try_from_bytes(rest).map(NatsMessage::Unsub),
            "MSG" => Ok(NatsMessage::Msg),
            "HMSG" => Ok(NatsMessage::HMsg),
            "PING" => Ok(NatsMessage::Ping),
            "PONG" => Ok(NatsMessage::Pong),
            "OK" => Ok(NatsMessage::Ok),
            "ERR" => Ok(NatsMessage::Err),
            */
            _ => return Err(eyre!("Unknown command").into()),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct Info<'a> {
    pub server_id: Cow<'a, str>,
    pub server_name: Cow<'a, str>,
    pub version: Cow<'a, str>,
    pub go: Cow<'a, str>,
    pub host: Cow<'a, str>,
    pub port: i32,
    pub headers: bool,
    pub max_payload: i32,
    pub proto: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_required: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls_required: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls_verify: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls_available: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connect_urls: Option<Vec<Cow<'a, str>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ws_connect_urls: Option<Vec<Cow<'a, str>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ldm: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git_commit: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jetstream: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_ip: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nonce: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cluster: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub domain: Option<Cow<'a, str>>,
}

impl<'a> Info<'a> {
    fn try_from_bytes(s: &'a [u8]) -> Result<Option<(Self, &'a [u8])>, eyre::Report> {
        // split on `\r\n`
        let Some(end) = memchr::memmem::find(s, b"\r\n") else {
            info!("did not locate end of INFO message in : {}", AsciiStr(s));
            return Ok(None);
        };

        info!("s: {}", AsciiStr(s));
        info!("end: {}", end);

        let (s, rem) = s.split_at(end);
        let rem = &rem[2..];

        info!("s: {}", AsciiStr(s));
        info!("rem: {}", AsciiStr(rem));

        let (info, rem_json): (_, &[u8]) =
            serde_json::from_slice(s).wrap_err("Failed to parse INFO json")?;
        if rem_json.len() != 0 {
            return Err(eyre!("Extra bytes after INFO json").into());
        }

        Ok(Some((info, rem)))
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct Connect<'a> {
    pub verbose: bool,
    pub pedantic: bool,
    pub tls_required: bool,
    pub auth_token: Option<Cow<'a, str>>,
    pub user: Option<Cow<'a, str>>,
    pub pass: Option<Cow<'a, str>>,
    pub name: Option<Cow<'a, str>>,
    pub lang: Cow<'a, str>,
    pub version: Cow<'a, str>,
    pub protocol: Option<i32>,
    pub echo: Option<bool>,
    pub sig: Option<Cow<'a, str>>,
    pub jwt: Option<Cow<'a, str>>,
    pub no_responders: Option<bool>,
    pub headers: Option<bool>,
    pub nk: Option<Cow<'a, str>>,
}

impl<'a> Connect<'a> {
    fn try_from_bytes(s: &'a [u8]) -> Result<Option<(Self, &'a [u8])>, eyre::Report> {
        // split on `\r\n`
        let Some(end) = memchr::memmem::find(s, b"\r\n") else {
            info!("did not locate end of CONNECT message");
            return Ok(None);
        };

        let (s, rem) = s.split_at(end);
        let rem = &rem[2..];

        let (connect, rem_json): (_, &[u8]) =
            serde_json::from_slice(s).wrap_err("Failed to parse CONNECT json")?;

        if rem_json.len() != 0 {
            return Err(eyre!("Extra bytes after CONNECT json").into());
        }

        Ok(Some((connect, rem)))
    }
}

/// Publish a payload to a subject. 2 line format with byte count for 2nd line.
/// `PUB <subject> [reply-to] <#bytes>\r\n[payload]\r\n`
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Pub<'a> {
    pub subject: Cow<'a, [u8]>,
    pub reply_to: Option<Cow<'a, [u8]>>,
    pub payload: Cow<'a, [u8]>,
}

impl<'a> Pub<'a> {
    /// Parse a string into a `Pub` message. Expects the `PUB ` to be trimmed already.
    /// Returns remainder of string that was unconsumed.
    ///
    /// If we don't have enough bytes yet, returns Ok(None).
    fn try_from_bytes(s: &'a [u8]) -> Result<Option<(Self, &'a [u8])>, eyre::Report> {
        // check subject
        let Some(subject_end) = memchr::memchr2(b' ', b'\r', s) else {
            // We don't have a subject terminator.
            // NOTE: we _could_ check if subject is valid so far to emit an early error here.
            info!("No subject terminator found");
            return Ok(None);
        };

        // if we have a `\r`, this is an invalid message because it ended too soon.
        // TODO: we could also verify that the partial subject is valid
        if s[subject_end] == b'\r' {
            return Err(eyre::eyre!(
                "Invalid message, found \\r before end of subject"
            ));
        }

        assert_eq!(s[subject_end], b' ');

        let (subject, rem) = s.split_at(subject_end);
        let rem = &rem[1..];

        // check reply-to OR bytes
        // look for the first space (' ') or '\r' (end of line)
        let Some(i) = memchr::memchr2(b' ', b'\r', rem) else {
            // We don't have a reply-to or byte count terminator.
            // NOTE: we _could_ check if reply-to is valid so far to emit an early error here.
            info!("No reply-to or byte count terminator found");
            return Ok(None);
        };

        let (reply_to_or_bytes, rem) = rem.split_at(i);
        let sep = rem[0];
        let rem = &rem[1..];
        let (reply_to, bytes_s, rem) = if sep == b' ' {
            // we have a reply-to
            let reply_to = reply_to_or_bytes;
            let Some(bytes_s_end) = memchr::memchr(b'\r', rem) else {
                // We don't have a byte count terminator.
                // NOTE: we _could_ check if reply-to is valid so far to emit an early error here.
                info!("No byte count terminator found");
                return Ok(None);
            };

            let (bytes_s, rem) = rem.split_at(bytes_s_end);

            (Some(reply_to), bytes_s, rem)
        } else {
            assert_eq!(sep, b'\r');

            // we have a byte count (complete, because we found a '\r')
            // we don't have a reply-to
            let bytes_s = reply_to_or_bytes;

            (None, bytes_s, rem)
        };

        let bytes_s = str::from_utf8(bytes_s).wrap_err("Failed to parse byte count as utf8")?;
        let bytes = bytes_s
            .parse::<usize>()
            .wrap_err("Failed to parse byte count")?;

        if rem.len() < bytes {
            info!("Not enough bytes for payload");
            return Ok(None);
        }

        let (payload, rem) = rem.split_at(bytes);

        Ok(Some((
            Self {
                subject: subject.into(),
                reply_to: reply_to.map(Into::into),
                payload: payload.into(),
            },
            rem,
        )))
    }
}

/// Pub with headers
/// `HPUB <subject> [reply-to] <#header bytes> <#total bytes>\r\n[headers]\r\n\r\n[payload]\r\n`
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct HPub<'a> {
    pub subject: Cow<'a, u8>,
    pub reply_to: Option<Cow<'a, u8>>,
    pub headers: Cow<'a, u8>,
    pub payload: Cow<'a, [u8]>,
}

impl<'a> HPub<'a> {
    fn try_from_bytes(s: &'a [u8]) -> Result<Option<(Self, &'a [u8])>, eyre::Report> {
        // find subject.
        let Some(sub_end) = memchr::memchr(b' ', s) else {
            return Err(eyre::eyre!("No subject found"));
        };

        todo!()
    }
}

/// `SUB <subject> [queue group] <sid>␍␊`
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Sub<'a> {
    pub subject: Cow<'a, str>,
    pub queue_group: Option<Cow<'a, str>>,
    pub sid: Cow<'a, str>,
}

/// `UNSUB <sid> [max_msgs]␍␊`
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Unsub<'a> {
    pub sid: Cow<'a, str>,
    pub max_msgs: Option<u64>,
}

/// `MSG <subject> <sid> [reply-to] <#bytes>␍␊[payload]␍␊`
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Msg<'a> {
    pub subject: Cow<'a, str>,
    pub sid: Cow<'a, str>,
    pub reply_to: Option<Cow<'a, str>>,
    pub payload: Cow<'a, [u8]>,
}

/// `HMSG <subject> <sid> [reply-to] <#header bytes> <#total bytes>␍␊[headers]␍␊␍␊[payload]␍␊`
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct HMsg<'a> {
    pub subject: Cow<'a, str>,
    pub sid: Cow<'a, str>,
    pub reply_to: Option<Cow<'a, str>>,
    pub headers: Option<Cow<'a, str>>,
    pub payload: Cow<'a, [u8]>,
}

/// `PING␍␊`
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Ping;

/// `PONG␍␊`
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Pong;

/// `+OK␍␊`
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct OkMsg;

/// `-ERR <reason>␍␊`
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ErrMsg<'a> {
    pub reason: Cow<'a, str>,
}

struct AsciiStr<T: AsRef<[u8]>>(T);

impl<T: AsRef<[u8]>> fmt::Display for AsciiStr<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s = self.0.as_ref();
        write!(f, "'")?;
        for &b in s {
            match b {
                b'a'..=b'z'
                | b'A'..=b'Z'
                | b'0'..=b'9'
                | b' '
                | b'!'
                | b'"'
                | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'('
                | b')'
                | b'*'
                | b'+'
                | b','
                | b'-'
                | b'.'
                | b'/'
                | b':'
                | b';'
                | b'<'
                | b'='
                | b'>'
                | b'?'
                | b'@'
                | b'['
                | b']'
                | b'^'
                | b'_'
                | b'`'
                | b'{'
                | b'|'
                | b'}'
                | b'~' => write!(f, "{}", b as char)?,
                b'\'' => write!(f, "'\\''")?,
                b'\r' => write!(f, "\\r")?,
                b'\n' => write!(f, "\\n")?,
                b'\\' => write!(f, "\\\\")?,
                _ => write!(f, "\\x{:02x}", b)?,
            }
        }
        write!(f, "'")?;
        Ok(())
    }
}
