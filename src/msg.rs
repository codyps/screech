use crate::parser::{self, Limits, ProtocolError};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum NatsMessage<'a> {
    // Keep cold handshake fields out of every hot PUB/HPUB parser result.
    Info(Box<Info<'a>>),
    Connect(Box<Connect<'a>>),
    Pub(Pub<'a>),
    HPub(HPub<'a>),
    Sub(Sub<'a>),
    Unsub(Unsub<'a>),
    Ping,
    Pong,
    Ok,
    Err,
}
impl<'a> NatsMessage<'a> {
    pub fn try_from_bytes(src: &'a [u8]) -> Result<Option<(Self, &'a [u8])>, ProtocolError> {
        parser::parse(src, Limits::default())
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

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(default)]
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
    pub nkey: Option<Cow<'a, str>>,
}
impl Default for Connect<'_> {
    fn default() -> Self {
        Self {
            verbose: true,
            pedantic: false,
            tls_required: false,
            auth_token: None,
            user: None,
            pass: None,
            name: None,
            lang: "".into(),
            version: "".into(),
            protocol: None,
            echo: Some(true),
            sig: None,
            jwt: None,
            no_responders: None,
            headers: None,
            nkey: None,
        }
    }
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Pub<'a> {
    pub subject: Cow<'a, [u8]>,
    pub reply_to: Option<Cow<'a, [u8]>>,
    pub payload: Cow<'a, [u8]>,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct HPub<'a> {
    pub subject: Cow<'a, [u8]>,
    pub reply_to: Option<Cow<'a, [u8]>>,
    pub headers: Cow<'a, [u8]>,
    pub payload: Cow<'a, [u8]>,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Sub<'a> {
    pub subject: Cow<'a, [u8]>,
    pub queue_group: Option<Cow<'a, str>>,
    pub sid: Cow<'a, str>,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Unsub<'a> {
    pub sid: Cow<'a, str>,
    pub max_msgs: Option<u64>,
}
