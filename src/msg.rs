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
    Pub,
    /// `HPUB <subject> [reply-to] <#header bytes> <#total bytes>\r\n[headers]\r\n\r\n[payload]\r\n`
    HPub,
    Sub,
    Unsub,
    Msg,
    HMsg,
    Ping,
    Pong,
    Ok,
    Err,
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
