//! Incremental, bounded framing. An incomplete input never consumes any bytes.
use crate::msg::*;
use std::borrow::Cow;

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ProtocolError(pub String);
fn invalid(s: &str) -> ProtocolError {
    ProtocolError(s.into())
}

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub control_line: usize,
    pub payload: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            control_line: 4096,
            payload: 1024 * 1024,
        }
    }
}

pub fn token(s: &str) -> bool {
    !s.is_empty() && !s.bytes().any(|b| b <= b' ' || b == 127)
}
#[inline(always)]
pub fn subject(s: impl AsRef<[u8]>, wildcard: bool) -> bool {
    let bytes = s.as_ref();
    let mut start = 0;
    for (i, &b) in bytes.iter().enumerate() {
        if b <= b' ' || b == 127 {
            return false;
        }
        if b == b'.' {
            if i == start
                || (i == start + 1 && (bytes[start] == b'>' || (bytes[start] == b'*' && !wildcard)))
            {
                return false;
            }
            start = i + 1;
        }
    }
    start < bytes.len()
        && (bytes.len() != start + 1 || (bytes[start] != b'*' && bytes[start] != b'>') || wildcard)
}

fn number(s: &[u8]) -> Result<usize, ProtocolError> {
    if s.is_empty() || !s.iter().all(u8::is_ascii_digit) {
        return Err(invalid("Invalid byte count"));
    }
    s.iter().try_fold(0usize, |n, b| {
        n.checked_mul(10)
            .and_then(|n| n.checked_add(usize::from(b - b'0')))
            .ok_or_else(|| invalid("Byte count overflow"))
    })
}

#[inline(always)]
pub fn parse(
    src: &[u8],
    limits: Limits,
) -> Result<Option<(NatsMessage<'_>, &[u8])>, ProtocolError> {
    let Some(end) = memchr::memmem::find(src, b"\r\n") else {
        if src.len() > limits.control_line {
            return Err(invalid("Maximum control line exceeded"));
        }
        return Ok(None);
    };
    if end > limits.control_line {
        return Err(invalid("Maximum control line exceeded"));
    }
    let line = &src[..end];
    let mut fields = line
        .split(|b| *b == b' ' || *b == b'\t')
        .filter(|v| !v.is_empty());
    let op = fields.next().ok_or_else(|| invalid("Empty command"))?;
    // All client operations have at most five arguments. Avoid heap allocation on
    // every publication; JSON is parsed as a whole object instead of tokenized.
    let mut argument_storage: [&[u8]; 5] = [b""; 5];
    let mut count = 0;
    if !op.eq_ignore_ascii_case(b"CONNECT")
        && !op.eq_ignore_ascii_case(b"INFO")
        && !op.eq_ignore_ascii_case(b"-ERR")
    {
        for field in fields {
            if count == argument_storage.len() {
                return Err(invalid("Too many arguments"));
            }
            argument_storage[count] = field;
            count += 1;
        }
    }
    let args = &argument_storage[..count];
    let rest = &src[end + 2..];
    let mut opcode = [0u8; 7];
    if op.len() > opcode.len() || !op.is_ascii() {
        return Err(invalid("Unknown protocol operation"));
    }
    for (dst, src) in opcode.iter_mut().zip(op.iter().copied()) {
        *dst = src.to_ascii_uppercase();
    }
    let cmd = &opcode[..op.len()];
    match cmd {
        b"PUB" | b"HPUB" => {
            let headers = cmd == b"HPUB";
            let base = if headers { 3 } else { 2 };
            if args.len() != base && args.len() != base + 1 {
                return Err(invalid("Invalid publish arguments"));
            }
            if !subject(args[0], false) {
                return Err(invalid("Invalid publish subject"));
            }
            let reply_to = if args.len() == base + 1 {
                if !subject(args[1], false) {
                    return Err(invalid("Invalid reply subject"));
                }
                Some(Cow::Borrowed(args[1]))
            } else {
                None
            };
            let total = number(args[args.len() - 1])?;
            if total > limits.payload {
                return Err(invalid("Maximum payload exceeded"));
            }
            let header_len = if headers {
                number(args[args.len() - 2])?
            } else {
                0
            };
            if header_len > total || (headers && header_len < 12) {
                return Err(invalid("Invalid header size"));
            }
            let frame_len = total
                .checked_add(2)
                .ok_or_else(|| invalid("Payload overflow"))?;
            if rest.len() < frame_len {
                return Ok(None);
            }
            if &rest[total..frame_len] != b"\r\n" {
                return Err(invalid("Invalid payload terminator"));
            }
            let payload = &rest[header_len..total];
            let msg = if headers {
                let hdr = &rest[..header_len];
                if !hdr.starts_with(b"NATS/1.0\r\n") || !hdr.ends_with(b"\r\n\r\n") {
                    return Err(invalid("Invalid headers"));
                }
                NatsMessage::HPub(HPub {
                    subject: args[0].into(),
                    reply_to,
                    headers: hdr.into(),
                    payload: payload.into(),
                })
            } else {
                NatsMessage::Pub(Pub {
                    subject: args[0].into(),
                    reply_to,
                    payload: payload.into(),
                })
            };
            Ok(Some((msg, &rest[frame_len..])))
        }
        _ => Ok(Some((parse_control(cmd, line, args)?, rest))),
    }
}

// Keep infrequent controls out of the inlined publication framing path.
#[cold]
#[inline(never)]
fn parse_control<'a>(
    cmd: &[u8],
    line: &'a [u8],
    args: &[&'a [u8]],
) -> Result<NatsMessage<'a>, ProtocolError> {
    let msg = match cmd {
        b"INFO" | b"CONNECT" => {
            let line = std::str::from_utf8(line).map_err(|_| invalid("Invalid control line"))?;
            let json = line[cmd.len()..].trim();
            if !json.starts_with('{') {
                return Err(invalid("Expected JSON object"));
            }
            if cmd == b"INFO" {
                NatsMessage::Info(Box::new(
                    serde_json::from_str(json).map_err(|_| invalid("Invalid INFO JSON"))?,
                ))
            } else {
                NatsMessage::Connect(Box::new(
                    serde_json::from_str(json).map_err(|_| invalid("Invalid CONNECT JSON"))?,
                ))
            }
        }
        b"PING" if args.is_empty() => NatsMessage::Ping,
        b"PONG" if args.is_empty() => NatsMessage::Pong,
        b"+OK" if args.is_empty() => NatsMessage::Ok,
        b"-ERR" => {
            std::str::from_utf8(line).map_err(|_| invalid("Invalid control line"))?;
            NatsMessage::Err
        }
        b"SUB" if args.len() == 2 || args.len() == 3 => {
            if !subject(args[0], true)
                || !args[1..]
                    .iter()
                    .all(|s| std::str::from_utf8(s).is_ok_and(token))
            {
                return Err(invalid("Invalid subscription"));
            }
            NatsMessage::Sub(Sub {
                subject: args[0].into(),
                queue_group: if args.len() == 3 {
                    Some(
                        std::str::from_utf8(args[1])
                            .map_err(|_| invalid("Invalid control line"))?
                            .into(),
                    )
                } else {
                    None
                },
                sid: std::str::from_utf8(args[args.len() - 1])
                    .map_err(|_| invalid("Invalid control line"))?
                    .into(),
            })
        }
        b"UNSUB" if args.len() == 1 || args.len() == 2 => {
            let max_msgs = if args.len() == 2 {
                Some(u64::try_from(number(args[1])?).map_err(|_| invalid("Invalid maximum"))?)
            } else {
                None
            };
            NatsMessage::Unsub(Unsub {
                sid: std::str::from_utf8(args[0])
                    .map_err(|_| invalid("Invalid control line"))?
                    .into(),
                max_msgs,
            })
        }
        _ => return Err(invalid("Unknown or malformed protocol operation")),
    };
    Ok(msg)
}
