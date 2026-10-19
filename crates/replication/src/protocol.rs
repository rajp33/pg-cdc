use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use fallible_iterator::FallibleIterator;
use pg_cdc_decoder::Lsn;
use postgres_protocol::message::backend::{ErrorResponseBody, Header, Message};
use postgres_protocol::message::frontend;

use crate::error::ReplicationError;

const PG_EPOCH_UNIX_MICROS: i64 = 946_684_800_000_000;
const COPY_BOTH_RESPONSE_TAG: u8 = b'W';
const AUTHENTICATION_TAG: u8 = b'R';
const SUPPORTED_AUTHENTICATION: [i32; 6] = [0, 3, 5, 10, 11, 12];
const XLOG_DATA_HEADER: usize = 25;
const KEEPALIVE_LEN: usize = 18;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationEvent {
    XLogData {
        wal_start: Lsn,
        wal_end: Lsn,
        data: Bytes,
    },
    KeepAlive {
        wal_end: Lsn,
    },
}

pub(crate) enum Backend {
    Message(Message),
    CopyBothResponse,
}

pub(crate) fn parse_backend(buf: &mut BytesMut) -> Result<Option<Backend>, ReplicationError> {
    let Some(header) = Header::parse(buf).map_err(|error| protocol_io(&error))? else {
        return Ok(None);
    };
    let total = usize::try_from(header.len())
        .map_err(|_| protocol("negative message length"))?
        .saturating_add(1);
    if buf.len() < total {
        buf.reserve(total - buf.len());
        return Ok(None);
    }
    match header.tag() {
        COPY_BOTH_RESPONSE_TAG => {
            buf.advance(total);
            Ok(Some(Backend::CopyBothResponse))
        }
        AUTHENTICATION_TAG => {
            let code = buf
                .get(5..9)
                .and_then(|bytes| <[u8; 4]>::try_from(bytes).ok())
                .map(i32::from_be_bytes)
                .ok_or_else(|| protocol("truncated authentication request"))?;
            if !SUPPORTED_AUTHENTICATION.contains(&code) {
                return Err(ReplicationError::UnsupportedAuthentication(code));
            }
            parse_message(buf)
        }
        _ => parse_message(buf),
    }
}

fn parse_message(buf: &mut BytesMut) -> Result<Option<Backend>, ReplicationError> {
    Message::parse(buf)
        .map(|message| message.map(Backend::Message))
        .map_err(|error| protocol_io(&error))
}

pub(crate) fn server_error(body: &ErrorResponseBody) -> ReplicationError {
    let mut sqlstate = String::new();
    let mut message = String::new();
    let mut fields = body.fields();
    while let Ok(Some(field)) = fields.next() {
        let value = String::from_utf8_lossy(field.value_bytes()).into_owned();
        match field.type_() {
            b'C' => sqlstate = value,
            b'M' => message = value,
            _ => {}
        }
    }
    ReplicationError::Server { sqlstate, message }
}

pub(crate) fn start_replication_sql(slot: &str, start: Lsn, publication: &str) -> String {
    format!(
        "START_REPLICATION SLOT {} LOGICAL {start} (proto_version '1', publication_names {})",
        quote_identifier(slot),
        quote_literal(&quote_identifier(publication)),
    )
}

fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn quote_literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

pub(crate) fn parse_copy_data(
    mut data: Bytes,
) -> Result<(ReplicationEvent, bool), ReplicationError> {
    match data.first().copied() {
        Some(b'w') if data.len() >= XLOG_DATA_HEADER => {
            let mut header = data.split_to(XLOG_DATA_HEADER);
            header.advance(1);
            let wal_start = Lsn(header.get_u64());
            let wal_end = Lsn(header.get_u64());
            Ok((
                ReplicationEvent::XLogData {
                    wal_start,
                    wal_end,
                    data,
                },
                false,
            ))
        }
        Some(b'k') if data.len() == KEEPALIVE_LEN => {
            data.advance(1);
            let wal_end = Lsn(data.get_u64());
            data.advance(8);
            let reply = data.get_u8() == 1;
            Ok((ReplicationEvent::KeepAlive { wal_end }, reply))
        }
        Some(tag) => Err(protocol(&format!(
            "unexpected replication message {tag:#04x} of {} bytes",
            data.len()
        ))),
        None => Err(protocol("empty CopyData message")),
    }
}

pub(crate) fn status_update(
    lsn: Lsn,
    clock: i64,
    buf: &mut BytesMut,
) -> Result<(), ReplicationError> {
    let mut body = BytesMut::with_capacity(34);
    body.put_u8(b'r');
    body.put_u64(lsn.0);
    body.put_u64(lsn.0);
    body.put_u64(lsn.0);
    body.put_i64(clock);
    body.put_u8(0);
    frontend::CopyData::new(body.freeze())?.write(buf);
    Ok(())
}

pub(crate) fn pg_time(now: SystemTime) -> i64 {
    let unix = now.duration_since(UNIX_EPOCH).unwrap_or_default();
    i64::try_from(unix.as_micros())
        .unwrap_or(i64::MAX)
        .saturating_sub(PG_EPOCH_UNIX_MICROS)
}

pub(crate) fn protocol(what: &str) -> ReplicationError {
    ReplicationError::Protocol(what.to_owned())
}

fn protocol_io(error: &std::io::Error) -> ReplicationError {
    ReplicationError::Protocol(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn status_update_matches_the_protocol_layout() {
        let mut buf = BytesMut::new();
        status_update(Lsn(0x0102_0304_0506_0708), 0x1112_1314_1516_1718, &mut buf).unwrap();
        let mut expected = vec![b'd', 0, 0, 0, 38, b'r'];
        for _ in 0..3 {
            expected.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
        }
        expected.extend_from_slice(&[0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0]);
        assert_eq!(buf.as_ref(), expected.as_slice());
    }

    #[test]
    fn pg_time_counts_microseconds_from_2000() {
        let y2k = UNIX_EPOCH + Duration::from_hours(262_968);
        assert_eq!(pg_time(y2k), 0);
        assert_eq!(pg_time(y2k + Duration::from_millis(1_500)), 1_500_000);
        assert_eq!(pg_time(UNIX_EPOCH), -PG_EPOCH_UNIX_MICROS);
    }

    #[test]
    fn parses_xlogdata_and_keepalive_frames() {
        let mut xlog = vec![b'w'];
        xlog.extend_from_slice(&0x10_u64.to_be_bytes());
        xlog.extend_from_slice(&0x20_u64.to_be_bytes());
        xlog.extend_from_slice(&7_i64.to_be_bytes());
        xlog.extend_from_slice(b"BEGIN");
        assert_eq!(
            parse_copy_data(Bytes::from(xlog)).unwrap(),
            (
                ReplicationEvent::XLogData {
                    wal_start: Lsn(0x10),
                    wal_end: Lsn(0x20),
                    data: Bytes::from_static(b"BEGIN"),
                },
                false
            )
        );
        for (flag, reply) in [(0, false), (1, true)] {
            let mut keepalive = vec![b'k'];
            keepalive.extend_from_slice(&0x30_u64.to_be_bytes());
            keepalive.extend_from_slice(&9_i64.to_be_bytes());
            keepalive.push(flag);
            assert_eq!(
                parse_copy_data(Bytes::from(keepalive)).unwrap(),
                (ReplicationEvent::KeepAlive { wal_end: Lsn(0x30) }, reply)
            );
        }
    }

    #[test]
    fn rejects_truncated_or_unknown_replication_messages() {
        for bad in [&b""[..], b"w\0\0\0", b"k\0\0\0\0\0\0\0\0", b"x123"] {
            let error = parse_copy_data(Bytes::copy_from_slice(bad)).unwrap_err();
            assert!(
                matches!(error, ReplicationError::Protocol(_)),
                "{bad:?}: {error:?}"
            );
        }
    }

    #[test]
    fn parse_backend_waits_for_a_whole_frame() {
        let mut buf = BytesMut::from(&b"W\0\0\0\x07\0\0"[..]);
        assert!(parse_backend(&mut buf).unwrap().is_none());
        buf.put_u8(0);
        assert!(matches!(
            parse_backend(&mut buf).unwrap(),
            Some(Backend::CopyBothResponse)
        ));
        assert!(buf.is_empty());
    }

    #[test]
    fn server_error_carries_sqlstate_and_message() {
        let mut buf = BytesMut::new();
        let body = b"SFATAL\0C28P01\0Mpassword authentication failed\0\0";
        buf.put_u8(b'E');
        buf.put_u32(u32::try_from(body.len() + 4).unwrap());
        buf.put_slice(body);
        let Some(Backend::Message(Message::ErrorResponse(body))) = parse_backend(&mut buf).unwrap()
        else {
            panic!("not an ErrorResponse");
        };
        let error = server_error(&body);
        assert_eq!(error.sqlstate(), Some("28P01"));
        assert_eq!(
            error.to_string(),
            "server error 28P01: password authentication failed"
        );
    }
}
