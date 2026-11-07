//! Bounded PostgreSQL Protocol 3.0 frames.

use bytes::BytesMut;
use postgres_protocol::message::{backend::Message, frontend};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::{config::PostgresConfig, errors::PostgresError};

pub(crate) const MAX_BACKEND_FRAME_LENGTH: i32 = 16_777_216;
pub(crate) const MIN_BACKEND_FRAME_LENGTH: i32 = 4;
#[expect(dead_code, reason = "Termination precedes session integration")]
pub(crate) const TERMINATE_FRAME: [u8; 5] = *b"X\0\0\0\x04";

/// Read exactly one backend frame, checking the length before allocating its body.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "Framing precedes session integration")
)]
pub(crate) async fn read_message<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<Message, PostgresError> {
    let mut header = [0; 5];
    reader
        .read_exact(&mut header)
        .await
        .map_err(|error| PostgresError::Transport(error.to_string()))?;
    let raw_len = i32::from_be_bytes([header[1], header[2], header[3], header[4]]);
    if !(MIN_BACKEND_FRAME_LENGTH..=MAX_BACKEND_FRAME_LENGTH).contains(&raw_len) {
        return Err(PostgresError::Protocol(
            "backend frame length is outside 4..=16777216".into(),
        ));
    }
    let frame_len = 1 + raw_len as usize;
    let mut frame = BytesMut::with_capacity(frame_len);
    frame.extend_from_slice(&header);
    frame.resize(frame_len, 0);
    reader
        .read_exact(&mut frame[5..])
        .await
        .map_err(|error| PostgresError::Transport(error.to_string()))?;
    match Message::parse(&mut frame) {
        Ok(Some(message)) if frame.is_empty() => Ok(message),
        Ok(_) => Err(PostgresError::Protocol(
            "backend parser did not consume the complete frame".into(),
        )),
        Err(error) => Err(PostgresError::Protocol(error.to_string())),
    }
}

/// Encode Protocol 3.0 startup parameters with explicit UTF8 client encoding.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "Framing precedes session integration")
)]
pub(crate) fn encode_startup_frame(config: &PostgresConfig) -> Result<BytesMut, PostgresError> {
    let mut frame = BytesMut::new();
    frontend::startup_message(
        [
            ("user", config.user.as_str()),
            ("database", config.database.as_str()),
            ("application_name", config.application_name.as_str()),
            ("client_encoding", "UTF8"),
        ],
        &mut frame,
    )
    .map_err(|error| PostgresError::Protocol(error.to_string()))?;
    Ok(frame)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::{PostgresConfig, RawPostgresOptions},
        errors::PostgresError,
    };
    use postgres_protocol::message::backend::Message;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};

    async fn decode_startup_messages(bytewise: bool) {
        let frames = b"R\0\0\0\x08\0\0\0\0S\0\0\0\x19client_encoding\0UTF8\0Z\0\0\0\x05I";
        let (mut writer, mut reader) = duplex(if bytewise { 1 } else { frames.len() });
        let send = async {
            if bytewise {
                for byte in frames {
                    writer.write_all(&[*byte]).await.unwrap();
                }
            } else {
                writer.write_all(frames).await.unwrap();
            }
        };
        let receive = async {
            assert!(matches!(
                read_message(&mut reader).await.unwrap(),
                Message::AuthenticationOk
            ));
            let Message::ParameterStatus(parameter) = read_message(&mut reader).await.unwrap()
            else {
                panic!("expected ParameterStatus");
            };
            assert_eq!(parameter.name().unwrap(), "client_encoding");
            assert_eq!(parameter.value().unwrap(), "UTF8");
            let Message::ReadyForQuery(ready) = read_message(&mut reader).await.unwrap() else {
                panic!("expected ReadyForQuery");
            };
            assert_eq!(ready.status(), b'I');
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            tokio::join!(send, receive)
        })
        .await
        .expect("adjacent frames must decode without waiting for more bytes");
    }

    #[tokio::test]
    async fn decodes_bytewise_frames() {
        decode_startup_messages(true).await;
    }

    #[tokio::test]
    async fn decodes_coalesced_adjacent_frames() {
        decode_startup_messages(false).await;
    }

    async fn decode_closed_frame(frame: &[u8]) -> Result<Message, PostgresError> {
        let (mut writer, mut reader) = duplex(frame.len().max(1));
        writer.write_all(frame).await.unwrap();
        drop(writer);
        read_message(&mut reader).await
    }

    #[tokio::test]
    async fn reports_eof_in_header_and_body() {
        for frame in [b"R\0\0".as_slice(), b"R\0\0\0\x08\0\0"] {
            assert!(matches!(
                decode_closed_frame(frame).await,
                Err(PostgresError::Transport(_))
            ));
        }
    }

    #[tokio::test]
    async fn rejects_invalid_lengths_without_reading_body() {
        for length in [-1_i32, 0, 1, 2, 3, 16_777_217, i32::MAX] {
            let (mut writer, mut reader) = duplex(6);
            let mut frame = vec![b'D'];
            frame.extend_from_slice(&length.to_be_bytes());
            frame.push(0xab);
            writer.write_all(&frame).await.unwrap();
            let result = tokio::time::timeout(Duration::from_secs(1), read_message(&mut reader))
                .await
                .expect("invalid length must fail while writer remains open");
            assert!(matches!(result, Err(PostgresError::Protocol(_))));
            assert_eq!(reader.read_u8().await.unwrap(), 0xab);
        }
    }

    #[tokio::test]
    async fn accepts_minimum_frame_length() {
        assert!(matches!(
            decode_closed_frame(b"n\0\0\0\x04").await.unwrap(),
            Message::NoData
        ));
    }

    #[tokio::test]
    async fn accepts_maximum_length_header_then_reports_body_eof() {
        assert!(matches!(
            decode_closed_frame(b"D\x01\0\0\0").await,
            Err(PostgresError::Transport(_))
        ));
    }

    #[tokio::test]
    async fn rejects_unknown_tags_and_malformed_bodies() {
        for frame in [
            b"?\0\0\0\x04".as_slice(),
            b"R\0\0\0\x09\0\0\0\0\xaa",
            b"S\0\0\0\x18client_encoding\0UTF8",
        ] {
            assert!(matches!(
                decode_closed_frame(frame).await,
                Err(PostgresError::Protocol(_))
            ));
        }
    }

    fn config() -> PostgresConfig {
        PostgresConfig::validate(RawPostgresOptions {
            user: "alice".into(),
            database: "analytics".into(),
            ..RawPostgresOptions::default()
        })
        .unwrap()
    }

    #[test]
    fn encodes_protocol_3_utf8_startup() {
        let frame = encode_startup_frame(&config()).unwrap();
        assert_eq!(&frame[4..8], &[0, 3, 0, 0]);
        assert_eq!(
            i32::from_be_bytes(frame[..4].try_into().unwrap()),
            frame.len() as i32
        );
        assert_eq!(
            &frame[8..],
            b"user\0alice\0database\0analytics\0application_name\0vertebra\0client_encoding\0UTF8\0\0"
        );
    }

    #[test]
    fn rejects_embedded_nul_in_startup_parameters() {
        let mut config = config();
        config.application_name = "invalid\0application".into();
        assert!(matches!(
            encode_startup_frame(&config),
            Err(PostgresError::Protocol(_))
        ));
    }
}
