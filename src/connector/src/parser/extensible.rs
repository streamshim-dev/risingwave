// Record decoding for `FORMAT PLAIN ENCODE EXTENSIBLE`.
//
// The payload is parsed exactly like `ENCODE JSON`, but every record first goes through
// `decode_record`, the single dispatch point that inspects the message headers and rewrites
// records written with a known serializer into the JSON shape the source schema expects. Support
// for a new serializer or envelope version is added as one more arm in `decode_with_serializer`.

use super::kafka_envelope::{self, EnvelopeError};
use crate::source::SourceMeta;

const SERIALIZER_PROTOBUF: &[u8] = b"protobuf";

/// Decodes one record according to the headers of its source message. Records without a known
/// serializer are returned untouched.
pub fn decode_record(payload: Vec<u8>, meta: &SourceMeta) -> Result<Vec<u8>, EnvelopeError> {
    match meta {
        SourceMeta::Kafka(meta) => decode_with_serializer(
            payload,
            meta.header_bytes(kafka_envelope::SERIALIZER_HEADER),
            meta.header_bytes(kafka_envelope::USER_ENCODING_HEADER),
        ),
        _ => Ok(payload),
    }
}

fn decode_with_serializer(
    payload: Vec<u8>,
    serializer: Option<&[u8]>,
    user_encoding: Option<&[u8]>,
) -> Result<Vec<u8>, EnvelopeError> {
    if serializer == Some(SERIALIZER_PROTOBUF) {
        kafka_envelope::convert_if_protobuf(payload, serializer, user_encoding)
    } else {
        Ok(payload)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    /// A protobuf record with `content` as field 1 and no headers.
    fn record(content: &[u8]) -> Vec<u8> {
        let mut out = vec![0x0a, content.len() as u8];
        out.extend(content);
        out
    }

    #[test]
    fn unknown_or_missing_serializer_is_untouched() {
        let json = br#"{"data":"x","headers":{}}"#.to_vec();
        for serializer in [
            None,
            Some(&b"json"[..]),
            Some(&b"Protobuf"[..]),
            Some(&b""[..]),
        ] {
            assert_eq!(
                decode_with_serializer(json.clone(), serializer, None).unwrap(),
                json
            );
        }
    }

    #[test]
    fn protobuf_serializer_is_converted() {
        let out = decode_with_serializer(record(b"hi"), Some(b"protobuf"), None).unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&out).unwrap(),
            json!({"data": "hi", "headers": {}})
        );
        let out =
            decode_with_serializer(record(b"hi"), Some(b"protobuf"), Some(b"base64")).unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&out).unwrap(),
            json!({"data": "aGk=", "headers": {}})
        );
    }

    #[test]
    fn malformed_protobuf_is_an_error() {
        assert!(
            decode_with_serializer(b"{\"data\":\"x\"}".to_vec(), Some(b"protobuf"), None).is_err()
        );
    }

    #[test]
    fn non_kafka_meta_is_untouched() {
        let payload = record(b"hi");
        assert_eq!(
            decode_record(payload.clone(), &SourceMeta::Empty).unwrap(),
            payload
        );
    }
}
