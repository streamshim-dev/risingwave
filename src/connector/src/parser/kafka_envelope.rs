// Protobuf-in-JSON-envelope support for Kafka sources (not upstream).
//
// The producer writes each Kafka record either as a JSON envelope
// (`{"data": "...", "headers": {"k": ["v"]}}`) or, with the protobuf flag on, as
//
//   message Value { bytes content = 1; map<string, ValueHeaders> headers = 2; }
//   message ValueHeaders { repeated string values = 1; }
//
// The format is announced per record by the Kafka header `serializer`. An `ENCODE JSON` source can
// not parse the protobuf form, so this module rewrites a protobuf record into the equivalent JSON
// envelope *before* the JSON parser runs. Records without `serializer: protobuf` are returned
// untouched, byte for byte.
//
// Output shape matches the Java consumer: `data` is base64(content) when the record carries
// `user-encoding: base64`, else content as UTF-8 text; `headers` is an object of string arrays.

use std::collections::BTreeMap;
use std::fmt;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;

pub const SERIALIZER_HEADER: &str = "serializer";
pub const USER_ENCODING_HEADER: &str = "user-encoding";

const PROTOBUF_SERIALIZER: &[u8] = b"protobuf";
const BASE64_ENCODING: &[u8] = b"base64";

#[derive(Debug, PartialEq, Eq)]
pub enum EnvelopeError {
    Truncated,
    BadVarint,
    BadWireType(u64),
}

impl fmt::Display for EnvelopeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => write!(f, "truncated protobuf record"),
            Self::BadVarint => write!(f, "invalid varint in protobuf record"),
            Self::BadWireType(w) => write!(f, "unsupported wire type {w} in protobuf record"),
        }
    }
}

impl std::error::Error for EnvelopeError {}

/// Returns the payload unchanged unless `serializer` is `protobuf`, in which case it returns the
/// legacy JSON envelope for it.
pub fn convert_if_protobuf(
    payload: Vec<u8>,
    serializer: Option<&[u8]>,
    user_encoding: Option<&[u8]>,
) -> Result<Vec<u8>, EnvelopeError> {
    if serializer != Some(PROTOBUF_SERIALIZER) {
        return Ok(payload);
    }
    protobuf_to_json_envelope(&payload, user_encoding == Some(BASE64_ENCODING))
}

pub fn protobuf_to_json_envelope(
    buf: &[u8],
    content_is_base64: bool,
) -> Result<Vec<u8>, EnvelopeError> {
    let (content, headers) = decode(buf)?;
    let data = if content_is_base64 {
        STANDARD.encode(content)
    } else {
        String::from_utf8_lossy(content).into_owned()
    };
    // Serializing a string/array map cannot fail.
    Ok(
        serde_json::to_vec(&serde_json::json!({ "data": data, "headers": headers }))
            .expect("envelope serialization is infallible"),
    )
}

fn read_varint(buf: &[u8], pos: &mut usize) -> Result<u64, EnvelopeError> {
    let mut value: u64 = 0;
    for i in 0..10 {
        let byte = *buf.get(*pos).ok_or(EnvelopeError::Truncated)?;
        *pos += 1;
        value |= u64::from(byte & 0x7f) << (7 * i);
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(EnvelopeError::BadVarint)
}

fn read_len_delimited<'a>(buf: &'a [u8], pos: &mut usize) -> Result<&'a [u8], EnvelopeError> {
    let len = usize::try_from(read_varint(buf, pos)?).map_err(|_| EnvelopeError::Truncated)?;
    let end = pos.checked_add(len).ok_or(EnvelopeError::Truncated)?;
    let slice = buf.get(*pos..end).ok_or(EnvelopeError::Truncated)?;
    *pos = end;
    Ok(slice)
}

/// Skips a field this schema does not know, as protobuf requires.
fn skip_field(buf: &[u8], pos: &mut usize, wire: u64) -> Result<(), EnvelopeError> {
    match wire {
        0 => {
            read_varint(buf, pos)?;
        }
        1 => advance(buf, pos, 8)?,
        2 => {
            read_len_delimited(buf, pos)?;
        }
        5 => advance(buf, pos, 4)?,
        w => return Err(EnvelopeError::BadWireType(w)),
    }
    Ok(())
}

fn advance(buf: &[u8], pos: &mut usize, n: usize) -> Result<(), EnvelopeError> {
    let end = pos.checked_add(n).ok_or(EnvelopeError::Truncated)?;
    if end > buf.len() {
        return Err(EnvelopeError::Truncated);
    }
    *pos = end;
    Ok(())
}

type Headers = BTreeMap<String, Vec<String>>;

fn decode(buf: &[u8]) -> Result<(&[u8], Headers), EnvelopeError> {
    let mut content: &[u8] = &[];
    let mut headers = Headers::new();
    let mut pos = 0;
    while pos < buf.len() {
        let tag = read_varint(buf, &mut pos)?;
        match (tag >> 3, tag & 7) {
            (1, 2) => content = read_len_delimited(buf, &mut pos)?,
            (2, 2) => {
                let entry = read_len_delimited(buf, &mut pos)?;
                // map<string, ValueHeaders>: a repeated key keeps the last entry.
                let (key, values) = decode_header_entry(entry)?;
                headers.insert(key, values);
            }
            (_, wire) => skip_field(buf, &mut pos, wire)?,
        }
    }
    Ok((content, headers))
}

fn decode_header_entry(entry: &[u8]) -> Result<(String, Vec<String>), EnvelopeError> {
    let mut key = String::new();
    let mut values = Vec::new();
    let mut pos = 0;
    while pos < entry.len() {
        let tag = read_varint(entry, &mut pos)?;
        match (tag >> 3, tag & 7) {
            (1, 2) => {
                key = String::from_utf8_lossy(read_len_delimited(entry, &mut pos)?).into_owned()
            }
            (2, 2) => {
                let inner = read_len_delimited(entry, &mut pos)?;
                values = decode_values(inner)?;
            }
            (_, wire) => skip_field(entry, &mut pos, wire)?,
        }
    }
    Ok((key, values))
}

fn decode_values(buf: &[u8]) -> Result<Vec<String>, EnvelopeError> {
    let mut values = Vec::new();
    let mut pos = 0;
    while pos < buf.len() {
        let tag = read_varint(buf, &mut pos)?;
        match (tag >> 3, tag & 7) {
            (1, 2) => values
                .push(String::from_utf8_lossy(read_len_delimited(buf, &mut pos)?).into_owned()),
            (_, wire) => skip_field(buf, &mut pos, wire)?,
        }
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    fn varint(mut v: usize) -> Vec<u8> {
        let mut out = vec![];
        loop {
            let b = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 {
                out.push(b);
                return out;
            }
            out.push(b | 0x80);
        }
    }

    fn field(num: u8, body: &[u8]) -> Vec<u8> {
        let mut out = vec![(num << 3) | 2];
        out.extend(varint(body.len()));
        out.extend(body);
        out
    }

    fn header(key: &str, values: &[&str]) -> Vec<u8> {
        let mut inner = vec![];
        for v in values {
            inner.extend(field(1, v.as_bytes()));
        }
        let mut entry = field(1, key.as_bytes());
        entry.extend(field(2, &inner));
        field(2, &entry)
    }

    fn record(content: &[u8], headers: &[(&str, &[&str])]) -> Vec<u8> {
        let mut out = field(1, content);
        for (k, v) in headers {
            out.extend(header(k, v));
        }
        out
    }

    fn convert(buf: &[u8], b64: bool) -> Value {
        serde_json::from_slice(&protobuf_to_json_envelope(buf, b64).unwrap()).unwrap()
    }

    #[test]
    fn plain_text_content() {
        let buf = record(br#"{"a":1}"#, &[("Content-Type", &["application/json"])]);
        assert_eq!(
            convert(&buf, false),
            json!({"data": r#"{"a":1}"#, "headers": {"Content-Type": ["application/json"]}})
        );
    }

    #[test]
    fn base64_content() {
        let buf = record(&[0xff, 0x00, 0x10, b'x'], &[]);
        assert_eq!(
            convert(&buf, true),
            json!({"data": "/wAQeA==", "headers": {}})
        );
    }

    #[test]
    fn multi_value_and_multi_header() {
        let buf = record(b"x", &[("h1", &["a", "b"]), ("h2", &["c"])]);
        assert_eq!(
            convert(&buf, false)["headers"],
            json!({"h1": ["a", "b"], "h2": ["c"]})
        );
    }

    #[test]
    fn repeated_map_key_keeps_last() {
        let buf = record(b"x", &[("h", &["old"]), ("h", &["new"])]);
        assert_eq!(convert(&buf, false)["headers"], json!({"h": ["new"]}));
    }

    #[test]
    fn empty_record_and_missing_content() {
        assert_eq!(convert(&[], false), json!({"data": "", "headers": {}}));
        // only headers, no content field (publish emits this for empty data)
        let buf = header("k", &["v"]);
        assert_eq!(
            convert(&buf, false),
            json!({"data": "", "headers": {"k": ["v"]}})
        );
        assert_eq!(convert(&[], true), json!({"data": "", "headers": {}}));
    }

    #[test]
    fn multibyte_utf8_and_long_content() {
        let text = "héllo ✓ 日本語";
        assert_eq!(
            convert(&record(text.as_bytes(), &[]), false)["data"],
            json!(text)
        );
        // length varint spanning 3 bytes
        let big = "y".repeat(300_000);
        assert_eq!(
            convert(&record(big.as_bytes(), &[]), false)["data"],
            json!(big)
        );
    }

    #[test]
    fn invalid_utf8_plaintext_is_lossy_like_java() {
        let buf = record(&[b'a', 0xff, b'b'], &[]);
        assert_eq!(convert(&buf, false)["data"], json!("a\u{fffd}b"));
    }

    #[test]
    fn unknown_fields_are_skipped() {
        let mut buf = record(b"x", &[]);
        buf.extend([0x18, 0x07]); // field 3 varint
        buf.extend(field(9, b"zz")); // field 9 len-delimited
        assert_eq!(convert(&buf, false), json!({"data": "x", "headers": {}}));
    }

    #[test]
    fn malformed_is_an_error() {
        // length says 5, only 2 bytes follow
        assert_eq!(
            protobuf_to_json_envelope(&[0x0a, 0x05, b'a', b'b'], false),
            Err(EnvelopeError::Truncated)
        );
        // tag only
        assert_eq!(
            protobuf_to_json_envelope(&[0x0a], false),
            Err(EnvelopeError::Truncated)
        );
        // varint never terminates
        assert_eq!(
            protobuf_to_json_envelope(
                &[
                    0x0a, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff
                ],
                false
            ),
            Err(EnvelopeError::BadVarint)
        );
        // group wire type
        assert_eq!(
            protobuf_to_json_envelope(&[0x0b], false),
            Err(EnvelopeError::BadWireType(3))
        );
        // a JSON envelope wrongly tagged as protobuf must error, not panic: '{' = field 15 wire 3
        assert!(protobuf_to_json_envelope(br#"{"data":"x"}"#, false).is_err());
    }

    #[test]
    fn non_protobuf_is_untouched() {
        let json = br#"{"data":"eA==","headers":{}}"#.to_vec();
        assert_eq!(convert_if_protobuf(json.clone(), None, None).unwrap(), json);
        assert_eq!(
            convert_if_protobuf(json.clone(), Some(b"json"), None).unwrap(),
            json
        );
        assert_eq!(
            convert_if_protobuf(json.clone(), Some(b"Protobuf"), Some(b"base64")).unwrap(),
            json
        );
    }

    #[test]
    fn converts_only_on_serializer_header() {
        let buf = record(b"hi", &[]);
        let out = convert_if_protobuf(buf.clone(), Some(b"protobuf"), None).unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&out).unwrap()["data"],
            json!("hi")
        );
        let out = convert_if_protobuf(buf, Some(b"protobuf"), Some(b"base64")).unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&out).unwrap()["data"],
            json!("aGk=")
        );
    }
}
