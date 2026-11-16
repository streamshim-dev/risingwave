use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use prost::Message as _;
use rdkafka::message::{Header, OwnedHeaders};
use risingwave_common::types::{DataType, JsonbVal, ScalarRefImpl, ToDatumRef, ToText};
use serde_json::{Value, json};

use super::*;

fn b64(s: &[u8]) -> String {
    STANDARD.encode(s)
}

fn envelope(data: &str, headers: Value) -> Envelope {
    Envelope {
        data: data.to_owned(),
        headers: headers.into(),
    }
}

fn pb(content: &[u8], headers: &[(&str, &[&str])]) -> Vec<u8> {
    wire::Value {
        content: content.to_vec(),
        headers: headers
            .iter()
            .map(|(k, v)| {
                (
                    k.to_string(),
                    wire::ValueHeaders {
                        values: v.iter().map(|s| s.to_string()).collect(),
                    },
                )
            })
            .collect(),
    }
    .encode_to_vec()
}

fn pb_decode(buf: &[u8], user_encoding: UserEncoding) -> ConnectorResult<Envelope> {
    decode(buf, Serializer::Protobuf, user_encoding)
}

fn json_decode(buf: &[u8]) -> ConnectorResult<Envelope> {
    decode(buf, Serializer::Json, UserEncoding::Other)
}

// ---------- header enums ----------

#[test]
fn serializer_from_header() {
    assert_eq!(
        Serializer::try_from(&b"json"[..]).unwrap(),
        Serializer::Json
    );
    assert_eq!(
        Serializer::try_from(&b"protobuf"[..]).unwrap(),
        Serializer::Protobuf
    );
    for bad in [&b"avro"[..], b"Protobuf", b"", b"json "] {
        assert!(Serializer::try_from(bad).is_err(), "{bad:?}");
    }
    assert_eq!(Serializer::default(), Serializer::Json);
}

#[test]
fn user_encoding_from_header() {
    assert_eq!(UserEncoding::from(&b"base64"[..]), UserEncoding::Base64);
    for other in [&b"plaintext"[..], b"", b"Base64", b"base64 "] {
        assert_eq!(UserEncoding::from(other), UserEncoding::Other, "{other:?}");
    }
    assert_eq!(UserEncoding::default(), UserEncoding::Other);
}

// ---------- base64 heuristic ----------

#[test]
fn heuristic_decodes_base64_text() {
    let obj = r#"{"kind":"Event","n":1}"#;
    let arr = r#"[{"data":"x"},{"data":"y"}]"#;
    let ws = "\n  {\"a\":1}";
    let long = format!(r#"[{{"data":"{}"}}]"#, "y".repeat(300_000));
    let cases: Vec<(String, String)> = vec![
        (b64(obj.as_bytes()), obj.to_owned()),
        (b64(arr.as_bytes()), arr.to_owned()),
        (b64(ws.as_bytes()), ws.to_owned()),
        (b64(b"{}"), "{}".to_owned()),
        (b64(b"[1]"), "[1]".to_owned()),
        (b64(b"[1,2]"), "[1,2]".to_owned()),
        ("e30=".to_owned(), "{}".to_owned()),
        // not JSON, but valid base64 of UTF-8 text: decoded as well
        ("c29tZXZhbHVl".to_owned(), "somevalue".to_owned()),
        (b64("héllo ✓".as_bytes()), "héllo ✓".to_owned()),
        (b64(long.as_bytes()), long),
    ];
    for (input, expected) in cases {
        assert_eq!(
            decode_with_base64_heuristic(input.clone()),
            expected,
            "input {:?}",
            &input[..input.len().min(40)]
        );
    }
}

#[test]
fn heuristic_keeps_other_input() {
    let good = b64(br#"{"a":"some longer json value here"}"#);
    let long_good = b64(&[b'{'; 300]);
    let mut bad_in_probe = good.clone();
    bad_in_probe.replace_range(24..25, "*");
    let mut bad_after_probe = long_good.clone();
    bad_after_probe.replace_range(200..201, "*");
    let mut bytes = br#"{"a":""#.to_vec();
    bytes.extend([0xff, 0xfe, b'"', b'}']);

    let samples = [
        // not base64, or too short
        "".to_owned(),
        "   ".to_owned(),
        "abc".to_owned(),
        "hello world".to_owned(),
        r#"{"a":1}"#.to_owned(),
        "[1,2]".to_owned(),
        "  \n{\"a\":1}".to_owned(),
        // valid alphabet, but decodes to invalid UTF-8
        "test".to_owned(),
        "abcd".to_owned(),
        "abcd1234".to_owned(),
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_owned(),
        b64(&[0xC3, 0x28, 0x00, 0x7F, 0xFF]),
        b64(&bytes),
        // wrong length
        format!("{good}A"),
        // padding in the middle, or not as `**==` / `***=` at the end
        format!("eyJh={}", &good[5..]),
        "e30=e30=".to_owned(),
        "e3=0".to_owned(),
        "=e30".to_owned(),
        "e=30".to_owned(),
        format!("{}ab=c", &good[..good.len() - 4]),
        format!("{}a=bc", &good[..good.len() - 4]),
        format!("{}=abc", &good[..good.len() - 4]),
        // invalid char inside the probe and after it
        bad_in_probe,
        bad_after_probe,
        // url-safe alphabet
        b64(&[0xfb, 0xff, 0xfe]).replace('+', "-").replace('/', "_"),
        // surrounding whitespace is never base64
        format!(" {good}"),
        format!("{good} "),
        format!("\n{good}"),
        format!("{good}\r\n"),
        " e30=".to_owned(),
        "e30= ".to_owned(),
        // non-canonical: unused bits of the last char are set
        "e31=".to_owned(),
    ];
    for s in samples {
        assert_eq!(decode_with_base64_heuristic(s.clone()), s, "input {s:?}");
    }
}

// ---------- protobuf wire format ----------

#[test]
fn wire_golden_bytes() {
    // content `{"a":1}`, headers {h: [v]}; the same record as case 3 of basic.slt.
    let bytes = hex_literal(b"0a077b2261223a317d12080a016812030a0176");
    let value = wire::Value::decode(bytes.as_slice()).unwrap();
    assert_eq!(value.content, br#"{"a":1}"#);
    assert_eq!(value.headers.len(), 1);
    assert_eq!(value.headers["h"].values, ["v"]);
    assert_eq!(value.encode_to_vec(), bytes);
}

#[test]
fn wire_round_trip() {
    let mut headers = std::collections::HashMap::new();
    headers.insert(
        "k".to_owned(),
        wire::ValueHeaders {
            values: vec!["a".to_owned(), "b".to_owned()],
        },
    );
    headers.insert("empty".to_owned(), wire::ValueHeaders { values: vec![] });
    let value = wire::Value {
        content: "héllo".as_bytes().to_vec(),
        headers,
    };
    assert_eq!(
        wire::Value::decode(value.encode_to_vec().as_slice()).unwrap(),
        value
    );
    assert_eq!(
        wire::Value::decode(&b""[..]).unwrap(),
        wire::Value::default()
    );
}

#[test]
fn wire_malformed_is_an_error() {
    let cases: [&[u8]; 3] = [
        &[0x0a, 0x05, b'a', b'b'], // length says 5, 2 bytes follow
        &[0x0a],                   // tag only
        &[
            0x0a, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        ], // bad varint
    ];
    for buf in cases {
        assert!(wire::Value::decode(buf).is_err(), "{buf:?}");
    }
}

fn hex_literal(hex: &[u8]) -> Vec<u8> {
    hex.chunks(2)
        .map(|c| u8::from_str_radix(std::str::from_utf8(c).unwrap(), 16).unwrap())
        .collect()
}

// ---------- protobuf envelope ----------

#[test]
fn protobuf_envelopes() {
    let inner = r#"[{"data":"x"}]"#;
    let looks_b64 = b64(br#"{"a":1}"#);
    let cases: Vec<(Vec<u8>, UserEncoding, Envelope)> = vec![
        (
            pb(br#"{"a":1}"#, &[("Content-Type", &["application/json"])]),
            UserEncoding::Other,
            envelope(r#"{"a":1}"#, json!({"Content-Type": ["application/json"]})),
        ),
        // user-encoding base64: content is the raw text, no heuristic
        (
            pb(b"somevalue", &[]),
            UserEncoding::Base64,
            envelope("somevalue", json!({})),
        ),
        (
            pb(looks_b64.as_bytes(), &[]),
            UserEncoding::Base64,
            envelope(&looks_b64, json!({})),
        ),
        // other user-encoding: heuristic
        (
            pb(b64(inner.as_bytes()).as_bytes(), &[]),
            UserEncoding::Other,
            envelope(inner, json!({})),
        ),
        (
            pb(b"some content", &[]),
            UserEncoding::Other,
            envelope("some content", json!({})),
        ),
        (
            pb(b"", &[("k", &["v"])]),
            UserEncoding::Other,
            envelope("", json!({"k": ["v"]})),
        ),
        (
            pb(b"x", &[("h1", &["a", "b"]), ("h2", &["c"]), ("h3", &[])]),
            UserEncoding::Other,
            envelope("x", json!({"h1": ["a", "b"], "h2": ["c"], "h3": []})),
        ),
        (
            pb("héllo ✓ 日本語".as_bytes(), &[]),
            UserEncoding::Other,
            envelope("héllo ✓ 日本語", json!({})),
        ),
    ];
    for (buf, user_encoding, expected) in cases {
        assert_eq!(pb_decode(&buf, user_encoding).unwrap(), expected);
    }
}

#[test]
fn protobuf_repeated_map_key_keeps_last() {
    let mut buf = pb(b"x", &[("h", &["old"])]);
    buf.extend(pb(b"", &[("h", &["new"])]));
    assert_eq!(
        pb_decode(&buf, UserEncoding::Other).unwrap(),
        envelope("x", json!({"h": ["new"]}))
    );
}

#[test]
fn protobuf_unknown_fields_are_skipped() {
    let mut buf = pb(b"x", &[]);
    buf.extend([0x18, 0x07]); // field 3, varint
    buf.extend([0x4a, 0x02, b'z', b'z']); // field 9, len-delimited
    assert_eq!(
        pb_decode(&buf, UserEncoding::Other).unwrap(),
        envelope("x", json!({}))
    );
}

#[test]
fn protobuf_errors() {
    let non_utf8 = pb(&[b'a', 0xff, b'b'], &[]);
    let cases: Vec<(&[u8], UserEncoding)> = vec![
        (&[0x0a, 0x05, b'a', b'b'], UserEncoding::Other),
        (&[0x0a], UserEncoding::Other),
        // JSON wrongly tagged as protobuf
        (br#"{"data":"x"}"#, UserEncoding::Other),
        // content that is not UTF-8, with and without the heuristic
        (&non_utf8, UserEncoding::Other),
        (&non_utf8, UserEncoding::Base64),
    ];
    for (buf, user_encoding) in cases {
        assert!(pb_decode(buf, user_encoding).is_err(), "{buf:?}");
    }
}

// ---------- JSON envelope ----------

#[test]
fn json_envelopes() {
    let inner = r#"[{"kind":"Event"}]"#;
    let with_b64 = json!({"data": b64(inner.as_bytes()), "headers": {"h": ["v"]}}).to_string();
    let plain = json!({"data": "the-data", "headers": {"header-1": ["foo"], "header-2": [], "header-3": ["bar", "baz"]}}).to_string();
    let cases: Vec<(&str, Envelope)> = vec![
        (&with_b64, envelope(inner, json!({"h": ["v"]}))),
        (
            &plain,
            envelope(
                "the-data",
                json!({"header-1": ["foo"], "header-2": [], "header-3": ["bar", "baz"]}),
            ),
        ),
        (
            r#"{"headers":{"k":["v"]}}"#,
            envelope("", json!({"k": ["v"]})),
        ),
        (r#"{"data":"x"}"#, envelope("x", json!({}))),
        (r#"{"data":null,"headers":null}"#, envelope("", json!({}))),
        (r#"{}"#, envelope("", json!({}))),
        (
            "\n {\"data\":\"x\",\"unknown\":1}",
            envelope("x", json!({})),
        ),
    ];
    for (payload, expected) in cases {
        assert_eq!(
            json_decode(payload.as_bytes()).unwrap(),
            expected,
            "{payload}"
        );
    }
}

#[test]
fn json_envelope_errors() {
    // Protobuf bytes (0a 20 ...) with a lost `serializer` header: leading 0x0A/0x20 are JSON
    // whitespace, and the rest is an array whose first element looks like an envelope.
    let mut lost_header = vec![0x0a, 0x20];
    lost_header.extend_from_slice(br#"[{"data":"x","headers":{}}]"#);
    let protobuf = pb(b"x", &[]);
    let cases: Vec<&[u8]> = vec![
        b"not json",
        &protobuf,
        &lost_header,
        br#"[{"data":"x"}]"#,
        br#"["x",{}]"#,
        b"[]",
        b"\"str\"",
        b"42",
        b"true",
        b"null",
        // wrong field types
        br#"{"data":1}"#,
        br#"{"data":{"a":1}}"#,
        br#"{"data":"x","headers":[]}"#,
        br#"{"data":"x","headers":{"h":"v"}}"#,
        br#"{"data":"x","headers":{"h":[1]}}"#,
    ];
    for bad in cases {
        assert!(
            json_decode(bad).is_err(),
            "{}",
            String::from_utf8_lossy(bad)
        );
    }
}

#[test]
fn empty_value_is_empty_envelope() {
    for serializer in [Serializer::Json, Serializer::Protobuf] {
        let env = decode(b"", serializer, UserEncoding::Other).unwrap();
        assert_eq!(env.data, "");
        assert_eq!(env.headers, JsonbVal::empty_object());
    }
}

// ---------- Kafka headers ----------

fn kafka_meta(headers: &[(&str, &str)]) -> SourceMeta {
    let mut owned = OwnedHeaders::new();
    for (k, v) in headers {
        owned = owned.insert(Header {
            key: k,
            value: Some(v.as_bytes()),
        });
    }
    SourceMeta::Kafka(KafkaMeta {
        timestamp: rdkafka::Timestamp::NotAvailable,
        headers: Some(owned),
    })
}

#[test]
fn kafka_headers_select_the_decoder() {
    let buf = pb(b"hi", &[]);
    let looks_b64 = b64(br#"{"a":1}"#);
    let b64_buf = pb(looks_b64.as_bytes(), &[]);
    let json_buf = br#"{"data":"x"}"#;
    let cases: Vec<(&[u8], SourceMeta, Option<Envelope>)> = vec![
        // last header wins
        (
            &buf,
            kafka_meta(&[("serializer", "json"), ("serializer", "protobuf")]),
            Some(envelope("hi", json!({}))),
        ),
        (
            &buf,
            kafka_meta(&[("serializer", "protobuf"), ("serializer", "json")]),
            None,
        ),
        (
            &b64_buf,
            kafka_meta(&[
                ("serializer", "protobuf"),
                ("user-encoding", "plaintext"),
                ("user-encoding", "base64"),
            ]),
            Some(envelope(&looks_b64, json!({}))),
        ),
        (
            &b64_buf,
            kafka_meta(&[("serializer", "protobuf")]),
            Some(envelope(r#"{"a":1}"#, json!({}))),
        ),
        // no serializer header, or a non-Kafka source: JSON
        (json_buf, kafka_meta(&[]), Some(envelope("x", json!({})))),
        (json_buf, SourceMeta::Empty, Some(envelope("x", json!({})))),
        // unknown serializer
        (json_buf, kafka_meta(&[("serializer", "avro")]), None),
    ];
    for (payload, meta, expected) in cases {
        let result = EnvelopeAccessBuilder::decode_record(payload, &meta);
        match expected {
            Some(expected) => assert_eq!(result.unwrap(), expected),
            None => assert!(result.is_err()),
        }
    }
}

// ---------- Access ----------

fn access_of(data: &str, headers: Value) -> EnvelopeAccess {
    EnvelopeAccess(envelope(data, headers))
}

#[test]
fn access_columns() {
    let access = access_of("d", json!({"k": ["v"]}));
    let data = access.access(&[DATA_COLUMN], &DataType::Varchar).unwrap();
    assert_eq!(data.to_datum_ref(), Some(ScalarRefImpl::Utf8("d")));
    let data = access.access(&[DATA_COLUMN], &DataType::Bytea).unwrap();
    assert_eq!(data.to_datum_ref(), Some(ScalarRefImpl::Bytea(b"d")));
    let headers = access.access(&[HEADERS_COLUMN], &DataType::Jsonb).unwrap();
    assert_eq!(headers.to_datum_ref().unwrap().to_text(), r#"{"k": ["v"]}"#);
    let payload = access.access(&[], &DataType::Jsonb).unwrap();
    assert_eq!(
        payload.to_datum_ref().unwrap().to_text(),
        r#"{"data": "d", "headers": {"k": ["v"]}}"#
    );
}

#[test]
fn access_errors() {
    let access = access_of("d", json!({}));
    assert!(access.access(&[DATA_COLUMN], &DataType::Int32).is_err());
    assert!(
        access
            .access(&[HEADERS_COLUMN], &DataType::Varchar)
            .is_err()
    );
    assert!(access.access(&["other"], &DataType::Varchar).is_err());
    assert!(access.access(&[], &DataType::Varchar).is_err());
}

#[tokio::test]
async fn generate_accessor_decodes_the_record() {
    let mut builder = EnvelopeAccessBuilder::new();
    let meta = kafka_meta(&[("serializer", "protobuf")]);
    let payload = pb(b"hello", &[("k", &["v"])]);
    let AccessImpl::Envelope(access) = builder.generate_accessor(payload, &meta).await.unwrap()
    else {
        panic!("expected an envelope accessor");
    };
    assert_eq!(access.0, envelope("hello", json!({"k": ["v"]})));

    let bad_meta = kafka_meta(&[("serializer", "avro")]);
    assert!(builder.generate_accessor(vec![1], &bad_meta).await.is_err());
}
