// Copyright 2026 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::time::Instant;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use prost::Message as _;
use rdkafka::message::{Header, OwnedHeaders};
use risingwave_common::types::{DataType, JsonbVal, ScalarRefImpl, ToDatumRef};
use serde_json::{Value, json};

use super::*;

fn b64(s: &[u8]) -> String {
    STANDARD.encode(s)
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

fn pb_decode(buf: &[u8], base64_encoding: bool) -> Envelope {
    let enc: &[u8] = if base64_encoding {
        b"base64"
    } else {
        b"plaintext"
    };
    decode(buf, Some(b"protobuf"), Some(enc)).unwrap()
}

fn headers(env: &Envelope) -> Value {
    env.headers.clone().take()
}

// ---------- heuristic ----------

#[test]
fn json_text_passes_through() {
    for s in [r#"{"a":1}"#, "[1,2]", "  \n{\"a\":1}", "\t[]"] {
        assert_eq!(maybe_decode(s.to_owned()), s);
    }
}

#[test]
fn base64_json_is_decoded() {
    let obj = r#"{"kind":"Event","n":1}"#;
    assert_eq!(maybe_decode(b64(obj.as_bytes())), obj);
    let arr = r#"[{"data":"x"},{"data":"y"}]"#;
    assert_eq!(maybe_decode(b64(arr.as_bytes())), arr);
    // leading whitespace inside the decoded text
    let ws = "\n  {\"a\":1}";
    assert_eq!(maybe_decode(b64(ws.as_bytes())), ws);
    // short enough to fit in the probe, with padding
    assert_eq!(maybe_decode(b64(b"{}")), "{}");
    assert_eq!(maybe_decode(b64(b"[1]")), "[1]");
}

#[test]
fn plain_text_is_untouched() {
    let samples = [
        "",
        "   ",
        "test",
        "abcd",
        "abcd1234",
        "hello world",
        // sha256 hex: valid alphabet, decodes to non-JSON
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        // base64 of non-JSON text
        &b64(b"just some plain text payload that is long enough"),
        // base64 of invalid UTF-8
        &b64(&[0xC3, 0x28, 0x00, 0x7F, 0xFF]),
        "c29tZXZhbHVl", // "somevalue": base64, but not JSON
    ];
    for s in samples {
        assert_eq!(maybe_decode(s.to_owned()), s, "input {s:?}");
    }
}

#[test]
fn malformed_base64_is_untouched() {
    let good = b64(br#"{"a":"some longer json value here"}"#);
    // wrong length
    let s = format!("{good}A");
    assert_eq!(maybe_decode(s.clone()), s);
    // padding in the middle
    let s = format!("eyJh={}", &good[5..]);
    assert_eq!(maybe_decode(s.clone()), s);
    // invalid char after the probe
    let mut s = good.clone();
    s.replace_range(24..25, "*");
    assert_eq!(maybe_decode(s.clone()), s);
    // url-safe alphabet is not standard base64
    let s = good.replace('+', "-").replace('/', "_");
    if s != good {
        assert_eq!(maybe_decode(s.clone()), s);
    }
    // trailing newline (MIME style) is not decoded
    let s = format!("{good}\n");
    assert_eq!(maybe_decode(s.clone()), s);
}

#[test]
fn non_canonical_base64_is_untouched() {
    // "{}" is "e30=": change the last data char so the unused bits are set.
    assert_eq!(maybe_decode("e31=".to_owned()), "e31=");
}

#[test]
fn base64_of_invalid_utf8_json_prefix_is_untouched() {
    let mut bytes = br#"{"a":""#.to_vec();
    bytes.extend([0xff, 0xfe, b'"', b'}']);
    let s = b64(&bytes);
    assert_eq!(maybe_decode(s.clone()), s);
}

#[test]
fn large_base64_json_is_decoded() {
    let big = format!(r#"[{{"data":"{}"}}]"#, "y".repeat(300_000));
    assert_eq!(maybe_decode(b64(big.as_bytes())), big);
}

// ---------- protobuf envelope ----------

#[test]
fn pb_plain_text_content() {
    let buf = pb(br#"{"a":1}"#, &[("Content-Type", &["application/json"])]);
    let env = pb_decode(&buf, false);
    assert_eq!(env.data, r#"{"a":1}"#);
    assert_eq!(headers(&env), json!({"Content-Type": ["application/json"]}));
}

#[test]
fn pb_base64_encoding_exposes_raw_text() {
    // user-encoding base64 means content holds the raw user bytes: no heuristic, no re-encode.
    let env = pb_decode(&pb(b"somevalue", &[]), true);
    assert_eq!(env.data, "somevalue");
    let env = pb_decode(&pb(br#"[{"x":1}]"#, &[]), true);
    assert_eq!(env.data, r#"[{"x":1}]"#);
    // raw text that happens to look like base64 JSON is NOT decoded on this path
    let looks_b64 = b64(br#"{"a":1}"#);
    let env = pb_decode(&pb(looks_b64.as_bytes(), &[]), true);
    assert_eq!(env.data, looks_b64);
}

#[test]
fn pb_base64_encoding_non_utf8_stays_base64() {
    let raw = [0xC3, 0x28, 0x00, 0x7F, 0xFF];
    let env = pb_decode(&pb(&raw, &[]), true);
    assert_eq!(env.data, "wygAf/8=");
}

#[test]
fn pb_plaintext_runs_heuristic() {
    let inner = r#"[{"data":"x"}]"#;
    let env = pb_decode(&pb(b64(inner.as_bytes()).as_bytes(), &[]), false);
    assert_eq!(env.data, inner);
    // missing user-encoding behaves like plaintext
    let buf = pb(b64(inner.as_bytes()).as_bytes(), &[]);
    let env = decode(&buf, Some(b"protobuf"), None).unwrap();
    assert_eq!(env.data, inner);
    let env = pb_decode(&pb(b"some content", &[]), false);
    assert_eq!(env.data, "some content");
}

#[test]
fn pb_multi_value_and_multi_header() {
    let env = pb_decode(
        &pb(b"x", &[("h1", &["a", "b"]), ("h2", &["c"]), ("h3", &[])]),
        false,
    );
    assert_eq!(
        headers(&env),
        json!({"h1": ["a", "b"], "h2": ["c"], "h3": []})
    );
}

#[test]
fn pb_repeated_map_key_keeps_last() {
    let mut buf = pb(b"x", &[("h", &["old"])]);
    buf.extend(pb(b"", &[("h", &["new"])]));
    let env = pb_decode(&buf, false);
    assert_eq!(headers(&env), json!({"h": ["new"]}));
}

#[test]
fn pb_headers_without_content() {
    // publish emits this for empty data with headers
    let mut buf = pb(b"", &[("k", &["v"])]);
    buf.retain(|_| true);
    let env = pb_decode(&buf, false);
    assert_eq!(env.data, "");
    assert_eq!(headers(&env), json!({"k": ["v"]}));
}

#[test]
fn pb_multibyte_and_long_content() {
    let text = "héllo ✓ 日本語";
    assert_eq!(pb_decode(&pb(text.as_bytes(), &[]), false).data, text);
    let big = "y".repeat(300_000);
    assert_eq!(pb_decode(&pb(big.as_bytes(), &[]), false).data, big);
}

#[test]
fn pb_invalid_utf8_plaintext_is_lossy() {
    let env = pb_decode(&pb(&[b'a', 0xff, b'b'], &[]), false);
    assert_eq!(env.data, "a\u{fffd}b");
}

#[test]
fn pb_unknown_fields_are_skipped() {
    let mut buf = pb(b"x", &[]);
    buf.extend([0x18, 0x07]); // field 3, varint
    buf.extend([0x4a, 0x02, b'z', b'z']); // field 9, len-delimited
    let env = pb_decode(&buf, false);
    assert_eq!(env.data, "x");
    assert_eq!(headers(&env), json!({}));
}

#[test]
fn pb_malformed_is_an_error() {
    let cases: [&[u8]; 4] = [
        &[0x0a, 0x05, b'a', b'b'], // length says 5, 2 bytes follow
        &[0x0a],                   // tag only
        &[
            0x0a, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        ], // bad varint
        br#"{"data":"x"}"#,        // JSON wrongly tagged as protobuf
    ];
    for buf in cases {
        assert!(
            decode(buf, Some(b"protobuf"), None).is_err(),
            "expected error for {buf:?}"
        );
    }
}

// ---------- JSON envelope ----------

#[test]
fn json_envelope_classic_base64_is_decoded() {
    let inner = r#"[{"kind":"Event"}]"#;
    let payload = json!({"data": b64(inner.as_bytes()), "headers": {"h": ["v"]}}).to_string();
    let env = decode(payload.as_bytes(), None, None).unwrap();
    assert_eq!(env.data, inner);
    assert_eq!(headers(&env), json!({"h": ["v"]}));
}

#[test]
fn json_envelope_plain_data_untouched() {
    let payload = json!({"data": "the-data", "headers": {"header-1": ["foo"], "header-2": [], "header-3": ["bar", "baz"]}}).to_string();
    let env = decode(payload.as_bytes(), None, None).unwrap();
    assert_eq!(env.data, "the-data");
    assert_eq!(
        headers(&env),
        json!({"header-1": ["foo"], "header-2": [], "header-3": ["bar", "baz"]})
    );
    // explicit json serializer header behaves the same
    let env = decode(payload.as_bytes(), Some(b"json"), None).unwrap();
    assert_eq!(env.data, "the-data");
}

#[test]
fn json_envelope_missing_fields() {
    let env = decode(br#"{"headers":{"k":["v"]}}"#, None, None).unwrap();
    assert_eq!(env.data, "");
    let env = decode(br#"{"data":"x"}"#, None, None).unwrap();
    assert_eq!(headers(&env), json!({}));
    let env = decode(br#"{"data":null,"headers":null}"#, None, None).unwrap();
    assert_eq!(env.data, "");
    assert_eq!(headers(&env), json!({}));
}

#[test]
fn json_envelope_malformed_is_an_error() {
    assert!(decode(b"not json", None, None).is_err());
    // protobuf bytes without the serializer header
    assert!(decode(&pb(b"x", &[]), None, None).is_err());
}

#[test]
fn empty_value_is_empty_envelope() {
    for ser in [None, Some(&b"protobuf"[..])] {
        let env = decode(b"", ser, None).unwrap();
        assert_eq!(env.data, "");
        assert_eq!(env.headers, JsonbVal::empty_object());
    }
}

#[test]
fn unknown_serializer_is_an_error() {
    assert!(decode(br#"{"data":"x"}"#, Some(b"avro"), None).is_err());
    assert!(decode(br#"{"data":"x"}"#, Some(b"Protobuf"), None).is_err());
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
fn last_kafka_header_wins() {
    let buf = pb(b"hi", &[]);
    // json first, protobuf last => protobuf
    let meta = kafka_meta(&[("serializer", "json"), ("serializer", "protobuf")]);
    let env = EnvelopeAccessBuilder::decode_record(&buf, &meta).unwrap();
    assert_eq!(env.data, "hi");
    // protobuf first, json last => parsed as JSON => error on protobuf bytes
    let meta = kafka_meta(&[("serializer", "protobuf"), ("serializer", "json")]);
    assert!(EnvelopeAccessBuilder::decode_record(&buf, &meta).is_err());
    let meta = kafka_meta(&[
        ("serializer", "protobuf"),
        ("user-encoding", "plaintext"),
        ("user-encoding", "base64"),
    ]);
    let looks_b64 = b64(br#"{"a":1}"#);
    let env = EnvelopeAccessBuilder::decode_record(&pb(looks_b64.as_bytes(), &[]), &meta).unwrap();
    assert_eq!(env.data, looks_b64);
}

#[test]
fn non_kafka_meta_is_json() {
    let env = EnvelopeAccessBuilder::decode_record(br#"{"data":"x"}"#, &SourceMeta::Empty).unwrap();
    assert_eq!(env.data, "x");
}

// ---------- Access ----------

#[test]
fn access_columns() {
    let access = EnvelopeAccess(Envelope {
        data: "d".to_owned(),
        headers: serde_json::json!({"k": ["v"]}).into(),
    });
    let data = access.access(&[DATA_COLUMN], &DataType::Varchar).unwrap();
    assert_eq!(data.to_datum_ref(), Some(ScalarRefImpl::Utf8("d")));
    let h = access.access(&[HEADERS_COLUMN], &DataType::Jsonb).unwrap();
    assert_eq!(h.to_datum_ref().unwrap().to_text(), r#"{"k": ["v"]}"#);
    assert!(access.access(&[DATA_COLUMN], &DataType::Int32).is_err());
    assert!(access.access(&["other"], &DataType::Varchar).is_err());
    assert!(access.access(&[], &DataType::Jsonb).is_ok());
}

// ---------- sanity timing (run with --ignored --nocapture) ----------

#[test]
#[ignore]
fn bench_maybe_decode() {
    let json = r#"{"kind":"Event","apiVersion":"audit.k8s.io/v1"}"#.repeat(60);
    let b64_json = b64(format!("[{json}]").as_bytes());
    let text = "plain user text payload, nothing to decode here".repeat(60);
    for (name, input) in [("json", json), ("base64-json", b64_json), ("text", text)] {
        let n = 1_000_000;
        let start = Instant::now();
        for _ in 0..n {
            std::hint::black_box(maybe_decode(std::hint::black_box(input.clone())));
        }
        let per = start.elapsed() / n;
        println!(
            "{name:12} {} bytes: {per:?}/call (incl. clone)",
            input.len()
        );
    }
}
