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

//! `FORMAT PLAIN ENCODE ENVELOPE`: Kafka records carrying a `{data, headers}` envelope,
//! serialized either as JSON or as protobuf (announced by the `serializer` message header).
//!
//! Every record is exposed as two columns, `data` (text) and `headers` (jsonb):
//!
//! | record                                   | `data`                                  |
//! |------------------------------------------|-----------------------------------------|
//! | JSON envelope (no `serializer` header, or `json`) | `maybe_decode(data)`                |
//! | protobuf, `user-encoding: base64`        | content as UTF-8 (base64 if not UTF-8)  |
//! | protobuf, other or no `user-encoding`    | `maybe_decode(content as UTF-8)`        |
//! | empty value                              | `""`, headers `{}`                      |
//!
//! Malformed records and unknown `serializer` values are an error, so the row is skipped and
//! counted as a source error.
//! When a header key repeats, the last value wins.
//!
//! This module is self-contained so it can be carried across RisingWave versions; see
//! `README.md` next to it for the places that register the encode.

mod heuristic;
mod wire;

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

use anyhow::Context as _;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use prost::Message as _;
use rdkafka::message::Headers as _;
use risingwave_common::types::{
    DataType, DatumCow, JsonbVal, Scalar as _, ScalarImpl, ScalarRefImpl,
};

pub use self::heuristic::maybe_decode;
use super::unified::{Access, AccessError, AccessImpl, AccessResult};
use crate::error::ConnectorResult;
use crate::parser::AccessBuilder;
use crate::source::SourceMeta;
use crate::source::kafka::KafkaMeta;

pub const DATA_COLUMN: &str = "data";
pub const HEADERS_COLUMN: &str = "headers";

const SERIALIZER_HEADER: &str = "serializer";
const USER_ENCODING_HEADER: &str = "user-encoding";

/// A decoded record.
#[derive(Debug, Clone, PartialEq)]
pub struct Envelope {
    pub data: String,
    pub headers: JsonbVal,
}

/// Decodes one record. `serializer` and `user_encoding` are the raw Kafka header values.
pub fn decode(
    payload: &[u8],
    serializer: Option<&[u8]>,
    user_encoding: Option<&[u8]>,
) -> ConnectorResult<Envelope> {
    if payload.is_empty() {
        return Ok(Envelope {
            data: String::new(),
            headers: JsonbVal::empty_object(),
        });
    }
    match serializer {
        None | Some(b"json") => decode_json(payload),
        Some(b"protobuf") => decode_protobuf(payload, user_encoding == Some(b"base64")),
        Some(other) => Err(anyhow::anyhow!(
            "unsupported serializer header: {}",
            String::from_utf8_lossy(other)
        )
        .into()),
    }
}

fn decode_protobuf(payload: &[u8], raw_bytes: bool) -> ConnectorResult<Envelope> {
    let value = wire::Value::decode(payload).context("failed to decode protobuf envelope")?;
    let data = if raw_bytes {
        // Content holds the user's raw bytes: expose them as text, or as base64 if not UTF-8
        // so nothing is lost.
        match String::from_utf8(value.content) {
            Ok(text) => text,
            Err(err) => STANDARD.encode(err.as_bytes()),
        }
    } else {
        maybe_decode(match String::from_utf8(value.content) {
            Ok(text) => text,
            Err(err) => String::from_utf8_lossy(err.as_bytes()).into_owned(),
        })
    };
    let headers: BTreeMap<String, Vec<String>> = value
        .headers
        .into_iter()
        .map(|(k, v)| (k, v.values))
        .collect();
    let headers = serde_json::to_value(headers).context("failed to convert envelope headers")?;
    Ok(Envelope {
        data,
        headers: headers.into(),
    })
}

fn decode_json(payload: &[u8]) -> ConnectorResult<Envelope> {
    // Parse as a generic value and require an object: deserializing straight into a struct
    // would also accept a JSON array whose first element is an object (serde reads structs
    // from sequences), which silently turns e.g. protobuf bytes with a lost `serializer`
    // header into a wrong row.
    let serde_json::Value::Object(mut envelope) =
        serde_json::from_slice(payload).context("failed to parse json envelope")?
    else {
        return Err(anyhow::anyhow!("json envelope must be an object").into());
    };
    let data = match envelope.remove("data") {
        None | Some(serde_json::Value::Null) => String::new(),
        Some(serde_json::Value::String(s)) => maybe_decode(s),
        Some(other) => other.to_string(),
    };
    let headers = match envelope.remove("headers") {
        None | Some(serde_json::Value::Null) => JsonbVal::empty_object(),
        Some(h) => h.into(),
    };
    Ok(Envelope { data, headers })
}

/// Value of the last header named `key`, like Java's `Headers::lastHeader`.
fn last_header<'a>(meta: &'a KafkaMeta, key: &str) -> Option<&'a [u8]> {
    let headers = meta.headers.as_ref()?;
    headers
        .iter()
        .filter(|h| h.key == key)
        .last()
        .and_then(|h| h.value)
}

#[derive(Debug, Default)]
pub struct EnvelopeAccessBuilder;

impl EnvelopeAccessBuilder {
    pub fn new() -> Self {
        Self
    }

    pub fn decode_record(payload: &[u8], meta: &SourceMeta) -> ConnectorResult<Envelope> {
        match meta {
            SourceMeta::Kafka(kafka) => decode(
                payload,
                last_header(kafka, SERIALIZER_HEADER),
                last_header(kafka, USER_ENCODING_HEADER),
            ),
            _ => decode(payload, None, None),
        }
    }
}

impl AccessBuilder for EnvelopeAccessBuilder {
    async fn generate_accessor(
        &mut self,
        payload: Vec<u8>,
        source_meta: &SourceMeta,
    ) -> ConnectorResult<AccessImpl<'_>> {
        Ok(AccessImpl::Envelope(EnvelopeAccess(Self::decode_record(
            &payload,
            source_meta,
        )?)))
    }
}

/// Column access over a decoded [`Envelope`], without re-serializing it.
#[derive(Debug)]
pub struct EnvelopeAccess(pub Envelope);

impl Access for EnvelopeAccess {
    fn access<'a>(&'a self, path: &[&str], type_expected: &DataType) -> AccessResult<DatumCow<'a>> {
        let type_error = |got: &str| AccessError::TypeError {
            expected: type_expected.to_string(),
            got: got.to_owned(),
            value: String::new(),
        };
        match path {
            [DATA_COLUMN] => match type_expected {
                DataType::Varchar => Ok(DatumCow::Borrowed(Some(ScalarRefImpl::Utf8(
                    self.0.data.as_str(),
                )))),
                DataType::Bytea => Ok(DatumCow::Borrowed(Some(ScalarRefImpl::Bytea(
                    self.0.data.as_bytes(),
                )))),
                _ => Err(type_error("varchar")),
            },
            [HEADERS_COLUMN] => match type_expected {
                DataType::Jsonb => Ok(DatumCow::Borrowed(Some(ScalarRefImpl::Jsonb(
                    self.0.headers.as_scalar_ref(),
                )))),
                _ => Err(type_error("jsonb")),
            },
            // `INCLUDE payload`: the whole envelope as jsonb.
            [] if *type_expected == DataType::Jsonb => {
                let value = serde_json::json!({
                    "data": self.0.data,
                    "headers": self.0.headers.clone().take(),
                });
                Ok(DatumCow::Owned(Some(ScalarImpl::Jsonb(value.into()))))
            }
            _ => Err(AccessError::Undefined {
                name: path.join("."),
                path: String::new(),
            }),
        }
    }
}
