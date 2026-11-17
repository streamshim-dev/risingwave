//! `FORMAT PLAIN ENCODE ENVELOPE`: Kafka records carrying a `{data, headers}` envelope,
//! serialized either as JSON or as protobuf (announced by the `serializer` message header).
//!
//! Every record is exposed as two columns, `data` (text) and `headers` (jsonb):
//!
//! | record                                   | `data`                                  |
//! |------------------------------------------|-----------------------------------------|
//! | JSON envelope (no `serializer` header, or `json`) | `decode_with_base64_heuristic(data)` |
//! | protobuf, `user-encoding: base64`        | content as UTF-8                        |
//! | protobuf, other or no `user-encoding`    | `decode_with_base64_heuristic(content as UTF-8)` |
//! | empty value                              | `""`, headers `{}`                      |
//!
//! Malformed records, content that is not UTF-8 and unknown `serializer` values are an error,
//! so the row is skipped and counted as a source error.
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
use prost::Message as _;
use rdkafka::message::Headers as _;
use risingwave_common::types::{
    DataType, DatumCow, JsonbVal, Scalar as _, ScalarImpl, ScalarRefImpl,
};
use serde::Deserialize;

pub use self::heuristic::decode_with_base64_heuristic;
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

/// How the envelope is serialized, from the `serializer` Kafka header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Serializer {
    /// No header, or `json`.
    #[default]
    Json,
    Protobuf,
}

impl TryFrom<&[u8]> for Serializer {
    type Error = anyhow::Error;

    fn try_from(header: &[u8]) -> Result<Self, Self::Error> {
        match header {
            b"json" => Ok(Self::Json),
            b"protobuf" => Ok(Self::Protobuf),
            other => Err(anyhow::anyhow!(
                "unsupported serializer header: {}",
                String::from_utf8_lossy(other)
            )),
        }
    }
}

/// How the producer encoded the user content, from the `user-encoding` Kafka header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum UserEncoding {
    /// The protobuf `content` holds the user's raw bytes: no base64 detection is needed.
    Base64,
    /// `plaintext`, a missing header or anything else. The content may still be base64 text
    /// written by the user, so it goes through the base64 heuristic.
    #[default]
    Other,
}

impl From<&[u8]> for UserEncoding {
    fn from(header: &[u8]) -> Self {
        match header {
            b"base64" => Self::Base64,
            _ => Self::Other,
        }
    }
}

/// Decodes one record.
pub fn decode(
    payload: &[u8],
    serializer: Serializer,
    user_encoding: UserEncoding,
) -> ConnectorResult<Envelope> {
    if payload.is_empty() {
        return Ok(Envelope {
            data: String::new(),
            headers: JsonbVal::empty_object(),
        });
    }
    match serializer {
        Serializer::Json => decode_json(payload),
        Serializer::Protobuf => decode_protobuf(payload, user_encoding),
    }
}

fn decode_protobuf(payload: &[u8], user_encoding: UserEncoding) -> ConnectorResult<Envelope> {
    let value = wire::Value::decode(payload).context("failed to decode protobuf envelope")?;
    let content =
        String::from_utf8(value.content).context("protobuf envelope content is not UTF-8")?;
    let data = match user_encoding {
        UserEncoding::Base64 => content,
        UserEncoding::Other => decode_with_base64_heuristic(content),
    };
    let headers: BTreeMap<String, Vec<String>> = value
        .headers
        .into_iter()
        .map(|(k, v)| (k, v.values))
        .collect();
    Ok(Envelope {
        data,
        headers: headers_to_jsonb(&headers)?,
    })
}

/// JSON form of the envelope. Both fields may be missing or `null`.
#[derive(Deserialize)]
struct JsonEnvelope {
    data: Option<String>,
    headers: Option<BTreeMap<String, Vec<String>>>,
}

fn decode_json(payload: &[u8]) -> ConnectorResult<Envelope> {
    // serde reads structs from JSON arrays too (as positional fields), which would silently turn
    // e.g. protobuf bytes with a lost `serializer` header into a wrong row.
    if payload.iter().find(|b| !b.is_ascii_whitespace()) != Some(&b'{') {
        return Err(anyhow::anyhow!("json envelope must be an object").into());
    }
    let envelope: JsonEnvelope =
        serde_json::from_slice(payload).context("failed to parse json envelope")?;
    Ok(Envelope {
        data: envelope
            .data
            .map(decode_with_base64_heuristic)
            .unwrap_or_default(),
        headers: match envelope.headers {
            Some(headers) => headers_to_jsonb(&headers)?,
            None => JsonbVal::empty_object(),
        },
    })
}

fn headers_to_jsonb(headers: &BTreeMap<String, Vec<String>>) -> ConnectorResult<JsonbVal> {
    let value = serde_json::to_value(headers).context("failed to convert envelope headers")?;
    Ok(value.into())
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
        let (serializer, user_encoding) = match meta {
            SourceMeta::Kafka(kafka) => (
                last_header(kafka, SERIALIZER_HEADER)
                    .map(Serializer::try_from)
                    .transpose()?
                    .unwrap_or_default(),
                last_header(kafka, USER_ENCODING_HEADER)
                    .map(UserEncoding::from)
                    .unwrap_or_default(),
            ),
            _ => Default::default(),
        };
        decode(payload, serializer, user_encoding)
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
