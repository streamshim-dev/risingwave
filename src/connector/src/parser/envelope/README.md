# `ENCODE ENVELOPE`

Kafka source encode for records that carry a `{data, headers}` envelope, as JSON or as
protobuf (`serializer: protobuf` message header). See `mod.rs` for the decoding rules.

```sql
CREATE SOURCE s (data VARCHAR, headers JSONB)
INCLUDE offset AS kafka_offset INCLUDE timestamp AS kafka_timestamp
WITH (connector = 'kafka', topic = 't', properties.bootstrap.server = '...')
FORMAT PLAIN ENCODE ENVELOPE;
```

## Porting to a new RisingWave version

This directory is self-contained and should apply unchanged. The registration commit adds
one arm at each place below; on conflicts, mirror whatever `ENCODE BYTES` / `ENCODE JSON` do
there in the new version.

| File | What |
|---|---|
| `proto/plan_common.proto` | `ENCODE_TYPE_ENVELOPE = 1001` (keep the high number: the catalog persists it) |
| `src/sqlparser/src/ast/statement.rs` | `Encode::Envelope`, `Display`, `from_keyword` |
| `src/sqlparser/tests/testdata/create.yaml` | parse round trip |
| `src/connector/src/source/base.rs` | `SourceEncode::Envelope`, `extract_source_struct` |
| `src/connector/src/parser/mod.rs` | `mod envelope;` |
| `src/connector/src/parser/config.rs` | `EncodingProperties::Envelope`, `SpecificParserConfig::new` |
| `src/connector/src/parser/access_builder.rs` | `AccessBuilderImpl::Envelope` |
| `src/connector/src/parser/unified/mod.rs` | `AccessImpl::Envelope` |
| `src/connector/src/parser/plain_parser.rs` | accept `Envelope` for `FORMAT PLAIN` |
| `src/connector/src/source/kafka/source/reader.rs` | `require_message_header` must be true for `Envelope` |
| `src/connector/src/sink/catalog/mod.rs` | reject as sink encode / key encode |
| `src/frontend/src/handler/create_source/validate.rs` | Kafka + `FORMAT PLAIN` only |
| `src/frontend/src/handler/create_source/external_schema.rs` | no external schema, `row_encode_to_prost` |
| `src/frontend/src/handler/create_source.rs` | column check: `data` varchar/bytea, `headers` jsonb |
| `src/frontend/src/handler/create_source/additional_column.rs` | allow `INCLUDE payload` |
| `src/frontend/src/handler/alter_source_column.rs` | alterable like JSON |
| `src/frontend/src/handler/alter_source_with_sr.rs` | `EncodeType` -> `Encode` |
| `src/frontend/src/handler/create_sink.rs` | reject as sink encode |

Tests: `cargo test -p risingwave_connector --lib parser::envelope` and
`e2e_test/source_inline/kafka/envelope/`.
