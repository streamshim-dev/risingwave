//! Protobuf form of the envelope, decoded with `prost` derive (no descriptor, no build step).
//!
//! ```proto
//! message Value {
//!   bytes content = 1;
//!   map<string, ValueHeaders> headers = 2;
//! }
//! message ValueHeaders {
//!   repeated string values = 1;
//! }
//! ```

use std::collections::HashMap;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Value {
    #[prost(bytes = "vec", tag = "1")]
    pub content: Vec<u8>,
    #[prost(map = "string, message", tag = "2")]
    pub headers: HashMap<String, ValueHeaders>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ValueHeaders {
    #[prost(string, repeated, tag = "1")]
    pub values: Vec<String>,
}
