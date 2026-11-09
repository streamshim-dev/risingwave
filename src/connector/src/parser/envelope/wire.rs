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
