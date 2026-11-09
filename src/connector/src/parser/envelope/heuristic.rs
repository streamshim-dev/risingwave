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

//! Cheap detection of base64-wrapped JSON text.
//!
//! The function never produces malformed output: when in doubt it returns the input unchanged.
//! Most inputs are rejected after looking at 1 to 20 bytes; only text whose decoded prefix
//! starts like JSON pays for a full decode.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;

/// Characters probed before committing to a full decode. A multiple of 4.
const PROBE_LEN: usize = 20;

/// Returns the decoded text if `s` is standard, padded base64 of UTF-8 text that starts with
/// `{` or `[` (after optional ASCII whitespace). Otherwise returns `s` unchanged.
pub fn maybe_decode(s: String) -> String {
    match try_decode(s.as_bytes()) {
        Some(decoded) => decoded,
        None => s,
    }
}

fn try_decode(bytes: &[u8]) -> Option<String> {
    let start = bytes.iter().position(|b| !b.is_ascii_whitespace())?;
    let body = &bytes[start..];
    // Already JSON (also covers every protobuf content and plain JSON text).
    if matches!(body[0], b'{' | b'[') {
        return None;
    }
    // Base64 never carries whitespace in our producers; trailing whitespace means plain text.
    if body.len() < 4 || body.len() % 4 != 0 {
        return None;
    }

    // Probe: decode only the first PROBE_LEN chars into a stack buffer.
    let probe_len = PROBE_LEN.min(body.len());
    let probe = &body[..probe_len];
    if !probe.iter().all(|b| is_b64_alphabet(*b)) {
        return None;
    }
    let mut head = [0u8; PROBE_LEN / 4 * 3];
    let written = if probe_len == body.len() {
        // The whole input fits in the probe, so it may end with padding.
        STANDARD.decode_slice(probe, &mut head).ok()?
    } else {
        // Padding can only appear at the very end, never inside the probe.
        if probe.contains(&b'=') {
            return None;
        }
        STANDARD.decode_slice(probe, &mut head).ok()?
    };
    let first = head[..written].iter().find(|b| !b.is_ascii_whitespace())?;
    if !matches!(first, b'{' | b'[') {
        return None;
    }

    // Full decode, once, into a pre-sized buffer. STANDARD enforces canonical padding.
    let mut out = Vec::with_capacity(body.len() / 4 * 3);
    STANDARD.decode_vec(body, &mut out).ok()?;
    String::from_utf8(out).ok()
}

#[inline]
fn is_b64_alphabet(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'+' || b == b'/' || b == b'='
}
