//! Cheap detection of base64-wrapped text.
//!
//! Some producers base64-encode the payload before it reaches the envelope. A value is only
//! decoded when it is strict standard base64 that decodes to valid UTF-8; otherwise it is
//! returned unchanged. Plain text that happens to be valid base64 of valid UTF-8 (for example
//! `c29tZXZhbHVl`) is decoded too: the heuristic cannot tell it apart, and downstream consumers
//! are expected to handle JSON text.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;

/// Characters checked against the alphabet before committing to a full decode, so that plain
/// text is rejected after a short prefix instead of a full scan.
const PROBE_LEN: usize = 100;

/// Returns the decoded text if `s` is strict standard base64 (alphabet `A-Z a-z 0-9 + /`, `=`
/// only as canonical padding of the last quadruplet) of UTF-8 text. Otherwise returns `s`
/// unchanged.
pub fn decode_with_base64_heuristic(s: String) -> String {
    match try_decode(s.as_bytes()) {
        Some(decoded) => decoded,
        None => s,
    }
}

fn try_decode(bytes: &[u8]) -> Option<String> {
    // Whole quadruplets only.
    if bytes.len() < 4 || bytes.len() % 4 != 0 {
        return None;
    }
    // Fail fast on the first characters. Padding can only be in the last quadruplet, so when the
    // probe stops short of it, it must be pure alphabet; the strict decode below validates the
    // rest.
    if bytes.len() > PROBE_LEN && !bytes[..PROBE_LEN].iter().all(|b| is_b64_alphabet(*b)) {
        return None;
    }
    // STANDARD enforces the alphabet and canonical padding, and rejects whitespace.
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    STANDARD.decode_vec(bytes, &mut out).ok()?;
    String::from_utf8(out).ok()
}

/// The 64 characters of the standard alphabet, without the padding `=`.
#[inline]
fn is_b64_alphabet(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'+' || b == b'/'
}
