//! Cheap detection of base64-wrapped JSON text.
//!
//! The function never produces malformed output: when in doubt it returns the input unchanged.
//! Most inputs are rejected after looking at 1 to 20 bytes; only text whose decoded prefix
//! starts like JSON pays for a full decode.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;

/// Characters probed before committing to a full decode. A multiple of 4.
const PROBE_LEN: usize = 20;

/// Returns the decoded text if `s` is strict standard base64 (alphabet `A-Z a-z 0-9 + /`, `=`
/// only as padding of the last quadruplet) of UTF-8 text that starts with `{` or `[` (after
/// optional ASCII whitespace). Otherwise returns `s` unchanged.
pub fn maybe_decode(s: String) -> String {
    match try_decode(s.as_bytes()) {
        Some(decoded) => decoded,
        None => s,
    }
}

fn try_decode(bytes: &[u8]) -> Option<String> {
    // Strict base64 carries no whitespace, so any leading or trailing whitespace means plain
    // text. This also covers JSON text with leading whitespace.
    let (first, last) = (*bytes.first()?, *bytes.last()?);
    if first.is_ascii_whitespace() || last.is_ascii_whitespace() {
        return None;
    }
    // Already JSON (also covers every protobuf content and plain JSON text).
    if matches!(first, b'{' | b'[') {
        return None;
    }
    // Whole quadruplets only, and the last one must be `****`, `***=` or `**==`.
    if bytes.len() < 4 || bytes.len() % 4 != 0 || !is_valid_last_quad(&bytes[bytes.len() - 4..]) {
        return None;
    }

    // Probe: decode only the first PROBE_LEN chars into a stack buffer. Padding can only be in
    // the last quadruplet, so a probe that stops short of it must not contain `=`.
    if bytes.len() > PROBE_LEN {
        let probe = &bytes[..PROBE_LEN];
        if !probe.iter().all(|b| is_b64_alphabet(*b)) {
            return None;
        }
        let mut head = [0u8; PROBE_LEN / 4 * 3];
        let written = STANDARD.decode_slice(probe, &mut head).ok()?;
        let first = head[..written].iter().find(|b| !b.is_ascii_whitespace())?;
        if !matches!(first, b'{' | b'[') {
            return None;
        }
    }

    // Full decode, once, into a pre-sized buffer. STANDARD enforces the alphabet and canonical
    // padding.
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    STANDARD.decode_vec(bytes, &mut out).ok()?;
    // Short inputs skip the probe, so check the decoded start here.
    let first = out.iter().find(|b| !b.is_ascii_whitespace())?;
    if !matches!(first, b'{' | b'[') {
        return None;
    }
    String::from_utf8(out).ok()
}

/// The last quadruplet of padded base64 is `****`, `***=` or `**==`.
#[inline]
fn is_valid_last_quad(quad: &[u8]) -> bool {
    let [a, b, c, d] = [quad[0], quad[1], quad[2], quad[3]];
    is_b64_alphabet(a)
        && is_b64_alphabet(b)
        && match (c, d) {
            (b'=', b'=') => true,
            (b'=', _) => false,
            (_, b'=') => is_b64_alphabet(c),
            _ => is_b64_alphabet(c) && is_b64_alphabet(d),
        }
}

/// The 64 characters of the standard alphabet, without the padding `=`.
#[inline]
fn is_b64_alphabet(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'+' || b == b'/'
}
