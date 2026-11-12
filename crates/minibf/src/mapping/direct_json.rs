//! JSON bodies written straight into the response, without the
//! blockfrost-openapi models and `serde_json`.
//!
//! For bodies big enough that encoding through the models dominates the
//! request, and for bodies that hold nested `serde_json::Value`s, which
//! [`write_value`] writes and [`drop_value`] frees at any nesting depth. A
//! writer must produce the bytes the serde path would, and its tests hold it
//! to that path.

use axum::{
    http::{header, HeaderValue},
    response::{IntoResponse, Response},
};

const HEX: &[u8; 16] = b"0123456789abcdef";

/// A body that writes its own JSON, byte for byte what `serde_json` encodes
/// for the model it stands in for.
pub trait WriteJson {
    /// Bytes to reserve before writing.
    fn size_hint(&self) -> usize {
        0
    }

    fn write_json(&self, out: &mut Vec<u8>);

    fn to_json_vec(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.size_hint());
        self.write_json(&mut out);
        out
    }
}

/// A [`WriteJson`] body, encoded and served as `application/json` like
/// `Json` serves a model.
///
/// Unlike `Json`, it encodes when built, so a heavy body can be encoded on the
/// blocking pool rather than on an async worker.
pub struct DirectJson(Vec<u8>);

impl DirectJson {
    pub fn encode(body: &impl WriteJson) -> Self {
        Self(body.to_json_vec())
    }
}

impl IntoResponse for DirectJson {
    fn into_response(self) -> Response {
        (
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            )],
            self.0,
        )
            .into_response()
    }
}

/// A JSON string, escaped as `serde_json` escapes it.
pub fn write_str(out: &mut Vec<u8>, value: &str) {
    serde_json::to_writer(&mut *out, value).expect("writing to a Vec cannot fail");
}

/// Bytes as a lowercase hex JSON string.
pub fn write_hex(out: &mut Vec<u8>, bytes: &[u8]) {
    out.push(b'"');
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize]);
        out.push(HEX[(byte & 0xf) as usize]);
    }
    out.push(b'"');
}

/// An integer as a JSON number.
pub fn write_int(out: &mut Vec<u8>, value: impl itoa::Integer) {
    out.extend_from_slice(itoa::Buffer::new().format(value).as_bytes());
}

/// An integer as a decimal JSON string, the way quantities are served.
pub fn write_int_str(out: &mut Vec<u8>, value: impl itoa::Integer) {
    out.push(b'"');
    write_int(out, value);
    out.push(b'"');
}

/// A `serde_json::Value`, byte for byte what `serde_json` encodes for it.
///
/// The parts still to write wait on a heap stack, so the call stack does not
/// grow with each nesting level of `value`. Strings, numbers and the other
/// leaves go through `serde_json`.
pub fn write_value(out: &mut Vec<u8>, value: &serde_json::Value) {
    enum Step<'a> {
        Text(&'static [u8]),
        Key(&'a str),
        Value(&'a serde_json::Value),
    }

    let mut stack = vec![Step::Value(value)];

    while let Some(step) = stack.pop() {
        match step {
            Step::Text(text) => out.extend_from_slice(text),
            Step::Key(key) => {
                write_str(out, key);
                out.push(b':');
            }
            // the stack pops the last push first, so the children go on in
            // reverse
            Step::Value(serde_json::Value::Array(items)) => {
                out.push(b'[');
                stack.push(Step::Text(b"]"));
                for (i, item) in items.iter().enumerate().rev() {
                    stack.push(Step::Value(item));
                    if i > 0 {
                        stack.push(Step::Text(b","));
                    }
                }
            }
            Step::Value(serde_json::Value::Object(fields)) => {
                out.push(b'{');
                stack.push(Step::Text(b"}"));
                for (i, (key, value)) in fields.iter().enumerate().rev() {
                    stack.push(Step::Value(value));
                    stack.push(Step::Key(key));
                    if i > 0 {
                        stack.push(Step::Text(b","));
                    }
                }
            }
            Step::Value(leaf) => {
                serde_json::to_writer(&mut *out, leaf).expect("writing to a Vec cannot fail")
            }
        }
    }
}

/// Frees a `serde_json::Value` with a heap stack. The `Drop` of `Value` uses
/// a call stack frame for each nesting level, so a body that holds nested
/// values frees them with this.
pub fn drop_value(value: serde_json::Value) {
    let mut stack = vec![value];

    while let Some(value) = stack.pop() {
        match value {
            serde_json::Value::Array(items) => stack.extend(items),
            serde_json::Value::Object(fields) => {
                stack.extend(fields.into_iter().map(|(_, value)| value))
            }
            _ => {}
        }
    }
}

/// A `serde_json::Value` body. It writes and frees the value with
/// [`write_value`] and [`drop_value`].
pub struct ValueBody(pub serde_json::Value);

impl WriteJson for ValueBody {
    fn write_json(&self, out: &mut Vec<u8>) {
        write_value(out, &self.0);
    }
}

impl Drop for ValueBody {
    fn drop(&mut self) {
        drop_value(std::mem::take(&mut self.0));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    /// A small value of random shape, with every kind of leaf and strings that
    /// need escapes.
    fn random_value(seed: &mut u64, depth: usize) -> Value {
        let mut next = |n: u64| {
            *seed ^= *seed << 13;
            *seed ^= *seed >> 7;
            *seed ^= *seed << 17;
            *seed % n
        };

        let texts = [
            "",
            "a",
            "q\"uote",
            "back\\slash",
            "line\nfeed",
            "\u{7}",
            "é€😀",
        ];

        if depth == 0 || next(4) == 0 {
            return match next(8) {
                0 => Value::Null,
                1 => Value::Bool(next(2) == 0),
                2 => json!(next(1000) as i64 - 500),
                3 => json!(u64::MAX - next(3)),
                4 => json!(i64::MIN + next(3) as i64),
                5 => json!((next(100_000) as f64 - 50_000.0) / 7.0),
                _ => Value::String(texts[next(texts.len() as u64) as usize].to_string()),
            };
        }

        let len = next(4) as usize;
        match next(2) {
            0 => Value::Array((0..len).map(|_| random_value(seed, depth - 1)).collect()),
            _ => Value::Object(
                (0..len)
                    .map(|i| {
                        let key = format!("{}{i}", texts[i % texts.len()]);
                        (key, random_value(seed, depth - 1))
                    })
                    .collect(),
            ),
        }
    }

    #[test]
    fn value_matches_serde() {
        let mut seed = 0x9e37_79b9_7f4a_7c15;

        for _ in 0..1000 {
            let value = random_value(&mut seed, 5);
            assert_eq!(
                ValueBody(value.clone()).to_json_vec(),
                serde_json::to_vec(&value).unwrap()
            );
            drop_value(value);
        }
    }

    /// A value with many nesting levels writes and frees in full.
    #[test]
    fn value_writes_and_frees_nested_values() {
        use crate::test_support::{nested_value, VALUE_LEVELS as LEVELS};

        let body = ValueBody(nested_value(LEVELS)).to_json_vec();

        assert_eq!(
            body,
            format!("{}0{}", r#"{"k":["#.repeat(LEVELS), "]}".repeat(LEVELS)).into_bytes()
        );
    }
}
