//! JSON bodies written straight into the response, without the
//! blockfrost-openapi models and `serde_json`.
//!
//! For bodies big enough that encoding through the models dominates the
//! request. A writer must produce the bytes the serde path would, and its tests
//! hold it to that path.

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

/// Serves a [`WriteJson`] body as `application/json`, as `Json` serves a
/// model.
pub struct DirectJson<T>(pub T);

impl<T: WriteJson> IntoResponse for DirectJson<T> {
    fn into_response(self) -> Response {
        (
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            )],
            self.0.to_json_vec(),
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
