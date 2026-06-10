//! Field-mask strategies for gRPC responses.
//!
//! A `google.protobuf.FieldMask` lets a client request a subset of a response's
//! fields. Two strategies for honoring it coexist here, picked per endpoint:
//!
//! - [`apply_mask`] is a *post-hoc projection*: it round-trips an already-built
//!   response through JSON and keeps only the requested paths. It works for any
//!   message but pays the cost of building (and serializing) fields the client
//!   discards, so it suits low-volume Query responses.
//! - [`BlockMask`] is a *source-level projection*: it interprets the mask up
//!   front so the producer can skip building representations the client did not
//!   request. This suits the high-volume Sync block streams, where parsing each
//!   block into its `AnyChainBlock` representation is the dominant cost.

use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};

fn get_nested_value(value: &Value, keys: &[&str]) -> Option<Value> {
    let mut current = value;
    for key in keys {
        match current {
            Value::Object(map) => current = map.get(*key)?,
            _ => return None,
        }
    }
    Some(current.clone())
}

fn set_nested_value(value: &mut Value, keys: &[&str], new_value: Value) {
    let mut current = value;
    for key in keys.iter().take(keys.len() - 1) {
        current = current
            .as_object_mut()
            .unwrap() // Safe to unwrap when used in conjunction with get_nested_value
            .entry(key.to_string())
            .or_insert_with(|| json!({}));
    }
    current
        .as_object_mut()
        .unwrap()
        .insert(keys.last().unwrap().to_string(), new_value);
}

pub fn apply_mask<T>(obj: T, paths: Vec<String>) -> Result<T, serde_json::Error>
where
    T: DeserializeOwned + Serialize,
{
    let json_value = serde_json::to_value(obj)?;
    // Create a new JSON object with only the specified paths
    let mut new_json = json!({});

    for path in paths {
        let keys: Vec<&str> = path.split('.').collect();
        if let Some(value) = get_nested_value(&json_value, &keys) {
            set_nested_value(&mut new_json, &keys, value);
        }
    }
    serde_json::from_value::<T>(new_json)
}

/// Source-level projection of an `AnyChainBlock` from a `FieldMask`.
///
/// `AnyChainBlock` carries two representations of the same block: the raw
/// `native_bytes` and a fully parsed `chain` block. This mask records which of
/// them a producer should populate, so the parsed representation is only built
/// (and the raw bytes only copied) when actually requested.
///
/// Unlike [`apply_mask`], this does not touch a built response — the caller
/// consults the flags while constructing the message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockMask {
    pub native_bytes: bool,
    pub chain: bool,
}

impl BlockMask {
    /// A mask that selects every representation.
    pub const fn all() -> Self {
        Self {
            native_bytes: true,
            chain: true,
        }
    }

    /// Interprets a `FieldMask`'s paths against `AnyChainBlock`.
    ///
    /// An absent or empty mask (the caller passes `&[]`) selects everything, so
    /// existing clients that send no mask are unaffected. Paths are matched
    /// leniently: a leading `block.` segment (referring to the repeated `block`
    /// field in the response message) is tolerated, as is a bare leaf name.
    /// Recognized leaves are `native_bytes` and the parsed chain
    /// (`cardano`/`chain`). Selecting the whole `block` field keeps both.
    pub fn from_paths(paths: &[String]) -> Self {
        if paths.is_empty() {
            return Self::all();
        }

        let mut mask = Self {
            native_bytes: false,
            chain: false,
        };

        for path in paths {
            // Tolerate the response-relative `block.` prefix.
            let field = path.strip_prefix("block.").unwrap_or(path);
            // Only the first segment matters for deciding what to populate.
            match field.split('.').next().unwrap_or("") {
                "native_bytes" => mask.native_bytes = true,
                "cardano" | "chain" => mask.chain = true,
                // The whole block (or an unrecognized empty path) keeps both.
                "" | "block" => return Self::all(),
                _ => {}
            }
        }

        mask
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    #[test]
    fn block_mask_from_paths_semantics() {
        // Absent/empty mask keeps everything.
        assert_eq!(BlockMask::from_paths(&[]), BlockMask::all());

        // Bare leaf names.
        let bytes_only = BlockMask::from_paths(&["native_bytes".to_string()]);
        assert!(bytes_only.native_bytes && !bytes_only.chain);

        let chain_only = BlockMask::from_paths(&["cardano".to_string()]);
        assert!(!chain_only.native_bytes && chain_only.chain);

        // Response-relative `block.` prefix is tolerated.
        let prefixed = BlockMask::from_paths(&["block.native_bytes".to_string()]);
        assert!(prefixed.native_bytes && !prefixed.chain);

        // Deeper paths into the parsed block select the chain representation.
        let deep = BlockMask::from_paths(&["block.cardano.header".to_string()]);
        assert!(!deep.native_bytes && deep.chain);

        // Selecting the whole block keeps both.
        assert_eq!(
            BlockMask::from_paths(&["block".to_string()]),
            BlockMask::all()
        );

        // Multiple paths accumulate.
        let both = BlockMask::from_paths(&["native_bytes".to_string(), "cardano".to_string()]);
        assert!(both.native_bytes && both.chain);
    }

    #[derive(Deserialize, Serialize, Default, Debug, PartialEq, Clone)]
    #[serde(default)]
    struct Bar {
        pub abc: usize,
    }

    #[derive(Deserialize, Serialize, Default, Debug, Clone)]
    #[serde(default)]
    struct Foo {
        pub foo: String,
        pub bar: Bar,
    }

    #[test]
    fn test_apply_mask() {
        let foobar = Foo {
            foo: "foo".into(),
            bar: Bar { abc: 10 },
        };
        let paths = vec!["foo".to_string()];
        let masked_foobar = apply_mask(foobar.clone(), paths).expect("Failed to serde");
        assert_eq!(masked_foobar.foo, "foo");
        assert_eq!(masked_foobar.bar, Bar::default());

        let paths = vec!["bar".to_string()];
        let masked_foobar = apply_mask(foobar, paths).expect("Failed to serde");
        assert_eq!(masked_foobar.foo, String::default());
        assert_eq!(masked_foobar.bar.abc, 10);
    }
}
