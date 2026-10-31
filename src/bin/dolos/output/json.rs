//! JSON for any `Serialize` record, map keys included.
//!
//! `serde_json` takes only strings, numbers and unit variants as map keys.
//! The ledger keys maps by credentials and DReps, enums with newtype
//! variants, so here a key that is not a string is written as one: a newtype
//! variant over a string or number as `Variant:value`
//! (`AddrKeyhash:9a1c…`), anything else as its compact JSON text. An
//! integer beyond 64 bits, which a JSON value cannot hold, is written as its
//! decimal string. Everything else is what `serde_json::to_value` makes.

use serde::ser::{self, Serialize};
use serde_json::{Error, Map, Value};

pub fn to_value<T: Serialize + ?Sized>(value: &T) -> Result<Value, Error> {
    value.serialize(Serializer)
}

/// A map key as the string JSON needs.
fn key_string(key: Value) -> String {
    match key {
        Value::String(key) => key,
        Value::Object(ref entries) if entries.len() == 1 => {
            let (tag, value) = entries.iter().next().expect("one entry");

            match value {
                Value::String(value) => format!("{tag}:{value}"),
                Value::Number(value) => format!("{tag}:{value}"),
                _ => key.to_string(),
            }
        }
        key => key.to_string(),
    }
}

struct Serializer;

pub struct Seq {
    variant: Option<&'static str>,
    items: Vec<Value>,
}

pub struct Entries {
    variant: Option<&'static str>,
    entries: Map<String, Value>,
    key: Option<String>,
}

/// `{variant: value}`, serde's external tagging, or `value` alone.
fn tagged(variant: Option<&'static str>, value: Value) -> Value {
    match variant {
        Some(variant) => Value::Object(Map::from_iter([(variant.to_owned(), value)])),
        None => value,
    }
}

impl ser::Serializer for Serializer {
    type Ok = Value;
    type Error = Error;
    type SerializeSeq = Seq;
    type SerializeTuple = Seq;
    type SerializeTupleStruct = Seq;
    type SerializeTupleVariant = Seq;
    type SerializeMap = Entries;
    type SerializeStruct = Entries;
    type SerializeStructVariant = Entries;

    fn serialize_bool(self, v: bool) -> Result<Value, Error> {
        serde_json::value::Serializer.serialize_bool(v)
    }

    fn serialize_i8(self, v: i8) -> Result<Value, Error> {
        serde_json::value::Serializer.serialize_i8(v)
    }

    fn serialize_i16(self, v: i16) -> Result<Value, Error> {
        serde_json::value::Serializer.serialize_i16(v)
    }

    fn serialize_i32(self, v: i32) -> Result<Value, Error> {
        serde_json::value::Serializer.serialize_i32(v)
    }

    fn serialize_i64(self, v: i64) -> Result<Value, Error> {
        serde_json::value::Serializer.serialize_i64(v)
    }

    fn serialize_i128(self, v: i128) -> Result<Value, Error> {
        match i64::try_from(v) {
            Ok(v) => Ok(Value::from(v)),
            Err(_) => Ok(Value::String(v.to_string())),
        }
    }

    fn serialize_u8(self, v: u8) -> Result<Value, Error> {
        serde_json::value::Serializer.serialize_u8(v)
    }

    fn serialize_u16(self, v: u16) -> Result<Value, Error> {
        serde_json::value::Serializer.serialize_u16(v)
    }

    fn serialize_u32(self, v: u32) -> Result<Value, Error> {
        serde_json::value::Serializer.serialize_u32(v)
    }

    fn serialize_u64(self, v: u64) -> Result<Value, Error> {
        serde_json::value::Serializer.serialize_u64(v)
    }

    fn serialize_u128(self, v: u128) -> Result<Value, Error> {
        match u64::try_from(v) {
            Ok(v) => Ok(Value::from(v)),
            Err(_) => Ok(Value::String(v.to_string())),
        }
    }

    fn serialize_f32(self, v: f32) -> Result<Value, Error> {
        serde_json::value::Serializer.serialize_f32(v)
    }

    fn serialize_f64(self, v: f64) -> Result<Value, Error> {
        serde_json::value::Serializer.serialize_f64(v)
    }

    fn serialize_char(self, v: char) -> Result<Value, Error> {
        serde_json::value::Serializer.serialize_char(v)
    }

    fn serialize_str(self, v: &str) -> Result<Value, Error> {
        serde_json::value::Serializer.serialize_str(v)
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Value, Error> {
        serde_json::value::Serializer.serialize_bytes(v)
    }

    fn serialize_none(self) -> Result<Value, Error> {
        Ok(Value::Null)
    }

    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<Value, Error> {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Value, Error> {
        Ok(Value::Null)
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Value, Error> {
        Ok(Value::Null)
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
    ) -> Result<Value, Error> {
        Ok(Value::String(variant.to_owned()))
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Value, Error> {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Value, Error> {
        Ok(tagged(Some(variant), to_value(value)?))
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Seq, Error> {
        Ok(Seq {
            variant: None,
            items: Vec::with_capacity(len.unwrap_or_default()),
        })
    }

    fn serialize_tuple(self, len: usize) -> Result<Seq, Error> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(self, _name: &'static str, len: usize) -> Result<Seq, Error> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Seq, Error> {
        Ok(Seq {
            variant: Some(variant),
            items: Vec::with_capacity(len),
        })
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Entries, Error> {
        Ok(Entries {
            variant: None,
            entries: Map::new(),
            key: None,
        })
    }

    fn serialize_struct(self, _name: &'static str, len: usize) -> Result<Entries, Error> {
        self.serialize_map(Some(len))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Entries, Error> {
        Ok(Entries {
            variant: Some(variant),
            entries: Map::new(),
            key: None,
        })
    }
}

impl Seq {
    fn push<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        self.items.push(to_value(value)?);
        Ok(())
    }

    fn finish(self) -> Result<Value, Error> {
        Ok(tagged(self.variant, Value::Array(self.items)))
    }
}

impl ser::SerializeSeq for Seq {
    type Ok = Value;
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        self.push(value)
    }

    fn end(self) -> Result<Value, Error> {
        self.finish()
    }
}

impl ser::SerializeTuple for Seq {
    type Ok = Value;
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        self.push(value)
    }

    fn end(self) -> Result<Value, Error> {
        self.finish()
    }
}

impl ser::SerializeTupleStruct for Seq {
    type Ok = Value;
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        self.push(value)
    }

    fn end(self) -> Result<Value, Error> {
        self.finish()
    }
}

impl ser::SerializeTupleVariant for Seq {
    type Ok = Value;
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        self.push(value)
    }

    fn end(self) -> Result<Value, Error> {
        self.finish()
    }
}

impl Entries {
    fn insert<T: Serialize + ?Sized>(&mut self, key: String, value: &T) -> Result<(), Error> {
        self.entries.insert(key, to_value(value)?);
        Ok(())
    }

    fn finish(self) -> Result<Value, Error> {
        Ok(tagged(self.variant, Value::Object(self.entries)))
    }
}

impl ser::SerializeMap for Entries {
    type Ok = Value;
    type Error = Error;

    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), Error> {
        self.key = Some(key_string(to_value(key)?));
        Ok(())
    }

    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        let key = self
            .key
            .take()
            .expect("serialize_value is called after serialize_key");

        self.insert(key, value)
    }

    fn end(self) -> Result<Value, Error> {
        self.finish()
    }
}

impl ser::SerializeStruct for Entries {
    type Ok = Value;
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Error> {
        self.insert(key.to_owned(), value)
    }

    fn end(self) -> Result<Value, Error> {
        self.finish()
    }
}

impl ser::SerializeStructVariant for Entries {
    type Ok = Value;
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Error> {
        self.insert(key.to_owned(), value)
    }

    fn end(self) -> Result<Value, Error> {
        self.finish()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use pallas::ledger::primitives::{conway::DRep, StakeCredential};
    use serde::Serialize;
    use serde_json::json;

    use super::to_value;

    /// 128-bit integers are numbers while 64 bits hold them, decimal strings
    /// beyond.
    #[test]
    fn wide_integers_beyond_64_bits_are_written_as_strings() {
        let wide = u128::from(u64::MAX) + 1;

        assert!(serde_json::to_value(wide).is_err(), "serde_json refuses it");

        assert_eq!(to_value(&wide).unwrap(), json!("18446744073709551616"));
        assert_eq!(to_value(&u128::from(u64::MAX)).unwrap(), json!(u64::MAX));
        assert_eq!(
            to_value(&(i128::from(i64::MIN) - 1)).unwrap(),
            json!("-9223372036854775809")
        );
        assert_eq!(to_value(&-1i128).unwrap(), json!(-1));
    }

    #[test]
    fn credential_keys_are_written_as_strings() {
        let key = [0xab; 28].into();

        let votes = BTreeMap::from([
            (StakeCredential::AddrKeyhash(key), 1u64),
            (StakeCredential::ScriptHash(key), 2),
        ]);
        let distr = BTreeMap::from([(DRep::Key(key), 3u64), (DRep::Abstain, 4)]);

        assert!(
            serde_json::to_value(&votes).is_err(),
            "serde_json refuses them"
        );

        let hash = "ab".repeat(28);

        assert_eq!(
            to_value(&votes).unwrap(),
            json!({
                format!("AddrKeyhash:{hash}"): 1,
                format!("ScriptHash:{hash}"): 2,
            })
        );
        assert_eq!(
            to_value(&distr).unwrap(),
            json!({ format!("Key:{hash}"): 3, "Abstain": 4 })
        );
    }

    #[test]
    fn everything_else_is_as_serde_json_writes_it() {
        #[derive(Serialize)]
        enum Shape {
            Unit,
            Newtype(u8),
            Tuple(u8, bool),
            Struct { a: Option<u8>, b: Vec<u8> },
        }

        #[derive(Serialize)]
        struct Record {
            shapes: Vec<Shape>,
            numbers: BTreeMap<u32, f64>,
            nothing: Option<()>,
            text: char,
            pair: (i64, String),
        }

        let record = Record {
            shapes: vec![
                Shape::Unit,
                Shape::Newtype(1),
                Shape::Tuple(2, true),
                Shape::Struct {
                    a: None,
                    b: vec![3],
                },
            ],
            numbers: BTreeMap::from([(7, 0.5), (8, f64::NAN)]),
            nothing: None,
            text: 'x',
            pair: (-1, "y".into()),
        };

        assert_eq!(
            to_value(&record).unwrap(),
            serde_json::to_value(&record).unwrap()
        );
    }
}
