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

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

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
