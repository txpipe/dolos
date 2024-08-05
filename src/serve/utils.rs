use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};

// Helper function to get a nested value from a serde_json::Value
pub fn get_nested_value(value: &Value, keys: &[&str]) -> Option<Value> {
    let mut current = value;
    for key in keys {
        match current {
            Value::Object(map) => current = map.get(*key)?,
            _ => return None,
        }
    }
    Some(current.clone())
}

// Helper function to set a nested value in a serde_json::Value
pub fn set_nested_value(value: &mut Value, keys: &[&str], new_value: Value) {
    let mut current = value;
    for key in keys.iter().take(keys.len() - 1) {
        current = current
            .as_object_mut()
            .unwrap()
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
    use super::*;

    #[test]
    fn test_apply_mask() {
        let pparams = pallas::interop::utxorpc::spec::cardano::PParams {
            coins_per_utxo_byte: 10,
            max_tx_size: 10,
            ..Default::default()
        };
        let paths = vec!["coinsPerUtxoByte".to_string()];
        let new_pparams = apply_mask(pparams, paths).expect("Failed to serde");
        assert_eq!(new_pparams.coins_per_utxo_byte, 10);
        assert_eq!(new_pparams.max_tx_size, 0);
    }
}
