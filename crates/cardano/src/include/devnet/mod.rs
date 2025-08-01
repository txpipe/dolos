use dolos_core::Genesis;
use std::path::Path;

pub const BYRON: &[u8] = include_bytes!("byron.json");
pub const SHELLEY: &[u8] = include_bytes!("shelley.json");
pub const ALONZO: &[u8] = include_bytes!("alonzo.json");
pub const CONWAY: &[u8] = include_bytes!("conway.json");

pub fn load() -> Genesis {
    Genesis {
        alonzo: serde_json::from_slice(ALONZO).unwrap(),
        conway: serde_json::from_slice(CONWAY).unwrap(),
        byron: serde_json::from_slice(BYRON).unwrap(),
        shelley: serde_json::from_slice(SHELLEY).unwrap(),
        force_protocol: Some(9),
    }
}

pub fn save(root: &Path) -> std::io::Result<()> {
    std::fs::write(root.join("byron.json"), BYRON)?;
    std::fs::write(root.join("shelley.json"), SHELLEY)?;
    std::fs::write(root.join("alonzo.json"), ALONZO)?;
    std::fs::write(root.join("conway.json"), CONWAY)?;

    Ok(())
}
