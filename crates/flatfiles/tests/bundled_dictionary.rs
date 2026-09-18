//! The bundled dictionary is the one its provenance describes and fits the
//! asset budget.

use dolos_flatfiles::{BUNDLED_DICTIONARY, COMPRESSION_LEVEL};
use sha2::{Digest, Sha256};

const PROVENANCE: &str = include_str!("../dictionary/cardano.dict.json");

/// Pull a string field out of the provenance without a JSON dependency.
fn field(name: &str) -> &'static str {
    let key = format!("\"{name}\": \"");
    let start = PROVENANCE.find(&key).expect("field present") + key.len();
    let end = PROVENANCE[start..].find('"').unwrap() + start;
    &PROVENANCE[start..end]
}

/// Pull a number field out of the provenance the same way.
fn number(name: &str) -> usize {
    let key = format!("\"{name}\": ");
    let start = PROVENANCE.find(&key).expect("field present") + key.len();
    PROVENANCE[start..]
        .split(|c: char| !c.is_ascii_digit())
        .next()
        .and_then(|s| s.parse().ok())
        .expect("numeric field")
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[test]
fn bundled_dictionary_matches_its_provenance() {
    assert_eq!(hex(&Sha256::digest(BUNDLED_DICTIONARY)), field("sha256"));
    assert!(BUNDLED_DICTIONARY.len() <= 128 * 1024, "asset over 128 KiB");
    assert_eq!(
        zstd::zstd_safe::get_dict_id_from_dict(BUNDLED_DICTIONARY).map_or(0, |id| id.get()),
        number("zstd_id") as u32
    );
    assert_eq!(BUNDLED_DICTIONARY.len(), number("bytes"));
}

#[test]
fn bundled_dictionary_round_trips_a_frame() {
    let body = b"a block body that is compressed with the bundled dictionary".repeat(20);
    let mut compressor =
        zstd::bulk::Compressor::with_dictionary(COMPRESSION_LEVEL, BUNDLED_DICTIONARY).unwrap();
    let frame = compressor.compress(&body).unwrap();
    let mut decompressor = zstd::bulk::Decompressor::with_dictionary(BUNDLED_DICTIONARY).unwrap();
    let back = decompressor.decompress(&frame, body.len()).unwrap();
    assert_eq!(back, body);
}
