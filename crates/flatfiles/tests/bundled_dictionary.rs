//! The bundled dictionary is the one its provenance describes and fits the
//! asset budget.

use dolos_flatfiles::compressed::bundled_dictionary;

const PROVENANCE: &str = include_str!("../dictionary/cardano.dict.json");

/// Pull a string field out of the provenance without a JSON dependency.
fn field(name: &str) -> &'static str {
    let key = format!("\"{name}\": \"");
    let start = PROVENANCE.find(&key).expect("field present") + key.len();
    let end = PROVENANCE[start..].find('"').unwrap() + start;
    &PROVENANCE[start..end]
}

#[test]
fn bundled_dictionary_matches_its_provenance() {
    let dictionary = bundled_dictionary();
    assert_eq!(dictionary.id().to_string(), field("sha256"));
    assert!(dictionary.bytes().len() <= 128 * 1024, "asset over 128 KiB");
    assert_ne!(dictionary.zstd_id(), 0, "not a trained zstd dictionary");
    let bytes: usize = PROVENANCE
        .split("\"bytes\": ")
        .nth(1)
        .and_then(|s| s.split(|c: char| !c.is_ascii_digit()).next())
        .and_then(|s| s.parse().ok())
        .expect("bytes field");
    assert_eq!(dictionary.bytes().len(), bytes);
}

#[test]
fn bundled_dictionary_round_trips_a_frame() {
    let dictionary = bundled_dictionary();
    let body = b"a block body that is compressed with the bundled dictionary".repeat(20);
    let mut compressor = zstd::bulk::Compressor::with_dictionary(3, dictionary.bytes()).unwrap();
    let frame = compressor.compress(&body).unwrap();
    let mut decompressor = zstd::bulk::Decompressor::with_dictionary(dictionary.bytes()).unwrap();
    let back = decompressor.decompress(&frame, body.len()).unwrap();
    assert_eq!(back, body);
}
