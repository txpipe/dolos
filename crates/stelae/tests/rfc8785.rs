//! RFC 8785 (JSON Canonicalization Scheme) conformance.
//!
//! The inscription digest is the identity of a stele: independent publishers
//! reproduce it, signatures cover it, and `history` chains it. All of that
//! rests on canonicalization being *the same function* everywhere — so the JCS
//! implementation is not an implementation detail to be assumed correct, it is
//! a conformance surface with an official test suite. This file runs it.
//!
//! Everything here goes through `stelae::canonical_json`, the same entry point
//! `Inscription::canonicalize` uses, rather than the underlying crate directly:
//! a passing vendored dependency proves nothing if the protocol reaches it by a
//! different path.
//!
//! Vector provenance is in `tests/data/rfc8785/README.md`.

use std::path::PathBuf;

fn data_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/data/rfc8785")
}

const VECTORS: &[&str] = &[
    "arrays",
    "french",
    "structures",
    "unicode",
    "values",
    "weird",
];

/// The six official vectors, compared byte for byte.
#[test]
fn official_vectors() {
    for name in VECTORS {
        let input = std::fs::read(data_dir().join(format!("input/{name}.json")))
            .unwrap_or_else(|e| panic!("reading input/{name}.json: {e}"));
        let expected = std::fs::read(data_dir().join(format!("output/{name}.json")))
            .unwrap_or_else(|e| panic!("reading output/{name}.json: {e}"));

        let value: serde_json::Value = serde_json::from_slice(&input).unwrap();
        let canonical = stelae::canonical_json(&value).unwrap();

        assert_eq!(
            canonical,
            expected,
            "vector {name}\n  got:      {}\n  expected: {}",
            String::from_utf8_lossy(&canonical),
            String::from_utf8_lossy(&expected),
        );
    }
}

/// Canonicalization is idempotent: feeding the canonical form back in returns
/// it unchanged. A publisher that re-canonicalizes a document it received must
/// land on the same bytes, or `history` verification breaks.
#[test]
fn canonicalization_is_idempotent() {
    for name in VECTORS {
        let expected = std::fs::read(data_dir().join(format!("output/{name}.json"))).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&expected).unwrap();

        assert_eq!(
            stelae::canonical_json(&value).unwrap(),
            expected,
            "vector {name} is not a fixed point"
        );
    }
}

/// RFC 8785 Appendix B, "Number Serialization Samples", verbatim: the IEEE 754
/// bit pattern and the JSON text ECMAScript renders for it.
///
/// The two NaN/Infinity rows of the table are omitted — they are not
/// representable in JSON, and `serde_json` has no way to hold them.
const APPENDIX_B: &[(u64, &str)] = &[
    (0x0000000000000000, "0"),
    (0x8000000000000000, "0"),
    (0x0000000000000001, "5e-324"),
    (0x8000000000000001, "-5e-324"),
    (0x7fefffffffffffff, "1.7976931348623157e+308"),
    (0xffefffffffffffff, "-1.7976931348623157e+308"),
    (0x4340000000000000, "9007199254740992"),
    (0xc340000000000000, "-9007199254740992"),
    (0x4430000000000000, "295147905179352830000"),
    (0x44b52d02c7e14af5, "9.999999999999997e+22"),
    (0x44b52d02c7e14af6, "1e+23"),
    (0x44b52d02c7e14af7, "1.0000000000000001e+23"),
    (0x444b1ae4d6e2ef4e, "999999999999999700000"),
    (0x444b1ae4d6e2ef4f, "999999999999999900000"),
    (0x444b1ae4d6e2ef50, "1e+21"),
    (0x3eb0c6f7a0b5ed8c, "9.999999999999997e-7"),
    (0x3eb0c6f7a0b5ed8d, "0.000001"),
    (0x41b3de4355555553, "333333333.3333332"),
    (0x41b3de4355555554, "333333333.33333325"),
    (0x41b3de4355555555, "333333333.3333333"),
    (0x41b3de4355555556, "333333333.3333334"),
    (0x41b3de4355555557, "333333333.33333343"),
    (0xbecbf647612f3696, "-0.0000033333333333333333"),
    (0x43143ff3c1cb0959, "1424953923781206.2"),
];

/// Number rendering is where two "conformant" implementations most plausibly
/// disagree, because it is ECMAScript's algorithm rather than anything JSON
/// specifies. Appendix B is the arbiter.
#[test]
fn appendix_b_number_serialization() {
    for (bits, expected) in APPENDIX_B {
        let value = serde_json::Number::from_f64(f64::from_bits(*bits))
            .map(serde_json::Value::Number)
            .unwrap_or_else(|| panic!("{bits:#018x} is not representable in JSON"));

        let canonical = String::from_utf8(stelae::canonical_json(&value).unwrap()).unwrap();

        assert_eq!(
            canonical, *expected,
            "IEEE 754 {bits:#018x} rendered as {canonical}, RFC 8785 says {expected}"
        );
    }
}

/// How integers render is the half of the question the inscription actually
/// depends on: every number in an inscription is an integer, and it must come
/// out as plain digits with no exponent and no decimal point.
#[test]
fn integers_render_as_plain_digits() {
    let cases: &[(i64, &str)] = &[
        (0, "0"),
        (1, "1"),
        (550, "550"),
        (-1, "-1"),
        (21600, "21600"),
        (43_210_000, "43210000"),
        (402_653_184, "402653184"),
        // An epoch's worth of mainnet blocks, and the largest value the
        // inscription rule admits.
        (40_000_000_000, "40000000000"),
        (stelae::MAX_SAFE_INTEGER, "9007199254740991"),
        (-stelae::MAX_SAFE_INTEGER, "-9007199254740991"),
    ];

    for (value, expected) in cases {
        let canonical =
            String::from_utf8(stelae::canonical_json(&serde_json::json!(value)).unwrap()).unwrap();
        assert_eq!(canonical, *expected, "integer {value}");
    }
}

/// Past 2^53 - 1 the JCS crate keeps working and starts lying: a `u64` renders
/// as the nearest double, silently. This test pins that behaviour so the
/// protocol's refusal to canonicalize such values is understood as load-bearing
/// rather than as belt-and-braces — see `inscription::check_safe_numbers`.
#[test]
fn beyond_the_safe_range_rendering_is_lossy() {
    let unsafe_value = serde_json::json!(u64::MAX);
    let canonical = String::from_utf8(stelae::canonical_json(&unsafe_value).unwrap()).unwrap();

    assert_eq!(canonical, "18446744073709552000");
    assert_ne!(canonical, u64::MAX.to_string());

    // Which is exactly why the inscription refuses it before it can reach the
    // canonicalizer.
    let err = stelae::inscription::check_safe_numbers(&unsafe_value).unwrap_err();
    assert!(
        matches!(err, stelae::Error::UnsafeInteger { .. }),
        "expected a refusal, got {err:?}"
    );

    // The boundary itself is fine on both sides of zero.
    for ok in [
        serde_json::json!(stelae::MAX_SAFE_INTEGER),
        serde_json::json!(-stelae::MAX_SAFE_INTEGER),
        serde_json::json!(0),
    ] {
        stelae::inscription::check_safe_numbers(&ok).unwrap();
    }
}
