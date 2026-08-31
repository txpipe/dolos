//! The inscription: a stele's canonical, signable document.
//!
//! The inscription is the OCI config blob of a stele and the anchor of the
//! whole protocol. Its sha256, taken over its RFC 8785 canonical JSON encoding,
//! is the stele's identity: what independent publishers reproduce, what
//! signatures cover, and what `history` chains together so the newest signed
//! inscription transitively attests every earlier one.
//!
//! Three of its fields — `position`, `parameters` and each layer's `scope` —
//! are the profile's, and this module never looks inside them. They are held as
//! [`serde_json::Value`], canonicalized like every other key, and hashed. That
//! is what lets determinism hold without the protocol knowing a profile's
//! vocabulary.
//!
//! ## Two rules that make the digest reproducible
//!
//! **Every number is an integer within ±(2^53 − 1).** RFC 8785 serializes
//! numbers per ECMAScript, so past that magnitude a `u64` renders as a rounded
//! double and two implementations diverge *silently*. The protocol refuses the
//! value instead ([`check_safe_numbers`]). Nothing in the schema needs the
//! range: an `uncompressedSize` for a mainnet epoch is around 4×10^10.
//!
//! **An invalid inscription has no digest.** [`Inscription::canonicalize`]
//! validates first, so it is not possible to obtain canonical bytes — and
//! therefore an identity — for a document that violates the schema, the naming
//! rules or the history invariant.

use serde::{Deserialize, Serialize};

use crate::{
    profile::{checked_layer_media_type, validate_profile_name, MediaType, Profile},
    Digest, Error, MAX_SAFE_INTEGER, SCHEMA_VERSION,
};

/// The profile a stele belongs to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ProfileRef {
    /// Reverse-DNS, vendor-owned, e.g. `io.txpipe.dolos.cardano`.
    pub name: String,
    /// Profile major version. A client refuses a value above the one it
    /// implements.
    pub version: u64,
}

/// Compression parameters. Transport policy, pinned so that blobs dedupe across
/// publishers in practice; identity never depends on it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Compression {
    pub algo: String,
    pub level: i64,
}

/// One prior publication, as attested by this inscription.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct HistoryEntry {
    pub sequence: u64,
    pub inscription_digest: Digest,
}

/// One layer of a stele, as the inscription describes it.
///
/// Note what is absent: the compressed digest and the compressed size. Those
/// are transport facts and live in the OCI manifest. The inscription carries
/// only what is identity — the uncompressed digest — so it stays reproducible
/// by a party that compressed differently, or not at all.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct LayerDescriptor {
    /// Profile-defined layer kind.
    pub kind: String,
    /// Profile-owned payload media type.
    pub media_type: String,
    /// sha256 over the uncompressed CBOR sequence — the layer's identity.
    pub diff_id: Digest,
    /// Number of records, header record included.
    pub records: u64,
    /// Uncompressed byte length. Summed across planned layers, this is the
    /// restore-time disk preflight.
    pub uncompressed_size: u64,
    /// Profile-owned, opaque to the protocol.
    pub scope: serde_json::Value,
}

/// The canonical, signable document of a stele.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Inscription {
    /// Inscription schema version. Verifiers fail closed on anything but
    /// [`SCHEMA_VERSION`].
    pub schema: u64,
    pub profile: ProfileRef,
    /// The protocol's monotonic ordering key. A profile decides what it counts
    /// (the Dolos profile sets it to the Cardano epoch).
    pub sequence: u64,
    /// Profile-owned, opaque: where in its source domain this stele stands.
    pub position: serde_json::Value,
    /// Profile-owned, opaque: the parameters a reader needs to interpret the
    /// layers.
    pub parameters: serde_json::Value,
    pub compression: Compression,
    /// Every prior publication, contiguous and ascending, ending at
    /// `sequence - 1`.
    pub history: Vec<HistoryEntry>,
    pub layers: Vec<LayerDescriptor>,
}

impl Inscription {
    /// An inscription at the current schema version, with no history and no
    /// layers yet.
    pub fn new(
        profile: &dyn Profile,
        sequence: u64,
        position: serde_json::Value,
        parameters: serde_json::Value,
        compression: Compression,
    ) -> Self {
        Self {
            schema: SCHEMA_VERSION,
            profile: ProfileRef {
                name: profile.name().to_owned(),
                version: profile.version(),
            },
            sequence,
            position,
            parameters,
            compression,
            history: Vec::new(),
            layers: Vec::new(),
        }
    }

    /// The RFC 8785 canonical JSON encoding of this inscription.
    ///
    /// Validates first: canonical bytes, and therefore an identity, exist only
    /// for a well-formed inscription.
    pub fn canonicalize(&self) -> Result<Vec<u8>, Error> {
        let value = serde_json::to_value(self)?;
        self.validate_structure()?;
        check_safe_numbers(&value)?;
        canonical_json(&value)
    }

    /// The stele's identity: sha256 of [`Inscription::canonicalize`].
    pub fn digest(&self) -> Result<Digest, Error> {
        Ok(Digest::compute(self.canonicalize()?))
    }

    /// Parse untrusted inscription bytes, fail-closed.
    ///
    /// Rejects an unknown generic key anywhere in the document (extension
    /// happens only inside `position`, `parameters` and `scope`), a schema
    /// version this implementation does not implement, a number outside the
    /// JCS-safe integer range, a malformed profile name or payload media type,
    /// and any violation of the history invariant.
    ///
    /// It deliberately does *not* check which profile the inscription belongs
    /// to — that needs the profile implementation, and is
    /// [`Inscription::check_profile`].
    ///
    /// Nor does it reject a document with a repeated JSON key, which JSON
    /// itself leaves undefined and `serde_json` resolves last-wins. That is not
    /// a hole in practice: canonicalizing the result yields different bytes
    /// from the input, so any check that compares an inscription against its
    /// digest — which is every use of one — refuses it. `SteleDir` makes the
    /// comparison explicit.
    pub fn parse(bytes: &[u8]) -> Result<Self, Error> {
        let value: serde_json::Value = serde_json::from_slice(bytes)?;

        // Checked on the raw document, so that numbers inside the profile's
        // opaque objects are covered too.
        check_safe_numbers(&value)?;

        let inscription: Self = serde_json::from_value(value)?;
        inscription.validate_structure()?;

        Ok(inscription)
    }

    /// Full validation of a locally built inscription.
    pub fn validate(&self) -> Result<(), Error> {
        let value = serde_json::to_value(self)?;
        self.validate_structure()?;
        check_safe_numbers(&value)
    }

    /// Refuse an inscription this profile implementation cannot read.
    ///
    /// Two ways it fails, both clean refusals rather than a partial restore:
    /// the inscription belongs to a different profile, or it needs a profile
    /// major version above the one implemented here. A layer of a kind this
    /// implementation does not define is *not* one of them — see
    /// [`Inscription::unknown_layers`] for why, and
    /// [`Inscription::check_profile_strict`] for the side that still refuses
    /// it.
    pub fn check_profile(&self, profile: &dyn Profile) -> Result<(), Error> {
        if self.profile.name != profile.name() {
            return Err(Error::UnknownProfile {
                found: self.profile.name.clone(),
                expected: profile.name().to_owned(),
            });
        }

        if self.profile.version > profile.version() {
            return Err(Error::UnsupportedProfileVersion {
                name: self.profile.name.clone(),
                found: self.profile.version,
                supported: profile.version(),
            });
        }

        let kinds = profile.kinds();

        for layer in &self.layers {
            // Left to `unknown_layers`. Its media type is still checked:
            // `validate_structure` has already established that it parses and
            // does not squat the reserved `stelae` vendor.
            if !kinds.contains(&layer.kind.as_str()) {
                continue;
            }

            // Pins the profile's own answer to the naming rules before it is
            // used as the yardstick.
            let defined = checked_layer_media_type(profile, &layer.kind)?;
            let expected = MediaType::parse(&defined)?;
            let found = MediaType::parse(&layer.media_type)?;

            // Compared on vendor and kind, not on the whole string. Those two
            // are what make a descriptor unambiguously this profile's layer;
            // the version and codec are transport detail a profile may move
            // within one major, and freezing them here would refuse an
            // inscription this implementation can otherwise read.
            //
            // `validate_structure` already established that `layer.media_type`
            // parses and does not squat the reserved vendor. What it cannot
            // know, without a profile in hand, is whether the name belongs to
            // the profile the inscription claims — that is this check.
            if found.vendor != expected.vendor || found.kind != expected.kind {
                return Err(Error::InvalidMediaType {
                    value: layer.media_type.clone(),
                    reason: format!(
                        "profile {:?} names layer kind {:?} {defined:?}",
                        profile.name(),
                        layer.kind,
                    ),
                });
            }
        }

        Ok(())
    }

    /// The layers whose kind `profile` does not define, in inscription order.
    ///
    /// The client half of an additive change. A profile that gains a kind
    /// publishes it as a new media type on a new layer, and an older reader
    /// cannot store what it does not model — but refusing the whole stele over
    /// it would make every additive change break every deployed reader. So the
    /// protocol reports the layers rather than refusing them, and the profile
    /// decides during planning what skipping each one costs.
    ///
    /// **Nothing here reads a scope.** Whether a layer is one a reader may skip
    /// is a profile question, answered from the profile-owned `scope` the
    /// descriptor carries, and the protocol interprets no field of it.
    pub fn unknown_layers<'a>(&'a self, profile: &dyn Profile) -> Vec<&'a LayerDescriptor> {
        let kinds = profile.kinds();

        self.layers
            .iter()
            .filter(|layer| !kinds.contains(&layer.kind.as_str()))
            .collect()
    }

    /// [`Inscription::check_profile`], plus a refusal of any unknown kind.
    ///
    /// What a publisher checks, and the asymmetry is the point: a reader
    /// consumes the layers it understands, while a publisher *attests* every
    /// layer it lists — its inscription is the document independent parties
    /// reproduce and, later, sign. Chaining onto a stele carrying a kind this
    /// binary cannot build means either dropping that layer from the new stele
    /// or attesting bytes it never read, and both are worse than stopping.
    pub fn check_profile_strict(&self, profile: &dyn Profile) -> Result<(), Error> {
        self.check_profile(profile)?;

        if let Some(layer) = self.unknown_layers(profile).first() {
            return Err(Error::UnknownLayerKind {
                profile: profile.name().to_owned(),
                kind: layer.kind.clone(),
            });
        }

        Ok(())
    }

    /// Layers of a given kind, in inscription order.
    pub fn layers_of_kind<'a>(
        &'a self,
        kind: &'a str,
    ) -> impl Iterator<Item = &'a LayerDescriptor> + 'a {
        self.layers.iter().filter(move |l| l.kind == kind)
    }

    /// Total uncompressed bytes across every layer — the restore-time disk
    /// preflight, in the one place that knows the sizes.
    pub fn uncompressed_size(&self) -> u64 {
        self.layers.iter().map(|l| l.uncompressed_size).sum()
    }

    fn validate_structure(&self) -> Result<(), Error> {
        if self.schema != SCHEMA_VERSION {
            return Err(Error::UnsupportedSchema { found: self.schema });
        }

        validate_profile_name(&self.profile.name)?;

        if self.profile.version == 0 {
            return Err(Error::InvalidProfileName {
                value: self.profile.name.clone(),
                reason: "profile major version must be at least 1".to_owned(),
            });
        }

        if self.compression.algo.is_empty() {
            return Err(Error::Canonicalization(
                "compression.algo must not be empty".to_owned(),
            ));
        }

        for layer in &self.layers {
            if layer.kind.is_empty() {
                return Err(Error::Canonicalization(
                    "a layer descriptor has an empty kind".to_owned(),
                ));
            }

            // Enforces the vendor-namespacing rules on whatever the publisher
            // wrote, including that `vnd.stelae.*` is never a payload type.
            MediaType::parse(&layer.media_type)?;
        }

        self.validate_history()
    }

    /// `history` holds exactly one entry per published sequence, strictly
    /// ascending, contiguous, ending at `sequence - 1`.
    ///
    /// The protocol cannot know where a repository's history *starts* — the
    /// first published sequence is a deployment parameter pinned alongside the
    /// repository — so the invariant checked here is the part that is
    /// universal: no gaps, no duplicates, no reordering, and nothing
    /// missing between the last entry and this stele.
    fn validate_history(&self) -> Result<(), Error> {
        for (index, entry) in self.history.iter().enumerate() {
            if entry.sequence >= self.sequence {
                return Err(Error::HistoryInvariant(format!(
                    "entry {index} has sequence {} which is not below this stele's sequence {}",
                    entry.sequence, self.sequence
                )));
            }

            if index == 0 {
                continue;
            }

            let previous = self.history[index - 1].sequence;

            match entry.sequence.cmp(&previous) {
                std::cmp::Ordering::Less => {
                    return Err(Error::HistoryInvariant(format!(
                        "entry {index} has sequence {} after sequence {previous}: \
                         history must be strictly ascending",
                        entry.sequence
                    )))
                }
                std::cmp::Ordering::Equal => {
                    return Err(Error::HistoryInvariant(format!(
                        "sequence {} appears more than once",
                        entry.sequence
                    )))
                }
                std::cmp::Ordering::Greater if entry.sequence != previous + 1 => {
                    return Err(Error::HistoryInvariant(format!(
                        "gap between sequence {previous} and sequence {}",
                        entry.sequence
                    )))
                }
                std::cmp::Ordering::Greater => {}
            }
        }

        if let Some(last) = self.history.last() {
            if last.sequence + 1 != self.sequence {
                return Err(Error::HistoryInvariant(format!(
                    "history ends at sequence {} but this stele is sequence {}",
                    last.sequence, self.sequence
                )));
            }
        }

        Ok(())
    }
}

/// RFC 8785 canonical JSON encoding of an arbitrary value.
///
/// Exposed so the conformance vectors exercise the same code path the
/// inscription digest depends on, rather than a parallel one.
pub fn canonical_json(value: &serde_json::Value) -> Result<Vec<u8>, Error> {
    serde_jcs::to_vec(value).map_err(|e| Error::Canonicalization(e.to_string()))
}

/// Assert that every number in `value` is an integer RFC 8785 renders exactly.
///
/// Walks into arrays and objects, so a profile's opaque `position`,
/// `parameters` and `scope` are held to the same rule as the generic keys. See
/// the module documentation for why the range matters.
pub fn check_safe_numbers(value: &serde_json::Value) -> Result<(), Error> {
    check_numbers_at(value, "$")
}

fn check_numbers_at(value: &serde_json::Value, path: &str) -> Result<(), Error> {
    match value {
        serde_json::Value::Number(number) => {
            if let Some(unsigned) = number.as_u64() {
                if unsigned > MAX_SAFE_INTEGER as u64 {
                    return Err(Error::UnsafeInteger {
                        path: path.to_owned(),
                        value: number.to_string(),
                    });
                }
                Ok(())
            } else if let Some(signed) = number.as_i64() {
                if signed < -MAX_SAFE_INTEGER {
                    return Err(Error::UnsafeInteger {
                        path: path.to_owned(),
                        value: number.to_string(),
                    });
                }
                Ok(())
            } else {
                Err(Error::NonIntegerNumber {
                    path: path.to_owned(),
                    value: number.to_string(),
                })
            }
        }
        serde_json::Value::Array(items) => items
            .iter()
            .enumerate()
            .try_for_each(|(index, item)| check_numbers_at(item, &format!("{path}[{index}]"))),
        serde_json::Value::Object(entries) => entries
            .iter()
            .try_for_each(|(key, item)| check_numbers_at(item, &format!("{path}.{key}"))),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct Toy;

    impl Profile for Toy {
        fn name(&self) -> &str {
            "dev.example.toy"
        }

        fn version(&self) -> u64 {
            1
        }

        fn kinds(&self) -> &[&str] {
            &["notes"]
        }

        fn layer_media_type(&self, kind: &str) -> Result<String, Error> {
            Ok(format!("application/vnd.example.stele.{kind}.v1+zstd"))
        }

        fn tag_for_sequence(&self, sequence: u64) -> Result<String, Error> {
            Ok(format!("note-{sequence}"))
        }
    }

    fn digest_of(byte: u8) -> Digest {
        Digest::from_bytes([byte; 32])
    }

    fn sample() -> Inscription {
        let mut inscription = Inscription::new(
            &Toy,
            3,
            json!({"chapter": 3, "label": "third"}),
            json!({"noteWidth": 40}),
            Compression {
                algo: "zstd".to_owned(),
                level: 9,
            },
        );

        inscription.history = vec![
            HistoryEntry {
                sequence: 1,
                inscription_digest: digest_of(1),
            },
            HistoryEntry {
                sequence: 2,
                inscription_digest: digest_of(2),
            },
        ];

        inscription.layers = vec![LayerDescriptor {
            kind: "notes".to_owned(),
            media_type: "application/vnd.example.stele.notes.v1+zstd".to_owned(),
            diff_id: digest_of(3),
            records: 5,
            uncompressed_size: 128,
            scope: json!({"chapter": 3}),
        }];

        inscription
    }

    #[test]
    fn canonical_form_is_stable_and_parses_back() {
        let inscription = sample();
        let canonical = inscription.canonicalize().unwrap();

        // Canonical JSON is sorted, minimal and free of insignificant
        // whitespace, so the bytes are the same on any run.
        assert_eq!(canonical, inscription.canonicalize().unwrap());
        let text = String::from_utf8(canonical.clone()).unwrap();
        assert!(text.starts_with(r#"{"compression":{"algo":"zstd","level":9},"history":"#));
        assert!(!text.contains(' '));

        let parsed = Inscription::parse(&canonical).unwrap();
        assert_eq!(parsed, inscription);
        assert_eq!(parsed.digest().unwrap(), inscription.digest().unwrap());
    }

    #[test]
    fn digest_is_sha256_of_the_canonical_bytes() {
        let inscription = sample();
        assert_eq!(
            inscription.digest().unwrap(),
            Digest::compute(inscription.canonicalize().unwrap())
        );
    }

    /// Key order in the source document must not reach the digest — that is the
    /// whole point of canonicalization.
    #[test]
    fn digest_ignores_source_key_order_and_whitespace() {
        let inscription = sample();
        let canonical = inscription.canonicalize().unwrap();

        let pretty = serde_json::to_vec_pretty(&inscription).unwrap();
        let reparsed = Inscription::parse(&pretty).unwrap();

        assert_ne!(pretty, canonical);
        assert_eq!(reparsed.canonicalize().unwrap(), canonical);
    }

    #[test]
    fn rejects_an_unknown_generic_key() {
        let inscription = sample();
        let mut value = serde_json::to_value(&inscription).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("extra".to_owned(), json!(1));

        let err = Inscription::parse(&serde_json::to_vec(&value).unwrap()).unwrap_err();
        assert!(
            matches!(&err, Error::Json(e) if e.to_string().contains("unknown field")),
            "{err:?}"
        );
    }

    /// Unknown keys are refused inside the generic *nested* objects too;
    /// extension happens only inside the three opaque ones.
    #[test]
    fn rejects_an_unknown_key_in_a_nested_generic_object() {
        for pointer in ["/profile", "/compression", "/layers/0", "/history/0"] {
            let inscription = sample();
            let mut value = serde_json::to_value(&inscription).unwrap();
            value
                .pointer_mut(pointer)
                .unwrap()
                .as_object_mut()
                .unwrap()
                .insert("extra".to_owned(), json!(1));

            let err = Inscription::parse(&serde_json::to_vec(&value).unwrap()).unwrap_err();
            assert!(
                matches!(&err, Error::Json(e) if e.to_string().contains("unknown field")),
                "{pointer}: {err:?}"
            );
        }
    }

    /// The opaque objects, by contrast, take anything — that is the extension
    /// point.
    #[test]
    fn opaque_objects_accept_arbitrary_shapes() {
        for value in [
            json!({"deeply": {"nested": [1, 2, {"three": true}]}}),
            json!([1, 2, 3]),
            json!("a string"),
            json!(null),
            json!(0),
        ] {
            let mut inscription = sample();
            inscription.position = value.clone();
            inscription.parameters = value.clone();
            inscription.layers[0].scope = value.clone();

            let canonical = inscription.canonicalize().unwrap();
            let parsed = Inscription::parse(&canonical).unwrap();
            assert_eq!(parsed.position, value);
            assert_eq!(parsed.layers[0].scope, value);
        }
    }

    #[test]
    fn rejects_an_unsupported_schema_version() {
        let mut inscription = sample();
        inscription.schema = 2;

        let err = inscription.validate().unwrap_err();
        assert!(
            matches!(err, Error::UnsupportedSchema { found: 2 }),
            "{err:?}"
        );

        // And it must not be possible to obtain a digest for it.
        assert!(inscription.canonicalize().is_err());
    }

    #[test]
    fn rejects_an_unknown_profile_and_a_higher_major_version() {
        let mut inscription = sample();
        inscription.profile.name = "com.acme.other".to_owned();
        let err = inscription.check_profile(&Toy).unwrap_err();
        assert!(matches!(err, Error::UnknownProfile { .. }), "{err:?}");

        let mut inscription = sample();
        inscription.profile.version = 2;
        let err = inscription.check_profile(&Toy).unwrap_err();
        assert!(
            matches!(
                err,
                Error::UnsupportedProfileVersion {
                    found: 2,
                    supported: 1,
                    ..
                }
            ),
            "{err:?}"
        );

        // An older major version is readable by a newer implementation.
        let inscription = sample();
        inscription.check_profile(&Toy).unwrap();

        // A layer kind the profile does not define is readable — skippable, and
        // reported — but not publishable.
        let mut inscription = sample();
        inscription.layers[0].kind = "blocks".to_owned();
        inscription.check_profile(&Toy).unwrap();

        assert_eq!(
            inscription
                .unknown_layers(&Toy)
                .iter()
                .map(|l| l.kind.as_str())
                .collect::<Vec<_>>(),
            vec!["blocks"],
        );

        let err = inscription.check_profile_strict(&Toy).unwrap_err();
        assert!(matches!(err, Error::UnknownLayerKind { .. }), "{err:?}");
    }

    #[test]
    fn rejects_a_malformed_profile_name() {
        let mut inscription = sample();
        inscription.profile.name = "toy".to_owned();
        assert!(matches!(
            inscription.validate().unwrap_err(),
            Error::InvalidProfileName { .. }
        ));
    }

    #[test]
    fn rejects_a_payload_media_type_in_the_reserved_namespace() {
        let mut inscription = sample();
        inscription.layers[0].media_type = "application/vnd.stelae.stele.notes.v1+zstd".to_owned();

        let err = inscription.validate().unwrap_err();
        assert!(matches!(err, Error::InvalidMediaType { .. }), "{err:?}");
    }

    #[test]
    fn history_accepts_a_contiguous_ascending_chain() {
        sample().validate().unwrap();

        // The first stele of a repository carries no history.
        let mut first = sample();
        first.sequence = 0;
        first.history.clear();
        first.validate().unwrap();
    }

    #[test]
    fn history_rejects_a_gap() {
        let mut inscription = sample();
        inscription.sequence = 4;
        inscription.history = vec![
            HistoryEntry {
                sequence: 1,
                inscription_digest: digest_of(1),
            },
            HistoryEntry {
                sequence: 3,
                inscription_digest: digest_of(3),
            },
        ];

        let err = inscription.validate().unwrap_err();
        assert!(
            matches!(&err, Error::HistoryInvariant(m) if m.contains("gap")),
            "{err:?}"
        );
    }

    #[test]
    fn history_rejects_a_duplicate() {
        let mut inscription = sample();
        inscription.history = vec![
            HistoryEntry {
                sequence: 2,
                inscription_digest: digest_of(1),
            },
            HistoryEntry {
                sequence: 2,
                inscription_digest: digest_of(2),
            },
        ];

        let err = inscription.validate().unwrap_err();
        assert!(
            matches!(&err, Error::HistoryInvariant(m) if m.contains("more than once")),
            "{err:?}"
        );
    }

    #[test]
    fn history_rejects_reordering() {
        let mut inscription = sample();
        inscription.history = vec![
            HistoryEntry {
                sequence: 2,
                inscription_digest: digest_of(2),
            },
            HistoryEntry {
                sequence: 1,
                inscription_digest: digest_of(1),
            },
        ];

        let err = inscription.validate().unwrap_err();
        assert!(
            matches!(&err, Error::HistoryInvariant(m) if m.contains("ascending")),
            "{err:?}"
        );
    }

    #[test]
    fn history_must_reach_the_current_sequence() {
        let mut inscription = sample();
        inscription.sequence = 9;

        let err = inscription.validate().unwrap_err();
        assert!(
            matches!(&err, Error::HistoryInvariant(m) if m.contains("ends at sequence")),
            "{err:?}"
        );

        let mut inscription = sample();
        inscription.history.push(HistoryEntry {
            sequence: 3,
            inscription_digest: digest_of(3),
        });
        let err = inscription.validate().unwrap_err();
        assert!(
            matches!(&err, Error::HistoryInvariant(m) if m.contains("not below")),
            "{err:?}"
        );
    }

    #[test]
    fn rejects_numbers_outside_the_jcs_safe_integer_range() {
        let safe = MAX_SAFE_INTEGER as u64;

        let mut inscription = sample();
        inscription.layers[0].uncompressed_size = safe;
        inscription.canonicalize().unwrap();

        let mut inscription = sample();
        inscription.layers[0].uncompressed_size = safe + 1;
        let err = inscription.canonicalize().unwrap_err();
        assert!(
            matches!(&err, Error::UnsafeInteger { path, .. } if path == "$.layers[0].uncompressedSize"),
            "{err:?}"
        );

        // Including inside the profile's opaque objects, which is where a
        // vendor would most plausibly put a raw u64.
        let mut inscription = sample();
        inscription.position = json!({"raw": u64::MAX});
        let err = inscription.canonicalize().unwrap_err();
        assert!(
            matches!(&err, Error::UnsafeInteger { path, .. } if path == "$.position.raw"),
            "{err:?}"
        );

        let mut inscription = sample();
        inscription.parameters = json!({"nested": [0, -9_007_199_254_740_992i64]});
        let err = inscription.canonicalize().unwrap_err();
        assert!(
            matches!(&err, Error::UnsafeInteger { path, .. } if path == "$.parameters.nested[1]"),
            "{err:?}"
        );
    }

    #[test]
    fn rejects_non_integer_numbers() {
        let mut inscription = sample();
        inscription.parameters = json!({"ratio": 0.5});

        let err = inscription.canonicalize().unwrap_err();
        assert!(
            matches!(&err, Error::NonIntegerNumber { path, .. } if path == "$.parameters.ratio"),
            "{err:?}"
        );

        // `56.0` is an integral value but a JSON float, and RFC 8785 renders it
        // through the float path. The rule is about the encoding, not the
        // value.
        let mut inscription = sample();
        inscription.parameters = json!({"whole": 56.0});
        assert!(matches!(
            inscription.canonicalize().unwrap_err(),
            Error::NonIntegerNumber { .. }
        ));
    }

    #[test]
    fn parse_rejects_unsafe_numbers_in_raw_bytes() {
        let raw = br#"{"schema":1,"profile":{"name":"dev.example.toy","version":1},"sequence":1,"position":{"n":18446744073709551615},"parameters":{},"compression":{"algo":"zstd","level":9},"history":[],"layers":[]}"#;

        let err = Inscription::parse(raw).unwrap_err();
        assert!(matches!(err, Error::UnsafeInteger { .. }), "{err:?}");
    }

    #[test]
    fn uncompressed_size_sums_the_layers() {
        let mut inscription = sample();
        inscription.layers.push(LayerDescriptor {
            kind: "notes".to_owned(),
            media_type: "application/vnd.example.stele.notes.v1+zstd".to_owned(),
            diff_id: digest_of(4),
            records: 2,
            uncompressed_size: 72,
            scope: json!({"chapter": 4}),
        });

        assert_eq!(inscription.uncompressed_size(), 200);
        assert_eq!(inscription.layers_of_kind("notes").count(), 2);
        assert_eq!(inscription.layers_of_kind("blocks").count(), 0);
    }
}
