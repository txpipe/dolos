//! The profile extension point and the naming rules that keep profiles apart.
//!
//! A [`Profile`] is a vendor's definition of what a stele of theirs contains:
//! which layer kinds exist, what a record looks like, what goes in `position`,
//! `parameters` and `scope`, and how a sequence renders as a tag. Dolos ships
//! the first one (`io.txpipe.dolos.cardano`); nothing in this crate knows that.
//!
//! ## Where the boundary runs
//!
//! The protocol **asks** for every vendor-owned string and **validates** the
//! answer; it never builds one. `checked_layer_media_type` and
//! `checked_tag_for_sequence` are that rule made mechanical — the core calls
//! those, not [`Profile::layer_media_type`] and [`Profile::tag_for_sequence`]
//! directly, so a profile that returns a colliding or malformed name is refused
//! at the boundary instead of publishing something a third party cannot coexist
//! with.
//!
//! The rules themselves are normative (ADR-004, "Naming, profiles and media
//! types"):
//!
//! 1. Payload media types are
//!    `application/vnd.{vendor}.stele.{kind}.v{n}+{codec}` and carry a vendor
//!    slot the publisher controls. `vnd.stelae.*` is reserved for the
//!    protocol's envelope types and is never a payload type.
//! 2. Profile names are reverse-DNS and vendor-owned.
//! 3. The protocol never parses layer bodies or a profile's opaque objects. An
//!    unknown profile is a clean refusal, never a partial restore.

use crate::{Error, MOVING_TAG, RESERVED_VENDOR};

/// A vendor's definition of what its stelae contain.
///
/// Implementations are expected to be cheap, stateless descriptions — the trait
/// answers questions about naming and kinds, and deliberately has no hook for
/// anything dataset-shaped. If an implementation of the protocol ever needs to
/// ask a profile about chain points, epochs, stores or blocks, the boundary has
/// failed and the fix belongs on the protocol side, not in another trait
/// method.
pub trait Profile {
    /// Reverse-DNS profile name, e.g. `io.txpipe.dolos.cardano`.
    fn name(&self) -> &str;

    /// Major version of the profile this implementation implements.
    ///
    /// A client refuses an inscription whose profile major version is above
    /// this: a newer major may have changed record shapes it would otherwise
    /// misread.
    fn version(&self) -> u64;

    /// The layer kinds this profile defines. Used to refuse an inscription that
    /// names a kind the profile does not know before any blob is fetched.
    fn kinds(&self) -> &[&str];

    /// Media type for a layer of `kind`. Vendor-owned; the protocol validates
    /// the shape but never composes the string.
    fn layer_media_type(&self, kind: &str) -> Result<String, Error>;

    /// The immutable tag under which a stele at `sequence` is published.
    fn tag_for_sequence(&self, sequence: u64) -> Result<String, Error>;

    /// The moving tag pointing at the most recent stele. Profiles may override
    /// the spelling; the protocol only requires that one exists.
    fn moving_tag(&self) -> &str {
        MOVING_TAG
    }
}

/// Ask `profile` for the media type of `kind` and enforce the protocol's naming
/// rules on the answer.
pub fn checked_layer_media_type(profile: &dyn Profile, kind: &str) -> Result<String, Error> {
    if !profile.kinds().contains(&kind) {
        return Err(Error::UnknownLayerKind {
            profile: profile.name().to_owned(),
            kind: kind.to_owned(),
        });
    }

    let media_type = profile.layer_media_type(kind)?;
    let parsed = MediaType::parse(&media_type)?;

    // Parsing establishes that the profile's answer is well formed; this
    // establishes that it is an answer to the question asked. Without it a
    // descriptor can carry a `kind` and a `mediaType` whose embedded kind
    // disagree — the exact ambiguity the naming rules exist to remove.
    if parsed.kind != kind {
        return Err(Error::InvalidMediaType {
            value: media_type,
            reason: format!(
                "names layer kind {:?}, but was asked for {kind:?}",
                parsed.kind
            ),
        });
    }

    Ok(media_type)
}

/// Ask `profile` for the immutable tag of `sequence` and enforce OCI tag syntax
/// on the answer.
pub fn checked_tag_for_sequence(profile: &dyn Profile, sequence: u64) -> Result<String, Error> {
    let tag = profile.tag_for_sequence(sequence)?;
    validate_tag(&tag)?;
    Ok(tag)
}

/// Validate a profile name: reverse-DNS, lowercase, at least two labels.
pub fn validate_profile_name(name: &str) -> Result<(), Error> {
    let invalid = |reason: &str| Error::InvalidProfileName {
        value: name.to_owned(),
        reason: reason.to_owned(),
    };

    if name.is_empty() {
        return Err(invalid("empty"));
    }

    let labels: Vec<&str> = name.split('.').collect();

    if labels.len() < 2 {
        return Err(invalid("expected reverse-DNS with at least two labels"));
    }

    for label in labels {
        if label.is_empty() {
            return Err(invalid("empty label"));
        }

        if !label
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        {
            return Err(invalid(
                "labels may contain lowercase ascii letters, digits and hyphens only",
            ));
        }

        if label.starts_with('-') || label.ends_with('-') {
            return Err(invalid("labels must not start or end with a hyphen"));
        }
    }

    Ok(())
}

/// Validate an OCI tag: `[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}`.
pub fn validate_tag(tag: &str) -> Result<(), Error> {
    let invalid = |reason: &str| Error::InvalidTag {
        value: tag.to_owned(),
        reason: reason.to_owned(),
    };

    if tag.is_empty() {
        return Err(invalid("empty"));
    }

    if tag.len() > 128 {
        return Err(invalid("longer than 128 characters"));
    }

    let mut bytes = tag.bytes();
    let first = bytes.next().expect("non-empty");

    if !(first.is_ascii_alphanumeric() || first == b'_') {
        return Err(invalid("must start with an alphanumeric or underscore"));
    }

    if !bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'.' || b == b'-') {
        return Err(invalid(
            "may contain alphanumerics, underscore, period and hyphen only",
        ));
    }

    Ok(())
}

/// A parsed payload media type:
/// `application/vnd.{vendor}.stele.{kind}.v{n}+{codec}`.
///
/// The protocol parses these to enforce the coexistence rules. It does not
/// build them and does not attach meaning to `kind` or `codec`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaType {
    pub vendor: String,
    pub kind: String,
    pub version: u64,
    pub codec: String,
}

const MEDIA_TYPE_PREFIX: &str = "application/vnd.";
const STELE_INFIX: &str = ".stele.";

impl MediaType {
    pub fn parse(value: &str) -> Result<Self, Error> {
        let invalid = |reason: &str| Error::InvalidMediaType {
            value: value.to_owned(),
            reason: reason.to_owned(),
        };

        let rest = value
            .strip_prefix(MEDIA_TYPE_PREFIX)
            .ok_or_else(|| invalid("expected the `application/vnd.` prefix"))?;

        let (body, codec) = rest
            .split_once('+')
            .ok_or_else(|| invalid("expected a `+{codec}` suffix"))?;

        if codec.is_empty() {
            return Err(invalid("empty codec"));
        }

        let (vendor, tail) = body
            .split_once(STELE_INFIX)
            .ok_or_else(|| invalid("expected `.stele.` between the vendor and the kind"))?;

        if vendor.is_empty() {
            return Err(invalid("empty vendor"));
        }

        // Rule 1: `vnd.stelae.*` names envelope types only. A payload type that
        // claimed it would make a vendor's blobs indistinguishable from the
        // protocol's own.
        if vendor == RESERVED_VENDOR || vendor.starts_with(&format!("{RESERVED_VENDOR}.")) {
            return Err(invalid(
                "`vnd.stelae.*` is reserved for the protocol's envelope types",
            ));
        }

        if !vendor
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.')
        {
            return Err(invalid(
                "vendor may contain lowercase ascii letters, digits, hyphens and periods only",
            ));
        }

        let (kind, version) = tail
            .rsplit_once(".v")
            .ok_or_else(|| invalid("expected a `.v{n}` version segment after the kind"))?;

        if kind.is_empty() {
            return Err(invalid("empty kind"));
        }

        if !kind
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        {
            return Err(invalid(
                "kind may contain lowercase ascii letters, digits and hyphens only",
            ));
        }

        if version.is_empty() || !version.bytes().all(|b| b.is_ascii_digit()) {
            return Err(invalid("version must be a decimal integer"));
        }

        let version = version
            .parse::<u64>()
            .map_err(|_| invalid("version is out of range"))?;

        Ok(Self {
            vendor: vendor.to_owned(),
            kind: kind.to_owned(),
            version,
            codec: codec.to_owned(),
        })
    }
}

impl std::fmt::Display for MediaType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{MEDIA_TYPE_PREFIX}{}{STELE_INFIX}{}.v{}+{}",
            self.vendor, self.kind, self.version, self.codec
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_reference_profile_media_types() {
        for (raw, vendor, kind) in [
            (
                "application/vnd.dolos.stele.blocks.v1+zstd",
                "dolos",
                "blocks",
            ),
            (
                "application/vnd.dolos.stele.state.v1+zstd",
                "dolos",
                "state",
            ),
            (
                "application/vnd.acme.stele.receipts.v1+zstd",
                "acme",
                "receipts",
            ),
            (
                "application/vnd.example.stele.notes.v1+zstd",
                "example",
                "notes",
            ),
        ] {
            let parsed = MediaType::parse(raw).unwrap();
            assert_eq!(parsed.vendor, vendor);
            assert_eq!(parsed.kind, kind);
            assert_eq!(parsed.version, 1);
            assert_eq!(parsed.codec, "zstd");
            assert_eq!(parsed.to_string(), raw);
        }
    }

    /// The reserved-vendor rule is the one that makes third-party coexistence
    /// safe, so it is checked in both spellings.
    #[test]
    fn rejects_the_reserved_vendor_slot() {
        for raw in [
            "application/vnd.stelae.stele.blocks.v1+zstd",
            "application/vnd.stelae.inscription.stele.x.v1+zstd",
        ] {
            let err = MediaType::parse(raw).unwrap_err();
            assert!(
                matches!(&err, Error::InvalidMediaType { reason, .. } if reason.contains("reserved")),
                "{raw}: {err:?}"
            );
        }
    }

    /// The protocol's own envelope types are not payload types and must not
    /// parse as one.
    #[test]
    fn envelope_types_are_not_payload_types() {
        for raw in [
            crate::ARTIFACT_TYPE,
            crate::INSCRIPTION_MEDIA_TYPE,
            crate::SIGNATURE_MEDIA_TYPE,
        ] {
            assert!(MediaType::parse(raw).is_err(), "{raw} must not parse");
        }
    }

    #[test]
    fn rejects_malformed_media_types() {
        for raw in [
            "application/json",
            "application/vnd.dolos.stele.blocks.v1",
            "application/vnd.dolos.blocks.v1+zstd",
            "application/vnd..stele.blocks.v1+zstd",
            "application/vnd.dolos.stele..v1+zstd",
            "application/vnd.dolos.stele.blocks.v+zstd",
            "application/vnd.dolos.stele.blocks.vx+zstd",
            "application/vnd.dolos.stele.blocks.v1+",
            "application/vnd.Dolos.stele.blocks.v1+zstd",
            "application/vnd.dolos.stele.Blocks.v1+zstd",
        ] {
            assert!(MediaType::parse(raw).is_err(), "{raw} must not parse");
        }
    }

    #[test]
    fn validates_profile_names() {
        for good in [
            "io.txpipe.dolos.cardano",
            "dev.example.toy",
            "com.acme.receipts",
            "a.b",
        ] {
            validate_profile_name(good).unwrap();
        }

        for bad in [
            "",
            "dolos",
            "io..dolos",
            "IO.txpipe.dolos",
            "io.txpipe.dolos_cardano",
            "io.-txpipe.dolos",
            "io.txpipe-.dolos",
        ] {
            assert!(validate_profile_name(bad).is_err(), "{bad} must not pass");
        }
    }

    #[test]
    fn validates_tags() {
        for good in ["latest", "epoch-550", "note-0", "v1.6.0", "_x"] {
            validate_tag(good).unwrap();
        }

        for bad in ["", ".hidden", "-leading", "with space", "with/slash"] {
            assert!(validate_tag(bad).is_err(), "{bad:?} must not pass");
        }

        assert!(validate_tag(&"a".repeat(128)).is_ok());
        assert!(validate_tag(&"a".repeat(129)).is_err());
    }

    struct Fake {
        media_type: &'static str,
        tag: &'static str,
    }

    impl Profile for Fake {
        fn name(&self) -> &str {
            "dev.example.fake"
        }

        fn version(&self) -> u64 {
            1
        }

        fn kinds(&self) -> &[&str] {
            &["notes"]
        }

        fn layer_media_type(&self, _kind: &str) -> Result<String, Error> {
            Ok(self.media_type.to_owned())
        }

        fn tag_for_sequence(&self, _sequence: u64) -> Result<String, Error> {
            Ok(self.tag.to_owned())
        }
    }

    /// A profile that hands back a colliding or malformed name is refused at
    /// the boundary — the protocol validates what it is given rather than
    /// trusting it, which is the other half of "never construct the string
    /// yourself".
    #[test]
    fn checked_accessors_refuse_bad_profile_answers() {
        let good = Fake {
            media_type: "application/vnd.example.stele.notes.v1+zstd",
            tag: "note-1",
        };
        assert_eq!(
            checked_layer_media_type(&good, "notes").unwrap(),
            "application/vnd.example.stele.notes.v1+zstd"
        );
        assert_eq!(checked_tag_for_sequence(&good, 1).unwrap(), "note-1");

        let colliding = Fake {
            media_type: "application/vnd.stelae.stele.notes.v1+zstd",
            tag: "note-1",
        };
        assert!(checked_layer_media_type(&colliding, "notes").is_err());

        let bad_tag = Fake {
            media_type: "application/vnd.example.stele.notes.v1+zstd",
            tag: "note 1",
        };
        assert!(checked_tag_for_sequence(&bad_tag, 1).is_err());

        // A kind the profile does not declare never reaches the profile at all.
        let err = checked_layer_media_type(&good, "blocks").unwrap_err();
        assert!(matches!(err, Error::UnknownLayerKind { .. }), "{err:?}");
    }

    /// A well-formed name for the *wrong* kind is still a wrong answer: the
    /// descriptor it would produce carries a `kind` and a `mediaType` that
    /// disagree, which is the ambiguity the naming rules exist to remove.
    #[test]
    fn checked_media_type_refuses_a_name_for_another_kind() {
        let mislabelled = Fake {
            media_type: "application/vnd.example.stele.blocks.v1+zstd",
            tag: "note-1",
        };

        let err = checked_layer_media_type(&mislabelled, "notes").unwrap_err();
        assert!(matches!(err, Error::InvalidMediaType { .. }), "{err:?}");
    }

    #[test]
    fn moving_tag_defaults_to_latest() {
        let profile = Fake {
            media_type: "application/vnd.example.stele.notes.v1+zstd",
            tag: "note-1",
        };
        assert_eq!(profile.moving_tag(), MOVING_TAG);
        validate_tag(profile.moving_tag()).unwrap();
    }
}
