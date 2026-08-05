//! Done criterion 2: every layer kind survives a real write and read.
//!
//! Not `encode` against `decode` — that pair can agree with each other and with
//! nothing else. Each kind goes typed record → encode → **layer written to a
//! `SteleDir`** → layer read back → decode → compared, through both of the
//! protocol's read paths. A record only counts as round-tripped once it has
//! been framed, compressed, digested, decompressed and re-framed.

mod common;

use common::*;
use dolos_core::{
    key_hash, ExactKind, ExactRecord, IndexRecord, MAX_EXACT_KEY_LEN, VERBATIM_KEY_DIMENSION,
};
use dolos_snapshot::{
    layers::{blocks, digests, indexes, logs, state},
    DolosProfile, Error, Scope, BLOCKS, DIGESTS, INDEXES, LOGS, STATE,
};
use stelae::{
    dir::SteleDir,
    frame::{self, CanonicalCbor},
    Profile,
};

/// Write one layer, read it back both ways, and decode what comes out.
fn roundtrip<T>(
    kind: &'static str,
    scope: &dyn Scope,
    records: &[T],
    encode: impl Fn(&T) -> Result<CanonicalCbor, Error>,
    decode: impl Fn(&[u8]) -> Result<T, Error>,
) -> Vec<T>
where
    T: std::fmt::Debug + PartialEq,
{
    let temp = tempfile::tempdir().unwrap();
    let stele = SteleDir::create(temp.path()).unwrap();

    let encoded = encode_all(records, encode);
    let written = write_layer(&stele, kind, scope, &encoded);

    assert_eq!(written.descriptor.kind, kind);
    assert_eq!(
        written.descriptor.records,
        records.len() as u64 + 1,
        "{kind}: the header record counts too"
    );

    let index = stele.blob_index().unwrap();
    let raw = read_both_ways(&stele, &index, &written.descriptor);

    // The bytes that came back are the bytes that went in, before anything is
    // decoded — so a decode bug cannot hide a framing bug.
    let expected: Vec<Vec<u8>> = encoded.iter().map(|r| r.as_bytes().to_vec()).collect();
    assert_eq!(raw, expected, "{kind}: layer content");

    let decoded: Vec<T> = raw.iter().map(|r| decode(r).unwrap()).collect();
    assert_eq!(&decoded, records, "{kind}: decoded records");

    decoded
}

#[test]
fn blocks_round_trip_through_a_layer() {
    let records = common::blocks();
    let decoded = roundtrip(
        BLOCKS,
        &epoch_scope(),
        &records,
        blocks::encode,
        blocks::decode,
    );

    // The Byron case: two blocks at one slot, distinguishable only by the order
    // they arrived in. A layer that sorted its records would swap or drop one.
    assert_eq!(decoded[0].slot, decoded[1].slot);
    assert_ne!(decoded[0].hash, decoded[1].hash);
    assert_eq!(decoded[0].hash, records[0].hash, "stream order survived");
    assert_eq!(decoded[1].hash, records[1].hash, "stream order survived");

    let mut order = blocks::OrderCheck::default();
    for record in &decoded {
        order.check(record).unwrap();
    }
}

#[test]
fn indexes_round_trip_through_a_layer() {
    let records = common::indexes();
    let decoded = roundtrip(
        INDEXES,
        &epoch_scope(),
        &records,
        indexes::encode,
        indexes::decode,
    );

    let mut order = indexes::OrderCheck::default();
    for record in &decoded {
        order.check(record).unwrap();
    }

    // The `metadata` dimension carries its logical u64 label verbatim, never
    // hashed — the one exception to the pre-hashed rule, and the one a
    // conformance check has to look at specifically, because a hashed label is
    // structurally valid and permanently unqueryable.
    let metadata = decoded
        .iter()
        .find_map(|record| match record {
            IndexRecord::Tag(tag) if tag.dimension() == VERBATIM_KEY_DIMENSION => Some(tag),
            _ => None,
        })
        .expect("the fixture carries a metadata tag");

    assert_eq!(metadata.key_hash.len(), 8);
    assert_eq!(u64::from_be_bytes(metadata.key_hash), METADATA_LABEL);
    assert_eq!(
        metadata.key_hash,
        key_hash(VERBATIM_KEY_DIMENSION, &METADATA_LABEL.to_be_bytes()).unwrap(),
        "the stored form is the one dolos-core defines",
    );

    // A block number is an 8-byte big-endian key, unlike the 32-byte hashes
    // either side of it — the width the exact-record type validates against.
    let block_num = decoded
        .iter()
        .find_map(|record| match record {
            IndexRecord::Exact(exact) if exact.kind == ExactKind::BlockNumber => Some(exact),
            _ => None,
        })
        .expect("the fixture carries a block_num record");

    assert_eq!(block_num.key().len(), 8);
    assert_eq!(
        u64::from_be_bytes(block_num.key().try_into().unwrap()),
        4242
    );
}

#[test]
fn logs_round_trip_through_a_layer() {
    let records = common::logs();
    let decoded = roundtrip(LOGS, &epoch_scope(), &records, logs::encode, logs::decode);

    let mut order = logs::OrderCheck::default();
    for record in &decoded {
        order.check(record).unwrap();
    }
}

#[test]
fn state_round_trips_through_a_layer_per_shard() {
    for shard in SHARDS {
        let records = common::state(shard);
        let decoded = roundtrip(
            STATE,
            &state_scope(shard),
            &records,
            state::encode,
            state::decode,
        );

        let mut order = state::OrderCheck::for_shard(shard);
        for record in &decoded {
            order.check(record).unwrap();
        }

        // The utxo value is the one field this profile composes rather than
        // carries, so it is the one that has to survive the trip as a value and
        // not merely as bytes.
        let utxo = decoded
            .iter()
            .find(|record| record.ns == dolos_snapshot::UTXOS)
            .expect("the fixture carries a utxo record");

        let (txo, value) = state::as_utxo(utxo).unwrap();
        assert_eq!(txo.1, 3);
        assert_eq!(value.0, 6);
    }
}

#[test]
fn digests_round_trip_through_a_layer() {
    let records = common::digests();
    let decoded = roundtrip(
        DIGESTS,
        &digests_scope(),
        &records,
        digests::encode,
        digests::decode,
    );

    let mut order = digests::OrderCheck::default();
    for record in &decoded {
        order.check(record).unwrap();
    }
}

/// A key wider than any exact kind's is refused *after* a clean layer read.
///
/// The framing layer has no opinion about it — the bytes are canonical CBOR and
/// the layer's digest is whatever it is — so the refusal has to come from the
/// codec, and it comes from `ExactRecord::new`, which is where the width rule
/// already lives. Anything else would be a second validation site free to
/// disagree with the first.
#[test]
fn an_over_wide_exact_key_survives_the_layer_and_is_refused_by_the_codec() {
    let temp = tempfile::tempdir().unwrap();
    let stele = SteleDir::create(temp.path()).unwrap();

    let forged = frame::encode(|e| {
        e.array(4)?
            .u64(indexes::EXACT_DISCRIMINANT)?
            .str(ExactKind::TxHash.as_str())?
            .bytes(&[0xcc; MAX_EXACT_KEY_LEN + 1])?
            .u64(END_SLOT)?;
        Ok(())
    })
    .unwrap();

    let written = write_layer(
        &stele,
        INDEXES,
        &epoch_scope(),
        std::slice::from_ref(&forged),
    );
    let index = stele.blob_index().unwrap();
    let raw = read_both_ways(&stele, &index, &written.descriptor);

    assert_eq!(raw, vec![forged.as_bytes().to_vec()]);

    let err = indexes::decode(&raw[0]).unwrap_err();
    assert!(matches!(err, Error::Index(_)), "{err:?}");

    // And the width the type does accept is refused when it is the wrong kind's.
    assert!(ExactRecord::new(ExactKind::TxHash, &4242u64.to_be_bytes(), 1).is_err());
}

/// A layer read under the wrong kind is refused before a record is handed out.
///
/// The blob's header names `blocks`; the descriptor claims `digests`. Both
/// kinds are this profile's, so what refuses it is the kind check and not the
/// profile check — `a_layer_of_another_profile_is_refused` covers the other
/// half.
#[test]
fn a_layer_read_under_the_wrong_kind_is_refused() {
    let temp = tempfile::tempdir().unwrap();
    let stele = SteleDir::create(temp.path()).unwrap();

    let written = write_layer(
        &stele,
        BLOCKS,
        &epoch_scope(),
        &encode_all(&common::blocks(), blocks::encode),
    );

    let index = stele.blob_index().unwrap();

    let mut mislabelled = written.descriptor.clone();
    mislabelled.kind = DIGESTS.to_owned();
    mislabelled.media_type = DolosProfile
        .layer_media_type(DIGESTS)
        .expect("a kind this profile defines");

    let err = stele
        .read_layer(&index, &DolosProfile, &mislabelled)
        .unwrap_err();
    assert!(
        matches!(err, stelae::Error::LayerMismatch { .. }),
        "{err:?}"
    );
}

/// A layer whose header names another profile is refused before a record is
/// handed out — the fail-closed rule, exercised from this profile's side.
///
/// The awkward case on purpose: a vendor whose records frame identically to
/// ours, under a kind name we also use. Nothing about the bytes distinguishes
/// the two layers. Only the profile name in the header record does, which is
/// why the protocol puts it there.
#[test]
fn a_layer_of_another_profile_is_refused() {
    struct Other;

    impl Profile for Other {
        fn name(&self) -> &str {
            "com.acme.receipts"
        }

        fn version(&self) -> u64 {
            1
        }

        fn kinds(&self) -> &[&str] {
            &[BLOCKS]
        }

        fn layer_media_type(&self, kind: &str) -> Result<String, stelae::Error> {
            Ok(format!("application/vnd.acme.stele.{kind}.v1+zstd"))
        }

        fn tag_for_sequence(&self, sequence: u64) -> Result<String, stelae::Error> {
            Ok(format!("r-{sequence}"))
        }
    }

    let temp = tempfile::tempdir().unwrap();
    let stele = SteleDir::create(temp.path()).unwrap();

    let scope = epoch_scope();
    let written = stele
        .write_layer(
            &Other,
            &stelae::dir::LayerSpec::new(BLOCKS, scope.header().unwrap(), scope.descriptor()),
            dolos_snapshot::COMPRESSION_LEVEL,
            &encode_all(&common::blocks(), blocks::encode),
        )
        .unwrap();

    let index = stele.blob_index().unwrap();

    let buffered = stele
        .read_layer(&index, &DolosProfile, &written.descriptor)
        .unwrap_err();
    assert!(
        matches!(buffered, stelae::Error::UnknownProfile { .. }),
        "{buffered:?}"
    );

    // And on the streaming path, which is the one a restore uses.
    let streamed = stele
        .stream_layer(
            &index,
            &DolosProfile,
            &written.descriptor,
            frame::Limits::default(),
        )
        .map(|_| ())
        .unwrap_err();
    assert!(
        matches!(streamed, stelae::Error::UnknownProfile { .. }),
        "{streamed:?}"
    );
}
