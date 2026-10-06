//! Done criterion 3: the golden digests.
//!
//! Every value pinned here is a sha256 over bytes this profile and ADR-004
//! together fully determine — the deterministic CBOR sequence of each layer,
//! and the RFC 8785 canonical JSON of the inscription. Nothing platform-,
//! timing- or compression-dependent enters them, so they hold across machines
//! and across zstd versions.
//!
//! That makes this file the drift alarm for the whole profile. These digests
//! *are* published identity: a change to a record shape, a scope key spelling,
//! a media type, a namespace string or an exact-kind literal moves one, and
//! moving one silently is precisely the failure Stelae exists to prevent. A
//! deliberate format change updates these in the same commit that changes the
//! ADR. An accidental one shows up here first — and a value that disagrees with
//! a re-run on unchanged code is encoding nondeterminism, which is a finding,
//! never a re-pin.
//!
//! A change to one of these items changes at least one digest:
//!
//! - the twenty-one media types
//! - the tag string
//! - the names of the sixteen archive dimensions
//! - the literals of the three exact-record kinds
//! - the three `log-{ns}` kind strings and the fifteen `state-{ns}` kind
//!   strings
//! - the shapes of the layer header and of the scope
//! - the key spellings in `position` and in `parameters`, also in the shard map
//!   and in the schema map
//!
//! The kind strings contain the eighteen namespace strings, because the log
//! and state records do not contain a namespace.

mod common;

use common::*;
use dolos_core::{BlockHash, ChainPoint};
use dolos_snapshot::{
    layers::{blocks, digests, indexes, logs, state},
    log_ns_for, state_ns_for, DolosProfile, RetainedEpochs, BLOCKS, DIGESTS, INDEXES,
};
use stelae::{
    dir::SteleDir,
    frame::Limits,
    inscription::{HistoryEntry, Inscription},
    Digest, SteleReader, SteleWriter,
};

/// `(kind, diffId, records, uncompressedSize)`, in inscription order.
///
/// `records` counts the protocol's header record, as a descriptor does.
const GOLDEN_LAYERS: [(&str, &str, u64, u64); 44] = [
    (
        BLOCKS,
        "sha256:14a05418723da3c0b4117b5f30ef07d96887b3e12eae114988ff299a654ff106",
        4,
        167,
    ),
    (
        INDEXES,
        "sha256:be485ff209757934d6336a169d8608b36fdbf11e98ac03a3ff33fd07cc50350c",
        20,
        531,
    ),
    (
        "log-account-epochs",
        "sha256:be63f933f87028f2650741a6964a8a0e1bae9a12f73e012f0854a4c568a23055",
        2,
        102,
    ),
    (
        "log-epochs",
        "sha256:a42213234eaff408cfbf9cde9f43eb782b41726da3e144d497c576f4cabf37c4",
        2,
        94,
    ),
    (
        "log-stakes",
        "sha256:e650e8487b5827fc62a14428eeeb637f3d4ade3ba97cb5ec58465393ab503b9b",
        2,
        94,
    ),
    (
        "state-account-epochs",
        "sha256:9391e4ef7ca6c4b9413c21365423daa77571e89aaaedc5cfff6dd81655e90e11",
        2,
        93,
    ),
    (
        "state-account-epochs",
        "sha256:9946fd4c26d9cdb01865650503f94bd2c1cf8367b4a40e3a8f7cb0d65f5718eb",
        2,
        93,
    ),
    (
        "state-accounts",
        "sha256:f213e003211e343abf8047535fa67e6dfa0e715ddfa0053a1fbef5213d5fe191",
        2,
        87,
    ),
    (
        "state-accounts",
        "sha256:9f56ac182d1d1793a9715475eca33c235902548e220c648ae84f53a4de241b24",
        2,
        87,
    ),
    (
        "state-accounts",
        "sha256:db47596747e81165ce2948f333cc6e6ab3c240f6973fb627df1d9b65dc50c8f2",
        2,
        87,
    ),
    (
        "state-accounts",
        "sha256:0c7bc44f2f59b16a803369072e19011789cf4221dc94f409626e52d00f10eb1f",
        2,
        87,
    ),
    (
        "state-assets",
        "sha256:59c96281ae4d1b7cc14cf8b7bbd7629526e8f3b1f6abd27bec605c68b0617803",
        2,
        85,
    ),
    (
        "state-assets",
        "sha256:0392762c0eb70060ef3e577b9f67350acc4a774bc819d4edc56ecd37d90e3830",
        2,
        85,
    ),
    (
        "state-assets",
        "sha256:8077fd64690d954acc788df3b8046d151729ccd2b7d1dae42ed177f3931747aa",
        2,
        85,
    ),
    (
        "state-assets",
        "sha256:c7c1ff92712bfa3bff88b9ce2445bb3f4e55139ac5bc89c989f41188f894eeff",
        2,
        85,
    ),
    (
        "state-datums",
        "sha256:51ccdf0f0eaf485ef81ebbf89830d090b053fe3b4bdf22c7857791ccb32fcb1c",
        2,
        85,
    ),
    (
        "state-datums",
        "sha256:403ed6f8e7d4c3b975577d534959ad446ed7282fbee2cde325ac08fdcac9f492",
        2,
        85,
    ),
    (
        "state-datums",
        "sha256:41c9a9cba05c8bfc0d7e345b820f62d4fc3e16fa5deef8ac224f9422ae1c3bb0",
        2,
        85,
    ),
    (
        "state-datums",
        "sha256:444ae56c151ffb66b8dd716e16b578d9ec4e339251ba24406d935b5214db27da",
        2,
        85,
    ),
    (
        "state-dreps",
        "sha256:69bae486654647dc4e56d7920c130002775679c5b3c4ac6b6a5d42ed3c7ee41a",
        2,
        84,
    ),
    (
        "state-dreps",
        "sha256:96f0a867e25607b374f34830c4499ae735a860486b866e740998c781ef0f29ca",
        2,
        84,
    ),
    (
        "state-epochs",
        "sha256:89594968aca68e2811574fda91f3da8531848cc0aaad64c6274290317ffeb909",
        2,
        85,
    ),
    (
        "state-epochs",
        "sha256:93b0aab4fd80315a8bf106397e7575f58b031a8fe9680dac5d21bf84c880a23a",
        2,
        85,
    ),
    (
        "state-eras",
        "sha256:347420017e065a7c98a5e07a615d9bfbb7be305b67ef56a2cf0cd44486b5d8ff",
        2,
        83,
    ),
    (
        "state-eras",
        "sha256:ad1c687b6b519d6d922764504a333d91733a98a251c606524532c6b066167648",
        2,
        83,
    ),
    (
        "state-gov",
        "sha256:81afb0f36d765a73bdfd273069ae00b616911b611938bf5bcce2de8e2e79d1a3",
        2,
        82,
    ),
    (
        "state-gov",
        "sha256:f569b56280287b58f66dc13c1f4c0b2f6895befb01399ec6d2c73e13a8679a97",
        2,
        82,
    ),
    (
        "state-metadata-labels",
        "sha256:fe9b354a62a7007ceb3042f4f2acea5b9b530f16cc7344c3180a0d0b9f927652",
        2,
        94,
    ),
    (
        "state-metadata-labels",
        "sha256:c2f8f9752e5a802d22b879abd2e6baf671322f9c6c0417cf37d115a69fb96f94",
        2,
        94,
    ),
    (
        "state-pending-mirs",
        "sha256:113c0fe6fc088ab6ce78a74659824d2936e7afc1effd5a85a79ba286df66353c",
        2,
        91,
    ),
    (
        "state-pending-mirs",
        "sha256:71453eb3f5ac8d93bc365bed5cffa7cfca32445f1ec0d1756316f11028b67b6b",
        2,
        91,
    ),
    (
        "state-pending-rewards",
        "sha256:a233fd258cc3608e538ca46d15fd2a711b552fe7e60d0c18a044e1a9ce78b084",
        2,
        94,
    ),
    (
        "state-pending-rewards",
        "sha256:7e077dd045b29091691e94ffb7c60b7566a7f94b1e37a3d6c026b6b72a2fb871",
        2,
        94,
    ),
    (
        "state-pools",
        "sha256:fd25e6d56516d92c0d950a84542f18221aa064aae3535d1b34940f34d3127589",
        2,
        84,
    ),
    (
        "state-pools",
        "sha256:7fc0d7c8fac6b730d8dca30a6a8ad04ac7b562e22412ce4e8340c1fff226d6ec",
        2,
        84,
    ),
    (
        "state-proposals",
        "sha256:33a21f13cfd63d79acab61e1af9e574066e114b765982e4fdf89103a71570597",
        2,
        88,
    ),
    (
        "state-proposals",
        "sha256:5dd164a8a7bbb494513300c6e108ee4235898105bbca37ac262ca92b88ed6bfb",
        2,
        88,
    ),
    (
        "state-stakes",
        "sha256:90ccd489027c6166ec5d269d3717fd9e2a918a499d07115aade1990c96200d92",
        2,
        85,
    ),
    (
        "state-stakes",
        "sha256:8a24bf24ef897aa68bf9d276b2a627a256734f80c4a886a34d38606d5f02c7a3",
        2,
        85,
    ),
    (
        "state-utxos",
        "sha256:b66979ba19b82c97de5031b923a09c8ebc26b4ba9d4d45477a84a905a2f6a752",
        2,
        91,
    ),
    (
        "state-utxos",
        "sha256:c532bceb633a91dd6cc0b8789cf3170a55aaa6676b37229ed399bdc36378ab21",
        2,
        91,
    ),
    (
        "state-utxos",
        "sha256:b882014a52055e12c9b4f1ef7c1aeddbdac5e4887146aab3b5b9f20f68b50d45",
        2,
        91,
    ),
    (
        "state-utxos",
        "sha256:5b863d374031f390aa04299fc4b3527258401e5d438f1d6e422539aad0e29244",
        2,
        91,
    ),
    (
        DIGESTS,
        "sha256:13f9bbdf676ac47ad7238a52fa525f4413a88335c23d2d567c888abd8dedec80",
        3,
        250,
    ),
];

/// The stele's identity: sha256 of the canonical inscription.
const GOLDEN_INSCRIPTION: &str =
    "sha256:b48d21323bfff18d1cb30735413db2a76f1fa90f4a1a0c6ebc954ac32f11187e";

fn history() -> Vec<HistoryEntry> {
    vec![
        HistoryEntry {
            sequence: EPOCH - 2,
            inscription_digest: Digest::from_bytes([0x55; 32]),
        },
        HistoryEntry {
            sequence: EPOCH - 1,
            inscription_digest: Digest::from_bytes([0x66; 32]),
        },
    ]
}

/// Write the whole fixture stele into `root`: forty-four layers and an
/// inscription.
fn write_stele(root: &std::path::Path) -> (Inscription, Digest) {
    let stele = SteleDir::create(root).unwrap();

    let point = ChainPoint::Specific(END_SLOT, BlockHash::new(POINT_HASH));

    let mut inscription = Inscription::new(
        &DolosProfile,
        EPOCH,
        dolos_snapshot::position(&network(), &point, EPOCH).unwrap(),
        dolos_snapshot::parameters(&RetainedEpochs::new(vec![DUMP_EPOCH]).unwrap()),
        dolos_snapshot::compression(),
    );

    inscription.history = history();

    inscription.layers = all_layers()
        .into_iter()
        .map(|(kind, scope, records)| {
            write_layer(&stele, kind, scope.as_ref(), &records).descriptor
        })
        .collect();

    let digest = stele.seal(&DolosProfile, &inscription).unwrap();

    (inscription, digest)
}

/// Each layer's identity, computed from the byte string a `diffId` covers,
/// without a directory in the way.
#[test]
fn per_kind_diff_ids_are_pinned() {
    let layers = all_layers();

    // Before the zip, not after: `zip` stops at the shorter side, so a dropped
    // layer would quietly shorten the loop rather than fail it — in the one test
    // whose whole job is to notice a layer changing.
    assert_eq!(
        layers.len(),
        GOLDEN_LAYERS.len(),
        "the stele no longer has the layers the goldens pin"
    );

    for ((kind, scope, records), (expected_kind, diff_id, count, size)) in
        layers.into_iter().zip(GOLDEN_LAYERS)
    {
        assert_eq!(kind, expected_kind);

        let bytes = sequence(kind, scope.as_ref(), &records);

        assert_eq!(
            Digest::compute(&bytes).to_string(),
            diff_id,
            "{kind}: diffId drifted"
        );
        assert_eq!(records.len() as u64 + 1, count, "{kind}: record count");
        assert_eq!(bytes.len() as u64, size, "{kind}: uncompressed size");
    }
}

/// The whole stele: written, read back through the streaming reader under the
/// default limits, and reproduced byte for byte by a second, independent write.
#[test]
fn a_complete_stele_reads_back_and_reproduces_its_digest() {
    let first = tempfile::tempdir().unwrap();
    let (inscription, digest) = write_stele(first.path());

    assert_eq!(
        digest.to_string(),
        GOLDEN_INSCRIPTION,
        "inscription digest drifted"
    );

    // The document survives the trip to disk in canonical form, and belongs to
    // this profile.
    let stele = SteleDir::open(first.path()).unwrap();
    let read = stele.read_inscription().unwrap();

    assert_eq!(read, inscription);
    assert_eq!(read.digest().unwrap(), digest);
    read.check_profile(&DolosProfile).unwrap();

    // Every layer streams back under the *default* record ceiling — the
    // confirmation the stelae crate's streaming reader was left waiting for from
    // its first real profile.
    let index = stele.blob_index().unwrap();
    assert_eq!(index.len(), GOLDEN_LAYERS.len());

    for descriptor in &read.layers {
        let mut reader = stele
            .stream_layer(&index, &DolosProfile, descriptor, Limits::default())
            .unwrap();

        assert_eq!(reader.header().profile, dolos_snapshot::PROFILE_NAME);
        assert_eq!(reader.header().kind, descriptor.kind);

        let mut records = 1u64;
        while let Some(record) = reader.next_record() {
            let record = record.unwrap();

            // Decoded, not merely counted: a record that frames cleanly and
            // does not decode is a layer this profile cannot restore.
            decode_one(&descriptor.kind, record);
            records += 1;
        }

        // Only now is the layer proven; everything above was read on the
        // strength of the descriptor.
        let digests = reader.finish().unwrap();

        assert_eq!(digests.diff_id, descriptor.diff_id, "{}", descriptor.kind);
        assert_eq!(records, descriptor.records, "{}", descriptor.kind);
    }

    // Written twice, independently: same document, same bytes, same blobs. This
    // is the property the whole protocol rests on, now under a real profile.
    let second = tempfile::tempdir().unwrap();
    let (again, again_digest) = write_stele(second.path());

    assert_eq!(again, inscription);
    assert_eq!(again_digest, digest);
    assert_eq!(
        std::fs::read(first.path().join("inscription.json")).unwrap(),
        std::fs::read(second.path().join("inscription.json")).unwrap(),
    );

    let second_index = SteleDir::open(second.path()).unwrap().blob_index().unwrap();
    for descriptor in &inscription.layers {
        assert_eq!(
            index.blob_for(&descriptor.diff_id),
            second_index.blob_for(&descriptor.diff_id),
            "layer {:?}",
            descriptor.kind,
        );
    }
}

/// The descriptors a `SteleDir` writes are the numbers the per-kind goldens
/// pin — so the two tests cannot drift apart and quietly agree with themselves.
#[test]
fn descriptors_match_the_per_kind_goldens() {
    let temp = tempfile::tempdir().unwrap();
    let (inscription, _) = write_stele(temp.path());

    assert_eq!(
        inscription.layers.len(),
        GOLDEN_LAYERS.len(),
        "the stele no longer has the layers the goldens pin"
    );

    for (descriptor, (kind, diff_id, records, size)) in inscription.layers.iter().zip(GOLDEN_LAYERS)
    {
        assert_eq!(descriptor.kind, kind);
        assert_eq!(descriptor.diff_id.to_string(), diff_id);
        assert_eq!(descriptor.records, records);
        assert_eq!(descriptor.uncompressed_size, size);
    }
}

/// The canonical document itself, so a change to a key spelling, a media type
/// or an ordering shows up in the diff as text rather than only as a moved
/// hash.
#[test]
fn the_canonical_inscription_is_pinned() {
    let temp = tempfile::tempdir().unwrap();
    let (inscription, _) = write_stele(temp.path());

    let canonical = String::from_utf8(inscription.canonicalize().unwrap()).unwrap();

    assert_eq!(canonical, CANONICAL_INSCRIPTION);

    // The vendor slot is ours and the protocol's reserved one is nowhere near a
    // payload type.
    assert!(canonical.contains("application/vnd.dolos.stele.blocks.v1+zstd"));
    assert!(!canonical.contains("vnd.stelae.stele"));
}

/// The whole document, so the diff of a format change is readable.
///
/// JCS sorts object keys, so the layout below is the protocol's, not this
/// crate's — but every *string* in it is this profile's, and that is what the
/// literal is for.
const CANONICAL_INSCRIPTION: &str = concat!(
    r#"{"compression":{"algo":"zstd","level":9},"history":[{"inscriptionDigest":"sha256:5555555555555555555555555555555555555555555555555555555555555555","sequence":5},{"inscriptionDigest":"sha256:6666666666666666666666666666666666666666666666666666666666666666","sequence":6}],"layers":["#,
    r#"{"diffId":"sha256:14a05418723da3c0b4117b5f30ef07d96887b3e12eae114988ff299a654ff106","kind":"blocks","#,
    r#""mediaType":"application/vnd.dolos.stele.blocks.v1+zstd","records":4,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":167},"#,
    r#"{"diffId":"sha256:be485ff209757934d6336a169d8608b36fdbf11e98ac03a3ff33fd07cc50350c","kind":"indexes","#,
    r#""mediaType":"application/vnd.dolos.stele.indexes.v1+zstd","records":20,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":531},"#,
    r#"{"diffId":"sha256:be63f933f87028f2650741a6964a8a0e1bae9a12f73e012f0854a4c568a23055","kind":"log-account-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.log-account-epochs.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":102},"#,
    r#"{"diffId":"sha256:a42213234eaff408cfbf9cde9f43eb782b41726da3e144d497c576f4cabf37c4","kind":"log-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.log-epochs.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":94},"#,
    r#"{"diffId":"sha256:e650e8487b5827fc62a14428eeeb637f3d4ade3ba97cb5ec58465393ab503b9b","kind":"log-stakes","#,
    r#""mediaType":"application/vnd.dolos.stele.log-stakes.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":94},"#,
    r#"{"diffId":"sha256:9391e4ef7ca6c4b9413c21365423daa77571e89aaaedc5cfff6dd81655e90e11","kind":"state-account-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-account-epochs.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":93},"#,
    r#"{"diffId":"sha256:9946fd4c26d9cdb01865650503f94bd2c1cf8367b4a40e3a8f7cb0d65f5718eb","kind":"state-account-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-account-epochs.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":93},"#,
    r#"{"diffId":"sha256:f213e003211e343abf8047535fa67e6dfa0e715ddfa0053a1fbef5213d5fe191","kind":"state-accounts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-accounts.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":87},"#,
    r#"{"diffId":"sha256:9f56ac182d1d1793a9715475eca33c235902548e220c648ae84f53a4de241b24","kind":"state-accounts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-accounts.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":1},"uncompressedSize":87},"#,
    r#"{"diffId":"sha256:db47596747e81165ce2948f333cc6e6ab3c240f6973fb627df1d9b65dc50c8f2","kind":"state-accounts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-accounts.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":87},"#,
    r#"{"diffId":"sha256:0c7bc44f2f59b16a803369072e19011789cf4221dc94f409626e52d00f10eb1f","kind":"state-accounts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-accounts.v1+zstd","records":2,"#,
    r#""scope":{"shard":1},"uncompressedSize":87},"#,
    r#"{"diffId":"sha256:59c96281ae4d1b7cc14cf8b7bbd7629526e8f3b1f6abd27bec605c68b0617803","kind":"state-assets","#,
    r#""mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:0392762c0eb70060ef3e577b9f67350acc4a774bc819d4edc56ecd37d90e3830","kind":"state-assets","#,
    r#""mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":1},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:8077fd64690d954acc788df3b8046d151729ccd2b7d1dae42ed177f3931747aa","kind":"state-assets","#,
    r#""mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:c7c1ff92712bfa3bff88b9ce2445bb3f4e55139ac5bc89c989f41188f894eeff","kind":"state-assets","#,
    r#""mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":2,"#,
    r#""scope":{"shard":1},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:51ccdf0f0eaf485ef81ebbf89830d090b053fe3b4bdf22c7857791ccb32fcb1c","kind":"state-datums","#,
    r#""mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:403ed6f8e7d4c3b975577d534959ad446ed7282fbee2cde325ac08fdcac9f492","kind":"state-datums","#,
    r#""mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":1},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:41c9a9cba05c8bfc0d7e345b820f62d4fc3e16fa5deef8ac224f9422ae1c3bb0","kind":"state-datums","#,
    r#""mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:444ae56c151ffb66b8dd716e16b578d9ec4e339251ba24406d935b5214db27da","kind":"state-datums","#,
    r#""mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":2,"#,
    r#""scope":{"shard":1},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:69bae486654647dc4e56d7920c130002775679c5b3c4ac6b6a5d42ed3c7ee41a","kind":"state-dreps","#,
    r#""mediaType":"application/vnd.dolos.stele.state-dreps.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":84},"#,
    r#"{"diffId":"sha256:96f0a867e25607b374f34830c4499ae735a860486b866e740998c781ef0f29ca","kind":"state-dreps","#,
    r#""mediaType":"application/vnd.dolos.stele.state-dreps.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":84},"#,
    r#"{"diffId":"sha256:89594968aca68e2811574fda91f3da8531848cc0aaad64c6274290317ffeb909","kind":"state-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-epochs.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:93b0aab4fd80315a8bf106397e7575f58b031a8fe9680dac5d21bf84c880a23a","kind":"state-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-epochs.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:347420017e065a7c98a5e07a615d9bfbb7be305b67ef56a2cf0cd44486b5d8ff","kind":"state-eras","#,
    r#""mediaType":"application/vnd.dolos.stele.state-eras.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":83},"#,
    r#"{"diffId":"sha256:ad1c687b6b519d6d922764504a333d91733a98a251c606524532c6b066167648","kind":"state-eras","#,
    r#""mediaType":"application/vnd.dolos.stele.state-eras.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":83},"#,
    r#"{"diffId":"sha256:81afb0f36d765a73bdfd273069ae00b616911b611938bf5bcce2de8e2e79d1a3","kind":"state-gov","#,
    r#""mediaType":"application/vnd.dolos.stele.state-gov.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":82},"#,
    r#"{"diffId":"sha256:f569b56280287b58f66dc13c1f4c0b2f6895befb01399ec6d2c73e13a8679a97","kind":"state-gov","#,
    r#""mediaType":"application/vnd.dolos.stele.state-gov.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":82},"#,
    r#"{"diffId":"sha256:fe9b354a62a7007ceb3042f4f2acea5b9b530f16cc7344c3180a0d0b9f927652","kind":"state-metadata-labels","#,
    r#""mediaType":"application/vnd.dolos.stele.state-metadata-labels.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":94},"#,
    r#"{"diffId":"sha256:c2f8f9752e5a802d22b879abd2e6baf671322f9c6c0417cf37d115a69fb96f94","kind":"state-metadata-labels","#,
    r#""mediaType":"application/vnd.dolos.stele.state-metadata-labels.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":94},"#,
    r#"{"diffId":"sha256:113c0fe6fc088ab6ce78a74659824d2936e7afc1effd5a85a79ba286df66353c","kind":"state-pending-mirs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pending-mirs.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:71453eb3f5ac8d93bc365bed5cffa7cfca32445f1ec0d1756316f11028b67b6b","kind":"state-pending-mirs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pending-mirs.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:a233fd258cc3608e538ca46d15fd2a711b552fe7e60d0c18a044e1a9ce78b084","kind":"state-pending-rewards","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pending-rewards.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":94},"#,
    r#"{"diffId":"sha256:7e077dd045b29091691e94ffb7c60b7566a7f94b1e37a3d6c026b6b72a2fb871","kind":"state-pending-rewards","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pending-rewards.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":94},"#,
    r#"{"diffId":"sha256:fd25e6d56516d92c0d950a84542f18221aa064aae3535d1b34940f34d3127589","kind":"state-pools","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pools.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":84},"#,
    r#"{"diffId":"sha256:7fc0d7c8fac6b730d8dca30a6a8ad04ac7b562e22412ce4e8340c1fff226d6ec","kind":"state-pools","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pools.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":84},"#,
    r#"{"diffId":"sha256:33a21f13cfd63d79acab61e1af9e574066e114b765982e4fdf89103a71570597","kind":"state-proposals","#,
    r#""mediaType":"application/vnd.dolos.stele.state-proposals.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":88},"#,
    r#"{"diffId":"sha256:5dd164a8a7bbb494513300c6e108ee4235898105bbca37ac262ca92b88ed6bfb","kind":"state-proposals","#,
    r#""mediaType":"application/vnd.dolos.stele.state-proposals.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":88},"#,
    r#"{"diffId":"sha256:90ccd489027c6166ec5d269d3717fd9e2a918a499d07115aade1990c96200d92","kind":"state-stakes","#,
    r#""mediaType":"application/vnd.dolos.stele.state-stakes.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:8a24bf24ef897aa68bf9d276b2a627a256734f80c4a886a34d38606d5f02c7a3","kind":"state-stakes","#,
    r#""mediaType":"application/vnd.dolos.stele.state-stakes.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:b66979ba19b82c97de5031b923a09c8ebc26b4ba9d4d45477a84a905a2f6a752","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:c532bceb633a91dd6cc0b8789cf3170a55aaa6676b37229ed399bdc36378ab21","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":1},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:b882014a52055e12c9b4f1ef7c1aeddbdac5e4887146aab3b5b9f20f68b50d45","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:5b863d374031f390aa04299fc4b3527258401e5d438f1d6e422539aad0e29244","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"shard":1},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:13f9bbdf676ac47ad7238a52fa525f4413a88335c23d2d567c888abd8dedec80","kind":"digests","#,
    r#""mediaType":"application/vnd.dolos.stele.digests.v1+zstd","records":3,"#,
    r#""scope":{"lastImmutable":3},"uncompressedSize":250}"#,
    r#"],"parameters":{"#,
    r#""indexKeyHash":"xxh3-64","#,
    r#""schemas":{"account-epochs":1,"account-stakes":0,"accounts":1,"assets":1,"datums":1,"dreps":1,"epochs":2,"eras":1,"gov":2,"leader-rewards":0,"member-rewards":0,"metadata-labels":1,"pending_mirs":1,"pending_rewards":1,"pool-deposit-refunds":0,"pools":1,"proposals":1,"stakes":1,"utxos":1},"#,
    r#""shards":{"account-epochs":1,"accounts":16,"assets":16,"datums":16,"dreps":1,"epochs":1,"eras":1,"gov":1,"metadata-labels":1,"pending_mirs":1,"pending_rewards":1,"pools":1,"proposals":1,"stakes":1,"utxos":16},"#,
    r#""stateEpochs":[4]"#,
    r#"},"position":{"#,
    r#""epoch":7,"network":{"magic":764824073,"name":"mainnet"},"#,
    r#""point":{"hash":"0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b","slot":101}},"profile":{"name":"io.txpipe.dolos.cardano","version":1},"schema":1,"sequence":7}"#,
);
/// Decode one record with the codec its layer kind names, so the streaming pass
/// exercises every codec rather than only the framing underneath them.
fn decode_one(kind: &str, record: &[u8]) {
    // One codec for all six log kinds: they differ in which namespace they
    // carry, never in how a record is written.
    if log_ns_for(kind).is_some() {
        logs::decode(record).unwrap();

        return;
    }

    // Also, one codec decodes all fifteen state kinds, for the same reason. The
    // kind gives the namespace that the codec needs.
    if let Some(ns) = state_ns_for(kind) {
        state::decode(ns, record).unwrap();

        return;
    }

    match kind {
        BLOCKS => {
            blocks::decode(record).unwrap();
        }
        INDEXES => {
            indexes::decode(record).unwrap();
        }
        DIGESTS => {
            digests::decode(record).unwrap();
        }
        other => panic!("no codec for layer kind {other:?}"),
    }
}
