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
//! Between them these freeze: the twenty-eight media types, the tag string, the
//! seventeen archive dimension names, the three exact-record kind literals, the
//! six `log-{ns}` and nineteen `state-{ns}` kind strings — which is where the
//! nineteen namespace strings live now that neither record names one — the
//! layer header and scope shapes, and the `position`/`parameters` key
//! spellings, the shard and schema maps included.

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
const GOLDEN_LAYERS: [(&str, &str, u64, u64); 53] = [
    (
        BLOCKS,
        "sha256:14a05418723da3c0b4117b5f30ef07d96887b3e12eae114988ff299a654ff106",
        4,
        167,
    ),
    (
        INDEXES,
        "sha256:a792d43a501efd106665c365f4e4281e142c6b9d78c6425176e5b4803ed2ac20",
        21,
        561,
    ),
    (
        "log-account-epochs",
        "sha256:3df20da5471b0991ffdaf69fa45f759fb8341c2d4e2d751fd5cba6ee8827c6b8",
        2,
        102,
    ),
    (
        "log-epochs",
        "sha256:729240832f261fab7560117cdd0be07c46577949f83d6b3c2d1a50b76daaacba",
        2,
        61,
    ),
    (
        "log-first-mints",
        "sha256:31dbb98dea598bf2a6004a2b069df8d96b1c28dc5b6e445941c690a7b21fc2c9",
        2,
        70,
    ),
    (
        "log-policy-first-mints",
        "sha256:aa3c5ce677eae1486956ec550a9341a3969cc745a2a094f9337ae29370908722",
        2,
        106,
    ),
    (
        "log-stake-addresses",
        "sha256:f92a9da68fed12295a18788dbafc148cbe714d05400a1f8e93fcca4894bd4b6a",
        2,
        103,
    ),
    (
        "log-stakes",
        "sha256:5c8712d4326c48346233b9c53d56017a148b599c3435cdc644136364f9a91c84",
        2,
        90,
    ),
    (
        "state-account-epochs",
        "sha256:90fab9f2698941985ddd5698d1d3972d0e606fd25cc3f1e51dc971fd16e2b491",
        2,
        93,
    ),
    (
        "state-account-epochs",
        "sha256:10e8e314782e69e65bd28175b0a3c1bedc70e6d30a5ee3d9e124046eb0da199f",
        2,
        93,
    ),
    (
        "state-accounts",
        "sha256:77189982f593e0fa32e54b0838069f98d8e84ec18ba08298a8d083d82b22ba21",
        2,
        87,
    ),
    (
        "state-accounts",
        "sha256:0ddf124266eeb0fc7fe6a986aef342cc37a928e5bb58e25f2d9a1142dfa5968e",
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
        "sha256:07eae967d58520d5369b11373bcd85e0833c23c71764f57338d12f3dc6695d33",
        2,
        81,
    ),
    (
        "state-dreps",
        "sha256:ae652a36f0a5613fb472641ec7e449e20dff25c7046bde6d5351b9d71ae57eec",
        2,
        81,
    ),
    (
        "state-epochs",
        "sha256:5b604db3f3a8e42d1715d952cf0e78eb105648a403e2fee213e49a2820eabb02",
        2,
        52,
    ),
    (
        "state-epochs",
        "sha256:7b025ece6a8952907bb649682334e98792da909abeacdb0860dadbd78ad9633b",
        2,
        52,
    ),
    (
        "state-eras",
        "sha256:6d9f25b9d117f3caa7214232d7bf7400e82fafb1d5c11ba12cce5b9a75470c4d",
        2,
        52,
    ),
    (
        "state-eras",
        "sha256:1573df446f633c129e58d3313217dedf5471466e0e5bae26860ca7afb292623d",
        2,
        52,
    ),
    (
        "state-first-mints",
        "sha256:7e08af09fa77a04c3d4034c9d61de64b5ac55c8f530ead1e22d78c4e30809c20",
        2,
        61,
    ),
    (
        "state-first-mints",
        "sha256:a2313782013b8e50106b61510a905892686ab7d9c1e2bd84e630d6c5a2d89c1b",
        2,
        61,
    ),
    (
        "state-gov",
        "sha256:97a19103a59982672d714cae6e4f1c2db1dc2a46fb1901825571b8bc9c8f6da7",
        2,
        49,
    ),
    (
        "state-gov",
        "sha256:642db5912192f2b660d4baae1cd3a1000a8d037ecc4935912487bd241fdc0cda",
        2,
        49,
    ),
    (
        "state-pending-mirs",
        "sha256:a94832233e82e2bcf23e4377980d11c1b2bc946e3c0e313cd2b6cf154a2582f4",
        2,
        91,
    ),
    (
        "state-pending-mirs",
        "sha256:88c72fc64b5ec615fdcfe52e22738d1374d99ef9ab8d3f967f54d18f1be9f0ee",
        2,
        91,
    ),
    (
        "state-pending-rewards",
        "sha256:c4d50a2c72733d04c5921fd13bc80551d065b693ff5925f3c9fc879e04fe6a85",
        2,
        94,
    ),
    (
        "state-pending-rewards",
        "sha256:c0ceefb4f25d77d0f50c532d95b833ad7916a4e22ce39f52e9e6bbc4393961f9",
        2,
        94,
    ),
    (
        "state-policy-first-mints",
        "sha256:5922f50a8e3cdc0b04b861ac729f9a33d36ce292ec049e926ce34c8a09da0e2d",
        2,
        98,
    ),
    (
        "state-policy-first-mints",
        "sha256:ecf7f9d3a84740e18b5742c676212006b012751b0ec5d942613ea14ecdb1c39d",
        2,
        98,
    ),
    (
        "state-pools",
        "sha256:f4c28cb45439937a57a914938cb62d313a438fa0d8d78207e0b22cc95fad1116",
        2,
        80,
    ),
    (
        "state-pools",
        "sha256:86896b29621619528d6de2b1ff0597a1d6c70a8b10eed4ab079f3af12aebc9c2",
        2,
        80,
    ),
    (
        "state-proposals",
        "sha256:f3e0a22826e627f7d6bb4d55b0d32b281e5343ee1cf558ab396c0aa645ad1b72",
        2,
        92,
    ),
    (
        "state-proposals",
        "sha256:b90882c88820aee7e7fd5174f465c691eecdf71ce09f4ccff1bc895595ce9e8c",
        2,
        92,
    ),
    (
        "state-staged-first-mints",
        "sha256:2c3e3400d42098aef22d846aa4dc51cd5f19bd6d0ccd39b56178728fdb14b864",
        2,
        134,
    ),
    (
        "state-staged-first-mints",
        "sha256:6de7ffbfaf07a73451fd4ddd568e7f76227cae6fa2f8a069bb26070056e6046c",
        2,
        134,
    ),
    (
        "state-staged-stake-addresses",
        "sha256:4b0705c99022a97547a8355290f20e7c1d81cfd8974d79c0d2d43315887803b4",
        2,
        167,
    ),
    (
        "state-staged-stake-addresses",
        "sha256:8da205d2e748c420093ebdb0d43566cb233a9651953bbd953c13f691706bc928",
        2,
        167,
    ),
    (
        "state-stake-addresses",
        "sha256:9d0f0cbb57182647da8dde31c52e7ce06c16b8e884449b45c22e2e0ae595bebe",
        2,
        94,
    ),
    (
        "state-stake-addresses",
        "sha256:2ea586f2f3293b4eb11bc631f38f9fb839a8963c92328c1db3605a57251c5e2a",
        2,
        94,
    ),
    (
        "state-stakes",
        "sha256:63d50f07ff2a528ae715dc75b5f5b2998c33041d1ae43006ec9f9f7b4b7aac9c",
        2,
        81,
    ),
    (
        "state-stakes",
        "sha256:4a7f93cc53fe5955d07e83851a80a7dea1057c2da0dbf4599e60cd1d90564f32",
        2,
        81,
    ),
    (
        "state-utxos",
        "sha256:34871099b0d3972e774f974b0ff15a5425bacca81d7c8365776d456d570ddc2e",
        2,
        91,
    ),
    (
        "state-utxos",
        "sha256:a781669519aedd65081ef36f385e490e7f6eef989b5199f3368470c57fb04b90",
        2,
        91,
    ),
    (
        "state-utxos",
        "sha256:0bb8be8053d2e0d5d754064c9a9d1a0a4c851a32b4a8c2b2ee7b7a4c7e604648",
        2,
        91,
    ),
    (
        "state-utxos",
        "sha256:c213ded17f52f619bb3f0907b06d585f995b2e86d57ba50d1ab9eb493078cedf",
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
    "sha256:15f54fcdbe70b999b9c2e2b1d8b074240aae65188e8f10f51f6435796a0a8ffa";

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

/// Write the whole fixture stele into `root`: its layers and an inscription.
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
    r#"{"diffId":"sha256:a792d43a501efd106665c365f4e4281e142c6b9d78c6425176e5b4803ed2ac20","kind":"indexes","#,
    r#""mediaType":"application/vnd.dolos.stele.indexes.v1+zstd","records":21,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":561},"#,
    r#"{"diffId":"sha256:3df20da5471b0991ffdaf69fa45f759fb8341c2d4e2d751fd5cba6ee8827c6b8","kind":"log-account-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.log-account-epochs.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":102},"#,
    r#"{"diffId":"sha256:729240832f261fab7560117cdd0be07c46577949f83d6b3c2d1a50b76daaacba","kind":"log-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.log-epochs.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":61},"#,
    r#"{"diffId":"sha256:31dbb98dea598bf2a6004a2b069df8d96b1c28dc5b6e445941c690a7b21fc2c9","kind":"log-first-mints","#,
    r#""mediaType":"application/vnd.dolos.stele.log-first-mints.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":70},"#,
    r#"{"diffId":"sha256:aa3c5ce677eae1486956ec550a9341a3969cc745a2a094f9337ae29370908722","kind":"log-policy-first-mints","#,
    r#""mediaType":"application/vnd.dolos.stele.log-policy-first-mints.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":106},"#,
    r#"{"diffId":"sha256:f92a9da68fed12295a18788dbafc148cbe714d05400a1f8e93fcca4894bd4b6a","kind":"log-stake-addresses","#,
    r#""mediaType":"application/vnd.dolos.stele.log-stake-addresses.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":103},"#,
    r#"{"diffId":"sha256:5c8712d4326c48346233b9c53d56017a148b599c3435cdc644136364f9a91c84","kind":"log-stakes","#,
    r#""mediaType":"application/vnd.dolos.stele.log-stakes.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":90},"#,
    r#"{"diffId":"sha256:90fab9f2698941985ddd5698d1d3972d0e606fd25cc3f1e51dc971fd16e2b491","kind":"state-account-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-account-epochs.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":93},"#,
    r#"{"diffId":"sha256:10e8e314782e69e65bd28175b0a3c1bedc70e6d30a5ee3d9e124046eb0da199f","kind":"state-account-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-account-epochs.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":93},"#,
    r#"{"diffId":"sha256:77189982f593e0fa32e54b0838069f98d8e84ec18ba08298a8d083d82b22ba21","kind":"state-accounts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-accounts.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":8},"uncompressedSize":87},"#,
    r#"{"diffId":"sha256:0ddf124266eeb0fc7fe6a986aef342cc37a928e5bb58e25f2d9a1142dfa5968e","kind":"state-accounts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-accounts.v1+zstd","records":2,"#,
    r#""scope":{"shard":8},"uncompressedSize":87},"#,
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
    r#"{"diffId":"sha256:07eae967d58520d5369b11373bcd85e0833c23c71764f57338d12f3dc6695d33","kind":"state-dreps","#,
    r#""mediaType":"application/vnd.dolos.stele.state-dreps.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":81},"#,
    r#"{"diffId":"sha256:ae652a36f0a5613fb472641ec7e449e20dff25c7046bde6d5351b9d71ae57eec","kind":"state-dreps","#,
    r#""mediaType":"application/vnd.dolos.stele.state-dreps.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":81},"#,
    r#"{"diffId":"sha256:5b604db3f3a8e42d1715d952cf0e78eb105648a403e2fee213e49a2820eabb02","kind":"state-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-epochs.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":52},"#,
    r#"{"diffId":"sha256:7b025ece6a8952907bb649682334e98792da909abeacdb0860dadbd78ad9633b","kind":"state-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-epochs.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":52},"#,
    r#"{"diffId":"sha256:6d9f25b9d117f3caa7214232d7bf7400e82fafb1d5c11ba12cce5b9a75470c4d","kind":"state-eras","#,
    r#""mediaType":"application/vnd.dolos.stele.state-eras.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":52},"#,
    r#"{"diffId":"sha256:1573df446f633c129e58d3313217dedf5471466e0e5bae26860ca7afb292623d","kind":"state-eras","#,
    r#""mediaType":"application/vnd.dolos.stele.state-eras.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":52},"#,
    r#"{"diffId":"sha256:7e08af09fa77a04c3d4034c9d61de64b5ac55c8f530ead1e22d78c4e30809c20","kind":"state-first-mints","#,
    r#""mediaType":"application/vnd.dolos.stele.state-first-mints.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":61},"#,
    r#"{"diffId":"sha256:a2313782013b8e50106b61510a905892686ab7d9c1e2bd84e630d6c5a2d89c1b","kind":"state-first-mints","#,
    r#""mediaType":"application/vnd.dolos.stele.state-first-mints.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":61},"#,
    r#"{"diffId":"sha256:97a19103a59982672d714cae6e4f1c2db1dc2a46fb1901825571b8bc9c8f6da7","kind":"state-gov","#,
    r#""mediaType":"application/vnd.dolos.stele.state-gov.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:642db5912192f2b660d4baae1cd3a1000a8d037ecc4935912487bd241fdc0cda","kind":"state-gov","#,
    r#""mediaType":"application/vnd.dolos.stele.state-gov.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:a94832233e82e2bcf23e4377980d11c1b2bc946e3c0e313cd2b6cf154a2582f4","kind":"state-pending-mirs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pending-mirs.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:88c72fc64b5ec615fdcfe52e22738d1374d99ef9ab8d3f967f54d18f1be9f0ee","kind":"state-pending-mirs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pending-mirs.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:c4d50a2c72733d04c5921fd13bc80551d065b693ff5925f3c9fc879e04fe6a85","kind":"state-pending-rewards","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pending-rewards.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":94},"#,
    r#"{"diffId":"sha256:c0ceefb4f25d77d0f50c532d95b833ad7916a4e22ce39f52e9e6bbc4393961f9","kind":"state-pending-rewards","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pending-rewards.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":94},"#,
    r#"{"diffId":"sha256:5922f50a8e3cdc0b04b861ac729f9a33d36ce292ec049e926ce34c8a09da0e2d","kind":"state-policy-first-mints","#,
    r#""mediaType":"application/vnd.dolos.stele.state-policy-first-mints.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":98},"#,
    r#"{"diffId":"sha256:ecf7f9d3a84740e18b5742c676212006b012751b0ec5d942613ea14ecdb1c39d","kind":"state-policy-first-mints","#,
    r#""mediaType":"application/vnd.dolos.stele.state-policy-first-mints.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":98},"#,
    r#"{"diffId":"sha256:f4c28cb45439937a57a914938cb62d313a438fa0d8d78207e0b22cc95fad1116","kind":"state-pools","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pools.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":80},"#,
    r#"{"diffId":"sha256:86896b29621619528d6de2b1ff0597a1d6c70a8b10eed4ab079f3af12aebc9c2","kind":"state-pools","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pools.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":80},"#,
    r#"{"diffId":"sha256:f3e0a22826e627f7d6bb4d55b0d32b281e5343ee1cf558ab396c0aa645ad1b72","kind":"state-proposals","#,
    r#""mediaType":"application/vnd.dolos.stele.state-proposals.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":92},"#,
    r#"{"diffId":"sha256:b90882c88820aee7e7fd5174f465c691eecdf71ce09f4ccff1bc895595ce9e8c","kind":"state-proposals","#,
    r#""mediaType":"application/vnd.dolos.stele.state-proposals.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":92},"#,
    r#"{"diffId":"sha256:2c3e3400d42098aef22d846aa4dc51cd5f19bd6d0ccd39b56178728fdb14b864","kind":"state-staged-first-mints","#,
    r#""mediaType":"application/vnd.dolos.stele.state-staged-first-mints.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":134},"#,
    r#"{"diffId":"sha256:6de7ffbfaf07a73451fd4ddd568e7f76227cae6fa2f8a069bb26070056e6046c","kind":"state-staged-first-mints","#,
    r#""mediaType":"application/vnd.dolos.stele.state-staged-first-mints.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":134},"#,
    r#"{"diffId":"sha256:4b0705c99022a97547a8355290f20e7c1d81cfd8974d79c0d2d43315887803b4","kind":"state-staged-stake-addresses","#,
    r#""mediaType":"application/vnd.dolos.stele.state-staged-stake-addresses.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":167},"#,
    r#"{"diffId":"sha256:8da205d2e748c420093ebdb0d43566cb233a9651953bbd953c13f691706bc928","kind":"state-staged-stake-addresses","#,
    r#""mediaType":"application/vnd.dolos.stele.state-staged-stake-addresses.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":167},"#,
    r#"{"diffId":"sha256:9d0f0cbb57182647da8dde31c52e7ce06c16b8e884449b45c22e2e0ae595bebe","kind":"state-stake-addresses","#,
    r#""mediaType":"application/vnd.dolos.stele.state-stake-addresses.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":94},"#,
    r#"{"diffId":"sha256:2ea586f2f3293b4eb11bc631f38f9fb839a8963c92328c1db3605a57251c5e2a","kind":"state-stake-addresses","#,
    r#""mediaType":"application/vnd.dolos.stele.state-stake-addresses.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":94},"#,
    r#"{"diffId":"sha256:63d50f07ff2a528ae715dc75b5f5b2998c33041d1ae43006ec9f9f7b4b7aac9c","kind":"state-stakes","#,
    r#""mediaType":"application/vnd.dolos.stele.state-stakes.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":81},"#,
    r#"{"diffId":"sha256:4a7f93cc53fe5955d07e83851a80a7dea1057c2da0dbf4599e60cd1d90564f32","kind":"state-stakes","#,
    r#""mediaType":"application/vnd.dolos.stele.state-stakes.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":81},"#,
    r#"{"diffId":"sha256:34871099b0d3972e774f974b0ff15a5425bacca81d7c8365776d456d570ddc2e","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:a781669519aedd65081ef36f385e490e7f6eef989b5199f3368470c57fb04b90","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":1},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:0bb8be8053d2e0d5d754064c9a9d1a0a4c851a32b4a8c2b2ee7b7a4c7e604648","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:c213ded17f52f619bb3f0907b06d585f995b2e86d57ba50d1ab9eb493078cedf","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"shard":1},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:13f9bbdf676ac47ad7238a52fa525f4413a88335c23d2d567c888abd8dedec80","kind":"digests","#,
    r#""mediaType":"application/vnd.dolos.stele.digests.v1+zstd","records":3,"#,
    r#""scope":{"lastImmutable":3},"uncompressedSize":250}"#,
    r#"],"parameters":{"#,
    r#""indexKeyHash":"xxh3-64","#,
    r#""schemas":{"account-epochs":1,"account-stakes":0,"accounts":1,"assets":1,"datums":1,"dreps":2,"epochs":3,"eras":2,"first-mints":2,"gov":4,"leader-rewards":0,"member-rewards":0,"pending_mirs":1,"pending_rewards":1,"policy-first-mints":1,"pool-deposit-refunds":0,"pools":2,"proposals":4,"staged-first-mints":2,"staged-stake-addresses":1,"stake-addresses":1,"stakes":2,"utxos":1},"#,
    r#""shards":{"account-epochs":1,"accounts":16,"assets":16,"datums":16,"dreps":1,"epochs":1,"eras":1,"first-mints":1,"gov":1,"pending_mirs":1,"pending_rewards":1,"policy-first-mints":1,"pools":1,"proposals":1,"staged-first-mints":1,"staged-stake-addresses":1,"stake-addresses":1,"stakes":1,"utxos":16},"#,
    r#""stateEpochs":[4]"#,
    r#"},"position":{"#,
    r#""epoch":7,"network":{"magic":764824073,"name":"mainnet"},"#,
    r#""point":{"hash":"0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b","slot":101}},"profile":{"name":"io.txpipe.dolos.cardano","version":1},"schema":1,"sequence":7}"#,
);
/// Decode one record with the codec its layer kind names, so the streaming pass
/// exercises every codec rather than only the framing underneath them.
fn decode_one(kind: &str, record: &[u8]) {
    // One codec for all five log kinds: they differ in which namespace they
    // carry, never in how a record is written.
    if let Some(ns) = log_ns_for(kind) {
        logs::decode(ns, record).unwrap();

        return;
    }

    // And one codec for all seventeen state kinds, for the same reason — the
    // namespace it needs is the one the kind names.
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
