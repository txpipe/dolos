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
//! Between them these freeze: every media type, the tag string, every archive
//! dimension name and exact-record kind literal, every `log-{ns}` and
//! `state-{ns}` kind string — which is where the namespace strings live now
//! that neither record names one — the layer header and scope shapes, and the
//! `position`/`parameters` key spellings, the shard and schema maps included.

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
const GOLDEN_LAYERS: [(&str, &str, u64, u64); 56] = [
    (
        BLOCKS,
        "sha256:14a05418723da3c0b4117b5f30ef07d96887b3e12eae114988ff299a654ff106",
        4,
        167,
    ),
    (
        INDEXES,
        "sha256:a0179ae003b2f11abfb41885369b6cd1a6c8996630fa49aa3bed238a74d1b433",
        23,
        619,
    ),
    (
        "log-account-address-chunks",
        "sha256:620b0484b8c0d6e305c6c9746950c9f9dd186b256fb2febf030d0e75507b3fe3",
        2,
        111,
    ),
    (
        "log-account-epochs",
        "sha256:9cfebfa5dd85cadd44e59d0e51b26c3adeca347f0aea30188d96dbf1eeb22be7",
        2,
        102,
    ),
    (
        "log-account-stats",
        "sha256:e055e50725da443014dab37602e16a8b40bbefc64183ac7a9e77159ec952143f",
        2,
        101,
    ),
    (
        "log-address-stats",
        "sha256:de7b9f85d5fc130fb4ed732da9448572233907f4966e5ac435530669239c87aa",
        2,
        130,
    ),
    (
        "log-epochs",
        "sha256:4d2741b35a2fab1aa2d7ae27a46f6b8dc3792a5b2994bd519147c997c152b759",
        2,
        61,
    ),
    (
        "log-first-mint-chunks",
        "sha256:f0df9e4ec72d62a077a9b945dde15af39da05a274afe9b8c6d551ed3bee53c70",
        2,
        76,
    ),
    (
        "log-payment-address-chunks",
        "sha256:e7d1ec1ce8d6a02b7ec02ca9150d7d96a7b3d061e2803b2b5dee6718bb0025a4",
        2,
        107,
    ),
    (
        "log-policy-mint-chunks",
        "sha256:7005d3cb79df14ac4be36098487a8233c7b4fd2a6307a7e9e1ed71c92df487e8",
        2,
        106,
    ),
    (
        "log-pool-epochs",
        "sha256:debe14e7c5b6203eaaf553f3ea607ab4ae63c209a6aeb35626f6eb331edb6cb7",
        2,
        95,
    ),
    (
        "state-account-addresses",
        "sha256:9c4a51d9ef3969cea7075cf31eaadf82efc9bef6e5427296976201af4663d8e5",
        2,
        165,
    ),
    (
        "state-account-addresses",
        "sha256:5296f2db380326b740501e5706a980bf5dc8f96d8fc6b5c3e37f825ebc43b204",
        2,
        165,
    ),
    (
        "state-account-stats",
        "sha256:f83b581901dec97c2fb1d0a696a6c4d0606268229843f9edc0b60eb955f084aa",
        2,
        100,
    ),
    (
        "state-account-stats",
        "sha256:9b8ac7a591e85ccd8db28fed9a68a56f382b148eb103a3e368c3275a6ae50d18",
        2,
        100,
    ),
    (
        "state-accounts",
        "sha256:b83b89a357ff5ab3f738c2b3ef238fc757d11ec59103caa7518d008de458d7b3",
        2,
        87,
    ),
    (
        "state-accounts",
        "sha256:f0d22f5dba398a9dc1a019aadb87397abfe163d0c4c204c540379681cd25ed12",
        2,
        87,
    ),
    (
        "state-address-stats",
        "sha256:7e213415880a0204a489ba415e224c44f119e64c630417dbb18fbf1ba9188f5a",
        2,
        129,
    ),
    (
        "state-address-stats",
        "sha256:24f9fd95534a65b4545874d62635d49f1bcc8cafdc8d1ef4e096938a3759fc57",
        2,
        129,
    ),
    (
        "state-assets",
        "sha256:b3245ac0b24b08885c3b78227a3d2d7792a20c061ad22931e9de15cf576d7a98",
        2,
        85,
    ),
    (
        "state-assets",
        "sha256:c04849646e7500199fd2125a678e0792fb4afc38d14afb12222804ae50ef6ad1",
        2,
        85,
    ),
    (
        "state-assets",
        "sha256:0f51ce1103a235397b071363f2874b0ee8d719beb903a1d8deedace236c1304d",
        2,
        85,
    ),
    (
        "state-assets",
        "sha256:8409ca3904487f879e04b59ad925cc396ae5f33a0cea0f6fca055049e8d0e88a",
        2,
        85,
    ),
    (
        "state-datums",
        "sha256:c0636a49414a0d2364a34d1da00eafcf09c8c24e30fb0dc2e2ad7a617b5d2e26",
        2,
        85,
    ),
    (
        "state-datums",
        "sha256:17b8d6ecef148a188789a33a23cf9aaac8ef087ebb05134bbd713c5a6c830b96",
        2,
        85,
    ),
    (
        "state-datums",
        "sha256:f7e000ca94cd68d6d4e9a4c9d760f2285723e51f5efb226a9176516a2e6fd8ea",
        2,
        85,
    ),
    (
        "state-datums",
        "sha256:bf69b05259405f578d98a23b1265d840b681c42d240afc9338cb8733ff4a3a3d",
        2,
        85,
    ),
    (
        "state-drep-sightings",
        "sha256:5e881935a336c0d1aad058c0c327f0dd6ff798e01fd6b5dac3b4b46d854750b1",
        2,
        90,
    ),
    (
        "state-drep-sightings",
        "sha256:96bf8000ed6d4cab232dd0a10089317efb6ab51e50e787b9f3e8d83eaa0f7e22",
        2,
        90,
    ),
    (
        "state-dreps",
        "sha256:011662d797212f56055565bb4cbc1b4224a645e9dd43f1cd115e172489859c36",
        2,
        81,
    ),
    (
        "state-dreps",
        "sha256:9fc6fc602158a3144351fd59155f014441895a11d18f7bc576594730ab20b10f",
        2,
        81,
    ),
    (
        "state-epochs",
        "sha256:2aa585726488630bdfd1fece3da710a9d078514c956d82a715dd4fb141216059",
        2,
        52,
    ),
    (
        "state-epochs",
        "sha256:b1372257b8a307fe0e307b6524124a90dd2a8a6df1cbc0816aeb0b63a8a28d5f",
        2,
        52,
    ),
    (
        "state-eras",
        "sha256:b306b62162d3d0a26677b3daf878d9d37c7272d2056a25930a8f8a3d8c6f26eb",
        2,
        52,
    ),
    (
        "state-eras",
        "sha256:ee3065fd62ca004f25c7df9e66482543dbe674ca82df1bc52c09594563367d50",
        2,
        52,
    ),
    (
        "state-first-mints",
        "sha256:1f3d6d83cc5ddfced2149039a35693ae7abdcd2842a7b05918c478ef191bdbb0",
        2,
        126,
    ),
    (
        "state-first-mints",
        "sha256:aaa00ca5fac62084c741d80ee15e02f7f056c3c6e9039141a708ae2e4a365a86",
        2,
        126,
    ),
    (
        "state-gov",
        "sha256:e8bcd891cb9d5120c20648b3c8d526546fcef5df261eab91b663e59f3a6ede20",
        2,
        49,
    ),
    (
        "state-gov",
        "sha256:cf17dd090de2e6799655a20db2022b9620318f75e61744bb836ce3576448f7c3",
        2,
        49,
    ),
    (
        "state-payment-addresses",
        "sha256:10fa47bcf5b5b11e4848ab80af253a454a721913b34daed38d53f05952c623de",
        2,
        129,
    ),
    (
        "state-payment-addresses",
        "sha256:470500fda557d4521f0d2ae877af11010bc258fbf46083379d97af0e5093c44c",
        2,
        129,
    ),
    (
        "state-payment-stats",
        "sha256:eefcb0fb276ce626162bdc2b631ff5fc65657cc32e21d3597d53c874eb624629",
        2,
        96,
    ),
    (
        "state-payment-stats",
        "sha256:6c372f7b57b0e23d7583642f5618dab8908f11328aa2e5ae0a2a4c2aab01b83c",
        2,
        96,
    ),
    (
        "state-pending-mirs",
        "sha256:4cec186cb47eb09ce5489ff8546150fd41f5b487817abb49ff55ada3572171d3",
        2,
        91,
    ),
    (
        "state-pending-mirs",
        "sha256:7f369e6843fefe2ecd949ee2da2a1c50086d94dd1645d6198c82964b3aab5a9b",
        2,
        91,
    ),
    (
        "state-pending-rewards",
        "sha256:8d74321ce5a87bfaa60d6885e16e86f197fe13c67e5190ceb2cbcd78c3b09982",
        2,
        94,
    ),
    (
        "state-pending-rewards",
        "sha256:b5ae299d68e4fc2b4773d0307570ddaf129c005ab1f35cf558dcc572554b4d44",
        2,
        94,
    ),
    (
        "state-pools",
        "sha256:6e845f53cd243141f920fdc36fe5f9ee1cb2be501a98153db16d24962fb0b543",
        2,
        80,
    ),
    (
        "state-pools",
        "sha256:327aa3385702408af30bf99fa8598137b278a03c6b67ca50217f35dc9abe08d2",
        2,
        80,
    ),
    (
        "state-proposals",
        "sha256:3389a16c3b581637f22372c4bfd45e778d290e89f09c26ce9890c7867bd34aab",
        2,
        92,
    ),
    (
        "state-proposals",
        "sha256:5bc43efd7302846ff88da868937be52e6015feac1ebe4166ede7a95679de0a6e",
        2,
        92,
    ),
    (
        "state-utxos",
        "sha256:e60abc01ded3329c4dfc781f818abac9c4a44721a661a921904b447b96b0e67a",
        2,
        91,
    ),
    (
        "state-utxos",
        "sha256:d72696d06221a2dc469ba0ecd0492a78e1b1a102afac9893d4b559ff1476cd2b",
        2,
        91,
    ),
    (
        "state-utxos",
        "sha256:9930e9dd0df8fc542a072ce4db389e76dfb5b107319fa804599acb8936efc777",
        2,
        91,
    ),
    (
        "state-utxos",
        "sha256:def4e4cfd3554d491e93a75c2616b1c6b2fdee4d61d33993a37bd8c4ebf991e9",
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
    "sha256:b89f603cac9e2a68bee5d60ce182ce4ee8d5b237cf2f7ed75690cdb1dae2d273";

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
    r#"{"diffId":"sha256:a0179ae003b2f11abfb41885369b6cd1a6c8996630fa49aa3bed238a74d1b433","kind":"indexes","#,
    r#""mediaType":"application/vnd.dolos.stele.indexes.v1+zstd","records":23,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":619},"#,
    r#"{"diffId":"sha256:620b0484b8c0d6e305c6c9746950c9f9dd186b256fb2febf030d0e75507b3fe3","kind":"log-account-address-chunks","#,
    r#""mediaType":"application/vnd.dolos.stele.log-account-address-chunks.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":111},"#,
    r#"{"diffId":"sha256:9cfebfa5dd85cadd44e59d0e51b26c3adeca347f0aea30188d96dbf1eeb22be7","kind":"log-account-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.log-account-epochs.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":102},"#,
    r#"{"diffId":"sha256:e055e50725da443014dab37602e16a8b40bbefc64183ac7a9e77159ec952143f","kind":"log-account-stats","#,
    r#""mediaType":"application/vnd.dolos.stele.log-account-stats.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":101},"#,
    r#"{"diffId":"sha256:de7b9f85d5fc130fb4ed732da9448572233907f4966e5ac435530669239c87aa","kind":"log-address-stats","#,
    r#""mediaType":"application/vnd.dolos.stele.log-address-stats.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":130},"#,
    r#"{"diffId":"sha256:4d2741b35a2fab1aa2d7ae27a46f6b8dc3792a5b2994bd519147c997c152b759","kind":"log-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.log-epochs.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":61},"#,
    r#"{"diffId":"sha256:f0df9e4ec72d62a077a9b945dde15af39da05a274afe9b8c6d551ed3bee53c70","kind":"log-first-mint-chunks","#,
    r#""mediaType":"application/vnd.dolos.stele.log-first-mint-chunks.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":76},"#,
    r#"{"diffId":"sha256:e7d1ec1ce8d6a02b7ec02ca9150d7d96a7b3d061e2803b2b5dee6718bb0025a4","kind":"log-payment-address-chunks","#,
    r#""mediaType":"application/vnd.dolos.stele.log-payment-address-chunks.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":107},"#,
    r#"{"diffId":"sha256:7005d3cb79df14ac4be36098487a8233c7b4fd2a6307a7e9e1ed71c92df487e8","kind":"log-policy-mint-chunks","#,
    r#""mediaType":"application/vnd.dolos.stele.log-policy-mint-chunks.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":106},"#,
    r#"{"diffId":"sha256:debe14e7c5b6203eaaf553f3ea607ab4ae63c209a6aeb35626f6eb331edb6cb7","kind":"log-pool-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.log-pool-epochs.v1+zstd","records":2,"#,
    r#""scope":{"endSlot":101,"epoch":7,"startSlot":100},"uncompressedSize":95},"#,
    r#"{"diffId":"sha256:9c4a51d9ef3969cea7075cf31eaadf82efc9bef6e5427296976201af4663d8e5","kind":"state-account-addresses","#,
    r#""mediaType":"application/vnd.dolos.stele.state-account-addresses.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":165},"#,
    r#"{"diffId":"sha256:5296f2db380326b740501e5706a980bf5dc8f96d8fc6b5c3e37f825ebc43b204","kind":"state-account-addresses","#,
    r#""mediaType":"application/vnd.dolos.stele.state-account-addresses.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":165},"#,
    r#"{"diffId":"sha256:f83b581901dec97c2fb1d0a696a6c4d0606268229843f9edc0b60eb955f084aa","kind":"state-account-stats","#,
    r#""mediaType":"application/vnd.dolos.stele.state-account-stats.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":100},"#,
    r#"{"diffId":"sha256:9b8ac7a591e85ccd8db28fed9a68a56f382b148eb103a3e368c3275a6ae50d18","kind":"state-account-stats","#,
    r#""mediaType":"application/vnd.dolos.stele.state-account-stats.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":100},"#,
    r#"{"diffId":"sha256:b83b89a357ff5ab3f738c2b3ef238fc757d11ec59103caa7518d008de458d7b3","kind":"state-accounts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-accounts.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":8},"uncompressedSize":87},"#,
    r#"{"diffId":"sha256:f0d22f5dba398a9dc1a019aadb87397abfe163d0c4c204c540379681cd25ed12","kind":"state-accounts","#,
    r#""mediaType":"application/vnd.dolos.stele.state-accounts.v1+zstd","records":2,"#,
    r#""scope":{"shard":8},"uncompressedSize":87},"#,
    r#"{"diffId":"sha256:7e213415880a0204a489ba415e224c44f119e64c630417dbb18fbf1ba9188f5a","kind":"state-address-stats","#,
    r#""mediaType":"application/vnd.dolos.stele.state-address-stats.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":129},"#,
    r#"{"diffId":"sha256:24f9fd95534a65b4545874d62635d49f1bcc8cafdc8d1ef4e096938a3759fc57","kind":"state-address-stats","#,
    r#""mediaType":"application/vnd.dolos.stele.state-address-stats.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":129},"#,
    r#"{"diffId":"sha256:b3245ac0b24b08885c3b78227a3d2d7792a20c061ad22931e9de15cf576d7a98","kind":"state-assets","#,
    r#""mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:c04849646e7500199fd2125a678e0792fb4afc38d14afb12222804ae50ef6ad1","kind":"state-assets","#,
    r#""mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":1},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:0f51ce1103a235397b071363f2874b0ee8d719beb903a1d8deedace236c1304d","kind":"state-assets","#,
    r#""mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:8409ca3904487f879e04b59ad925cc396ae5f33a0cea0f6fca055049e8d0e88a","kind":"state-assets","#,
    r#""mediaType":"application/vnd.dolos.stele.state-assets.v1+zstd","records":2,"#,
    r#""scope":{"shard":1},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:c0636a49414a0d2364a34d1da00eafcf09c8c24e30fb0dc2e2ad7a617b5d2e26","kind":"state-datums","#,
    r#""mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:17b8d6ecef148a188789a33a23cf9aaac8ef087ebb05134bbd713c5a6c830b96","kind":"state-datums","#,
    r#""mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":1},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:f7e000ca94cd68d6d4e9a4c9d760f2285723e51f5efb226a9176516a2e6fd8ea","kind":"state-datums","#,
    r#""mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:bf69b05259405f578d98a23b1265d840b681c42d240afc9338cb8733ff4a3a3d","kind":"state-datums","#,
    r#""mediaType":"application/vnd.dolos.stele.state-datums.v1+zstd","records":2,"#,
    r#""scope":{"shard":1},"uncompressedSize":85},"#,
    r#"{"diffId":"sha256:5e881935a336c0d1aad058c0c327f0dd6ff798e01fd6b5dac3b4b46d854750b1","kind":"state-drep-sightings","#,
    r#""mediaType":"application/vnd.dolos.stele.state-drep-sightings.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":90},"#,
    r#"{"diffId":"sha256:96bf8000ed6d4cab232dd0a10089317efb6ab51e50e787b9f3e8d83eaa0f7e22","kind":"state-drep-sightings","#,
    r#""mediaType":"application/vnd.dolos.stele.state-drep-sightings.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":90},"#,
    r#"{"diffId":"sha256:011662d797212f56055565bb4cbc1b4224a645e9dd43f1cd115e172489859c36","kind":"state-dreps","#,
    r#""mediaType":"application/vnd.dolos.stele.state-dreps.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":81},"#,
    r#"{"diffId":"sha256:9fc6fc602158a3144351fd59155f014441895a11d18f7bc576594730ab20b10f","kind":"state-dreps","#,
    r#""mediaType":"application/vnd.dolos.stele.state-dreps.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":81},"#,
    r#"{"diffId":"sha256:2aa585726488630bdfd1fece3da710a9d078514c956d82a715dd4fb141216059","kind":"state-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-epochs.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":52},"#,
    r#"{"diffId":"sha256:b1372257b8a307fe0e307b6524124a90dd2a8a6df1cbc0816aeb0b63a8a28d5f","kind":"state-epochs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-epochs.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":52},"#,
    r#"{"diffId":"sha256:b306b62162d3d0a26677b3daf878d9d37c7272d2056a25930a8f8a3d8c6f26eb","kind":"state-eras","#,
    r#""mediaType":"application/vnd.dolos.stele.state-eras.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":52},"#,
    r#"{"diffId":"sha256:ee3065fd62ca004f25c7df9e66482543dbe674ca82df1bc52c09594563367d50","kind":"state-eras","#,
    r#""mediaType":"application/vnd.dolos.stele.state-eras.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":52},"#,
    r#"{"diffId":"sha256:1f3d6d83cc5ddfced2149039a35693ae7abdcd2842a7b05918c478ef191bdbb0","kind":"state-first-mints","#,
    r#""mediaType":"application/vnd.dolos.stele.state-first-mints.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":126},"#,
    r#"{"diffId":"sha256:aaa00ca5fac62084c741d80ee15e02f7f056c3c6e9039141a708ae2e4a365a86","kind":"state-first-mints","#,
    r#""mediaType":"application/vnd.dolos.stele.state-first-mints.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":126},"#,
    r#"{"diffId":"sha256:e8bcd891cb9d5120c20648b3c8d526546fcef5df261eab91b663e59f3a6ede20","kind":"state-gov","#,
    r#""mediaType":"application/vnd.dolos.stele.state-gov.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:cf17dd090de2e6799655a20db2022b9620318f75e61744bb836ce3576448f7c3","kind":"state-gov","#,
    r#""mediaType":"application/vnd.dolos.stele.state-gov.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":49},"#,
    r#"{"diffId":"sha256:10fa47bcf5b5b11e4848ab80af253a454a721913b34daed38d53f05952c623de","kind":"state-payment-addresses","#,
    r#""mediaType":"application/vnd.dolos.stele.state-payment-addresses.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":129},"#,
    r#"{"diffId":"sha256:470500fda557d4521f0d2ae877af11010bc258fbf46083379d97af0e5093c44c","kind":"state-payment-addresses","#,
    r#""mediaType":"application/vnd.dolos.stele.state-payment-addresses.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":129},"#,
    r#"{"diffId":"sha256:eefcb0fb276ce626162bdc2b631ff5fc65657cc32e21d3597d53c874eb624629","kind":"state-payment-stats","#,
    r#""mediaType":"application/vnd.dolos.stele.state-payment-stats.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":96},"#,
    r#"{"diffId":"sha256:6c372f7b57b0e23d7583642f5618dab8908f11328aa2e5ae0a2a4c2aab01b83c","kind":"state-payment-stats","#,
    r#""mediaType":"application/vnd.dolos.stele.state-payment-stats.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":96},"#,
    r#"{"diffId":"sha256:4cec186cb47eb09ce5489ff8546150fd41f5b487817abb49ff55ada3572171d3","kind":"state-pending-mirs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pending-mirs.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:7f369e6843fefe2ecd949ee2da2a1c50086d94dd1645d6198c82964b3aab5a9b","kind":"state-pending-mirs","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pending-mirs.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:8d74321ce5a87bfaa60d6885e16e86f197fe13c67e5190ceb2cbcd78c3b09982","kind":"state-pending-rewards","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pending-rewards.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":94},"#,
    r#"{"diffId":"sha256:b5ae299d68e4fc2b4773d0307570ddaf129c005ab1f35cf558dcc572554b4d44","kind":"state-pending-rewards","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pending-rewards.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":94},"#,
    r#"{"diffId":"sha256:6e845f53cd243141f920fdc36fe5f9ee1cb2be501a98153db16d24962fb0b543","kind":"state-pools","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pools.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":80},"#,
    r#"{"diffId":"sha256:327aa3385702408af30bf99fa8598137b278a03c6b67ca50217f35dc9abe08d2","kind":"state-pools","#,
    r#""mediaType":"application/vnd.dolos.stele.state-pools.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":80},"#,
    r#"{"diffId":"sha256:3389a16c3b581637f22372c4bfd45e778d290e89f09c26ce9890c7867bd34aab","kind":"state-proposals","#,
    r#""mediaType":"application/vnd.dolos.stele.state-proposals.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":92},"#,
    r#"{"diffId":"sha256:5bc43efd7302846ff88da868937be52e6015feac1ebe4166ede7a95679de0a6e","kind":"state-proposals","#,
    r#""mediaType":"application/vnd.dolos.stele.state-proposals.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":92},"#,
    r#"{"diffId":"sha256:e60abc01ded3329c4dfc781f818abac9c4a44721a661a921904b447b96b0e67a","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:d72696d06221a2dc469ba0ecd0492a78e1b1a102afac9893d4b559ff1476cd2b","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"epoch":4,"shard":1},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:9930e9dd0df8fc542a072ce4db389e76dfb5b107319fa804599acb8936efc777","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"shard":0},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:def4e4cfd3554d491e93a75c2616b1c6b2fdee4d61d33993a37bd8c4ebf991e9","kind":"state-utxos","#,
    r#""mediaType":"application/vnd.dolos.stele.state-utxos.v1+zstd","records":2,"#,
    r#""scope":{"shard":1},"uncompressedSize":91},"#,
    r#"{"diffId":"sha256:13f9bbdf676ac47ad7238a52fa525f4413a88335c23d2d567c888abd8dedec80","kind":"digests","#,
    r#""mediaType":"application/vnd.dolos.stele.digests.v1+zstd","records":3,"#,
    r#""scope":{"lastImmutable":3},"uncompressedSize":250}"#,
    r#"],"parameters":{"#,
    r#""indexKeyHash":"xxh3-64","#,
    r#""schemas":{"account-address-chunks":1,"account-addresses":1,"account-epochs":1,"account-stakes":0,"account-stats":1,"accounts":1,"address-stats":1,"assets":1,"datums":1,"drep-sightings":1,"dreps":3,"epochs":3,"eras":2,"first-mint-chunks":2,"first-mints":2,"gov":4,"leader-rewards":0,"member-rewards":0,"payment-address-chunks":1,"payment-addresses":1,"payment-stats":1,"pending-mirs":1,"pending-rewards":1,"policy-mint-chunks":1,"pool-deposit-refunds":0,"pool-epochs":2,"pools":2,"proposals":4,"utxos":1},"#,
    r#""shards":{"account-addresses":1,"account-stats":1,"accounts":16,"address-stats":1,"assets":16,"datums":16,"drep-sightings":1,"dreps":1,"epochs":1,"eras":1,"first-mints":1,"gov":1,"payment-addresses":1,"payment-stats":1,"pending-mirs":1,"pending-rewards":1,"pools":1,"proposals":1,"utxos":16},"#,
    r#""stateEpochs":[4]"#,
    r#"},"position":{"#,
    r#""epoch":7,"network":{"magic":764824073,"name":"mainnet"},"#,
    r#""point":{"hash":"0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b","slot":101}},"profile":{"name":"io.txpipe.dolos.cardano","version":1},"schema":1,"sequence":7}"#,
);
/// Decode one record with the codec its layer kind names, so the streaming pass
/// exercises every codec rather than only the framing underneath them.
fn decode_one(kind: &str, record: &[u8]) {
    // One codec for every log kind: they differ in which namespace they
    // carry, never in how a record is written.
    if let Some(ns) = log_ns_for(kind) {
        logs::decode(ns, record).unwrap();

        return;
    }

    // And one codec for every state kind, for the same reason — the
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
