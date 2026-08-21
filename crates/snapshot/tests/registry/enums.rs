//! The variant tables: the contract's hard edge.
//!
//! Numbered *fields* evolve tolerantly — a decoder skips one it does not know
//! and defaults one that is missing. Numbered *variants* do not: a minicbor
//! enum refuses an index it has never heard of, whatever the field policy
//! says. So within a media-type version, **adding a variant to any enum
//! reachable from a record is reader-breaking**: it needs a media-type version
//! bump on that namespace's kind, or an explicit ADR waiver (decision 0026).
//!
//! That is why the tables below are per *variant* rather than per type. Each
//! one pins the index a variant encodes to, so a reordering, an insertion in
//! the middle, or a removal that closes a gap all move bytes here. The tables
//! cover the enums this profile owns and the Pallas enums its records embed —
//! a Pallas upgrade that renumbered `Relay` or `DRep` would otherwise change
//! published bytes with nothing in this repository to notice.
//!
//! The variant *order* in a table is the declaration order of the type, not
//! the numeric order of the indexes: `StakeCredential` declares its variants
//! out of index order, and a table that quietly sorted them would hide exactly
//! the kind of drift it exists to catch.
//!
//! Reachability is the whole membership rule, and it is narrower than "an enum
//! the governance code touches". `Voter` is not here, for instance: it selects
//! *which* of a proposal's three vote maps a vote lands in and is never itself
//! stored in an entity value, so no stele byte depends on its indexes.
//!
//! One reachable enum is covered indirectly instead of by a table of its own:
//! `EpochPosition` is `pub(super)` to `model::epoch_value` and so unnameable
//! from here, and is pinned by the canaries that carry it (`accounts`,
//! `epochs`, `pools`). `NonceVariant` does have a table, whose rows encode the
//! `Nonce` that wraps it — the variant is never written bare, so pinning it
//! bare would pin bytes no record can contain.

use dolos_cardano::pallas::codec::minicbor;

use super::canaries;

/// One enum's pinned variant table.
pub struct Table {
    /// The type, spelled as the code spells it.
    pub name: &'static str,

    /// What a failure should tell whoever hits it.
    pub policy: &'static str,

    /// `(variant, hex)` in declaration order.
    pub pinned: &'static [(&'static str, &'static str)],

    /// The same variants, encoded now.
    pub encode: fn() -> Vec<(&'static str, Vec<u8>)>,
}

/// The policy every table in this file carries. Stated once, pointed at from
/// each table, and printed by the assertion that fails — the failure message
/// is where the rule has to be legible.
const VARIANT_POLICY: &str = "\
variant addition is reader-breaking within v{x}: a decoder refuses an index it \
does not know. Growing this enum requires a media-type version bump on the \
kinds that carry it, or an explicit ADR waiver (decision 0026). Renumbering or \
removing a variant is breaking unconditionally.";

fn encode_all<T: minicbor::Encode<()>>(
    values: Vec<(&'static str, T)>,
) -> Vec<(&'static str, Vec<u8>)> {
    values
        .into_iter()
        .map(|(name, value)| (name, minicbor::to_vec(&value).expect("a canary encodes")))
        .collect()
}

macro_rules! table {
    ($fn_name:ident, $name:literal, $source:path, $pinned:expr) => {{
        fn $fn_name() -> Vec<(&'static str, Vec<u8>)> {
            encode_all($source())
        }

        Table {
            name: $name,
            policy: VARIANT_POLICY,
            pinned: $pinned,
            encode: $fn_name,
        }
    }};
}

pub fn tables() -> Vec<Table> {
    vec![
        table!(
            enc_pparam_value,
            "PParamValue",
            canaries::every_pparam_value,
            PPARAM_VALUE
        ),
        table!(
            enc_proposal_action,
            "ProposalAction",
            canaries::every_proposal_action,
            PROPOSAL_ACTION
        ),
        table!(
            enc_gov_purpose,
            "GovPurpose",
            canaries::every_gov_purpose,
            GOV_PURPOSE
        ),
        table!(
            enc_pool_delegation,
            "PoolDelegation",
            canaries::every_pool_delegation,
            POOL_DELEGATION
        ),
        table!(
            enc_drep_delegation,
            "DRepDelegation",
            canaries::every_drep_delegation,
            DREP_DELEGATION
        ),
        table!(
            enc_committee_authorization,
            "CommitteeAuthorization",
            canaries::every_committee_authorization,
            COMMITTEE_AUTHORIZATION
        ),
        table!(enc_drep, "DRep", canaries::every_drep, DREP),
        table!(
            enc_stake_credential,
            "StakeCredential",
            canaries::every_stake_credential,
            STAKE_CREDENTIAL
        ),
        table!(enc_vote, "Vote", canaries::every_vote, VOTE),
        table!(enc_relay, "Relay", canaries::every_relay, RELAY),
        table!(
            enc_nonce_variant,
            "NonceVariant",
            canaries::every_nonce_variant,
            NONCE_VARIANT
        ),
    ]
}

// The pinned tables. Short by nature — a variant canary is the discriminant
// plus one small payload — so they stay inline, next to the policy above.

const PPARAM_VALUE: &[(&str, &str)] = &[
    ("SystemStart", "82001a59c6d5d3"),
    ("EpochLength", "82011a00069780"),
    ("SlotLength", "820201"),
    ("MinFeeA", "8203182c"),
    ("MinFeeB", "82041a00025ef5"),
    ("MaxBlockBodySize", "82051a00016000"),
    ("MaxTransactionSize", "8206194000"),
    ("MaxBlockHeaderSize", "820719044c"),
    ("KeyDeposit", "82081a001e8480"),
    ("PoolDeposit", "82091a1dcd6500"),
    ("DesiredNumberOfStakePools", "820a1901f4"),
    ("ProtocolVersion", "820b820a01"),
    ("MinUtxoValue", "820c1a000f4240"),
    ("MinPoolCost", "820d1a0a21fe80"),
    ("ExpansionRate", "820ed81e82031903e8"),
    ("TreasuryGrowthRate", "820fd81e82020a"),
    ("MaximumEpoch", "821012"),
    ("PoolPledgeInfluence", "8211d81e82030a"),
    ("DecentralizationConstant", "8212d81e820001"),
    (
        "ExtraEntropy",
        concat!(
            "8213820158204445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f6061",
            "6263"
        ),
    ),
    ("AdaPerUtxoByte", "82141910d6"),
    (
        "ExecutionCosts",
        "821582d81e82190241192710d81e821902d11a00989680",
    ),
    ("MaxTxExUnits", "8216821a00d59f801b00000002540be400"),
    ("MaxBlockExUnits", "8217821a03b20b801b00000004a817c800"),
    ("MaxValueSize", "821818191388"),
    ("CollateralPercentage", "8218191896"),
    ("MaxCollateralInputs", "82181a03"),
    (
        "PoolVotingThresholds",
        concat!(
            "82181b85d81e8218331864d81e8218341864d81e8218351864d81e8218361864d81e8218",
            "371864"
        ),
    ),
    (
        "DrepVotingThresholds",
        concat!(
            "82181c8ad81e82183d1864d81e82183e1864d81e82183f1864d81e8218401864d81e8218",
            "411864d81e8218421864d81e8218431864d81e8218441864d81e8218451864d81e821846",
            "1864"
        ),
    ),
    ("MinCommitteeSize", "82181d07"),
    ("CommitteeTermLimit", "82181e1892"),
    ("GovernanceActionValidityPeriod", "82181f06"),
    ("GovernanceActionDeposit", "8218201b000000174876e800"),
    ("DrepDeposit", "8218211a1dcd6500"),
    ("DrepInactivityPeriod", "82182214"),
    ("MinFeeRefScriptCostPerByte", "821823d81e820f01"),
    ("CostModelsPlutusV1", "821824831a000189b4201901a4"),
    ("CostModelsPlutusV2", "821825831a000189b5211901a5"),
    ("CostModelsPlutusV3", "821826831a000189b6221901a6"),
    ("CostModelsUnknown", "821827a204830102030583040506"),
];

const PROPOSAL_ACTION: &[(&str, &str)] = &[
    (
        "ParamChange",
        concat!(
            "82008181848203182d820b820a01821582d81e82190241192710d81e821902d11a009896",
            "808216821a00d59f801b00000002540be400"
        ),
    ),
    ("HardFork", "820181820a02"),
    (
        "TreasuryWithdrawal",
        concat!(
            "82028182828200581c22232425262728292a2b2c2d2e2f303132333435363738393a3b3c",
            "3d1a000f4240828201581c232425262728292a2b2c2d2e2f303132333435363738393a3b",
            "3c3d3e1a001e8480"
        ),
    ),
    ("Other", "820380"),
    ("NoConfidence", "820480"),
    (
        "UpdateCommittee",
        concat!(
            "820583818200581c2425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f",
            "81828201581c25262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f401902",
            "1cd81e820203"
        ),
    ),
    (
        "NewConstitution",
        concat!(
            "82068282781968747470733a2f2f63616e6172792e696e76616c69642f33385820262728",
            "292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445581c2728292a2b",
            "2c2d2e2f303132333435363738393a3b3c3d3e3f404142"
        ),
    ),
    ("Info", "820780"),
];

const GOV_PURPOSE: &[(&str, &str)] = &[
    ("PParamUpdate", "00"),
    ("HardFork", "01"),
    ("Committee", "02"),
    ("Constitution", "03"),
];

const POOL_DELEGATION: &[(&str, &str)] = &[
    (
        "Pool",
        "820081581c28292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f40414243",
    ),
    ("NotDelegated", "820180"),
];

const DREP_DELEGATION: &[(&str, &str)] = &[
    (
        "Delegated",
        "8200818200581c292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f4041424344",
    ),
    ("NotDelegated", "820180"),
];

const COMMITTEE_AUTHORIZATION: &[(&str, &str)] = &[
    (
        "HotCredential",
        "8200818200581c2a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445",
    ),
    (
        "Resigned",
        concat!(
            "82018182781968747470733a2f2f63616e6172792e696e76616c69642f343358202b2c2d",
            "2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a"
        ),
    ),
];

const DREP: &[(&str, &str)] = &[
    (
        "Key",
        "8200581c2c2d2e2f303132333435363738393a3b3c3d3e3f4041424344454647",
    ),
    (
        "Script",
        "8201581c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748",
    ),
    ("Abstain", "8102"),
    ("NoConfidence", "8103"),
];

const STAKE_CREDENTIAL: &[(&str, &str)] = &[
    (
        "AddrKeyhash",
        "8200581c2e2f303132333435363738393a3b3c3d3e3f40414243444546474849",
    ),
    (
        "ScriptHash",
        "8201581c2f303132333435363738393a3b3c3d3e3f404142434445464748494a",
    ),
];

const VOTE: &[(&str, &str)] = &[("No", "00"), ("Yes", "01"), ("Abstain", "02")];

const RELAY: &[(&str, &str)] = &[
    (
        "SingleHostAddr",
        "8400190bb944010203045002030405060708090a0b0c0d0e0f1011",
    ),
    (
        "SingleHostName",
        "8301190bba7472656c61792e63616e6172792e696e76616c6964",
    ),
    (
        "MultiHostName",
        "8202781c5f63617264616e6f2e5f7463702e63616e6172792e696e76616c6964",
    ),
];

const NONCE_VARIANT: &[(&str, &str)] = &[
    ("NeutralNonce", "8100"),
    (
        "Nonce",
        "82015820393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758",
    ),
];
