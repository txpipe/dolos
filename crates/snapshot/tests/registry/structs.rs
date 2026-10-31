//! The Pallas struct canaries.
//!
//! The records embed Pallas structs, and the `ProtocolVersion` tuple, whose
//! bytes this repository does not define. The namespace canaries carry them,
//! but a Pallas upgrade that moved one would fail there as a whole-record diff
//! that does not name the struct. Each is pinned here on its own, so the
//! failure names it. The Pallas enums are pinned in [`super::enums`].

use dolos_cardano::pallas::{
    codec::{minicbor, utils::Bytes},
    ledger::primitives::{
        conway::{DRepVotingThresholds, PoolVotingThresholds},
        ExUnitPrices, ExUnits, Nonce, NonceVariant, PoolMetadata, ProtocolVersion,
    },
};

use super::canaries::{anchor, bytes_of, gov_action_id, hash32, rational};

/// One Pallas struct's pinned encoding.
pub struct Pinned {
    /// The type, spelled as the code spells it.
    pub name: &'static str,

    pub hex: &'static str,

    /// The canary, encoded now.
    pub encode: fn() -> Vec<u8>,
}

/// What a failure should tell whoever hits it.
pub const POLICY: &str = "\
a Pallas struct a record embeds changed its encoding: every record carrying \
it changes bytes. Hold the Pallas upgrade, or bump the media type of the \
kinds that carry the struct (decision 0026).";

fn encode<T: minicbor::Encode<()>>(value: T) -> Vec<u8> {
    minicbor::to_vec(value).expect("a canary encodes")
}

pub fn pinned() -> Vec<Pinned> {
    vec![
        Pinned {
            name: "Anchor",
            hex: concat!(
                "82781968747470733a2f2f63616e6172792e696e76616c69642f383058205051525354",
                "55565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f"
            ),
            encode: || encode(anchor(0x50)),
        },
        Pinned {
            name: "GovActionId",
            hex: concat!(
                "8258205152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f",
                "7007"
            ),
            encode: || encode(gov_action_id(0x51, 7)),
        },
        Pinned {
            name: "RationalNumber",
            hex: "d81e82031864",
            encode: || encode(rational(3, 100)),
        },
        Pinned {
            name: "PoolMetadata",
            hex: concat!(
                "82782068747470733a2f2f63616e6172792e696e76616c69642f706f6f6c2e6a736f",
                "6e582052535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f70",
                "71"
            ),
            encode: || {
                encode(PoolMetadata {
                    url: "https://canary.invalid/pool.json".to_string(),
                    hash: Bytes::from(bytes_of::<32>(0x52).to_vec()),
                })
            },
        },
        Pinned {
            name: "ExUnits",
            hex: "821a00d59f801b00000002540be400",
            encode: || {
                encode(ExUnits {
                    mem: 14_000_000,
                    steps: 10_000_000_000,
                })
            },
        },
        Pinned {
            name: "ProtocolVersion",
            hex: "820a01",
            encode: || encode::<ProtocolVersion>((10, 1)),
        },
        Pinned {
            name: "ExUnitPrices",
            hex: "82d81e82190241192710d81e821902d11a00989680",
            encode: || {
                encode(ExUnitPrices {
                    mem_price: rational(577, 10_000),
                    step_price: rational(721, 10_000_000),
                })
            },
        },
        Pinned {
            name: "Nonce",
            hex: concat!(
                "82015820535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f70",
                "7172"
            ),
            encode: || {
                encode(Nonce {
                    variant: NonceVariant::Nonce,
                    hash: Some(hash32(0x53)),
                })
            },
        },
        Pinned {
            name: "PoolVotingThresholds",
            hex: concat!(
                "85d81e8218331864d81e8218341864d81e8218351864d81e8218361864d81e821837",
                "1864"
            ),
            encode: || {
                encode(PoolVotingThresholds {
                    motion_no_confidence: rational(51, 100),
                    committee_normal: rational(52, 100),
                    committee_no_confidence: rational(53, 100),
                    hard_fork_initiation: rational(54, 100),
                    security_voting_threshold: rational(55, 100),
                })
            },
        },
        Pinned {
            name: "DRepVotingThresholds",
            hex: concat!(
                "8ad81e82183d1864d81e82183e1864d81e82183f1864d81e8218401864d81e821841",
                "1864d81e8218421864d81e8218431864d81e8218441864d81e8218451864d81e8218",
                "461864"
            ),
            encode: || {
                encode(DRepVotingThresholds {
                    motion_no_confidence: rational(61, 100),
                    committee_normal: rational(62, 100),
                    committee_no_confidence: rational(63, 100),
                    update_constitution: rational(64, 100),
                    hard_fork_initiation: rational(65, 100),
                    pp_network_group: rational(66, 100),
                    pp_economic_group: rational(67, 100),
                    pp_technical_group: rational(68, 100),
                    pp_governance_group: rational(69, 100),
                    treasury_withdrawal: rational(70, 100),
                })
            },
        },
    ]
}
