//! The Pallas struct canaries.
//!
//! The records embed Pallas structs whose bytes this repository does not
//! define. The namespace canaries carry them, but a Pallas upgrade that moved
//! one would fail there as a whole-record diff that does not name the struct.
//! Each struct here is pinned on its own, so the failure names it.

use dolos_cardano::pallas::{
    codec::{minicbor, utils::Bytes},
    ledger::primitives::{ExUnits, PoolMetadata, ProtocolVersion},
};

use super::canaries::{anchor, bytes_of, gov_action_id, rational};

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
    ]
}
