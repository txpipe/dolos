pub mod pointers {
    use pallas::ledger::{addresses::Pointer, primitives::StakeCredential};
    use tracing::warn;

    pub fn pointer_to_cred(pointer: &Pointer) -> Option<StakeCredential> {
        match (pointer.slot(), pointer.tx_idx(), pointer.cert_idx()) {
            // preview
            (2940289, 1, 0) => Some(StakeCredential::AddrKeyhash(
                "0c90492bbe7eb33f38173255e547dc3194abcec5cd29cdf504bb4f03"
                    .parse()
                    .unwrap(),
            )),
            (100, 2, 0) => None,
            (1, 1, 1) => None,
            (0, 0, 0) => None,
            (0, 1, 10000) => None,
            (10000000, 1, 1) => None,
            (100, 100, 1) => None,
            (1, 1, 1000) => None,
            (1, 1, 0) => None,
            (50, 50, 5) => None,

            // preprod
            (10612742, 0, 0) => Some(StakeCredential::AddrKeyhash(
                "4dcca876aac2fcc561f7df3da772d747e2148c9a05c7b27e49a05ea2"
                    .parse()
                    .unwrap(),
            )),
            (70549345, 1, 0) => Some(StakeCredential::AddrKeyhash(
                "b1a3b1ef9460dc7bef8ffdf49ce4e01b1cc2505c614ee62b3223f458"
                    .parse()
                    .unwrap(),
            )),
            (82626550, 0, 0) => None,
            (2498243, 27, 3) => None,

            // mainnet
            (4495800, 11, 0) => Some(StakeCredential::AddrKeyhash(
                "bc1597ad71c55d2d009a9274b3831ded155118dd769f5376decc1369"
                    .parse()
                    .unwrap(),
            )),
            (20095460, 2, 0) => Some(StakeCredential::AddrKeyhash(
                "1332d859dd71f5b1089052a049690d81f7367eac9fafaef80b4da395"
                    .parse()
                    .unwrap(),
            )),

            // Add all unmapped pointers from analysis as None
            (12, 12, 12) => None,
            (62, 96, 105) => None,
            (116, 49, 0) => None,
            (124, 21, 3807) => None,
            (13005, 15312, 1878946283) => None,
            (13200, 526450, 149104513) => None,
            (222624, 45784521, 167387965) => None,
            (105, 13146, 24) => None,
            (16292793057, 1011302, 20) => None,
            (18446744073709551615, 1221092, 2) => Some(StakeCredential::AddrKeyhash(
                "1332d859dd71f5b1089052a049690d81f7367eac9fafaef80b4da395"
                    .parse()
                    .unwrap(),
            )),
            (53004562, 9, 0) => Some(StakeCredential::AddrKeyhash(
                "e46c33afa9ca60cfeb3b7452a415c271772020b3f57ac90c496a6127"
                    .parse()
                    .unwrap(),
            )),
            (156960568, 15, 0) => Some(StakeCredential::AddrKeyhash(
                "a3d3ba720c11bb6b7364bb0ee2abfca79ec135aaafe0bd0b89f24121"
                    .parse()
                    .unwrap(),
            )),
            (78312587, 5, 0) => Some(StakeCredential::AddrKeyhash(
                "a773914d934899b3656f7f4edc3293c5804dc288faa468f6587f05e6"
                    .parse()
                    .unwrap(),
            )),

            (slot, tx_idx, cert_idx) => {
                warn!(slot, tx_idx, cert_idx, "missing pointer mapping");
                panic!()
            }
        }
    }
}

/// Enactment epochs for the pre-Conway update proposals whose timing dolos
/// cannot derive.
///
/// The Conway oracle this module used to carry is gone: ratification is
/// computed now (`ewrap::ratify`). What is left is a different thing that
/// happened to live in the same table, and it is not about governance at
/// all — it is the *legacy* update mechanism, the one that carried every
/// protocol parameter change and every hard fork before Conway.
///
/// A legacy update proposal enacts at the boundary closing the epoch it
/// names, and dolos derives that epoch from the epoch the proposal was
/// submitted in. Three classes of proposal break that derivation, and this
/// is the observed answer for each:
///
/// * **Shelley-era `ppup` targeting** — an update proposal carries the epoch
///   its change targets, and enacts at `target + 1` rather than one epoch after
///   submission. Mainnet's decentralisation schedule was submitted an epoch
///   ahead throughout, so every row of it lands here.
/// * **Byron endorsement** — a Byron update applies after enough block issuers
///   endorse it and a stabilisation window passes, not at the next boundary.
/// * **Quorum delay** — a hard-fork proposal waits for the genesis-key quorum,
///   which can take epochs (preview's v8→v9 waited until 645).
///
/// Reading the proposal's own target epoch and modelling Byron endorsement
/// would replace all of this with a rule. That is new ledger semantics and
/// belongs to its own plan; until then, deleting these rows would move
/// every era boundary they set and shift every epoch number after it.
pub mod pre_conway_updates {
    use pallas::ledger::primitives::Epoch;

    pub mod preview {
        use pallas::ledger::primitives::Epoch;

        /// Two hard forks: v7→v8 with a two-epoch lag, v8→v9 held up by quorum.
        pub fn enacts_at(proposal: &str) -> Option<Epoch> {
            let epoch = match proposal {
                // v7→v8 hard fork proposals (epoch 20, 2-epoch lag: effect at 22)
                "cbc14ec74b2a20d6c4cc307e73b5a2465eb6cd68df64704f7bc844dac6018500#0" => 21,
                "7722b914ab9ccab873cd70cb5c39e7ce3bb0f5daf72de8ece56dbc06807b5486#0" => 21,
                // v8→v9 hard fork proposals (quorum not reached until epoch 645)
                "99c48b116cf5536bbdd8f9fe0d5a4e7894309a6b5f0b984a264ce497bd61b351#0" => 645,
                "4fa27875bc4d00a1f40eae2b50b791d48fca4a0f8af4d44f0ceeb6c7662f689c#0" => 645,
                _ => return None,
            };

            Some(epoch)
        }
    }

    pub mod preprod {
        use pallas::ledger::primitives::Epoch;

        /// The Byron intra-era fork, the Shelley fork, and one delayed
        /// parameter update.
        pub fn enacts_at(proposal: &str) -> Option<Epoch> {
            let epoch = match proposal {
                // Byron intra-era hardfork
                "9972ffaee13b4afcf1a133434161ce25e8ecaf34b7a76e06b0c642125cf911a9#0" => 1,
                // Shelley hardfork
                "f48fffc65e16c3808720b38110a6d284250360108b6198a44331eb0de8e49817#0" => 3,
                // delayed pparam update
                "82b8de69d2ddd8b926e2af5979dc55ec18282d73bfe009c25bea9ef75e3fe11e#0" => 27,
                _ => return None,
            };

            Some(epoch)
        }
    }

    pub mod mainnet {
        use pallas::ledger::primitives::Epoch;

        /// The decentralisation schedule and two later parameter updates, all
        /// submitted one epoch before the epoch they target.
        pub fn enacts_at(proposal: &str) -> Option<Epoch> {
            let epoch = match proposal {
                // Decentralisation updates submitted one epoch before their target epoch.
                // RatifiedCurrentEpoch would enact these one epoch too early; use target
                // epoch so they enact at target_epoch + 1, matching the ppup/fpup pipeline.
                "a6713824eeef48508bd35e851bcf4021a93b5995127feb9910b1e1b88de2c225#0" => 214,
                "3da44150612379b337f0865bbe1c210e8f34a9d02280803e9ea90173d3361574#0" => 215,
                "319c8b8865bdc6ce896f3722aa54da9d9fd125429a7e05af1955004f69217eca#0" => 219,
                "e67064c5e85b74062a13a0ed9290f8f7d6c81440e39be081a334e33b57ec810d#0" => 221,
                "32c8bdd8791fee095c9074f7163410cc41eb05f5d6632afd96ab8578ad9ca215#0" => 222,
                "f10937dfd495061cdb3c6ae56af6d522391205f39318acf9098969224b97d1e8#0" => 224,
                "1d29f276d893e72183969dc39594c1cabadace86e8add3ce71af470c7c475b9d#0" => 225,
                "05d1302ff8d070d4e7545415f81c7d824d7601694d92053026c5cd7d58a7814a#0" => 228,
                "5bd9fa498676741dcf990ceb98512d91bfd0481093839827bea5abe1bbd89136#0" => 229,
                "db2be7716618fb6aa775c6052a39a9efe67f6a235ca42c8c28a681094aab82ec#0" => 232,
                "8fd8ea3d1933e05f6d474d315e1ff0d60e567a79f73fd3cec98b9cdac54ba75a#0" => 242,
                "f6334261e19a6a4ff028684b1cad38b4f9c03290e5c24ce2ac11d6e9a33fe0b5#0" => 245,
                "3dd110c031c23f9187441464edc8b84d4f9cd62df6cc3a04bf62fde5359ebd5a#0" => 246,
                "42362f1aea613711dfe527541f5a8de71579e6980d9887a1d4db29ef1b601863#0" => 247,
                "054257a09038d69832949b07b8d97a24687919a777e967933d85469480777e2e#0" => 248,
                "abbbf81e0fb1e4de222df18a9ca5fdfe3e9b9e2efc0cde6c42552789746c9852#0" => 249,
                "1bd8310b660c3086530763e67edae7087654a8642c981b3bc4ea89f33f3ed67c#0" => 250,
                "ee4876fa27951d12b17b647dbbc46f303b7b0e2dd416d92e88a6da7a58c6851e#0" => 252,
                "e2e52847e2b1d47032cee3b91419ec0f5078b7d31fa5e939bde6a77e97a9f04b#0" => 253,
                "956fb654686351da5367f326902e33b1200448624d756ca3ad7cf77db4c1bf52#0" => 255,
                "f7902182392800c8ea0b6fa048100263ae522fe5ee2fb1388f881921018dd6fc#0" => 256,
                // Non-d-parameter proposals submitted one epoch before target
                "51fa37794d2107d7d8705cd69594c5162ced13f922235a99d06aff20c64656b6#0" => 289,
                "8230f33cd7ad3f8601e94ea2b18abdc591187e190ea8ebecc25e20fc66200f13#0" => 364,
                "3abda97c78c71e8a21473529aca94d78d364dfa1a866ef8245885e18085b4e4c#0" => 364,
                "a83f479c5635e1e563a19f6e72a1be59fb082bbf31de90cc176850ee799b08ac#0" => 393,
                "62c3c13187423c47f629e6187f36fbd61a9ba1d05d101588340cfbfdf47b22d2#0" => 393,
                _ => return None,
            };

            Some(epoch)
        }
    }

    /// The curated enactment epoch of a legacy update proposal, or `None`
    /// when the submission epoch is the right answer.
    pub fn enacts_at(magic: u32, proposal: &str) -> Option<Epoch> {
        match magic {
            764824073 => mainnet::enacts_at(proposal),
            1 => preprod::enacts_at(proposal),
            2 => preview::enacts_at(proposal),
            _ => None,
        }
    }
}
