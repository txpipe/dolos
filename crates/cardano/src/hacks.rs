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
/// submitted in. Four classes of proposal break that derivation, and this
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
/// * **No quorum** — a proposal that never reaches the genesis-key quorum never
///   enacts. Its row moves it to the boundary that enacts an identical proposal
///   (preview's epoch-3 cost model update).
///
/// Reading the proposal's own target epoch, counting the genesis-key quorum,
/// and modelling Byron endorsement would replace all of this with a rule. That
/// is new ledger semantics and belongs to its own plan; until then, deleting
/// these rows would move every era boundary they set and shift every epoch
/// number after it.
pub mod pre_conway_updates {
    use pallas::ledger::primitives::Epoch;

    pub mod preview {
        use pallas::ledger::primitives::Epoch;

        /// Two hard forks (v7→v8 with a two-epoch lag, v8→v9 held up by
        /// quorum) and one cost model update without quorum.
        pub fn enacts_at(proposal: &str) -> Option<Epoch> {
            let epoch = match proposal {
                // cost model update without quorum (3 of 7 genesis keys); enacts
                // with the identical epoch-8 proposal (effect at 9)
                "edb991bbbfcb05dbccaea6660db60bbfe25c52dec086273cf131a6c21e923ed2#0" => 8,
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

/// The protocol-9 DRep delegator sets dolos cannot reconstruct (#1364).
///
/// Under protocol 9 a DRep's deregistration (`clearDRepDelegations`) clears
/// every account in the delegator set the DRep holds, and that set was not
/// kept in step with the accounts: a re-delegation left the account in its
/// previous DRep's set (cardano-ledger #4772). Dolos decides by position
/// instead — a delegation older than the certificate — which disagrees in two
/// forms:
///
/// * **Stale reverse delegation** — the account moved from X to Y, X
///   deregistered, and the ledger cleared the delegation to Y. Listed `Clear`.
/// * **Delegation before registration** — the account delegated to D in the
///   transaction that registered D, so it is not in the set D's later
///   deregistration folds over. Listed `Keep`.
///
/// Each entry is one deregistration certificate, keyed by its transaction and
/// certificate index, with the certificate's `(slot, tx index)` position — the
/// one `DRepState::unregistered_at` holds — and the accounts it settles.
/// Modelling the PV9 delegator sets would replace the table; the founder ruled
/// for the table (2026-10-07). Networks without entries keep the positional
/// rule.
pub mod pv9_drep_delegs {
    use std::collections::HashMap;

    use dolos_core::{BlockSlot, TxOrder};
    use pallas::ledger::primitives::{conway::DRep, StakeCredential};

    /// What the certificate's PV9 delegator set did to the account.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Action {
        /// The set held the account: its delegation is cleared, whatever DRep
        /// it points at now.
        Clear,
        /// The set did not hold the account: its delegation survives.
        Keep,
    }

    pub struct Entry {
        pub tx: &'static str,
        pub cert_index: u32,
        pub drep: &'static str,
        pub position: (BlockSlot, TxOrder),
        pub accounts: &'static [(&'static str, Action)],
    }

    /// How one boundary settles an account listed under a retiring DRep's
    /// certificate.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct Settlement {
        pub drep: DRep,
        pub unregistered_at: (BlockSlot, TxOrder),
        pub action: Action,
    }

    /// Mainnet: 32 certificates in epochs 507–536, 51 stale reverse
    /// delegations. Transcribed from the dolos-mainnet-pre-pass-triage note
    /// (question 1, correction), `pv9-correction-entries.tsv`.
    pub mod mainnet {
        use super::{Action::*, Entry};

        pub const ENTRIES: &[Entry] = &[
            Entry {
                tx: "15b3969f4247ad5a888fb7b8180acab359fa4a4eed1c9aab3a1424e8aaede5c1",
                cert_index: 0,
                drep: "key:c02dff4a3c2ec74b2fd56a6aa654bbc759898047cb2d26268068bc04",
                position: (133870962, 5),
                accounts: &[(
                    "key:c752ade32ffb274d0834af477c18b6bfff42952719f1c4949d6a43e9",
                    Clear,
                )],
            },
            Entry {
                tx: "132ea4654d6596afb45d4cf350d136981165434e1370bf33578a186e5aab301e",
                cert_index: 0,
                drep: "key:b6f4547ad049d7443ee5695761e1aa5e446cfccf72c7ed0d8ad8edfa",
                position: (134049099, 11),
                accounts: &[(
                    "key:9a7fefceee469b5c2c8f0d5a4a0e1530040a882f67b63aa7333278cc",
                    Clear,
                )],
            },
            Entry {
                tx: "f0704afec289f385eed65ff11b9623de8d9d14fb72ecbd19d44d83d9fafb9179",
                cert_index: 0,
                drep: "key:f7c63b63a511122235e8e92ac41690647de42723b4eefc7b0d61f34a",
                position: (134052675, 10),
                accounts: &[(
                    "key:d0bd22bc185ff74e4f212d5ed089aee57b3c442b3b52e08aaff8a19c",
                    Clear,
                )],
            },
            Entry {
                tx: "6b4edc0fecc33f57d099463b6f680fd0405f7dd20436a7bea6b4edf90b5b3832",
                cert_index: 0,
                drep: "key:26ad43ed8afa9a9c8e6b04282d6b8cea708945ab1ea30ee8f20e1358",
                position: (134295356, 5),
                accounts: &[(
                    "key:73230628616d8619428318e64d163f29571c06f53fc4aac3afb7d5bf",
                    Clear,
                )],
            },
            Entry {
                tx: "15c290c28c57b36d41b4013ad9dcbb5e0858ea0324111dd55c2ddb5e43ae8718",
                cert_index: 0,
                drep: "key:837a4344ac0577bbc749b5e1ca9e24bc512e682ac112ffef4aa1ac77",
                position: (134757917, 6),
                accounts: &[(
                    "key:0f224a3aad818e1e9edea71ef57ca2d9124ef2cf84b0e6d111eff677",
                    Clear,
                )],
            },
            Entry {
                tx: "4c6507f050e8b0d1dc199381b191d89061bf3786400489ec455050e7b4879d11",
                cert_index: 0,
                drep: "key:34526e9a56531abee4e90644ec2197fbf189c60165a6a7b37d8dd6ed",
                position: (135171445, 0),
                accounts: &[
                    (
                        "key:0258f08038a2b61560ef7df50a9240830f7507dd14ec897724cc7712",
                        Clear,
                    ),
                    (
                        "key:10726615798fdbc705e39b3b7e1b4014bb905b39290a9c938369b019",
                        Clear,
                    ),
                    (
                        "key:657ddde4eaf710a7309c4c430fab738254956ab6e6a675239aa85e28",
                        Clear,
                    ),
                    (
                        "key:7cf9b77ba678a69d25a156468259bd891e0ad761e0de742e26be3a4a",
                        Clear,
                    ),
                    (
                        "key:a17fcf50e0decc7ceb7bbfecbbc18e49c7a181d3fd42a8c45424b99b",
                        Clear,
                    ),
                    (
                        "key:f0d254eae50a8e68de138fb5a32f58507b7935ec2119394e78c7aa9e",
                        Clear,
                    ),
                ],
            },
            Entry {
                tx: "9b7eea940b7a528a75b0d629b8e26d51410ab3cc184e378da3a7a2acb6a2495e",
                cert_index: 0,
                drep: "key:e6254d848799c0fc46615c7fb15a7f63cdf6cb3a4f59d4119c03ef7f",
                position: (135462776, 2),
                accounts: &[(
                    "key:acade19983ab3f9c52a1848415ac0039a4bed2dc22c3a0d2e4acb64c",
                    Clear,
                )],
            },
            Entry {
                tx: "bc4ab4217d5d2ed8854ca5364c8a0357b2df78d1e9f4e2be235cb2174f78715d",
                cert_index: 0,
                drep: "key:8e6b8e46cd4ba1cff93fe7187c2844c27fe791dcd55af243bbb2b7e3",
                position: (136142055, 5),
                accounts: &[(
                    "key:2d1c444ebc4cb022a9b06a4bffd0b39ef43ff16b2568299ce1ea299f",
                    Clear,
                )],
            },
            Entry {
                tx: "3adc826716deead21c9541c194a2441e1bffde87d3da9f7e9286e8987987de35",
                cert_index: 0,
                drep: "key:612024517017b32dda0e5a7d90cb00f5f90d643c5f849d95d5dfc172",
                position: (136237354, 10),
                accounts: &[(
                    "key:5c3b9951cbd03a6087e90a29b15506df004b2e6d9b0115c267634203",
                    Clear,
                )],
            },
            Entry {
                tx: "b38494853593c867706abb599b9c0ddef8b119e33477b90886826357cd25d292",
                cert_index: 0,
                drep: "script:e5ab37261b3d63600d566564879370aea031ea3108b0a6bd8cef58aa",
                position: (136310931, 12),
                accounts: &[(
                    "key:f3f1ddfc434e1482aeaa07932c69bdf1568e254049efc496a68fa68d",
                    Clear,
                )],
            },
            Entry {
                tx: "a65612f6446e9fbfc0795e68fae71eaf30c09c17ded9ef5ac6e409500a54ec6a",
                cert_index: 0,
                drep: "key:d90fb95d2b9c7931be50868dd0a02e4faa18b34ea6d3cabf0b50e2c5",
                position: (136851040, 33),
                accounts: &[
                    (
                        "key:30e905a7c5305963bf7ab20b0d1f98c7985bf1afde16371dce87cce0",
                        Clear,
                    ),
                    (
                        "key:3a57af7913f0578ceddb35cd72e9abc3644189afb6607ce6d22d076e",
                        Clear,
                    ),
                ],
            },
            Entry {
                tx: "7a8d94425de0478bbd3b5b181c21ee451e43f947ca39036b632d6bb45bf9dfe9",
                cert_index: 1,
                drep: "key:fc1bf2fcd39fd2a84404415312f7d9ff459bb7eab71437fe39e20bb5",
                position: (136963166, 17),
                accounts: &[(
                    "key:37b257ad15f8506600f5cde2570d0fc44a4a5ce3f7966a126f224a23",
                    Clear,
                )],
            },
            Entry {
                tx: "e18872bc3260bb005a9633ebb404389cd5a6fb2ba07ce2e96bbb1f26ee52b03a",
                cert_index: 0,
                drep: "key:e77b7e4024111721c1a0ce8536b9b6405cb9f2b845be2380b289f1d5",
                position: (137197383, 10),
                accounts: &[(
                    "key:864173106505813a498b61b03540d7844327a9ec87f8faa5e818f30b",
                    Clear,
                )],
            },
            Entry {
                tx: "85fa1b078970ce14a768c9aac8878b2dee9ca2e55fad9c7ca5922568852d471b",
                cert_index: 0,
                drep: "key:3cb76957f95d7e692f1b744e07cff602e97ac1bc806bf95a3a37c403",
                position: (137311038, 18),
                accounts: &[
                    (
                        "key:2e86bfdc61fdc07bb8e059a0e0902ae72247ccf5a297425fe9b4d04f",
                        Clear,
                    ),
                    (
                        "key:f40665a7200bb2ae9a698e1b46980997dfa80e87ef4536ecaf916ef9",
                        Clear,
                    ),
                ],
            },
            Entry {
                tx: "e8ccb410a6831221c5a9f24c227af4cc12b9861ba5506e9b8d991d85453e4560",
                cert_index: 0,
                drep: "key:0733696a35276dbff685b9955906aa632500cb2e1dd08b4c5a40af01",
                position: (137693754, 3),
                accounts: &[(
                    "key:7852ba479de702b1658b0b093d2c241dc49af0bd52f5276c3f2bb170",
                    Clear,
                )],
            },
            Entry {
                tx: "41188a8442a0aaf5a8acdd54c734837b05394cbe13245253a46d98c826d405f1",
                cert_index: 0,
                drep: "key:7397c3c0a8bee8842bd1fd4e90e71a335686134975da5efaacd2490a",
                position: (138803556, 5),
                accounts: &[(
                    "key:83125956a5329b43e59c14e40dfd0720b101f0a4ba7e74913bb8acd9",
                    Clear,
                )],
            },
            Entry {
                tx: "95b9f945b68012aba226cdb9d1a34756e88002e325c433c74b8d3e3a4b57a003",
                cert_index: 0,
                drep: "key:d887b6fe119f6397a69f889b66ea2af2bfeb2e15ede7f0400ecafcf6",
                position: (139182438, 11),
                accounts: &[
                    (
                        "key:4eb85eda1b978a052914aea9c2c6d591b448f16801eeddcaefa004de",
                        Clear,
                    ),
                    (
                        "key:8c6ac27874bc0fac34fe734436561f825406b9dcd9875f92995b2292",
                        Clear,
                    ),
                    (
                        "key:ac4b165a1dcd1eee03d3208b9cad75b5e231406542befd004ec87a05",
                        Clear,
                    ),
                    (
                        "key:e34ce0cb31c297ce17822f8f041f9299dfac6cf82693a2921178a529",
                        Clear,
                    ),
                ],
            },
            Entry {
                tx: "47de4b243c0b89c0e07bf0223c5ab80c6e37ecca1127c4c9670844a3a3ffe2f3",
                cert_index: 0,
                drep: "key:44cd281d4f6bfa418b8901a6992fd37c22097b4afb214be6389c97a8",
                position: (139257049, 22),
                accounts: &[(
                    "key:8f3199fe19c8f8b5d00669be1a83d1e2dc53f9b3f4803ed0a8c18cd3",
                    Clear,
                )],
            },
            Entry {
                tx: "b623580ad3163cdfc0fc1133b1a5143a53cc39ac347c34666ab052739b6ba658",
                cert_index: 0,
                drep: "key:08473aaec49151aac193b9dd40ea1a3843f70a78698252e1b92825a0",
                position: (139737889, 11),
                accounts: &[(
                    "key:18ef022bd504828851767fbc4ebfb4dd34701b8ee8f1eecd3dfd06c3",
                    Clear,
                )],
            },
            Entry {
                tx: "edbcd8d50940f37eca6d378dc38d594c45528485cdc4a9738e003b58d5870d17",
                cert_index: 1,
                drep: "key:805d4eb74bda794d3ac27d73d44f58de99c68ebfb8c782aeadc22c58",
                position: (139783792, 32),
                accounts: &[(
                    "key:45bee542d3f10956d4abe1f35c6fc7d28083be20a03cf5855eea57ef",
                    Clear,
                )],
            },
            Entry {
                tx: "d56e104c9664e56bc7095ec0b24e2c8c816c4fa042c47081ca830df91b864eda",
                cert_index: 0,
                drep: "key:acc903d6d3e23812d7baccdae16a981ac0186fd8cd76940d4a44b1ba",
                position: (140028023, 13),
                accounts: &[(
                    "key:4ed8049a363e2734749ac21d340915d47ae740c3194a78654c40be42",
                    Clear,
                )],
            },
            Entry {
                tx: "9bf58a805061309cca423ce6ac8f2394923af355c0609babb824e29d522d6a6a",
                cert_index: 0,
                drep: "key:80c6995b85376dfa5921122d96994f9912e404aa785d34fd57a6a724",
                position: (140143181, 42),
                accounts: &[
                    (
                        "key:e2c5df3ceb4ba8afc2539f236932223446ba259a9975fdaaa3e9bda8",
                        Clear,
                    ),
                    (
                        "key:f8224117abe440c5b408958d881cace2fe73b1ace57e91db9ecee85a",
                        Clear,
                    ),
                ],
            },
            Entry {
                tx: "671ffacad1e1ef63560b90ae66e6b6c39408b18f19e8c6e0272845826f0461a7",
                cert_index: 0,
                drep: "key:0f85f486c7b29ea3b34e517d3da2571e081a1635a41b9bbf44c9aba5",
                position: (140788516, 1),
                accounts: &[(
                    "key:084aba7656965b15a519be69435cfb446a0418755e42153836a75023",
                    Clear,
                )],
            },
            Entry {
                tx: "28c25ad9b1da9a649cd3d113515dc912243fe4f39caae367b73cab08825ee1bd",
                cert_index: 0,
                drep: "key:07b5be329e147eeb58132084cbfe70ddeccaf94dfab2a6f43a5228de",
                position: (142288756, 39),
                accounts: &[
                    (
                        "key:2e5c7001348ee84207f51508abc5545ea5292267dffd9525d5aef71f",
                        Clear,
                    ),
                    (
                        "key:7ddce666ba07b4442f13a41edb168b468b247a61ddded4e2081b8dc0",
                        Clear,
                    ),
                    (
                        "key:9f7bab3d76f36b7652378909164cf556e1d8f7af548b671483479491",
                        Clear,
                    ),
                    (
                        "key:a360c303ceec73a9de2b67982efffbaaebbb0215c94fe82f04bd4913",
                        Clear,
                    ),
                    (
                        "key:d7ca0cdf3f38c81ad3b3d1515f3abae09ad7768e34635bf923c1312b",
                        Clear,
                    ),
                    (
                        "key:edae3ea2fdbf05da583808378ee97b57696a3c55ca8cfe8a4491d2a2",
                        Clear,
                    ),
                    (
                        "key:fc081c86bd2a91e2f3c4291f4e252f4af87a196f882f51cd2f3f11c0",
                        Clear,
                    ),
                ],
            },
            Entry {
                tx: "be69757a628369095aee2146eb53c78bcfe9638ec0affa40458c42de96e54dee",
                cert_index: 0,
                drep: "key:f209d5e990d9d05aa4cb886cd9a6d52508db345b14bd72ebc690b370",
                position: (142298971, 68),
                accounts: &[(
                    "key:0123d67ed0e23001c0d62d3d65c2d37a53403731a2f434baa8a436af",
                    Clear,
                )],
            },
            Entry {
                tx: "a0e0946642466daa2ed97167680b37d8779f34af4412b4d6a319765e112c23fd",
                cert_index: 0,
                drep: "key:51180094d8cbe3f418970c7afc684b099fa769bc9e02d3b7f9a9a1e2",
                position: (143586027, 3),
                accounts: &[
                    (
                        "key:58e25ad27e2e2d151b3febfbfbcaaa123ee04353f5d68bc4f3b51bed",
                        Clear,
                    ),
                    (
                        "key:78205efe58614810c56df27a2471c853b159473a53aa9c40ae39e8d3",
                        Clear,
                    ),
                    (
                        "key:f4daa7c97c971fad715c939f6724f28ffe6f6f88298f2aa027724d8d",
                        Clear,
                    ),
                ],
            },
            Entry {
                tx: "205c6938b38be42addc04c2e646a66251ceceb897ac2543994bfe40577c5d394",
                cert_index: 0,
                drep: "key:483f8b175d4bd0a3cd09af5f2957e89943d6b6f80b0508438d727300",
                position: (144050134, 7),
                accounts: &[(
                    "key:8653ca32415a8342325a672429e63cddd4a910edcdf95192f1318f88",
                    Clear,
                )],
            },
            Entry {
                tx: "037d399fda5a392200689c413ecf739c8b3a0aa7eccadfda518ea44ee2a181cf",
                cert_index: 0,
                drep: "key:ba79348ccfa62a1c0f58f028379154cb513805b4b814a5a188c411dd",
                position: (145069848, 34),
                accounts: &[(
                    "key:8fd283ef9cf23b1cdb711ad7dab29319f2fd183d6366b165981b0910",
                    Clear,
                )],
            },
            Entry {
                tx: "3e69f28e435200e926a0bf164ef904efa820c0bc89828b3d6ba9ee4f2dd6f9b1",
                cert_index: 0,
                drep: "key:619c277a49bcecd23f353dcfa4ded6e3e40b100cebdc1327bf7d8bfb",
                position: (145145670, 6),
                accounts: &[(
                    "key:79d149613a79dc6226a3f3b447bd4d41fd4ced11fdc5896cbd867cfc",
                    Clear,
                )],
            },
            Entry {
                tx: "6de9753ca547caf941b569e2c39a846aa0c47e866a1259580cb782aa6df5ffd0",
                cert_index: 0,
                drep: "key:c899dfb72f3bca954ca81ddc90d1a0896bb5be3a41cfc8312975c705",
                position: (145769320, 61),
                accounts: &[(
                    "key:dd8777709f8be94d59db54617d6fb44fbd1306dbe51a8fb16b1e054c",
                    Clear,
                )],
            },
            Entry {
                tx: "67cd8a76c3cd173d8c796aee5121475f6839ab51f4a54d820b7e1a37004668c2",
                cert_index: 0,
                drep: "key:b737385d81481532cf3d45ab7284f128d5b8ccea26ab1657c02ac159",
                position: (145772823, 5),
                accounts: &[(
                    "key:35115af60a3625c7557357b647cb1415bcc415eef6f2bd74771bf147",
                    Clear,
                )],
            },
            Entry {
                tx: "6e12af657ce33830abac4fd13239a0be63d3dc2f531771495c050eb812811dd0",
                cert_index: 0,
                drep: "key:ddfd8f7ab4307876896df7a884606675790988bb7117d22270b57265",
                position: (146593231, 15),
                accounts: &[(
                    "key:333b39c224db5b90786527673419021bae01a1244944f652b19a05db",
                    Clear,
                )],
            },
        ];
    }

    /// Preview: 6 certificates, 5 stale reverse delegations and 1
    /// delegation before registration. Same source as mainnet.
    pub mod preview {
        use super::{Action::*, Entry};

        pub const ENTRIES: &[Entry] = &[
            Entry {
                tx: "292af7e12511222aed7271af7167adc892b70badea91e467becff0afd109fb87",
                cert_index: 0,
                drep: "key:4589ce916b6aba99aa5531364aa2623d7e0a9e3c35fffe6700bb0e86",
                position: (58810216, 1),
                accounts: &[(
                    "key:3dd92899f772d4616a1a7fcbf1c4b161deb8f7eecc77d6bbecee62ac",
                    Clear,
                )],
            },
            Entry {
                tx: "e60bc19420fd1d79477b7931ebff29f1c82a176451950ddfd4bb70e8edcad0ed",
                cert_index: 0,
                drep: "key:2eed24ec0f9cb10dbd3e888f14cce9d7dee2b4fd82397893e468bcbe",
                position: (58874469, 0),
                accounts: &[(
                    "key:edc52808d58a420ce2df3da8c7118c858c7aa136fa7b2b1baa3e81ed",
                    Clear,
                )],
            },
            Entry {
                tx: "49df64bb6ae61cf97a4a7b770f7dbdcaf40930790df91ebd66d2bd16b20d3118",
                cert_index: 0,
                drep: "key:fdabe5ea6847b5753a32b4c47b5bbc14cecbe8ac7032cae568229502",
                position: (61218028, 0),
                accounts: &[(
                    "key:9448dbd85831eb183cc373d27ec4707f7197e6ddd8d9c25721e8be1f",
                    Clear,
                )],
            },
            Entry {
                tx: "3b3c658c30a03218ac875014d718c3415276948ad6b7b3fa4df3635bfe5c6434",
                cert_index: 0,
                drep: "key:fd160e2bc2c49423f164c4d2d65693ac0607932685dd6384797f2a98",
                position: (62587542, 0),
                accounts: &[(
                    "key:0a0478096d694283fcb7ce9f7af1c4f37d3ce7741ec1722160bc7087",
                    Clear,
                )],
            },
            Entry {
                tx: "85a3ef0caa6f85255a0c2aa3285f3313c4f047c035f14713ac713a46ac9d5bc7",
                cert_index: 0,
                drep: "key:217691139c3cdcb68500255cd1f3c3c215b7edea51e39807b3599a78",
                position: (63010743, 2),
                accounts: &[(
                    "key:7448bfa23b68459fbd23c27d80b89a9bc78dd4ee973efbec9779c6b7",
                    Clear,
                )],
            },
            Entry {
                tx: "9463319947d6f3fee2197e17168c4c8074bd23b1a91d63c122125bca5cbcc653",
                cert_index: 0,
                drep: "key:ea87ec24b249060c054dedd2e575d4fb92d8ac9b637072518feb55aa",
                position: (64134175, 0),
                accounts: &[(
                    "key:5a44ea95660a90c807c9787115c925ff7f6616c583f5235094327461",
                    Keep,
                )],
            },
        ];
    }

    /// The entries for a network, empty where it has none (preprod).
    pub fn entries(magic: u32) -> &'static [Entry] {
        match magic {
            764824073 => mainnet::ENTRIES,
            2 => preview::ENTRIES,
            _ => &[],
        }
    }

    fn parse_hash(value: &str) -> (&str, pallas::crypto::hash::Hash<28>) {
        let (kind, hex) = value.split_once(':').expect("kind-prefixed hash");
        (kind, hex.parse().expect("28-byte hash"))
    }

    pub fn parse_drep(value: &str) -> DRep {
        match parse_hash(value) {
            ("key", hash) => DRep::Key(hash),
            ("script", hash) => DRep::Script(hash),
            (kind, _) => panic!("unknown drep kind {kind}"),
        }
    }

    pub fn parse_credential(value: &str) -> StakeCredential {
        match parse_hash(value) {
            ("key", hash) => StakeCredential::AddrKeyhash(hash),
            ("script", hash) => StakeCredential::ScriptHash(hash),
            (kind, _) => panic!("unknown credential kind {kind}"),
        }
    }

    /// The accounts settled by the certificates this boundary acts on: the
    /// entries whose DRep and position match one of `retiring`.
    pub fn settle(
        entries: &[Entry],
        retiring: &[(DRep, (BlockSlot, TxOrder))],
    ) -> HashMap<StakeCredential, Settlement> {
        let mut settled = HashMap::new();

        for entry in entries {
            let Some((drep, unregistered_at)) = retiring
                .iter()
                .find(|(drep, at)| *at == entry.position && *drep == parse_drep(entry.drep))
            else {
                continue;
            };

            for (account, action) in entry.accounts {
                settled.insert(
                    parse_credential(account),
                    Settlement {
                        drep: drep.clone(),
                        unregistered_at: *unregistered_at,
                        action: *action,
                    },
                );
            }
        }

        settled
    }

    #[cfg(test)]
    mod tests {
        use std::collections::BTreeSet;

        use super::*;

        const MAINNET: u32 = 764824073;
        const PREPROD: u32 = 1;
        const PREVIEW: u32 = 2;

        /// The flattened `(network, account, action, drep, tx, cert index,
        /// slot, tx index)` rows the triage's TSV carries.
        type Row = (
            String,
            String,
            String,
            String,
            String,
            u32,
            BlockSlot,
            TxOrder,
        );

        fn table_rows() -> BTreeSet<Row> {
            [("mainnet", MAINNET), ("preview", PREVIEW)]
                .into_iter()
                .flat_map(|(network, magic)| {
                    entries(magic).iter().flat_map(move |entry| {
                        entry.accounts.iter().map(move |(account, action)| {
                            (
                                network.to_owned(),
                                account.to_string(),
                                format!("{action:?}").to_lowercase(),
                                entry.drep.to_owned(),
                                entry.tx.to_owned(),
                                entry.cert_index,
                                entry.position.0,
                                entry.position.1,
                            )
                        })
                    })
                })
                .collect()
        }

        fn tsv_rows() -> BTreeSet<Row> {
            let tsv = include_str!("../test_data/pv9-correction-entries.tsv");
            let mut lines = tsv.lines();
            let header: Vec<_> = lines.next().unwrap().split('\t').collect();
            let column = |name: &str| header.iter().position(|x| *x == name).unwrap();

            let [network, account, action, drep, tx, cert, slot, tx_index] = [
                "network",
                "account",
                "action",
                "dereg_drep",
                "dereg_tx",
                "dereg_cert_index",
                "dereg_slot",
                "dereg_tx_index",
            ]
            .map(column);

            lines
                .map(|line| {
                    let fields: Vec<_> = line.split('\t').collect();

                    (
                        fields[network].to_owned(),
                        fields[account].to_owned(),
                        fields[action].to_owned(),
                        fields[drep].to_owned(),
                        fields[tx].to_owned(),
                        fields[cert].parse().unwrap(),
                        fields[slot].parse().unwrap(),
                        fields[tx_index].parse().unwrap(),
                    )
                })
                .collect()
        }

        #[test]
        fn the_table_matches_the_triage_tsv() {
            let table = table_rows();
            assert_eq!(table, tsv_rows());

            let certificates = |magic| {
                entries(magic)
                    .iter()
                    .map(|entry| (entry.tx, entry.cert_index))
                    .collect::<BTreeSet<_>>()
                    .len()
            };
            let count = |network: &str, action: &str| {
                table
                    .iter()
                    .filter(|row| row.0 == network && row.2 == action)
                    .count()
            };

            assert_eq!(certificates(MAINNET), 32);
            assert_eq!(count("mainnet", "clear"), 51);
            assert_eq!(count("mainnet", "keep"), 0);
            assert_eq!(certificates(PREVIEW), 6);
            assert_eq!(count("preview", "clear"), 5);
            assert_eq!(count("preview", "keep"), 1);
            assert!(entries(PREPROD).is_empty());

            // every spelling parses, and no account is listed twice
            for magic in [MAINNET, PREVIEW] {
                let mut accounts = BTreeSet::new();

                for entry in entries(magic) {
                    parse_drep(entry.drep);

                    for (account, _) in entry.accounts {
                        assert!(accounts.insert(parse_credential(account)));
                    }
                }
            }
        }

        fn retiring(entry: &Entry) -> Vec<(DRep, (BlockSlot, TxOrder))> {
            vec![(parse_drep(entry.drep), entry.position)]
        }

        #[test]
        fn entries_apply_only_on_their_own_network() {
            for (own, other) in [(MAINNET, PREVIEW), (PREVIEW, MAINNET)] {
                for entry in entries(own) {
                    let retiring = retiring(entry);

                    assert_eq!(settle(entries(own), &retiring).len(), entry.accounts.len());
                    assert!(settle(entries(other), &retiring).is_empty());
                    assert!(settle(entries(PREPROD), &retiring).is_empty());
                }
            }
        }

        #[test]
        fn an_entry_needs_its_drep_at_its_position() {
            let entry = &entries(PREVIEW)[0];
            let (drep, (slot, tx_index)) = retiring(entry).remove(0);
            let other = parse_drep(entries(PREVIEW)[1].drep);

            assert!(settle(entries(PREVIEW), &[(other, (slot, tx_index))]).is_empty());
            assert!(settle(entries(PREVIEW), &[(drep.clone(), (slot, tx_index + 1))]).is_empty());
            assert!(settle(entries(PREVIEW), &[(drep, (slot + 1, tx_index))]).is_empty());
        }
    }
}
