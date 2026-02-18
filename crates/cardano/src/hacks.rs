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

pub mod proposals {
    use pallas::ledger::primitives::Epoch;

    pub enum ProposalOutcome {
        Canceled(Epoch),
        Ratified(Epoch),
        RatifiedCurrentEpoch,
        Unknown,
    }

    pub mod preview {
        use super::ProposalOutcome;
        use super::ProposalOutcome::*;

        pub fn outcome(protocol: u16, proposal: &str) -> ProposalOutcome {
            match proposal {
                "69c948cde90c6b9d7d61595e8534c106ec44132cb049ab2558399db1260c1f69#0" => {
                    Ratified(1095)
                }
                "ac993231c39a4ee13bcf888e971e099809c4c08d96a7572aa3611a5ed42fa7d4#0" => {
                    Ratified(1012)
                }
                "602d8572263929bdb0aba911d45ecf4bf0a2430e2f263f89df7114d168985f57#0" => {
                    Ratified(998)
                }
                "6214314b6d6a30118d259c9597c0e0120b76aa521e322044c4290fcaac86e27a#0" => {
                    Ratified(997)
                }
                "f4188b8676bae7f3bb26626e57d1bf6b5212dc078581d00233e27f55a1392b0e#0" => {
                    Ratified(993)
                }
                "35b81b424956f018bb4a4bb9e160375c1921a3b40b60a1efc539bcd6b5b20159#0" => {
                    Ratified(963)
                }
                "049ae5d612b2fa825655809133b023d60c7f8cac683c278cf95de1622e4592f3#0" => {
                    Ratified(742)
                }
                "1f47f3cf2e4f9109be2efe9182cae08307e5778bdfea2150e6903c48edca0b8f#0" => {
                    Ratified(735)
                }
                "2841a581076167a0662f1b4f1a38bcc8eff386f9ce45c33ae33b1fe8289de210#0" => {
                    Canceled(736)
                }
                "95af8608dc7aaf5c73025066b509a2c11c829f2984009ac92e7053d123fbce57#0" => {
                    Canceled(736)
                }
                "3d573646d495b939ad019afc2653fbad023615b4ee5643d41fd9fa9cdb91fe29#0" => {
                    Canceled(994)
                }
                "4c7b63801d0a0f0bb3d83cf9f0951ceb0e453e1532a3f5a1f2988a8d9778a862#0" => {
                    Canceled(994)
                }
                "25a16ada4a57fd29a1ac5f62f585d923ffe3e23321512380dfd276f6c73b1451#0" => {
                    Canceled(1013)
                }
                "77cc6292907df30d4340aa389dda453ea03aae1aa18a71c1856ac10851498188#0" => {
                    Canceled(1013)
                }
                "8f6918be1e1762cae1a378882d0b4037e0e7176ce69ce4f874a170edbc4d837d#0" => {
                    Canceled(994)
                }
                "045bbfb7ec34ec5e5a4fe110a59cfe0ce799018cef0d68483ec2b704f6503b9f#0" => {
                    Canceled(994)
                }
                "4b0b84ce4a791228a9b844b70cc2cc8e19b5dc009422db062ff139abcb7a20f7#0" => {
                    Canceled(994)
                }
                "5c526a482838979d2ffc5864b0f8878593080d3cd3d6b4759c66ffac81b17402#0" => {
                    Canceled(994)
                }
                "2eb2213de142e5c95a7f2b5b5a66fbb181b19782eb3b1f5db4485ffc558da9f3#0" => {
                    Canceled(994)
                }
                "5375aedecf005d3e212795f018573ea8f31dfc1b9d98d9ee5f24ebfb93bfb83b#0" => {
                    Canceled(994)
                }
                "fad5f42e6648c04b506864a787d684ac03d54df0232473e81ccd404867af76c5#0" => {
                    Canceled(994)
                }
                "0d732b47248d70a8aa61e3560f3fddcaf3809d82c065cbdfaa36f7931db637d7#0" => {
                    Canceled(994)
                }
                "16d11d0a34d76bffbb394c71b6b04696d80373b12b8dc849ccd0d981e91c66e5#0" => {
                    Canceled(994)
                }
                "5552b8a209f9ffadc5aa583ef583caaf1776ca53bc98972165a5b583c4045328#0" => {
                    Canceled(994)
                }
                "9a0ba26ff6fcf78cc30760eded027f4a23916724307c5b8196a0e13885ecc717#0" => {
                    Canceled(994)
                }
                "eede35403b0f5d3db8e6fe80a038f24c498c5b4675817e32a67e338f29511d94#0" => {
                    Canceled(994)
                }
                "c9fd3e7b0ae40a1ac3e656931376d79a7d2b33727e5083c91d89d74b7d6ab765#0" => {
                    Canceled(994)
                }
                "1ed2479b2abab685bd0c148e880d9b4c006bf21991b0b5a0000e66df62d6ee4a#0" => {
                    Canceled(994)
                }
                "58d46770900cd81bd5529e4f8b8c2c03e6de897c5ca89f55cbc7451176fe7ac5#0" => {
                    Canceled(994)
                }
                "4f25b3fffda8ef20bc8a5ccf1fb6e9a7ab267046c49a6d70bc9e642d0d049868#0" => {
                    Canceled(994)
                }
                "7f630df922fd14374ce402ed989d928817c56fd185a4c76a359c5a8c689a676e#0" => {
                    Canceled(994)
                }
                "88f29172775d69e08b2efd09d82e5d80e2139a58d53401984f47ae1e44b4017f#0" => {
                    Canceled(994)
                }
                "90c124b693fe2bddc19b4cd0a4e7af92f4b668355e5e8607b828c753d66dbbfc#0" => {
                    Canceled(994)
                }
                "98f2ffe1c2ae6ae57cafb5ee4829e6c656c5e35ee38bce1688d8537fb4707f1e#0" => {
                    Canceled(994)
                }
                "b9dc48d2defba697dd3bdb2316808fe894167a319a678b0ee246b292a9328b10#0" => {
                    Canceled(994)
                }
                "ca61b3b660b626bfc49135ee5006555630679c20c4033642fde2f4de16d5946d#0" => {
                    Canceled(994)
                }
                "d61d08ef309dacc162507f9c3b99080f1be1fa31ea59319684e2d569f9ee4970#0" => {
                    Canceled(994)
                }
                "e7082ec2717eff54fa78c2812a28d3799f1256aa00f1657f20f03dffeb8ac55d#0" => {
                    Canceled(994)
                }
                "0176514f66026da634cfd9c37e4575645a80aca4e69ad83fe0e468be5f4b5c0c#0" => {
                    Canceled(1096)
                }
                "609896ea7a615392bdc8c9ef0df74338ecd16d581c0cd698aa539c3f782650e3#0" => {
                    Canceled(1096)
                }
                "00be4823e37a7a70875408bf9df377ee48c0fa0d02cbe118acfef8cd0b92d3f7#0" => {
                    Canceled(998)
                }
                "51e82c898ba142adefb676277b9f8f48487569c3a8528c2f68c0aaa038315519#0" => {
                    Canceled(998)
                }
                "60233953f6e9e56333bf9acbfd2a7262fecfc60b7f4487e59b0bfce79fbe749a#0" => {
                    Canceled(998)
                }
                "c3f38851329c7829eadc86c082e160f7d47e1c03e16e3281420bb741a7d438e9#0" => {
                    Canceled(1096)
                }
                "4bc0ee7f2cc3a4e47b50b38431ba813893d5f1dbb3cee42a31f8deb57934c987#0" => {
                    Canceled(1096)
                }
                "f046a88280e6c5b18dd057027964860f6b0b7918f4532d50455ad257a14a70ed#0" => {
                    Canceled(1096)
                }
                // v7→v8 hard fork proposals (epoch 20, 2-epoch lag: effect at 22)
                "cbc14ec74b2a20d6c4cc307e73b5a2465eb6cd68df64704f7bc844dac6018500#0" => {
                    Ratified(21)
                }
                "7722b914ab9ccab873cd70cb5c39e7ce3bb0f5daf72de8ece56dbc06807b5486#0" => {
                    Ratified(21)
                }
                // v8→v9 hard fork proposals (quorum not reached until epoch 645)
                "99c48b116cf5536bbdd8f9fe0d5a4e7894309a6b5f0b984a264ce497bd61b351#0" => {
                    Ratified(645)
                }
                "4fa27875bc4d00a1f40eae2b50b791d48fca4a0f8af4d44f0ceeb6c7662f689c#0" => {
                    Ratified(645)
                }
                _ => match protocol {
                    0..=8 => RatifiedCurrentEpoch,
                    _ => Unknown,
                },
            }
        }
    }

    pub mod preprod {
        use super::ProposalOutcome;
        use super::ProposalOutcome::*;

        pub fn outcome(protocol: u16, proposal: &str) -> ProposalOutcome {
            match proposal {
                // Byron intra-era hardfork
                "9972ffaee13b4afcf1a133434161ce25e8ecaf34b7a76e06b0c642125cf911a9#0" => Ratified(1),
                // Shelley hardfork
                "f48fffc65e16c3808720b38110a6d284250360108b6198a44331eb0de8e49817#0" => Ratified(3),
                // delayed pparam update
                "82b8de69d2ddd8b926e2af5979dc55ec18282d73bfe009c25bea9ef75e3fe11e#0" => {
                    Ratified(27)
                }
                "e974fecbf45ac386a76605e9e847a2e5d27c007fdd0be674cbad538e0c35fe01#0" => {
                    Canceled(180)
                }
                "ccb27f6b0d58c25ae33fd821b62c387f5230dae930afd07489fa3df56ae56522#0" => {
                    Ratified(180)
                }
                "b52f02288e3ce8c7e57455522f4edd09c12797749e2db32098ecbe980b645d45#0" => {
                    Ratified(179)
                }
                "ba588be9a6c9c5ffba7dd4166cf295ae082be53028717005d1aeceb625e65461#0" => {
                    Ratified(228)
                }
                "6f8b70a482e10ae4077d70730826ef27f72b08e148118a5171c72e7fe3c6d551#0" => {
                    Ratified(231)
                }
                "49578eba0c840e822e0688b09112f3f9baaeb51dd0e346c5a4f9d03d2cbc1953#0" => {
                    Ratified(232)
                }
                "158ef6b249b7c3ec219c62d11f0b8e766a356472d023bd7b1e736efed977f3c6#0" => {
                    Ratified(251)
                }
                _ => match protocol {
                    0..=8 => RatifiedCurrentEpoch,
                    _ => Unknown,
                },
            }
        }
    }

    pub mod mainnet {
        use super::ProposalOutcome;
        use super::ProposalOutcome::*;

        pub fn outcome(protocol: u16, proposal: &str) -> ProposalOutcome {
            match proposal {
                // Replace Interim Constitutional Committee
                "47a0e7a4f9383b1afc2192b23b41824d65ac978d7741aca61fc1fa16833d1111#0" => {
                    Ratified(580)
                }

                // Withdraw ₳99,600 for BloxBean Java Tools Maintenance and Enhancement
                "2c7f900b7ff68f317a7b0e42231d4aed36227660baf2ee9a4be7e880eb977313#0" => {
                    Ratified(575)
                }

                // Withdraw ₳104,347 for MLabs Research towards Tooling for Elliptical Curves...
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#36" => {
                    Ratified(575)
                }

                // Withdraw ₳750,000 for Cardano Product Committee: Community-driven 2030 Carda...
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#35" => {
                    Ratified(577)
                }

                // Withdraw ₳314,800 for PyCardano administered by Intersect
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#34" => {
                    Ratified(575)
                }

                // Withdraw ₳199,911 for OpShin - Python Smart Contracts for Cardano
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#33" => {
                    Ratified(575)
                }

                // Withdraw ₳26,840,000 for Input Output Research (IOR): Cardano Vision - Wor...
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#32" => {
                    Ratified(575)
                }

                // Withdraw ₳4,000,000 for Expanding Stablecoin / Cardano Native Asset Support...
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#31" => {
                    Ratified(575)
                }

                // Withdraw ₳889,500 for Cardano Ecosystem Pavilions at Exhibitions
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#29" => {
                    Ratified(577)
                }

                // Withdraw ₳3,126,000 for Ecosystem Exchange Listing and Market Making service...
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#28" => {
                    Ratified(577)
                }

                // Withdraw ₳12,000,000 for Cardano Builder DAO administered by Intersect
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#26" => {
                    Ratified(576)
                }

                // Withdraw ₳6,000,000 for Unveiling the First Unified Global Events Marketing S...
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#25" => {
                    Ratified(576)
                }

                // Withdraw ₳6,000,000 for Cardano Summit 2025 and regional tech events
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#24" => {
                    Ratified(575)
                }

                // Withdraw ₳69,459,000 for Catalyst 2025 Proposal by Input Output: Advancing De...
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#23" => {
                    Ratified(574)
                }

                // Withdraw ₳592,780 for Beyond Minimum Viable Governance: Iteratively Improvin....
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#22" => {
                    Ratified(577)
                }

                // Withdraw ₳15,750,000 for a MBO for the Cardano ecosystem: Intersect
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#21" => {
                    Ratified(575)
                }

                // Withdraw ₳212,000 for AdaStat.net Cardano blockchain explorer
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#20" => {
                    Ratified(575)
                }

                // Withdraw ₳605,000 for A free Native Asset CDN for Cardano Developers
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#19" => {
                    Ratified(577)
                }

                // Withdraw ₳266,667 for Cexplorer.io -- Developer-Focused Blockchain Explorer...
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#18" => {
                    Ratified(575)
                }

                // Withdraw ₳657,692 for Scalus - DApps Development Platform
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#17" => {
                    Ratified(575)
                }

                // Withdraw ₳583,000 for Eternl Maintenance administered by Intersect
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#16" => {
                    Ratified(575)
                }

                // Withdraw ₳700,000 for ZK Bridge administered by Intersect
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#15" => {
                    Ratified(575)
                }

                // Withdraw ₳11,070,323 for TWEAG's Proposals for multiple core budget project...
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#14" => {
                    Ratified(575)
                }

                // Withdraw ₳243,478 for MLabs Core Tool Maintenance & Enhancement: Plutarch
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#13" => {
                    Ratified(575)
                }

                // Withdraw ₳578,571 for Gerolamo - Cardano node in typescript
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#12" => {
                    Ratified(575)
                }

                // Withdraw ₳5,885,000 for OSC Budget Proposal - Paid Open Source Model...
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#11" => {
                    Ratified(575)
                }

                // Withdraw ₳600,000 for Complete Web3 developer stack to make Cardano the smart...
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#10" => {
                    Ratified(575)
                }

                // Withdraw ₳300,000 for Ledger App Rewrite administered by Intersect
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#9" => {
                    Ratified(575)
                }

                // Withdraw ₳220,914 for Dolos: Sustaining a Lightweight Cardano Data Node
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#8" => {
                    Ratified(575)
                }

                // Withdraw ₳1,161,000 for zkFold ZK Rollup administered by Intersect
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#7" => {
                    Ratified(575)
                }

                // Withdraw ₳130,903 for Lucid Evolution Maintenance administered by Intersect
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#6" => {
                    Ratified(575)
                }

                // Withdraw ₳220,914 for UTxO RPC: Sustaining Cardano Blockchain Integration
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#5" => {
                    Ratified(575)
                }

                // Withdraw ₳220,914 for Pallas: Sustaining Critical Rust Tooling for Cardano
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#4" => {
                    Ratified(575)
                }

                // Withdraw ₳424,800 for Hardware Wallets Maintenance administered by Intersect
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#3" => {
                    Ratified(575)
                }

                // Withdraw ₳1,300,000 for Blockfrost Platform community budget proposal
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#2" => {
                    Ratified(575)
                }

                // Withdraw ₳96,817,080 for 2025 Input Output Engineering Core Development Proposal
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#1" => {
                    Ratified(574)
                }

                // Withdraw ₳2,162,096 for Midgard - Optimistic Rollups administered by Intersect
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#0" => {
                    Ratified(574)
                }

                // Withdraw ₳45,217 for MLabs Core Tool Maintenance & Enhancement: Cardano.nix
                "3cf29192a0ee1a77985054072edcdb566ac14707730637c4635d8fb6813cb4c9#0" => {
                    Ratified(575)
                }

                // Amaru Treasury Withdrawal 2025
                "60ed6ab43c840ff888a8af30a1ed27b41e9f4a91a89822b2b63d1bfc52aeec45#0" => {
                    Ratified(570)
                }

                // Cardano Constitution to Replace the Interim Constitution
                "8c653ee5c9800e6d31e79b5a7f7d4400c81d44717ad4db633dc18d4c07e4a4fd#0" => {
                    Ratified(541)
                }

                // Hard Fork to Protocol Version 10 ("Plomin" Hard Fork)
                "0b19476e40bbbb5e1e8ce153523762e2b6859e7ecacbaf06eae0ee6a447e79b9#0" => {
                    Ratified(536)
                }

                // Plutus V3 Cost Model Parameter Changes Prior to Chang#2
                "b2a591ac219ce6dcca5847e0248015209c7cb0436aa6bd6863d0c1f152a60bc5#0" => {
                    Ratified(525)
                }

                // Decentralisation updates submitted one epoch before their target epoch.
                // RatifiedCurrentEpoch would enact these one epoch too early; use target
                // epoch so they enact at target_epoch + 1, matching the ppup/fpup pipeline.
                "a6713824eeef48508bd35e851bcf4021a93b5995127feb9910b1e1b88de2c225#0" => {
                    Ratified(214)
                }
                "3da44150612379b337f0865bbe1c210e8f34a9d02280803e9ea90173d3361574#0" => {
                    Ratified(215)
                }
                "319c8b8865bdc6ce896f3722aa54da9d9fd125429a7e05af1955004f69217eca#0" => {
                    Ratified(219)
                }
                "e67064c5e85b74062a13a0ed9290f8f7d6c81440e39be081a334e33b57ec810d#0" => {
                    Ratified(221)
                }
                "32c8bdd8791fee095c9074f7163410cc41eb05f5d6632afd96ab8578ad9ca215#0" => {
                    Ratified(222)
                }
                "f10937dfd495061cdb3c6ae56af6d522391205f39318acf9098969224b97d1e8#0" => {
                    Ratified(224)
                }
                "1d29f276d893e72183969dc39594c1cabadace86e8add3ce71af470c7c475b9d#0" => {
                    Ratified(225)
                }
                "05d1302ff8d070d4e7545415f81c7d824d7601694d92053026c5cd7d58a7814a#0" => {
                    Ratified(228)
                }
                "5bd9fa498676741dcf990ceb98512d91bfd0481093839827bea5abe1bbd89136#0" => {
                    Ratified(229)
                }
                "db2be7716618fb6aa775c6052a39a9efe67f6a235ca42c8c28a681094aab82ec#0" => {
                    Ratified(232)
                }
                "8fd8ea3d1933e05f6d474d315e1ff0d60e567a79f73fd3cec98b9cdac54ba75a#0" => {
                    Ratified(242)
                }
                "f6334261e19a6a4ff028684b1cad38b4f9c03290e5c24ce2ac11d6e9a33fe0b5#0" => {
                    Ratified(245)
                }
                "3dd110c031c23f9187441464edc8b84d4f9cd62df6cc3a04bf62fde5359ebd5a#0" => {
                    Ratified(246)
                }
                "42362f1aea613711dfe527541f5a8de71579e6980d9887a1d4db29ef1b601863#0" => {
                    Ratified(247)
                }
                "054257a09038d69832949b07b8d97a24687919a777e967933d85469480777e2e#0" => {
                    Ratified(248)
                }
                "abbbf81e0fb1e4de222df18a9ca5fdfe3e9b9e2efc0cde6c42552789746c9852#0" => {
                    Ratified(249)
                }
                "1bd8310b660c3086530763e67edae7087654a8642c981b3bc4ea89f33f3ed67c#0" => {
                    Ratified(250)
                }
                "ee4876fa27951d12b17b647dbbc46f303b7b0e2dd416d92e88a6da7a58c6851e#0" => {
                    Ratified(252)
                }
                "e2e52847e2b1d47032cee3b91419ec0f5078b7d31fa5e939bde6a77e97a9f04b#0" => {
                    Ratified(253)
                }
                "956fb654686351da5367f326902e33b1200448624d756ca3ad7cf77db4c1bf52#0" => {
                    Ratified(255)
                }
                "f7902182392800c8ea0b6fa048100263ae522fe5ee2fb1388f881921018dd6fc#0" => {
                    Ratified(256)
                }
                // Non-d-parameter proposals submitted one epoch before target
                "51fa37794d2107d7d8705cd69594c5162ced13f922235a99d06aff20c64656b6#0" => {
                    Ratified(289)
                }
                "8230f33cd7ad3f8601e94ea2b18abdc591187e190ea8ebecc25e20fc66200f13#0" => {
                    Ratified(364)
                }
                "3abda97c78c71e8a21473529aca94d78d364dfa1a866ef8245885e18085b4e4c#0" => {
                    Ratified(364)
                }
                "a83f479c5635e1e563a19f6e72a1be59fb082bbf31de90cc176850ee799b08ac#0" => {
                    Ratified(393)
                }
                "62c3c13187423c47f629e6187f36fbd61a9ba1d05d101588340cfbfdf47b22d2#0" => {
                    Ratified(393)
                }
                // Treasury Withdrawal for Catalyst Fund 14
                "03f671791fd97011f30e4d6b76c9a91f4f6bcfb60ee37e5399b9545bb3f2757a#0" => {
                    Ratified(597)
                }
                // Replace Constitutional Committee (2nd replacement)
                "4dab331457b61b824bbc6ba4b9d9be4750e25c0b5dd42207aeb63c7431a6b704#0" => {
                    Ratified(601)
                }
                // Treasury Withdrawal
                "f8393f1ff814d3d52336a97712361fed933d9ef9e8d0909e1d31536a549fd22f#0" => {
                    Ratified(605)
                }
                // New Constitution
                "91a79f5c934b7c91e3027736d565080c2b6611fb8484b1156fdf16121fcfb410#0" => {
                    Ratified(608)
                }

                _ => match protocol {
                    0..=8 => RatifiedCurrentEpoch,
                    _ => Unknown,
                },
            }
        }
    }

    pub fn outcome(magic: u32, protocol: u16, proposal: &str) -> ProposalOutcome {
        match magic {
            764824073 => mainnet::outcome(protocol, proposal),
            1 => preprod::outcome(protocol, proposal),
            2 => preview::outcome(protocol, proposal),
            _ => ProposalOutcome::Unknown,
        }
    }
}
