pub mod pointers {
    use pallas::ledger::{addresses::Pointer, primitives::StakeCredential};

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

            // TODO: check actual certs behind this txs
            (82626550, 0, 0) => None,
            (2498243, 27, 3) => None,
            (10612742, 0, 0) => None,
            (70549345, 1, 0) => None,

            x => {
                dbg!(x);
                //panic!("unknown pointer: {:?}", x);
                None
            }
        }
    }
}

pub mod forks {
    use crate::{EraProtocol, EraTransition};
    use pallas::ledger::primitives::Epoch;
    use pallas::network::miniprotocols::PREPROD_MAGIC;

    mod preprod {
        use super::*;

        pub fn era_transition(epoch: Epoch) -> Option<EraTransition> {
            match epoch {
                2 => Some(EraTransition {
                    prev_version: EraProtocol::from(0),
                    new_version: EraProtocol::from(1),
                }),
                3 => Some(EraTransition {
                    prev_version: EraProtocol::from(1),
                    new_version: EraProtocol::from(2),
                }),
                _ => None,
            }
        }
    }

    pub fn era_transition(magic: u32, epoch: Epoch) -> Option<EraTransition> {
        match magic as u64 {
            PREPROD_MAGIC => preprod::era_transition(epoch),
            _ => None,
        }
    }
}

pub mod proposals {
    use pallas::ledger::primitives::Epoch;

    pub mod preview {
        use crate::Epoch;

        /// Get the enacment epoch for each proposal, indexed as
        /// transation#index, hex encoded.
        ///
        /// https://preview.gov.tools/outcomes?sort=newestFirst&status=enacted
        pub fn enactment_epoch_by_proposal_id(proposal: &str) -> Option<Epoch> {
            match proposal {
                // Remove Expired Constitutional Committee Members
                "ac993231c39a4ee13bcf888e971e099809c4c08d96a7572aa3611a5ed42fa7d4#0" => Some(1013),

                // Update the Cardano committeeMinSize Parameter
                "602d8572263929bdb0aba911d45ecf4bf0a2430e2f263f89df7114d168985f57#0" => Some(999),

                // Ensure Sufficient Long Term CC Expirations
                "6214314b6d6a30118d259c9597c0e0120b76aa521e322044c4290fcaac86e27a#0" => Some(998),

                // Update the Cardano Preview Network Constitutional Committee Members
                "f4188b8676bae7f3bb26626e57d1bf6b5212dc078581d00233e27f55a1392b0e#0" => Some(994),

                // Increase dRepActivity from 20 to 31
                "35b81b424956f018bb4a4bb9e160375c1921a3b40b60a1efc539bcd6b5b20159#0" => Some(964),

                // Hard Fork to Protocol Version 10
                "049ae5d612b2fa825655809133b023d60c7f8cac683c278cf95de1622e4592f3#0" => Some(743),

                // Plutus V3 Cost Model Parameter Changes Prior to Chang#2
                "1f47f3cf2e4f9109be2efe9182cae08307e5778bdfea2150e6903c48edca0b8f#0" => Some(736),
                _ => None,
            }
        }

        pub fn dropped_epoch_by_proposal_id(proposal: &str) -> Option<Epoch> {
            match proposal {
                "2841a581076167a0662f1b4f1a38bcc8eff386f9ce45c33ae33b1fe8289de210#0" => Some(736),
                "95af8608dc7aaf5c73025066b509a2c11c829f2984009ac92e7053d123fbce57#0" => Some(736),
                "3d573646d495b939ad019afc2653fbad023615b4ee5643d41fd9fa9cdb91fe29#0" => Some(994),
                "4c7b63801d0a0f0bb3d83cf9f0951ceb0e453e1532a3f5a1f2988a8d9778a862#0" => Some(994),
                "25a16ada4a57fd29a1ac5f62f585d923ffe3e23321512380dfd276f6c73b1451#0" => Some(1013),
                "77cc6292907df30d4340aa389dda453ea03aae1aa18a71c1856ac10851498188#0" => Some(1013),
                "8f6918be1e1762cae1a378882d0b4037e0e7176ce69ce4f874a170edbc4d837d#0" => Some(994),
                "045bbfb7ec34ec5e5a4fe110a59cfe0ce799018cef0d68483ec2b704f6503b9f#0" => Some(994),
                "4b0b84ce4a791228a9b844b70cc2cc8e19b5dc009422db062ff139abcb7a20f7#0" => Some(994),
                "5c526a482838979d2ffc5864b0f8878593080d3cd3d6b4759c66ffac81b17402#0" => Some(994),
                "2eb2213de142e5c95a7f2b5b5a66fbb181b19782eb3b1f5db4485ffc558da9f3#0" => Some(994),
                "5375aedecf005d3e212795f018573ea8f31dfc1b9d98d9ee5f24ebfb93bfb83b#0" => Some(994),
                "fad5f42e6648c04b506864a787d684ac03d54df0232473e81ccd404867af76c5#0" => Some(994),
                "0d732b47248d70a8aa61e3560f3fddcaf3809d82c065cbdfaa36f7931db637d7#0" => Some(994),
                "16d11d0a34d76bffbb394c71b6b04696d80373b12b8dc849ccd0d981e91c66e5#0" => Some(994),
                "5552b8a209f9ffadc5aa583ef583caaf1776ca53bc98972165a5b583c4045328#0" => Some(994),
                "9a0ba26ff6fcf78cc30760eded027f4a23916724307c5b8196a0e13885ecc717#0" => Some(994),
                "eede35403b0f5d3db8e6fe80a038f24c498c5b4675817e32a67e338f29511d94#0" => Some(994),
                "c9fd3e7b0ae40a1ac3e656931376d79a7d2b33727e5083c91d89d74b7d6ab765#0" => Some(994),
                "1ed2479b2abab685bd0c148e880d9b4c006bf21991b0b5a0000e66df62d6ee4a#0" => Some(994),
                "58d46770900cd81bd5529e4f8b8c2c03e6de897c5ca89f55cbc7451176fe7ac5#0" => Some(994),
                "4f25b3fffda8ef20bc8a5ccf1fb6e9a7ab267046c49a6d70bc9e642d0d049868#0" => Some(994),
                "7f630df922fd14374ce402ed989d928817c56fd185a4c76a359c5a8c689a676e#0" => Some(994),
                "88f29172775d69e08b2efd09d82e5d80e2139a58d53401984f47ae1e44b4017f#0" => Some(994),
                "90c124b693fe2bddc19b4cd0a4e7af92f4b668355e5e8607b828c753d66dbbfc#0" => Some(994),
                "98f2ffe1c2ae6ae57cafb5ee4829e6c656c5e35ee38bce1688d8537fb4707f1e#0" => Some(994),
                "b9dc48d2defba697dd3bdb2316808fe894167a319a678b0ee246b292a9328b10#0" => Some(994),
                "ca61b3b660b626bfc49135ee5006555630679c20c4033642fde2f4de16d5946d#0" => Some(994),
                "d61d08ef309dacc162507f9c3b99080f1be1fa31ea59319684e2d569f9ee4970#0" => Some(994),
                "e7082ec2717eff54fa78c2812a28d3799f1256aa00f1657f20f03dffeb8ac55d#0" => Some(994),
                "0176514f66026da634cfd9c37e4575645a80aca4e69ad83fe0e468be5f4b5c0c#0" => Some(1096),
                "609896ea7a615392bdc8c9ef0df74338ecd16d581c0cd698aa539c3f782650e3#0" => Some(1096),
                "00be4823e37a7a70875408bf9df377ee48c0fa0d02cbe118acfef8cd0b92d3f7#0" => Some(998),
                "51e82c898ba142adefb676277b9f8f48487569c3a8528c2f68c0aaa038315519#0" => Some(998),
                "60233953f6e9e56333bf9acbfd2a7262fecfc60b7f4487e59b0bfce79fbe749a#0" => Some(998),
                "c3f38851329c7829eadc86c082e160f7d47e1c03e16e3281420bb741a7d438e9#0" => Some(1096),
                "4bc0ee7f2cc3a4e47b50b38431ba813893d5f1dbb3cee42a31f8deb57934c987#0" => Some(1096),
                "f046a88280e6c5b18dd057027964860f6b0b7918f4532d50455ad257a14a70ed#0" => Some(1096),
                _ => None,
            }
        }
    }

    pub mod preprod {
        use crate::Epoch;

        /// Get the enacment epoch for each proposal, indexed as
        /// transation#index, hex encoded.
        ///
        /// https://pre-prod.gov.tools/outcomes?sort=newestFirst&status=enacted
        pub fn enactment_epoch_by_proposal_id(proposal: &str) -> Option<Epoch> {
            match proposal {
                // Update the Cardano committeeMinSize Parameter
                "49578eba0c840e822e0688b09112f3f9baaeb51dd0e346c5a4f9d03d2cbc1953#0" => Some(233),

                // Update the Cardano committeeMinSize Parameter
                "6f8b70a482e10ae4077d70730826ef27f72b08e148118a5171c72e7fe3c6d551#0" => Some(232),

                // Update the Cardano Preprod Network Constitutional Committee Members
                "ba588be9a6c9c5ffba7dd4166cf295ae082be53028717005d1aeceb625e65461#0" => Some(229),

                // Hard Fork to Protocol Version 10
                "ccb27f6b0d58c25ae33fd821b62c387f5230dae930afd07489fa3df56ae56522#0" => Some(181),

                // Plutus V3 Cost Model Parameter Changes Prior to Chang#2
                "b52f02288e3ce8c7e57455522f4edd09c12797749e2db32098ecbe980b645d45#0" => Some(180),

                _ => None,
            }
        }
    }

    pub mod mainnet {
        use crate::Epoch;

        /// Get the enacment epoch for each proposal, indexed as
        /// transation#index, hex encoded.
        ///
        /// Information for this can be found here:
        /// https://gov.tools/outcomes?sort=newestFirst&status=enacted.
        /// This was build doing:
        ///
        /// ```shell
        /// curl \
        ///     'https://be.outcomes.gov.tools/governance-actions?search=&filters=enacted&sort=newestFirst&page=1&limit=100' \
        ///     | jq -r '.[] | {"description": .title, "id": "\(.tx_hash)#\(.index)", "enacted": .status.enacted_epoch} | "// \(.description)\n\"\(.id)\" => Some(\(.enacted)),\n" ' \
        ///     | pbcopy
        /// ```
        pub fn enactment_epoch_by_proposal_id(proposal: &str) -> Option<Epoch> {
            match proposal {
                // Replace Interim Constitutional Committee
                "47a0e7a4f9383b1afc2192b23b41824d65ac978d7741aca61fc1fa16833d1111#0" => Some(581),

                // Withdraw ₳99,600 for BloxBean Java Tools Maintenance and Enhancement
                "2c7f900b7ff68f317a7b0e42231d4aed36227660baf2ee9a4be7e880eb977313#0" => Some(576),

                // Withdraw ₳104,347 for MLabs Research towards Tooling for Elliptical Curves...
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#36" => Some(576),

                // Withdraw ₳750,000 for Cardano Product Committee: Community-driven 2030 Carda...
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#35" => Some(578),

                // Withdraw ₳314,800 for PyCardano administered by Intersect
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#34" => Some(576),

                // Withdraw ₳199,911 for OpShin - Python Smart Contracts for Cardano
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#33" => Some(576),

                // Withdraw ₳26,840,000 for Input Output Research (IOR): Cardano Vision - Wor...
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#32" => Some(576),

                // Withdraw ₳4,000,000 for Expanding Stablecoin / Cardano Native Asset Support...
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#31" => Some(576),

                // Withdraw ₳889,500 for Cardano Ecosystem Pavilions at Exhibitions
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#29" => Some(578),

                // Withdraw ₳3,126,000 for Ecosystem Exchange Listing and Market Making service...
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#28" => Some(578),

                // Withdraw ₳12,000,000 for Cardano Builder DAO administered by Intersect
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#26" => Some(577),

                // Withdraw ₳6,000,000 for Unveiling the First Unified Global Events Marketing S...
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#25" => Some(577),

                // Withdraw ₳6,000,000 for Cardano Summit 2025 and regional tech events
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#24" => Some(576),

                // Withdraw ₳69,459,000 for Catalyst 2025 Proposal by Input Output: Advancing De...
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#23" => Some(575),

                // Withdraw ₳592,780 for Beyond Minimum Viable Governance: Iteratively Improvin....
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#22" => Some(578),

                // Withdraw ₳15,750,000 for a MBO for the Cardano ecosystem: Intersect
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#21" => Some(576),

                // Withdraw ₳212,000 for AdaStat.net Cardano blockchain explorer
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#20" => Some(576),

                // Withdraw ₳605,000 for A free Native Asset CDN for Cardano Developers
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#19" => Some(578),

                // Withdraw ₳266,667 for Cexplorer.io -- Developer-Focused Blockchain Explorer...
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#18" => Some(576),

                // Withdraw ₳657,692 for Scalus - DApps Development Platform
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#17" => Some(576),

                // Withdraw ₳583,000 for Eternl Maintenance administered by Intersect
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#16" => Some(576),

                // Withdraw ₳700,000 for ZK Bridge administered by Intersect
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#15" => Some(576),

                // Withdraw ₳11,070,323 for TWEAG's Proposals for multiple core budget project...
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#14" => Some(576),

                // Withdraw ₳243,478 for MLabs Core Tool Maintenance & Enhancement: Plutarch
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#13" => Some(576),

                // Withdraw ₳578,571 for Gerolamo - Cardano node in typescript
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#12" => Some(576),

                // Withdraw ₳5,885,000 for OSC Budget Proposal - Paid Open Source Model...
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#11" => Some(576),

                // Withdraw ₳600,000 for Complete Web3 developer stack to make Cardano the smart...
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#10" => Some(576),

                // Withdraw ₳300,000 for Ledger App Rewrite administered by Intersect
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#9" => Some(576),

                // Withdraw ₳220,914 for Dolos: Sustaining a Lightweight Cardano Data Node
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#8" => Some(576),

                // Withdraw ₳1,161,000 for zkFold ZK Rollup administered by Intersect
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#7" => Some(576),

                // Withdraw ₳130,903 for Lucid Evolution Maintenance administered by Intersect
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#6" => Some(576),

                // Withdraw ₳220,914 for UTxO RPC: Sustaining Cardano Blockchain Integration
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#5" => Some(576),

                // Withdraw ₳220,914 for Pallas: Sustaining Critical Rust Tooling for Cardano
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#4" => Some(576),

                // Withdraw ₳424,800 for Hardware Wallets Maintenance administered by Intersect
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#3" => Some(576),

                // Withdraw ₳1,300,000 for Blockfrost Platform community budget proposal
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#2" => Some(576),

                // Withdraw ₳96,817,080 for 2025 Input Output Engineering Core Development Proposal
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#1" => Some(575),

                // Withdraw ₳2,162,096 for Midgard - Optimistic Rollups administered by Intersect
                "8ad3d454f3496a35cb0d07b0fd32f687f66338b7d60e787fc0a22939e5d8833e#0" => Some(575),

                // Withdraw ₳45,217 for MLabs Core Tool Maintenance & Enhancement: Cardano.nix
                "3cf29192a0ee1a77985054072edcdb566ac14707730637c4635d8fb6813cb4c9#0" => Some(576),

                // Amaru Treasury Withdrawal 2025
                "60ed6ab43c840ff888a8af30a1ed27b41e9f4a91a89822b2b63d1bfc52aeec45#0" => Some(571),

                // Cardano Constitution to Replace the Interim Constitution
                "8c653ee5c9800e6d31e79b5a7f7d4400c81d44717ad4db633dc18d4c07e4a4fd#0" => Some(542),

                // Hard Fork to Protocol Version 10 ("Plomin" Hard Fork)
                "0b19476e40bbbb5e1e8ce153523762e2b6859e7ecacbaf06eae0ee6a447e79b9#0" => Some(537),

                // Plutus V3 Cost Model Parameter Changes Prior to Chang#2
                "b2a591ac219ce6dcca5847e0248015209c7cb0436aa6bd6863d0c1f152a60bc5#0" => Some(526),

                _ => None,
            }
        }
    }

    pub fn enactment_epoch_by_proposal_id(magic: u32, proposal: &str) -> Option<Epoch> {
        match magic {
            764824073 => mainnet::enactment_epoch_by_proposal_id(proposal),
            1 => preprod::enactment_epoch_by_proposal_id(proposal),
            2 => preview::enactment_epoch_by_proposal_id(proposal),
            _ => None,
        }
    }

    pub fn dropped_epoch_by_proposal_id(magic: u32, proposal: &str) -> Option<Epoch> {
        // TODO: complete mainnet and preprod
        match magic {
            //764824073 => mainnet::dropped_epoch_by_proposal_id(proposal),
            //1 => preprod::dropped_epoch_by_proposal_id(proposal),
            2 => preview::dropped_epoch_by_proposal_id(proposal),
            _ => None,
        }
    }
}
