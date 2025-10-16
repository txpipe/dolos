pub mod proposals {
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
}
