use axum::http::StatusCode;
use dolos_cardano::{
    indexes::{AsyncCardanoQueryExt, SlotOrder},
    model::{AccountState, ProposalState},
    pallas_extras, AccountKey, ChainSummary,
};
use dolos_core::{ArchiveStore as _, Domain};
use futures_util::StreamExt;
use pallas::{
    crypto::hash::{Hash, Hasher},
    ledger::{
        addresses::{Address, Network, StakeAddress, StakePayload},
        primitives::Epoch,
        traverse::{MultiEraBlock, MultiEraCert, MultiEraTx},
    },
};

use crate::{error::Error, pagination::Pagination, Facade};

mod addresses;
mod addresses_assets;
mod addresses_total;
mod by_stake;
mod delegations;
mod history;
mod mirs;
mod registrations;
mod rewards;
mod transactions;
mod utxos;
mod withdrawals;

pub use addresses::by_stake_addresses;
pub use addresses_assets::by_stake_addresses_assets;
pub use addresses_total::by_stake_addresses_total;
pub use by_stake::by_stake;
pub use delegations::by_stake_delegations;
pub use history::by_stake_history;
pub use mirs::by_stake_mirs;
pub use registrations::by_stake_registrations;
pub use rewards::by_stake_rewards;
pub use transactions::by_stake_transactions;
pub use utxos::by_stake_utxos;
pub use withdrawals::by_stake_withdrawals;

struct AccountKeyParam {
    address: StakeAddress,
    account: AccountKey,
}

impl AccountKeyParam {
    /// The key the account-certs archive tag is filed under: the account
    /// key's bytes, which the indexer writes as the credential's CBOR.
    fn cert_tag(&self) -> Vec<u8> {
        dolos_core::keys::encode(&self.account).to_vec()
    }
}

fn stake_address_from_cip_19_credential(address: &str, network: Network) -> Option<StakeAddress> {
    let (hrp, payload) = bech32::decode(address).ok()?;

    let payload = match hrp.as_str() {
        // Ed25519 verification key.
        "stake_vk" => {
            let key: [u8; 32] = <[u8; 32]>::try_from(&payload[..]).ok()?;
            let hash: Hash<28> = Hasher::<224>::hash(&key);
            StakePayload::Stake(hash)
        }
        // Raw key-hash credential.
        "stake_vkh" => {
            StakePayload::Stake(Hash::<28>::from(<[u8; 28]>::try_from(&payload[..]).ok()?))
        }
        // Raw script-hash credential.
        "script" => {
            StakePayload::Script(Hash::<28>::from(<[u8; 28]>::try_from(&payload[..]).ok()?))
        }
        _ => return None,
    };

    Some(StakeAddress::new(network, payload))
}

fn parse_account_key_param(address: &str, network: Network) -> Result<AccountKeyParam, Error> {
    let address =
        if let Some(stake_address) = stake_address_from_cip_19_credential(address, network) {
            stake_address
        } else {
            let parsed = pallas::ledger::addresses::Address::from_bech32(address)
                .map_err(|_| Error::InvalidStakeAddress)?;

            let stake_address = match parsed {
                Address::Shelley(x) => pallas_extras::shelley_address_to_stake_address(&x),
                Address::Stake(x) => Some(x),
                _ => None,
            };

            stake_address.ok_or(Error::InvalidStakeAddress)?
        };

    let account = AccountKey(dolos_cardano::pallas_extras::stake_address_to_cred(
        &address,
    ));

    Ok(AccountKeyParam { address, account })
}

/// Whether a proposal names the credential as its return account: a live one
/// in state, or a resolved one in the final-row log.
fn account_appears_in_proposals<D>(
    domain: &Facade<D>,
    account: &AccountKey,
) -> Result<bool, StatusCode>
where
    Option<ProposalState>: From<D::Entity>,
    D: Domain + Clone + Send + Sync + 'static,
{
    let names =
        |proposal: &ProposalState| proposal.reward_account.as_ref() == Some(account.credential());

    for item in domain.iter_cardano_entities::<ProposalState>(None)? {
        let (_, proposal) = item.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        if names(&proposal) {
            return Ok(true);
        }
    }

    let logged = domain
        .archive()
        .iter_logs_typed::<ProposalState>(None)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    for item in logged {
        let (_, _, proposal) = item.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        if names(&proposal) {
            return Ok(true);
        }
    }

    Ok(false)
}

fn account_exists<D>(domain: &Facade<D>, account: &AccountKey) -> Result<bool, StatusCode>
where
    Option<AccountState>: From<D::Entity>,
    Option<ProposalState>: From<D::Entity>,
    D: Domain + Clone + Send + Sync + 'static,
{
    if domain.cardano_entity_exists::<AccountState>(account)? {
        return Ok(true);
    }
    account_appears_in_proposals(domain, account)
}

struct AccountActivityModelBuilder<T> {
    stake_address: StakeAddress,
    network: Network,
    page_size: usize,
    page_number: usize,
    skipped: usize,
    items: Vec<T>,
}

struct AccountActionContext<'a, D: Domain> {
    domain: &'a Facade<D>,
    epoch: Epoch,
    network: Network,
    block: &'a MultiEraBlock<'a>,
    chain: &'a ChainSummary,
}

impl<T> AccountActivityModelBuilder<T> {
    fn new(
        stake_address: StakeAddress,
        network: Network,
        page_size: usize,
        page_number: usize,
    ) -> Self {
        Self {
            stake_address,
            network,
            page_size,
            page_number,
            skipped: 0,
            items: vec![],
        }
    }

    fn should_skip(&self) -> bool {
        self.skipped < (self.page_number - 1) * self.page_size
    }

    fn add(&mut self, item: T) {
        if self.should_skip() {
            self.skipped += 1;
        } else {
            self.items.push(item);
        }
    }

    fn needs_more(&self) -> bool {
        self.items.len() < self.page_size
    }

    fn scan_block_certs<D, F>(
        &mut self,
        domain: &Facade<D>,
        epoch: Epoch,
        block: &MultiEraBlock,
        chain: &ChainSummary,
        mapper: F,
        order: crate::pagination::Order,
    ) -> Result<(), StatusCode>
    where
        D: Domain,
        F: Fn(
            &AccountActionContext<'_, D>,
            &StakeAddress,
            &MultiEraTx,
            &MultiEraCert,
        ) -> Result<Option<T>, StatusCode>,
    {
        let txs = block.txs();
        let mut block_items = vec![];
        let ctx = AccountActionContext {
            domain,
            epoch,
            network: self.network,
            block,
            chain,
        };

        for tx in txs {
            for cert in tx.certs() {
                if let Some(model) = mapper(&ctx, &self.stake_address, &tx, &cert)? {
                    block_items.push(model);
                }
            }
        }

        if matches!(order, crate::pagination::Order::Desc) {
            block_items.reverse();
        }

        for item in block_items {
            if !self.needs_more() {
                break;
            }
            self.add(item);
        }

        Ok(())
    }
}

async fn by_stake_actions<D, F, T>(
    stake_address: &str,
    pagination: Pagination,
    domain: Facade<D>,
    mapper: F,
) -> Result<Vec<T>, Error>
where
    Option<AccountState>: From<D::Entity>,
    Option<ProposalState>: From<D::Entity>,
    F: Fn(
        &AccountActionContext<'_, D>,
        &StakeAddress,
        &MultiEraTx,
        &MultiEraCert,
    ) -> Result<Option<T>, StatusCode>,
    D: Domain + Clone + Send + Sync + 'static,
{
    let network = domain.get_network_id()?;
    let account_key = parse_account_key_param(stake_address, network)?;

    if !account_exists(&domain, &account_key.account)? {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let chain = domain
        .get_chain_summary()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let network = domain
        .get_network_id()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let cert_tag = account_key.cert_tag();

    let mut builder = AccountActivityModelBuilder::new(
        account_key.address,
        network,
        pagination.count,
        pagination.page as usize,
    );

    let (start_slot, end_slot) = pagination.start_and_end_slots(&domain).await?;
    let stream = domain.query().blocks_by_account_certs_stream(
        &cert_tag,
        start_slot,
        end_slot,
        SlotOrder::from(pagination.order),
    );

    let mut stream = Box::pin(stream);

    while let Some(res) = stream.next().await {
        if !builder.needs_more() {
            break;
        }

        let (slot, block) = res.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let Some(block) = block else {
            continue;
        };

        let (epoch, _) = chain.slot_epoch(slot);

        let block = MultiEraBlock::decode(&block).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        builder.scan_block_certs(&domain, epoch, &block, &chain, &mapper, pagination.order)?;
    }

    Ok(builder.items)
}

fn address_belongs_to_account(address: &Address, account: &[u8]) -> bool {
    match address {
        Address::Shelley(shelley) => pallas_extras::shelley_address_to_stake_address(shelley)
            .map(|x| x.to_vec() == account)
            .unwrap_or(false),
        Address::Stake(stake) => stake.to_vec() == account,
        Address::Byron(_) => false,
    }
}

#[cfg(test)]
pub(super) mod testing {
    use super::*;
    use crate::test_support::TestApp;

    pub fn invalid_stake_address() -> &'static str {
        "not-a-stake"
    }

    pub fn missing_stake_address() -> &'static str {
        "stake_test1uqysjzgfpyysjzgfpyysjzgfpyysjzgfpyysjzgfpyysjzgeeww5k"
    }

    pub async fn assert_status(app: &TestApp, path: &str, expected: StatusCode) {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            expected,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
    }

    pub fn asset_unit(policy: [u8; 28]) -> String {
        format!("{}{}", hex::encode(policy), hex::encode(b"tok"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestApp;
    use blockfrost_openapi::models::{
        account_addresses_total::AccountAddressesTotal, account_content::AccountContent,
    };
    use dolos_core::{StateStore as _, StateWriter as _};
    use dolos_testing::synthetic::SyntheticBlockConfig;

    fn encode_bech32(hrp: &str, payload: &[u8]) -> String {
        let hrp = bech32::Hrp::parse(hrp).expect("invalid hrp");
        bech32::encode::<bech32::Bech32>(hrp, payload).expect("failed to encode bech32")
    }

    #[test]
    fn cip_19_credential_forms_resolve_to_canonical_stake_address() {
        let network = Network::Testnet;

        let vk = [7u8; 32];
        let key_hash: Hash<28> = Hasher::<224>::hash(&vk);
        let script_hash = Hash::<28>::from([9u8; 28]);

        // `stake_vk`: Ed25519 key hashed into a key credential.
        let canonical_key = StakeAddress::new(network, StakePayload::Stake(key_hash));
        let expected_key =
            parse_account_key_param(&canonical_key.to_bech32().expect("bech32"), network)
                .expect("canonical key address")
                .account;

        let stake_vk = parse_account_key_param(&encode_bech32("stake_vk", &vk), network)
            .expect("stake_vk resolves");
        assert_eq!(stake_vk.account, expected_key);
        assert_eq!(stake_vk.address, canonical_key);

        // `stake_vkh`: raw key-hash credential.
        let stake_vkh =
            parse_account_key_param(&encode_bech32("stake_vkh", key_hash.as_ref()), network)
                .expect("stake_vkh resolves");
        assert_eq!(stake_vkh.account, expected_key);
        assert_eq!(stake_vkh.address, canonical_key);

        // `script`: raw script-hash credential.
        let canonical_script = StakeAddress::new(network, StakePayload::Script(script_hash));
        let expected_script =
            parse_account_key_param(&canonical_script.to_bech32().expect("bech32"), network)
                .expect("canonical script address")
                .account;

        let script =
            parse_account_key_param(&encode_bech32("script", script_hash.as_ref()), network)
                .expect("script resolves");
        assert_eq!(script.account, expected_script);
        assert_eq!(script.address, canonical_script);
    }

    #[test]
    fn malformed_credential_forms_are_rejected() {
        let network = Network::Testnet;

        let assert_bad_request = |address: String| {
            let result = parse_account_key_param(&address, network).err();
            assert!(matches!(result, Some(Error::InvalidStakeAddress)));
        };

        // `stake_vk` requires a 32-byte key.
        assert_bad_request(encode_bech32("stake_vk", &[7u8; 16]));
        // `stake_vkh` requires a 28-byte hash.
        assert_bad_request(encode_bech32("stake_vkh", &[7u8; 20]));
        // `script` requires a 28-byte hash.
        assert_bad_request(encode_bech32("script", &[9u8; 32]));
        // An unknown prefix is not a credential and is not a valid address.
        assert_bad_request(encode_bech32("addr_vk", &[7u8; 32]));
    }

    fn proposal_return_accounts_block(
        slot: u64,
        block_number: u64,
        prev_hash: Hash<32>,
        valid: bool,
    ) -> dolos_core::RawBlock {
        use pallas::{
            codec::utils::{NonEmptySet, Set},
            ledger::primitives::conway::{Anchor, GovAction, ProposalProcedure, TransactionBody},
        };

        let account = |payload| StakeAddress::new(Network::Testnet, payload).to_vec();
        let proposals = [
            StakePayload::Stake(Hash::from([0x93; 28])),
            StakePayload::Script(Hash::from([0x93; 28])),
        ]
        .into_iter()
        .map(|payload| ProposalProcedure {
            deposit: 100_000_000,
            reward_account: account(payload).into(),
            gov_action: GovAction::Information,
            anchor: Anchor {
                url: "https://example.invalid/proposal".to_string(),
                content_hash: Hash::from([0; 32]),
            },
        })
        .collect::<Vec<_>>();

        let body = TransactionBody {
            inputs: Set::from(vec![]),
            outputs: vec![],
            fee: 0,
            ttl: None,
            certificates: None,
            withdrawals: None,
            auxiliary_data_hash: None,
            validity_interval_start: None,
            mint: None,
            script_data_hash: None,
            collateral: None,
            required_signers: None,
            network_id: None,
            collateral_return: None,
            total_collateral: None,
            reference_inputs: None,
            voting_procedures: None,
            proposal_procedures: Some(NonEmptySet::try_from(proposals).unwrap()),
            treasury_value: None,
            donation: None,
        };

        let (_, raw) = dolos_testing::blocks::make_conway_block_with_txs_at(
            slot,
            block_number,
            Some(prev_hash),
            vec![(body, valid)],
            None,
        );
        raw
    }

    /// The account endpoints of an account that only proposals name as their
    /// return account: proposals that are `valid`, `resolved` and, when
    /// `moved`, moved to the final-row log as ESTART moves them.
    async fn assert_proposal_account_endpoints(valid: bool, resolved: bool, moved: bool) {
        use dolos_core::import::ImportExt as _;

        let mut imported = None;
        let app = TestApp::new_with_cfg_and_setup(SyntheticBlockConfig::default(), |domain, _| {
            let tip = domain.state().read_cursor().unwrap().unwrap();
            let block = domain
                .archive()
                .get_block_by_slot(&tip.slot())
                .unwrap()
                .unwrap();
            let number = MultiEraBlock::decode(&block).unwrap().number();
            domain
                .import_blocks(vec![proposal_return_accounts_block(
                    tip.slot() + 1,
                    number + 1,
                    tip.hash().unwrap(),
                    valid,
                )])
                .expect("import proposal block");

            if valid && resolved {
                let writer = domain.state().start_writer().unwrap();
                for item in domain
                    .state()
                    .iter_entities_typed::<ProposalState>(None)
                    .unwrap()
                {
                    let (key, mut proposal) = item.unwrap();
                    if proposal.idx == 0 {
                        proposal.ratified_epoch = Some(2);
                    } else {
                        proposal.canceled_epoch = Some(3);
                    }
                    writer.write_entity_typed(&key, &proposal).unwrap();
                }
                writer.commit().unwrap();
            }

            if moved {
                crate::routes::governance::testing::move_proposals_to_log(domain, true);

                assert!(domain
                    .state()
                    .iter_entities_typed::<ProposalState>(None)
                    .unwrap()
                    .next()
                    .is_none());
            }

            imported = Some(domain.clone());
        });

        let imported = imported.unwrap();
        for payload in [
            StakePayload::Stake(Hash::from([0x93; 28])),
            StakePayload::Script(Hash::from([0x93; 28])),
        ] {
            let address = StakeAddress::new(Network::Testnet, payload);
            let credential = pallas_extras::stake_address_to_cred(&address);
            let key = AccountKey::from(credential);
            assert!(imported
                .state()
                .read_entity_typed::<AccountState>(&key)
                .unwrap()
                .is_none());
            let account = address.to_bech32().unwrap();
            for suffix in [
                "registrations",
                "delegations",
                "withdrawals",
                "mirs",
                "rewards",
                "history",
                "addresses",
                "addresses/assets",
                "transactions",
                "utxos",
            ] {
                let path = format!("/accounts/{account}/{suffix}");
                let (status, bytes) = app.get_bytes(&path).await;
                if valid || suffix == "utxos" {
                    assert_eq!(
                        status,
                        StatusCode::OK,
                        "{path}: {}",
                        String::from_utf8_lossy(&bytes)
                    );
                    let rows: Vec<serde_json::Value> = serde_json::from_slice(&bytes).unwrap();
                    assert!(rows.is_empty(), "{path}: {rows:?}");
                } else {
                    assert_eq!(status, StatusCode::NOT_FOUND, "{path}");
                }
            }
            let path = format!("/accounts/{account}");
            let (status, bytes) = app.get_bytes(&path).await;
            if valid {
                assert_eq!(status, StatusCode::OK, "{path}");
                let content: AccountContent = serde_json::from_slice(&bytes).unwrap();
                assert!(!content.registered);
                assert!(!content.active);
                assert_eq!(content.controlled_amount, "0");
                assert_eq!(content.withdrawable_amount, "0");
                assert!(content.pool_id.is_none());
                assert!(content.drep_id.is_none());
            } else {
                assert_eq!(status, StatusCode::NOT_FOUND, "{path}");
            }
            let path = format!("/accounts/{account}/addresses/total");
            let (status, bytes) = app.get_bytes(&path).await;
            if valid {
                assert_eq!(status, StatusCode::OK, "{path}");
                let total: AccountAddressesTotal = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(total.tx_count, 0);
            } else {
                assert_eq!(status, StatusCode::NOT_FOUND, "{path}");
            }
            assert!(imported
                .state()
                .read_entity_typed::<AccountState>(&key)
                .unwrap()
                .is_none());
        }
    }

    #[tokio::test]
    async fn accounts_named_only_by_proposal_returns_need_no_account_state() {
        assert_proposal_account_endpoints(true, false, false).await;
    }

    #[tokio::test]
    async fn accounts_named_only_by_resolved_proposals_remain_known() {
        assert_proposal_account_endpoints(true, true, false).await;
    }

    /// The final-row log still names the account once ESTART has moved the
    /// proposals out of state.
    #[tokio::test]
    async fn accounts_named_only_by_moved_proposals_remain_known() {
        assert_proposal_account_endpoints(true, true, true).await;
    }

    #[tokio::test]
    async fn accounts_named_only_by_invalid_proposals_remain_unknown() {
        assert_proposal_account_endpoints(false, false, false).await;
    }
}
