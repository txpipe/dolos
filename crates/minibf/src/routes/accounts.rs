use std::{collections::BTreeSet, ops::Deref};

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::{
    account_addresses_content_inner::AccountAddressesContentInner,
    account_content::AccountContent,
    account_delegation_content_inner::AccountDelegationContentInner,
    account_registration_content_inner::{AccountRegistrationContentInner, Action},
    account_reward_content_inner::AccountRewardContentInner,
    address_utxo_content_inner::AddressUtxoContentInner,
};

use dolos_cardano::{model::AccountState, pallas_extras, ChainSummary, RewardLog};
use dolos_core::{ArchiveStore, Domain, EntityKey, StateStore};
use pallas::{
    codec::minicbor,
    crypto::hash::Hash,
    ledger::{
        addresses::{Address, Network, StakeAddress, StakePayload},
        primitives::Epoch,
        traverse::{MultiEraBlock, MultiEraCert, MultiEraTx},
    },
};

use pallas::ledger::primitives::alonzo::Certificate as AlonzoCert;
use pallas::ledger::primitives::conway::Certificate as ConwayCert;

use crate::{
    error::Error,
    mapping::{self, bech32_drep, bech32_pool, IntoModel},
    pagination::{Pagination, PaginationParameters},
    Facade,
};

struct AccountKeyParam {
    address: StakeAddress,
    entity_key: Vec<u8>,
}

fn parse_account_key_param(address: &str) -> Result<AccountKeyParam, StatusCode> {
    let address = pallas::ledger::addresses::Address::from_bech32(address)
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let address = match address {
        Address::Shelley(x) => pallas_extras::shelley_address_to_stake_address(&x),
        Address::Stake(x) => Some(x),
        _ => None,
    };

    let address = address.ok_or(StatusCode::BAD_REQUEST)?;

    let stake_cred = dolos_cardano::pallas_extras::stake_address_to_cred(&address);

    let entity_key = minicbor::to_vec(&stake_cred).unwrap();

    Ok(AccountKeyParam {
        address,
        entity_key,
    })
}

struct AccountModelBuilder<'a> {
    account_state: dolos_cardano::model::AccountState,
    stake_address: Option<StakeAddress>,
    tip_slot: Option<u64>,
    chain: Option<&'a ChainSummary>,
}

impl<'a> IntoModel<AccountContent> for AccountModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<AccountContent, StatusCode> {
        let tip_slot = self.tip_slot.ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

        let chain = self.chain.ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

        let stake_address = self
            .stake_address
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?
            .to_bech32()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let (current_epoch, _) = chain.slot_epoch(tip_slot);

        let active_epoch = self
            .account_state
            .registered_at
            .or(self.account_state.deregistered_at)
            .map(|x| chain.slot_epoch(x))
            .map(|(x, _)| x);

        let pool_id = self
            .account_state
            .delegated_pool_at(current_epoch)
            .map(bech32_pool)
            .transpose()?;

        let drep_id = self
            .account_state
            .delegated_drep_at(current_epoch)
            .map(bech32_drep)
            .transpose()?;

        let active = active_epoch.map(|x| x < current_epoch).unwrap_or(false) && pool_id.is_some();

        let stake = self.account_state.stake.live().cloned().unwrap_or_default();

        let out = AccountContent {
            stake_address,
            active,
            active_epoch: active_epoch.map(|x| x as i32),
            controlled_amount: stake.total().to_string(),
            rewards_sum: stake.rewards_sum.to_string(),
            withdrawals_sum: stake.withdrawals_sum.to_string(),
            reserves_sum: "0".to_string(),
            treasury_sum: "0".to_string(),
            withdrawable_amount: stake.withdrawable().to_string(),
            pool_id,
            drep_id,
        };

        Ok(out)
    }
}

impl<'a> IntoModel<Vec<AccountAddressesContentInner>> for AccountModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<Vec<AccountAddressesContentInner>, StatusCode> {
        let out: Vec<_> = vec![]
            .into_iter()
            .map(|x| AccountAddressesContentInner { address: x })
            .collect();

        Ok(out)
    }
}

pub async fn by_stake<D: Domain>(
    Path(stake_address): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<AccountContent>, StatusCode>
where
    Option<AccountState>: From<D::Entity>,
{
    let account_key = parse_account_key_param(&stake_address)?;

    let state = domain
        .read_cardano_entity::<AccountState>(account_key.entity_key.as_slice())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let tip_slot = domain
        .archive()
        .get_tip()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map(|(slot, _)| slot)
        .unwrap_or_default();

    let chain = domain
        .get_chain_summary()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let model = AccountModelBuilder {
        account_state: state,
        stake_address: Some(account_key.address),
        tip_slot: Some(tip_slot),
        chain: Some(&chain),
    }
    .into_model()?;

    Ok(Json(model))
}

pub async fn by_stake_addresses<D: Domain>(
    Path(stake_address): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AccountAddressesContentInner>>, Error>
where
    Option<AccountState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;
    let account_key = parse_account_key_param(&stake_address)?;

    let mut blocks = domain
        .archive()
        .iter_blocks_with_stake(&account_key.address.to_vec())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut items = vec![];
    let mut skipped = 0;
    let mut seen = BTreeSet::new();

    while items.len() < pagination.count {
        let Some(block) = blocks.next() else {
            break;
        };
        let Ok((_, Some(block))) = block else {
            return Err(StatusCode::INTERNAL_SERVER_ERROR.into());
        };

        let block = MultiEraBlock::decode(&block).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        for (_, utxo) in block.txs().iter().flat_map(|tx| tx.produces()) {
            let address = utxo
                .address()
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            if match &address {
                Address::Shelley(shelley) => {
                    pallas_extras::shelley_address_to_stake_address(shelley)
                        .map(|x| x.to_vec() == account_key.address.to_vec())
                        .unwrap_or(false)
                }
                Address::Stake(stake) => stake.to_vec() == account_key.address.to_vec(),
                Address::Byron(_) => false,
            } && seen.insert(address.to_string())
            {
                if skipped < (pagination.page as usize - 1) * pagination.count {
                    skipped += 1;
                } else {
                    items.push(AccountAddressesContentInner {
                        address: address.to_string(),
                    });
                }
            }
        }
    }

    Ok(Json(items))
}

pub async fn by_stake_utxos<D: Domain>(
    Path(address): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AddressUtxoContentInner>>, Error> {
    let pagination = Pagination::try_from(params)?;

    let account_key = parse_account_key_param(&address)?;

    let payload = match account_key.address.payload() {
        StakePayload::Stake(payload) => payload.as_slice(),
        StakePayload::Script(payload) => payload.as_slice(),
    };

    let refs = domain
        .state()
        .get_utxo_by_stake(payload)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let utxos = super::utxos::load_utxo_models(&domain, refs, pagination)?;

    Ok(Json(utxos))
}

fn build_delegation(
    stake_address: &StakeAddress,
    tx: &MultiEraTx,
    cert: &MultiEraCert,
    epoch: Epoch,
    network: Network,
) -> Result<Option<AccountDelegationContentInner>, StatusCode> {
    let (cred, pool) = match cert {
        MultiEraCert::AlonzoCompatible(cert) => match cert.deref().deref() {
            AlonzoCert::StakeDelegation(cred, pool) => (cred, pool),
            _ => return Ok(None),
        },
        MultiEraCert::Conway(cert) => match cert.deref().deref() {
            ConwayCert::StakeDelegation(cred, pool) => (cred, pool),
            _ => return Ok(None),
        },
        _ => return Ok(None),
    };

    let address = mapping::stake_cred_to_address(cred, network);

    if address != *stake_address {
        return Ok(None);
    }

    let pool = mapping::bech32_pool(pool)?;

    Ok(Some(AccountDelegationContentInner {
        active_epoch: (epoch + 2) as i32,
        tx_hash: tx.hash().to_string(),
        amount: tx
            .outputs()
            .iter()
            .map(|x| x.value().coin())
            .sum::<u64>()
            .to_string(),
        pool_id: pool,
    }))
}

fn build_registration(
    stake_address: &StakeAddress,
    tx: &MultiEraTx,
    cert: &MultiEraCert,
    _epoch: Epoch,
    network: Network,
) -> Result<Option<AccountRegistrationContentInner>, StatusCode> {
    let (cred, is_registration) = match cert {
        MultiEraCert::AlonzoCompatible(cert) => match cert.deref().deref() {
            AlonzoCert::StakeRegistration(cred) => (cred, true),
            AlonzoCert::StakeDeregistration(cred) => (cred, false),
            _ => return Ok(None),
        },
        MultiEraCert::Conway(cert) => match cert.deref().deref() {
            ConwayCert::StakeRegistration(cred) => (cred, true),
            ConwayCert::StakeDeregistration(cred) => (cred, false),
            ConwayCert::Reg(cred, _) => (cred, true),
            ConwayCert::UnReg(cred, _) => (cred, false),
            ConwayCert::StakeRegDeleg(cred, _, _) => (cred, true),
            ConwayCert::StakeVoteRegDeleg(cred, _, _, _) => (cred, true),
            _ => return Ok(None),
        },
        _ => return Ok(None),
    };

    let address = mapping::stake_cred_to_address(cred, network);

    if address != *stake_address {
        return Ok(None);
    }

    Ok(Some(AccountRegistrationContentInner {
        tx_hash: tx.hash().to_string(),
        action: if is_registration {
            Action::Registered
        } else {
            Action::Deregistered
        },
    }))
}

struct AccountActivityModelBuilder<T> {
    stake_address: StakeAddress,
    network: Network,
    page_size: usize,
    page_number: usize,
    skipped: usize,
    items: Vec<T>,
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

    fn scan_block_certs<F>(
        &mut self,
        epoch: Epoch,
        block: &MultiEraBlock,
        mapper: F,
    ) -> Result<(), StatusCode>
    where
        F: Fn(
            &StakeAddress,
            &MultiEraTx,
            &MultiEraCert,
            Epoch,
            Network,
        ) -> Result<Option<T>, StatusCode>,
    {
        let txs = block.txs();

        for tx in txs {
            let certs = tx.certs();

            for cert in certs {
                let model = mapper(&self.stake_address, &tx, &cert, epoch, self.network)?;

                if let Some(model) = model {
                    self.add(model);
                }
            }
        }

        Ok(())
    }
}

impl IntoModel<Vec<AccountDelegationContentInner>>
    for AccountActivityModelBuilder<AccountDelegationContentInner>
{
    type SortKey = ();

    fn into_model(self) -> Result<Vec<AccountDelegationContentInner>, StatusCode> {
        Ok(self.items)
    }
}

impl IntoModel<Vec<AccountAddressesContentInner>>
    for AccountActivityModelBuilder<AccountAddressesContentInner>
{
    type SortKey = ();

    fn into_model(self) -> Result<Vec<AccountAddressesContentInner>, StatusCode> {
        Ok(self.items)
    }
}

pub async fn by_stake_actions<D: Domain, F, T>(
    stake_address: &str,
    pagination: Pagination,
    domain: Facade<D>,
    mapper: F,
) -> Result<Vec<T>, Error>
where
    Option<AccountState>: From<D::Entity>,
    F: Fn(
        &StakeAddress,
        &MultiEraTx,
        &MultiEraCert,
        Epoch,
        Network,
    ) -> Result<Option<T>, StatusCode>,
{
    let account_key = parse_account_key_param(stake_address)?;

    let chain = domain
        .get_chain_summary()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let network = domain
        .get_network_id()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut builder = AccountActivityModelBuilder::new(
        account_key.address,
        network,
        pagination.count,
        pagination.page as usize,
    );

    let mut blocks = domain
        .archive()
        .iter_blocks_with_account_certs(&account_key.entity_key)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    while builder.needs_more() {
        let Some(block) = blocks.next() else {
            break;
        };

        let Ok((slot, Some(block))) = block else {
            return Err(StatusCode::INTERNAL_SERVER_ERROR.into());
        };

        let (epoch, _) = chain.slot_epoch(slot);

        let block = MultiEraBlock::decode(&block).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        builder.scan_block_certs(epoch, &block, &mapper)?;
    }

    Ok(builder.items)
}

pub async fn by_stake_delegations<D: Domain>(
    Path(stake_address): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AccountDelegationContentInner>>, Error>
where
    Option<AccountState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;

    let items = by_stake_actions::<D, _, AccountDelegationContentInner>(
        &stake_address,
        pagination,
        domain,
        build_delegation,
    )
    .await?;

    Ok(Json(items))
}

pub async fn by_stake_registrations<D: Domain>(
    Path(stake_address): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AccountRegistrationContentInner>>, Error>
where
    Option<AccountState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;

    let items = by_stake_actions::<D, _, AccountRegistrationContentInner>(
        &stake_address,
        pagination,
        domain,
        build_registration,
    )
    .await?;

    Ok(Json(items))
}

pub async fn by_stake_rewards<D: Domain>(
    Path(stake_address): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AccountRewardContentInner>>, Error>
where
    Option<AccountState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;
    let account_key = parse_account_key_param(&stake_address)?;
    let tip = domain.get_tip_slot()?;
    let summary = domain.get_chain_summary()?;
    let (epoch, _) = summary.slot_epoch(tip);

    let rewards = domain
        .iter_cardano_logs_per_epoch::<RewardLog>(account_key.entity_key.into(), 0..epoch)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mapped: Vec<_> = rewards
        .into_iter()
        .filter(|(_, reward)| reward.amount > 0)
        .skip(pagination.skip())
        .take(pagination.count)
        .map(|(epoch, reward)| {
            let operator = Hash::<28>::from(EntityKey::from(reward.pool_id));
            let pool_id = mapping::bech32_pool(operator)?;

            let r#type = if reward.as_leader {
                blockfrost_openapi::models::account_reward_content_inner::Type::Leader
            } else {
                blockfrost_openapi::models::account_reward_content_inner::Type::Member
            };

            let out = AccountRewardContentInner {
                epoch: epoch as i32,
                amount: reward.amount.to_string(),
                pool_id,
                r#type,
            };

            Ok(out)
        })
        .collect::<Result<Vec<_>, StatusCode>>()?;

    Ok(Json(mapped))
}
