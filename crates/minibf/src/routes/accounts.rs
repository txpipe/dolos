use std::ops::Deref;

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

use dolos_cardano::{
    model::{AccountActivity, RewardLog},
    pparams::ChainSummary,
};
use dolos_core::{ArchiveStore, Domain, State3Store as _, StateStore};
use pallas::ledger::{
    addresses::{Network, StakeAddress, StakePayload},
    traverse::{MultiEraBlock, MultiEraCert, MultiEraTx},
};

use pallas::ledger::primitives::alonzo::Certificate as AlonzoCert;
use pallas::ledger::primitives::conway::Certificate as ConwayCert;

use crate::{
    error::Error,
    mapping::{self, bech32_drep, bech32_pool, bytes_to_address_bech32, IntoModel},
    pagination::{Pagination, PaginationParameters},
    Facade,
};

fn ensure_stake_address(address: &str) -> Result<StakeAddress, StatusCode> {
    let address = pallas::ledger::addresses::Address::from_bech32(address)
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let address = match address {
        pallas::ledger::addresses::Address::Shelley(addr) => {
            StakeAddress::try_from(addr).map_err(|_| StatusCode::BAD_REQUEST)?
        }
        pallas::ledger::addresses::Address::Stake(addr) => addr,
        _ => return Err(StatusCode::BAD_REQUEST),
    };

    Ok(address)
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

        let (current_epoch, _) = dolos_cardano::slot_epoch(tip_slot, chain);

        let active = self
            .account_state
            .active_epoch
            .map(|x| x <= current_epoch)
            .unwrap_or_default();

        let pool_id = self
            .account_state
            .pool_id
            .as_ref()
            .map(bech32_pool)
            .transpose()?;

        let drep_id = self
            .account_state
            .drep_id
            .as_ref()
            .map(bech32_drep)
            .transpose()?;

        let out = AccountContent {
            stake_address,
            active,
            active_epoch: self.account_state.active_epoch.map(|x| x as i32),
            controlled_amount: self.account_state.controlled_amount.to_string(),
            rewards_sum: self.account_state.rewards_sum.to_string(),
            withdrawals_sum: self.account_state.withdrawals_sum.to_string(),
            reserves_sum: self.account_state.reserves_sum.to_string(),
            treasury_sum: self.account_state.treasury_sum.to_string(),
            withdrawable_amount: self.account_state.withdrawable_amount.to_string(),
            pool_id,
            drep_id,
        };

        Ok(out)
    }
}

impl<'a> IntoModel<Vec<AccountAddressesContentInner>> for AccountModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<Vec<AccountAddressesContentInner>, StatusCode> {
        let addresses: Vec<_> = self
            .account_state
            .seen_addresses
            .iter()
            .map(|x| bytes_to_address_bech32(x.as_slice()))
            .collect::<Result<_, _>>()?;

        let out: Vec<_> = addresses
            .into_iter()
            .map(|x| AccountAddressesContentInner { address: x })
            .collect();

        Ok(out)
    }
}

pub async fn by_stake<D: Domain>(
    Path(stake_address): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<AccountContent>, StatusCode> {
    let stake_address = ensure_stake_address(&stake_address)?;

    let state = domain
        .state3()
        .read_entity_typed::<dolos_cardano::model::AccountState>(stake_address.to_vec())
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
        stake_address: Some(stake_address),
        tip_slot: Some(tip_slot),
        chain: Some(&chain),
    }
    .into_model()?;

    Ok(Json(model))
}

pub async fn by_stake_addresses<D: Domain>(
    Path(stake_address): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AccountAddressesContentInner>>, StatusCode> {
    let stake_address = ensure_stake_address(&stake_address)?;

    let Some(state) = domain
        .state3()
        .read_entity_typed::<dolos_cardano::model::AccountState>(stake_address.to_vec())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Ok(Json(vec![]));
    };

    let model = AccountModelBuilder {
        account_state: state,
        stake_address: Some(stake_address),
        tip_slot: None,
        chain: None,
    }
    .into_model()?;

    Ok(Json(model))
}

pub async fn by_stake_utxos<D: Domain>(
    Path(address): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AddressUtxoContentInner>>, Error> {
    let pagination = Pagination::try_from(params)?;

    let address = ensure_stake_address(&address)?;
    let payload = match address.payload() {
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

const MAX_SCAN_DEPTH: usize = 5000;

fn build_delegation(
    stake_address: &StakeAddress,
    tx: &MultiEraTx,
    cert: &MultiEraCert,
    epoch: u32,
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
    _epoch: u32,
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

    fn scan_block<F>(
        &mut self,
        epoch: u32,
        block: &MultiEraBlock,
        mapper: F,
    ) -> Result<(), StatusCode>
    where
        F: Fn(
            &StakeAddress,
            &MultiEraTx,
            &MultiEraCert,
            u32,
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

pub async fn by_stake_actions<D: Domain, F, T>(
    stake_address: &str,
    pagination: Pagination,
    domain: Facade<D>,
    mapper: F,
) -> Result<Vec<T>, Error>
where
    F: Fn(&StakeAddress, &MultiEraTx, &MultiEraCert, u32, Network) -> Result<Option<T>, StatusCode>,
{
    let stake_address = ensure_stake_address(stake_address)?;

    let stake_hash = match stake_address.payload() {
        StakePayload::Stake(x) => x.to_vec(),
        StakePayload::Script(x) => x.to_vec(),
    };

    let chain = domain
        .get_chain_summary()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let network = domain
        .get_network_id()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut slot_iter = domain
        .state3()
        .iter_entity_values_typed::<AccountActivity>(stake_hash)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .take(MAX_SCAN_DEPTH);

    let mut builder = AccountActivityModelBuilder::new(
        stake_address,
        network,
        pagination.count,
        pagination.page as usize,
    );

    while builder.needs_more() {
        let Some(slot) = slot_iter.next() else {
            break;
        };

        let AccountActivity(slot) = slot.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let (epoch, _) = dolos_cardano::slot_epoch(slot, &chain);

        let block = domain
            .archive()
            .get_block_by_slot(&slot)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .ok_or(StatusCode::NOT_FOUND)?;

        let block = MultiEraBlock::decode(&block).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        builder.scan_block(epoch, &block, &mapper)?;
    }

    Ok(builder.items)
}

pub async fn by_stake_delegations<D: Domain>(
    Path(stake_address): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AccountDelegationContentInner>>, Error> {
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
) -> Result<Json<Vec<AccountRegistrationContentInner>>, Error> {
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

impl IntoModel<AccountRewardContentInner> for RewardLog {
    type SortKey = ();

    fn into_model(self) -> Result<AccountRewardContentInner, StatusCode> {
        let pool_id = mapping::bech32_pool(self.pool_id)?;

        let r#type = if self.as_leader {
            blockfrost_openapi::models::account_reward_content_inner::Type::Leader
        } else {
            blockfrost_openapi::models::account_reward_content_inner::Type::Member
        };

        let out = AccountRewardContentInner {
            epoch: self.epoch as i32,
            amount: self.amount.to_string(),
            pool_id,
            r#type,
        };

        Ok(out)
    }
}

pub async fn by_stake_rewards<D: Domain>(
    Path(stake_address): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AccountRewardContentInner>>, Error> {
    let pagination = Pagination::try_from(params)?;

    let stake_address = ensure_stake_address(&stake_address)?;

    let stake_hash = match stake_address.payload() {
        StakePayload::Stake(x) => x.to_vec(),
        StakePayload::Script(x) => x.to_vec(),
    };

    let items: Vec<_> = domain
        .state3()
        .iter_entity_values_typed::<RewardLog>(stake_hash)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .skip(pagination.skip())
        .take(pagination.count)
        .collect::<Result<_, _>>()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mapped = items
        .into_iter()
        .map(|x| x.into_model())
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Json(mapped))
}
