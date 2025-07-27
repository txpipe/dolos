use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::{
    account_content::AccountContent, address_utxo_content_inner::AddressUtxoContentInner,
};

use dolos_cardano::pparams::ChainSummary;
use dolos_core::{ArchiveStore, Domain, State3Store as _, StateStore};
use pallas::ledger::addresses::StakeAddress;

use crate::{
    mapping::{bech32_drep, bech32_pool, IntoModel},
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
    stake_address: StakeAddress,
    account_state: dolos_cardano::model::AccountState,
    tip_slot: u64,
    chain: &'a ChainSummary,
}

impl<'a> IntoModel<AccountContent> for AccountModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<AccountContent, StatusCode> {
        let (current_epoch, _) = dolos_cardano::slot_epoch(self.tip_slot, self.chain);

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
            stake_address: self.stake_address.to_bech32().unwrap(),
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
        stake_address,
        account_state: state,
        tip_slot,
        chain: &chain,
    }
    .into_model()?;

    Ok(Json(model))
}

pub async fn by_stake_utxos<D: Domain>(
    Path(address): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AddressUtxoContentInner>>, StatusCode> {
    let pagination = Pagination::try_from(params)?;

    let address = ensure_stake_address(&address)?;

    let refs = domain
        .state()
        .get_utxo_by_address(&address.to_vec())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let utxos = super::utxos::load_utxo_models(&domain, refs, pagination)?;

    Ok(Json(utxos))
}
