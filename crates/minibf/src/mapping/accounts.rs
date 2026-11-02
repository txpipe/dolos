use axum::http::StatusCode;
use blockfrost_openapi::models::account_content::AccountContent;
use dolos_cardano::{model::AccountState, ChainSummary};
use pallas::ledger::addresses::StakeAddress;

use crate::mapping::{bech32_drep, bech32_pool, IntoModel};

pub struct AccountModelBuilder<'a> {
    pub account_state: AccountState,
    pub stake_address: Option<StakeAddress>,
    pub tip_slot: Option<u64>,
    pub chain: Option<&'a ChainSummary>,
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
            .or(self.account_state.retired_pool.as_ref())
            .map(bech32_pool)
            .transpose()?;

        let drep_id = self
            .account_state
            .delegated_drep_at(current_epoch)
            .map(bech32_drep)
            .transpose()?;

        let active = pool_id.is_some();

        let stake = self.account_state.stake.live().cloned().unwrap_or_default();

        let out = AccountContent {
            stake_address,
            active,
            registered: self.account_state.is_registered(),
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
