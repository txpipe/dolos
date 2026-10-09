/// A protocol parameter model built off a
/// [`PParamsSet`](dolos_cardano::PParamsSet), with the fields every
/// Blockfrost parameter model renders the same way filled in.
///
/// `/epochs/{n}/parameters` and `/governance/proposals/…/parameters` share
/// thirty-odd nullable fields that read straight off the set and differ in
/// the rest: the epoch model reports the values in force, so it types most
/// of its fields as plain values and falls back to genesis, while the
/// proposal model reports a change, so every field of it is nullable. The
/// caller passes the set, the [`RatioFormat`](crate::mapping::RatioFormat)
/// to write ratios with and the model literal holding the fields the model
/// owns, and the shared ones are added to it. The literal stays exhaustive,
/// so a field the model grows, or one named on both sides, is a compile
/// error rather than a silent default.
///
/// The expansion uses `?`, so it belongs in a function returning
/// `Result<_, StatusCode>`: the models type counts and sizes as `i32` while a
/// proposal can set them anywhere in the chain's range, and a value past
/// `i32::MAX` is a 500 rather than a parameter wrapped negative.
macro_rules! protocol_params_model {
    ($params:ident, $ratio:ident, $model:ident { $($field:ident: $value:expr),* $(,)? }) => {{
        use $crate::mapping::RatioFormat as _;

        $model {
            $($field: $value,)*
            max_val_size: $params.max_value_size().map(|x| x.to_string()),
            collateral_percent: $params
                .collateral_percentage()
                .map($crate::mapping::i32_or_500)
                .transpose()?,
            max_collateral_inputs: $params
                .max_collateral_inputs()
                .map($crate::mapping::i32_or_500)
                .transpose()?,
            // One db-sync column, under both of the names Blockfrost gives it.
            coins_per_utxo_size: $params.ada_per_utxo_byte().map(|x| x.to_string()),
            coins_per_utxo_word: $params.ada_per_utxo_byte().map(|x| x.to_string()),
            price_mem: $params
                .execution_costs()
                .map(|x| $ratio::to_f64::<4>(&x.mem_price)),
            price_step: $params
                .execution_costs()
                .map(|x| $ratio::to_f64::<9>(&x.step_price)),
            max_tx_ex_mem: $params.max_tx_ex_units().map(|x| x.mem.to_string()),
            max_tx_ex_steps: $params.max_tx_ex_units().map(|x| x.steps.to_string()),
            max_block_ex_mem: $params.max_block_ex_units().map(|x| x.mem.to_string()),
            max_block_ex_steps: $params.max_block_ex_units().map(|x| x.steps.to_string()),
            min_fee_ref_script_cost_per_byte: $params
                .min_fee_ref_script_cost_per_byte()
                .map(|x| $ratio::to_f64::<3>(&x)),
            drep_deposit: $params.drep_deposit().map(|x| x.to_string()),
            drep_activity: $params.drep_inactivity_period().map(|x| x.to_string()),
            pvt_motion_no_confidence: $params
                .pool_voting_thresholds()
                .map(|x| $ratio::to_f64::<3>(&x.motion_no_confidence)),
            pvt_committee_normal: $params
                .pool_voting_thresholds()
                .map(|x| $ratio::to_f64::<3>(&x.committee_normal)),
            pvt_committee_no_confidence: $params
                .pool_voting_thresholds()
                .map(|x| $ratio::to_f64::<3>(&x.committee_no_confidence)),
            pvt_hard_fork_initiation: $params
                .pool_voting_thresholds()
                .map(|x| $ratio::to_f64::<3>(&x.hard_fork_initiation)),
            // The other column Blockfrost renders under two names.
            pvtpp_security_group: $params
                .pool_voting_thresholds()
                .map(|x| $ratio::to_f64::<3>(&x.security_voting_threshold)),
            pvt_p_p_security_group: $params
                .pool_voting_thresholds()
                .map(|x| $ratio::to_f64::<3>(&x.security_voting_threshold)),
            dvt_motion_no_confidence: $params
                .drep_voting_thresholds()
                .map(|x| $ratio::to_f64::<3>(&x.motion_no_confidence)),
            dvt_committee_normal: $params
                .drep_voting_thresholds()
                .map(|x| $ratio::to_f64::<3>(&x.committee_normal)),
            dvt_committee_no_confidence: $params
                .drep_voting_thresholds()
                .map(|x| $ratio::to_f64::<3>(&x.committee_no_confidence)),
            dvt_update_to_constitution: $params
                .drep_voting_thresholds()
                .map(|x| $ratio::to_f64::<3>(&x.update_constitution)),
            dvt_hard_fork_initiation: $params
                .drep_voting_thresholds()
                .map(|x| $ratio::to_f64::<3>(&x.hard_fork_initiation)),
            dvt_p_p_network_group: $params
                .drep_voting_thresholds()
                .map(|x| $ratio::to_f64::<3>(&x.pp_network_group)),
            dvt_p_p_economic_group: $params
                .drep_voting_thresholds()
                .map(|x| $ratio::to_f64::<3>(&x.pp_economic_group)),
            dvt_p_p_technical_group: $params
                .drep_voting_thresholds()
                .map(|x| $ratio::to_f64::<3>(&x.pp_technical_group)),
            dvt_p_p_gov_group: $params
                .drep_voting_thresholds()
                .map(|x| $ratio::to_f64::<3>(&x.pp_governance_group)),
            dvt_treasury_withdrawal: $params
                .drep_voting_thresholds()
                .map(|x| $ratio::to_f64::<3>(&x.treasury_withdrawal)),
            committee_min_size: $params.min_committee_size().map(|x| x.to_string()),
            committee_max_term_length: $params.committee_term_limit().map(|x| x.to_string()),
            gov_action_lifetime: $params
                .governance_action_validity_period()
                .map(|x| x.to_string()),
            gov_action_deposit: $params.governance_action_deposit().map(|x| x.to_string()),
            // Babbage retired it, so neither model has a value to show.
            extra_entropy: None,
        }
    }};
}

pub(crate) use protocol_params_model;
