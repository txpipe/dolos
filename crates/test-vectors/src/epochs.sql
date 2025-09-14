WITH

pparams as (
    SELECT epoch_no AS "epoch",
    min_fee_a AS "min_fee_a",
    min_fee_b AS "min_fee_b",
    max_block_size AS "max_block_size",
    max_tx_size AS "max_tx_size",
    max_bh_size AS "max_block_header_size",
    key_deposit::TEXT AS "key_deposit",
    -- cast to TEXT to avoid number overflow
    pool_deposit::TEXT AS "pool_deposit",
    -- cast to TEXT to avoid number overflow
    max_epoch AS "e_max",
    optimal_pool_count AS "n_opt",
    influence AS "a0",
    monetary_expand_rate AS "rho",
    treasury_growth_rate AS "tau",
    decentralisation AS "decentralisation_param",
    extra_entropy AS "extra_entropy",
    protocol_major AS "protocol_major_ver",
    protocol_minor AS "protocol_minor_ver",
    COALESCE(coins_per_utxo_size, min_utxo_value)::TEXT AS "min_utxo",
    -- cast to TEXT to avoid number overflow; DEPRECATED since Alonzo, uses coins_per_utxo_size for backwards compatibility despite dbsync 13.2+ setting this to 0
    min_pool_cost::TEXT AS "min_pool_cost",
    -- cast to TEXT to avoid number overflow
    encode(nonce, 'hex') AS "nonce",
    cm.costs AS "cost_models",
    price_mem AS "price_mem",
    price_step AS "price_step",
    max_tx_ex_mem::TEXT AS "max_tx_ex_mem",
    -- cast to TEXT to avoid number overflow
    max_tx_ex_steps::TEXT AS "max_tx_ex_steps",
    -- cast to TEXT to avoid number overflow
    max_block_ex_mem::TEXT AS "max_block_ex_mem",
    -- cast to TEXT to avoid number overflow
    max_block_ex_steps::TEXT AS "max_block_ex_steps",
    -- cast to TEXT to avoid number overflow
    max_val_size::TEXT AS "max_val_size",
    -- cast to TEXT to avoid number overflow
    collateral_percent AS "collateral_percent",
    max_collateral_inputs AS "max_collateral_inputs",
    coins_per_utxo_size::TEXT AS "coins_per_utxo_size",
    -- cast to TEXT to avoid number overflow
    coins_per_utxo_size::TEXT AS "coins_per_utxo_word",
    -- deprecated
    pvt_motion_no_confidence AS "pvt_motion_no_confidence",
    pvt_committee_normal AS "pvt_committee_normal",
    pvt_committee_no_confidence AS "pvt_committee_no_confidence",
    pvt_hard_fork_initiation AS "pvt_hard_fork_initiation",
    dvt_motion_no_confidence AS "dvt_motion_no_confidence",
    dvt_committee_normal AS "dvt_committee_normal",
    dvt_committee_no_confidence AS "dvt_committee_no_confidence",
    dvt_update_to_constitution AS "dvt_update_to_constitution",
    dvt_hard_fork_initiation AS "dvt_hard_fork_initiation",
    dvt_p_p_network_group AS "dvt_p_p_network_group",
    dvt_p_p_economic_group AS "dvt_p_p_economic_group",
    dvt_p_p_technical_group AS "dvt_p_p_technical_group",
    dvt_p_p_gov_group AS "dvt_p_p_gov_group",
    dvt_treasury_withdrawal AS "dvt_treasury_withdrawal",
    committee_min_size AS "committee_min_size",
    committee_max_term_length AS "committee_max_term_length",
    gov_action_lifetime AS "gov_action_lifetime",
    gov_action_deposit AS "gov_action_deposit",
    drep_deposit::INTEGER AS "drep_deposit",
    drep_activity AS "drep_activity",
    pvtpp_security_group AS "pvtpp_security_group",
    pvtpp_security_group AS "pvt_p_p_security_group",
    min_fee_ref_script_cost_per_byte AS "min_fee_ref_script_cost_per_byte"
    FROM epoch_param ep
    LEFT JOIN cost_model cm ON (ep.cost_model_id = cm.id)
)


SELECT 
epoch_no,
treasury::TEXT,
reserves::TEXT,
rewards::TEXT,
utxo::TEXT,
deposits_stake::TEXT,
fees::TEXT,
deposits_drep::TEXT,
deposits_proposal::TEXT,
pp.*
FROM ada_pots as ap
LEFT JOIN pparams as pp ON pp.epoch = ap.epoch_no
WHERE ap.epoch_no > {{ epoch }} and ap.epoch_no <= {{ epoch }} + 2

{{ limit }}