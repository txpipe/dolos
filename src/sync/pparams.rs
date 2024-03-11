use gasket::framework::{AsWorkError, WorkerError};
use pallas::{
    applying::{
        utils::{
            AlonzoProtParams, BabbageProtParams, ByronProtParams, FeePolicy, ShelleyProtParams,
        },
        Environment, MultiEraProtParams,
    },
    ledger::{
        configs::{byron, shelley},
        traverse::{Era, MultiEraUpdate},
    },
};
use tracing::{info, warn};

use crate::storage::applydb::ApplyDB;

pub struct Genesis<'a> {
    pub byron: &'a byron::GenesisFile,
    pub shelley: &'a shelley::GenesisFile,
}

fn pparams_from_byron_genesis(
    byron: &byron::GenesisFile,
) -> Result<MultiEraProtParams, WorkerError> {
    let out = pallas::applying::MultiEraProtParams::Byron(ByronProtParams {
        fee_policy: FeePolicy {
            summand: byron
                .block_version_data
                .tx_fee_policy
                .summand
                .parse()
                .or_panic()?,
            multiplier: byron
                .block_version_data
                .tx_fee_policy
                .multiplier
                .parse()
                .or_panic()?,
        },
        max_tx_size: byron.block_version_data.max_tx_size.parse().or_panic()?,
    });

    Ok(out)
}

fn pparams_from_shelley_genesis(
    shelley: &shelley::GenesisFile,
) -> Result<MultiEraProtParams, WorkerError> {
    let out = pallas::applying::MultiEraProtParams::Shelley(ShelleyProtParams {
        fee_policy: FeePolicy {
            summand: shelley.protocol_params.min_fee_a,
            multiplier: shelley.protocol_params.min_fee_b,
        },
        max_tx_size: shelley.protocol_params.max_tx_size,
        min_lovelace: shelley.protocol_params.min_u_tx_o_value,
    });

    Ok(out)
}

fn apply_era_hardfork(
    genesis: &Genesis,
    new_protocol: u64,
) -> Result<MultiEraProtParams, WorkerError> {
    match new_protocol {
        1 => pparams_from_byron_genesis(genesis.byron),
        2..=4 => pparams_from_shelley_genesis(genesis.shelley),
        x => {
            unimplemented!("don't know how to handle hardfork for protocol {x}");
        }
    }
}

fn apply_param_update(
    genesis: &Genesis,
    era: Era,
    current: MultiEraProtParams,
    update: MultiEraUpdate,
) -> Result<MultiEraProtParams, WorkerError> {
    match current {
        MultiEraProtParams::Byron(mut pparams) => {
            assert_eq!(u16::from(era), 1, "pparam update doesn't match era");

            if let Some((major, _, _)) = update.byron_proposed_block_version() {
                warn!(major, "found new byron protocol update proposal");
                return apply_era_hardfork(genesis, major as u64);
            }

            if let Some(pallas::ledger::primitives::byron::TxFeePol::Variant0(new)) =
                update.byron_proposed_fee_policy()
            {
                warn!("found new byron fee policy update proposal");

                let new = new.unwrap();
                pparams.fee_policy = FeePolicy {
                    summand: new.0 as u64,
                    multiplier: new.1 as u64,
                };
            }

            if let Some(new) = update.byron_proposed_max_tx_size() {
                warn!("found new byron max tx size update proposal");
                pparams.max_tx_size = new;
            }

            Ok(MultiEraProtParams::Byron(pparams))
        }
        MultiEraProtParams::Shelley(mut pparams) => {
            assert_eq!(u16::from(era), 2, "pparam update doesn't match era");

            if let Some((major, _)) = update.first_proposed_protocol_version() {
                warn!(major, "found new shelley protocol update proposal");
                return apply_era_hardfork(genesis, major);
            }

            if let Some(x) = update.first_proposed_minfee_a() {
                warn!(x, "found new minfee a update proposal");
                pparams.fee_policy.summand = x as u64;
            }

            if let Some(x) = update.first_proposed_minfee_b() {
                warn!(x, "found new minfee b update proposal");
                pparams.fee_policy.multiplier = x as u64;
            }

            if let Some(x) = update.first_proposed_max_transaction_size() {
                warn!(x, "found new max tx size update proposal");
                pparams.max_tx_size = x as u64;
            }

            // TODO: where's the min utxo value in the network primitives for shelley? do we
            // have them wrong in Pallas?

            Ok(MultiEraProtParams::Shelley(pparams))
        }
        _ => unimplemented!(),
    }
}

// TODO: perform proper protocol parameters update for the Alonzo era.
pub fn compute_pparams(
    genesis: Genesis,
    ledger: &ApplyDB,
    epoch: u64,
    block_slot: u64,
    prot_magic: u32,
    network_id: u8,
) -> Result<Environment, WorkerError> {
    if prot_magic == 1 && network_id == 0 {
        // Preprod
        if (7..=11).contains(&epoch) {
            // Alonzo era
            Ok(Environment {
                block_slot,
                prot_magic,
                network_id,
                prot_params: MultiEraProtParams::Alonzo(AlonzoProtParams {
                    fee_policy: FeePolicy {
                        summand: 155381,
                        multiplier: 44,
                    },
                    max_tx_size: 16384,
                    max_block_ex_mem: 50000000,
                    max_block_ex_steps: 40000000000,
                    max_tx_ex_mem: 10000000,
                    max_tx_ex_steps: 10000000000,
                    max_val_size: 5000,
                    collateral_percent: 150,
                    max_collateral_inputs: 3,
                    coins_per_utxo_word: 34482,
                }),
            })
        } else if 12 >= epoch {
            // Babbage era
            Ok(Environment {
                block_slot,
                prot_magic,
                network_id,
                prot_params: MultiEraProtParams::Babbage(BabbageProtParams {
                    fee_policy: FeePolicy {
                        summand: 155381,
                        multiplier: 44,
                    },
                    max_tx_size: 16384,
                    max_block_ex_mem: if (12..=27).contains(&epoch) {
                        50000000
                    } else {
                        62000000
                    },
                    max_block_ex_steps: if (12..=50).contains(&epoch) {
                        40000000000
                    } else {
                        20000000000
                    },
                    max_tx_ex_mem: if (12..=27).contains(&epoch) {
                        10000000
                    } else {
                        14000000
                    },
                    max_tx_ex_steps: 10000000000,
                    max_val_size: 5000,
                    collateral_percent: 150,
                    max_collateral_inputs: 3,
                    coins_per_utxo_word: 4310,
                }),
            })
        } else {
            // Eras prior to Alonzo and Babbage (Byron and ShelleyMA)
            let mut out = Environment {
                block_slot,
                prot_magic,
                network_id,
                prot_params: apply_era_hardfork(&genesis, 1)?,
            };

            let updates = ledger.get_pparams_updates(epoch).or_panic()?;

            info!(epoch, updates = updates.len(), "computing pparams");

            for (era, _, cbor) in updates {
                let era = Era::try_from(era).or_panic()?;
                let update = MultiEraUpdate::decode_for_era(era, &cbor).or_panic()?;
                out.prot_params = apply_param_update(&genesis, era, out.prot_params, update)?;
            }

            Ok(out)
        }
    } else if prot_magic == 2 && network_id == 0 {
        // Preview
        if (1..=2).contains(&epoch) {
            // Alonzo era
            Ok(Environment {
                block_slot,
                prot_magic,
                network_id,
                prot_params: MultiEraProtParams::Alonzo(AlonzoProtParams {
                    fee_policy: FeePolicy {
                        summand: 155381,
                        multiplier: 44,
                    },
                    max_tx_size: 16384,
                    max_block_ex_mem: 50000000,
                    max_block_ex_steps: 40000000000,
                    max_tx_ex_mem: 10000000,
                    max_tx_ex_steps: 10000000000,
                    max_val_size: 5000,
                    collateral_percent: 150,
                    max_collateral_inputs: 3,
                    coins_per_utxo_word: 34482,
                }),
            })
        } else if 3 >= epoch {
            // Babbage era
            Ok(Environment {
                block_slot,
                prot_magic,
                network_id,
                prot_params: MultiEraProtParams::Babbage(BabbageProtParams {
                    fee_policy: FeePolicy {
                        summand: 155381,
                        multiplier: 44,
                    },
                    max_tx_size: 16384,
                    max_block_ex_mem: if (3..=8).contains(&epoch) {
                        50000000
                    } else {
                        62000000
                    },
                    max_block_ex_steps: if (3..=106).contains(&epoch) {
                        40000000000
                    } else {
                        20000000000
                    },
                    max_tx_ex_mem: if (3..=8).contains(&epoch) {
                        10000000
                    } else {
                        14000000
                    },
                    max_tx_ex_steps: 10000000000,
                    max_val_size: 5000,
                    collateral_percent: 150,
                    max_collateral_inputs: 3,
                    coins_per_utxo_word: 4310,
                }),
            })
        } else {
            // Preview networks do not have Byron nor ShelleyMA
            Err(WorkerError::Panic)
        }
    } else {
        // All other cases are assumed to require a mainnet environment
        if (290..=364).contains(&epoch) {
            // Alonzo era
            Ok(Environment {
                block_slot,
                prot_magic,
                network_id,
                prot_params: MultiEraProtParams::Alonzo(AlonzoProtParams {
                    fee_policy: FeePolicy {
                        summand: 155381,
                        multiplier: 44,
                    },
                    max_tx_size: 16384,
                    max_block_ex_mem: if (290..=321).contains(&epoch) {
                        50000000
                    } else if (321..328).contains(&epoch) {
                        56000000
                    } else {
                        62000000
                    },
                    max_block_ex_steps: 40000000000,
                    max_tx_ex_mem: if (..=306).contains(&epoch) {
                        10000000
                    } else if (306..319).contains(&epoch) {
                        11250000
                    } else {
                        14000000
                    },
                    max_tx_ex_steps: 10000000000,
                    max_val_size: 5000,
                    collateral_percent: 150,
                    max_collateral_inputs: 3,
                    coins_per_utxo_word: 34482,
                }),
            })
        } else if epoch >= 365 {
            // Babbage era
            Ok(Environment {
                block_slot,
                prot_magic,
                network_id,
                prot_params: MultiEraProtParams::Babbage(BabbageProtParams {
                    fee_policy: FeePolicy {
                        summand: 155381,
                        multiplier: 44,
                    },
                    max_tx_size: 16384,
                    max_block_ex_mem: 62000000,
                    max_block_ex_steps: if (365..=393).contains(&epoch) {
                        40000000000
                    } else {
                        20000000000
                    },
                    max_tx_ex_mem: 14000000,
                    max_tx_ex_steps: 10000000000,
                    max_val_size: 5000,
                    collateral_percent: 150,
                    max_collateral_inputs: 3,
                    coins_per_utxo_word: 4310,
                }),
            })
        } else {
            // Eras prior to Alonzo and Babbage (Byron and ShelleyMA)
            let mut out = Environment {
                block_slot,
                prot_magic,
                network_id,
                prot_params: apply_era_hardfork(&genesis, 1)?,
            };

            let updates = ledger.get_pparams_updates(epoch).or_panic()?;

            info!(epoch, updates = updates.len(), "computing pparams");

            for (era, _, cbor) in updates {
                let era = Era::try_from(era).or_panic()?;
                let update = MultiEraUpdate::decode_for_era(era, &cbor).or_panic()?;
                out.prot_params = apply_param_update(&genesis, era, out.prot_params, update)?;
            }

            Ok(out)
        }
    }
}
