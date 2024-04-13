use pallas::applying::{
    utils::{AlonzoProtParams, BabbageProtParams, FeePolicy},
    MultiEraProtParams,
};

fn preprod_values_epoch_7() -> MultiEraProtParams {
    MultiEraProtParams::Alonzo(AlonzoProtParams {
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
    })
}

fn preprod_values_epoch_12() -> MultiEraProtParams {
    MultiEraProtParams::Babbage(BabbageProtParams {
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
        coins_per_utxo_word: 4310,
    })
}

fn preprod_values_epoch_28() -> MultiEraProtParams {
    MultiEraProtParams::Babbage(BabbageProtParams {
        fee_policy: FeePolicy {
            summand: 155381,
            multiplier: 44,
        },
        max_tx_size: 16384,
        max_block_ex_mem: 62000000,
        max_block_ex_steps: 40000000000,
        max_tx_ex_mem: 14000000,
        max_tx_ex_steps: 10000000000,
        max_val_size: 5000,
        collateral_percent: 150,
        max_collateral_inputs: 3,
        coins_per_utxo_word: 4310,
    })
}

fn preprod_values_epoch_51() -> MultiEraProtParams {
    MultiEraProtParams::Babbage(BabbageProtParams {
        fee_policy: FeePolicy {
            summand: 155381,
            multiplier: 44,
        },
        max_tx_size: 16384,
        max_block_ex_mem: 62000000,
        max_block_ex_steps: 20000000000,
        max_tx_ex_mem: 14000000,
        max_tx_ex_steps: 10000000000,
        max_val_size: 5000,
        collateral_percent: 150,
        max_collateral_inputs: 3,
        coins_per_utxo_word: 4310,
    })
}

fn preview_values_epoch_1() -> MultiEraProtParams {
    MultiEraProtParams::Alonzo(AlonzoProtParams {
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
    })
}

fn preview_values_epoch_3() -> MultiEraProtParams {
    MultiEraProtParams::Babbage(BabbageProtParams {
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
        coins_per_utxo_word: 4310,
    })
}

fn preview_values_epoch_9() -> MultiEraProtParams {
    MultiEraProtParams::Babbage(BabbageProtParams {
        fee_policy: FeePolicy {
            summand: 155381,
            multiplier: 44,
        },
        max_tx_size: 16384,
        max_block_ex_mem: 62000000,
        max_block_ex_steps: 40000000000,
        max_tx_ex_mem: 14000000,
        max_tx_ex_steps: 10000000000,
        max_val_size: 5000,
        collateral_percent: 150,
        max_collateral_inputs: 3,
        coins_per_utxo_word: 4310,
    })
}

fn preview_values_epoch_107() -> MultiEraProtParams {
    MultiEraProtParams::Babbage(BabbageProtParams {
        fee_policy: FeePolicy {
            summand: 155381,
            multiplier: 44,
        },
        max_tx_size: 16384,
        max_block_ex_mem: 62000000,
        max_block_ex_steps: 20000000000,
        max_tx_ex_mem: 14000000,
        max_tx_ex_steps: 10000000000,
        max_val_size: 5000,
        collateral_percent: 150,
        max_collateral_inputs: 3,
        coins_per_utxo_word: 4310,
    })
}

fn mainnet_values_epoch_290() -> MultiEraProtParams {
    MultiEraProtParams::Alonzo(AlonzoProtParams {
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
    })
}

fn mainnet_values_epoch_306() -> MultiEraProtParams {
    MultiEraProtParams::Alonzo(AlonzoProtParams {
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
    })
}

fn mainnet_values_epoch_319() -> MultiEraProtParams {
    MultiEraProtParams::Alonzo(AlonzoProtParams {
        fee_policy: FeePolicy {
            summand: 155381,
            multiplier: 44,
        },
        max_tx_size: 16384,
        max_block_ex_mem: 50000000,
        max_block_ex_steps: 40000000000,
        max_tx_ex_mem: 11250000,
        max_tx_ex_steps: 10000000000,
        max_val_size: 5000,
        collateral_percent: 150,
        max_collateral_inputs: 3,
        coins_per_utxo_word: 34482,
    })
}

fn mainnet_values_epoch_322() -> MultiEraProtParams {
    MultiEraProtParams::Alonzo(AlonzoProtParams {
        fee_policy: FeePolicy {
            summand: 155381,
            multiplier: 44,
        },
        max_tx_size: 16384,
        max_block_ex_mem: 56000000,
        max_block_ex_steps: 40000000000,
        max_tx_ex_mem: 14000000,
        max_tx_ex_steps: 10000000000,
        max_val_size: 5000,
        collateral_percent: 150,
        max_collateral_inputs: 3,
        coins_per_utxo_word: 34482,
    })
}

fn mainnet_values_epoch_328() -> MultiEraProtParams {
    MultiEraProtParams::Alonzo(AlonzoProtParams {
        fee_policy: FeePolicy {
            summand: 155381,
            multiplier: 44,
        },
        max_tx_size: 16384,
        max_block_ex_mem: 62000000,
        max_block_ex_steps: 40000000000,
        max_tx_ex_mem: 14000000,
        max_tx_ex_steps: 10000000000,
        max_val_size: 5000,
        collateral_percent: 150,
        max_collateral_inputs: 3,
        coins_per_utxo_word: 34482,
    })
}

fn mainnet_values_epoch_365() -> MultiEraProtParams {
    MultiEraProtParams::Babbage(BabbageProtParams {
        fee_policy: FeePolicy {
            summand: 155381,
            multiplier: 44,
        },
        max_tx_size: 16384,
        max_block_ex_mem: 62000000,
        max_block_ex_steps: 40000000000,
        max_tx_ex_mem: 14000000,
        max_tx_ex_steps: 10000000000,
        max_val_size: 5000,
        collateral_percent: 150,
        max_collateral_inputs: 3,
        coins_per_utxo_word: 4310,
    })
}

fn mainnet_values_epoch_394() -> MultiEraProtParams {
    MultiEraProtParams::Babbage(BabbageProtParams {
        fee_policy: FeePolicy {
            summand: 155381,
            multiplier: 44,
        },
        max_tx_size: 16384,
        max_block_ex_mem: 62000000,
        max_block_ex_steps: 20000000000,
        max_tx_ex_mem: 14000000,
        max_tx_ex_steps: 10000000000,
        max_val_size: 5000,
        collateral_percent: 150,
        max_collateral_inputs: 3,
        coins_per_utxo_word: 4310,
    })
}
