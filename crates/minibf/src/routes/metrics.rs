use axum::{extract::State, http::StatusCode, Json};
use crate::{Config, Facade};
use dolos_core::{ArchiveStore as _, BlockBody, Domain};
use pallas::{
    codec::{minicbor, utils::Bytes},
    crypto::hash::{Hash, Hasher},
    ledger::{
        addresses::{Address, Network, ShelleyPaymentPart, StakeAddress, StakePayload},
        primitives::{
            alonzo::{self, Certificate as AlonzoCert},
            conway::{Certificate as ConwayCert, DRep, DatumOption, RedeemerTag, ScriptRef},
            ExUnitPrices, StakeCredential,
        },
        traverse::{
            ComputeHash, MultiEraBlock, MultiEraCert, MultiEraHeader, MultiEraInput,
            MultiEraOutput, MultiEraRedeemer, MultiEraTx, MultiEraValue, OriginalHash,
        },
    },
};

pub async fn metrics<D: Domain> (
    State(domain): State<Facade<D>>,
) -> Result<String, StatusCode> {
    let (_, tip) = domain
        .archive()
        .get_tip()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let block = MultiEraBlock::decode(&tip).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let slot = block.slot();
    let number = block.number();
    Ok(format!("dolos_slot {}\ndolos_block_number {}", slot, number))
}
