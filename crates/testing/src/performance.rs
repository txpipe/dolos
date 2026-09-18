use std::sync::Arc;

use dolos_cardano::{model::AccountEpochLog, rupd::credential_to_key};
use dolos_core::{
    import::ImportExt, ArchiveStore, ArchiveWriter, ChainError, Domain, LogKey, RawBlock,
    TemporalKey,
};
use pallas::{
    crypto::hash::Hash,
    ledger::{
        addresses::{Network, StakeAddress, StakePayload},
        primitives::StakeCredential,
    },
};
use serde::{Deserialize, Serialize};

use crate::{
    measured::MeasuredStores,
    synthetic::{build_synthetic_blocks, SyntheticBlockConfig, SyntheticVectors},
    toy_domain::{ToyDomain, ToyStores},
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FixtureShape {
    pub blocks: usize,
    pub transactions_per_block: usize,
    pub log_rows: usize,
    pub pool_stride: usize,
    pub page: usize,
    pub page_size: usize,
    pub seed: u64,
}

impl Default for FixtureShape {
    fn default() -> Self {
        Self {
            blocks: 16,
            transactions_per_block: 3,
            log_rows: 256,
            pool_stride: 8,
            page: 2,
            page_size: 2,
            seed: 0,
        }
    }
}

impl FixtureShape {
    pub fn validate(&self) -> Result<(), String> {
        if self.blocks < 4 || self.blocks > 10_000 {
            return Err("blocks must be between 4 and 10000 (one preview epoch)".into());
        }
        if self.transactions_per_block == 0 || self.transactions_per_block > 100 {
            return Err("transactions_per_block must be between 1 and 100".into());
        }
        if self.log_rows == 0 || self.pool_stride == 0 || self.log_rows < self.pool_stride {
            return Err("log_rows must be positive and at least pool_stride > 0".into());
        }
        let page_end = self
            .page
            .checked_mul(self.page_size)
            .ok_or("pagination overflow")?;
        if self.page == 0
            || self.page_size == 0
            || self.page_size > 100
            || page_end > self.blocks
            || page_end > self.log_rows.div_ceil(self.pool_stride)
        {
            return Err("page must be positive, page_size 1..100, with enough blocks and pool matches to fill the page".into());
        }
        Ok(())
    }

    pub fn credential(&self, row: usize) -> StakeCredential {
        let mut bytes = [0u8; 28];
        bytes[..8].copy_from_slice(&self.seed.to_be_bytes());
        bytes[20..].copy_from_slice(&(row as u64).to_be_bytes());
        StakeCredential::AddrKeyhash(Hash::from(bytes))
    }

    pub fn stake_address(&self, row: usize) -> String {
        let StakeCredential::AddrKeyhash(hash) = self.credential(row) else {
            unreachable!()
        };
        StakeAddress::new(Network::Testnet, StakePayload::Stake(hash))
            .to_bech32()
            .expect("valid stake address")
    }
}

pub struct ApiFixture<Stores: ToyStores> {
    pub domain: ToyDomain<MeasuredStores<Stores>>,
    pub vectors: SyntheticVectors,
    pub blocks: Vec<RawBlock>,
    pub tail: Vec<RawBlock>,
    pub shape: FixtureShape,
    pub epoch: u64,
}

impl<Stores: ToyStores> ApiFixture<Stores> {
    pub fn new(
        stores: Stores,
        shape: FixtureShape,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        shape.validate().map_err(std::io::Error::other)?;
        let config = SyntheticBlockConfig {
            block_count: shape.blocks * 2,
            txs_per_block: shape.transactions_per_block,
            slot: 1,
            metadata_value: format!("benchmark-{}", shape.seed),
            ..Default::default()
        };
        let (mut blocks, vectors, chain_config) = build_synthetic_blocks(config);
        let tail = blocks.split_off(shape.blocks);
        let domain = ToyDomain::with_stores(
            Arc::new(dolos_cardano::include::preview::load()),
            chain_config,
            None,
            None,
            MeasuredStores::new(stores),
        );
        for batch in blocks.chunks(100) {
            domain.import_blocks(batch.to_vec())?;
        }
        let summary = dolos_cardano::eras::load_era_summary::<ToyDomain<MeasuredStores<Stores>>>(
            domain.state(),
        )?;
        let (epoch, _) = summary.slot_epoch(vectors.blocks[0].slot);
        let pool_hash = decode_pool(&vectors.pool_id)?;
        for start in (0..shape.log_rows).step_by(1000) {
            let writer = domain.archive().start_writer()?;
            for row in start..(start + 1000).min(shape.log_rows) {
                let key = LogKey::from((
                    TemporalKey::from(summary.epoch_start(epoch)),
                    credential_to_key(&shape.credential(row)),
                ));
                let log = AccountEpochLog {
                    active_stake: Some(1_000_000 + row as u64),
                    pool_id: Some(if row % shape.pool_stride == 0 {
                        pool_hash
                    } else {
                        Hash::from([0x55; 28])
                    }),
                    ..Default::default()
                };
                writer.write_log_typed(&key, &log)?;
            }
            writer.commit()?;
        }
        domain.archive().counters.reset();
        Ok(Self {
            domain,
            vectors,
            blocks,
            tail,
            shape,
            epoch,
        })
    }
}

pub fn decode_pool(pool: &str) -> Result<Hash<28>, ChainError> {
    use bech32::FromBase32;
    let (_, data, _) = bech32::decode(pool).map_err(|_| ChainError::InvalidPoolParams)?;
    let bytes = Vec::<u8>::from_base32(&data).map_err(|_| ChainError::InvalidPoolParams)?;
    let bytes: [u8; 28] = bytes
        .try_into()
        .map_err(|_| ChainError::InvalidPoolParams)?;
    Ok(Hash::from(bytes))
}
