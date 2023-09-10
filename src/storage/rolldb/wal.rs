use rocksdb::{IteratorMode, WriteBatch, DB};
use serde::{Deserialize, Serialize};

use crate::{prelude::BlockSlot, storage::kvtable::*};

pub type Seq = u64;

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum WalAction {
    Apply,
    Undo,
    Mark,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Value(WalAction, super::BlockSlot, super::BlockHash);

impl Value {
    pub fn into_apply(
        slot: impl Into<super::BlockSlot>,
        hash: impl Into<super::BlockHash>,
    ) -> Self {
        Self(WalAction::Apply, slot.into(), hash.into())
    }

    pub fn action(&self) -> WalAction {
        self.0
    }

    pub fn slot(&self) -> super::BlockSlot {
        self.1
    }

    pub fn hash(&self) -> &super::BlockHash {
        &self.2
    }

    pub fn into_undo(self) -> Option<Self> {
        match self.0 {
            WalAction::Apply => Some(Self(WalAction::Undo, self.1, self.2)),
            WalAction::Undo => None,
            WalAction::Mark => None,
        }
    }

    pub fn into_mark(self) -> Option<Self> {
        match self.0 {
            WalAction::Apply => Some(Self(WalAction::Mark, self.1, self.2)),
            WalAction::Mark => Some(Self(WalAction::Mark, self.1, self.2)),
            WalAction::Undo => None,
        }
    }

    pub fn is_apply(&self) -> bool {
        matches!(self.0, WalAction::Apply)
    }

    pub fn is_mark(&self) -> bool {
        matches!(self.0, WalAction::Mark)
    }

    pub fn is_undo(&self) -> bool {
        matches!(self.0, WalAction::Undo)
    }
}

// slot => block hash
pub struct WalKV;

impl KVTable<DBInt, DBSerde<Value>> for WalKV {
    const CF_NAME: &'static str = "WalKV";
}

impl WalKV {
    fn stage_append(
        db: &DB,
        last_seq: Seq,
        value: Value,
        batch: &mut WriteBatch,
    ) -> Result<u64, super::Error> {
        let new_seq = last_seq + 1;

        Self::stage_upsert(db, DBInt(new_seq), DBSerde(value), batch);

        Ok(new_seq)
    }

    pub fn stage_roll_back(
        db: &DB,
        mut last_seq: Seq,
        until: super::BlockSlot,
        batch: &mut WriteBatch,
    ) -> Result<(u64, Vec<BlockSlot>), super::Error> {
        let iter = WalKV::iter_values(db, IteratorMode::End);
        let mut removed_blocks = vec![];

        for step in iter {
            let value = step.map_err(|_| super::Error::IO)?.0;

            if value.slot() <= until {
                last_seq = Self::stage_append(db, last_seq, value.into_mark().unwrap(), batch)?;
                break;
            }

            match value.into_undo() {
                Some(undo) => {
                    removed_blocks.push(undo.slot());
                    last_seq = Self::stage_append(db, last_seq, undo, batch)?;
                }
                None => continue,
            };
        }

        Ok((last_seq, removed_blocks))
    }

    pub fn stage_roll_forward(
        db: &DB,
        last_seq: u64,
        slot: super::BlockSlot,
        hash: super::BlockHash,
        batch: &mut WriteBatch,
    ) -> Result<u64, super::Error> {
        let last_seq =
            Self::stage_append(db, last_seq, Value(WalAction::Apply, slot, hash), batch)?;

        Ok(last_seq)
    }

    pub fn find_tip(db: &DB) -> Result<Option<(super::BlockSlot, super::BlockHash)>, super::Error> {
        let iter = WalKV::iter_values(db, IteratorMode::End);

        for value in iter {
            if let Value(WalAction::Apply | WalAction::Mark, slot, hash) = value?.0 {
                return Ok(Some((slot, hash)));
            }
        }

        Ok(None)
    }
}
