use rocksdb::{IteratorMode, WriteBatch, DB};
use serde::{Deserialize, Serialize};
use std::convert::{TryFrom, TryInto};

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum WalAction {
    Apply,
    Undo,
    Mark,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Value(WalAction, super::BlockSlot, super::BlockHash);

impl Value {
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
            WalAction::Undo => None,
            WalAction::Mark => None,
        }
    }

    pub fn is_apply(&self) -> bool {
        match self.0 {
            WalAction::Apply => true,
            _ => false,
        }
    }

    pub fn is_mark(&self) -> bool {
        match self.0 {
            WalAction::Mark => true,
            _ => false,
        }
    }

    pub fn is_undo(&self) -> bool {
        match self.0 {
            WalAction::Undo => true,
            _ => false,
        }
    }
}

crate::kv_table!(pub WalKV: super::types::DBInt => super::types::DBSerde<Value>);

impl WalKV {
    fn stage_append(
        db: &DB,
        last_seq: u64,
        value: Value,
        batch: &mut WriteBatch,
    ) -> Result<u64, super::Error> {
        let new_seq = last_seq + 1;

        Self::stage_upsert(
            db,
            super::types::DBInt(new_seq),
            super::types::DBSerde(value),
            batch,
        )?;

        Ok(new_seq)
    }

    pub fn stage_roll_back(
        db: &DB,
        mut last_seq: u64,
        until: super::BlockSlot,
        batch: &mut WriteBatch,
    ) -> Result<u64, super::Error> {
        let iter = WalKV::iter_values(db, IteratorMode::End);

        for step in iter {
            let value = step.map_err(|_| super::Error::IO)?.0;

            if value.slot() <= until {
                last_seq = Self::stage_append(db, last_seq, value.into_mark().unwrap(), batch)?;
                break;
            }

            match value.into_undo() {
                Some(undo) => {
                    last_seq = Self::stage_append(db, last_seq, undo, batch)?;
                }
                None => continue,
            };
        }

        Ok(last_seq)
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
            match value?.0 {
                Value(WalAction::Apply | WalAction::Mark, slot, hash) => {
                    return Ok(Some((slot, hash)))
                }
                _ => (),
            }
        }

        Ok(None)
    }
}
