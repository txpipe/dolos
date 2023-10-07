use rocksdb::{IteratorMode, WriteBatch, DB};
use serde::{Deserialize, Serialize};

use crate::storage::kvtable::*;

pub type Seq = u64;

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum WalAction {
    Apply,
    Undo,
    Mark,
    Origin,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Value(WalAction, super::BlockSlot, super::BlockHash);

impl Value {
    pub fn origin() -> Self {
        Self(WalAction::Origin, 0, super::BlockHash::new([0; 32]))
    }

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
            WalAction::Origin => None,
        }
    }

    pub fn into_mark(self) -> Option<Self> {
        match self.0 {
            WalAction::Apply => Some(Self(WalAction::Mark, self.1, self.2)),
            WalAction::Mark => Some(Self(WalAction::Mark, self.1, self.2)),
            WalAction::Origin => Some(Self(WalAction::Origin, self.1, self.2)),
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

    pub fn is_origin(&self) -> bool {
        matches!(self.0, WalAction::Origin)
    }
}

// slot => block hash
pub struct WalKV;

impl KVTable<DBInt, DBSerde<Value>> for WalKV {
    const CF_NAME: &'static str = "WalKV";
}

pub struct WalIterator<'a>(pub EntryIterator<'a, DBInt, DBSerde<Value>>);

impl Iterator for WalIterator<'_> {
    type Item = Result<(u64, Value), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|v| v.map(|(seq, val)| (seq.0, val.0)))
    }
}

impl WalKV {
    pub fn initialize(db: &DB) -> Result<Seq, Error> {
        if Self::is_empty(db) {
            Self::write_seed(db)?;
            Ok(0)
        } else {
            let last = Self::last_key(db)?.map(|x| x.0);
            Ok(last.unwrap())
        }
    }

    fn write_seed(db: &DB) -> Result<(), Error> {
        let mut batch = WriteBatch::default();
        let k = DBInt(0);
        let v = DBSerde(Value::origin());
        Self::stage_upsert(db, k, v, &mut batch);

        db.write(batch).map_err(|_| Error::IO)
    }

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

    pub fn stage_roll_back_origin(
        db: &DB,
        mut last_seq: Seq,
        batch: &mut WriteBatch,
    ) -> Result<u64, super::Error> {
        let iter = WalKV::iter_values(db, IteratorMode::End);

        for step in iter {
            let value = step.map_err(|_| super::Error::IO)?.0;

            if value.is_origin() {
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
            if let Value(WalAction::Apply | WalAction::Mark, slot, hash) = value?.0 {
                return Ok(Some((slot, hash)));
            }
        }

        Ok(None)
    }
}
