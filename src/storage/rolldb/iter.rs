use crate::storage::kvtable::*;

type Error = crate::storage::kvtable::Error;

type Item = Result<super::wal::Value, Error>;

enum Cursor<'a> {
    Chain(EntryIterator<'a, DBInt, DBHash>),
    Wal(ValueIterator<'a, DBSerde<super::wal::Value>>),
}

impl<'a> Cursor<'a> {
    fn is_chain(&self) -> bool {
        matches!(self, Cursor::Chain(_))
    }

    fn next(&mut self) -> Option<Item> {
        match self {
            Cursor::Chain(iter) => {
                let next = iter.next()?;

                let next = next.map(|(slot, hash)| super::wal::Value::into_apply(slot, hash));

                Some(next)
            }
            Cursor::Wal(iter) => {
                let next = iter.next()?;

                let next = next.map(|val| val.0);

                Some(next)
            }
        }
    }
}

pub struct RollIterator<'a> {
    db: &'a rocksdb::DB,
    cursor: Cursor<'a>,
}

impl<'a> RollIterator<'a> {
    pub fn from_origin(db: &'a rocksdb::DB) -> Self {
        Self {
            db,
            cursor: Cursor::Chain(super::ChainKV::iter_entries_start(db)),
        }
    }

    pub fn from_chain(db: &'a rocksdb::DB, slot: super::BlockSlot) -> Self {
        Self {
            db,
            cursor: Cursor::Chain(super::ChainKV::iter_entries_from(db, slot.into())),
        }
    }

    pub fn from_wal(db: &'a rocksdb::DB, seq: super::wal::Seq) -> Self {
        Self {
            db,
            cursor: Cursor::Wal(super::wal::WalKV::iter_values_from(db, seq.into())),
        }
    }
}

impl<'a> Iterator for RollIterator<'a> {
    type Item = Item;

    fn next(&mut self) -> Option<Self::Item> {
        let mut next = self.cursor.next();

        if next.is_none() && self.cursor.is_chain() {
            let from = rocksdb::IteratorMode::Start;
            let iter = super::wal::WalKV::iter_values(self.db, from);
            self.cursor = Cursor::Wal(iter);

            next = self.cursor.next();
        }

        next
    }
}
