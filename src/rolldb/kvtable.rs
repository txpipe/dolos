use std::marker::PhantomData;

type RocksIterator<'a> = rocksdb::DBIteratorWithThreadMode<'a, rocksdb::DB>;

pub struct ValueIterator<'a, V>(RocksIterator<'a>, PhantomData<V>);

impl<'a, V> ValueIterator<'a, V> {
    pub fn new(inner: RocksIterator<'a>) -> Self {
        Self(inner, Default::default())
    }
}

impl<'a, V> Iterator for ValueIterator<'a, V>
where
    V: From<Box<[u8]>>,
{
    type Item = Result<V, super::Error>;

    fn next(&mut self) -> Option<Result<V, super::Error>> {
        match self.0.next() {
            Some(Ok((_, value))) => Some(Ok(V::from(value))),
            Some(Err(err)) => {
                tracing::error!(?err);
                Some(Err(super::Error::IO))
            }
            None => None,
        }
    }
}

pub struct KeyIterator<'a, K>(RocksIterator<'a>, PhantomData<K>);

impl<'a, K> KeyIterator<'a, K> {
    pub fn new(inner: RocksIterator<'a>) -> Self {
        Self(inner, Default::default())
    }
}

impl<'a, K> Iterator for KeyIterator<'a, K>
where
    K: From<Box<[u8]>>,
{
    type Item = Result<K, super::Error>;

    fn next(&mut self) -> Option<Result<K, super::Error>> {
        match self.0.next() {
            Some(Ok((key, _))) => Some(Ok(K::from(key))),
            Some(Err(err)) => {
                tracing::error!(?err);
                Some(Err(super::Error::IO))
            }
            None => None,
        }
    }
}

pub struct EntryIterator<'a, K, V>(RocksIterator<'a>, PhantomData<(K, V)>);

impl<'a, K, V> EntryIterator<'a, K, V> {
    pub fn new(inner: RocksIterator<'a>) -> Self {
        Self(inner, Default::default())
    }
}

impl<'a, K, V> Iterator for EntryIterator<'a, K, V>
where
    K: From<Box<[u8]>>,
    V: From<Box<[u8]>>,
{
    type Item = Result<(K, V), super::Error>;

    fn next(&mut self) -> Option<Result<(K, V), super::Error>> {
        match self.0.next() {
            Some(Ok((key, value))) => {
                let key_out = K::from(key);
                let value_out = V::from(value);

                Some(Ok((key_out, value_out)))
            }
            Some(Err(err)) => {
                tracing::error!(?err);
                Some(Err(super::Error::IO))
            }
            None => None,
        }
    }
}

pub trait KVTable<K, V>
where
    Box<[u8]>: From<K>,
    Box<[u8]>: From<V>,
    K: From<Box<[u8]>>,
    V: From<Box<[u8]>>,
{
    const CF_NAME: &'static str;

    fn cf(db: &rocksdb::DB) -> rocksdb::ColumnFamilyRef {
        db.cf_handle(Self::CF_NAME).unwrap()
    }

    fn get_by_key(db: &rocksdb::DB, k: K) -> Result<Option<V>, crate::rolldb::Error> {
        let cf = Self::cf(db);
        let raw_key = Box::<[u8]>::from(k);
        let raw_value = db
            .get_cf(&cf, raw_key)
            .map_err(|_| crate::rolldb::Error::IO)?
            .map(|x| Box::from(x.as_slice()));

        match raw_value {
            Some(x) => {
                let out = <V>::from(x);
                Ok(Some(out))
            }
            None => Ok(None),
        }
    }

    fn stage_upsert(db: &rocksdb::DB, k: K, v: V, batch: &mut rocksdb::WriteBatch) {
        let cf = Self::cf(db);

        let k_raw = Box::<[u8]>::from(k);
        let v_raw = Box::<[u8]>::from(v);

        batch.put_cf(&cf, k_raw, v_raw);
    }

    fn iter_keys<'a>(
        db: &'a rocksdb::DB,
        mode: rocksdb::IteratorMode,
    ) -> crate::rolldb::kvtable::KeyIterator<'a, K> {
        let cf = Self::cf(db);
        let inner = db.iterator_cf(&cf, mode);
        crate::rolldb::kvtable::KeyIterator::new(inner)
    }

    fn iter_values<'a>(
        db: &'a rocksdb::DB,
        mode: rocksdb::IteratorMode,
    ) -> crate::rolldb::kvtable::ValueIterator<'a, V> {
        let cf = Self::cf(db);
        let inner = db.iterator_cf(&cf, mode);
        crate::rolldb::kvtable::ValueIterator::new(inner)
    }

    fn iter_entries<'a>(
        db: &'a rocksdb::DB,
        mode: rocksdb::IteratorMode,
    ) -> crate::rolldb::kvtable::EntryIterator<'a, K, V> {
        let cf = Self::cf(db);
        let inner = db.iterator_cf(&cf, mode);
        crate::rolldb::kvtable::EntryIterator::new(inner)
    }

    fn iter_entries_from<'a>(
        db: &'a rocksdb::DB,
        from: K,
    ) -> crate::rolldb::kvtable::EntryIterator<'a, K, V> {
        let cf = Self::cf(db);
        let from_raw = Box::<[u8]>::from(from);
        let inner = db.iterator_cf(
            &cf,
            rocksdb::IteratorMode::From(&from_raw, rocksdb::Direction::Forward),
        );
        crate::rolldb::kvtable::EntryIterator::new(inner)
    }

    fn last_key(db: &rocksdb::DB) -> Result<Option<K>, crate::rolldb::Error> {
        let mut iter = Self::iter_keys(db, rocksdb::IteratorMode::End);

        match iter.next() {
            None => Ok(None),
            Some(x) => Ok(Some(x?)),
        }
    }

    fn last_value(db: &rocksdb::DB) -> Result<Option<V>, crate::rolldb::Error> {
        let mut iter = Self::iter_values(db, rocksdb::IteratorMode::End);

        match iter.next() {
            None => Ok(None),
            Some(x) => Ok(Some(x?)),
        }
    }

    fn last_entry(db: &rocksdb::DB) -> Result<Option<(K, V)>, crate::rolldb::Error> {
        let mut iter = Self::iter_entries(db, rocksdb::IteratorMode::End);

        match iter.next() {
            None => Ok(None),
            Some(x) => Ok(Some(x?)),
        }
    }

    fn stage_delete(db: &rocksdb::DB, key: K, batch: &mut rocksdb::WriteBatch) {
        let cf = Self::cf(db);
        let k_raw = Box::<[u8]>::from(key);
        batch.delete_cf(&cf, k_raw);
    }
}
