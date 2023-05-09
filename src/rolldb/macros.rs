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

#[macro_export]
macro_rules! kv_table {
    ($vis:vis $name:ident : $key_type:ty => $value_type:ty) => {
        $vis struct $name;

        impl $name {
            pub const CF_NAME: &str = stringify!($name);

            pub fn cf(db: &rocksdb::DB) -> rocksdb::ColumnFamilyRef {
                db.cf_handle(Self::CF_NAME).unwrap()
            }

            pub fn get_by_key(
                db: &rocksdb::DB,
                k: $key_type,
            ) -> Result<Option<$value_type>, $crate::rolldb::Error> {
                let cf = Self::cf(db);
                let raw_key = Box::<[u8]>::try_from(k).map_err(|_| $crate::rolldb::Error::Serde)?;
                let raw_value = db
                    .get_cf(&cf, raw_key)
                    .map_err(|_| $crate::rolldb::Error::IO)?
                    .map(|x| Box::from(x.as_slice()));

                match raw_value {
                    Some(x) => {
                        let out =
                            <$value_type>::try_from(x).map_err(|_| $crate::rolldb::Error::Serde)?;
                        Ok(Some(out))
                    }
                    None => Ok(None),
                }
            }

            pub fn stage_upsert(
                db: &rocksdb::DB,
                k: $key_type,
                v: $value_type,
                batch: &mut rocksdb::WriteBatch,
            ) {
                let cf = Self::cf(db);

                let k_raw = Box::<[u8]>::from(k);
                let v_raw = Box::<[u8]>::from(v);

                batch.put_cf(&cf, k_raw, v_raw);
            }

            pub fn iter_keys<'a>(
                db: &'a rocksdb::DB,
                mode: rocksdb::IteratorMode,
            ) -> crate::rolldb::macros::KeyIterator<'a, $key_type> {
                let cf = Self::cf(db);
                let inner = db.iterator_cf(&cf, mode);
                crate::rolldb::macros::KeyIterator::new(inner)
            }

            pub fn iter_values<'a>(
                db: &'a rocksdb::DB,
                mode: rocksdb::IteratorMode,
            ) -> crate::rolldb::macros::ValueIterator<'a, $value_type> {
                let cf = Self::cf(db);
                let inner = db.iterator_cf(&cf, mode);
                crate::rolldb::macros::ValueIterator::new(inner)
            }

            pub fn iter_entries<'a>(
                db: &'a rocksdb::DB,
                mode: rocksdb::IteratorMode,
            ) -> crate::rolldb::macros::EntryIterator<'a, $key_type, $value_type> {
                let cf = Self::cf(db);
                let inner = db.iterator_cf(&cf, mode);
                crate::rolldb::macros::EntryIterator::new(inner)
            }

            pub fn iter_entries_from<'a>(
                db: &'a rocksdb::DB,
                from: $key_type,
            ) -> crate::rolldb::macros::EntryIterator<'a, $key_type, $value_type> {
                let cf = Self::cf(db);
                let from_raw = Box::<[u8]>::from(from);
                let inner = db.iterator_cf(&cf, rocksdb::IteratorMode::From(&from_raw, rocksdb::Direction::Forward));
                crate::rolldb::macros::EntryIterator::new(inner)
            }

            pub fn last_key(db: &DB) -> Result<Option<$key_type>, $crate::rolldb::Error> {
                let mut iter = Self::iter_keys(db, rocksdb::IteratorMode::End);

                match iter.next() {
                    None => Ok(None),
                    Some(x) => Ok(Some(x?)),
                }
            }

            pub fn last_value(db: &DB) -> Result<Option<$value_type>, $crate::rolldb::Error> {
                let mut iter = Self::iter_values(db, rocksdb::IteratorMode::End);

                match iter.next() {
                    None => Ok(None),
                    Some(x) => Ok(Some(x?)),
                }
            }

            pub fn stage_delete(db: &DB, key: $key_type, batch: &mut WriteBatch) {
                let cf = Self::cf(db);
                let k_raw = Box::<[u8]>::from(key);
                batch.delete_cf(&cf, k_raw);
            }
        }
    };
}
