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
    V: TryFrom<Box<[u8]>, Error = super::Error>,
{
    type Item = Result<V, super::Error>;

    fn next(&mut self) -> Option<Result<V, super::Error>> {
        match self.0.next() {
            Some(Ok((_, value))) => Some(V::try_from(value)),
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
    K: TryFrom<Box<[u8]>, Error = super::Error>,
{
    type Item = Result<K, super::Error>;

    fn next(&mut self) -> Option<Result<K, super::Error>> {
        match self.0.next() {
            Some(Ok((key, _))) => Some(K::try_from(key)),
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
                    .get_cf(cf, raw_key)
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
            ) -> Result<(), $crate::rolldb::Error> {
                let cf = Self::cf(db);

                let k_raw = Box::<[u8]>::try_from(k).map_err(|_| $crate::rolldb::Error::Serde)?;
                let v_raw = Box::<[u8]>::try_from(v).map_err(|_| $crate::rolldb::Error::Serde)?;

                batch.put_cf(cf, k_raw, v_raw);

                Ok(())
            }

            pub fn iter_keys<'a>(
                db: &'a rocksdb::DB,
                mode: rocksdb::IteratorMode,
            ) -> crate::rolldb::macros::KeyIterator<'a, $key_type> {
                let cf = Self::cf(db);
                let inner = db.iterator_cf(cf, mode);
                crate::rolldb::macros::KeyIterator::new(inner)
            }

            pub fn iter_values<'a>(
                db: &'a rocksdb::DB,
                mode: rocksdb::IteratorMode,
            ) -> crate::rolldb::macros::ValueIterator<'a, $value_type> {
                let cf = Self::cf(db);
                let inner = db.iterator_cf(cf, mode);
                crate::rolldb::macros::ValueIterator::new(inner)
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
        }
    };
}
