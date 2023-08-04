use std::{collections::HashMap, hash::{Hash, BuildHasher}, str::FromStr};

use easy_ext::ext;
use hyper::{HeaderMap, http::HeaderName};
use once_cell::sync::OnceCell;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};


#[ext(StaticVarExt)]
pub impl<T> OnceCell<RwLock<T>> {
    /// get().unwrap().read()
    #[inline]
    fn read(&self) -> RwLockReadGuard<'_, T> {
        self.get().unwrap().read()
    }

    /// get().unwrap().write()
    #[inline]
    fn write(&self) -> RwLockWriteGuard<'_, T> {
        self.get().unwrap().write()
    }
}

#[ext(StaticVarVecExt)]
pub impl<T> OnceCell<RwLock<Vec<T>>> {
    /// get().unwrap().write().drain(..).collect()
    #[inline]
    fn drain(&self) -> Vec<T> {
        self.write().drain(..).collect()
    }
}

#[ext(StaticVarHashVecExt)]
pub impl<K: Eq + Hash, S: BuildHasher, T> OnceCell<RwLock<HashMap<K, RwLock<Vec<T>>, S>>> {

    /// get().unwrap().read()[&key].write().push(item);
    #[inline]
    fn push(&self, key: K, item: T) {
        self.read()[&key].write().push(item);
    }

    /// get().unwrap().read()[&key].write().drain(..).collect()
    #[inline]
    fn drain(&self, key: K) -> Vec<T> {
        self.read()[&key].write().drain(..).collect()
    }
}

#[ext(TupledResultTranspose)]
pub impl<T, E> (Result<T, E>, Result<T, E>) {
    #[inline]
    fn transpose(self) -> Result<(T, T), E> {
        match self {
            (Ok(a), Ok(b)) => Ok((a, b)),
            (Err(a), Err(_)) => Err(a),
            (Err(a), Ok(_)) => Err(a),
            (Ok(_), Err(b)) => Err(b),
        }
    }
}

#[ext(HashMapToHeaderMap)]
pub impl<K: AsRef<str>, V: AsRef<str>> HashMap<K, V> {
    #[inline]
    fn to_header_map(&self) -> anyhow::Result<HeaderMap> {
        let mut header = HeaderMap::new();
        for (key, value) in self {
            header.insert(HeaderName::from_str(key.as_ref())?, value.as_ref().parse()?);
        }
        Ok(header)
    }
}

#[ext(ResultFlatten)]
pub impl<T, E> Result<Result<T, E>, E> {
    #[inline]
    fn flatten_(self) -> Result<T, E> {
        match self {
            Ok(Ok(x)) => Ok(x),
            Ok(Err(e)) => Err(e),
            Err(e) => Err(e),
        }
    }
}