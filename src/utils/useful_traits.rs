use std::{collections::HashMap, hash::{Hash, BuildHasher}};

use easy_ext::ext;
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