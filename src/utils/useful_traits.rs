use once_cell::sync::OnceCell;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

pub trait StaticVarExt<T: ?Sized> {
    fn read(&self) -> RwLockReadGuard<'_, T>;
    fn write(&self) -> RwLockWriteGuard<'_, T>;
}

impl<T> StaticVarExt<T> for OnceCell<RwLock<T>> {
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

pub trait StaticVarVecExt<T> {
    fn drain(&self) -> Vec<T>;
}

impl<T> StaticVarVecExt<T> for OnceCell<RwLock<Vec<T>>> {
    /// get().unwrap().write().drain(..).collect()
    fn drain(&self) -> Vec<T> {
        self.write().drain(..).collect()
    }
}
