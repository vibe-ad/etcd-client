use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

#[cfg(any(feature = "tls-openssl", feature = "failover"))]
use std::sync::{Mutex, MutexGuard};

pub trait RwLockExt<T: ?Sized> {
    fn read_unpoisoned(&self) -> RwLockReadGuard<'_, T>;
    fn write_unpoisoned(&self) -> RwLockWriteGuard<'_, T>;
}

impl<T: ?Sized> RwLockExt<T> for RwLock<T> {
    fn read_unpoisoned(&self) -> RwLockReadGuard<'_, T> {
        match self.read() {
            Ok(guard) => guard,
            Err(e) => {
                self.clear_poison();
                e.into_inner()
            }
        }
    }

    fn write_unpoisoned(&self) -> RwLockWriteGuard<'_, T> {
        match self.write() {
            Ok(guard) => guard,
            Err(e) => {
                self.clear_poison();
                e.into_inner()
            }
        }
    }
}

#[cfg(any(feature = "tls-openssl", feature = "failover"))]
pub trait MutexExt<T: ?Sized> {
    fn lock_unpoisoned(&self) -> MutexGuard<'_, T>;
}

#[cfg(any(feature = "tls-openssl", feature = "failover"))]
impl<T: ?Sized> MutexExt<T> for Mutex<T> {
    fn lock_unpoisoned(&self) -> MutexGuard<'_, T> {
        match self.lock() {
            Ok(guard) => guard,
            Err(e) => {
                self.clear_poison();
                e.into_inner()
            }
        }
    }
}
