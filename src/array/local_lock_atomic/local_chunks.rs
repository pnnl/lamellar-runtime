use crate::array::iterator::local_iterator::{IndexedLocalIterator, LocalIterator};
use crate::array::iterator::private::*;
use crate::array::local_lock_atomic::*;
use crate::array::LamellarArray;
use crate::config;
use crate::darc::local_rw_darc::{LocalRwDarcReadGuard, LocalRwDarcWriteGuard};
use crate::memregion::Dist;

use std::sync::Arc;

/// An iterator over immutable (nonoverlapping) local chunks (of size chunk_size) of a [LocalLockArray]
/// This struct is created by calling [LocalLockArray::read_local_chunks] or [LocalLockArray::blocking_read_local_chunks]
#[derive(Clone)]
pub struct LocalLockLocalChunks<T: Dist> {
    chunk_size: usize,
    index: usize,     //global index within the array local data
    end_index: usize, //global index within the array local data
    array: LocalLockArray<T>,
    // lock: LocalRwDarc<()>,
    // lock_guard: Arc<RwLockReadGuardArc<()>>,
    lock_guard: Arc<LocalRwDarcReadGuard<()>>,
}

impl<T: Dist> IterClone for LocalLockLocalChunks<T> {
    fn iter_clone(&self, _: Sealed) -> Self {
        LocalLockLocalChunks {
            chunk_size: self.chunk_size,
            index: self.index,
            end_index: self.end_index,
            array: self.array.clone(),
            // lock: self.lock.clone(),
            lock_guard: self.lock_guard.clone(),
        }
    }
}

/// An iterator over mutable (nonoverlapping) local chunks (of size chunk_size) of a [LocalLockArray]
/// This struct is created by calling [LocalLockArray""write_local_chunks] or [LocalLockArray::blocking_write_local_chunks]
pub struct LocalLockLocalChunksMut<T: Dist> {
    // data: &'a mut [T],
    chunk_size: usize,
    index: usize,     //global index within the array local data
    end_index: usize, //global index within the array local data
    array: LocalLockArray<T>,
    // lock: LocalRwDarc<()>,
    lock_guard: Arc<LocalRwDarcWriteGuard<()>>,
}

impl<T: Dist> IterClone for LocalLockLocalChunksMut<T> {
    fn iter_clone(&self, _: Sealed) -> Self {
        LocalLockLocalChunksMut {
            chunk_size: self.chunk_size,
            index: self.index,
            end_index: self.end_index,
            array: self.array.clone(),
            // lock: self.lock.clone(),
            lock_guard: self.lock_guard.clone(),
        }
    }
}

#[derive(Debug)]
pub struct LocalLockMutChunkLocalData<'a, T: Dist> {
    data: &'a mut [T],
    _index: usize,
    _lock_guard: Arc<LocalRwDarcWriteGuard<()>>,
}

impl<T: Dist> Deref for LocalLockMutChunkLocalData<'_, T> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        self.data
    }
}
impl<T: Dist> DerefMut for LocalLockMutChunkLocalData<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.data
    }
}

impl<T: Dist> LocalIterator for LocalLockLocalChunks<T> {
    type Item = LocalLockLocalData<T>;
    type Array = LocalLockArray<T>;
    fn init(&self, start_i: usize, cnt: usize) -> Self {
        //these are with respect to the single elements, not chunk indexing and cnt
        let end_i = std::cmp::min(
            (start_i + cnt) * self.chunk_size,
            self.array.num_elems_local(),
        );
        let new_start_i = start_i * self.chunk_size;
        // println!(
        //     "start_i {} new_start_i {} end_i {} cnt: {}",
        //     start_i, new_start_i, end_i, cnt
        // );
        LocalLockLocalChunks {
            chunk_size: self.chunk_size,
            index: new_start_i,
            end_index: end_i,
            array: self.array.clone(),
            // lock: self.lock.clone(),
            lock_guard: self.lock_guard.clone(),
        }
    }
    fn array(&self) -> Self::Array {
        self.array.clone()
    }
    fn next(&mut self) -> Option<Self::Item> {
        // println!("next index {} end_index: {}", self.index, self.end_index);
        if self.index < self.end_index {
            let start_i = self.index;
            self.index += self.chunk_size;
            let end_i = std::cmp::min(self.index, self.end_index);
            // println!(
            //     "start_i {} end_i {} self.index {} self.end_index {}",
            //     start_i, end_i, self.index, self.end_index
            // );
            Some(LocalLockLocalData {
                array: self.array.clone(),
                start_index: start_i,
                end_index: end_i,
                // lock: self.lock.clone(),
                lock_guard: self.lock_guard.clone(),
            })
        } else {
            None
        }
    }
    fn elems(&self, in_elems: usize) -> usize {
        in_elems / self.chunk_size + (in_elems % self.chunk_size != 0) as usize
    }

    fn advance_index(&mut self, count: usize) {
        self.index = std::cmp::min(self.index + count * self.chunk_size, self.end_index);
    }
}

impl<T: Dist> IndexedLocalIterator for LocalLockLocalChunks<T> {
    fn iterator_index(&self, index: usize) -> Option<usize> {
        if index * self.chunk_size < self.array.len() {
            Some(index) //everyone at this point as calculated the actual index (cause we are local only) so just return it
        } else {
            None
        }
    }
}

impl<T: Dist + 'static> LocalIterator for LocalLockLocalChunksMut<T> {
    type Item = LocalLockMutChunkLocalData<'static, T>;
    type Array = LocalLockArray<T>;
    fn init(&self, start_i: usize, cnt: usize) -> Self {
        let end_i = std::cmp::min(
            (start_i + cnt) * self.chunk_size,
            self.array.num_elems_local(),
        );
        let new_start_i = start_i * self.chunk_size;
        // println!(
        //     "mut start_i {} new_start_i {} end_i {} cnt: {}",
        //     start_i, new_start_i, end_i, cnt
        // );
        LocalLockLocalChunksMut {
            chunk_size: self.chunk_size,
            index: new_start_i,
            end_index: end_i,
            array: self.array.clone(),
            // lock: self.lock.clone(),
            lock_guard: self.lock_guard.clone(),
        }
    }
    fn array(&self) -> Self::Array {
        self.array.clone()
    }
    fn next(&mut self) -> Option<Self::Item> {
        // println!(
        //     "mut next index {} end_index: {}",
        //     self.index, self.end_index
        // );
        if self.index < self.end_index {
            let start_i = self.index;
            self.index += self.chunk_size;
            let end_i = std::cmp::min(self.index, self.end_index);
            // println!(
            //     "mut start_i {} end_i {} self.index {} self.end_index {}",
            //     start_i, end_i, self.index, self.end_index
            // );
            Some(LocalLockMutChunkLocalData {
                //TODO we can probably do this similar to non mut way to avoid the unsafe...
                data: unsafe {
                    std::slice::from_raw_parts_mut(
                        self.array.array.local_as_mut_ptr().offset(start_i as isize),
                        end_i - start_i,
                    )
                },
                _index: 0,
                _lock_guard: self.lock_guard.clone(),
            })
        } else {
            None
        }
    }
    fn elems(&self, in_elems: usize) -> usize {
        in_elems / self.chunk_size + (in_elems % self.chunk_size != 0) as usize
    }

    fn advance_index(&mut self, count: usize) {
        self.index = std::cmp::min(self.index + count * self.chunk_size, self.end_index);
    }
}

impl<T: Dist + 'static> IndexedLocalIterator for LocalLockLocalChunksMut<T> {
    fn iterator_index(&self, index: usize) -> Option<usize> {
        if index * self.chunk_size < self.array.len() {
            //hmm should this be local num elems?
            Some(index) //everyone at this point as calculated the actual index (cause we are local only) so just return it
        } else {
            None
        }
    }
}

impl<T: Dist> LocalLockArray<T> {
    /// mutably iterate over fixed sized chunks(slices) of the local data of this array.
    /// the returned iterator is a lamellar [LocalIterator] and also captures a read lock on the local data.
    /// This call will block the calling task until a read lock is acquired.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,40,Distribution::Block);
    /// let my_pe = world.my_pe();
    /// world.block_on(async move {
    ///     let _ = array.read_local_chunks(5).await.enumerate().for_each(move|(i,chunk)| {
    ///         println!("PE: {my_pe} i: {i} chunk: {chunk:?}");
    ///     }).spawn();
    ///     array.await_all().await;
    /// });
    /// ```
    pub async fn read_local_chunks(&self, chunk_size: usize) -> LocalLockLocalChunks<T> {
        let lock = Arc::new(self.lock.read().await);
        LocalLockLocalChunks {
            chunk_size,
            index: 0,
            end_index: 0,
            array: self.clone(),
            // lock: self.lock.clone(),
            lock_guard: lock,
        }
    }

    /// immutably iterate over fixed sized chunks(slices) of the local data of this array.
    /// the returned iterator is a lamellar [LocalIterator] and also captures a read lock on the local data.
    /// This call will block the calling thread until a read lock is acquired.
    /// Calling within an asynchronous block may lead to deadlock, use [read_lock](self::LocalLockArray::read_local_chunks) instead.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,40,Distribution::Block);
    /// let my_pe = world.my_pe();
    ///
    /// let _ = array.blocking_read_local_chunks(5).enumerate().for_each(move|(i,chunk)| {
    ///     println!("PE: {my_pe} i: {i} chunk: {chunk:?}");
    /// }).block();
    ///
    /// ```
    pub fn blocking_read_local_chunks(&self, chunk_size: usize) -> LocalLockLocalChunks<T> {
        if std::thread::current().id() != *crate::MAIN_THREAD {
            let msg = format!("
                [LAMELLAR WARNING] You are calling `LocalLockArray::blocking_read_local_chunks` from within an async context which may lead to deadlock, it is recommended that you use `read_local_chunks().await;` instead! 
                Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {}", std::backtrace::Backtrace::capture()
            );
            match config().blocking_call_warning {
                Some(val) if val => println!("{msg}"),
                _ => println!("{msg}"),
            }
        }
        let lock = Arc::new(self.array.block_on(self.lock.read()));
        LocalLockLocalChunks {
            chunk_size,
            index: 0,
            end_index: 0,
            array: self.clone(),
            // lock: self.lock.clone(),
            lock_guard: lock,
        }
    }

    /// mutably iterate over fixed sized chunks(slices) of the local data of this array.
    /// the returned iterator is a lamellar [LocalIterator] and also captures the write lock on the local data.
    /// This call will block the calling task until the write lock is acquired.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,40,Distribution::Block);
    /// let my_pe = world.my_pe();
    /// world.block_on(async move {
    ///     let _ = array.write_local_chunks(5).await.enumerate().for_each(move|(i,chunk)| {
    ///         println!("PE: {my_pe} i: {i} chunk: {chunk:?}");
    ///     }).spawn();
    ///     array.await_all().await;
    /// });
    /// ```
    pub async fn write_local_chunks(&self, chunk_size: usize) -> LocalLockLocalChunksMut<T> {
        let lock = Arc::new(self.lock.write().await);
        LocalLockLocalChunksMut {
            chunk_size,
            index: 0,
            end_index: 0,
            array: self.clone(),
            // lock: self.lock.clone(),
            lock_guard: lock,
        }
    }

    /// mutably iterate over fixed sized chunks(slices) of the local data of this array.
    /// the returned iterator is a lamellar [LocalIterator] and also captures the write lock on the local data.
    /// This call will block the calling thread until the write lock is acquired.
    /// Calling within an asynchronous block may lead to deadlock, use [write_lock](self::LocalLockArray::write_local_chunks) instead.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,40,Distribution::Block);
    /// let my_pe = world.my_pe();
    ///
    /// let _ = array.blocking_write_local_chunks(5).enumerate().for_each(move|(i,chunk)| {
    ///     println!("PE: {my_pe} i: {i} chunk: {chunk:?}");
    /// }).spawn();
    /// array.wait_all();
    ///
    /// ```
    pub fn blocking_write_local_chunks(&self, chunk_size: usize) -> LocalLockLocalChunksMut<T> {
        if std::thread::current().id() != *crate::MAIN_THREAD {
            let msg = format!("
                [LAMELLAR WARNING] You are calling `LocalLockArray::blocking_write_local_chunks` from within an async context which may lead to deadlock, it is recommended that you use `write_local_chunks().await;` instead! 
                Set LAMELLAR_BLOCKING_CALL_WARNING=0 to disable this warning, Set RUST_LIB_BACKTRACE=1 to see where the call is occcuring: {}", std::backtrace::Backtrace::capture()
            );
            match config().blocking_call_warning {
                Some(val) if val => println!("{msg}"),
                _ => println!("{msg}"),
            }
        }
        let lock = Arc::new(self.array.block_on(self.lock.write()));
        LocalLockLocalChunksMut {
            chunk_size,
            index: 0,
            end_index: 0,
            array: self.clone(),
            // lock: self.lock.clone(),
            lock_guard: lock,
        }
    }
}
