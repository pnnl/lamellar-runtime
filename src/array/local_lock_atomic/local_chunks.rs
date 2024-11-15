use crate::array::iterator::local_iterator::{IndexedLocalIterator, LocalIterator};
use crate::array::iterator::private::*;
use crate::array::local_lock_atomic::*;
use crate::array::LamellarArray;
use crate::darc::local_rw_darc::{LocalRwDarcReadGuard, LocalRwDarcWriteGuard};
use crate::memregion::Dist;

use std::sync::Arc;

use self::iterator::IterLockFuture;

/// An iterator over immutable (nonoverlapping) local chunks (of size chunk_size) of a [LocalLockArray]
/// This struct is created by awaiting or blocking on the handle returned by [LocalLockArray::read_local_chunks]
#[derive(Clone)]
pub struct LocalLockLocalChunks<T: Dist> {
    pub(crate) chunk_size: usize,
    pub(crate) index: usize,     //global index within the array local data
    pub(crate) end_index: usize, //global index within the array local data
    pub(crate) array: LocalLockArray<T>,
    pub(crate) lock_guard: Arc<LocalRwDarcReadGuard<()>>,
}

impl<T: Dist> InnerIter for LocalLockLocalChunks<T> {
    fn lock_if_needed(&self, _s: Sealed) -> Option<IterLockFuture> {
        None
    }
    fn iter_clone(&self, _s: Sealed) -> Self {
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
/// This struct is created by awaiting or blocking on the handle returned by [LocalLockArray::write_local_chunks]
pub struct LocalLockLocalChunksMut<T: Dist> {
    // data: &'a mut [T],
    pub(crate) chunk_size: usize,
    pub(crate) index: usize,     //global index within the array local data
    pub(crate) end_index: usize, //global index within the array local data
    pub(crate) array: LocalLockArray<T>,
    // lock: LocalRwDarc<()>,
    pub(crate) lock_guard: Arc<LocalRwDarcWriteGuard<()>>,
}

impl<T: Dist> InnerIter for LocalLockLocalChunksMut<T> {
    fn lock_if_needed(&self, _s: Sealed) -> Option<IterLockFuture> {
        None
    }
    fn iter_clone(&self, _s: Sealed) -> Self {
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

/// Provides mutable access to a chunk of a PEs local data to provide "local" indexing while maintaining safety guarantees of the array type.
///
/// This derefences down to a `&mut [T]`.
///
/// This struct is the item type returned when iterating over a [LocalLockLocalChunksMut] iterator created using [LocalLockArray::write_local_chunks].
/// While the Local Chunk iterator is valid, each chunk is guaranteed to have exclusive access to the data it points to (allowing for the safe deref into `&mut [T]`), preventing any other local or remote access.
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
    fn init(&self, start_i: usize, cnt: usize, _s: Sealed) -> Self {
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
    fn init(&self, start_i: usize, cnt: usize, _s: Sealed) -> Self {
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
    /// Constructs a handle for immutably iterating over fixed sized chunks(slices) of the local data of this array.
    /// This handle must be either await'd in an async context or block'd in an non-async context.
    /// Awaiting or blocking will not return until the read lock has been acquired.
    ///
    /// the returned iterator is a lamellar [LocalIterator] and also captures a read lock on the local data.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,40,Distribution::Block).block();
    /// let my_pe = world.my_pe();
    /// //block in a non-async context
    /// let _ = array.read_local_chunks(5).block().enumerate().for_each(move|(i,chunk)| {
    ///     println!("PE: {my_pe} i: {i} chunk: {chunk:?}");
    /// }).block();
    ///
    /// //await in an async context
    /// world.block_on(async move {
    ///     let _ = array.read_local_chunks(5).await.enumerate().for_each(move|(i,chunk)| {
    ///         println!("PE: {my_pe} i: {i} chunk: {chunk:?}");
    ///     }).await;
    /// });
    ///
    /// ```
    pub fn read_local_chunks(&self, chunk_size: usize) -> LocalLockLocalChunksHandle<T> {
        let lock = self.lock.read();
        LocalLockLocalChunksHandle {
            chunk_size,
            index: 0,
            end_index: 0,
            array: self.clone(),
            lock_handle: lock,
        }
    }
    /// Constructs a handle for mutably iterating over fixed sized chunks(slices) of the local data of this array.
    /// This handle must be either await'd in an async context or block'd in an non-async context.
    /// Awaiting or blocking will not return until the write lock has been acquired.
    ///
    /// the returned iterator is a lamellar [LocalIterator] and also captures a write lock on the local data.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: LocalLockArray<usize> = LocalLockArray::new(&world,40,Distribution::Block).block();
    /// let my_pe = world.my_pe();
    /// let _ = array.write_local_chunks(5).block().enumerate().for_each(move|(i, mut chunk)| {
    ///         for elem in chunk.iter_mut() {
    ///             *elem = i;
    ///         }
    ///     }).block();
    /// world.block_on(async move {
    ///     let _ = array.write_local_chunks(5).await.enumerate().for_each(move|(i, mut chunk)| {
    ///         for elem in chunk.iter_mut() {
    ///             *elem = i;
    ///         }
    ///     }).await;
    /// });
    /// ```
    pub fn write_local_chunks(&self, chunk_size: usize) -> LocalLockLocalChunksMutHandle<T> {
        let lock = self.lock.write();
        LocalLockLocalChunksMutHandle {
            chunk_size,
            index: 0,
            end_index: 0,
            array: self.clone(),
            // lock: self.lock.clone(),
            lock_handle: lock,
        }
    }
}
