use crate::array::iterator::local_iterator::{IndexedLocalIterator, LocalIterator};
use crate::array::iterator::private::*;
use crate::array::r#unsafe::*;
use crate::array::LamellarArray;
use crate::memregion::Dist;

use self::iterator::IterLockFuture;

/// An iterator over immutable (nonoverlapping) local chunks (of size chunk_size) of an [UnsafeArray]
/// This struct is created by calling [UnsafeArray::local_chunks]
#[derive(Clone)]
pub struct UnsafeLocalChunks<T: Dist> {
    chunk_size: usize,
    index: usize,     //global index within the array local data
    end_index: usize, //global index within the array local data
    array: UnsafeArray<T>,
}

impl<T: Dist> InnerIter for UnsafeLocalChunks<T> {
    fn lock_if_needed(&self, _s: Sealed) -> Option<IterLockFuture> {
        None
    }
    fn iter_clone(&self, _s: Sealed) -> Self {
        UnsafeLocalChunks {
            chunk_size: self.chunk_size,
            index: self.index,
            end_index: self.end_index,
            array: self.array.clone(),
        }
    }
}

/// An iterator over immutable (nonoverlapping) local chunks (of size chunk_size) of an [UnsafeArray]
/// This struct is created by calling [UnsafeArray::local_chunks_mut]
#[derive(Clone)]
pub struct UnsafeLocalChunksMut<T: Dist> {
    chunk_size: usize,
    index: usize,     //global index within the array local data
    end_index: usize, //global index within the array local data
    array: UnsafeArray<T>,
}

impl<T: Dist> InnerIter for UnsafeLocalChunksMut<T> {
    fn lock_if_needed(&self, _s: Sealed) -> Option<IterLockFuture> {
        None
    }
    fn iter_clone(&self, _s: Sealed) -> Self {
        UnsafeLocalChunksMut {
            chunk_size: self.chunk_size,
            index: self.index,
            end_index: self.end_index,
            array: self.array.clone(),
        }
    }
}

impl<T: Dist + 'static> LocalIterator for UnsafeLocalChunks<T> {
    type Item = &'static [T];
    type Array = UnsafeArray<T>;
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
        UnsafeLocalChunks {
            chunk_size: self.chunk_size,
            index: new_start_i,
            end_index: end_i,
            array: self.array.clone(),
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
            Some(unsafe {
                std::slice::from_raw_parts_mut(
                    self.array.local_as_mut_ptr().offset(start_i as isize),
                    end_i - start_i,
                )
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

impl<T: Dist + 'static> IndexedLocalIterator for UnsafeLocalChunks<T> {
    fn iterator_index(&self, index: usize) -> Option<usize> {
        if index * self.chunk_size < self.array.len() {
            Some(index) //everyone at this point as calculated the actual index (cause we are local only) so just return it
        } else {
            None
        }
    }
}

impl<T: Dist + 'static> LocalIterator for UnsafeLocalChunksMut<T> {
    type Item = &'static mut [T];
    type Array = UnsafeArray<T>;
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
        UnsafeLocalChunksMut {
            chunk_size: self.chunk_size,
            index: new_start_i,
            end_index: end_i,
            array: self.array.clone(),
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
            Some(unsafe {
                std::slice::from_raw_parts_mut(
                    self.array.local_as_mut_ptr().offset(start_i as isize),
                    end_i - start_i,
                )
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

impl<T: Dist + 'static> IndexedLocalIterator for UnsafeLocalChunksMut<T> {
    fn iterator_index(&self, index: usize) -> Option<usize> {
        if index * self.chunk_size < self.array.len() {
            Some(index) //everyone at this point as calculated the actual index (cause we are local only) so just return it
        } else {
            None
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    /// immutably iterate over fixed sized chunks(slices) of the local data of this array.
    /// the returned iterator is a lamellar [LocalIterator]
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world,40,Distribution::Block).block();
    /// let my_pe = world.my_pe();
    ///
    /// let _ = unsafe{array.local_chunks(5).enumerate().for_each(move|(i,chunk)| {
    ///     println!("PE: {my_pe} i: {i} chunk: {chunk:?}");
    /// })}.spawn();
    /// array.wait_all();
    ///
    /// ```
    pub unsafe fn local_chunks(&self, chunk_size: usize) -> UnsafeLocalChunks<T> {
        UnsafeLocalChunks {
            chunk_size,
            index: 0,
            end_index: 0,
            array: self.clone(),
        }
    }

    /// mutably iterate over fixed sized chunks(slices) of the local data of this array.
    /// the returned iterator is a lamellar [LocalIterator]
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world,40,Distribution::Block).block();
    /// let my_pe = world.my_pe();
    ///
    /// unsafe{
    ///     let _ = array.local_chunks_mut(5).enumerate().for_each(move|(i,chunk)| {
    ///         println!("PE: {my_pe} i: {i} chunk: {chunk:?}");
    ///     }).spawn();
    /// }
    /// array.wait_all();
    ///
    /// ```
    pub unsafe fn local_chunks_mut(&self, chunk_size: usize) -> UnsafeLocalChunksMut<T> {
        UnsafeLocalChunksMut {
            chunk_size,
            index: 0,
            end_index: 0,
            array: self.clone(),
        }
    }
}
