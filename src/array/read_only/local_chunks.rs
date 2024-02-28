use crate::array::iterator::local_iterator::{IndexedLocalIterator, LocalIterator};
use crate::array::iterator::private::*;
use crate::array::read_only::*;
use crate::array::LamellarArray;
use crate::memregion::Dist;

#[derive(Clone)]
pub struct ReadOnlyLocalChunks<T: Dist> {
    chunk_size: usize,
    index: usize,     //global index within the array local data
    end_index: usize, //global index within the array local data
    array: ReadOnlyArray<T>,
}

impl<T: Dist> IterClone for ReadOnlyLocalChunks<T> {
    fn iter_clone(&self, _: Sealed) -> Self {
        ReadOnlyLocalChunks {
            chunk_size: self.chunk_size,
            index: self.index,
            end_index: self.end_index,
            array: self.array.clone(),
        }
    }
}

impl<T: Dist + 'static> LocalIterator for ReadOnlyLocalChunks<T> {
    type Item = &'static [T];
    type Array = ReadOnlyArray<T>;
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
        ReadOnlyLocalChunks {
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
                    self.array.array.local_as_mut_ptr().offset(start_i as isize),
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

impl<T: Dist + 'static> IndexedLocalIterator for ReadOnlyLocalChunks<T> {
    fn iterator_index(&self, index: usize) -> Option<usize> {
        if index * self.chunk_size < self.array.len() {
            Some(index) //everyone at this point as calculated the actual index (cause we are local only) so just return it
        } else {
            None
        }
    }
}

impl<T: Dist> ReadOnlyArray<T> {
    pub fn local_chunks(&self, chunk_size: usize) -> ReadOnlyLocalChunks<T> {
        ReadOnlyLocalChunks {
            chunk_size,
            index: 0,
            end_index: 0,
            array: self.clone(),
        }
    }
}
