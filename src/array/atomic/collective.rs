use crate::{
    array::collective::{
        broadcast_handle::{
            ArrayCollectiveAllToAllHandle, ArrayCollectiveAllToAllIntoBufferHandle,
            ArrayCollectiveBroadcastHandle, ArrayCollectiveBroadcastIntoBufferHandle,
            ArrayCollectiveScatterHandle, ArrayCollectiveScatterIntoBufferHandle,
        },
        gather_handle::{
            ArrayCollectiveAllGatherHandle, ArrayCollectiveAllGatherIntoBufferHandle,
            ArrayCollectiveGatherHandle, ArrayCollectiveGatherIntoBufferHandle,
        },
        reduce_handle::{
            ArrayCollectiveAllReduceHandle, ArrayCollectiveAllReduceInPlaceHandle,
            ArrayCollectiveAllReduceIntoBufferHandle, ArrayCollectiveReduceHandle,
            ArrayCollectiveReduceIntoBufferHandle,
        },
        reduce_scatter_handle::{
            ArrayCollectiveReduceScatterHandle, ArrayCollectiveReduceScatterIntoBufferHandle,
        },
    },
    lamellae::collective::{
        BroadcastInput, RootOrLamellarBuffer, RootSrcOrLamellarBuffer, ScatterInput,
    },
    AsLamellarBuffer, AtomicArray, Dist, ElementArithmeticOps, ElementBitWiseOps,
    ElementComparePartialEqOps, LamellarBuffer,
};

impl<T: ElementArithmeticOps + Default> AtomicArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// All-reduce sum of `len` elements starting at `index`, delivering the result to all PEs.
    ///
    /// Performs a collective all-reduce sum over `len` array elements beginning at `index`.
    /// Every PE participates and receives the final reduced value. The returned
    /// `ArrayCollectiveAllReduceHandle` must be driven to completion by calling `.spawn()` or
    /// `.block()` on it. Atomicity is at the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.sum_all(0, 1) }.block();
    ///```
    pub unsafe fn sum_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => array.sum_all(index, len),
            AtomicArray::NativeAtomicArray(array) => array.sum_all(index, len),
            AtomicArray::GenericAtomicArray(array) => array.sum_all(index, len),
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// All-reduce product of `len` elements starting at `index`, delivering the result to all PEs.
    ///
    /// Performs a collective all-reduce product over `len` array elements beginning at `index`.
    /// Every PE participates and receives the final reduced value. The returned
    /// `ArrayCollectiveAllReduceHandle` must be driven to completion by calling `.spawn()` or
    /// `.block()` on it. Atomicity is at the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.prod_all(0, 1) }.block();
    ///```
    pub unsafe fn prod_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => array.prod_all(index, len),
            AtomicArray::NativeAtomicArray(array) => array.prod_all(index, len),
            AtomicArray::GenericAtomicArray(array) => array.prod_all(index, len),
        }
    }
}

impl<T: ElementComparePartialEqOps + Default> AtomicArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// All-reduce max of `len` elements starting at `index`, delivering the result to all PEs.
    ///
    /// Performs a collective all-reduce maximum over `len` array elements beginning at `index`.
    /// Every PE participates and receives the final reduced value. The returned
    /// `ArrayCollectiveAllReduceHandle` must be driven to completion by calling `.spawn()` or
    /// `.block()` on it. Atomicity is at the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.max_all(0, 1) }.block();
    ///```
    pub unsafe fn max_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => array.max_all(index, len),
            AtomicArray::NativeAtomicArray(array) => array.max_all(index, len),
            AtomicArray::GenericAtomicArray(array) => array.max_all(index, len),
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// All-reduce min of `len` elements starting at `index`, delivering the result to all PEs.
    ///
    /// Performs a collective all-reduce minimum over `len` array elements beginning at `index`.
    /// Every PE participates and receives the final reduced value. The returned
    /// `ArrayCollectiveAllReduceHandle` must be driven to completion by calling `.spawn()` or
    /// `.block()` on it. Atomicity is at the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.min_all(0, 1) }.block();
    ///```
    pub unsafe fn min_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => array.min_all(index, len),
            AtomicArray::NativeAtomicArray(array) => array.min_all(index, len),
            AtomicArray::GenericAtomicArray(array) => array.min_all(index, len),
        }
    }
}

impl<T: ElementBitWiseOps + Default> AtomicArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// All-reduce bitwise AND of `len` elements starting at `index`, delivering the result to all PEs.
    ///
    /// Performs a collective all-reduce bitwise AND over `len` array elements beginning at
    /// `index`. Every PE participates and receives the final reduced value. The returned
    /// `ArrayCollectiveAllReduceHandle` must be driven to completion by calling `.spawn()` or
    /// `.block()` on it. Atomicity is at the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.bit_and_all(0, 1) }.block();
    ///```
    pub unsafe fn bit_and_all(
        &self,
        index: usize,
        len: usize,
    ) -> ArrayCollectiveAllReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => array.bit_and_all(index, len),
            AtomicArray::NativeAtomicArray(array) => array.bit_and_all(index, len),
            AtomicArray::GenericAtomicArray(array) => array.bit_and_all(index, len),
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// All-reduce bitwise XOR of `len` elements starting at `index`, delivering the result to all PEs.
    ///
    /// Performs a collective all-reduce bitwise XOR over `len` array elements beginning at
    /// `index`. Every PE participates and receives the final reduced value. The returned
    /// `ArrayCollectiveAllReduceHandle` must be driven to completion by calling `.spawn()` or
    /// `.block()` on it. Atomicity is at the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.bit_xor_all(0, 1) }.block();
    ///```
    pub unsafe fn bit_xor_all(
        &self,
        index: usize,
        len: usize,
    ) -> ArrayCollectiveAllReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => array.bit_xor_all(index, len),
            AtomicArray::NativeAtomicArray(array) => array.bit_xor_all(index, len),
            AtomicArray::GenericAtomicArray(array) => array.bit_xor_all(index, len),
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// All-reduce bitwise OR of `len` elements starting at `index`, delivering the result to all PEs.
    ///
    /// Performs a collective all-reduce bitwise OR over `len` array elements beginning at
    /// `index`. Every PE participates and receives the final reduced value. The returned
    /// `ArrayCollectiveAllReduceHandle` must be driven to completion by calling `.spawn()` or
    /// `.block()` on it. Atomicity is at the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.bit_or_all(0, 1) }.block();
    ///```
    pub unsafe fn bit_or_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => array.bit_or_all(index, len),
            AtomicArray::NativeAtomicArray(array) => array.bit_or_all(index, len),
            AtomicArray::GenericAtomicArray(array) => array.bit_or_all(index, len),
        }
    }
}

impl<T: ElementArithmeticOps + Default> AtomicArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// Like `sum_all` but places the result into caller-supplied `buffer`. Returns an `ArrayCollectiveAllReduceIntoBufferHandle`.
    ///
    /// Performs a collective all-reduce sum over `len` array elements beginning at `index` and
    /// writes the result into `buffer` rather than allocating a new buffer. Every PE participates
    /// and receives the result. The returned `ArrayCollectiveAllReduceIntoBufferHandle` must be
    /// driven to completion by calling `.spawn()` or `.block()` on it. Atomicity is at the
    /// element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// unsafe { array.sum_all_into_buffer(0, 1, buf) }.block();
    ///```
    pub unsafe fn sum_all_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => array.sum_all_into_buffer(index, len, buffer),
            AtomicArray::NativeAtomicArray(array) => array.sum_all_into_buffer(index, len, buffer),
            AtomicArray::GenericAtomicArray(array) => array.sum_all_into_buffer(index, len, buffer),
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Like `prod_all` but places the result into caller-supplied `buffer`. Returns an `ArrayCollectiveAllReduceIntoBufferHandle`.
    ///
    /// Performs a collective all-reduce product over `len` array elements beginning at `index` and
    /// writes the result into `buffer` rather than allocating a new buffer. Every PE participates
    /// and receives the result. The returned `ArrayCollectiveAllReduceIntoBufferHandle` must be
    /// driven to completion by calling `.spawn()` or `.block()` on it. Atomicity is at the
    /// element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// unsafe { array.prod_all_into_buffer(0, 1, buf) }.block();
    ///```
    pub unsafe fn prod_all_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array.prod_all_into_buffer(index, len, buffer)
            }
            AtomicArray::NativeAtomicArray(array) => array.prod_all_into_buffer(index, len, buffer),
            AtomicArray::GenericAtomicArray(array) => {
                array.prod_all_into_buffer(index, len, buffer)
            }
        }
    }
}

impl<T: ElementComparePartialEqOps + Default> AtomicArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// Like `max_all` but places the result into caller-supplied `buffer`. Returns an `ArrayCollectiveAllReduceIntoBufferHandle`.
    ///
    /// Performs a collective all-reduce maximum over `len` array elements beginning at `index` and
    /// writes the result into `buffer` rather than allocating a new buffer. Every PE participates
    /// and receives the result. The returned `ArrayCollectiveAllReduceIntoBufferHandle` must be
    /// driven to completion by calling `.spawn()` or `.block()` on it. Atomicity is at the
    /// element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// unsafe { array.max_all_into_buffer(0, 1, buf) }.block();
    ///```
    pub unsafe fn max_all_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => array.max_all_into_buffer(index, len, buffer),
            AtomicArray::NativeAtomicArray(array) => array.max_all_into_buffer(index, len, buffer),
            AtomicArray::GenericAtomicArray(array) => array.max_all_into_buffer(index, len, buffer),
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Like `min_all` but places the result into caller-supplied `buffer`. Returns an `ArrayCollectiveAllReduceIntoBufferHandle`.
    ///
    /// Performs a collective all-reduce minimum over `len` array elements beginning at `index` and
    /// writes the result into `buffer` rather than allocating a new buffer. Every PE participates
    /// and receives the result. The returned `ArrayCollectiveAllReduceIntoBufferHandle` must be
    /// driven to completion by calling `.spawn()` or `.block()` on it. Atomicity is at the
    /// element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// unsafe { array.min_all_into_buffer(0, 1, buf) }.block();
    ///```
    pub unsafe fn min_all_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => array.min_all_into_buffer(index, len, buffer),
            AtomicArray::NativeAtomicArray(array) => array.min_all_into_buffer(index, len, buffer),
            AtomicArray::GenericAtomicArray(array) => array.min_all_into_buffer(index, len, buffer),
        }
    }
}

impl<T: ElementBitWiseOps + Default> AtomicArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// Like `bit_and_all` but places the result into caller-supplied `buffer`. Returns an `ArrayCollectiveAllReduceIntoBufferHandle`.
    ///
    /// Performs a collective all-reduce bitwise AND over `len` array elements beginning at
    /// `index` and writes the result into `buffer` rather than allocating a new buffer. Every PE
    /// participates and receives the result. The returned `ArrayCollectiveAllReduceIntoBufferHandle`
    /// must be driven to completion by calling `.spawn()` or `.block()` on it. Atomicity is at
    /// the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// unsafe { array.bit_and_all_into_buffer(0, 1, buf) }.block();
    ///```
    pub unsafe fn bit_and_all_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array.bit_and_all_into_buffer(index, len, buffer)
            }
            AtomicArray::NativeAtomicArray(array) => {
                array.bit_and_all_into_buffer(index, len, buffer)
            }
            AtomicArray::GenericAtomicArray(array) => {
                array.bit_and_all_into_buffer(index, len, buffer)
            }
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Like `bit_xor_all` but places the result into caller-supplied `buffer`. Returns an `ArrayCollectiveAllReduceIntoBufferHandle`.
    ///
    /// Performs a collective all-reduce bitwise XOR over `len` array elements beginning at
    /// `index` and writes the result into `buffer` rather than allocating a new buffer. Every PE
    /// participates and receives the result. The returned `ArrayCollectiveAllReduceIntoBufferHandle`
    /// must be driven to completion by calling `.spawn()` or `.block()` on it. Atomicity is at
    /// the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// unsafe { array.bit_xor_all_into_buffer(0, 1, buf) }.block();
    ///```
    pub unsafe fn bit_xor_all_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array.bit_xor_all_into_buffer(index, len, buffer)
            }
            AtomicArray::NativeAtomicArray(array) => {
                array.bit_xor_all_into_buffer(index, len, buffer)
            }
            AtomicArray::GenericAtomicArray(array) => {
                array.bit_xor_all_into_buffer(index, len, buffer)
            }
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Like `bit_or_all` but places the result into caller-supplied `buffer`. Returns an `ArrayCollectiveAllReduceIntoBufferHandle`.
    ///
    /// Performs a collective all-reduce bitwise OR over `len` array elements beginning at
    /// `index` and writes the result into `buffer` rather than allocating a new buffer. Every PE
    /// participates and receives the result. The returned `ArrayCollectiveAllReduceIntoBufferHandle`
    /// must be driven to completion by calling `.spawn()` or `.block()` on it. Atomicity is at
    /// the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// unsafe { array.bit_or_all_into_buffer(0, 1, buf) }.block();
    ///```
    pub unsafe fn bit_or_all_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array.bit_or_all_into_buffer(index, len, buffer)
            }
            AtomicArray::NativeAtomicArray(array) => {
                array.bit_or_all_into_buffer(index, len, buffer)
            }
            AtomicArray::GenericAtomicArray(array) => {
                array.bit_or_all_into_buffer(index, len, buffer)
            }
        }
    }
}

impl<T: ElementArithmeticOps + Default> AtomicArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// All-reduce sum in place, using `src_and_dst` as both input and output. Currently only supported on `NetworkAtomicArray`; panics on other array variants.
    ///
    /// Performs a collective all-reduce sum using `src_and_dst` as both the source data and the
    /// destination for the result. Every PE participates and the reduced value is written back
    /// into `src_and_dst` on each PE. The returned `ArrayCollectiveAllReduceInPlaceHandle` must
    /// be driven to completion by calling `.spawn()` or `.block()` on it. Atomicity is at the
    /// element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// unsafe { array.sum_all_in_place(buf) }.block();
    ///```
    pub unsafe fn sum_all_in_place<B: AsLamellarBuffer<T>>(
        &self,
        src_and_dst: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => array.sum_all_in_place(src_and_dst),
            _ => {
                todo!("collective reduce operations currently only supported on network atomic arrays")
            } // AtomicArray::NativeAtomicArray(array) => {
              //     array
              //         .sum_all_in_place(src_and_dst)
              // },
              // AtomicArray::GenericAtomicArray(array) => {
              //     array
              //         .sum_all_in_place(src_and_dst)
              // },
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// All-reduce max in place, using `src_and_dst` as both input and output. Currently only supported on `NetworkAtomicArray`; panics on other array variants.
    ///
    /// Performs a collective all-reduce maximum using `src_and_dst` as both the source data and
    /// the destination for the result. Every PE participates and the reduced value is written back
    /// into `src_and_dst` on each PE. The returned `ArrayCollectiveAllReduceInPlaceHandle` must
    /// be driven to completion by calling `.spawn()` or `.block()` on it. Atomicity is at the
    /// element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// unsafe { array.max_all_in_place(buf) }.block();
    ///```
    pub unsafe fn max_all_in_place<B: AsLamellarBuffer<T>>(
        &self,
        src_and_dst: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => array.max_all_in_place(src_and_dst),
            _ => {
                todo!("collective reduce operations currently only supported on network atomic arrays")
            } // AtomicArray::NativeAtomicArray(array) => {
              //     array
              //         .max_all_in_place(src_and_dst)
              // },
              // AtomicArray::GenericAtomicArray(array) => {
              //     array
              //         .max_all_in_place(src_and_dst)
              // },
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// All-reduce min in place, using `src_and_dst` as both input and output. Currently only supported on `NetworkAtomicArray`; panics on other array variants.
    ///
    /// Performs a collective all-reduce minimum using `src_and_dst` as both the source data and
    /// the destination for the result. Every PE participates and the reduced value is written back
    /// into `src_and_dst` on each PE. The returned `ArrayCollectiveAllReduceInPlaceHandle` must
    /// be driven to completion by calling `.spawn()` or `.block()` on it. Atomicity is at the
    /// element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// unsafe { array.min_all_in_place(buf) }.block();
    ///```
    pub unsafe fn min_all_in_place<B: AsLamellarBuffer<T>>(
        &self,
        src_and_dst: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => array.min_all_in_place(src_and_dst),
            _ => {
                todo!("collective reduce operations currently only supported on network atomic arrays")
            } // AtomicArray::NativeAtomicArray(array) => {
              //     array
              //         .min_all_in_place(src_and_dst)
              // },
              // AtomicArray::GenericAtomicArray(array) => {
              //     array
              //         .min_all_in_place(src_and_dst)
              // },
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// All-reduce product in place, using `src_and_dst` as both input and output. Currently only supported on `NetworkAtomicArray`; panics on other array variants.
    ///
    /// Performs a collective all-reduce product using `src_and_dst` as both the source data and
    /// the destination for the result. Every PE participates and the reduced value is written back
    /// into `src_and_dst` on each PE. The returned `ArrayCollectiveAllReduceInPlaceHandle` must
    /// be driven to completion by calling `.spawn()` or `.block()` on it. Atomicity is at the
    /// element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// unsafe { array.prod_all_in_place(buf) }.block();
    ///```
    pub unsafe fn prod_all_in_place<B: AsLamellarBuffer<T>>(
        &self,
        src_and_dst: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => array.prod_all_in_place(src_and_dst),
            _ => {
                todo!("collective reduce operations currently only supported on network atomic arrays")
            } // AtomicArray::NativeAtomicArray(array) => {
              //     array
              //         .prod_all_in_place(src_and_dst)
              // },
              // AtomicArray::GenericAtomicArray(array) => {
              //     array
              //         .prod_all_in_place(src_and_dst)
              // },
        }
    }
}

impl<T: ElementBitWiseOps + Default> AtomicArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// All-reduce bitwise AND in place, using `src_and_dst` as both input and output. Currently only supported on `NetworkAtomicArray`; panics on other array variants.
    ///
    /// Performs a collective all-reduce bitwise AND using `src_and_dst` as both the source data
    /// and the destination for the result. Every PE participates and the reduced value is written
    /// back into `src_and_dst` on each PE. The returned `ArrayCollectiveAllReduceInPlaceHandle`
    /// must be driven to completion by calling `.spawn()` or `.block()` on it. Atomicity is at
    /// the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// unsafe { array.bit_and_all_in_place(buf) }.block();
    ///```
    pub unsafe fn bit_and_all_in_place<B: AsLamellarBuffer<T>>(
        &self,
        src_and_dst: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => array.bit_and_all_in_place(src_and_dst),
            _ => {
                todo!("collective reduce operations currently only supported on network atomic arrays")
            }
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// All-reduce bitwise XOR in place, using `src_and_dst` as both input and output. Currently only supported on `NetworkAtomicArray`; panics on other array variants.
    ///
    /// Performs a collective all-reduce bitwise XOR using `src_and_dst` as both the source data
    /// and the destination for the result. Every PE participates and the reduced value is written
    /// back into `src_and_dst` on each PE. The returned `ArrayCollectiveAllReduceInPlaceHandle`
    /// must be driven to completion by calling `.spawn()` or `.block()` on it. Atomicity is at
    /// the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// unsafe { array.bit_xor_all_in_place(buf) }.block();
    ///```
    pub unsafe fn bit_xor_all_in_place<B: AsLamellarBuffer<T>>(
        &self,
        src_and_dst: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => array.bit_xor_all_in_place(src_and_dst),
            _ => {
                todo!("collective reduce operations currently only supported on network atomic arrays")
            }
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// All-reduce bitwise OR in place, using `src_and_dst` as both input and output. Currently only supported on `NetworkAtomicArray`; panics on other array variants.
    ///
    /// Performs a collective all-reduce bitwise OR using `src_and_dst` as both the source data
    /// and the destination for the result. Every PE participates and the reduced value is written
    /// back into `src_and_dst` on each PE. The returned `ArrayCollectiveAllReduceInPlaceHandle`
    /// must be driven to completion by calling `.spawn()` or `.block()` on it. Atomicity is at
    /// the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// unsafe { array.bit_or_all_in_place(buf) }.block();
    ///```
    pub unsafe fn bit_or_all_in_place<B: AsLamellarBuffer<T>>(
        &self,
        src_and_dst: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => array.bit_or_all_in_place(src_and_dst),
            _ => {
                todo!("collective reduce operations currently only supported on network atomic arrays")
            }
        }
    }
}

impl<T: ElementArithmeticOps + Default> AtomicArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// Reduce sum of `len` elements starting at `index`, delivering the result only to PE `pe`.
    ///
    /// Performs a collective reduce sum over `len` array elements beginning at `index`. Every PE
    /// participates but only the designated PE `pe` receives the final value. The returned
    /// `ArrayCollectiveReduceHandle` must be driven to completion by calling `.spawn()` or
    /// `.block()` on it. Atomicity is at the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.sum_at_pe(0, 1, 0) }.block();
    ///```
    pub unsafe fn sum_at_pe(
        &self,
        index: usize,
        len: usize,
        pe: usize,
    ) -> ArrayCollectiveReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => array.sum_at_pe(index, len, pe),
            AtomicArray::NativeAtomicArray(array) => array.sum_at_pe(index, len, pe),
            AtomicArray::GenericAtomicArray(array) => array.sum_at_pe(index, len, pe),
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Reduce product of `len` elements starting at `index`, delivering the result only to PE `pe`.
    ///
    /// Performs a collective reduce product over `len` array elements beginning at `index`. Every
    /// PE participates but only the designated PE `pe` receives the final value. The returned
    /// `ArrayCollectiveReduceHandle` must be driven to completion by calling `.spawn()` or
    /// `.block()` on it. Atomicity is at the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.prod_at_pe(0, 1, 0) }.block();
    ///```
    pub unsafe fn prod_at_pe(
        &self,
        index: usize,
        len: usize,
        pe: usize,
    ) -> ArrayCollectiveReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => array.prod_at_pe(index, len, pe),
            AtomicArray::NativeAtomicArray(array) => array.prod_at_pe(index, len, pe),
            AtomicArray::GenericAtomicArray(array) => array.prod_at_pe(index, len, pe),
        }
    }
}

impl<T: ElementComparePartialEqOps + Default> AtomicArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// Reduce max of `len` elements starting at `index`, delivering the result only to PE `pe`.
    ///
    /// Performs a collective reduce maximum over `len` array elements beginning at `index`. Every
    /// PE participates but only the designated PE `pe` receives the final value. The returned
    /// `ArrayCollectiveReduceHandle` must be driven to completion by calling `.spawn()` or
    /// `.block()` on it. Atomicity is at the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.max_at_pe(0, 1, 0) }.block();
    ///```
    pub unsafe fn max_at_pe(
        &self,
        index: usize,
        len: usize,
        pe: usize,
    ) -> ArrayCollectiveReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => array.max_at_pe(index, len, pe),
            AtomicArray::NativeAtomicArray(array) => array.max_at_pe(index, len, pe),
            AtomicArray::GenericAtomicArray(array) => array.max_at_pe(index, len, pe),
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Reduce min of `len` elements starting at `index`, delivering the result only to PE `pe`.
    ///
    /// Performs a collective reduce minimum over `len` array elements beginning at `index`. Every
    /// PE participates but only the designated PE `pe` receives the final value. The returned
    /// `ArrayCollectiveReduceHandle` must be driven to completion by calling `.spawn()` or
    /// `.block()` on it. Atomicity is at the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.min_at_pe(0, 1, 0) }.block();
    ///```
    pub unsafe fn min_at_pe(
        &self,
        index: usize,
        len: usize,
        pe: usize,
    ) -> ArrayCollectiveReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => array.min_at_pe(index, len, pe),
            AtomicArray::NativeAtomicArray(array) => array.min_at_pe(index, len, pe),
            AtomicArray::GenericAtomicArray(array) => array.min_at_pe(index, len, pe),
        }
    }
}

impl<T: ElementBitWiseOps + Default> AtomicArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// Reduce bitwise AND of `len` elements starting at `index`, delivering the result only to PE `pe`.
    ///
    /// Performs a collective reduce bitwise AND over `len` array elements beginning at `index`.
    /// Every PE participates but only the designated PE `pe` receives the final value. The
    /// returned `ArrayCollectiveReduceHandle` must be driven to completion by calling `.spawn()`
    /// or `.block()` on it. Atomicity is at the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.bit_and_at_pe(0, 1, 0) }.block();
    ///```
    pub unsafe fn bit_and_at_pe(
        &self,
        index: usize,
        len: usize,
        pe: usize,
    ) -> ArrayCollectiveReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => array.bit_and_at_pe(index, len, pe),
            AtomicArray::NativeAtomicArray(array) => array.bit_and_at_pe(index, len, pe),
            AtomicArray::GenericAtomicArray(array) => array.bit_and_at_pe(index, len, pe),
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Reduce bitwise XOR of `len` elements starting at `index`, delivering the result only to PE `pe`.
    ///
    /// Performs a collective reduce bitwise XOR over `len` array elements beginning at `index`.
    /// Every PE participates but only the designated PE `pe` receives the final value. The
    /// returned `ArrayCollectiveReduceHandle` must be driven to completion by calling `.spawn()`
    /// or `.block()` on it. Atomicity is at the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.bit_xor_at_pe(0, 1, 0) }.block();
    ///```
    pub unsafe fn bit_xor_at_pe(
        &self,
        index: usize,
        len: usize,
        pe: usize,
    ) -> ArrayCollectiveReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => array.bit_xor_at_pe(index, len, pe),
            AtomicArray::NativeAtomicArray(array) => array.bit_xor_at_pe(index, len, pe),
            AtomicArray::GenericAtomicArray(array) => array.bit_xor_at_pe(index, len, pe),
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Reduce bitwise OR of `len` elements starting at `index`, delivering the result only to PE `pe`.
    ///
    /// Performs a collective reduce bitwise OR over `len` array elements beginning at `index`.
    /// Every PE participates but only the designated PE `pe` receives the final value. The
    /// returned `ArrayCollectiveReduceHandle` must be driven to completion by calling `.spawn()`
    /// or `.block()` on it. Atomicity is at the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.bit_or_at_pe(0, 1, 0) }.block();
    ///```
    pub unsafe fn bit_or_at_pe(
        &self,
        index: usize,
        len: usize,
        pe: usize,
    ) -> ArrayCollectiveReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => array.bit_or_at_pe(index, len, pe),
            AtomicArray::NativeAtomicArray(array) => array.bit_or_at_pe(index, len, pe),
            AtomicArray::GenericAtomicArray(array) => array.bit_or_at_pe(index, len, pe),
        }
    }
}

impl<T: ElementArithmeticOps + Default> AtomicArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// Like `sum_at_pe` but places the result into caller-supplied `target: RootOrLamellarBuffer`.
    ///
    /// Performs a collective reduce sum over `len` array elements beginning at `index` and writes
    /// the result into `target` on the root PE rather than allocating a new buffer. Every PE
    /// participates but only the designated root PE receives the result. The returned
    /// `ArrayCollectiveReduceIntoBufferHandle` must be driven to completion by calling `.spawn()`
    /// or `.block()` on it. Atomicity is at the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// unsafe { array.sum_at_pe_into_buffer(0, 1, RootOrLamellarBuffer::Root(0)) }.block();
    ///```
    pub unsafe fn sum_at_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        target: RootOrLamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array.sum_at_pe_into_buffer(index, len, target)
            }
            AtomicArray::NativeAtomicArray(array) => {
                array.sum_at_pe_into_buffer(index, len, target)
            }
            AtomicArray::GenericAtomicArray(array) => {
                array.sum_at_pe_into_buffer(index, len, target)
            }
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Like `prod_at_pe` but places the result into caller-supplied `target: RootOrLamellarBuffer`.
    ///
    /// Performs a collective reduce product over `len` array elements beginning at `index` and
    /// writes the result into `target` on the root PE rather than allocating a new buffer. Every
    /// PE participates but only the designated root PE receives the result. The returned
    /// `ArrayCollectiveReduceIntoBufferHandle` must be driven to completion by calling `.spawn()`
    /// or `.block()` on it. Atomicity is at the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// unsafe { array.prod_at_pe_into_buffer(0, 1, RootOrLamellarBuffer::Root(0)) }.block();
    ///```
    pub unsafe fn prod_at_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        target: RootOrLamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array.prod_at_pe_into_buffer(index, len, target)
            }
            AtomicArray::NativeAtomicArray(array) => {
                array.prod_at_pe_into_buffer(index, len, target)
            }
            AtomicArray::GenericAtomicArray(array) => {
                array.prod_at_pe_into_buffer(index, len, target)
            }
        }
    }
}

impl<T: ElementComparePartialEqOps + Default> AtomicArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// Like `max_at_pe` but places the result into caller-supplied `target: RootOrLamellarBuffer`.
    ///
    /// Performs a collective reduce maximum over `len` array elements beginning at `index` and
    /// writes the result into `target` on the root PE rather than allocating a new buffer. Every
    /// PE participates but only the designated root PE receives the result. The returned
    /// `ArrayCollectiveReduceIntoBufferHandle` must be driven to completion by calling `.spawn()`
    /// or `.block()` on it. Atomicity is at the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// unsafe { array.max_at_pe_into_buffer(0, 1, RootOrLamellarBuffer::Root(0)) }.block();
    ///```
    pub unsafe fn max_at_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        target: RootOrLamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array.max_at_pe_into_buffer(index, len, target)
            }
            AtomicArray::NativeAtomicArray(array) => {
                array.max_at_pe_into_buffer(index, len, target)
            }
            AtomicArray::GenericAtomicArray(array) => {
                array.max_at_pe_into_buffer(index, len, target)
            }
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Like `min_at_pe` but places the result into caller-supplied `target: RootOrLamellarBuffer`.
    ///
    /// Performs a collective reduce minimum over `len` array elements beginning at `index` and
    /// writes the result into `target` on the root PE rather than allocating a new buffer. Every
    /// PE participates but only the designated root PE receives the result. The returned
    /// `ArrayCollectiveReduceIntoBufferHandle` must be driven to completion by calling `.spawn()`
    /// or `.block()` on it. Atomicity is at the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// unsafe { array.min_at_pe_into_buffer(0, 1, RootOrLamellarBuffer::Root(0)) }.block();
    ///```
    pub unsafe fn min_at_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        target: RootOrLamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array.min_at_pe_into_buffer(index, len, target)
            }
            AtomicArray::NativeAtomicArray(array) => {
                array.min_at_pe_into_buffer(index, len, target)
            }
            AtomicArray::GenericAtomicArray(array) => {
                array.min_at_pe_into_buffer(index, len, target)
            }
        }
    }
}

impl<T: ElementBitWiseOps + Default> AtomicArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// Like `bit_and_at_pe` but places the result into caller-supplied `target: RootOrLamellarBuffer`.
    ///
    /// Performs a collective reduce bitwise AND over `len` array elements beginning at `index`
    /// and writes the result into `target` on the root PE rather than allocating a new buffer.
    /// Every PE participates but only the designated root PE receives the result. The returned
    /// `ArrayCollectiveReduceIntoBufferHandle` must be driven to completion by calling `.spawn()`
    /// or `.block()` on it. Atomicity is at the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// unsafe { array.bit_and_at_pe_into_buffer(0, 1, RootOrLamellarBuffer::Root(0)) }.block();
    ///```
    pub unsafe fn bit_and_at_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        target: RootOrLamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array.bit_and_at_pe_into_buffer(index, len, target)
            }
            AtomicArray::NativeAtomicArray(array) => {
                array.bit_and_at_pe_into_buffer(index, len, target)
            }
            AtomicArray::GenericAtomicArray(array) => {
                array.bit_and_at_pe_into_buffer(index, len, target)
            }
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Like `bit_xor_at_pe` but places the result into caller-supplied `target: RootOrLamellarBuffer`.
    ///
    /// Performs a collective reduce bitwise XOR over `len` array elements beginning at `index`
    /// and writes the result into `target` on the root PE rather than allocating a new buffer.
    /// Every PE participates but only the designated root PE receives the result. The returned
    /// `ArrayCollectiveReduceIntoBufferHandle` must be driven to completion by calling `.spawn()`
    /// or `.block()` on it. Atomicity is at the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// unsafe { array.bit_xor_at_pe_into_buffer(0, 1, RootOrLamellarBuffer::Root(0)) }.block();
    ///```
    pub unsafe fn bit_xor_at_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        target: RootOrLamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array.bit_xor_at_pe_into_buffer(index, len, target)
            }
            AtomicArray::NativeAtomicArray(array) => {
                array.bit_xor_at_pe_into_buffer(index, len, target)
            }
            AtomicArray::GenericAtomicArray(array) => {
                array.bit_xor_at_pe_into_buffer(index, len, target)
            }
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Like `bit_or_at_pe` but places the result into caller-supplied `target: RootOrLamellarBuffer`.
    ///
    /// Performs a collective reduce bitwise OR over `len` array elements beginning at `index`
    /// and writes the result into `target` on the root PE rather than allocating a new buffer.
    /// Every PE participates but only the designated root PE receives the result. The returned
    /// `ArrayCollectiveReduceIntoBufferHandle` must be driven to completion by calling `.spawn()`
    /// or `.block()` on it. Atomicity is at the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// unsafe { array.bit_or_at_pe_into_buffer(0, 1, RootOrLamellarBuffer::Root(0)) }.block();
    ///```
    pub unsafe fn bit_or_at_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        target: RootOrLamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array.bit_or_at_pe_into_buffer(index, len, target)
            }
            AtomicArray::NativeAtomicArray(array) => {
                array.bit_or_at_pe_into_buffer(index, len, target)
            }
            AtomicArray::GenericAtomicArray(array) => {
                array.bit_or_at_pe_into_buffer(index, len, target)
            }
        }
    }
}

// impl<T: Dist + Default> AtomicArray<T> {
//     pub unsafe fn sum_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         match self {
//             AtomicArray::NetworkAtomicArray(array) => {
//                 array
//                     .sum_at_pe_in_place(pe)
//             },
//             _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
//         }
//     }

//     pub unsafe fn max_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         match self {
//             AtomicArray::NetworkAtomicArray(array) => {
//                 array
//                     .max_at_pe_in_place(pe)
//             },
//             _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
//         }
//     }

//     pub unsafe fn min_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         match self {
//             AtomicArray::NetworkAtomicArray(array) => {
//                 array
//                     .min_at_pe_in_place(pe)
//             },
//             _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
//         }
//     }

//     pub unsafe fn prod_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         match self {
//             AtomicArray::NetworkAtomicArray(array) => {
//                 array
//                     .prod_at_pe_in_place(pe)
//             },
//             _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
//         }
//     }

//     pub unsafe fn bit_and_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         match self {
//             AtomicArray::NetworkAtomicArray(array) => {
//                 array
//                     .bit_and_at_pe_in_place(pe)
//             },
//             _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
//         }
//     }

//     pub unsafe fn bit_xor_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         match self {
//             AtomicArray::NetworkAtomicArray(array) => {
//                 array
//                     .bit_xor_at_pe_in_place(pe)
//             },
//             _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
//         }
//     }

//     pub unsafe fn bit_or_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         match self {
//             AtomicArray::NetworkAtomicArray(array) => {
//                 array
//                     .bit_or_at_pe_in_place(pe)
//             },
//             _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
//          }
//     }
// }

impl<T: Dist + Default> AtomicArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// Gathers `len` elements starting at `index` from every PE, delivering the concatenated result to all PEs.
    ///
    /// Each PE contributes `len` elements beginning at `index` from its local portion of the
    /// array. The contributions are concatenated in PE order and the full result is delivered to
    /// every PE. The returned `ArrayCollectiveAllGatherHandle` must be driven to completion by
    /// calling `.spawn()` or `.block()` on it. Atomicity is at the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.gather_all(0, 1) }.block();
    ///```
    pub unsafe fn gather_all(&self, index: usize, len: usize) -> ArrayCollectiveAllGatherHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => array.gather_all(index, len),
            AtomicArray::NativeAtomicArray(array) => array.gather_all(index, len),
            AtomicArray::GenericAtomicArray(array) => array.gather_all(index, len),
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Like `gather_all` but places the result into caller-supplied `buffer`.
    ///
    /// Each PE contributes `len` elements beginning at `index` from its local portion of the
    /// array. The contributions are concatenated in PE order and the full result is written into
    /// `buffer` on every PE rather than allocating a new buffer. The returned
    /// `ArrayCollectiveAllGatherIntoBufferHandle` must be driven to completion by calling
    /// `.spawn()` or `.block()` on it. Atomicity is at the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// unsafe { array.gather_all_into_buffer(0, 1, buf) }.block();
    ///```
    pub unsafe fn gather_all_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllGatherIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array.gather_all_into_buffer(index, len, buffer)
            }
            AtomicArray::NativeAtomicArray(array) => {
                array.gather_all_into_buffer(index, len, buffer)
            }
            AtomicArray::GenericAtomicArray(array) => {
                array.gather_all_into_buffer(index, len, buffer)
            }
        }
    }
}

impl<T: Dist + Default> AtomicArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// Gathers `len` elements starting at `index` from every PE, delivering the concatenated result only to PE `pe`.
    ///
    /// Each PE contributes `len` elements beginning at `index` from its local portion of the
    /// array. The contributions are concatenated in PE order and the result is delivered only to
    /// the designated PE `pe`. The returned `ArrayCollectiveGatherHandle` must be driven to
    /// completion by calling `.spawn()` or `.block()` on it. Atomicity is at the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.gather_at_pe(0, 1, 0) }.block();
    ///```
    pub unsafe fn gather_at_pe(
        &self,
        index: usize,
        len: usize,
        pe: usize,
    ) -> ArrayCollectiveGatherHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => array.gather_at_pe(index, len, pe),
            AtomicArray::NativeAtomicArray(array) => array.gather_at_pe(index, len, pe),
            AtomicArray::GenericAtomicArray(array) => array.gather_at_pe(index, len, pe),
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Like `gather_at_pe` but places the result into caller-supplied `target: RootOrLamellarBuffer`.
    ///
    /// Each PE contributes `len` elements beginning at `index` from its local portion of the
    /// array. The contributions are concatenated in PE order and the result is written into
    /// `target` on the root PE rather than allocating a new buffer. Every PE participates but
    /// only the root PE receives the data. The returned `ArrayCollectiveGatherIntoBufferHandle`
    /// must be driven to completion by calling `.spawn()` or `.block()` on it. Atomicity is at
    /// the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// unsafe { array.gather_at_pe_into_buffer(0, 1, RootOrLamellarBuffer::Root(0)) }.block();
    ///```
    pub unsafe fn gather_at_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        target: RootOrLamellarBuffer<T, B>,
    ) -> ArrayCollectiveGatherIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array.gather_at_pe_into_buffer(index, len, target)
            }
            AtomicArray::NativeAtomicArray(array) => {
                array.gather_at_pe_into_buffer(index, len, target)
            }
            AtomicArray::GenericAtomicArray(array) => {
                array.gather_at_pe_into_buffer(index, len, target)
            }
        }
    }
}

impl<T: Dist + Default> AtomicArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// All-to-all exchange: each PE sends `len` elements starting at `index` to every other PE, each receiving one segment per PE.
    ///
    /// Every PE sends `len` elements beginning at `index` to each of the other PEs and receives
    /// one corresponding segment from each PE in return. The returned
    /// `ArrayCollectiveAllToAllHandle` must be driven to completion by calling `.spawn()` or
    /// `.block()` on it. Atomicity is at the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.alltoall(0, 1) }.block();
    ///```
    pub unsafe fn alltoall(&self, index: usize, len: usize) -> ArrayCollectiveAllToAllHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => array.alltoall(index, len),
            AtomicArray::NativeAtomicArray(array) => array.alltoall(index, len),
            AtomicArray::GenericAtomicArray(array) => array.alltoall(index, len),
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Like `alltoall` but places received data into caller-supplied `buffer`.
    ///
    /// Every PE sends `len` elements beginning at `index` to each of the other PEs and receives
    /// one corresponding segment from each PE into `buffer` rather than allocating a new buffer.
    /// The returned `ArrayCollectiveAllToAllIntoBufferHandle` must be driven to completion by
    /// calling `.spawn()` or `.block()` on it. Atomicity is at the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// unsafe { array.alltoall_into_buffer(0, 1, buf) }.block();
    ///```
    pub unsafe fn alltoall_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllToAllIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array.alltoall_into_buffer(index, len, buffer)
            }
            AtomicArray::NativeAtomicArray(array) => array.alltoall_into_buffer(index, len, buffer),
            AtomicArray::GenericAtomicArray(array) => {
                array.alltoall_into_buffer(index, len, buffer)
            }
        }
    }
}

impl<T: Dist + Default> AtomicArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// Broadcasts `len` elements from the root PE specified by `src_or_root_pe: BroadcastInput` to all PEs.
    ///
    /// The root PE (identified by `src_or_root_pe`) sends `len` elements to all other PEs. Every
    /// PE participates and receives the broadcast data. The returned
    /// `ArrayCollectiveBroadcastHandle` must be driven to completion by calling `.spawn()` or
    /// `.block()` on it. Atomicity is at the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// unsafe { array.broadcast_from_pe(BroadcastInput::Root(0), 1) }.block();
    ///```
    pub unsafe fn broadcast_from_pe(
        &self,
        src_or_root_pe: BroadcastInput,
        len: usize,
    ) -> ArrayCollectiveBroadcastHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => array.broadcast_from_pe(src_or_root_pe, len),
            AtomicArray::NativeAtomicArray(array) => array.broadcast_from_pe(src_or_root_pe, len),
            AtomicArray::GenericAtomicArray(array) => array.broadcast_from_pe(src_or_root_pe, len),
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Like `broadcast_from_pe` but places received data into caller-supplied `target: RootSrcOrLamellarBuffer`.
    ///
    /// The root PE (identified within `target`) sends `len` elements to all other PEs, which
    /// receive the data into `target` rather than allocating a new buffer. The returned
    /// `ArrayCollectiveBroadcastIntoBufferHandle` must be driven to completion by calling
    /// `.spawn()` or `.block()` on it. Atomicity is at the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// unsafe { array.broadcast_from_pe_into_buffer(RootSrcOrLamellarBuffer::Root(0, buf), 1) }.block();
    ///```
    pub unsafe fn broadcast_from_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        target: RootSrcOrLamellarBuffer<T, B>,
        len: usize,
    ) -> ArrayCollectiveBroadcastIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array.broadcast_from_pe_into_buffer(target, len)
            }
            AtomicArray::NativeAtomicArray(array) => {
                array.broadcast_from_pe_into_buffer(target, len)
            }
            AtomicArray::GenericAtomicArray(array) => {
                array.broadcast_from_pe_into_buffer(target, len)
            }
        }
    }
}

impl<T: Dist + Default> AtomicArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// Scatters segments of data from the root PE specified by `src_or_root_pe: ScatterInput` to each PE, with `len` elements per PE.
    ///
    /// The root PE (identified by `src_or_root_pe`) divides its data into segments of `len`
    /// elements and sends one segment to each PE. Every PE participates and receives its
    /// designated segment. The returned `ArrayCollectiveScatterHandle` must be driven to
    /// completion by calling `.spawn()` or `.block()` on it. Atomicity is at the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// unsafe { array.scatter_from_pe(ScatterInput::Root(0), 1) }.block();
    ///```
    pub unsafe fn scatter_from_pe(
        &self,
        src_or_root_pe: ScatterInput,
        len: usize,
    ) -> ArrayCollectiveScatterHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => array.scatter_from_pe(src_or_root_pe, len),
            AtomicArray::NativeAtomicArray(array) => array.scatter_from_pe(src_or_root_pe, len),
            AtomicArray::GenericAtomicArray(array) => array.scatter_from_pe(src_or_root_pe, len),
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Like `scatter_from_pe` but places received segment into caller-supplied `buf`.
    ///
    /// The root PE (identified by `src_or_root_pe`) divides its data into segments of `len`
    /// elements and sends one segment to each PE, which receives the data into `buf` rather than
    /// allocating a new buffer. The returned `ArrayCollectiveScatterIntoBufferHandle` must be
    /// driven to completion by calling `.spawn()` or `.block()` on it. Atomicity is at the
    /// element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// unsafe { array.scatter_from_pe_into_buffer(buf, ScatterInput::Root(0), 1) }.block();
    ///```
    pub unsafe fn scatter_from_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        buf: LamellarBuffer<T, B>,
        src_or_root_pe: ScatterInput,
        len: usize,
    ) -> ArrayCollectiveScatterIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array.scatter_from_pe_into_buffer(buf, src_or_root_pe, len)
            }
            AtomicArray::NativeAtomicArray(array) => {
                array.scatter_from_pe_into_buffer(buf, src_or_root_pe, len)
            }
            AtomicArray::GenericAtomicArray(array) => {
                array.scatter_from_pe_into_buffer(buf, src_or_root_pe, len)
            }
        }
    }
}

impl<T: ElementArithmeticOps + Default> AtomicArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// Reduce-scatter sum: reduces `len` elements starting at `index` and distributes disjoint result segments across all PEs.
    ///
    /// Performs a collective reduce-scatter sum over `len` array elements beginning at `index`.
    /// After reduction each PE receives a disjoint segment of the result. The returned
    /// `ArrayCollectiveReduceScatterHandle` must be driven to completion by calling `.spawn()` or
    /// `.block()` on it. Atomicity is at the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.sum_scatter(0, 1) }.block();
    ///```
    pub unsafe fn sum_scatter(
        &self,
        index: usize,
        len: usize,
    ) -> ArrayCollectiveReduceScatterHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => array.sum_scatter(index, len),
            AtomicArray::NativeAtomicArray(array) => array.sum_scatter(index, len),
            AtomicArray::GenericAtomicArray(array) => array.sum_scatter(index, len),
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Reduce-scatter product: reduces `len` elements starting at `index` and distributes disjoint result segments across all PEs.
    ///
    /// Performs a collective reduce-scatter product over `len` array elements beginning at
    /// `index`. After reduction each PE receives a disjoint segment of the result. The returned
    /// `ArrayCollectiveReduceScatterHandle` must be driven to completion by calling `.spawn()` or
    /// `.block()` on it. Atomicity is at the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.prod_scatter(0, 1) }.block();
    ///```
    pub unsafe fn prod_scatter(
        &self,
        index: usize,
        len: usize,
    ) -> ArrayCollectiveReduceScatterHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => array.prod_scatter(index, len),
            AtomicArray::NativeAtomicArray(array) => array.prod_scatter(index, len),
            AtomicArray::GenericAtomicArray(array) => array.prod_scatter(index, len),
        }
    }
}

impl<T: ElementComparePartialEqOps + Default> AtomicArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// Reduce-scatter max: reduces `len` elements starting at `index` and distributes disjoint result segments across all PEs.
    ///
    /// Performs a collective reduce-scatter maximum over `len` array elements beginning at
    /// `index`. After reduction each PE receives a disjoint segment of the result. The returned
    /// `ArrayCollectiveReduceScatterHandle` must be driven to completion by calling `.spawn()` or
    /// `.block()` on it. Atomicity is at the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.max_scatter(0, 1) }.block();
    ///```
    pub unsafe fn max_scatter(
        &self,
        index: usize,
        len: usize,
    ) -> ArrayCollectiveReduceScatterHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => array.max_scatter(index, len),
            AtomicArray::NativeAtomicArray(array) => array.max_scatter(index, len),
            AtomicArray::GenericAtomicArray(array) => array.max_scatter(index, len),
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Reduce-scatter min: reduces `len` elements starting at `index` and distributes disjoint result segments across all PEs.
    ///
    /// Performs a collective reduce-scatter minimum over `len` array elements beginning at
    /// `index`. After reduction each PE receives a disjoint segment of the result. The returned
    /// `ArrayCollectiveReduceScatterHandle` must be driven to completion by calling `.spawn()` or
    /// `.block()` on it. Atomicity is at the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.min_scatter(0, 1) }.block();
    ///```
    pub unsafe fn min_scatter(
        &self,
        index: usize,
        len: usize,
    ) -> ArrayCollectiveReduceScatterHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => array.min_scatter(index, len),
            AtomicArray::NativeAtomicArray(array) => array.min_scatter(index, len),
            AtomicArray::GenericAtomicArray(array) => array.min_scatter(index, len),
        }
    }
}

impl<T: ElementBitWiseOps + Default> AtomicArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// Reduce-scatter bitwise AND: reduces `len` elements starting at `index` and distributes disjoint result segments across all PEs.
    ///
    /// Performs a collective reduce-scatter bitwise AND over `len` array elements beginning at
    /// `index`. After reduction each PE receives a disjoint segment of the result. The returned
    /// `ArrayCollectiveReduceScatterHandle` must be driven to completion by calling `.spawn()` or
    /// `.block()` on it. Atomicity is at the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.bit_and_scatter(0, 1) }.block();
    ///```
    pub unsafe fn bit_and_scatter(
        &self,
        index: usize,
        len: usize,
    ) -> ArrayCollectiveReduceScatterHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => array.bit_and_scatter(index, len),
            AtomicArray::NativeAtomicArray(array) => array.bit_and_scatter(index, len),
            AtomicArray::GenericAtomicArray(array) => array.bit_and_scatter(index, len),
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Reduce-scatter bitwise XOR: reduces `len` elements starting at `index` and distributes disjoint result segments across all PEs.
    ///
    /// Performs a collective reduce-scatter bitwise XOR over `len` array elements beginning at
    /// `index`. After reduction each PE receives a disjoint segment of the result. The returned
    /// `ArrayCollectiveReduceScatterHandle` must be driven to completion by calling `.spawn()` or
    /// `.block()` on it. Atomicity is at the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.bit_xor_scatter(0, 1) }.block();
    ///```
    pub unsafe fn bit_xor_scatter(
        &self,
        index: usize,
        len: usize,
    ) -> ArrayCollectiveReduceScatterHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => array.bit_xor_scatter(index, len),
            AtomicArray::NativeAtomicArray(array) => array.bit_xor_scatter(index, len),
            AtomicArray::GenericAtomicArray(array) => array.bit_xor_scatter(index, len),
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Reduce-scatter bitwise OR: reduces `len` elements starting at `index` and distributes disjoint result segments across all PEs.
    ///
    /// Performs a collective reduce-scatter bitwise OR over `len` array elements beginning at
    /// `index`. After reduction each PE receives a disjoint segment of the result. The returned
    /// `ArrayCollectiveReduceScatterHandle` must be driven to completion by calling `.spawn()` or
    /// `.block()` on it. Atomicity is at the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.bit_or_scatter(0, 1) }.block();
    ///```
    pub unsafe fn bit_or_scatter(
        &self,
        index: usize,
        len: usize,
    ) -> ArrayCollectiveReduceScatterHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => array.bit_or_scatter(index, len),
            AtomicArray::NativeAtomicArray(array) => array.bit_or_scatter(index, len),
            AtomicArray::GenericAtomicArray(array) => array.bit_or_scatter(index, len),
        }
    }
}

impl<T: ElementArithmeticOps + Default> AtomicArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// Like `sum_scatter` but places received segment into caller-supplied `buffer`.
    ///
    /// Performs a collective reduce-scatter sum over `len` array elements beginning at `index`
    /// and writes each PE's disjoint result segment into `buffer` rather than allocating a new
    /// buffer. The returned `ArrayCollectiveReduceScatterIntoBufferHandle` must be driven to
    /// completion by calling `.spawn()` or `.block()` on it. Atomicity is at the element level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// unsafe { array.sum_scatter_into_buffer(0, 1, buf) }.block();
    ///```
    pub unsafe fn sum_scatter_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array.sum_scatter_into_buffer(index, len, buffer)
            }
            AtomicArray::NativeAtomicArray(array) => {
                array.sum_scatter_into_buffer(index, len, buffer)
            }
            AtomicArray::GenericAtomicArray(array) => {
                array.sum_scatter_into_buffer(index, len, buffer)
            }
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Like `prod_scatter` but places received segment into caller-supplied `buffer`.
    ///
    /// Performs a collective reduce-scatter product over `len` array elements beginning at
    /// `index` and writes each PE's disjoint result segment into `buffer` rather than allocating
    /// a new buffer. The returned `ArrayCollectiveReduceScatterIntoBufferHandle` must be driven
    /// to completion by calling `.spawn()` or `.block()` on it. Atomicity is at the element
    /// level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// unsafe { array.prod_scatter_into_buffer(0, 1, buf) }.block();
    ///```
    pub unsafe fn prod_scatter_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array.prod_scatter_into_buffer(index, len, buffer)
            }
            AtomicArray::NativeAtomicArray(array) => {
                array.prod_scatter_into_buffer(index, len, buffer)
            }
            AtomicArray::GenericAtomicArray(array) => {
                array.prod_scatter_into_buffer(index, len, buffer)
            }
        }
    }
}

impl<T: ElementComparePartialEqOps + Default> AtomicArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// Like `max_scatter` but places received segment into caller-supplied `buffer`.
    ///
    /// Performs a collective reduce-scatter maximum over `len` array elements beginning at
    /// `index` and writes each PE's disjoint result segment into `buffer` rather than allocating
    /// a new buffer. The returned `ArrayCollectiveReduceScatterIntoBufferHandle` must be driven
    /// to completion by calling `.spawn()` or `.block()` on it. Atomicity is at the element
    /// level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// unsafe { array.max_scatter_into_buffer(0, 1, buf) }.block();
    ///```
    pub unsafe fn max_scatter_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array.max_scatter_into_buffer(index, len, buffer)
            }
            AtomicArray::NativeAtomicArray(array) => {
                array.max_scatter_into_buffer(index, len, buffer)
            }
            AtomicArray::GenericAtomicArray(array) => {
                array.max_scatter_into_buffer(index, len, buffer)
            }
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Like `min_scatter` but places received segment into caller-supplied `buffer`.
    ///
    /// Performs a collective reduce-scatter minimum over `len` array elements beginning at
    /// `index` and writes each PE's disjoint result segment into `buffer` rather than allocating
    /// a new buffer. The returned `ArrayCollectiveReduceScatterIntoBufferHandle` must be driven
    /// to completion by calling `.spawn()` or `.block()` on it. Atomicity is at the element
    /// level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// unsafe { array.min_scatter_into_buffer(0, 1, buf) }.block();
    ///```
    pub unsafe fn min_scatter_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array.min_scatter_into_buffer(index, len, buffer)
            }
            AtomicArray::NativeAtomicArray(array) => {
                array.min_scatter_into_buffer(index, len, buffer)
            }
            AtomicArray::GenericAtomicArray(array) => {
                array.min_scatter_into_buffer(index, len, buffer)
            }
        }
    }
}

impl<T: ElementBitWiseOps + Default> AtomicArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// Like `bit_and_scatter` but places received segment into caller-supplied `buffer`.
    ///
    /// Performs a collective reduce-scatter bitwise AND over `len` array elements beginning at
    /// `index` and writes each PE's disjoint result segment into `buffer` rather than allocating
    /// a new buffer. The returned `ArrayCollectiveReduceScatterIntoBufferHandle` must be driven
    /// to completion by calling `.spawn()` or `.block()` on it. Atomicity is at the element
    /// level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// unsafe { array.bit_and_scatter_into_buffer(0, 1, buf) }.block();
    ///```
    pub unsafe fn bit_and_scatter_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array.bit_and_scatter_into_buffer(index, len, buffer)
            }
            AtomicArray::NativeAtomicArray(array) => {
                array.bit_and_scatter_into_buffer(index, len, buffer)
            }
            AtomicArray::GenericAtomicArray(array) => {
                array.bit_and_scatter_into_buffer(index, len, buffer)
            }
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Like `bit_xor_scatter` but places received segment into caller-supplied `buffer`.
    ///
    /// Performs a collective reduce-scatter bitwise XOR over `len` array elements beginning at
    /// `index` and writes each PE's disjoint result segment into `buffer` rather than allocating
    /// a new buffer. The returned `ArrayCollectiveReduceScatterIntoBufferHandle` must be driven
    /// to completion by calling `.spawn()` or `.block()` on it. Atomicity is at the element
    /// level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// unsafe { array.bit_xor_scatter_into_buffer(0, 1, buf) }.block();
    ///```
    pub unsafe fn bit_xor_scatter_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array.bit_xor_scatter_into_buffer(index, len, buffer)
            }
            AtomicArray::NativeAtomicArray(array) => {
                array.bit_xor_scatter_into_buffer(index, len, buffer)
            }
            AtomicArray::GenericAtomicArray(array) => {
                array.bit_xor_scatter_into_buffer(index, len, buffer)
            }
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Like `bit_or_scatter` but places received segment into caller-supplied `buffer`.
    ///
    /// Performs a collective reduce-scatter bitwise OR over `len` array elements beginning at
    /// `index` and writes each PE's disjoint result segment into `buffer` rather than allocating
    /// a new buffer. The returned `ArrayCollectiveReduceScatterIntoBufferHandle` must be driven
    /// to completion by calling `.spawn()` or `.block()` on it. Atomicity is at the element
    /// level.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: AtomicArray<usize> = AtomicArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// unsafe { array.bit_or_scatter_into_buffer(0, 1, buf) }.block();
    ///```
    pub unsafe fn bit_or_scatter_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array.bit_or_scatter_into_buffer(index, len, buffer)
            }
            AtomicArray::NativeAtomicArray(array) => {
                array.bit_or_scatter_into_buffer(index, len, buffer)
            }
            AtomicArray::GenericAtomicArray(array) => {
                array.bit_or_scatter_into_buffer(index, len, buffer)
            }
        }
    }
}
