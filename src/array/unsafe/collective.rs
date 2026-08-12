use crate::array::collective::broadcast_handle::{
    ArrayCollectiveAllToAllHandle, ArrayCollectiveAllToAllIntoBufferHandle,
    ArrayCollectiveAllToAllIntoBufferState, ArrayCollectiveAllToAllState,
    ArrayCollectiveBroadcastHandle, ArrayCollectiveBroadcastIntoBufferHandle,
    ArrayCollectiveBroadcastIntoBufferState, ArrayCollectiveBroadcastState,
    ArrayCollectiveScatterHandle, ArrayCollectiveScatterIntoBufferHandle,
    ArrayCollectiveScatterIntoBufferState, ArrayCollectiveScatterState,
};
use crate::array::collective::gather_handle::{
    ArrayCollectiveAllGatherHandle, ArrayCollectiveAllGatherIntoBufferHandle,
    ArrayCollectiveAllGatherIntoBufferState, ArrayCollectiveAllGatherState,
    ArrayCollectiveGatherHandle, ArrayCollectiveGatherIntoBufferHandle,
    ArrayCollectiveGatherIntoBufferState, ArrayCollectiveGatherState,
};
use crate::array::collective::reduce_handle::{
    ArrayCollectiveAllReduceHandle, ArrayCollectiveAllReduceInPlaceHandle,
    ArrayCollectiveAllReduceInPlaceState, ArrayCollectiveAllReduceIntoBufferHandle,
    ArrayCollectiveAllReduceIntoBufferState, ArrayCollectiveAllReduceState,
    ArrayCollectiveReduceHandle, ArrayCollectiveReduceIntoBufferHandle,
    ArrayCollectiveReduceIntoBufferState, ArrayCollectiveReduceState,
};
use crate::array::collective::reduce_scatter_handle::{
    ArrayCollectiveReduceScatterHandle, ArrayCollectiveReduceScatterIntoBufferHandle,
    ArrayCollectiveReduceScatterIntoBufferState, ArrayCollectiveReduceScatterState,
};
use crate::array::private::LamellarArrayPrivate;
use crate::lamellae::collective::{
    BroadcastInput, ReduceOp, RootOrLamellarBuffer, RootSrcOrLamellarBuffer, ScatterInput,
};
use crate::Dist;
use crate::{AsLamellarBuffer, LamellarBuffer, UnsafeArray};

impl<T: Dist> UnsafeArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// All-reduce sum of `len` elements starting at `index`, delivering the result to all PEs.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// sum is computed across all PEs and every PE receives the result.
    /// Returns an [`ArrayCollectiveAllReduceHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.sum_all(0, array.num_elems_local()) }.block();
    ///```
    pub unsafe fn sum_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all(index, len, ReduceOp::Sum);

        ArrayCollectiveAllReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceState::CollectiveAllReduce(req),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// All-reduce max of `len` elements starting at `index`, delivering the result to all PEs.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// maximum is computed across all PEs and every PE receives the result.
    /// Returns an [`ArrayCollectiveAllReduceHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.max_all(0, array.num_elems_local()) }.block();
    ///```
    pub unsafe fn max_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all(index, len, ReduceOp::Max);

        ArrayCollectiveAllReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceState::CollectiveAllReduce(req),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// All-reduce min of `len` elements starting at `index`, delivering the result to all PEs.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// minimum is computed across all PEs and every PE receives the result.
    /// Returns an [`ArrayCollectiveAllReduceHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.min_all(0, array.num_elems_local()) }.block();
    ///```
    pub unsafe fn min_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all(index, len, ReduceOp::Min);

        ArrayCollectiveAllReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceState::CollectiveAllReduce(req),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// All-reduce product of `len` elements starting at `index`, delivering the result to all PEs.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// product is computed across all PEs and every PE receives the result.
    /// Returns an [`ArrayCollectiveAllReduceHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.prod_all(0, array.num_elems_local()) }.block();
    ///```
    pub unsafe fn prod_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all(index, len, ReduceOp::Prod);

        ArrayCollectiveAllReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceState::CollectiveAllReduce(req),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// All-reduce bitwise AND of `len` elements starting at `index`, delivering the result to all PEs.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// bitwise AND is computed across all PEs and every PE receives the result.
    /// Returns an [`ArrayCollectiveAllReduceHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.bit_and_all(0, array.num_elems_local()) }.block();
    ///```
    pub unsafe fn bit_and_all(
        &self,
        index: usize,
        len: usize,
    ) -> ArrayCollectiveAllReduceHandle<T> {
        let req =
            self.inner
                .data
                .mem_region
                .as_base::<T>()
                .reduce_all(index, len, ReduceOp::BitAnd);

        ArrayCollectiveAllReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceState::CollectiveAllReduce(req),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// All-reduce bitwise XOR of `len` elements starting at `index`, delivering the result to all PEs.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// bitwise XOR is computed across all PEs and every PE receives the result.
    /// Returns an [`ArrayCollectiveAllReduceHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.bit_xor_all(0, array.num_elems_local()) }.block();
    ///```
    pub unsafe fn bit_xor_all(
        &self,
        index: usize,
        len: usize,
    ) -> ArrayCollectiveAllReduceHandle<T> {
        let req =
            self.inner
                .data
                .mem_region
                .as_base::<T>()
                .reduce_all(index, len, ReduceOp::BitXor);

        ArrayCollectiveAllReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceState::CollectiveAllReduce(req),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// All-reduce bitwise OR of `len` elements starting at `index`, delivering the result to all PEs.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// bitwise OR is computed across all PEs and every PE receives the result.
    /// Returns an [`ArrayCollectiveAllReduceHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.bit_or_all(0, array.num_elems_local()) }.block();
    ///```
    pub unsafe fn bit_or_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all(index, len, ReduceOp::BitOr);

        ArrayCollectiveAllReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceState::CollectiveAllReduce(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// All-reduce sum of `len` elements starting at `index`, writing the result into `buffer` on all PEs.
    ///
    /// Like [`sum_all`](UnsafeArray::sum_all) but places the result into a caller-supplied
    /// [`LamellarBuffer`] instead of allocating one internally.
    /// Returns an [`ArrayCollectiveAllReduceIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    /// The buffer must be large enough to hold the result.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// let _result = unsafe { array.sum_all_into_buffer(0, array.num_elems_local(), buf.into()) }.block();
    ///```
    pub unsafe fn sum_all_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_into_buffer(index, len, ReduceOp::Sum, buffer);

        ArrayCollectiveAllReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// All-reduce max of `len` elements starting at `index`, writing the result into `buffer` on all PEs.
    ///
    /// Like [`max_all`](UnsafeArray::max_all) but places the result into a caller-supplied
    /// [`LamellarBuffer`] instead of allocating one internally.
    /// Returns an [`ArrayCollectiveAllReduceIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    /// The buffer must be large enough to hold the result.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// let _result = unsafe { array.max_all_into_buffer(0, array.num_elems_local(), buf.into()) }.block();
    ///```
    pub unsafe fn max_all_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_into_buffer(index, len, ReduceOp::Max, buffer);

        ArrayCollectiveAllReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// All-reduce min of `len` elements starting at `index`, writing the result into `buffer` on all PEs.
    ///
    /// Like [`min_all`](UnsafeArray::min_all) but places the result into a caller-supplied
    /// [`LamellarBuffer`] instead of allocating one internally.
    /// Returns an [`ArrayCollectiveAllReduceIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    /// The buffer must be large enough to hold the result.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// let _result = unsafe { array.min_all_into_buffer(0, array.num_elems_local(), buf.into()) }.block();
    ///```
    pub unsafe fn min_all_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_into_buffer(index, len, ReduceOp::Min, buffer);

        ArrayCollectiveAllReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// All-reduce product of `len` elements starting at `index`, writing the result into `buffer` on all PEs.
    ///
    /// Like [`prod_all`](UnsafeArray::prod_all) but places the result into a caller-supplied
    /// [`LamellarBuffer`] instead of allocating one internally.
    /// Returns an [`ArrayCollectiveAllReduceIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    /// The buffer must be large enough to hold the result.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// let _result = unsafe { array.prod_all_into_buffer(0, array.num_elems_local(), buf.into()) }.block();
    ///```
    pub unsafe fn prod_all_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_into_buffer(index, len, ReduceOp::Prod, buffer);

        ArrayCollectiveAllReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// All-reduce bitwise AND of `len` elements starting at `index`, writing the result into `buffer` on all PEs.
    ///
    /// Like [`bit_and_all`](UnsafeArray::bit_and_all) but places the result into a caller-supplied
    /// [`LamellarBuffer`] instead of allocating one internally.
    /// Returns an [`ArrayCollectiveAllReduceIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    /// The buffer must be large enough to hold the result.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// let _result = unsafe { array.bit_and_all_into_buffer(0, array.num_elems_local(), buf.into()) }.block();
    ///```
    pub unsafe fn bit_and_all_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_into_buffer(index, len, ReduceOp::BitAnd, buffer);

        ArrayCollectiveAllReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// All-reduce bitwise XOR of `len` elements starting at `index`, writing the result into `buffer` on all PEs.
    ///
    /// Like [`bit_xor_all`](UnsafeArray::bit_xor_all) but places the result into a caller-supplied
    /// [`LamellarBuffer`] instead of allocating one internally.
    /// Returns an [`ArrayCollectiveAllReduceIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    /// The buffer must be large enough to hold the result.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// let _result = unsafe { array.bit_xor_all_into_buffer(0, array.num_elems_local(), buf.into()) }.block();
    ///```
    pub unsafe fn bit_xor_all_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_into_buffer(index, len, ReduceOp::BitXor, buffer);

        ArrayCollectiveAllReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// All-reduce bitwise OR of `len` elements starting at `index`, writing the result into `buffer` on all PEs.
    ///
    /// Like [`bit_or_all`](UnsafeArray::bit_or_all) but places the result into a caller-supplied
    /// [`LamellarBuffer`] instead of allocating one internally.
    /// Returns an [`ArrayCollectiveAllReduceIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    /// The buffer must be large enough to hold the result.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// let _result = unsafe { array.bit_or_all_into_buffer(0, array.num_elems_local(), buf.into()) }.block();
    ///```
    pub unsafe fn bit_or_all_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_into_buffer(index, len, ReduceOp::BitOr, buffer);

        ArrayCollectiveAllReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// All-reduce sum in place, using `src_and_dst` as both input and output on every PE.
    ///
    /// Each PE provides elements via `src_and_dst`; the global sum overwrites the same buffer.
    /// Returns an [`ArrayCollectiveAllReduceInPlaceHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. The buffer must be valid for both read and write for the duration of
    /// the operation.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// let _result = unsafe { array.sum_all_in_place(buf.into()) }.block();
    ///```
    pub unsafe fn sum_all_in_place<B: AsLamellarBuffer<T>>(
        &self,
        src_and_dst: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_in_place(src_and_dst, ReduceOp::Sum);

        ArrayCollectiveAllReduceInPlaceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceInPlaceState::CollectiveAllReduceInPlace(req),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// All-reduce max in place, using `src_and_dst` as both input and output on every PE.
    ///
    /// Each PE provides elements via `src_and_dst`; the global max overwrites the same buffer.
    /// Returns an [`ArrayCollectiveAllReduceInPlaceHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. The buffer must be valid for both read and write for the duration of
    /// the operation.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// let _result = unsafe { array.max_all_in_place(buf.into()) }.block();
    ///```
    pub unsafe fn max_all_in_place<B: AsLamellarBuffer<T>>(
        &self,
        src_and_dst: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_in_place(src_and_dst, ReduceOp::Max);

        ArrayCollectiveAllReduceInPlaceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceInPlaceState::CollectiveAllReduceInPlace(req),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// All-reduce min in place, using `src_and_dst` as both input and output on every PE.
    ///
    /// Each PE provides elements via `src_and_dst`; the global min overwrites the same buffer.
    /// Returns an [`ArrayCollectiveAllReduceInPlaceHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. The buffer must be valid for both read and write for the duration of
    /// the operation.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// let _result = unsafe { array.min_all_in_place(buf.into()) }.block();
    ///```
    pub unsafe fn min_all_in_place<B: AsLamellarBuffer<T>>(
        &self,
        src_and_dst: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_in_place(src_and_dst, ReduceOp::Min);

        ArrayCollectiveAllReduceInPlaceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceInPlaceState::CollectiveAllReduceInPlace(req),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// All-reduce product in place, using `src_and_dst` as both input and output on every PE.
    ///
    /// Each PE provides elements via `src_and_dst`; the global product overwrites the same buffer.
    /// Returns an [`ArrayCollectiveAllReduceInPlaceHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. The buffer must be valid for both read and write for the duration of
    /// the operation.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// let _result = unsafe { array.prod_all_in_place(buf.into()) }.block();
    ///```
    pub unsafe fn prod_all_in_place<B: AsLamellarBuffer<T>>(
        &self,
        src_and_dst: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_in_place(src_and_dst, ReduceOp::Prod);

        ArrayCollectiveAllReduceInPlaceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceInPlaceState::CollectiveAllReduceInPlace(req),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// All-reduce bitwise AND in place, using `src_and_dst` as both input and output on every PE.
    ///
    /// Each PE provides elements via `src_and_dst`; the global bitwise AND overwrites the same buffer.
    /// Returns an [`ArrayCollectiveAllReduceInPlaceHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. The buffer must be valid for both read and write for the duration of
    /// the operation.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// let _result = unsafe { array.bit_and_all_in_place(buf.into()) }.block();
    ///```
    pub unsafe fn bit_and_all_in_place<B: AsLamellarBuffer<T>>(
        &self,
        src_and_dst: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_in_place(src_and_dst, ReduceOp::BitAnd);

        ArrayCollectiveAllReduceInPlaceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceInPlaceState::CollectiveAllReduceInPlace(req),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// All-reduce bitwise XOR in place, using `src_and_dst` as both input and output on every PE.
    ///
    /// Each PE provides elements via `src_and_dst`; the global bitwise XOR overwrites the same buffer.
    /// Returns an [`ArrayCollectiveAllReduceInPlaceHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. The buffer must be valid for both read and write for the duration of
    /// the operation.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// let _result = unsafe { array.bit_xor_all_in_place(buf.into()) }.block();
    ///```
    pub unsafe fn bit_xor_all_in_place<B: AsLamellarBuffer<T>>(
        &self,
        src_and_dst: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_in_place(src_and_dst, ReduceOp::BitXor);

        ArrayCollectiveAllReduceInPlaceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceInPlaceState::CollectiveAllReduceInPlace(req),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// All-reduce bitwise OR in place, using `src_and_dst` as both input and output on every PE.
    ///
    /// Each PE provides elements via `src_and_dst`; the global bitwise OR overwrites the same buffer.
    /// Returns an [`ArrayCollectiveAllReduceInPlaceHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. The buffer must be valid for both read and write for the duration of
    /// the operation.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// let _result = unsafe { array.bit_or_all_in_place(buf.into()) }.block();
    ///```
    pub unsafe fn bit_or_all_in_place<B: AsLamellarBuffer<T>>(
        &self,
        src_and_dst: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_all_in_place(src_and_dst, ReduceOp::BitOr);

        ArrayCollectiveAllReduceInPlaceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllReduceInPlaceState::CollectiveAllReduceInPlace(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// Reduce sum of `len` elements starting at `index`, delivering the result only to `pe`.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// sum is computed and the result is stored on the designated root PE `pe`.
    /// Returns an [`ArrayCollectiveReduceHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `pe` must be
    /// a valid PE index.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.sum_at_pe(0, array.num_elems_local(), 0) }.block();
    ///```
    pub unsafe fn sum_at_pe(
        &self,
        index: usize,
        len: usize,
        pe: usize,
    ) -> ArrayCollectiveReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce(ReduceOp::Sum, index, len, pe);

        ArrayCollectiveReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceState::CollectiveReduce(req),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Reduce max of `len` elements starting at `index`, delivering the result only to `pe`.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// maximum is computed and the result is stored on the designated root PE `pe`.
    /// Returns an [`ArrayCollectiveReduceHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `pe` must be
    /// a valid PE index.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.max_at_pe(0, array.num_elems_local(), 0) }.block();
    ///```
    pub unsafe fn max_at_pe(
        &self,
        index: usize,
        len: usize,
        pe: usize,
    ) -> ArrayCollectiveReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce(ReduceOp::Max, index, len, pe);

        ArrayCollectiveReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceState::CollectiveReduce(req),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Reduce min of `len` elements starting at `index`, delivering the result only to `pe`.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// minimum is computed and the result is stored on the designated root PE `pe`.
    /// Returns an [`ArrayCollectiveReduceHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `pe` must be
    /// a valid PE index.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.min_at_pe(0, array.num_elems_local(), 0) }.block();
    ///```
    pub unsafe fn min_at_pe(
        &self,
        index: usize,
        len: usize,
        pe: usize,
    ) -> ArrayCollectiveReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce(ReduceOp::Min, index, len, pe);

        ArrayCollectiveReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceState::CollectiveReduce(req),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Reduce product of `len` elements starting at `index`, delivering the result only to `pe`.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// product is computed and the result is stored on the designated root PE `pe`.
    /// Returns an [`ArrayCollectiveReduceHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `pe` must be
    /// a valid PE index.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.prod_at_pe(0, array.num_elems_local(), 0) }.block();
    ///```
    pub unsafe fn prod_at_pe(
        &self,
        index: usize,
        len: usize,
        pe: usize,
    ) -> ArrayCollectiveReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce(ReduceOp::Prod, index, len, pe);

        ArrayCollectiveReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceState::CollectiveReduce(req),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Reduce bitwise OR of `len` elements starting at `index`, delivering the result only to `pe`.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// bitwise OR is computed and the result is stored on the designated root PE `pe`.
    /// Returns an [`ArrayCollectiveReduceHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `pe` must be
    /// a valid PE index.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.bit_or_at_pe(0, array.num_elems_local(), 0) }.block();
    ///```
    pub unsafe fn bit_or_at_pe(
        &self,
        index: usize,
        len: usize,
        pe: usize,
    ) -> ArrayCollectiveReduceHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce(ReduceOp::BitOr, index, len, pe);

        ArrayCollectiveReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceState::CollectiveReduce(req),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Reduce bitwise AND of `len` elements starting at `index`, delivering the result only to `pe`.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// bitwise AND is computed and the result is stored on the designated root PE `pe`.
    /// Returns an [`ArrayCollectiveReduceHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `pe` must be
    /// a valid PE index.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.bit_and_at_pe(0, array.num_elems_local(), 0) }.block();
    ///```
    pub unsafe fn bit_and_at_pe(
        &self,
        index: usize,
        len: usize,
        pe: usize,
    ) -> ArrayCollectiveReduceHandle<T> {
        let req =
            self.inner
                .data
                .mem_region
                .as_base::<T>()
                .reduce(ReduceOp::BitAnd, index, len, pe);

        ArrayCollectiveReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceState::CollectiveReduce(req),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Reduce bitwise XOR of `len` elements starting at `index`, delivering the result only to `pe`.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// bitwise XOR is computed and the result is stored on the designated root PE `pe`.
    /// Returns an [`ArrayCollectiveReduceHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `pe` must be
    /// a valid PE index.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.bit_xor_at_pe(0, array.num_elems_local(), 0) }.block();
    ///```
    pub unsafe fn bit_xor_at_pe(
        &self,
        index: usize,
        len: usize,
        pe: usize,
    ) -> ArrayCollectiveReduceHandle<T> {
        let req =
            self.inner
                .data
                .mem_region
                .as_base::<T>()
                .reduce(ReduceOp::BitXor, index, len, pe);

        ArrayCollectiveReduceHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceState::CollectiveReduce(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// Reduce sum of `len` elements starting at `index`, writing the result into `dst` on the root PE.
    ///
    /// Like [`sum_at_pe`](UnsafeArray::sum_at_pe) but the caller supplies the destination buffer via
    /// `dst: RootOrLamellarBuffer` instead of having the runtime allocate one. The root PE is encoded
    /// in the `dst` value. Returns an [`ArrayCollectiveReduceIntoBufferHandle`] that must be
    /// `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `dst` must be
    /// large enough to hold one element.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// let _result = unsafe { array.sum_at_pe_into_buffer(0, array.num_elems_local(), RootOrLamellarBuffer::Root(buf.into())) }.block();
    ///```
    pub unsafe fn sum_at_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        dst: RootOrLamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_into_buffer(ReduceOp::Sum, index, len, dst);

        ArrayCollectiveReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Reduce max of `len` elements starting at `index`, writing the result into `dst` on the root PE.
    ///
    /// Like [`max_at_pe`](UnsafeArray::max_at_pe) but the caller supplies the destination buffer via
    /// `dst: RootOrLamellarBuffer` instead of having the runtime allocate one. The root PE is encoded
    /// in the `dst` value. Returns an [`ArrayCollectiveReduceIntoBufferHandle`] that must be
    /// `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `dst` must be
    /// large enough to hold one element.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// let _result = unsafe { array.max_at_pe_into_buffer(0, array.num_elems_local(), RootOrLamellarBuffer::Root(buf.into())) }.block();
    ///```
    pub unsafe fn max_at_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        dst: RootOrLamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_into_buffer(ReduceOp::Max, index, len, dst);

        ArrayCollectiveReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Reduce min of `len` elements starting at `index`, writing the result into `dst` on the root PE.
    ///
    /// Like [`min_at_pe`](UnsafeArray::min_at_pe) but the caller supplies the destination buffer via
    /// `dst: RootOrLamellarBuffer` instead of having the runtime allocate one. The root PE is encoded
    /// in the `dst` value. Returns an [`ArrayCollectiveReduceIntoBufferHandle`] that must be
    /// `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `dst` must be
    /// large enough to hold one element.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// let _result = unsafe { array.min_at_pe_into_buffer(0, array.num_elems_local(), RootOrLamellarBuffer::Root(buf.into())) }.block();
    ///```
    pub unsafe fn min_at_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        dst: RootOrLamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_into_buffer(ReduceOp::Min, index, len, dst);

        ArrayCollectiveReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Reduce product of `len` elements starting at `index`, writing the result into `dst` on the root PE.
    ///
    /// Like [`prod_at_pe`](UnsafeArray::prod_at_pe) but the caller supplies the destination buffer via
    /// `dst: RootOrLamellarBuffer` instead of having the runtime allocate one. The root PE is encoded
    /// in the `dst` value. Returns an [`ArrayCollectiveReduceIntoBufferHandle`] that must be
    /// `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `dst` must be
    /// large enough to hold one element.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// let _result = unsafe { array.prod_at_pe_into_buffer(0, array.num_elems_local(), RootOrLamellarBuffer::Root(buf.into())) }.block();
    ///```
    pub unsafe fn prod_at_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        dst: RootOrLamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_into_buffer(ReduceOp::Prod, index, len, dst);

        ArrayCollectiveReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Reduce bitwise OR of `len` elements starting at `index`, writing the result into `dst` on the root PE.
    ///
    /// Like [`bit_or_at_pe`](UnsafeArray::bit_or_at_pe) but the caller supplies the destination buffer via
    /// `dst: RootOrLamellarBuffer` instead of having the runtime allocate one. The root PE is encoded
    /// in the `dst` value. Returns an [`ArrayCollectiveReduceIntoBufferHandle`] that must be
    /// `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `dst` must be
    /// large enough to hold one element.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// let _result = unsafe { array.bit_or_at_pe_into_buffer(0, array.num_elems_local(), RootOrLamellarBuffer::Root(buf.into())) }.block();
    ///```
    pub unsafe fn bit_or_at_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        dst: RootOrLamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_into_buffer(ReduceOp::BitOr, index, len, dst);

        ArrayCollectiveReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Reduce bitwise AND of `len` elements starting at `index`, writing the result into `dst` on the root PE.
    ///
    /// Like [`bit_and_at_pe`](UnsafeArray::bit_and_at_pe) but the caller supplies the destination buffer via
    /// `dst: RootOrLamellarBuffer` instead of having the runtime allocate one. The root PE is encoded
    /// in the `dst` value. Returns an [`ArrayCollectiveReduceIntoBufferHandle`] that must be
    /// `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `dst` must be
    /// large enough to hold one element.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// let _result = unsafe { array.bit_and_at_pe_into_buffer(0, array.num_elems_local(), RootOrLamellarBuffer::Root(buf.into())) }.block();
    ///```
    pub unsafe fn bit_and_at_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        dst: RootOrLamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_into_buffer(ReduceOp::BitAnd, index, len, dst);

        ArrayCollectiveReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Reduce bitwise XOR of `len` elements starting at `index`, writing the result into `dst` on the root PE.
    ///
    /// Like [`bit_xor_at_pe`](UnsafeArray::bit_xor_at_pe) but the caller supplies the destination buffer via
    /// `dst: RootOrLamellarBuffer` instead of having the runtime allocate one. The root PE is encoded
    /// in the `dst` value. Returns an [`ArrayCollectiveReduceIntoBufferHandle`] that must be
    /// `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `dst` must be
    /// large enough to hold one element.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(1);
    /// let _result = unsafe { array.bit_xor_at_pe_into_buffer(0, array.num_elems_local(), RootOrLamellarBuffer::Root(buf.into())) }.block();
    ///```
    pub unsafe fn bit_xor_at_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        dst: RootOrLamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_into_buffer(ReduceOp::BitXor, index, len, dst);

        ArrayCollectiveReduceIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req),
            spawned: false,
        }
    }
}

// impl<T: Dist> UnsafeArray<T> {
//     pub unsafe fn sum_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         let req = self
//             .inner
//             .data
//             .mem_region
//             .as_base::<T>()
//             .reduce_in_place(ReduceOp::Sum, pe);

//         ArrayCollectiveReduceInPlaceHandle {
//             array: self.as_lamellar_byte_array(),
//             state: ArrayCollectiveReduceInPlaceState::CollectiveReduceInPlace(req),
//             spawned: false,
//         }
//     }

//     pub unsafe fn max_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         let req = self
//             .inner
//             .data
//             .mem_region
//             .as_base::<T>()
//             .reduce_in_place(ReduceOp::Max, pe);

//         ArrayCollectiveReduceInPlaceHandle {
//             array: self.as_lamellar_byte_array(),
//             state: ArrayCollectiveReduceInPlaceState::CollectiveReduceInPlace(req),
//             spawned: false,
//         }
//     }

//     pub unsafe fn min_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         let req = self
//             .inner
//             .data
//             .mem_region
//             .as_base::<T>()
//             .reduce_in_place(ReduceOp::Min, pe);

//         ArrayCollectiveReduceInPlaceHandle {
//             array: self.as_lamellar_byte_array(),
//             state: ArrayCollectiveReduceInPlaceState::CollectiveReduceInPlace(req),
//             spawned: false,
//         }
//     }

//     pub unsafe fn prod_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         let req = self
//             .inner
//             .data
//             .mem_region
//             .as_base::<T>()
//             .reduce_in_place(ReduceOp::Prod, pe);

//         ArrayCollectiveReduceInPlaceHandle {
//             array: self.as_lamellar_byte_array(),
//             state: ArrayCollectiveReduceInPlaceState::CollectiveReduceInPlace(req),
//             spawned: false,
//         }
//     }

//     pub unsafe fn bit_or_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         let req = self
//             .inner
//             .data
//             .mem_region
//             .as_base::<T>()
//             .reduce_in_place(ReduceOp::BitOr, pe);

//         ArrayCollectiveReduceInPlaceHandle {
//             array: self.as_lamellar_byte_array(),
//             state: ArrayCollectiveReduceInPlaceState::CollectiveReduceInPlace(req),
//             spawned: false,
//         }
//     }

//     pub unsafe fn bit_and_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         let req = self
//             .inner
//             .data
//             .mem_region
//             .as_base::<T>()
//             .reduce_in_place(ReduceOp::BitAnd, pe);

//         ArrayCollectiveReduceInPlaceHandle {
//             array: self.as_lamellar_byte_array(),
//             state: ArrayCollectiveReduceInPlaceState::CollectiveReduceInPlace(req),
//             spawned: false,
//         }
//     }

//     pub unsafe fn bit_xor_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         let req = self
//             .inner
//             .data
//             .mem_region
//             .as_base::<T>()
//             .reduce_in_place(ReduceOp::BitXor, pe);

//         ArrayCollectiveReduceInPlaceHandle {
//             array: self.as_lamellar_byte_array(),
//             state: ArrayCollectiveReduceInPlaceState::CollectiveReduceInPlace(req),
//             spawned: false,
//         }
//     }
// }

impl<T: Dist> UnsafeArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// Gathers `len` elements starting at `index` from every PE, delivering the concatenated result to all PEs.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The gathered
    /// data from all PEs is concatenated and every PE receives the full result.
    /// Returns an [`ArrayCollectiveAllGatherHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.gather_all(0, array.num_elems_local()) }.block();
    ///```
    pub unsafe fn gather_all(&self, index: usize, len: usize) -> ArrayCollectiveAllGatherHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .gather_all(index, len);

        ArrayCollectiveAllGatherHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllGatherState::CollectiveAllGather(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// Gathers `len` elements starting at `index` from every PE into a caller-supplied `buffer`, delivering to all PEs.
    ///
    /// Like [`gather_all`](UnsafeArray::gather_all) but places the concatenated result into the
    /// caller-supplied [`LamellarBuffer`] instead of allocating one internally.
    /// Returns an [`ArrayCollectiveAllGatherIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `buffer` must
    /// be large enough to hold `num_pes * len` elements.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(world.num_pes());
    /// let _result = unsafe { array.gather_all_into_buffer(0, array.num_elems_local(), buf.into()) }.block();
    ///```
    pub unsafe fn gather_all_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllGatherIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .gather_all_into_buffer(index, len, buffer);

        ArrayCollectiveAllGatherIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllGatherIntoBufferState::CollectiveAllGatherIntoBuffer(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// Gathers `len` elements starting at `index` from every PE, delivering the concatenated result only to PE `pe`.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The gathered
    /// data is concatenated and delivered only to the designated root PE `pe`.
    /// Returns an [`ArrayCollectiveGatherHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `pe` must be
    /// a valid PE index.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.gather_at_pe(0, array.num_elems_local(), 0) }.block();
    ///```
    pub unsafe fn gather_at_pe(
        &self,
        index: usize,
        len: usize,
        pe: usize,
    ) -> ArrayCollectiveGatherHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .gather(index, len, pe);

        ArrayCollectiveGatherHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveGatherState::CollectiveGather(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// Gathers `len` elements starting at `index` from every PE into a caller-supplied `dst`, delivering only to the root PE.
    ///
    /// Like [`gather_at_pe`](UnsafeArray::gather_at_pe) but places the concatenated result into the
    /// caller-supplied `dst: RootOrLamellarBuffer`. The root PE is encoded in the `dst` value.
    /// Returns an [`ArrayCollectiveGatherIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `dst` must be
    /// large enough to hold `num_pes * len` elements.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(world.num_pes());
    /// let _result = unsafe { array.gather_at_pe_into_buffer(0, array.num_elems_local(), RootOrLamellarBuffer::Root(buf.into())) }.block();
    ///```
    pub unsafe fn gather_at_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        dst: RootOrLamellarBuffer<T, B>,
    ) -> ArrayCollectiveGatherIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .gather_into_buffer(index, len, dst);

        ArrayCollectiveGatherIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveGatherIntoBufferState::CollectiveGatherIntoBuffer(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// All-to-all exchange: each PE sends `len` elements starting at `index` to every other PE.
    ///
    /// Each PE sends `len` elements from its local segment beginning at `index` to every other PE,
    /// and receives one segment per PE. The result on each PE is the concatenation of segments
    /// received from all PEs.
    /// Returns an [`ArrayCollectiveAllToAllHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.alltoall(0, array.num_elems_local()) }.block();
    ///```
    pub unsafe fn alltoall(&self, index: usize, len: usize) -> ArrayCollectiveAllToAllHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .alltoall(index, len);

        ArrayCollectiveAllToAllHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllToAllState::CollectiveAllToAll(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// All-to-all exchange placing received data into caller-supplied `buffer`.
    ///
    /// Like [`alltoall`](UnsafeArray::alltoall) but places the received data into the caller-supplied
    /// [`LamellarBuffer`] instead of allocating one internally.
    /// Returns an [`ArrayCollectiveAllToAllIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `buffer` must
    /// be large enough to hold `num_pes * len` elements.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(world.num_pes());
    /// let _result = unsafe { array.alltoall_into_buffer(0, array.num_elems_local(), buf.into()) }.block();
    ///```
    pub unsafe fn alltoall_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveAllToAllIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .alltoall_into_buffer(index, len, buffer);

        ArrayCollectiveAllToAllIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveAllToAllIntoBufferState::CollectiveAllToAllIntoBuffer(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// Broadcasts `len` elements from the root PE specified by `src_or_root_pe` to all PEs.
    ///
    /// `src_or_root_pe: BroadcastInput` encodes both the root PE index and, on the root, the source
    /// buffer. All non-root PEs receive the broadcast data into their local array segment.
    /// Returns an [`ArrayCollectiveBroadcastHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. The root PE's source buffer must hold at least `len` elements.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.broadcast_from_pe(BroadcastInput::Root(0), array.num_elems_local()) }.block();
    ///```
    pub unsafe fn broadcast_from_pe(
        &self,
        src_or_root_pe: BroadcastInput,
        len: usize,
    ) -> ArrayCollectiveBroadcastHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .broadcast(src_or_root_pe, len);

        ArrayCollectiveBroadcastHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveBroadcastState::CollectiveBroadcast(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// Broadcasts `len` elements from the root PE, placing received data into caller-supplied `dst`.
    ///
    /// Like [`broadcast_from_pe`](UnsafeArray::broadcast_from_pe) but places received data into the
    /// caller-supplied `dst: RootSrcOrLamellarBuffer`. On the root PE `dst` encodes both the source
    /// data and the destination; on non-root PEs it is the destination buffer.
    /// Returns an [`ArrayCollectiveBroadcastIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. The root PE's source buffer must hold at least `len` elements. All
    /// destination buffers must be large enough to hold `len` elements.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.broadcast_from_pe_into_buffer(RootSrcOrLamellarBuffer::<usize, OneSidedMemoryRegion<usize>>::Root(0), 1) }.block();
    ///```
    pub unsafe fn broadcast_from_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        dst: RootSrcOrLamellarBuffer<T, B>,
        len: usize,
    ) -> ArrayCollectiveBroadcastIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .broadcast_into_buffer(dst, len);

        ArrayCollectiveBroadcastIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveBroadcastIntoBufferState::CollectiveBroadcastIntoBuffer(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// Scatters segments of data from the root PE to each PE, with `len` elements per PE.
    ///
    /// `src_or_root_pe: ScatterInput` encodes the root PE index and, on the root, the source buffer
    /// containing `num_pes * len` elements. Each PE receives its disjoint `len`-element segment.
    /// Returns an [`ArrayCollectiveScatterHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. The root PE's source buffer must hold at least `num_pes * len` elements.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.scatter_from_pe(ScatterInput::Root(0), array.num_elems_local()) }.block();
    ///```
    pub unsafe fn scatter_from_pe(
        &self,
        src_or_root_pe: ScatterInput,
        len: usize,
    ) -> ArrayCollectiveScatterHandle<T> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .scatter(src_or_root_pe, len);

        ArrayCollectiveScatterHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveScatterState::CollectiveScatter(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// Scatters segments from the root PE, placing each PE's received segment into caller-supplied `buffer`.
    ///
    /// Like [`scatter_from_pe`](UnsafeArray::scatter_from_pe) but places the received `len`-element
    /// segment into the caller-supplied [`LamellarBuffer`] instead of writing into the local array.
    /// `src_or_root_pe: ScatterInput` encodes the root PE.
    /// Returns an [`ArrayCollectiveScatterIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. The root PE's source buffer must hold at least `num_pes * len` elements.
    /// Each PE's `buffer` must be large enough to hold `len` elements.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(array.num_elems_local());
    /// let _result = unsafe { array.scatter_from_pe_into_buffer(buf.into(), ScatterInput::Root(0), array.num_elems_local()) }.block();
    ///```
    pub unsafe fn scatter_from_pe_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        buffer: LamellarBuffer<T, B>,
        src_or_root_pe: ScatterInput,
        len: usize,
    ) -> ArrayCollectiveScatterIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .scatter_into_buffer(buffer, src_or_root_pe, len);

        ArrayCollectiveScatterIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveScatterIntoBufferState::CollectiveScatterIntoBuffer(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// Reduce-scatter sum: reduces `len` elements starting at `index` and distributes disjoint result segments across PEs.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// sum is computed and the result is split into disjoint segments, one per PE, each PE receiving
    /// its own segment.
    /// Returns an [`ArrayCollectiveReduceScatterHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.sum_scatter(0, array.num_elems_local()) }.block();
    ///```
    pub unsafe fn sum_scatter(
        &self,
        index: usize,
        len: usize,
    ) -> ArrayCollectiveReduceScatterHandle<T> {
        let req =
            self.inner
                .data
                .mem_region
                .as_base::<T>()
                .reduce_scatter(ReduceOp::Sum, index, len);

        ArrayCollectiveReduceScatterHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterState::CollectiveReduceScatter(req),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Reduce-scatter max: reduces `len` elements starting at `index` and distributes disjoint result segments across PEs.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// maximum is computed and the result is split into disjoint segments, one per PE, each PE receiving
    /// its own segment.
    /// Returns an [`ArrayCollectiveReduceScatterHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.max_scatter(0, array.num_elems_local()) }.block();
    ///```
    pub unsafe fn max_scatter(
        &self,
        index: usize,
        len: usize,
    ) -> ArrayCollectiveReduceScatterHandle<T> {
        let req =
            self.inner
                .data
                .mem_region
                .as_base::<T>()
                .reduce_scatter(ReduceOp::Max, index, len);

        ArrayCollectiveReduceScatterHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterState::CollectiveReduceScatter(req),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Reduce-scatter min: reduces `len` elements starting at `index` and distributes disjoint result segments across PEs.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// minimum is computed and the result is split into disjoint segments, one per PE, each PE receiving
    /// its own segment.
    /// Returns an [`ArrayCollectiveReduceScatterHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.min_scatter(0, array.num_elems_local()) }.block();
    ///```
    pub unsafe fn min_scatter(
        &self,
        index: usize,
        len: usize,
    ) -> ArrayCollectiveReduceScatterHandle<T> {
        let req =
            self.inner
                .data
                .mem_region
                .as_base::<T>()
                .reduce_scatter(ReduceOp::Min, index, len);

        ArrayCollectiveReduceScatterHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterState::CollectiveReduceScatter(req),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Reduce-scatter product: reduces `len` elements starting at `index` and distributes disjoint result segments across PEs.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// product is computed and the result is split into disjoint segments, one per PE, each PE receiving
    /// its own segment.
    /// Returns an [`ArrayCollectiveReduceScatterHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.prod_scatter(0, array.num_elems_local()) }.block();
    ///```
    pub unsafe fn prod_scatter(
        &self,
        index: usize,
        len: usize,
    ) -> ArrayCollectiveReduceScatterHandle<T> {
        let req =
            self.inner
                .data
                .mem_region
                .as_base::<T>()
                .reduce_scatter(ReduceOp::Prod, index, len);

        ArrayCollectiveReduceScatterHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterState::CollectiveReduceScatter(req),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Reduce-scatter bitwise AND: reduces `len` elements starting at `index` and distributes disjoint result segments across PEs.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// bitwise AND is computed and the result is split into disjoint segments, one per PE, each PE receiving
    /// its own segment.
    /// Returns an [`ArrayCollectiveReduceScatterHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.bit_and_scatter(0, array.num_elems_local()) }.block();
    ///```
    pub unsafe fn bit_and_scatter(
        &self,
        index: usize,
        len: usize,
    ) -> ArrayCollectiveReduceScatterHandle<T> {
        let req =
            self.inner
                .data
                .mem_region
                .as_base::<T>()
                .reduce_scatter(ReduceOp::BitAnd, index, len);

        ArrayCollectiveReduceScatterHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterState::CollectiveReduceScatter(req),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Reduce-scatter bitwise XOR: reduces `len` elements starting at `index` and distributes disjoint result segments across PEs.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// bitwise XOR is computed and the result is split into disjoint segments, one per PE, each PE receiving
    /// its own segment.
    /// Returns an [`ArrayCollectiveReduceScatterHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.bit_xor_scatter(0, array.num_elems_local()) }.block();
    ///```
    pub unsafe fn bit_xor_scatter(
        &self,
        index: usize,
        len: usize,
    ) -> ArrayCollectiveReduceScatterHandle<T> {
        let req =
            self.inner
                .data
                .mem_region
                .as_base::<T>()
                .reduce_scatter(ReduceOp::BitXor, index, len);

        ArrayCollectiveReduceScatterHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterState::CollectiveReduceScatter(req),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Reduce-scatter bitwise OR: reduces `len` elements starting at `index` and distributes disjoint result segments across PEs.
    ///
    /// Each PE contributes `len` elements from its local segment beginning at `index`. The global
    /// bitwise OR is computed and the result is split into disjoint segments, one per PE, each PE receiving
    /// its own segment.
    /// Returns an [`ArrayCollectiveReduceScatterHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let _result = unsafe { array.bit_or_scatter(0, array.num_elems_local()) }.block();
    ///```
    pub unsafe fn bit_or_scatter(
        &self,
        index: usize,
        len: usize,
    ) -> ArrayCollectiveReduceScatterHandle<T> {
        let req =
            self.inner
                .data
                .mem_region
                .as_base::<T>()
                .reduce_scatter(ReduceOp::BitOr, index, len);

        ArrayCollectiveReduceScatterHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterState::CollectiveReduceScatter(req),
            spawned: false,
        }
    }
}

impl<T: Dist> UnsafeArray<T> {
    #[doc(alias("Collective", "collective"))]
    /// Reduce-scatter sum, placing each PE's result segment into caller-supplied `buffer`.
    ///
    /// Like [`sum_scatter`](UnsafeArray::sum_scatter) but places the received result segment into
    /// the caller-supplied [`LamellarBuffer`] instead of writing into the local array.
    /// Returns an [`ArrayCollectiveReduceScatterIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `buffer` must be
    /// large enough to hold the PE's result segment.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(array.num_elems_local());
    /// let _result = unsafe { array.sum_scatter_into_buffer(0, array.num_elems_local(), buf.into()) }.block();
    ///```
    pub unsafe fn sum_scatter_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_scatter_into_buffer(ReduceOp::Sum, index, len, buffer);

        ArrayCollectiveReduceScatterIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBuffer(
                req,
            ),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Reduce-scatter max, placing each PE's result segment into caller-supplied `buffer`.
    ///
    /// Like [`max_scatter`](UnsafeArray::max_scatter) but places the received result segment into
    /// the caller-supplied [`LamellarBuffer`] instead of writing into the local array.
    /// Returns an [`ArrayCollectiveReduceScatterIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `buffer` must be
    /// large enough to hold the PE's result segment.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(array.num_elems_local());
    /// let _result = unsafe { array.max_scatter_into_buffer(0, array.num_elems_local(), buf.into()) }.block();
    ///```
    pub unsafe fn max_scatter_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_scatter_into_buffer(ReduceOp::Max, index, len, buffer);

        ArrayCollectiveReduceScatterIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBuffer(
                req,
            ),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Reduce-scatter min, placing each PE's result segment into caller-supplied `buffer`.
    ///
    /// Like [`min_scatter`](UnsafeArray::min_scatter) but places the received result segment into
    /// the caller-supplied [`LamellarBuffer`] instead of writing into the local array.
    /// Returns an [`ArrayCollectiveReduceScatterIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `buffer` must be
    /// large enough to hold the PE's result segment.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(array.num_elems_local());
    /// let _result = unsafe { array.min_scatter_into_buffer(0, array.num_elems_local(), buf.into()) }.block();
    ///```
    pub unsafe fn min_scatter_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_scatter_into_buffer(ReduceOp::Min, index, len, buffer);

        ArrayCollectiveReduceScatterIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBuffer(
                req,
            ),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Reduce-scatter product, placing each PE's result segment into caller-supplied `buffer`.
    ///
    /// Like [`prod_scatter`](UnsafeArray::prod_scatter) but places the received result segment into
    /// the caller-supplied [`LamellarBuffer`] instead of writing into the local array.
    /// Returns an [`ArrayCollectiveReduceScatterIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `buffer` must be
    /// large enough to hold the PE's result segment.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(array.num_elems_local());
    /// let _result = unsafe { array.prod_scatter_into_buffer(0, array.num_elems_local(), buf.into()) }.block();
    ///```
    pub unsafe fn prod_scatter_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_scatter_into_buffer(ReduceOp::Prod, index, len, buffer);

        ArrayCollectiveReduceScatterIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBuffer(
                req,
            ),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Reduce-scatter bitwise AND, placing each PE's result segment into caller-supplied `buffer`.
    ///
    /// Like [`bit_and_scatter`](UnsafeArray::bit_and_scatter) but places the received result segment into
    /// the caller-supplied [`LamellarBuffer`] instead of writing into the local array.
    /// Returns an [`ArrayCollectiveReduceScatterIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `buffer` must be
    /// large enough to hold the PE's result segment.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(array.num_elems_local());
    /// let _result = unsafe { array.bit_and_scatter_into_buffer(0, array.num_elems_local(), buf.into()) }.block();
    ///```
    pub unsafe fn bit_and_scatter_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_scatter_into_buffer(ReduceOp::BitAnd, index, len, buffer);

        ArrayCollectiveReduceScatterIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBuffer(
                req,
            ),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Reduce-scatter bitwise XOR, placing each PE's result segment into caller-supplied `buffer`.
    ///
    /// Like [`bit_xor_scatter`](UnsafeArray::bit_xor_scatter) but places the received result segment into
    /// the caller-supplied [`LamellarBuffer`] instead of writing into the local array.
    /// Returns an [`ArrayCollectiveReduceScatterIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `buffer` must be
    /// large enough to hold the PE's result segment.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(array.num_elems_local());
    /// let _result = unsafe { array.bit_xor_scatter_into_buffer(0, array.num_elems_local(), buf.into()) }.block();
    ///```
    pub unsafe fn bit_xor_scatter_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_scatter_into_buffer(ReduceOp::BitXor, index, len, buffer);

        ArrayCollectiveReduceScatterIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBuffer(
                req,
            ),
            spawned: false,
        }
    }

    #[doc(alias("Collective", "collective"))]
    /// Reduce-scatter bitwise OR, placing each PE's result segment into caller-supplied `buffer`.
    ///
    /// Like [`bit_or_scatter`](UnsafeArray::bit_or_scatter) but places the received result segment into
    /// the caller-supplied [`LamellarBuffer`] instead of writing into the local array.
    /// Returns an [`ArrayCollectiveReduceScatterIntoBufferHandle`] that must be `spawn()`ed or `block()`ed.
    ///
    /// # Safety
    /// `UnsafeArray` does not enforce mutual exclusion; concurrent access to the same elements is
    /// undefined behavior. `index + len` must not exceed the local segment length. `buffer` must be
    /// large enough to hold the PE's result segment.
    ///
    /// # Collective Operation
    /// All PEs in the team must call this function.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world, world.num_pes(), Distribution::Block).block();
    /// world.barrier();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(array.num_elems_local());
    /// let _result = unsafe { array.bit_or_scatter_into_buffer(0, array.num_elems_local(), buf.into()) }.block();
    ///```
    pub unsafe fn bit_or_scatter_into_buffer<B: AsLamellarBuffer<T>>(
        &self,
        index: usize,
        len: usize,
        buffer: LamellarBuffer<T, B>,
    ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        let req = self
            .inner
            .data
            .mem_region
            .as_base::<T>()
            .reduce_scatter_into_buffer(ReduceOp::BitOr, index, len, buffer);

        ArrayCollectiveReduceScatterIntoBufferHandle {
            array: self.as_lamellar_byte_array(),
            state: ArrayCollectiveReduceScatterIntoBufferState::CollectiveReduceScatterIntoBuffer(
                req,
            ),
            spawned: false,
        }
    }
}
