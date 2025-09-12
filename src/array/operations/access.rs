use crate::array::*;

use super::handle::{
    ArrayBatchOpHandle, ArrayFetchBatchOpHandle, ArrayFetchOpHandle, ArrayOpHandle,
};

#[doc(alias("One-sided", "onesided"))]
/// The interface for remotely writing elements
///
/// These operations can be performed using any 'safe' [LamellarWriteArray]  type
/// For UnsafeArrays please see [UnsafeAccessOps]
///
/// Both single element operations and batched element operations are provided
///
/// Generally if you are performing a large number of operations it will be better to
/// use a batched version instead of multiple single element opertations. While the
/// Runtime internally performs message aggregation for both single element and batched
/// operations, single element operates have to be treated as individual requests, resulting
/// in allocation and bookkeeping overheads. A single batched call on the other hand is treated
/// as a single request by the runtime. (See [ReadOnlyOps] for an example comparing single vs batched load operations of a list of indices)
///
/// The results of a batched operation are returned to the user in the same order as the input indices.
///
/// # One-sided Operation
/// performing either single or batched operations are both one-sided, with the calling PE performing any necessary work to
/// initate and execute active messages that are sent to remote PEs.
/// For Ops that return results, the result will only be available on the calling PE.
///
/// # Note
/// For both single index and batched operations there are no guarantees to the order in which individual operations occur (an individal operation is guaranteed to be atomic though).
///
/// # Batched Types
/// Three types of batched operations can be performed
/// ## One Value - Many Indicies
/// In this type, the same value will be applied to the provided indices
///```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
/// let array = AtomicArray::<usize>::new(&world,100,Distribution::Block).block();
///
/// let indices = vec![3,54,12,88,29,68];
/// let val = 10;
/// array.batch_store(indices,val).block();
///```
/// ## Many Values - One Index
/// In this type, multiple values will be applied to the given index
///```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
/// let array = AtomicArray::<usize>::new(&world,100,Distribution::Block).block();
///
/// let vals = vec![3,54,12,88,29,68];
/// let index = 10;
/// array.batch_store(index,vals).block();
///```
/// ## Many Values - Many Indicies
/// In this type, values and indices have a one-to-one correspondance.
///
/// If the two lists are unequal in length, the longer of the two will be truncated so that it matches the length of the shorter
///```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
/// let array = AtomicArray::<usize>::new(&world,100,Distribution::Block).block();
///
/// let indices = vec![3,54,12,88,29,68];
/// let vals = vec![12,2,1,10000,12,13];
/// array.batch_store(indices,vals).block();
///```
pub trait AccessOps<T: ElementOps>: private::LamellarArrayPrivate<T> {
    /// This call stores the supplied `val` into the element specified by `index`
    ///
    /// A future is returned as the result of this call, which is used to detect when the operation has completed
    ///
    /// # Note
    /// This future is only lazy with respect to checking for completion, not
    /// with respect to launching the operation. That is, the operation will
    /// occur regardless of if the future is ever polled or not, Enabling
    /// a "fire and forget" programming model.
    ///
    /// # Examples
    ///
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array = AtomicArray::<usize>::new(&world,100,Distribution::Block).block();
    ///
    /// let idx = 53;
    /// let val = 10;
    /// let req = array.store(idx,val);
    /// req.block();
    ///```
    #[tracing::instrument(skip_all, level = "debug")]
    fn store<'a>(&self, index: usize, val: T) -> ArrayOpHandle<T> {
        self.inner_array()
            .initiate_op(val, index, ArrayOpCmd::Store, self.as_lamellar_byte_array())
            .into()
    }

    /// This call performs a batched vesion of the [store][AccessOps::store] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [AccessOps] documentation for more information on batch operation input
    ///
    /// A future is returned as the result of this call, which is used to detect when the operation has completed
    ///
    /// # Note
    /// This future is only lazy with respect to checking for completion, not
    /// with respect to launching the operation. That is, the operation will
    /// occur regardless of if the future is ever polled or not, Enabling
    /// a "fire and forget" programming model.
    ///
    /// # Examples
    ///
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array = AtomicArray::<usize>::new(&world,100,Distribution::Block).block();
    ///
    /// let indices = vec![3,54,12,88,29,68];
    /// let req = array.batch_store(indices,10);
    /// req.block();
    ///```
    #[tracing::instrument(skip_all, level = "debug")]
    fn batch_store<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayBatchOpHandle<T> {
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::Store,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call swaps the supplied `val` into the element specified by `index`, returning the old value
    ///
    /// A future is returned as the result of this call, which is used to retrieve
    /// the results after the (possibly remote) operations have finished.
    ///
    /// # Note
    /// This future is only lazy with respect to retrieving the result, not
    /// with respect to launching the operation. That is, the operation will
    /// occur regardless of if the future is ever polled or not, Enabling
    /// a "fire and forget" programming model.
    ///
    /// # Examples
    ///
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array = AtomicArray::<usize>::new(&world,100,Distribution::Block).block();
    ///
    /// let idx = 53;
    /// let new = 10;
    /// let req = array.swap(idx,new);
    /// let old = req.block();
    ///```
    #[tracing::instrument(skip_all, level = "debug")]
    fn swap<'a>(&self, index: usize, val: T) -> ArrayFetchOpHandle<T> {
        self.inner_array()
            .initiate_batch_fetch_op_2(val, index, ArrayOpCmd::Swap, self.as_lamellar_byte_array())
            .into()
    }

    /// This call performs a batched vesion of the [swap][AccessOps::swap] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [AccessOps] documentation for more information on batch operation input
    ///
    /// A future is returned as the result of this call, which is used to retrieve
    /// the results after the (possibly remote) operations have finished.
    ///
    /// # Note
    /// This future is only lazy with respect to checking for completion, not
    /// with respect to launching the operation. That is, the operation will
    /// occur regardless of if the future is ever polled or not, Enabling
    /// a "fire and forget" programming model.
    ///
    /// # Examples
    ///
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array = AtomicArray::<usize>::new(&world,100,Distribution::Block).block();
    ///
    /// let indices = vec![3,54,12,88,29,68];
    /// let req = array.batch_swap(indices,10);
    /// let old_vals = req.block();
    ///```
    #[tracing::instrument(skip_all, level = "debug")]
    fn batch_swap<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayFetchBatchOpHandle<T> {
        self.inner_array().initiate_batch_fetch_op_2(
            val,
            index,
            ArrayOpCmd::Swap,
            self.as_lamellar_byte_array(),
        )
    }
}

#[doc(alias("One-sided", "onesided"))]
/// The interface for remotely writing elements on [UnsafeArray]s
///
/// Both single element operations and batched element operations are provided
///
/// Generally if you are performing a large number of operations it will be better to
/// use a batched version instead of multiple single element opertations. While the
/// Runtime internally performs message aggregation for both single element and batched
/// operations, single element operates have to be treated as individual requests, resulting
/// in allocation and bookkeeping overheads. A single batched call on the other hand is treated
/// as a single request by the runtime. (See [ReadOnlyOps] for an example comparing single vs batched load operations of a list of indices)
///
/// The results of a batched operation are returned to the user in the same order as the input indices.
///
/// # One-sided Operation
/// performing either single or batched operations are both one-sided, with the calling PE performing any necessary work to
/// initate and execute active messages that are sent to remote PEs.
/// For Ops that return results, the result will only be available on the calling PE.
///
/// # Note
/// For both single index and batched operations there are no guarantees to the order in which individual operations occur.
///
/// # Batched Types
/// Three types of batched operations can be performed
/// ## One Value - Many Indicies
/// In this type, the same value will be applied to the provided indices
///```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
/// let array = UnsafeArray::<usize>::new(&world,100,Distribution::Block).block();
///
/// let indices = vec![3,54,12,88,29,68];
/// let val = 10;
/// unsafe{array.batch_store(indices,val).block()};
///```
/// ## Many Values - One Index
/// In this type, multiple values will be applied to the given index
///```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
/// let array = UnsafeArray::<usize>::new(&world,100,Distribution::Block).block();
///
/// let vals = vec![3,54,12,88,29,68];
/// let index = 10;
/// unsafe{array.batch_store(index,vals).block()};
///```
/// ## Many Values - Many Indicies
/// In this type, values and indices have a one-to-one correspondance.
///
/// If the two lists are unequal in length, the longer of the two will be truncated so that it matches the length of the shorter
///```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
/// let array = UnsafeArray::<usize>::new(&world,100,Distribution::Block).block();
///
/// let indices = vec![3,54,12,88,29,68];
/// let vals = vec![12,2,1,10000,12,13];
/// unsafe{array.batch_store(indices,vals).block()};
///```
pub trait UnsafeAccessOps<T: ElementOps>: private::LamellarArrayPrivate<T> {
    /// This call stores the supplied `val` into the element specified by `index`
    ///
    /// A future is returned as the result of this call, which is used to detect when the operation has completed
    ///
    /// # Note
    /// This future is only lazy with respect to checking for completion, not
    /// with respect to launching the operation. That is, the operation will
    /// occur regardless of if the future is ever polled or not, Enabling
    /// a "fire and forget" programming model.
    ///
    /// # Examples
    ///
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array = UnsafeArray::<usize>::new(&world,100,Distribution::Block).block();
    ///
    /// let idx = 53;
    /// let val = 10;
    /// let req = unsafe{array.store(idx,val)};
    /// req.block();
    ///```
    #[tracing::instrument(skip_all, level = "debug")]
    unsafe fn store<'a>(&self, index: usize, val: T) -> ArrayOpHandle<T> {
        self.inner_array()
            .initiate_op(val, index, ArrayOpCmd::Store, self.as_lamellar_byte_array())
            .into()
    }

    /// This call performs a batched vesion of the [store][AccessOps::store] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [AccessOps] documentation for more information on batch operation input
    ///
    /// A future is returned as the result of this call, which is used to detect when the operation has completed
    ///
    /// # Note
    /// This future is only lazy with respect to checking for completion, not
    /// with respect to launching the operation. That is, the operation will
    /// occur regardless of if the future is ever polled or not, Enabling
    /// a "fire and forget" programming model.
    ///
    /// # Examples
    ///
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array = UnsafeArray::<usize>::new(&world,100,Distribution::Block).block();
    ///
    /// let indices = vec![3,54,12,88,29,68];
    /// let req = unsafe{array.batch_store(indices,10)};
    /// req.block();
    ///```
    #[tracing::instrument(skip_all, level = "debug")]
    unsafe fn batch_store<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayBatchOpHandle<T> {
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::Store,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call swaps the supplied `val` into the element specified by `index`, returning the old value
    ///
    /// A future is returned as the result of this call, which is used to retrieve
    /// the results after the (possibly remote) operations have finished.
    ///
    /// # Note
    /// This future is only lazy with respect to retrieving the result, not
    /// with respect to launching the operation. That is, the operation will
    /// occur regardless of if the future is ever polled or not, Enabling
    /// a "fire and forget" programming model.
    ///
    /// # Examples
    ///
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array = UnsafeArray::<usize>::new(&world,100,Distribution::Block).block();
    ///
    /// let idx = 53;
    /// let new = 10;
    /// let req = unsafe{array.swap(idx,new)};
    /// let old = req.block();
    ///```
    #[tracing::instrument(skip_all, level = "debug")]
    unsafe fn swap<'a>(&self, index: usize, val: T) -> ArrayFetchOpHandle<T> {
        self.inner_array()
            .initiate_batch_fetch_op_2(val, index, ArrayOpCmd::Swap, self.as_lamellar_byte_array())
            .into()
    }

    /// This call performs a batched vesion of the [swap][AccessOps::swap] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [AccessOps] documentation for more information on batch operation input
    ///
    /// A future is returned as the result of this call, which is used to retrieve
    /// the results after the (possibly remote) operations have finished.
    ///
    /// # Note
    /// This future is only lazy with respect to checking for completion, not
    /// with respect to launching the operation. That is, the operation will
    /// occur regardless of if the future is ever polled or not, Enabling
    /// a "fire and forget" programming model.
    ///
    /// # Examples
    ///
    ///```
    /// use lamellar::array::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let array = UnsafeArray::<usize>::new(&world,100,Distribution::Block).block();
    ///
    /// let indices = vec![3,54,12,88,29,68];
    /// let req = unsafe{array.batch_swap(indices,10)};
    /// let old_vals = req.block();
    ///```
    #[tracing::instrument(skip_all, level = "debug")]
    unsafe fn batch_swap<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayFetchBatchOpHandle<T> {
        self.inner_array().initiate_batch_fetch_op_2(
            val,
            index,
            ArrayOpCmd::Swap,
            self.as_lamellar_byte_array(),
        )
    }
}

#[doc(hidden)]
pub trait LocalAccessOps<T: Dist + ElementOps> {
    fn local_store(&mut self, idx_vals: impl Iterator<Item = (usize, T)>);
    fn local_swap(&mut self, idx_vals: impl Iterator<Item = (usize, T)>) -> Vec<T>;
}

impl<T: Dist + ElementOps> LocalAccessOps<T> for LamellarMutLocalData<'_, T> {
    fn local_store(&mut self, idx_vals: impl Iterator<Item = (usize, T)>) {
        match self {
            LamellarMutLocalData::Slice(data) => data.local_store(idx_vals),
            LamellarMutLocalData::LocalLock(ref mut data) => {
                let mut slice: &mut [T] = &mut *data;
                slice.local_store(idx_vals)
            }
            LamellarMutLocalData::GlobalLock(ref mut data) => {
                let mut slice: &mut [T] = &mut *data;
                slice.local_store(idx_vals)
            }
            LamellarMutLocalData::NativeAtomic(ref mut data) => data.local_store(idx_vals),
            LamellarMutLocalData::GenericAtomic(ref mut data) => data.local_store(idx_vals),
            LamellarMutLocalData::NetworkAtomic(ref mut data) => data.local_store(idx_vals),
        }
    }

    fn local_swap(&mut self, idx_vals: impl Iterator<Item = (usize, T)>) -> Vec<T> {
        match self {
            LamellarMutLocalData::Slice(data) => data.local_swap(idx_vals),
            LamellarMutLocalData::LocalLock(ref mut data) => {
                let mut slice: &mut [T] = &mut *data;
                slice.local_swap(idx_vals)
            }
            LamellarMutLocalData::GlobalLock(ref mut data) => {
                let mut slice: &mut [T] = &mut *data;
                slice.local_swap(idx_vals)
            }
            LamellarMutLocalData::NativeAtomic(ref mut data) => data.local_swap(idx_vals),
            LamellarMutLocalData::GenericAtomic(ref mut data) => data.local_swap(idx_vals),
            LamellarMutLocalData::NetworkAtomic(ref mut data) => data.local_swap(idx_vals),
        }
    }
}

impl<T: Dist + ElementOps> LocalAccessOps<T> for &mut [T] {
    fn local_store(&mut self, idx_vals: impl Iterator<Item = (usize, T)>) {
        for (idx, val) in idx_vals {
            self[idx] = val;
        }
    }

    fn local_swap(&mut self, idx_vals: impl Iterator<Item = (usize, T)>) -> Vec<T> {
        idx_vals
            .map(|(idx, val)| std::mem::replace(&mut self[idx], val))
            .collect()
    }
}
