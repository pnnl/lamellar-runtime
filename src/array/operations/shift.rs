use crate::array::*;

use super::handle::{
    ArrayBatchOpHandle, ArrayFetchBatchOpHandle, ArrayFetchOpHandle, ArrayOpHandle,
};
/// Supertrait specifying elements of the array support remote Shift operations
/// - Left ```<<```
/// - Right ```>>```
pub trait ElementShiftOps: std::ops::ShlAssign + std::ops::ShrAssign + Dist + Sized {}

// #[doc(hidden)]
// impl<T> ElementShiftOps for T where T: std::ops::ShlAssign + std::ops::ShrAssign + Dist //+ AmDist,,,
// {
// }

#[doc(alias("One-sided", "onesided"))]
/// The interface for performing remote Shift operations on array elements
///
/// These operations can be performed using any [LamellarWriteArray] type
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
/// One type of batched operation can be performed
/// ## One Value - Many Indicies
/// In this type, the same value will be applied to the provided indices
///```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
/// let array = AtomicArray::<usize>::new(&world,100,Distribution::Block);
///
/// let indices = vec![3,54,12,88,29,68];
/// array.block_on(array.batch_fetch_shl(indices,2));
///```
pub trait ShiftOps<T: ElementShiftOps>: private::LamellarArrayPrivate<T> {
    /// This call performs an in place left shift of `val` bits on the element specified by `index`.
    ///
    /// A future is returned as the result of this call, which is used to detect when the operation has completed.
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
    /// let array = AtomicArray::<usize>::new(&world,4,Distribution::Block);
    ///
    /// let idx = 53;
    /// let val = 2;
    /// let req = array.shl(idx,val);
    /// array.block_on(req);
    ///```
    //#[tracing::instrument(skip_all)]
    fn shl(&self, index: usize, val: T) -> ArrayOpHandle {
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::Shl,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call performs a batched vesion of the [shl][ShiftOps::shl] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [ShiftOps] documentation for more information on batch operation input
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
    /// let array = AtomicArray::<usize>::new(&world,100,Distribution::Block);
    ///
    /// let indices = vec![3,54,12,88,29,68];
    /// let req = array.batch_shl(indices,3);
    /// array.block_on(req);
    ///```
    //#[tracing::instrument(skip_all)]
    fn batch_shl<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayBatchOpHandle {
        // self.inner_array().initiate_batch_op(val, index, ArrayOpCmd::Shl)
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::Shl,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call performs an in place left shift of `val` bits on the element specified by `index`, returning the old value
    ///
    /// A future is returned as the result of this call, which is used to retrieve
    /// the result after the (possibly remote) operation has finished.
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
    /// let array = AtomicArray::<usize>::new(&world,100,Distribution::Block);
    ///
    /// let idx = 53;
    /// let val = 2;
    /// let req = array.fetch_shl(idx,val);
    /// let old = array.block_on(req);
    ///```
    //#[tracing::instrument(skip_all)]
    fn fetch_shl(&self, index: usize, val: T) -> ArrayFetchOpHandle<T> {
        self.inner_array()
            .initiate_batch_fetch_op_2(
                val,
                index,
                ArrayOpCmd::FetchShl,
                self.as_lamellar_byte_array(),
            )
            .into()
    }

    /// This call performs a batched vesion of the [fetch_shl][ShiftOps::fetch_shl] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [ShiftOps] documentation for more information on batch operation input
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
    /// let array = AtomicArray::<usize>::new(&world,100,Distribution::Block);
    ///
    /// let indices = vec![3,54,12,88,29,68];
    /// let req = array.batch_fetch_shl(indices,10);
    /// let old_vals = array.block_on(req);
    ///```
    //#[tracing::instrument(skip_all)]
    fn batch_fetch_shl<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayFetchBatchOpHandle<T> {
        self.inner_array().initiate_batch_fetch_op_2(
            val,
            index,
            ArrayOpCmd::FetchShl,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call performs an in place right shift of `val` bits on the element specified by `index`.
    ///
    /// A future is returned as the result of this call, which is used to detect when the operation has completed.
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
    /// let array = AtomicArray::<usize>::new(&world,4,Distribution::Block);
    ///
    /// let idx = 53;
    /// let val = 2;
    /// let req = array.shl(idx,val);
    /// array.block_on(req);
    ///```
    //#[tracing::instrument(skip_all)]
    fn shr<'a>(&self, index: usize, val: T) -> ArrayOpHandle {
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::Shr,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call performs a batched vesion of the [shl][ShiftOps::shl] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [ShiftOps] documentation for more information on batch operation input
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
    /// let array = AtomicArray::<usize>::new(&world,100,Distribution::Block);
    ///
    /// let indices = vec![3,54,12,88,29,68];
    /// let req = array.batch_shr(indices,3);
    /// array.block_on(req);
    ///```
    //#[tracing::instrument(skip_all)]
    fn batch_shr<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayBatchOpHandle {
        // self.inner_array().initiate_batch_op(val, index, ArrayOpCmd::Shr)
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::Shr,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call performs an in place right shift of `val` bits on the element specified by `index`, returning the old value
    ///
    /// A future is returned as the result of this call, which is used to retrieve
    /// the result after the (possibly remote) operation has finished.
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
    /// let array = AtomicArray::<usize>::new(&world,100,Distribution::Block);
    ///
    /// let idx = 53;
    /// let val = 2;
    /// let req = array.fetch_shl(idx,val);
    /// let old = array.block_on(req);
    ///```
    //#[tracing::instrument(skip_all)]
    fn fetch_shr<'a>(&self, index: usize, val: T) -> ArrayFetchOpHandle<T> {
        self.inner_array()
            .initiate_batch_fetch_op_2(
                val,
                index,
                ArrayOpCmd::FetchShr,
                self.as_lamellar_byte_array(),
            )
            .into()
    }

    /// This call performs a batched vesion of the [fetch_shr][ShiftOps::fetch_shr] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [ShiftOps] documentation for more information on batch operation input
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
    /// let array = AtomicArray::<usize>::new(&world,100,Distribution::Block);
    ///
    /// let indices = vec![3,54,12,88,29,68];
    /// let req = array.batch_fetch_shr(indices,10);
    /// let old_vals = array.block_on(req);
    ///```
    //#[tracing::instrument(skip_all)]
    fn batch_fetch_shr<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayFetchBatchOpHandle<T> {
        self.inner_array().initiate_batch_fetch_op_2(
            val,
            index,
            ArrayOpCmd::FetchShr,
            self.as_lamellar_byte_array(),
        )
    }
}

#[doc(hidden)]
pub trait LocalShiftOps<T: Dist + ElementShiftOps> {
    fn local_shl(&self, index: usize, val: T) {
        self.local_fetch_shl(index, val);
    }
    fn local_fetch_shl(&self, index: usize, val: T) -> T;
    fn local_shr(&self, index: usize, val: T) {
        self.local_fetch_shr(index, val);
    }
    fn local_fetch_shr(&self, index: usize, val: T) -> T;
}
