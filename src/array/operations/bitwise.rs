use crate::array::*;

use super::handle::{
    ArrayBatchOpHandle, ArrayFetchBatchOpHandle, ArrayFetchOpHandle, ArrayOpHandle,
};
/// Supertrait specifying elements of the array support remote bitwise operations
/// - And ```&```
/// - Or ```|```
/// - Xor ```^```
pub trait ElementBitWiseOps:
    std::ops::BitAndAssign + std::ops::BitOrAssign + std::ops::BitXorAssign + Dist + Sized
//+ AmDist
{
}

// //#[doc(hidden)]
// impl<T> ElementBitWiseOps for T where
//     T: std::ops::BitAndAssign + std::ops::BitOrAssign + std::ops::BitXorAssign + Dist //+ AmDist
// {
// }

#[doc(alias("One-sided", "onesided"))]
/// The interface for performing remote bitwise operations on array elements
///
/// These operations can be performed using any safe [LamellarWriteArray] type
/// for UnsafeArrays please see [UnsafeBitWiseOps]
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
/// let val = 0b100101001;
/// array.block_on(array.batch_fetch_bit_and(indices,val));
///```
/// ## Many Values - One Index
/// In this type, multiple values will be applied to the given index
///```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
/// let array = AtomicArray::<usize>::new(&world,100,Distribution::Block).block();
///
/// let vals = vec![0x3,0x54,0b11101,88,29,0x68];
/// let index = 10;
/// array.block_on(array.batch_bit_or(index,vals));
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
/// let vals = vec![0x12,2,1,0b10000,12,0x13];
/// array.block_on(array.batch_fetch_bit_or(indices,vals));
///```
pub trait BitWiseOps<T: ElementBitWiseOps>: private::LamellarArrayPrivate<T> {
    /// This call performs a bitwise `and` with the element specified by `index` and the supplied `val`.
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
    /// let array = AtomicArray::<usize>::new(&world,100,Distribution::Block).block();
    ///
    /// let idx = 53;
    /// let val = 0b100101001;
    /// let req = array.bit_and(idx,val);
    /// array.block_on(req);
    ///```
    //#[tracing::instrument(skip_all)]
    fn bit_and<'a>(&self, index: usize, val: T) -> ArrayOpHandle {
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::And,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call performs a batched vesion of the [bit_and][BitWiseOps::bit_and] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [BitWiseOps] documentation for more information on batch operation input
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
    /// let req = array.batch_bit_and(indices,10);
    /// array.block_on(req);
    ///```
    //#[tracing::instrument(skip_all)]
    fn batch_bit_and<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayBatchOpHandle {
        // self.inner_array().initiate_op(val, index, ArrayOpCmd::And)
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::And,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call performs a bitwise `and` with the element specified by `index` and the supplied `val`, returning the old value
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
    /// let array = AtomicArray::<usize>::new(&world,100,Distribution::Block).block();
    ///
    /// let idx = 53;
    /// let val = 10;
    /// let req = array.fetch_bit_and(idx,val);
    /// let old = array.block_on(req);
    ///```
    //#[tracing::instrument(skip_all)]
    fn fetch_bit_and<'a>(&self, index: usize, val: T) -> ArrayFetchOpHandle<T> {
        self.inner_array()
            .initiate_batch_fetch_op_2(
                val,
                index,
                ArrayOpCmd::FetchAnd,
                self.as_lamellar_byte_array(),
            )
            .into()
    }

    /// This call performs a batched vesion of the [fetch_bit_and][BitWiseOps::fetch_bit_and] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [BitWiseOps] documentation for more information on batch operation input
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
    /// let req = array.batch_fetch_bit_and(indices,10);
    /// let old_vals = array.block_on(req);
    ///```
    //#[tracing::instrument(skip_all)]
    fn batch_fetch_bit_and<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayFetchBatchOpHandle<T> {
        self.inner_array().initiate_batch_fetch_op_2(
            val,
            index,
            ArrayOpCmd::FetchAnd,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call performs a bitwise `or` with the element specified by `index` and the supplied `val`.
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
    /// let array = AtomicArray::<usize>::new(&world,100,Distribution::Block).block();
    ///
    /// let idx = 53;
    /// let val = 0b100101001;
    /// let req = array.bit_or(idx,val);
    /// array.block_on(req);
    ///```
    //#[tracing::instrument(skip_all)]
    fn bit_or<'a>(&self, index: usize, val: T) -> ArrayOpHandle {
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::Or,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call performs a batched vesion of the [bit_or][BitWiseOps::bit_or] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [BitWiseOps] documentation for more information on batch operation input
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
    /// let req = array.batch_bit_or(indices,10);
    /// array.block_on(req);
    ///```
    //#[tracing::instrument(skip_all)]
    fn batch_bit_or<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayBatchOpHandle {
        // self.inner_array().initiate_op(val, index, ArrayOpCmd::Or)
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::Or,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call performs a bitwise `or` with the element specified by `index` and the supplied `val`, returning the old value
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
    /// let array = AtomicArray::<usize>::new(&world,100,Distribution::Block).block();
    ///
    /// let idx = 53;
    /// let val = 10;
    /// let req = array.fetch_bit_or(idx,val);
    /// let old = array.block_on(req);
    ///```
    //#[tracing::instrument(skip_all)]
    fn fetch_bit_or<'a>(&self, index: usize, val: T) -> ArrayFetchOpHandle<T> {
        self.inner_array()
            .initiate_batch_fetch_op_2(
                val,
                index,
                ArrayOpCmd::FetchOr,
                self.as_lamellar_byte_array(),
            )
            .into()
    }

    /// This call performs a batched vesion of the [fetch_bit_or][BitWiseOps::fetch_bit_or] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [BitWiseOps] documentation for more information on batch operation input
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
    /// let req = array.batch_fetch_bit_or(indices,10);
    /// let old_vals = array.block_on(req);
    ///```
    //#[tracing::instrument(skip_all)]
    fn batch_fetch_bit_or<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayFetchBatchOpHandle<T> {
        self.inner_array().initiate_batch_fetch_op_2(
            val,
            index,
            ArrayOpCmd::FetchOr,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call performs a bitwise `xor` with the element specified by `index` and the supplied `val`.
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
    /// let array = AtomicArray::<usize>::new(&world,100,Distribution::Block).block();
    ///
    /// let idx = 53;
    /// let val = 0b100101001;
    /// let req = array.bit_xor(idx,val);
    /// array.block_on(req);
    ///```
    //#[tracing::instrument(skip_all)]
    fn bit_xor<'a>(&self, index: usize, val: T) -> ArrayOpHandle {
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::Xor,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call performs a batched vesion of the [bit_xor][BitWiseOps::bit_xor] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [BitWiseOps] documentation for more information on batch operation input
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
    /// let req = array.batch_bit_xor(indices,10);
    /// array.block_on(req);
    ///```
    //#[tracing::instrument(skip_all)]
    fn batch_bit_xor<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayBatchOpHandle {
        // self.inner_array().initiate_op(val, index, ArrayOpCmd::Xor)
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::Xor,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call performs a bitwise `xor` with the element specified by `index` and the supplied `val`, returning the old value
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
    /// let array = AtomicArray::<usize>::new(&world,100,Distribution::Block).block();
    ///
    /// let idx = 53;
    /// let val = 10;
    /// let req = array.fetch_bit_xor(idx,val);
    /// let old = array.block_on(req);
    ///```
    //#[tracing::instrument(skip_all)]
    fn fetch_bit_xor<'a>(&self, index: usize, val: T) -> ArrayFetchOpHandle<T> {
        self.inner_array()
            .initiate_batch_fetch_op_2(
                val,
                index,
                ArrayOpCmd::FetchXor,
                self.as_lamellar_byte_array(),
            )
            .into()
    }

    /// This call performs a batched vesion of the [fetch_bit_xor][BitWiseOps::fetch_bit_xor] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [BitWiseOps] documentation for more information on batch operation input
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
    /// let req = array.batch_fetch_bit_xor(indices,10);
    /// let old_vals = array.block_on(req);
    ///```
    //#[tracing::instrument(skip_all)]
    fn batch_fetch_bit_xor<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayFetchBatchOpHandle<T> {
        self.inner_array().initiate_batch_fetch_op_2(
            val,
            index,
            ArrayOpCmd::FetchXor,
            self.as_lamellar_byte_array(),
        )
    }
}

#[doc(alias("One-sided", "onesided"))]
/// The interface for performing remote bitwise operations on [UnsafeArray] elements
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
/// let val = 0b100101001;
/// array.block_on(unsafe{array.batch_fetch_bit_and(indices,val)});
///```
/// ## Many Values - One Index
/// In this type, multiple values will be applied to the given index
///```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
/// let array = UnsafeArray::<usize>::new(&world,100,Distribution::Block).block();
///
/// let vals = vec![0x3,0x54,0b11101,88,29,0x68];
/// let index = 10;
/// array.block_on(unsafe{array.batch_bit_or(index,vals)});
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
/// let vals = vec![0x12,2,1,0b10000,12,0x13];
/// array.block_on(unsafe{array.batch_fetch_bit_or(indices,vals)});
///```
pub trait UnsafeBitWiseOps<T: ElementBitWiseOps>: private::LamellarArrayPrivate<T> {
    /// This call performs a bitwise `and` with the element specified by `index` and the supplied `val`.
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
    /// let array = UnsafeArray::<usize>::new(&world,100,Distribution::Block).block();
    ///
    /// let idx = 53;
    /// let val = 0b100101001;
    /// let req = unsafe{ array.bit_and(idx,val)};
    /// array.block_on(req);
    ///```
    //#[tracing::instrument(skip_all)]
    unsafe fn bit_and<'a>(&self, index: usize, val: T) -> ArrayOpHandle {
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::And,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call performs a batched vesion of the [bit_and][BitWiseOps::bit_and] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [BitWiseOps] documentation for more information on batch operation input
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
    /// let req = unsafe{ array.batch_bit_and(indices,10)};
    /// array.block_on(req);
    ///```
    //#[tracing::instrument(skip_all)]
    unsafe fn batch_bit_and<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayBatchOpHandle {
        // self.inner_array().initiate_op(val, index, ArrayOpCmd::And)
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::And,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call performs a bitwise `and` with the element specified by `index` and the supplied `val`, returning the old value
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
    /// let array = UnsafeArray::<usize>::new(&world,100,Distribution::Block).block();
    ///
    /// let idx = 53;
    /// let val = 10;
    /// let req = unsafe{ array.fetch_bit_and(idx,val)};
    /// let old = array.block_on(req);
    ///```
    //#[tracing::instrument(skip_all)]
    unsafe fn fetch_bit_and<'a>(&self, index: usize, val: T) -> ArrayFetchOpHandle<T> {
        self.inner_array()
            .initiate_batch_fetch_op_2(
                val,
                index,
                ArrayOpCmd::FetchAnd,
                self.as_lamellar_byte_array(),
            )
            .into()
    }

    /// This call performs a batched vesion of the [fetch_bit_and][BitWiseOps::fetch_bit_and] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [BitWiseOps] documentation for more information on batch operation input
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
    /// let req = unsafe{ array.batch_fetch_bit_and(indices,10)};
    /// let old_vals = array.block_on(req);
    ///```
    //#[tracing::instrument(skip_all)]
    unsafe fn batch_fetch_bit_and<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayFetchBatchOpHandle<T> {
        self.inner_array().initiate_batch_fetch_op_2(
            val,
            index,
            ArrayOpCmd::FetchAnd,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call performs a bitwise `or` with the element specified by `index` and the supplied `val`.
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
    /// let array = UnsafeArray::<usize>::new(&world,100,Distribution::Block).block();
    ///
    /// let idx = 53;
    /// let val = 0b100101001;
    /// let req = unsafe{ array.bit_or(idx,val)};
    /// array.block_on(req);
    ///```
    //#[tracing::instrument(skip_all)]
    unsafe fn bit_or<'a>(&self, index: usize, val: T) -> ArrayOpHandle {
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::Or,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call performs a batched vesion of the [bit_or][BitWiseOps::bit_or] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [BitWiseOps] documentation for more information on batch operation input
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
    /// let req = unsafe{ array.batch_bit_or(indices,10)};
    /// array.block_on(req);
    ///```
    //#[tracing::instrument(skip_all)]
    unsafe fn batch_bit_or<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayBatchOpHandle {
        // self.inner_array().initiate_op(val, index, ArrayOpCmd::Or)
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::Or,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call performs a bitwise `or` with the element specified by `index` and the supplied `val`, returning the old value
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
    /// let array = UnsafeArray::<usize>::new(&world,100,Distribution::Block).block();
    ///
    /// let idx = 53;
    /// let val = 10;
    /// let req = unsafe{ array.fetch_bit_or(idx,val)};
    /// let old = array.block_on(req);
    ///```
    //#[tracing::instrument(skip_all)]
    unsafe fn fetch_bit_or<'a>(&self, index: usize, val: T) -> ArrayFetchOpHandle<T> {
        self.inner_array()
            .initiate_batch_fetch_op_2(
                val,
                index,
                ArrayOpCmd::FetchOr,
                self.as_lamellar_byte_array(),
            )
            .into()
    }

    /// This call performs a batched vesion of the [fetch_bit_or][BitWiseOps::fetch_bit_or] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [BitWiseOps] documentation for more information on batch operation input
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
    /// let req = unsafe{ array.batch_fetch_bit_or(indices,10)};
    /// let old_vals = array.block_on(req);
    ///```
    //#[tracing::instrument(skip_all)]
    unsafe fn batch_fetch_bit_or<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayFetchBatchOpHandle<T> {
        self.inner_array().initiate_batch_fetch_op_2(
            val,
            index,
            ArrayOpCmd::FetchOr,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call performs a bitwise `xor` with the element specified by `index` and the supplied `val`.
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
    /// let array = UnsafeArray::<usize>::new(&world,100,Distribution::Block).block();
    ///
    /// let idx = 53;
    /// let val = 0b100101001;
    /// let req = unsafe{ array.bit_xor(idx,val)};
    /// array.block_on(req);
    ///```
    //#[tracing::instrument(skip_all)]
    unsafe fn bit_xor<'a>(&self, index: usize, val: T) -> ArrayOpHandle {
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::Xor,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call performs a batched vesion of the [bit_xor][BitWiseOps::bit_xor] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [BitWiseOps] documentation for more information on batch operation input
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
    /// let req = unsafe{ array.batch_bit_xor(indices,10)};
    /// array.block_on(req);
    ///```
    //#[tracing::instrument(skip_all)]
    unsafe fn batch_bit_xor<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayBatchOpHandle {
        // self.inner_array().initiate_op(val, index, ArrayOpCmd::Xor)
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::Xor,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call performs a bitwise `xor` with the element specified by `index` and the supplied `val`, returning the old value
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
    /// let array = UnsafeArray::<usize>::new(&world,100,Distribution::Block).block();
    ///
    /// let idx = 53;
    /// let val = 10;
    /// let req = unsafe{ array.fetch_bit_xor(idx,val)};
    /// let old = array.block_on(req);
    ///```
    //#[tracing::instrument(skip_all)]
    unsafe fn fetch_bit_xor<'a>(&self, index: usize, val: T) -> ArrayFetchOpHandle<T> {
        self.inner_array()
            .initiate_batch_fetch_op_2(
                val,
                index,
                ArrayOpCmd::FetchXor,
                self.as_lamellar_byte_array(),
            )
            .into()
    }

    /// This call performs a batched vesion of the [fetch_bit_xor][BitWiseOps::fetch_bit_xor] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [BitWiseOps] documentation for more information on batch operation input
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
    /// let req = unsafe{ array.batch_fetch_bit_xor(indices,10)};
    /// let old_vals = array.block_on(req);
    ///```
    //#[tracing::instrument(skip_all)]
    unsafe fn batch_fetch_bit_xor<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayFetchBatchOpHandle<T> {
        self.inner_array().initiate_batch_fetch_op_2(
            val,
            index,
            ArrayOpCmd::FetchXor,
            self.as_lamellar_byte_array(),
        )
    }
}

#[doc(hidden)]
pub trait LocalBitWiseOps<T: Dist + ElementBitWiseOps> {
    fn local_bit_and(&self, index: usize, val: T) {
        self.local_fetch_bit_and(index, val);
    }
    fn local_fetch_bit_and(&self, index: usize, val: T) -> T;
    fn local_bit_or(&self, index: usize, val: T) {
        self.local_fetch_bit_or(index, val);
    }
    fn local_fetch_bit_or(&self, index: usize, val: T) -> T;
}
