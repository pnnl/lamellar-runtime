use crate::array::*;

use super::handle::{
    ArrayBatchOpHandle, ArrayFetchBatchOpHandle, ArrayFetchOpHandle, ArrayOpHandle,
};
/// Supertrait specifying elements of the array support remote arithmetic assign operations
/// - Addition ```+=```
/// - Subtraction ```-=```
/// - Multiplication ```*=```
/// - Division ```/=```
/// - Remainder ```%=```
pub trait ElementArithmeticOps:
    std::ops::AddAssign
    + std::ops::SubAssign
    + std::ops::MulAssign
    + std::ops::DivAssign
    + std::ops::RemAssign
    + Dist
    + Sized
{
}

// We dont want to auto derive this because we want to require that users
// use the #[AmData(ArrayOps(Arithmetic))] macro to derive it for them
// impl<T> ElementArithmeticOps for T where
//     T: std::ops::AddAssign
//         + std::ops::SubAssign
//         + std::ops::MulAssign
//         + std::ops::DivAssign
//         + std::ops::RemAssign
//         + Dist
// {
// }

#[doc(alias("One-sided", "onesided"))]
/// The interface for performing remote arithmetic operations on array elements
///
/// These operations can be performed using any safe [LamellarWriteArray] type
/// for UnsafeArrays please see [UnsafeArithmeticOps] instead.
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
/// array.batch_fetch_add(indices,val).block();
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
/// array.batch_sub(index,vals).block();
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
/// array.batch_fetch_mul(indices,vals).block();
///```
pub trait ArithmeticOps<T: Dist + ElementArithmeticOps>: private::LamellarArrayPrivate<T> {
    /// This call adds the supplied `val` into the element specified by `index`
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
    /// let req = array.add(idx,val);
    /// req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    fn add(&self, index: usize, val: T) -> ArrayOpHandle {
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::Add,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call performs a batched vesion of the [add][ArithmeticOps::add] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [ArithmeticOps] documentation for more information on batch operation input
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
    /// let req = array.batch_add(indices,10);
    /// req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    fn batch_add<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayBatchOpHandle {
        // self.inner_array().initiate_op(val, index, ArrayOpCmd::Add)
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::Add,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call adds the supplied `val` into the element specified by `index`, returning the old value
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
    /// let req = array.fetch_add(idx,val);
    /// let old = req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    fn fetch_add(&self, index: usize, val: T) -> ArrayFetchOpHandle<T> {
        self.inner_array()
            .initiate_batch_fetch_op_2(
                val,
                index,
                ArrayOpCmd::FetchAdd,
                self.as_lamellar_byte_array(),
            )
            .into()
    }

    /// This call performs a batched vesion of the [fetch_add][ArithmeticOps::fetch_add] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [ArithmeticOps] documentation for more information on batch operation input
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
    /// let req = array.batch_fetch_add(indices,10);
    /// let old_vals = req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    fn batch_fetch_add<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayFetchBatchOpHandle<T> {
        self.inner_array().initiate_batch_fetch_op_2(
            val,
            index,
            ArrayOpCmd::FetchAdd,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call subtracts the supplied `val` from the element specified by `index`
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
    /// let req = array.sub(idx,val);
    /// req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    fn sub<'a>(&self, index: usize, val: T) -> ArrayOpHandle {
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::Sub,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call performs a batched vesion of the [sub][ArithmeticOps::sub] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [ArithmeticOps] documentation for more information on batch operation input
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
    /// let req = array.batch_sub(indices,10);
    /// req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    fn batch_sub<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayBatchOpHandle {
        // self.inner_array().initiate_op(val, index, ArrayOpCmd::Sub)
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::Sub,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call subtracts the supplied `val` from the element specified by `index`, returning the old value
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
    /// let req = array.fetch_sub(idx,val);
    /// let old = req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    fn fetch_sub<'a>(&self, index: usize, val: T) -> ArrayFetchOpHandle<T> {
        self.inner_array()
            .initiate_batch_fetch_op_2(
                val,
                index,
                ArrayOpCmd::FetchSub,
                self.as_lamellar_byte_array(),
            )
            .into()
    }

    /// This call performs a batched vesion of the [fetch_sub][ArithmeticOps::fetch_sub] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [ArithmeticOps] documentation for more information on batch operation input
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
    /// let req = array.batch_fetch_sub(indices,10);
    /// let old_vals = req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    fn batch_fetch_sub<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayFetchBatchOpHandle<T> {
        self.inner_array().initiate_batch_fetch_op_2(
            val,
            index,
            ArrayOpCmd::FetchSub,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call multiplies the supplied `val` by the element specified by `index` and stores the result.
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
    /// let req = array.mul(idx,val);
    /// req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    fn mul<'a>(&self, index: usize, val: T) -> ArrayOpHandle {
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::Mul,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call performs a batched vesion of the [mul][ArithmeticOps::mul] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [ArithmeticOps] documentation for more information on batch operation input
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
    /// let req = array.batch_mul(indices,10);
    /// req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    fn batch_mul<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayBatchOpHandle {
        // self.inner_array().initiate_op(val, index, ArrayOpCmd::Mul)
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::Mul,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call multiplies the supplied `val` with the element specified by `index`, returning the old value
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
    /// let req = array.fetch_mul(idx,val);
    /// let old = req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    fn fetch_mul<'a>(&self, index: usize, val: T) -> ArrayFetchOpHandle<T> {
        self.inner_array()
            .initiate_batch_fetch_op_2(
                val,
                index,
                ArrayOpCmd::FetchMul,
                self.as_lamellar_byte_array(),
            )
            .into()
    }

    /// This call performs a batched vesion of the [fetch_mul][ArithmeticOps::fetch_mul] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [ArithmeticOps] documentation for more information on batch operation input
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
    /// let req = array.batch_fetch_mul(indices,10);
    /// let old_vals = req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    fn batch_fetch_mul<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayFetchBatchOpHandle<T> {
        self.inner_array().initiate_batch_fetch_op_2(
            val,
            index,
            ArrayOpCmd::FetchMul,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call divides the element specified by `index` with the supplied `val` and stores the result
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
    /// let req = array.div(idx,val);
    /// req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    fn div<'a>(&self, index: usize, val: T) -> ArrayOpHandle {
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::Div,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call performs a batched vesion of the [div][ArithmeticOps::div] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [ArithmeticOps] documentation for more information on batch operation input
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
    /// let req = array.batch_div(indices,10);
    /// req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    fn batch_div<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayBatchOpHandle {
        // self.inner_array().initiate_op(val, index, ArrayOpCmd::Div)
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::Div,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call divides the element specified by `index` with the supplied `val`, returning the old value
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
    /// let req = array.fetch_div(idx,val);
    /// let old = req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    fn fetch_div<'a>(&self, index: usize, val: T) -> ArrayFetchOpHandle<T> {
        self.inner_array()
            .initiate_batch_fetch_op_2(
                val,
                index,
                ArrayOpCmd::FetchDiv,
                self.as_lamellar_byte_array(),
            )
            .into()
    }

    /// This call performs a batched vesion of the [fetch_div][ArithmeticOps::fetch_div] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [ArithmeticOps] documentation for more information on batch operation input
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
    /// let req = array.batch_fetch_div(indices,10);
    /// let old_vals = req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    fn batch_fetch_div<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayFetchBatchOpHandle<T> {
        self.inner_array().initiate_batch_fetch_op_2(
            val,
            index,
            ArrayOpCmd::FetchDiv,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call divides the element specified by `index` with the supplied `val` and stores the result
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
    /// let req = array.rem(idx,val);
    /// req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    fn rem<'a>(&self, index: usize, val: T) -> ArrayOpHandle {
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::Rem,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call performs a batched vesion of the [rem][ArithmeticOps::rem] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [ArithmeticOps] documentation for more information on batch operation input
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
    /// let req = array.batch_rem(indices,10);
    /// req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    fn batch_rem<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayBatchOpHandle {
        // self.inner_array().initiate_op(val, index, ArrayOpCmd::Rem)
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::Rem,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call divides the element specified by `index` with the supplied `val`, returning the old value
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
    /// let req = array.fetch_rem(idx,val);
    /// let old = req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    fn fetch_rem<'a>(&self, index: usize, val: T) -> ArrayFetchOpHandle<T> {
        self.inner_array()
            .initiate_batch_fetch_op_2(
                val,
                index,
                ArrayOpCmd::FetchRem,
                self.as_lamellar_byte_array(),
            )
            .into()
    }

    /// This call performs a batched vesion of the [fetch_rem][ArithmeticOps::fetch_rem] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [ArithmeticOps] documentation for more information on batch operation input
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
    /// let req = array.batch_fetch_rem(indices,10);
    /// let old_vals = req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    fn batch_fetch_rem<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayFetchBatchOpHandle<T> {
        self.inner_array().initiate_batch_fetch_op_2(
            val,
            index,
            ArrayOpCmd::FetchRem,
            self.as_lamellar_byte_array(),
        )
    }
}

#[doc(alias("One-sided", "onesided"))]
/// The interface for performing remote arithmetic operations on [UnsafeArray] elements
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
/// unsafe{array.batch_fetch_add(indices,val).block()};
///```
/// ## Many Values - One Index
/// In this type, multiple values will be applied to the given index
///```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
/// let array = UnsafeArray::<isize>::new(&world,100,Distribution::Block).block();
///
/// let vals = vec![3,54,12,88,29,68];
/// let index = 10;
/// unsafe{array.batch_sub(index,vals).block()};
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
/// unsafe{array.batch_fetch_mul(indices,vals).block()};
///```
pub trait UnsafeArithmeticOps<T: Dist + ElementArithmeticOps>:
    private::LamellarArrayPrivate<T>
{
    /// This call adds the supplied `val` into the element specified by `index`
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
    /// let req = unsafe{ array.add(idx,val) };
    /// req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    unsafe fn add(&self, index: usize, val: T) -> ArrayOpHandle {
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::Add,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call performs a batched vesion of the [add][ArithmeticOps::add] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [ArithmeticOps] documentation for more information on batch operation input
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
    /// let req = unsafe{ array.batch_add(indices,10) };
    /// req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    unsafe fn batch_add<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayBatchOpHandle {
        // self.inner_array().initiate_op(val, index, ArrayOpCmd::Add)
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::Add,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call adds the supplied `val` into the element specified by `index`, returning the old value
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
    /// let req = unsafe{ array.fetch_add(idx,val) };
    /// let old = req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    unsafe fn fetch_add(&self, index: usize, val: T) -> ArrayFetchOpHandle<T> {
        self.inner_array()
            .initiate_batch_fetch_op_2(
                val,
                index,
                ArrayOpCmd::FetchAdd,
                self.as_lamellar_byte_array(),
            )
            .into()
    }

    /// This call performs a batched vesion of the [fetch_add][ArithmeticOps::fetch_add] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [ArithmeticOps] documentation for more information on batch operation input
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
    /// let req = unsafe{ array.batch_fetch_add(indices,10) };
    /// let old_vals = req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    unsafe fn batch_fetch_add<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayFetchBatchOpHandle<T> {
        self.inner_array().initiate_batch_fetch_op_2(
            val,
            index,
            ArrayOpCmd::FetchAdd,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call subtracts the supplied `val` from the element specified by `index`
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
    /// let array = UnsafeArray::<isize>::new(&world,100,Distribution::Block).block();
    ///
    /// let idx = 53;
    /// let val = 10;
    /// let req = unsafe{ array.sub(idx,val) };
    /// req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    unsafe fn sub<'a>(&self, index: usize, val: T) -> ArrayOpHandle {
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::Sub,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call performs a batched vesion of the [sub][ArithmeticOps::sub] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [ArithmeticOps] documentation for more information on batch operation input
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
    /// let array = UnsafeArray::<isize>::new(&world,100,Distribution::Block).block();
    ///
    /// let indices = vec![3,54,12,88,29,68];
    /// let req = unsafe{ array.batch_sub(indices,10) };
    /// req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    unsafe fn batch_sub<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayBatchOpHandle {
        // self.inner_array().initiate_op(val, index, ArrayOpCmd::Sub)
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::Sub,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call subtracts the supplied `val` from the element specified by `index`, returning the old value
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
    /// let array = UnsafeArray::<isize>::new(&world,100,Distribution::Block).block();
    ///
    /// let idx = 53;
    /// let val = 10;
    /// let req = unsafe{ array.fetch_sub(idx,val) };
    /// let old = req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    unsafe fn fetch_sub<'a>(&self, index: usize, val: T) -> ArrayFetchOpHandle<T> {
        self.inner_array()
            .initiate_batch_fetch_op_2(
                val,
                index,
                ArrayOpCmd::FetchSub,
                self.as_lamellar_byte_array(),
            )
            .into()
    }

    /// This call performs a batched vesion of the [fetch_sub][ArithmeticOps::fetch_sub] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [ArithmeticOps] documentation for more information on batch operation input
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
    /// let array = UnsafeArray::<isize>::new(&world,100,Distribution::Block).block();
    ///
    /// let indices = vec![3,54,12,88,29,68];
    /// let req = unsafe{ array.batch_fetch_sub(indices,10) };
    /// let old_vals = req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    unsafe fn batch_fetch_sub<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayFetchBatchOpHandle<T> {
        self.inner_array().initiate_batch_fetch_op_2(
            val,
            index,
            ArrayOpCmd::FetchSub,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call multiplies the supplied `val` by the element specified by `index` and stores the result.
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
    /// let req = unsafe{ array.mul(idx,val) };
    /// req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    unsafe fn mul<'a>(&self, index: usize, val: T) -> ArrayOpHandle {
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::Mul,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call performs a batched vesion of the [mul][ArithmeticOps::mul] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [ArithmeticOps] documentation for more information on batch operation input
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
    /// let req = unsafe{ array.batch_mul(indices,10) };
    /// req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    unsafe fn batch_mul<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayBatchOpHandle {
        // self.inner_array().initiate_op(val, index, ArrayOpCmd::Mul)
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::Mul,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call multiplies the supplied `val` with the element specified by `index`, returning the old value
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
    /// let req = unsafe{ array.fetch_mul(idx,val) };
    /// let old = req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    unsafe fn fetch_mul<'a>(&self, index: usize, val: T) -> ArrayFetchOpHandle<T> {
        self.inner_array()
            .initiate_batch_fetch_op_2(
                val,
                index,
                ArrayOpCmd::FetchMul,
                self.as_lamellar_byte_array(),
            )
            .into()
    }

    /// This call performs a batched vesion of the [fetch_mul][ArithmeticOps::fetch_mul] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [ArithmeticOps] documentation for more information on batch operation input
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
    /// let req = unsafe{ array.batch_fetch_mul(indices,10) };
    /// let old_vals = req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    unsafe fn batch_fetch_mul<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayFetchBatchOpHandle<T> {
        self.inner_array().initiate_batch_fetch_op_2(
            val,
            index,
            ArrayOpCmd::FetchMul,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call divides the element specified by `index` with the supplied `val` and stores the result
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
    /// let req = unsafe{ array.div(idx,val) };
    /// req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    unsafe fn div<'a>(&self, index: usize, val: T) -> ArrayOpHandle {
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::Div,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call performs a batched vesion of the [div][ArithmeticOps::div] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [ArithmeticOps] documentation for more information on batch operation input
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
    /// let req = unsafe{ array.batch_div(indices,10) };
    /// req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    unsafe fn batch_div<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayBatchOpHandle {
        // self.inner_array().initiate_op(val, index, ArrayOpCmd::Div)
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::Div,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call divides the element specified by `index` with the supplied `val`, returning the old value
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
    /// let req = unsafe{ array.fetch_div(idx,val) };
    /// let old = req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    unsafe fn fetch_div<'a>(&self, index: usize, val: T) -> ArrayFetchOpHandle<T> {
        self.inner_array()
            .initiate_batch_fetch_op_2(
                val,
                index,
                ArrayOpCmd::FetchDiv,
                self.as_lamellar_byte_array(),
            )
            .into()
    }

    /// This call performs a batched vesion of the [fetch_div][ArithmeticOps::fetch_div] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [ArithmeticOps] documentation for more information on batch operation input
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
    /// let req = unsafe{ array.batch_fetch_div(indices,10) };
    /// let old_vals = req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    unsafe fn batch_fetch_div<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayFetchBatchOpHandle<T> {
        self.inner_array().initiate_batch_fetch_op_2(
            val,
            index,
            ArrayOpCmd::FetchDiv,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call divides the element specified by `index` with the supplied `val` and stores the result
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
    /// let req = unsafe{ array.rem(idx,val) };
    /// req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    unsafe fn rem<'a>(&self, index: usize, val: T) -> ArrayOpHandle {
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::Rem,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call performs a batched vesion of the [rem][ArithmeticOps::rem] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [ArithmeticOps] documentation for more information on batch operation input
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
    /// let req = unsafe{ array.batch_rem(indices,10) };
    /// req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    unsafe fn batch_rem<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayBatchOpHandle {
        // self.inner_array().initiate_op(val, index, ArrayOpCmd::Rem)
        self.inner_array().initiate_batch_op(
            val,
            index,
            ArrayOpCmd::Rem,
            self.as_lamellar_byte_array(),
        )
    }

    /// This call divides the element specified by `index` with the supplied `val`, returning the old value
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
    /// let req = unsafe{ array.fetch_rem(idx,val) };
    /// let old = req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    unsafe fn fetch_rem<'a>(&self, index: usize, val: T) -> ArrayFetchOpHandle<T> {
        self.inner_array()
            .initiate_batch_fetch_op_2(
                val,
                index,
                ArrayOpCmd::FetchRem,
                self.as_lamellar_byte_array(),
            )
            .into()
    }

    /// This call performs a batched vesion of the [fetch_rem][ArithmeticOps::fetch_rem] function,
    ///
    /// Instead of a single value and index this function expects a list of `vals`, or a list of `indices` or both.
    /// Please see the general [ArithmeticOps] documentation for more information on batch operation input
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
    /// let req = unsafe{ array.batch_fetch_rem(indices,10) };
    /// let old_vals = req.block();
    ///```
    //#[tracing::instrument(skip_all)]
    unsafe fn batch_fetch_rem<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> ArrayFetchBatchOpHandle<T> {
        self.inner_array().initiate_batch_fetch_op_2(
            val,
            index,
            ArrayOpCmd::FetchRem,
            self.as_lamellar_byte_array(),
        )
    }
}

#[doc(hidden)]
pub trait LocalArithmeticOps<T: Dist + ElementArithmeticOps> {
    fn local_add(&self, index: usize, val: T) {
        self.local_fetch_add(index, val);
    }
    fn local_fetch_add(&self, index: usize, val: T) -> T;
    fn local_sub(&self, index: usize, val: T) {
        self.local_fetch_sub(index, val);
    }
    fn local_fetch_sub(&self, index: usize, val: T) -> T;
    fn local_mul(&self, index: usize, val: T) {
        self.local_fetch_mul(index, val);
    }
    fn local_fetch_mul(&self, index: usize, val: T) -> T;
    fn local_div(&self, index: usize, val: T) {
        self.local_fetch_div(index, val);
    }
    fn local_fetch_div(&self, index: usize, val: T) -> T;
}
