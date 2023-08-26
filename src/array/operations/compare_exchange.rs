use crate::array::*;

/// Supertrait specifying elements of the array support remote Equality operations
/// - ```==```
/// - ```!=```
pub trait ElementCompareEqOps: std::cmp::Eq + Dist + Sized //+ AmDist
{
}
// impl<T> ElementCompareEqOps for T where T: std::cmp::Eq + Dist //+ AmDist,,,,,,,,,,,,
// {
// }

/// Supertrait specifying elements of the array support remote Partial Equality operations
pub trait ElementComparePartialEqOps:
    std::cmp::PartialEq + std::cmp::PartialOrd + Dist + Sized //+ AmDist
{
}
// impl<T> ElementComparePartialEqOps for T where
//     T: std::cmp::PartialEq + std::cmp::PartialOrd + Dist //+ AmDist
// {
// }

#[doc(alias("One-sided", "onesided"))]
/// The interface for performing remote compare and exchange operations on array elements
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
/// For both single index and batched operations there are no guarantees to the order in which individual operations occur (an individal operation is guaranteed to be atomic though)
///
/// # Batched Types
/// Three types of batched operations can be performed
///
/// Currently only the indicies and new values can be batched, for all the batch types below you can only pass a single `current val` which will be used in each individual operation of the batch
/// We plan to support batched `current vals` in a future release.
/// ## One Value - Many Indicies
/// In this type, the same value will be applied to the provided indices
///```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
/// let array = AtomicArray::<usize>::new(&world,100,Distribution::Block);
///
/// let indices = vec![3,54,12,88,29,68];
/// let current = 0;
/// let new = 10;
/// array.block_on(array.batch_compare_exchange(indices,current,new));
///```
/// ## Many Values - One Index
/// In this type, multiple values will be applied to the given index
///```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
/// let array = AtomicArray::<usize>::new(&world,100,Distribution::Block);
///
/// let new_vals = vec![3,54,11101,88,29,68];
/// let current = 0;
/// let index = 10;
/// array.block_on(array.batch_compare_exchange(index,current,new_vals));
///```
/// ## Many Values - Many Indicies
/// In this type, values and indices have a one-to-one correspondance.
///
/// If the two lists are unequal in length, the longer of the two will be truncated so that it matches the length of the shorter
///```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
/// let array = AtomicArray::<usize>::new(&world,100,Distribution::Block);
///
/// let indices = vec![3,54,12,88,29,68];
/// let new_vals = vec![12,2,1,10000,12,13];
/// let current = 0;
/// array.block_on(array.batch_compare_exchange(indices,current,new_vals));
///```
pub trait CompareExchangeOps<T: ElementCompareEqOps>: private::LamellarArrayPrivate<T> {
    /// This call stores the `new` value into the element specified by `index` if the current value is the same as `current`.
    ///
    /// the return value is a result indicating whether the new value was written into the element and contains the previous value.
    /// On success this previous value is gauranteed to be equal to `current`
    ///
    /// A future is returned as the result of this call, which is used to detect when the operation has completed and retrieve the returned value.
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
    /// let idx = 53;
    /// let val = 10;
    /// let current = 0;
    /// let req = array.compare_exchange(idx,current,val);
    /// let result = array.block_on(req);
    ///```
    #[tracing::instrument(skip_all)]
    fn compare_exchange<'a>(
        &self,
        index: usize,
        current: T,
        new: T,
    ) -> Pin<Box<dyn Future<Output = Result<T, T>> + Send>> {
        let result = self.inner_array().initiate_batch_result_op_2(
            new,
            index,
            ArrayOpCmd::CompareExchange(current),
            self.as_lamellar_byte_array(),
        );
        Box::pin(async move { result.await[0] })
    }

    /// This call performs a batched vesion of the [compare_exchange][CompareExchangeOps::compare_exchange] function,
    ///
    /// Instead of a single value and index this function expects a list of (new)`vals`, or a list of `indices` or both.
    /// Note that presently only a single `current` value can be provided, and will be used for all operations in the batch.
    /// Please see the general [CompareExchangeOps] documentation for more information on batch operation input
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
    /// let current = 0;
    /// let req = array.batch_compare_exchange(indices,current,10);
    /// let results = array.block_on(req);
    ///```
    #[tracing::instrument(skip_all)]
    fn batch_compare_exchange<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        current: T,
        new: impl OpInput<'a, T>,
    ) -> Pin<Box<dyn Future<Output = Vec<Result<T, T>>> + Send>> {
        self.inner_array().initiate_batch_result_op_2(
            new,
            index,
            ArrayOpCmd::CompareExchange(current),
            self.as_lamellar_byte_array(),
        )
    }
}

#[doc(alias("One-sided", "onesided"))]
/// The interface for performing remote compare and exchange operations within a given epsilon on array elements
///
/// Useful for element types that only impl [PartialEq][std::cmp::PartialEq] instead of [Eq][std::cmp::Eq] (e.g `f32`,`f64`).
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
/// Three types of batched operations can be performed
///
/// Currently only the indicies and new values can be batched, for all the batch types below you can only pass a single `current val` and a single `epsilon` which will be used in each individual operation of the batch
/// We plan to support batched `current vals` and `epsilons` in a future release.
///
/// ## One Value - Many Indicies
/// In this type, the same value will be applied to the provided indices
///```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
/// let array = AtomicArray::<f32>::new(&world,100,Distribution::Block);
///
/// let indices = vec![3,54,11,88,29,68];
/// let current = 0.0;
/// let new = 10.5;
/// let epsilon = 0.1;
/// array.block_on(array.batch_compare_exchange_epsilon(indices,current,new,epsilon));
///```
/// ## Many Values - One Index
/// In this type, multiple values will be applied to the given index
///```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
/// let array = AtomicArray::<f32>::new(&world,100,Distribution::Block);
///
/// let new_vals = vec![3.0,54.8,12.9,88.1,29.2,68.9];
/// let current = 0.0;
/// let index = 10;
/// let epsilon = 0.1;
/// array.block_on(array.batch_compare_exchange_epsilon(index,current,new_vals,epsilon));
///```
/// ## Many Values - Many Indicies
/// In this type, values and indices have a one-to-one correspondance.
///
/// If the two lists are unequal in length, the longer of the two will be truncated so that it matches the length of the shorter
///```
/// use lamellar::array::prelude::*;
///
/// let world = LamellarWorldBuilder::new().build();
/// let array = AtomicArray::<f32>::new(&world,100,Distribution::Block);
///
/// let indices = vec![3,54,12,88,29,68];
/// let new_vals = vec![12.1,2.321,1.7,10000.0,12.4,13.7];
/// let current = 0.0;
/// let epsilon = 0.1;
/// array.block_on(array.batch_compare_exchange_epsilon(indices,current,new_vals,epsilon));
///```
pub trait CompareExchangeEpsilonOps<T: ElementComparePartialEqOps>:
    private::LamellarArrayPrivate<T>
{
    /// This call stores the `new` value into the element specified by `index` if the current value is the same as `current` plus or minus `epslion`.
    ///
    /// e.g. ``` if current - epsilon < array[index] && array[index] < current + epsilon { array[index] = new }```
    ///
    /// The return value is a result indicating whether the new value was written into the element and contains the previous value.
    /// On success this previous value is gauranteed to be within epsilon of `current`
    ///
    /// A future is returned as the result of this call, which is used to detect when the operation has completed and retrieve the returned value.
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
    /// let array = AtomicArray::<f32>::new(&world,100,Distribution::Block);
    ///
    /// let idx = 53;
    /// let val = 10.3;
    /// let current = 0.0;
    /// let epsilon = 0.1;
    /// let req = array.compare_exchange_epsilon(idx,current,val,epsilon);
    /// let result = array.block_on(req);
    ///```
    #[tracing::instrument(skip_all)]
    fn compare_exchange_epsilon<'a>(
        &self,
        index: usize,
        current: T,
        new: T,
        eps: T,
    ) -> Pin<Box<dyn Future<Output = Result<T, T>> + Send>> {
        let result = self.inner_array().initiate_batch_result_op_2(
            new,
            index,
            ArrayOpCmd::CompareExchangeEps(current, eps),
            self.as_lamellar_byte_array(),
        );
        Box::pin(async move { result.await[0] })
    }

    /// This call performs a batched vesion of the [compare_exchange_epsilon][CompareExchangeEpsilonOps::compare_exchange_epsilon] function,
    ///
    /// Instead of a single value and index this function expects a list of (new)`vals`, or a list of `indices` or both.
    /// Note that presently only a single `current` value and a single `epsilon` value can be provided, and they will be used for all operations in the batch.
    /// Please see the general [CompareExchangeEpsilonOps] documentation for more information on batch operation input
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
    /// let array = AtomicArray::<f32>::new(&world,100,Distribution::Block);
    ///
    /// let indices = vec![3,54,12,88,29,68];
    /// let current = 0.0;
    /// let epsilon = 0.001;
    /// let req = array.batch_compare_exchange_epsilon(indices,current,10.321,epsilon);
    /// let results = array.block_on(req);
    ///```
    #[tracing::instrument(skip_all)]
    fn batch_compare_exchange_epsilon<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        current: T,
        new: impl OpInput<'a, T>,
        eps: T,
    ) -> Pin<Box<dyn Future<Output = Vec<Result<T, T>>> + Send>> {
        self.inner_array().initiate_batch_result_op_2(
            new,
            index,
            ArrayOpCmd::CompareExchangeEps(current, eps),
            self.as_lamellar_byte_array(),
        )
    }
}
