mod consumer;
mod distributed;
mod local;

use crate::array::r#unsafe::*;

use crate::array::iterator::distributed_iterator::{DistIter, DistIterMut};
use crate::array::iterator::local_iterator::{LocalIter, LocalIterMut};
use crate::array::iterator::one_sided_iterator::OneSidedIter;
use crate::array::*;
use crate::memregion::Dist;

impl<T: Dist> UnsafeArray<T> {
    #[doc(alias = "Collective")]
    /// Create an immutable [DistributedIterator][crate::array::DistributedIterator] for this UnsafeArray
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the array to enter the call otherwise deadlock will occur (i.e. barriers are being called internally)
    /// Throughout execution of the iteration, data movement may occur amongst various PEs
    ///
    /// # Safety
    /// Data in UnsafeArrays are always unsafe as there are no protections on how remote PE's may access any other PE's local data.
    /// It is also possible to have mutable and immutable references to this arrays data on the same PE
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world,100,Distribution::Cyclic).block();
    ///
    /// unsafe {
    ///     world.block_on(
    ///         array.dist_iter().for_each(move |elem| println!("PE{my_pe} elem {elem}") )
    ///     );
    /// }
    ///```
    pub unsafe fn dist_iter(&self) -> DistIter<'static, T, UnsafeArray<T>> {
        DistIter::new(self.clone().into(), 0, 0)
    }

    #[doc(alias = "Collective")]
    /// Create a mutable [DistributedIterator][crate::array::DistributedIterator] for this UnsafeArray
    ///
    /// # Collective Operation
    /// Requires all PEs associated with the array to enter the call otherwise deadlock will occur (i.e. barriers are being called internally)
    /// Throughout execution of the iteration, data movement may occur amongst various PEs
    ///
    /// # Safety
    /// Data in UnsafeArrays are always unsafe as there are no protections on how remote PE's may access any other PE's local data.
    /// It is also possible to have mutable and immutable references to this arrays data on the same PE
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world,100,Distribution::Cyclic).block();
    ///
    /// unsafe {
    ///     world.block_on(
    ///         array.dist_iter_mut().for_each(move |elem| *elem = my_pe)
    ///     );
    /// }
    ///```
    pub unsafe fn dist_iter_mut(&self) -> DistIterMut<'static, T, UnsafeArray<T>> {
        DistIterMut::new(self.clone().into(), 0, 0)
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Create an immutable [LocalIterator][crate::array::LocalIterator] for this UnsafeArray
    ///
    /// # Safety
    /// Data in UnsafeArrays are always unsafe as there are no protections on how remote PE's may access any other PE's local data.
    /// It is also possible to have mutable and immutable references to this arrays data on the same PE
    ///
    /// # One-sided Operation
    /// The iteration is launched and local to only the calling PE.
    /// No data movement from remote PEs is required
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world,100,Distribution::Cyclic).block();
    ///
    /// unsafe {
    ///     world.block_on(
    ///         array.local_iter().for_each(move |elem| println!("PE{my_pe} elem {elem}"))
    ///     );
    /// }
    ///```
    pub unsafe fn local_iter(&self) -> LocalIter<'static, T, UnsafeArray<T>> {
        LocalIter::new(self.clone().into(), 0, 0)
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Create a mutable [LocalIterator][crate::array::LocalIterator] for this UnsafeArray
    ///
    /// # Safety
    /// Data in UnsafeArrays are always unsafe as there are no protections on how remote PE's may access any other PE's local data.
    /// It is also possible to have mutable and immutable references to this arrays data on the same PE
    ///
    /// # One-sided Operation
    /// The iteration is launched and local to only the calling PE.
    /// No data movement from remote PEs is required
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world,100,Distribution::Cyclic).block();
    ///
    /// unsafe {
    ///     world.block_on(
    ///         array.local_iter_mut().for_each(move |elem| *elem = my_pe)
    ///     );
    /// }
    pub unsafe fn local_iter_mut(&self) -> LocalIterMut<'static, T, UnsafeArray<T>> {
        LocalIterMut::new(self.clone().into(), 0, 0)
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Create an immutable [OneSidedIterator][crate::array::OneSidedIterator] for this UnsafeArray
    ///
    /// # Safety
    /// Data in UnsafeArrays are always unsafe as there are no protections on how remote PE's may access any other PE's local data.
    /// It is also possible to have mutable and immutable references to this arrays data on the same PE
    ///
    /// # One-sided Operation
    /// The iteration is launched and local to only the calling PE.
    /// Data movement will occur with the remote PEs to transfer their data to the calling PE
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world,100,Distribution::Cyclic).block();
    ///
    /// unsafe {
    ///     if my_pe == 0 {
    ///         for elem in array.onesided_iter().into_iter() { //"into_iter()" converts into a standard Rust Iterator
    ///             println!("PE{my_pe} elem {elem}");
    ///         }
    ///     }
    /// }
    ///```
    pub unsafe fn onesided_iter(&self) -> OneSidedIter<'_, T, UnsafeArray<T>> {
        OneSidedIter::new(self.clone(), self.inner.data.team.clone(), 1)
    }

    #[doc(alias("One-sided", "onesided"))]
    /// Create an immutable [OneSidedIterator][crate::array::OneSidedIterator] for this UnsafeArray
    /// which will transfer and buffer `buf_size` elements at a time (to more efficient utilize the underlying lamellae network)
    ///
    /// The buffering is transparent to the user.
    ///
    /// This iterator typcially outperforms the non buffered version.
    ///
    /// # Safety
    /// Data in UnsafeArrays are always unsafe as there are no protections on how remote PE's may access any other PE's local data.
    /// It is also possible to have mutable and immutable references to this arrays data on the same PE
    ///
    /// # One-sided Operation
    /// The iteration is launched and local to only the calling PE.
    /// Data movement will occur with the remote PEs to transfer their data to the calling PE
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array: UnsafeArray<usize> = UnsafeArray::new(&world,100,Distribution::Cyclic).block();
    ///
    /// unsafe {
    ///     if my_pe == 0 {
    ///         for elem in array.buffered_onesided_iter(100).into_iter() { // "into_iter()" converts into a standard Rust Iterator
    ///             println!("PE{my_pe} elem {elem}");
    ///         }
    ///     }
    /// }
    ///```
    pub unsafe fn buffered_onesided_iter(
        &self,
        buf_size: usize,
    ) -> OneSidedIter<'_, T, UnsafeArray<T>> {
        OneSidedIter::new(
            self.clone(),
            self.inner.data.team.clone(),
            std::cmp::min(buf_size, self.len()),
        )
    }
}
