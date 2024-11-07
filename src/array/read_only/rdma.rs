use crate::array::*;

impl<T: Dist> ReadOnlyArray<T> {
    /// Performs a raw RDMA "Get" of the data in this array starting at the provided index into the specified buffer
    ///
    /// The length of the Get is dictated by the length of the buffer.
    ///
    /// The runtime provides no internal mechanism to check for completion when using this call.
    /// i.e. this means the users themselves will be responsible for determining when the transfer is complete
    ///
    ///
    /// # Safety
    /// This call is unsafe with respect to `buf` given that currently it must be one of the low-level [Memory Region][crate::memregion] types,
    /// there will be no gaurantees that there doesn't exist other readers/writers either locally or remotely.
    ///
    /// It is guaranteed though that the data in the ReadOnlyArray itself is immutable.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array = ReadOnlyArray::<usize>::new(&world,12,Distribution::Block).block();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(12);
    /// unsafe {
    ///     for elem in buf.as_mut_slice()
    ///                          .expect("we just created it so we know its local") { //initialize mem_region
    ///         *elem = buf.len(); //we will used this val as completion detection
    ///     }
    /// }
    /// array.wait_all();
    /// array.barrier();
    /// println!("PE{my_pe} array data: {:?}",unsafe{buf.as_slice().unwrap()});
    /// if my_pe == 0 { //only perfrom the transfer from one PE
    ///     unsafe {array.get_unchecked(0,&buf)} ;
    ///     println!();
    /// }
    /// // wait for the data to show up
    /// unsafe {
    ///     for elem in buf.as_slice().unwrap(){
    ///         while *elem == buf.len(){
    ///             std::thread::yield_now();    
    ///         }
    ///     }
    /// }
    ///    
    /// println!("PE{my_pe} buf data: {:?}",unsafe{buf.as_slice().unwrap()});
    ///
    ///```
    /// Possible output on A 4 PE system (ordering with respect to PEs may change)
    ///```text
    /// PE0: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    /// PE1: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    /// PE2: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    /// PE3: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    ///
    /// PE1: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    /// PE2: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    /// PE3: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    /// PE0: buf data [0,0,0,0,0,0,0,0,0,0,0,0] //we only did the "get" on PE0, also likely to be printed last since the other PEs do not wait for PE0 in this example
    ///```
    pub unsafe fn get_unchecked<U: TeamTryInto<LamellarArrayRdmaOutput<T>> + LamellarWrite>(
        &self,
        index: usize,
        buf: U,
    ) {
        self.array.get_unchecked(index, buf)
    }

    /// Performs a blocking (active message based) "Get" of the data in this array starting at the provided index into the specified buffer
    ///
    /// The length of the Get is dictated by the length of the buffer.
    ///
    /// When this function returns, `buf` will have been populated with the results of the `get`
    ///
    /// # Safety
    /// This call is unsafe with respect to `buf` given that currently it must be one of the low-level [Memory Region][crate::memregion] types,
    /// there will be no gaurantees that there doesn't exist other readers/writers either locally or remotely.
    ///
    /// It is guaranteed though that the data in the ReadOnlyArray itself is immutable.
    ///
    /// # Examples
    ///```
    /// use lamellar::array::prelude::*;
    /// use lamellar::memregion::prelude::*;
    ///
    /// let world = LamellarWorldBuilder::new().build();
    /// let my_pe = world.my_pe();
    /// let array = ReadOnlyArray::<usize>::new(&world,12,Distribution::Block).block();
    /// let buf = world.alloc_one_sided_mem_region::<usize>(12);
    /// unsafe {
    ///     for elem in buf.as_mut_slice()
    ///                          .expect("we just created it so we know its local") { //initialize mem_region
    ///         *elem = buf.len();
    ///     }
    /// }
    /// array.wait_all();
    /// array.barrier();
    /// println!("PE{my_pe} array data: {:?}",unsafe{buf.as_slice().unwrap()});
    /// if my_pe == 0 { //only perfrom the transfer from one PE
    ///     println!();
    ///     unsafe{ array.blocking_get(0,&buf);}
    /// }
    /// println!("PE{my_pe} buf data: {:?}",unsafe{buf.as_slice().unwrap()});
    ///```
    /// Possible output on A 4 PE system (ordering with respect to PEs may change)
    ///```text
    /// PE0: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    /// PE1: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    /// PE2: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    /// PE3: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    ///
    /// PE1: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    /// PE2: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    /// PE3: buf data [12,12,12,12,12,12,12,12,12,12,12,12]
    /// PE0: buf data  [0,0,0,0,0,0,0,0,0,0,0,0] //we only did the "get" on PE0, also likely to be printed last since the other PEs do not wait for PE0 in this example
    ///```
    pub unsafe fn blocking_get<U: TeamTryInto<LamellarArrayRdmaOutput<T>> + LamellarWrite>(
        &self,
        index: usize,
        buf: U,
    ) {
        self.array.blocking_get(index, buf)
    }
}

impl<T: Dist + 'static> LamellarArrayInternalGet<T> for ReadOnlyArray<T> {
    unsafe fn internal_get<U: Into<LamellarMemoryRegion<T>>>(
        &self,
        index: usize,
        buf: U,
    ) -> ArrayRdmaHandle {
        self.array.internal_get(index, buf)
    }
    unsafe fn internal_at(&self, index: usize) -> ArrayRdmaAtHandle<T> {
        self.array.internal_at(index)
    }
}

impl<T: Dist + 'static> LamellarArrayGet<T> for ReadOnlyArray<T> {
    unsafe fn get<U: TeamTryInto<LamellarArrayRdmaOutput<T>> + LamellarWrite>(
        &self,
        index: usize,
        buf: U,
    ) -> ArrayRdmaHandle {
        self.array.get(index, buf)
    }
    fn at(&self, index: usize) -> ArrayRdmaAtHandle<T> {
        unsafe { self.array.at(index) }
    }
}
