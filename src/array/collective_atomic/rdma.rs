use crate::array::collective_atomic::*;
use crate::array::LamellarWrite;
use crate::array::*;
use crate::lamellar_request::LamellarRequest;
use crate::memregion::{
    AsBase, Dist, MemoryRegionRDMA, RTMemoryRegionRDMA, RegisteredMemoryRegion, SubRegion,
};


impl<T: Dist> LamellarArrayRead<T> for CollectiveAtomicArray<T> {
    // unsafe fn get_unchecked<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(
    //     &self,
    //     index: usize,
    //     buf: U,
    // ) {
    //     self.array.get_unchecked(index, buf)
    // }
    fn iget<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(&self, index: usize, buf: U) {
        self.array.iget(index, buf)
    }
    fn get<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(&self, index: usize, buf: U) {
        self.array.get(index, buf)
    }
    fn iat(&self, index: usize) -> T {
        self.array.iat(index)
    }
}

impl<T: Dist> LamellarArrayWrite<T> for CollectiveAtomicArray<T> {
    // fn put_unchecked<U: MyInto<LamellarArrayInput<T>> + LamellarRead>(&self, index: usize, buf: U) {
    //     self.array.put_unchecked(index, buf)
    // }
}