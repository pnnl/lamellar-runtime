use crate::array::atomic::*;
use crate::array::LamellarWrite;
use crate::array::*;
use crate::lamellar_request::LamellarRequest;
use crate::memregion::{
    AsBase, Dist, MemoryRegionRDMA, RTMemoryRegionRDMA, RegisteredMemoryRegion, SubRegion,
};

// enum AtomicArrayReadInput{
//     ReadOnlyArray(array),
//     LocalOnlyRead(array),
// }

// enum AtomicArrayWriteInput{

// }

impl<T: Dist> AtomicArray<T> {
//      //these change into
//     // load, store, swap
//     // pub unsafe fn get_unchecked<U: MyInto<LamellarArrayInput<T>> + LamellarWrite >(&self, index: usize, buf: U) {
//     //     // self.array.get_unchecked(index,buf)
//     // }
//     // pub  fn iget<U: MyInto<LamellarArrayInput<T>> + LamellarWrite >(&self, index: usize, buf: U) {
//     //     // self.array.get_unchecked(index,buf)
//     // }
//     // pub fn put<U: MyInto<LamellarArrayInput<T>> + LamellarRead >(&self, index: usize, buf: U) {
//     //     // self.array.get_unchecked(index,buf)
//     // }
//     // pub fn at(&self, index: usize) -> T {
//     //     self.array.at(index)
//     // }
}


impl<T: Dist> LamellarArrayRead<T> for AtomicArray<T> {
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

impl<T: Dist> LamellarArrayWrite<T> for AtomicArray<T> {
    // fn put_unchecked<U: MyInto<LamellarArrayInput<T>> + LamellarRead>(&self, index: usize, buf: U) {
    //     self.array.put_unchecked(index, buf)
    // }
}