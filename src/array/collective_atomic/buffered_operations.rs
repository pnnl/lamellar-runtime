use crate::active_messaging::*;
use crate::array::collective_atomic::*;
use crate::array::*;
use crate::lamellar_request::LamellarRequest;
// use crate::memregion::Dist;
use std::any::TypeId;
use std::collections::HashMap;

type OpFn = fn(*const u8, CollectiveAtomicByteArray, usize) -> LamellarArcAm;

lazy_static! {
    static ref OPS: HashMap<(ArrayOpCmd, TypeId), OpFn> = {
        let mut map = HashMap::new();
        for op in crate::inventory::iter::<CollectiveAtomicArrayOp> {
            map.insert(op.id.clone(), op.op);
        }
        map
    };
}

pub struct CollectiveAtomicArrayOp {
    pub id: (ArrayOpCmd, TypeId),
    pub op: OpFn,
}

crate::inventory::collect!(CollectiveAtomicArrayOp);

type BufFn = fn(CollectiveAtomicByteArray) -> Arc<dyn BufferOp>;

lazy_static! {
        pub(crate) static ref BUFOPS: HashMap<TypeId, BufFn> = {
        let mut map = HashMap::new();
        for op in crate::inventory::iter::<CollectiveAtomicArrayOpBuf> {
            map.insert(op.id.clone(), op.op);
        }
        map
    };
}

pub struct CollectiveAtomicArrayOpBuf {
    pub id: TypeId,
    pub op: BufFn,
}

crate::inventory::collect!(CollectiveAtomicArrayOpBuf);

impl<T: AmDist + Dist + 'static> CollectiveAtomicArray<T> {
    fn initiate_op(
        &self,
        pe: usize,
        val: T,
        local_index: usize,
        op: ArrayOpCmd,
    ) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync> {
        self.array.initiate_op(pe,val,local_index,op)
    }

    fn initiate_fetch_op(
        &self,
        pe: usize,
        val: T,
        local_index: usize,
        op: ArrayOpCmd,
    ) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        self.array.initiate_fetch_op(pe,val,local_index,op)
    }

    pub fn store(
        &self,
        index: usize,
        val: T,
    ) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>> {
        let pe = self.pe_for_dist_index(index).expect("index out of bounds");
        let local_index = self.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        Some(self.initiate_op(pe, val, local_index, ArrayOpCmd::Store))
    }

    pub fn load(&self, index: usize) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        let pe = self.pe_for_dist_index(index).expect("index out of bounds");
        let local_index = self.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        let dummy_val = self.array.dummy_val(); //we dont actually do anything with this except satisfy apis;
        self.initiate_fetch_op(pe, dummy_val, local_index, ArrayOpCmd::Load)
    }

    pub fn swap(&self, index: usize, val: T) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        let pe = self.pe_for_dist_index(index).expect("index out of bounds");
        let local_index = self.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        self.initiate_fetch_op(pe, val, local_index, ArrayOpCmd::Swap)
    }
}

impl<T: ElementArithmeticOps + 'static> ArithmeticOps<T> for CollectiveAtomicArray<T> {
    fn add(
        &self,
        index: usize,
        val: T,
    ) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>> {
        let pe = self.pe_for_dist_index(index).expect("index out of bounds");
        let local_index = self.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        Some(self.initiate_op(pe, val, local_index, ArrayOpCmd::Add))
    }
    fn fetch_add(
        &self,
        index: usize,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        let pe = self.pe_for_dist_index(index).expect("index out of bounds");
        let local_index = self.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        self.initiate_fetch_op(pe, val, local_index, ArrayOpCmd::FetchAdd)
    }
    fn sub(
        &self,
        index: usize,
        val: T,
    ) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>> {
        let pe = self.pe_for_dist_index(index).expect("index out of bounds");
        let local_index = self.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        Some(self.initiate_op(pe, val, local_index, ArrayOpCmd::Sub))
    }
    fn fetch_sub(
        &self,
        index: usize,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        let pe = self.pe_for_dist_index(index).expect("index out of bounds");
        let local_index = self.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        self.initiate_fetch_op(pe, val, local_index, ArrayOpCmd::FetchSub)
    }
    fn mul(
        &self,
        index: usize,
        val: T,
    ) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>> {
        let pe = self.pe_for_dist_index(index).expect("index out of bounds");
        let local_index = self.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        Some(self.initiate_op(pe, val, local_index, ArrayOpCmd::Mul))
    }
    fn fetch_mul(
        &self,
        index: usize,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        let pe = self.pe_for_dist_index(index).expect("index out of bounds");
        let local_index = self.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        self.initiate_fetch_op(pe, val, local_index, ArrayOpCmd::FetchMul)
    }
    fn div(
        &self,
        index: usize,
        val: T,
    ) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>> {
        let pe = self.pe_for_dist_index(index).expect("index out of bounds");
        let local_index = self.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        Some(self.initiate_op(pe, val, local_index, ArrayOpCmd::Div))
    }
    fn fetch_div(
        &self,
        index: usize,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        let pe = self.pe_for_dist_index(index).expect("index out of bounds");
        let local_index = self.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        self.initiate_fetch_op(pe, val, local_index, ArrayOpCmd::FetchDiv)
    }
}

impl<T: ElementBitWiseOps + 'static> BitWiseOps<T> for CollectiveAtomicArray<T> {
    fn bit_and(
        &self,
        index: usize,
        val: T,
    ) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>> {
        let pe = self.pe_for_dist_index(index).expect("index out of bounds");
        let local_index = self.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        Some(self.initiate_op(pe, val, local_index, ArrayOpCmd::And))
    }
    fn fetch_bit_and(
        &self,
        index: usize,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        let pe = self.pe_for_dist_index(index).expect("index out of bounds");
        let local_index = self.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        self.initiate_fetch_op(pe, val, local_index, ArrayOpCmd::FetchAnd)
    }

    fn bit_or(
        &self,
        index: usize,
        val: T,
    ) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>> {
        let pe = self.pe_for_dist_index(index).expect("index out of bounds");
        let local_index = self.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        Some(self.initiate_op(pe, val, local_index, ArrayOpCmd::Or))
    }
    fn fetch_bit_or(
        &self,
        index: usize,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        let pe = self.pe_for_dist_index(index).expect("index out of bounds");
        let local_index = self.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        self.initiate_fetch_op(pe, val, local_index, ArrayOpCmd::FetchOr)
    }
}

// // impl<T: Dist + std::ops::AddAssign> CollectiveAtomicArray<T> {
// impl<T: ElementArithmeticOps> LocalArithmeticOps<T> for CollectiveAtomicArray<T> {
//     fn local_fetch_add(&self, index: usize, val: T) -> T {
//         // println!("local_add LocalArithmeticOps<T> for CollectiveAtomicArray<T> ");
//         // let _lock = self.lock.write();
//         let mut slice = self.local_as_mut_slice(); //this locks the array
//         let orig = slice[index]; //this locks the
//         slice[index] += val;
//         orig
//     }
//     fn local_fetch_sub(&self, index: usize, val: T) -> T {
//         // println!("local_sub LocalArithmeticOps<T> for CollectiveAtomicArray<T> ");
//         let mut slice = self.local_as_mut_slice(); //this locks the array
//         let orig = slice[index];
//         slice[index] -= val;
//         orig
//     }
//     fn local_fetch_mul(&self, index: usize, val: T) -> T {
//         // println!("local_sub LocalArithmeticOps<T> for CollectiveAtomicArray<T> ");
//         let mut slice = self.local_as_mut_slice(); //this locks the array
//         let orig = slice[index];
//         slice[index] *= val;
//         orig
//     }
//     fn local_fetch_div(&self, index: usize, val: T) -> T {
//         // println!("local_sub LocalArithmeticOps<T> for CollectiveAtomicArray<T> ");
//         let mut slice = self.local_as_mut_slice(); //this locks the array
//         let orig = slice[index];
//         slice[index] /= val;
//         // println!("div i: {:?} {:?} {:?} {:?}",index,orig,val,self.local_as_mut_slice()[index]);
//         orig
//     }
// }
// impl<T: ElementBitWiseOps> LocalBitWiseOps<T> for CollectiveAtomicArray<T> {
//     fn local_fetch_bit_and(&self, index: usize, val: T) -> T {
//         let mut slice = self.local_as_mut_slice(); //this locks the array
//         // println!("local_sub LocalArithmeticOps<T> for CollectiveAtomicArray<T> ");
//         let orig = slice[index];
//         slice[index] &= val;
//         // println!("and i: {:?} {:?} {:?} {:?}",index,orig,val,self.local_as_mut_slice()[index]);
//         orig
//     }
//     fn local_fetch_bit_or(&self, index: usize, val: T) -> T {
//         let mut slice = self.local_as_mut_slice(); //this locks the array
//         // println!("local_sub LocalArithmeticOps<T> for CollectiveAtomicArray<T> ");
//         let orig = slice[index];
//         slice[index] |= val;
//         orig
//     }
// }
// impl<T: ElementOps> LocalAtomicOps<T> for CollectiveAtomicArray<T> {
//     fn local_load(&self, index: usize, _val: T) -> T {
//         self.local_as_mut_slice()[index]
//     }

//     fn local_store(&self, index: usize, val: T) {
//         self.local_as_mut_slice()[index] = val; //this locks the array
//     }

//     fn local_swap(&self, index: usize, val: T) -> T {
//         let mut slice = self.local_as_mut_slice(); //this locks the array
//         let orig = slice[index];
//         slice[index] = val;
//         orig
//     }
// }
// // }

// #[macro_export]
// macro_rules! CollectiveAtomicArray_create_ops {
//     ($a:ty, $name:ident) => {
//         paste::paste!{
//             $crate::collectiveatomicarray_register!{$a,ArrayOpCmd::Add,[<$name dist_add>],[<$name local_add>]}
//             $crate::collectiveatomicarray_register!{$a,ArrayOpCmd::FetchAdd,[<$name dist_fetch_add>],[<$name local_add>]}
//             $crate::collectiveatomicarray_register!{$a,ArrayOpCmd::Sub,[<$name dist_sub>],[<$name local_sub>]}
//             $crate::collectiveatomicarray_register!{$a,ArrayOpCmd::FetchSub,[<$name dist_fetch_sub>],[<$name local_sub>]}
//             $crate::collectiveatomicarray_register!{$a,ArrayOpCmd::Mul,[<$name dist_mul>],[<$name local_mul>]}
//             $crate::collectiveatomicarray_register!{$a,ArrayOpCmd::FetchMul,[<$name dist_fetch_mul>],[<$name local_mul>]}
//             $crate::collectiveatomicarray_register!{$a,ArrayOpCmd::Div,[<$name dist_div>],[<$name local_div>]}
//             $crate::collectiveatomicarray_register!{$a,ArrayOpCmd::FetchDiv,[<$name dist_fetch_div>],[<$name local_div>]}

//         }
//     }
// }

// #[macro_export]
// macro_rules! CollectiveAtomicArray_create_bitwise_ops {
//     ($a:ty, $name:ident) => {
//         paste::paste!{
//             $crate::collectiveatomicarray_register!{$a,ArrayOpCmd::And,[<$name dist_bit_and>],[<$name local_bit_and>]}
//             $crate::collectiveatomicarray_register!{$a,ArrayOpCmd::FetchAnd,[<$name dist_fetch_bit_and>],[<$name local_bit_and>]}
//             $crate::collectiveatomicarray_register!{$a,ArrayOpCmd::Or,[<$name dist_bit_or>],[<$name local_bit_or>]}
//             $crate::collectiveatomicarray_register!{$a,ArrayOpCmd::FetchOr,[<$name dist_fetch_bit_or>],[<$name local_bit_or>]}
//         }
//     }
// }

// #[macro_export]
// macro_rules! CollectiveAtomicArray_create_atomic_ops {
//     ($a:ty, $name:ident) => {
//         paste::paste!{
//             $crate::collectiveatomicarray_register!{$a,ArrayOpCmd::Store,[<$name dist_store>],[<$name local_store>]}
//             $crate::collectiveatomicarray_register!{$a,ArrayOpCmd::Load,[<$name dist_load>],[<$name local_load>]}
//             $crate::collectiveatomicarray_register!{$a,ArrayOpCmd::Swap,[<$name dist_swap>],[<$name local_swap>]}
//         }
//     }
// }
// #[macro_export]
// macro_rules! collectiveatomicarray_register {
//     ($id:ident, $optype:path, $op:ident, $local:ident) => {
//         inventory::submit! {
//             #![crate =$crate]
//             $crate::array::CollectiveAtomicArrayOp{
//                 id: ($optype,std::any::TypeId::of::<$id>()),
//                 op: $op,
//             }
//         }
//     };
// }
