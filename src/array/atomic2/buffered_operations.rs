use crate::active_messaging::*;
use crate::array::atomic2::*;
use crate::array::*;
use crate::lamellar_request::LamellarRequest;
// use crate::memregion::Dist;
use std::any::TypeId;
use std::collections::HashMap;

type OpFn = fn(*const u8, Atomic2ByteArray, usize) -> LamellarArcAm;

lazy_static! {
    static ref OPS: HashMap<(ArrayOpCmd, TypeId), OpFn> = {
        let mut map = HashMap::new();
        for op in crate::inventory::iter::<Atomic2ArrayOp> {
            map.insert(op.id.clone(), op.op);
        }
        map
    };
}

pub struct Atomic2ArrayOp {
    pub id: (ArrayOpCmd, TypeId),
    pub op: OpFn,
}

crate::inventory::collect!(Atomic2ArrayOp);

type BufFn = fn(Atomic2ByteArray) -> Arc<dyn BufferOp>;

lazy_static! {
    pub(crate) static ref BUFOPS: HashMap<TypeId, BufFn> = {
        let mut map = HashMap::new();
        for op in crate::inventory::iter::<Atomic2ArrayOpBuf> {
            map.insert(op.id.clone(), op.op);
        }
        map
    };
}

pub struct Atomic2ArrayOpBuf {
    pub id: TypeId,
    pub op: BufFn,
}

crate::inventory::collect!(Atomic2ArrayOpBuf);

impl<T: AmDist + Dist + 'static> Atomic2Array<T> {
    fn initiate_op(
        &self,
        pe: usize,
        val: T,
        index: impl OpInput<usize>,
        op: ArrayOpCmd,
    ) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync> {
        let mut pe_offsets: HashMap<usize,Vec<usize>> = HashMap::new();
        let indices = index.as_op_input();
        for i in indices{
            let pe = self
            .pe_for_dist_index(i)
            .expect("index out of bounds");
            let local_index = self.pe_offset_for_dist_index(pe, i).unwrap(); //calculated pe above
            pe_offsets.entry(pe).or_insert(Vec::new()).push(local_index);
        }
        pe_offsets.iter().map(|(pe,vals)| self.array.initiate_op(*pe, val, &vals, op)).last().unwrap()
    }

    fn initiate_fetch_op(
        &self,
        pe: usize,
        val: T,
        local_index: usize,
        op: ArrayOpCmd,
    ) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        self.array.initiate_fetch_op(pe, val, local_index, op)
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

impl<T: ElementArithmeticOps + 'static> ArithmeticOps<T> for Atomic2Array<T> {
    fn add(
        &self,
        index: impl OpInput<usize>,
        val: T,
    ) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>> {
        // let pe = self.pe_for_dist_index(index).expect("index out of bounds");
        // let local_index = self.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        Some(self.initiate_op(0, val, index, ArrayOpCmd::Add))
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

impl<T: ElementBitWiseOps + 'static> BitWiseOps<T> for Atomic2Array<T> {
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

// // impl<T: Dist + std::ops::AddAssign> Atomic2Array<T> {
// impl<T: ElementArithmeticOps> LocalArithmeticOps<T> for Atomic2Array<T> {
//     fn local_fetch_add(&self, index: usize, val: T) -> T {
//         // println!("local_add LocalArithmeticOps<T> for Atomic2Array<T> ");
//         // let _lock = self.lock.write();
//         let mut slice = self.local_as_mut_slice(); //this locks the array
//         let orig = slice[index]; //this locks the
//         slice[index] += val;
//         orig
//     }
//     fn local_fetch_sub(&self, index: usize, val: T) -> T {
//         // println!("local_sub LocalArithmeticOps<T> for Atomic2Array<T> ");
//         let mut slice = self.local_as_mut_slice(); //this locks the array
//         let orig = slice[index];
//         slice[index] -= val;
//         orig
//     }
//     fn local_fetch_mul(&self, index: usize, val: T) -> T {
//         // println!("local_sub LocalArithmeticOps<T> for Atomic2Array<T> ");
//         let mut slice = self.local_as_mut_slice(); //this locks the array
//         let orig = slice[index];
//         slice[index] *= val;
//         orig
//     }
//     fn local_fetch_div(&self, index: usize, val: T) -> T {
//         // println!("local_sub LocalArithmeticOps<T> for Atomic2Array<T> ");
//         let mut slice = self.local_as_mut_slice(); //this locks the array
//         let orig = slice[index];
//         slice[index] /= val;
//         // println!("div i: {:?} {:?} {:?} {:?}",index,orig,val,self.local_as_mut_slice()[index]);
//         orig
//     }
// }
// impl<T: ElementBitWiseOps> LocalBitWiseOps<T> for Atomic2Array<T> {
//     fn local_fetch_bit_and(&self, index: usize, val: T) -> T {
//         let mut slice = self.local_as_mut_slice(); //this locks the array
//         // println!("local_sub LocalArithmeticOps<T> for Atomic2Array<T> ");
//         let orig = slice[index];
//         slice[index] &= val;
//         // println!("and i: {:?} {:?} {:?} {:?}",index,orig,val,self.local_as_mut_slice()[index]);
//         orig
//     }
//     fn local_fetch_bit_or(&self, index: usize, val: T) -> T {
//         let mut slice = self.local_as_mut_slice(); //this locks the array
//         // println!("local_sub LocalArithmeticOps<T> for Atomic2Array<T> ");
//         let orig = slice[index];
//         slice[index] |= val;
//         orig
//     }
// }
// impl<T: ElementOps> LocalAtomicOps<T> for Atomic2Array<T> {
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
// macro_rules! Atomic2Array_create_ops {
//     ($a:ty, $name:ident) => {
//         paste::paste!{
//             $crate::Atomic2Array_register!{$a,ArrayOpCmd::Add,[<$name dist_add>],[<$name local_add>]}
//             $crate::Atomic2Array_register!{$a,ArrayOpCmd::FetchAdd,[<$name dist_fetch_add>],[<$name local_add>]}
//             $crate::Atomic2Array_register!{$a,ArrayOpCmd::Sub,[<$name dist_sub>],[<$name local_sub>]}
//             $crate::Atomic2Array_register!{$a,ArrayOpCmd::FetchSub,[<$name dist_fetch_sub>],[<$name local_sub>]}
//             $crate::Atomic2Array_register!{$a,ArrayOpCmd::Mul,[<$name dist_mul>],[<$name local_mul>]}
//             $crate::Atomic2Array_register!{$a,ArrayOpCmd::FetchMul,[<$name dist_fetch_mul>],[<$name local_mul>]}
//             $crate::Atomic2Array_register!{$a,ArrayOpCmd::Div,[<$name dist_div>],[<$name local_div>]}
//             $crate::Atomic2Array_register!{$a,ArrayOpCmd::FetchDiv,[<$name dist_fetch_div>],[<$name local_div>]}

//         }
//     }
// }

// #[macro_export]
// macro_rules! Atomic2Array_create_bitwise_ops {
//     ($a:ty, $name:ident) => {
//         paste::paste!{
//             $crate::Atomic2Array_register!{$a,ArrayOpCmd::And,[<$name dist_bit_and>],[<$name local_bit_and>]}
//             $crate::Atomic2Array_register!{$a,ArrayOpCmd::FetchAnd,[<$name dist_fetch_bit_and>],[<$name local_bit_and>]}
//             $crate::Atomic2Array_register!{$a,ArrayOpCmd::Or,[<$name dist_bit_or>],[<$name local_bit_or>]}
//             $crate::Atomic2Array_register!{$a,ArrayOpCmd::FetchOr,[<$name dist_fetch_bit_or>],[<$name local_bit_or>]}
//         }
//     }
// }

// #[macro_export]
// macro_rules! Atomic2Array_create_atomic_ops {
//     ($a:ty, $name:ident) => {
//         paste::paste!{
//             $crate::Atomic2Array_register!{$a,ArrayOpCmd::Store,[<$name dist_store>],[<$name local_store>]}
//             $crate::Atomic2Array_register!{$a,ArrayOpCmd::Load,[<$name dist_load>],[<$name local_load>]}
//             $crate::Atomic2Array_register!{$a,ArrayOpCmd::Swap,[<$name dist_swap>],[<$name local_swap>]}
//         }
//     }
// }
// #[macro_export]
// macro_rules! Atomic2Array_register {
//     ($id:ident, $optype:path, $op:ident, $local:ident) => {
//         inventory::submit! {
//             #![crate =$crate]
//             $crate::array::Atomic2ArrayOp{
//                 id: ($optype,std::any::TypeId::of::<$id>()),
//                 op: $op,
//             }
//         }
//     };
// }
