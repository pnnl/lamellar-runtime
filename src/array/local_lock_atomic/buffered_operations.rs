use crate::array::local_lock_atomic::*;
use crate::array::*;
use std::any::TypeId;
use std::collections::HashMap;

type BufFn = fn(LocalLockAtomicByteArrayWeak) -> Arc<dyn BufferOp>;

lazy_static! {
    pub(crate) static ref BUFOPS: HashMap<TypeId, BufFn> = {
        let mut map = HashMap::new();
        for op in crate::inventory::iter::<LocalLockAtomicArrayOpBuf> {
            map.insert(op.id.clone(), op.op);
        }
        map
    };
}

pub struct LocalLockAtomicArrayOpBuf {
    pub id: TypeId,
    pub op: BufFn,
}

crate::inventory::collect!(LocalLockAtomicArrayOpBuf);

impl<T: ElementOps + 'static> AccessOps<T> for LocalLockAtomicArray<T> {}

impl<T: ElementArithmeticOps + 'static> ArithmeticOps<T> for LocalLockAtomicArray<T> {}

impl<T: ElementBitWiseOps + 'static> BitWiseOps<T> for LocalLockAtomicArray<T> {}

impl<T: ElementCompareEqOps + 'static> CompareExchangeOps<T> for LocalLockAtomicArray<T> {}

impl<T: ElementComparePartialEqOps + 'static> CompareExchangeEpsilonOps<T>
    for LocalLockAtomicArray<T>
{
}

// // impl<T: Dist + std::ops::AddAssign> LocalLockAtomicArray<T> {
// impl<T: ElementArithmeticOps> LocalArithmeticOps<T> for LocalLockAtomicArray<T> {
//     fn local_fetch_add(&self, index: impl OpInput<'a,usize>, val: T) -> T {
//         // println!("local_add LocalArithmeticOps<T> for LocalLockAtomicArray<T> ");
//         // let _lock = self.lock.write();
//         let mut slice = self.local_as_mut_slice(); //this locks the array
//         let orig = slice[index]; //this locks the
//         slice[index] += val;
//         orig
//     }
//     fn local_fetch_sub(&self, index: impl OpInput<'a,usize>, val: T) -> T {
//         // println!("local_sub LocalArithmeticOps<T> for LocalLockAtomicArray<T> ");
//         let mut slice = self.local_as_mut_slice(); //this locks the array
//         let orig = slice[index];
//         slice[index] -= val;
//         orig
//     }
//     fn local_fetch_mul(&self, index: impl OpInput<'a,usize>, val: T) -> T {
//         // println!("local_sub LocalArithmeticOps<T> for LocalLockAtomicArray<T> ");
//         let mut slice = self.local_as_mut_slice(); //this locks the array
//         let orig = slice[index];
//         slice[index] *= val;
//         orig
//     }
//     fn local_fetch_div(&self, index: impl OpInput<'a,usize>, val: T) -> T {
//         // println!("local_sub LocalArithmeticOps<T> for LocalLockAtomicArray<T> ");
//         let mut slice = self.local_as_mut_slice(); //this locks the array
//         let orig = slice[index];
//         slice[index] /= val;
//         // println!("div i: {:?} {:?} {:?} {:?}",index,orig,val,self.local_as_mut_slice()[index]);
//         orig
//     }
// }
// impl<T: ElementBitWiseOps> LocalBitWiseOps<T> for LocalLockAtomicArray<T> {
//     fn local_fetch_bit_and(&self, index: impl OpInput<'a,usize>, val: T) -> T {
//         let mut slice = self.local_as_mut_slice(); //this locks the array
//         // println!("local_sub LocalArithmeticOps<T> for LocalLockAtomicArray<T> ");
//         let orig = slice[index];
//         slice[index] &= val;
//         // println!("and i: {:?} {:?} {:?} {:?}",index,orig,val,self.local_as_mut_slice()[index]);
//         orig
//     }
//     fn local_fetch_bit_or(&self, index: impl OpInput<'a,usize>, val: T) -> T {
//         let mut slice = self.local_as_mut_slice(); //this locks the array
//         // println!("local_sub LocalArithmeticOps<T> for LocalLockAtomicArray<T> ");
//         let orig = slice[index];
//         slice[index] |= val;
//         orig
//     }
// }
// impl<T: ElementOps> LocalAtomicOps<T> for LocalLockAtomicArray<T> {
//     fn local_load(&self, index: impl OpInput<'a,usize>, _val: T) -> T {
//         self.local_as_mut_slice()[index]
//     }

//     fn local_store(&self, index: impl OpInput<'a,usize>, val: T) {
//         self.local_as_mut_slice()[index] = val; //this locks the array
//     }

//     fn local_swap(&self, index: impl OpInput<'a,usize>, val: T) -> T {
//         let mut slice = self.local_as_mut_slice(); //this locks the array
//         let orig = slice[index];
//         slice[index] = val;
//         orig
//     }
// }
// // }

// #[macro_export]
// macro_rules! LocalLockAtomicArray_create_ops {
//     ($a:ty, $name:ident) => {
//         paste::paste!{
//             $crate::LocalLockAtomicArray_register!{$a,ArrayOpCmd::Add,[<$name dist_add>],[<$name local_add>]}
//             $crate::LocalLockAtomicArray_register!{$a,ArrayOpCmd::FetchAdd,[<$name dist_fetch_add>],[<$name local_add>]}
//             $crate::LocalLockAtomicArray_register!{$a,ArrayOpCmd::Sub,[<$name dist_sub>],[<$name local_sub>]}
//             $crate::LocalLockAtomicArray_register!{$a,ArrayOpCmd::FetchSub,[<$name dist_fetch_sub>],[<$name local_sub>]}
//             $crate::LocalLockAtomicArray_register!{$a,ArrayOpCmd::Mul,[<$name dist_mul>],[<$name local_mul>]}
//             $crate::LocalLockAtomicArray_register!{$a,ArrayOpCmd::FetchMul,[<$name dist_fetch_mul>],[<$name local_mul>]}
//             $crate::LocalLockAtomicArray_register!{$a,ArrayOpCmd::Div,[<$name dist_div>],[<$name local_div>]}
//             $crate::LocalLockAtomicArray_register!{$a,ArrayOpCmd::FetchDiv,[<$name dist_fetch_div>],[<$name local_div>]}

//         }
//     }
// }

// #[macro_export]
// macro_rules! LocalLockAtomicArray_create_bitwise_ops {
//     ($a:ty, $name:ident) => {
//         paste::paste!{
//             $crate::LocalLockAtomicArray_register!{$a,ArrayOpCmd::And,[<$name dist_bit_and>],[<$name local_bit_and>]}
//             $crate::LocalLockAtomicArray_register!{$a,ArrayOpCmd::FetchAnd,[<$name dist_fetch_bit_and>],[<$name local_bit_and>]}
//             $crate::LocalLockAtomicArray_register!{$a,ArrayOpCmd::Or,[<$name dist_bit_or>],[<$name local_bit_or>]}
//             $crate::LocalLockAtomicArray_register!{$a,ArrayOpCmd::FetchOr,[<$name dist_fetch_bit_or>],[<$name local_bit_or>]}
//         }
//     }
// }

// #[macro_export]
// macro_rules! LocalLockAtomicArray_create_atomic_ops {
//     ($a:ty, $name:ident) => {
//         paste::paste!{
//             $crate::LocalLockAtomicArray_register!{$a,ArrayOpCmd::Store,[<$name dist_store>],[<$name local_store>]}
//             $crate::LocalLockAtomicArray_register!{$a,ArrayOpCmd::Load,[<$name dist_load>],[<$name local_load>]}
//             $crate::LocalLockAtomicArray_register!{$a,ArrayOpCmd::Swap,[<$name dist_swap>],[<$name local_swap>]}
//         }
//     }
// }
// #[macro_export]
// macro_rules! LocalLockAtomicArray_register {
//     ($id:ident, $optype:path, $op:ident, $local:ident) => {
//         inventory::submit! {
//             #![crate =$crate]
//             $crate::array::LocalLockAtomicArrayOp{
//                 id: ($optype,std::any::TypeId::of::<$id>()),
//                 op: $op,
//             }
//         }
//     };
// }
