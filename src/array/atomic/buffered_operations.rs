use crate::active_messaging::*;
use crate::array::atomic::*;
use crate::array::*;
use crate::lamellar_request::LamellarRequest;
use crate::memregion::Dist;

use std::any::TypeId;
use std::collections::HashMap;

type OpFn = fn(*const u8, AtomicByteArray, usize) -> LamellarArcAm;
type LocalOpFn = fn(*mut u8, AtomicByteArray, usize);
// type DistOpFn = fn(*const u8, AtomicByteArray, usize);

lazy_static! {
    pub(crate) static ref OPS: HashMap<(ArrayOpCmd, TypeId), (OpFn, LocalOpFn)> = {
        let mut map = HashMap::new();
        for op in crate::inventory::iter::<AtomicArrayOp> {
            map.insert(op.id.clone(), (op.op, op.local));
        }
        map
    };
}

pub struct AtomicArrayOp {
    pub id: (ArrayOpCmd, TypeId),
    pub op: OpFn,
    pub local: LocalOpFn,
}

crate::inventory::collect!(AtomicArrayOp);

type BufFn = fn(AtomicByteArray) -> Arc<dyn BufferOp>;

lazy_static! {
    pub(crate) static ref BUFOPS: HashMap<TypeId, BufFn> = {
        let mut map = HashMap::new();
        for op in crate::inventory::iter::<AtomicArrayOpBuf> {
            map.insert(op.id.clone(), op.op);
        }
        map
    };
}

pub struct AtomicArrayOpBuf {
    pub id: TypeId,
    pub op: BufFn,
}

crate::inventory::collect!(AtomicArrayOpBuf);

// impl<T: Dist + 'static> AtomicElement<T> {
//     fn local_op(&self, val: T, op: ArrayOpCmd) -> T {
//         if let Some(funcs) = OPS.get(&(op, TypeId::of::<T>())) {
//             let mut val = val;
//             let array: AtomicByteArray = self.array.clone().into();
//             funcs.1(&mut val as *mut T as *mut u8, array, self.local_index);
//             val
//         } else {
//             panic!("type has not been registered");
//         }
//     }
// }

// impl<T: ElementOps + 'static> AtomicElement<T> {
//     pub fn load(&self) -> T {
//         self.local_op(
//             unsafe { self.array.__local_as_slice()[0] },
//             ArrayOpCmd::Load,
//         )
//     }

//     pub fn store(&self, val: T) {
//         self.local_op(val, ArrayOpCmd::Store);
//     }

//     pub fn swap(&self, val: T) -> T {
//         self.local_op(val, ArrayOpCmd::Swap)
//     }
// }

// impl<T: ElementArithmeticOps + 'static> AtomicElement<T> {
//     pub fn add(&self, val: T) {
//         self.local_op(val, ArrayOpCmd::Add);
//     }
//     pub fn fetch_add(&self, val: T) -> T {
//         self.local_op(val, ArrayOpCmd::FetchAdd)
//     }
//     pub fn sub(&self, val: T) {
//         self.local_op(val, ArrayOpCmd::Sub);
//     }
//     pub fn fetch_sub(&self, val: T) -> T {
//         self.local_op(val, ArrayOpCmd::FetchSub)
//     }
//     pub fn mul(&self, val: T) {
//         self.local_op(val, ArrayOpCmd::Mul);
//     }
//     pub fn fetch_mul(&self, val: T) -> T {
//         self.local_op(val, ArrayOpCmd::FetchMul)
//     }
//     pub fn div(&self, val: T) {
//         self.local_op(val, ArrayOpCmd::Div);
//     }
//     pub fn fetch_div(&self, val: T) -> T {
//         self.local_op(val, ArrayOpCmd::FetchDiv)
//     }
// }

// impl<T: ElementBitWiseOps + 'static> AtomicElement<T> {
//     pub fn bit_and(&self, val: T) {
//         self.local_op(val, ArrayOpCmd::And);
//     }
//     pub fn fetch_bit_and(&self, val: T) -> T {
//         self.local_op(val, ArrayOpCmd::FetchAnd)
//     }
//     pub fn bit_or(&self, val: T) {
//         self.local_op(val, ArrayOpCmd::Or);
//     }
//     pub fn fetch_bit_or(&self, val: T) -> T {
//         self.local_op(val, ArrayOpCmd::FetchOr)
//     }
// }

impl<T: AmDist + Dist + 'static> AtomicArray<T> {
    fn initiate_op<'a>(
        &self,
        val: T,
        index: impl OpInput<'a, usize>,
        op: ArrayOpCmd,
    ) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.initiate_op(val, index, op),
            AtomicArray::GenericAtomicArray(array) => array.initiate_op(val, index, op),
        }
    }

    fn initiate_fetch_op<'a>(
        &self,
        val: T,
        index: impl OpInput<'a, usize>,
        op: ArrayOpCmd,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>> + Send + Sync> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.initiate_fetch_op(val, index, op),
            AtomicArray::GenericAtomicArray(array) => array.initiate_fetch_op(val, index, op),
        }
    }

    pub fn load<'a>(
        &self,
        index: impl OpInput<'a, usize>,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>> + Send + Sync> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.load(index),
            AtomicArray::GenericAtomicArray(array) => array.load(index),
        }
    }

    pub fn store(
        &self,
        index: usize,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.store(index, val),
            AtomicArray::GenericAtomicArray(array) => array.store(index, val),
        }
    }

    pub fn swap(
        &self,
        index: usize,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>> + Send + Sync> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.swap(index, val),
            AtomicArray::GenericAtomicArray(array) => array.swap(index, val),
        }
    }
}

impl<T: ElementArithmeticOps + 'static> ArithmeticOps<T> for AtomicArray<T> {
    fn add<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync> {
        // println!("atomic add");
        self.initiate_op(val, index, ArrayOpCmd::Add)
    }
    fn fetch_add<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>> + Send + Sync> {
        self.initiate_fetch_op(val, index, ArrayOpCmd::FetchAdd)
    }
    fn sub<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync> {
        self.initiate_op(val, index, ArrayOpCmd::Sub)
    }
    fn fetch_sub<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>> + Send + Sync> {
        self.initiate_fetch_op(val, index, ArrayOpCmd::FetchSub)
    }
    fn mul<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync> {
        self.initiate_op(val, index, ArrayOpCmd::Mul)
    }
    fn fetch_mul<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>> + Send + Sync> {
        self.initiate_fetch_op(val, index, ArrayOpCmd::FetchMul)
    }
    fn div<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync> {
        self.initiate_op(val, index, ArrayOpCmd::Div)
    }
    fn fetch_div<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>> + Send + Sync> {
        self.initiate_fetch_op(val, index, ArrayOpCmd::FetchDiv)
    }
}

impl<T: ElementBitWiseOps + 'static> BitWiseOps<T> for AtomicArray<T> {
    fn bit_and<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync> {
        // println!("and val {:?}",val);
        self.initiate_op(val, index, ArrayOpCmd::And)
    }
    fn fetch_bit_and<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>> + Send + Sync> {
        self.initiate_fetch_op(val, index, ArrayOpCmd::FetchAnd)
    }
    fn bit_or<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync> {
        // println!("or");
        self.initiate_op(val, index, ArrayOpCmd::Or)
    }
    fn fetch_bit_or<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>> + Send + Sync> {
        // println!("fetch_or");
        self.initiate_fetch_op(val, index, ArrayOpCmd::FetchOr)
    }
}

// #[macro_export]
// macro_rules! atomic_ops {
//     ($a:ty, $name:ident) => {
//         impl LocalArithmeticOps<$a> for AtomicArray<$a> {
//             fn local_fetch_add(&self, index: usize, val: $a) -> $a {
//                 // println!("LocalArithmeticOps native {}local_add func ", stringify!($name));
//                 use $crate::array::AtomicOps;
//                 unsafe { self.__local_as_mut_slice()[index].fetch_add(val) }
//             }
//             fn local_fetch_sub(&self, index: usize, val: $a) -> $a {
//                 use $crate::array::AtomicOps;
//                 unsafe { self.__local_as_mut_slice()[index].fetch_sub(val) }
//             }
//             fn local_fetch_mul(&self, index: usize, val: $a) -> $a {
//                 use $crate::array::AtomicOps;
//                 unsafe { self.__local_as_mut_slice()[index].fetch_mul(val) }
//             }
//             fn local_fetch_div(&self, index: usize, val: $a) -> $a {
//                 use $crate::array::AtomicOps;
//                 unsafe { self.__local_as_mut_slice()[index].fetch_div(val) }
//             }
//         }

//         paste::paste!{
//             #[allow(non_snake_case)]
//             fn [<$name local_add>](val: *mut u8, array: $crate::array::AtomicByteArray, index: usize) {
//                 let typed_val = unsafe { *(val as *mut $a) };
//                 // println!("native {}local_add func ", stringify!($name));
//                 let array: AtomicArray<$a> = array.into();
//                 use $crate::array::AtomicOps;
//                 unsafe { *(val as *mut $a) = array.__local_as_mut_slice()[index].fetch_add(typed_val) };
//             }
//             $crate::atomicarray_register!{ $a,ArrayOpCmd::Add,[<$name dist_add>],[<$name local_add>] }
//             $crate::atomicarray_register!{ $a,ArrayOpCmd::FetchAdd,[<$name dist_fetch_add>],[<$name local_add>] }

//             #[allow(non_snake_case)]
//             fn [<$name local_sub>](val: *mut u8, array: $crate::array::AtomicByteArray, index: usize) {
//                 let typed_val = unsafe { *(val as *mut $a) };
//                 // println!("native {}local_sub func ", stringify!($name));
//                 let array: AtomicArray<$a> = array.into();
//                 use $crate::array::AtomicOps;
//                 unsafe { *(val as *mut $a) = array.__local_as_mut_slice()[index].fetch_sub(typed_val) };
//             }
//             $crate::atomicarray_register!{ $a,ArrayOpCmd::Sub,[<$name dist_sub>],[<$name local_sub>] }
//             $crate::atomicarray_register!{ $a,ArrayOpCmd::FetchSub,[<$name dist_fetch_sub>],[<$name local_sub>] }

//             #[allow(non_snake_case)]
//             fn [<$name local_mul>](val: *mut u8, array: $crate::array::AtomicByteArray, index: usize) {
//                 let typed_val = unsafe { *(val as *mut $a) };
//                 // println!("native {}local_mul func ", stringify!($name));
//                 let array: AtomicArray<$a> = array.into();
//                 use $crate::array::AtomicOps;
//                 unsafe { *(val as *mut $a) = array.__local_as_mut_slice()[index].fetch_mul(typed_val) };
//             }
//             $crate::atomicarray_register!{ $a,ArrayOpCmd::Mul,[<$name dist_mul>],[<$name local_mul>] }
//             $crate::atomicarray_register!{ $a,ArrayOpCmd::FetchMul,[<$name dist_fetch_mul>],[<$name local_mul>] }

//             #[allow(non_snake_case)]
//             fn [<$name local_div>](val: *mut u8, array: $crate::array::AtomicByteArray, index: usize) {
//                 let typed_val = unsafe { *(val as *mut $a) };
//                 // println!("native {}local_mul func ", stringify!($name));
//                 let array: AtomicArray<$a> = array.into();
//                 use $crate::array::AtomicOps;
//                 unsafe { *(val as *mut $a) = array.__local_as_mut_slice()[index].fetch_div(typed_val) };
//             }
//             $crate::atomicarray_register!{ $a,ArrayOpCmd::Div,[<$name dist_div>],[<$name local_div>] }
//             $crate::atomicarray_register!{ $a,ArrayOpCmd::FetchDiv,[<$name dist_fetch_div>],[<$name local_div>] }
//         }
//     };
// }
// #[macro_export]
// macro_rules! atomic_bitwise_ops {
//     ($a:ty, $name:ident) => {
//         impl LocalBitWiseOps<$a> for AtomicArray<$a> {
//             // fn local_bit_and(&self, index: usize, val: $a) {
//             //     self.local_fetch_bit_and(index,val);
//             // }
//             fn local_fetch_bit_and(&self, index: usize, val: $a) -> $a {
//                 use $crate::array::AtomicOps;
//                 unsafe { let temp = self.__local_as_mut_slice()[index].fetch_bit_and(val);
//                 // println!("and temp: {:?} {:?} ",temp,self.__local_as_mut_slice()[index]);
//                 temp}
//             }
//             // fn local_bit_or(&self, index: usize, val: $a) {
//             //     self.local_fetch_bit_or(index,val);
//             // }
//             fn local_fetch_bit_or(&self, index: usize, val: $a) -> $a {
//                 use $crate::array::AtomicOps;
//                 unsafe {let temp =  self.__local_as_mut_slice()[index].fetch_bit_or(val);
//                 // println!("or temp: {:?} {:?} ",temp,self.__local_as_mut_slice()[index]);
//                 temp}
//             }
//         }

//         paste::paste!{
//             #[allow(non_snake_case)]
//             fn [<$name local_bit_and>](val: *mut u8, array: $crate::array::AtomicByteArray, index: usize) {
//                 let typed_val = unsafe { *(val as *mut $a) };
//                 // println!("native {}local_mul func ", stringify!($name));
//                 let array: AtomicArray<$a> = array.into();
//                 use $crate::array::AtomicOps;
//                 unsafe { *(val as *mut $a) = array.__local_as_mut_slice()[index].fetch_bit_and(typed_val)};
//             }
//             $crate::atomicarray_register!{ $a,ArrayOpCmd::And,[<$name dist_bit_and>],[<$name local_bit_and>] }
//             $crate::atomicarray_register!{ $a,ArrayOpCmd::FetchAnd,[<$name dist_fetch_bit_and>],[<$name local_bit_and>] }

//             #[allow(non_snake_case)]
//             fn [<$name local_bit_or>](val: *mut u8, array: $crate::array::AtomicByteArray, index: usize) {
//                 let typed_val = unsafe { *(val as *mut $a) };
//                 // println!("native {}local_mul func ", stringify!($name));
//                 let array: AtomicArray<$a> = array.into();
//                 use $crate::array::AtomicOps;
//                 unsafe { *(val as *mut $a) = array.__local_as_mut_slice()[index].fetch_bit_or(typed_val)};
//             }
//             $crate::atomicarray_register!{ $a,ArrayOpCmd::Or,[<$name dist_bit_or>],[<$name local_bit_or>] }
//             $crate::atomicarray_register!{ $a,ArrayOpCmd::FetchOr,[<$name dist_fetch_bit_or>],[<$name local_bit_or>] }
//         }
//     };
// }

// #[macro_export]
// macro_rules! atomic_misc_ops {
//     ($a:ty, $name:ident) => {
//         impl LocalAtomicOps<$a> for AtomicArray<$a> {
//             fn local_load(&self, index: usize, _val: $a) -> $a {
//                 use $crate::array::AtomicOps;
//                 unsafe { self.__local_as_mut_slice()[index].load() }
//             }
//             fn local_store(&self,index: usize, val: $a) {
//                 use $crate::array::AtomicOps;
//                 // println!("1. local store {:?} {:?}",index, val);
//                 unsafe { self.__local_as_mut_slice()[index].store(val); }
//             }
//             fn local_swap(&self,index: usize, val: $a) -> $a {
//                 use $crate::array::AtomicOps;
//                 unsafe { self.__local_as_mut_slice()[index].swap(val) }
//             }
//         }
//         paste::paste!{
//             #[allow(non_snake_case)]
//             fn [<$name local_load>](val: *mut u8, array: $crate::array::AtomicByteArray, index: usize) {
//                 // let typed_val = unsafe { *(val as *mut $a) };
//                 // println!("native {}local_mul func ", stringify!($name));
//                 let array: AtomicArray<$a> = array.into();
//                 use $crate::array::AtomicOps;
//                 unsafe { *(val as *mut $a) = array.__local_as_mut_slice()[index].load() };
//             }
//             $crate::atomicarray_register!{ $a,ArrayOpCmd::Load,[<$name dist_load>],[<$name local_load>] }

//             #[allow(non_snake_case)]
//             fn [<$name local_store>](val: *mut u8, array: $crate::array::AtomicByteArray, index: usize) {
//                 let typed_val = unsafe { *(val as *mut $a) };
//                 // println!("native {}local_mul func ", stringify!($name));
//                 let array: AtomicArray<$a> = array.into();
//                 use $crate::array::AtomicOps;
//                 // println!("2. local store {:?} {:?}",index, typed_val);
//                 unsafe { array.__local_as_mut_slice()[index].store(typed_val) };
//             }
//             $crate::atomicarray_register!{ $a,ArrayOpCmd::Store,[<$name dist_store>],[<$name local_store>] }

//             #[allow(non_snake_case)]
//             fn [<$name local_swap>](val: *mut u8, array: $crate::array::AtomicByteArray, index: usize) {
//                 let typed_val = unsafe { *(val as *mut $a) };
//                 // println!("native {}local_mul func ", stringify!($name));
//                 let array: AtomicArray<$a> = array.into();
//                 use $crate::array::AtomicOps;
//                 unsafe { *(val as *mut $a) = array.__local_as_mut_slice()[index].swap(typed_val) };
//             }
//             $crate::atomicarray_register!{ $a,ArrayOpCmd::Swap,[<$name dist_swap>],[<$name local_swap>] }
//         }
//     }
// }

// #[macro_export]
// macro_rules! non_atomic_ops {
//     ($a:ty, $name:ident) => {
//         impl LocalArithmeticOps<$a> for AtomicArray<$a> {
//             fn local_fetch_add(&self, index: usize, val: $a) -> $a {
//                 // println!(
//                 //     "mutex local_add LocalArithmeticOps<{}> for AtomicArray<{}>  ",
//                 //     stringify!($a),
//                 //     stringify!($a)
//                 // );
//                 let _lock = self.lock_index(index);
//                 unsafe {
//                     let orig  = self.__local_as_mut_slice()[index];
//                     self.__local_as_mut_slice()[index] += val;
//                     orig
//                 }

//             }
//             fn local_fetch_sub(&self, index: usize, val: $a) -> $a {
//                 // println!(
//                 //     "mutex local_sub LocalArithmeticOps<{}> for AtomicArray<{}>  ",
//                 //     stringify!($a),
//                 //     stringify!($a)
//                 // );
//                 let _lock = self.lock_index(index);
//                 unsafe {
//                     let orig  = self.__local_as_mut_slice()[index];
//                     self.__local_as_mut_slice()[index] -= val ;
//                     orig
//                 }
//             }
//             fn local_fetch_mul(&self, index: usize, val: $a) -> $a {
//                 let _lock = self.lock_index(index);
//                 unsafe {
//                     let orig  = self.__local_as_mut_slice()[index];
//                     self.__local_as_mut_slice()[index] *= val ;
//                     orig
//                 }
//             }
//             fn local_fetch_div(&self, index: usize, val: $a) -> $a {
//                 let _lock = self.lock_index(index);
//                 unsafe {
//                     let orig  = self.__local_as_mut_slice()[index];
//                     self.__local_as_mut_slice()[index] /= val ;
//                     orig
//                 }
//             }
//         }

//         paste::paste!{
//             #[allow(non_snake_case)]
//             fn [<$name local_add>](val: *mut u8, array: $crate::array::AtomicByteArray, index: usize) {
//                 let typed_val = unsafe { *(val as *mut $a) };
//                 let array: AtomicArray<$a> = array.into();
//                 // println!("mutex {}local_add func", stringify!($name));
//                 let _lock = array.lock_index(index).expect("no lock exists!");
//                 unsafe {
//                     *(val as *mut $a) = array.__local_as_mut_slice()[index];
//                     array.__local_as_mut_slice()[index] += typed_val;
//                 }
//             }
//             $crate::atomicarray_register!{$a,ArrayOpCmd::Add,[<$name dist_add>],[<$name local_add>]}
//             $crate::atomicarray_register!{$a,ArrayOpCmd::FetchAdd,[<$name dist_fetch_add>],[<$name local_add>]}

//             #[allow(non_snake_case)]
//             fn [<$name local_sub>](val: *mut u8, array: $crate::array::AtomicByteArray, index: usize) {
//                 let typed_val = unsafe { *(val as *mut $a) };
//                 let array: AtomicArray<$a> = array.into();
//                 // println!("mutex {}local_sub func", stringify!($name));
//                 let _lock = array.lock_index(index).expect("no lock exists!");
//                 unsafe {
//                     *(val as *mut $a) = array.__local_as_mut_slice()[index];
//                     array.__local_as_mut_slice()[index] -= typed_val;
//                 }
//             }
//             $crate::atomicarray_register!{$a,ArrayOpCmd::Sub,[<$name dist_sub>],[<$name local_sub>]}
//             $crate::atomicarray_register!{$a,ArrayOpCmd::FetchSub,[<$name dist_fetch_sub>],[<$name local_sub>]}

//             #[allow(non_snake_case)]
//             fn [<$name local_mul>](val: *mut u8, array: $crate::array::AtomicByteArray, index: usize) {
//                 let typed_val = unsafe { *(val as *mut $a) };
//                 let array: AtomicArray<$a> = array.into();
//                 // println!("mutex {}local_mul func", stringify!($name));
//                 let _lock = array.lock_index(index).expect("no lock exists!");
//                 unsafe {
//                     *(val as *mut $a) = array.__local_as_mut_slice()[index];
//                     array.__local_as_mut_slice()[index] *= typed_val;
//                 }
//             }
//             $crate::atomicarray_register!{$a,ArrayOpCmd::Mul,[<$name dist_mul>],[<$name local_mul>]}
//             $crate::atomicarray_register!{$a,ArrayOpCmd::FetchMul,[<$name dist_fetch_mul>],[<$name local_mul>]}

//             #[allow(non_snake_case)]
//             fn [<$name local_div>](val: *mut u8, array: $crate::array::AtomicByteArray, index: usize) {
//                 let typed_val = unsafe { *(val as *mut $a) };
//                 let array: AtomicArray<$a> = array.into();
//                 // println!("mutex {}local_mul func", stringify!($name));
//                 let _lock = array.lock_index(index).expect("no lock exists!");
//                 unsafe {
//                     *(val as *mut $a) = array.__local_as_mut_slice()[index];
//                     array.__local_as_mut_slice()[index] /= typed_val;
//                 }
//             }
//             $crate::atomicarray_register!{$a,ArrayOpCmd::Div,[<$name dist_div>],[<$name local_div>]}
//             $crate::atomicarray_register!{$a,ArrayOpCmd::FetchDiv,[<$name dist_fetch_div>],[<$name local_div>]}
//         }
//     };
// }

// #[macro_export]
// macro_rules! non_atomic_bitwise_ops {
//     ($a:ty, $name:ident) => {
//         impl LocalBitWiseOps<$a> for AtomicArray<$a> {
//             fn local_fetch_bit_and(&self, index: usize, val: $a) -> $a {
//                 // println!(
//                 //     "mutex local_add LocalArithmeticOps<{}> for AtomicArray<{}>  ",
//                 //     stringify!($a),
//                 //     stringify!($a)
//                 // );
//                 let _lock = self.lock_index(index);
//                 unsafe {
//                     let orig  = self.__local_as_mut_slice()[index];
//                     self.__local_as_mut_slice()[index] &= val;
//                     orig
//                 }

//             }
//             fn local_fetch_bit_or(&self, index: usize, val: $a) -> $a {
//                 // println!(
//                 //     "mutex local_sub LocalArithmeticOps<{}> for AtomicArray<{}>  ",
//                 //     stringify!($a),
//                 //     stringify!($a)
//                 // );
//                 let _lock = self.lock_index(index);
//                 unsafe {
//                     let orig  = self.__local_as_mut_slice()[index];
//                     self.__local_as_mut_slice()[index] |= val ;
//                     orig
//                 }
//             }
//         }

//         paste::paste!{
//             #[allow(non_snake_case)]
//             fn [<$name local_bit_and>](val: *mut u8, array: $crate::array::AtomicByteArray, index: usize) {
//                 let typed_val = unsafe { *(val as *mut $a) };
//                 let array: AtomicArray<$a> = array.into();
//                 let _lock = array.lock_index(index).expect("no lock exists!");
//                 unsafe {
//                     *(val as *mut $a) = array.__local_as_mut_slice()[index];
//                     array.__local_as_mut_slice()[index] &= typed_val;
//                 }
//             }
//             $crate::atomicarray_register!{$a,ArrayOpCmd::And,[<$name dist_bit_and>],[<$name local_bit_and>]}
//             $crate::atomicarray_register!{$a,ArrayOpCmd::FetchAnd,[<$name dist_fetch_bit_and>],[<$name local_bit_and>]}

//             #[allow(non_snake_case)]
//             fn [<$name local_bit_or>](val: *mut u8, array: $crate::array::AtomicByteArray, index: usize) {
//                 let typed_val = unsafe { *(val as *mut $a) };
//                 let array: AtomicArray<$a> = array.into();
//                 // println!("mutex {}local_sub func", stringify!($name));
//                 let _lock = array.lock_index(index).expect("no lock exists!");
//                 unsafe {
//                     *(val as *mut $a) = array.__local_as_mut_slice()[index];
//                     array.__local_as_mut_slice()[index] |= typed_val;
//                 }
//             }
//             $crate::atomicarray_register!{$a,ArrayOpCmd::Or,[<$name dist_bit_or>],[<$name local_bit_or>]}
//             $crate::atomicarray_register!{$a,ArrayOpCmd::FetchOr,[<$name dist_fetch_bit_or>],[<$name local_bit_or>]}
//         }
//     };
// }

// #[macro_export]
// macro_rules! non_atomic_misc_ops {
//     ($a:ty, $name:ident) => {
//         impl LocalAtomicOps<$a> for AtomicArray<$a> {
//             fn local_load(&self, index: usize, _val: $a) -> $a {
//                 let _lock = self.lock_index(index);
//                 unsafe {
//                     self.__local_as_mut_slice()[index]
//                 }
//             }
//             fn local_store(&self, index: usize, val: $a) {
//                 let _lock = self.lock_index(index);
//                 unsafe {
//                     self.__local_as_mut_slice()[index] = val;
//                 }
//             }
//             fn local_swap(&self, index: usize, val: $a) -> $a {
//                 let _lock = self.lock_index(index);
//                 unsafe {
//                     let orig  = self.__local_as_mut_slice()[index];
//                     self.__local_as_mut_slice()[index] = val;
//                     orig
//                 }
//             }
//         }
//         paste::paste!{
//             #[allow(non_snake_case)]
//             fn [<$name local_load>](val: *mut u8, array: $crate::array::AtomicByteArray, index: usize) {
//                 // let typed_val = unsafe { *(val as *mut $a) };
//                 let array: AtomicArray<$a> = array.into();
//                 // println!("mutex {}local_mul func", stringify!($name));
//                 let _lock = array.lock_index(index).expect("no lock exists!");
//                 unsafe {
//                     *(val as *mut $a) = array.__local_as_mut_slice()[index];
//                 }
//             }
//             $crate::atomicarray_register!{$a,ArrayOpCmd::Load,[<$name dist_load>],[<$name local_load>]}

//             #[allow(non_snake_case)]
//             fn [<$name local_store>](val: *mut u8, array: $crate::array::AtomicByteArray, index: usize) {
//                 let typed_val = unsafe { *(val as *mut $a) };
//                 let array: AtomicArray<$a> = array.into();
//                 // println!("mutex {}local_mul func", stringify!($name));
//                 let _lock = array.lock_index(index).expect("no lock exists!");
//                 unsafe {
//                     array.__local_as_mut_slice()[index] = typed_val;
//                 }
//             }
//             $crate::atomicarray_register!{$a,ArrayOpCmd::Store,[<$name dist_store>],[<$name local_store>]}

//             #[allow(non_snake_case)]
//             fn [<$name local_swap>](val: *mut u8, array: $crate::array::AtomicByteArray, index: usize) {
//                 let typed_val = unsafe { *(val as *mut $a) };
//                 let array: AtomicArray<$a> = array.into();
//                 // println!("mutex {}local_mul func", stringify!($name));
//                 let _lock = array.lock_index(index).expect("no lock exists!");
//                 unsafe {
//                     *(val as *mut $a) = array.__local_as_mut_slice()[index];
//                     array.__local_as_mut_slice()[index] = typed_val;
//                 }
//             }
//             $crate::atomicarray_register!{$a,ArrayOpCmd::Swap,[<$name dist_swap>],[<$name local_swap>]}
//         }
//     }
// }

// #[macro_export]
// macro_rules! AtomicArray_create_ops {
//     (u8, $name:ident) => {
//         $crate::atomic_ops!(u8, $name);
//     };
//     (u16, $name:ident) => {
//         $crate::atomic_ops!(u16, $name);
//     };
//     (u32, $name:ident) => {
//         $crate::atomic_ops!(u32, $name);
//     };
//     (u64, $name:ident) => {
//         $crate::atomic_ops!(u64, $name);
//     };
//     (usize, $name:ident) => {
//         $crate::atomic_ops!(usize, $name);
//     };
//     (i8, $name:ident) => {
//         $crate::atomic_ops!(i8, $name);
//     };
//     (i16, $name:ident) => {
//         $crate::atomic_ops!(i16, $name);
//     };
//     (i32, $name:ident) => {
//         $crate::atomic_ops!(i32, $name);
//     };
//     (i64, $name:ident) => {
//         $crate::atomic_ops!(i64, $name);
//     };
//     (isize, $name:ident) => {
//         $crate::atomic_ops!(isize, $name);
//     };
//     ($a:ty, $name:ident) => {
//         $crate::non_atomic_ops!($a, $name);
//     };
// }

// #[macro_export]
// macro_rules! AtomicArray_create_bitwise_ops {
//     (u8, $name:ident) => {
//         $crate::atomic_bitwise_ops!(u8, $name);
//     };
//     (u16, $name:ident) => {
//         $crate::atomic_bitwise_ops!(u16, $name);
//     };
//     (u32, $name:ident) => {
//         $crate::atomic_bitwise_ops!(u32, $name);
//     };
//     (u64, $name:ident) => {
//         $crate::atomic_bitwise_ops!(u64, $name);
//     };
//     (usize, $name:ident) => {
//         $crate::atomic_bitwise_ops!(usize, $name);
//     };
//     (i8, $name:ident) => {
//         $crate::atomic_bitwise_ops!(i8, $name);
//     };
//     (i16, $name:ident) => {
//         $crate::atomic_bitwise_ops!(i16, $name);
//     };
//     (i32, $name:ident) => {
//         $crate::atomic_bitwise_ops!(i32, $name);
//     };
//     (i64, $name:ident) => {
//         $crate::atomic_bitwise_ops!(i64, $name);
//     };
//     (isize, $name:ident) => {
//         $crate::atomic_bitwise_ops!(isize, $name);
//     };
//     ($a:ty, $name:ident) => {
//         $crate::non_atomic_bitwise_ops!($a, $name);
//     };
// }

// #[macro_export]
// macro_rules! AtomicArray_create_atomic_ops {
//     (u8, $name:ident) => {
//         $crate::atomic_misc_ops!(u8, $name);
//     };
//     (u16, $name:ident) => {
//         $crate::atomic_misc_ops!(u16, $name);
//     };
//     (u32, $name:ident) => {
//         $crate::atomic_misc_ops!(u32, $name);
//     };
//     (u64, $name:ident) => {
//         $crate::atomic_misc_ops!(u64, $name);
//     };
//     (usize, $name:ident) => {
//         $crate::atomic_misc_ops!(usize, $name);
//     };
//     (i8, $name:ident) => {
//         $crate::atomic_misc_ops!(i8, $name);
//     };
//     (i16, $name:ident) => {
//         $crate::atomic_misc_ops!(i16, $name);
//     };
//     (i32, $name:ident) => {
//         $crate::atomic_misc_ops!(i32, $name);
//     };
//     (i64, $name:ident) => {
//         $crate::atomic_misc_ops!(i64, $name);
//     };
//     (isize, $name:ident) => {
//         $crate::atomic_misc_ops!(isize, $name);
//     };
//     ($a:ty, $name:ident) => {
//         $crate::non_atomic_misc_ops!($a, $name);
//     };
// }

// #[macro_export]
// macro_rules! atomicarray_register {
//     ($id:ident, $optype:path, $op:ident, $local:ident) => {
//         inventory::submit! {
//             #![crate = $crate]
//             $crate::array::AtomicArrayOp{
//                 id: ($optype,std::any::TypeId::of::<$id>()),
//                 op: $op,
//                 local: $local
//             }
//         }
//     };
// }
