use crate::active_messaging::*;
use crate::array::atomic::*;
use crate::array::*;
use crate::lamellar_request::LamellarRequest;
use crate::memregion::Dist;
use std::any::TypeId;
use std::collections::HashMap;

type OpFn = fn(*const u8, AtomicArray<u8>, usize, bool) -> LamellarArcAm;
type LocalOpFn = fn(*mut u8, AtomicArray<u8>, usize);
// type DistOpFn = fn(*const u8, AtomicArray<u8>, usize);


pub struct AtomicArrayOp {
    pub id: (ArrayOpCmd,TypeId),
    pub op: OpFn,
    pub local: LocalOpFn,
}

crate::inventory::collect!(AtomicArrayOp);

lazy_static! {
    static ref OPS: HashMap<(ArrayOpCmd,TypeId), (OpFn,LocalOpFn)> = {
        let mut map = HashMap::new();
        for op in crate::inventory::iter::<AtomicArrayOp> {
            map.insert(op.id.clone(),(op.op,op.local));
        }
        map
        // map.insert(TypeId::of::<f64>(), f64_add::add as AddFn );
    };
}

impl<T: ElementOps  + 'static> AtomicArray<T> {
    // pub fn fetch_add(&self, index: usize, val: T) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>>{
    //     self.add(index,val)
    // }
    // pub fn fetch_sub(&self, index: usize, val: T) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>>{
    //     self.sub(index,val)
    // }

    fn initiate_op(&self, index: usize, val: T, op: ArrayOpCmd)  -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>>{
        // println!("add ArrayOps<T> for &AtomicArray<T> ");
        if let Some(funcs) = OPS.get(&(op,TypeId::of::<T>())) {
            let pe = self.pe_for_dist_index(index);
            let local_index = self.pe_offset_for_dist_index(pe, index);
            let array: AtomicArray<u8> = unsafe { self.as_base_inner() };
            if pe == self.my_pe() {
                let mut val = val;
                funcs.1(&mut val as *mut T as *mut u8, array, local_index);
                None
            } else {
                let am = funcs.0(&val as *const T as *const u8, array, local_index,false);
                Some(self.array.dist_op(pe,am))
            }
        } else {
            let name = std::any::type_name::<T>().split("::").last().unwrap();
            panic!("the type {:?} has not been registered! this typically means you need to derive \"ArrayOps\" for the type . e.g. 
            #[derive(lamellar::ArrayOps)]
            struct {:?}{{
                ....
            }}
            note this also requires the type to impl Serialize + Deserialize, you can manually derive these, or use the lamellar::AmData attribute proc macro, e.g.
            #[lamellar::AMData(ArrayOps, any other traits you derive)]
            struct {:?}{{
                ....
            }}",name,name,name);
        }
    } 
    fn initiate_fetch_op(&self, index: usize, val: T, op: ArrayOpCmd)  -> Box<dyn LamellarRequest<Output = T> + Send + Sync>{
        // println!("add ArrayOps<T> for &AtomicArray<T> ");
        if let Some(funcs) = OPS.get(&(op,TypeId::of::<T>())) {
            let pe = self.pe_for_dist_index(index);
            let local_index = self.pe_offset_for_dist_index(pe, index);
            let array: AtomicArray<u8> = unsafe { self.as_base_inner() };
            if pe == self.my_pe() 
            {
                let mut val = val;
                funcs.1(&mut val as *mut T as *mut u8, array, local_index);
                Box::new(LocalOpResult{val})
            } else {
                let am = funcs.0(&val as *const T as *const u8, array, local_index,true);
                self.array.dist_fetch_op(pe,am)
            }
        } else {
            let name = std::any::type_name::<T>().split("::").last().unwrap();
            panic!("the type {:?} has not been registered! this typically means you need to derive \"ArrayOps\" for the type . e.g. 
            #[derive(lamellar::ArrayOps)]
            struct {:?}{{
                ....
            }}
            note this also requires the type to impl Serialize + Deserialize, you can manually derive these, or use the lamellar::AmData attribute proc macro, e.g.
            #[lamellar::AMData(ArrayOps, any other traits you derive)]
            struct {:?}{{
                ....
            }}",name,name,name);
        }
    } 
}


impl<T: ElementOps  + 'static> ArrayOps<T> for AtomicArray<T> {
    fn add(
        &self,
        index: usize,
        val: T,
    ) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>> {
        self.initiate_op(index,val,ArrayOpCmd::Add)
    }
    fn fetch_add(
        &self,
        index: usize,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        self.initiate_fetch_op(index,val,ArrayOpCmd::Add)
    }
    fn sub(
        &self,
        index: usize,
        val: T,
    ) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>> {
        self.initiate_op(index,val,ArrayOpCmd::Sub)
    }
    fn fetch_sub(
        &self,
        index: usize,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        self.initiate_fetch_op(index,val,ArrayOpCmd::Sub)
    }
    fn mul(
        &self,
        index: usize,
        val: T,
    ) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>> {
        self.initiate_op(index,val,ArrayOpCmd::Mul)
    }
    fn fetch_mul(
        &self,
        index: usize,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        self.initiate_fetch_op(index,val,ArrayOpCmd::Mul)
    }
}

// impl<T: Dist + ElementOps> ArrayLocalOps<T> for &AtomicArray<T> {
//     fn local_add(&self, index: usize, val: T) {
//         println!("local_add ArrayLocalOps<T> for &AtomicArray<T>");
//         let _lock = self.lock_index(index);
//         self.array.local_add(index, val);
//     }
//     fn local_sub(&self, index: usize, val: T) {
//         println!("local_sub ArrayLocalOps<T> for &AtomicArray<T>");
//         let _lock = self.lock_index(index);
//         self.array.local_sub(index, val);
//     }
// }

#[macro_export]
macro_rules! atomic_ops {
    ($a:ty, $name:ident) => {
        impl ArrayLocalOps<$a> for AtomicArray<$a> {
            fn local_add(&self, index: usize, val: $a) -> $a {
                // println!(
                //     "native local_add ArrayLocalOps<{}>  for AtomicArray<{}>  ",
                //     stringify!($a),
                //     stringify!($a)
                // );
                use $crate::array::AtomicOps;
                unsafe { self.local_as_mut_slice()[index].fetch_add(val) }
            }
            fn local_sub(&self, index: usize, val: $a) -> $a {
                // println!(
                //     "native local_sub ArrayLocalOps<{}>  for AtomicArray<{}>  ",
                //     stringify!($a),
                //     stringify!($a)
                // );
                use $crate::array::AtomicOps;
                unsafe { self.local_as_mut_slice()[index].fetch_sub(val) }
            }
            fn local_mul(&self, index: usize, val: $a) -> $a {
                // println!(                                       
                //     "native local_mul ArrayLocalOps<{}>  for AtomicArray<{}>  ",
                //     stringify!($a),
                //     stringify!($a)
                // );
                use $crate::array::AtomicOps;
                unsafe { self.local_as_mut_slice()[index].fetch_mul(val) }
            }
        }

        paste::paste!{
            #[allow(non_snake_case)]
            fn [<$name local_add>](val: *mut u8, array: $crate::array::AtomicArray<u8>, index: usize) {
                let typed_val = unsafe { *(val as *mut $a) };
                // println!("native {}local_add func ", stringify!($name));
                let array = unsafe { array.to_base_inner::<$a>() };
                use $crate::array::AtomicOps;
                unsafe { *(val as *mut $a) = array.local_as_mut_slice()[index].fetch_add(typed_val) };
            }
            $crate::atomicarray_register!{ $a,ArrayOpCmd::Add,[<$name dist_add>],[<$name local_add>] }

            #[allow(non_snake_case)]
            fn [<$name local_sub>](val: *mut u8, array: $crate::array::AtomicArray<u8>, index: usize) {
                let typed_val = unsafe { *(val as *mut $a) };
                // println!("native {}local_sub func ", stringify!($name));
                let array = unsafe { array.to_base_inner::<$a>() };
                use $crate::array::AtomicOps;
                unsafe { *(val as *mut $a) = array.local_as_mut_slice()[index].fetch_sub(typed_val) };
            }
            $crate::atomicarray_register!{ $a,ArrayOpCmd::Sub,[<$name dist_sub>],[<$name local_sub>] }

            #[allow(non_snake_case)]
            fn [<$name local_mul>](val: *mut u8, array: $crate::array::AtomicArray<u8>, index: usize) {
                let typed_val = unsafe { *(val as *mut $a) };
                // println!("native {}local_mul func ", stringify!($name));
                let array = unsafe { array.to_base_inner::<$a>() };
                use $crate::array::AtomicOps;
                unsafe { *(val as *mut $a) = array.local_as_mut_slice()[index].fetch_mul(typed_val) };
            }
            $crate::atomicarray_register!{ $a,ArrayOpCmd::Mul,[<$name dist_mul>],[<$name local_mul>] }
        }
    };
}

#[macro_export]
macro_rules! non_atomic_ops {
    ($a:ty, $name:ident) => {
        impl ArrayLocalOps<$a> for AtomicArray<$a> {
            fn local_add(&self, index: usize, val: $a) -> $a {
                // println!(
                //     "mutex local_add ArrayLocalOps<{}> for AtomicArray<{}>  ",
                //     stringify!($a),
                //     stringify!($a)
                // );
                let _lock = self.lock_index(index);
                unsafe { 
                    self.local_as_mut_slice()[index] += val;
                    self.local_as_mut_slice()[index]
                }
                
            }
            fn local_sub(&self, index: usize, val: $a) -> $a {
                // println!(
                //     "mutex local_sub ArrayLocalOps<{}> for AtomicArray<{}>  ",
                //     stringify!($a),
                //     stringify!($a)
                // );
                let _lock = self.lock_index(index);
                unsafe { 
                    self.local_as_mut_slice()[index] -= val ;
                    self.local_as_mut_slice()[index]
                }
            }
            fn local_mul(&self, index: usize, val: $a) -> $a {
                // println!(
                //     "mutex local_sub ArrayLocalOps<{}> for AtomicArray<{}>  ",
                //     stringify!($a),
                //     stringify!($a)
                // );
                let _lock = self.lock_index(index);
                unsafe { 
                    self.local_as_mut_slice()[index] *= val ;
                    self.local_as_mut_slice()[index]
                }
            }
        }

        paste::paste!{
            #[allow(non_snake_case)]
            fn [<$name local_add>](val: *mut u8, array: $crate::array::AtomicArray<u8>, index: usize) {
                let typed_val = unsafe { *(val as *mut $a) };
                let array = unsafe { array.to_base_inner::<$a>() };
                // println!("mutex {}local_add func", stringify!($name));
                let _lock = array.lock_index(index).expect("no lock exists!");
                unsafe {
                    array.local_as_mut_slice()[index] += typed_val;
                    *(val as *mut $a) = array.local_as_mut_slice()[index];
                }
            }
            $crate::atomicarray_register!{$a,ArrayOpCmd::Add,[<$name dist_add>],[<$name local_add>]}

            #[allow(non_snake_case)]
            fn [<$name local_sub>](val: *mut u8, array: $crate::array::AtomicArray<u8>, index: usize) {
                let typed_val = unsafe { *(val as *mut $a) };
                let array = unsafe { array.to_base_inner::<$a>() };
                // println!("mutex {}local_sub func", stringify!($name));
                let _lock = array.lock_index(index).expect("no lock exists!");
                unsafe { 
                    array.local_as_mut_slice()[index] -= typed_val;
                    *(val as *mut $a) = array.local_as_mut_slice()[index];
                }
            }
            $crate::atomicarray_register!{$a,ArrayOpCmd::Sub,[<$name dist_sub>],[<$name local_sub>]}

            #[allow(non_snake_case)]
            fn [<$name local_mul>](val: *mut u8, array: $crate::array::AtomicArray<u8>, index: usize) {
                let typed_val = unsafe { *(val as *mut $a) };
                let array = unsafe { array.to_base_inner::<$a>() };
                // println!("mutex {}local_mul func", stringify!($name));
                let _lock = array.lock_index(index).expect("no lock exists!");
                unsafe { 
                    array.local_as_mut_slice()[index] *= typed_val;
                    *(val as *mut $a) = array.local_as_mut_slice()[index];
                }
            }
            $crate::atomicarray_register!{$a,ArrayOpCmd::Mul,[<$name dist_mul>],[<$name local_mul>]}
        }

        
    };
}

#[macro_export]
macro_rules! AtomicArray_create_ops {
    (u8, $name:ident) => {
        $crate::atomic_ops!(u8, $name);
    };
    (u16, $name:ident) => {
        $crate::atomic_ops!(u16, $name);
    };
    (u32, $name:ident) => {
        $crate::atomic_ops!(u32, $name);
    };
    (u64, $name:ident) => {
        $crate::atomic_ops!(u64, $name);
    };
    (usize, $name:ident) => {
        $crate::atomic_ops!(usize, $name);
    };
    (i8, $name:ident) => {
        $crate::atomic_ops!(i8, $name);
    };
    (i16, $name:ident) => {
        $crate::atomic_ops!(i16, $name);
    };
    (i32, $name:ident) => {
        $crate::atomic_ops!(i32, $name);
    };
    (i64, $name:ident) => {
        $crate::atomic_ops!(i64, $name);
    };
    (isize, $name:ident) => {
        $crate::atomic_ops!(isize, $name);
    };
    ($a:ty, $name:ident) => {
        $crate::non_atomic_ops!($a, $name);
    };
}

#[macro_export]
macro_rules! atomicarray_register {
    ($id:ident, $optype:path, $op:ident, $local:ident) => {
        inventory::submit! {
            #![crate = $crate]
            $crate::array::AtomicArrayOp{
                id: ($optype,std::any::TypeId::of::<$id>()),
                op: $op,
                local: $local
            }
        }
    };
}

