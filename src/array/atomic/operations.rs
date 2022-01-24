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


pub struct AtomicArrayOp {
    pub id: (ArrayOpCmd,TypeId),
    pub op: OpFn,
    pub local: LocalOpFn,
}

crate::inventory::collect!(AtomicArrayOp);

lazy_static! {
    pub(crate) static ref OPS: HashMap<(ArrayOpCmd,TypeId), (OpFn,LocalOpFn)> = {
        let mut map = HashMap::new();
        for op in crate::inventory::iter::<AtomicArrayOp> {
            map.insert(op.id.clone(),(op.op,op.local));
        }
        map
    };
}

impl<T:  AmDist + Dist  + 'static> AtomicArray<T> {

    fn initiate_op(&self, index: usize, val: T, op: ArrayOpCmd)  -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>>{
        // println!("add ArithmeticOps<T> for &AtomicArray<T> ");
        if let Some(funcs) = OPS.get(&(op,TypeId::of::<T>())) {
            let pe = self.pe_for_dist_index(index).expect("index out of bounds");
            let local_index = self.pe_offset_for_dist_index(pe, index).unwrap();//calculated pe above
            let array: AtomicByteArray = self.clone().into();
            if pe == self.my_pe() {
                let mut val = val;
                funcs.1(&mut val as *mut T as *mut u8, array, local_index);
                None
            } else {
                let am = funcs.0(&val as *const T as *const u8, array, local_index);
                Some(self.array.dist_op(pe,am))
            }
        } else {
            let name = std::any::type_name::<T>().split("::").last().unwrap();
            panic!("the type {:?} has not been registered! this typically means you need to derive \"ArithmeticOps\" for the type . e.g. 
            #[derive(lamellar::ArithmeticOps)]
            struct {:?}{{
                ....
            }}
            note this also requires the type to impl Serialize + Deserialize, you can manually derive these, or use the lamellar::AmData attribute proc macro, e.g.
            #[lamellar::AMData(ArithmeticOps, any other traits you derive)]
            struct {:?}{{
                ....
            }}",name,name,name);
        }
    } 
    fn initiate_fetch_op(&self, index: usize, val: T, op: ArrayOpCmd)  -> Box<dyn LamellarRequest<Output = T> + Send + Sync>{
        // println!("add ArithmeticOps<T> for &AtomicArray<T> ");
        if let Some(funcs) = OPS.get(&(op,TypeId::of::<T>())) {
            let pe = self.pe_for_dist_index(index).expect("index out of bounds");
            let local_index = self.pe_offset_for_dist_index(pe, index).unwrap();//calculated pe above
            let array: AtomicByteArray = self.clone().into();
            if pe == self.my_pe() 
            {
                let mut val = val;
                funcs.1(&mut val as *mut T as *mut u8, array, local_index);
                Box::new(LocalOpResult{val})
            } else {
                let am = funcs.0(&val as *const T as *const u8, array, local_index);
                self.array.dist_fetch_op(pe,am)
            }
        } else {
            let name = std::any::type_name::<T>().split("::").last().unwrap();
            panic!("the type {:?} has not been registered for the operation: {:?}! this typically means you need to derive \"ArithmeticOps\" for the type . e.g. 
            #[derive(lamellar::ArithmeticOps)]
            struct {:?}{{
                ....
            }}
            note this also requires the type to impl Serialize + Deserialize, you can manually derive these, or use the lamellar::AmData attribute proc macro, e.g.
            #[lamellar::AMData(ArithmeticOps, any other traits you derive)]
            struct {:?}{{
                ....
            }}",name,op,name,name);
        }
    } 

    pub fn load(&self, index: usize) -> Box<dyn LamellarRequest<Output = T> + Send + Sync>{
        self.initiate_fetch_op(index,unsafe{self.local_as_slice()[0]},ArrayOpCmd::Load)
    }

    pub fn store(&self, index: usize, val: T) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>>{
        self.initiate_op(index,val,ArrayOpCmd::Store)
    }

    pub fn swap(&self, index: usize, val: T)  -> Box<dyn LamellarRequest<Output = T> + Send + Sync>{
        self.initiate_fetch_op(index,val,ArrayOpCmd::Swap)
    }
}


impl<T: ElementArithmeticOps  + 'static> ArithmeticOps<T> for AtomicArray<T> {
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
        self.initiate_fetch_op(index,val,ArrayOpCmd::FetchAdd)
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
        self.initiate_fetch_op(index,val,ArrayOpCmd::FetchSub)
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
        self.initiate_fetch_op(index,val,ArrayOpCmd::FetchMul)
    }
    fn div(
        &self,
        index: usize,
        val: T,
    ) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>> {
        self.initiate_op(index,val,ArrayOpCmd::Div)
    }
    fn fetch_div(
        &self,
        index: usize,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        self.initiate_fetch_op(index,val,ArrayOpCmd::FetchDiv)
    }
}
impl<T:  ElementBitWiseOps + 'static> BitWiseOps<T> for AtomicArray<T> {
    fn bit_and(
        &self,
        index: usize,
        val: T,
    ) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>> {
        // println!("and val {:?}",val);
        self.initiate_op(index,val,ArrayOpCmd::And)
    }
    fn fetch_bit_and(
        &self,
        index: usize,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        self.initiate_fetch_op(index,val,ArrayOpCmd::FetchAnd)
    }
    fn bit_or(
        &self,
        index: usize,
        val: T,
    ) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>> {
        // println!("or");
        self.initiate_op(index,val,ArrayOpCmd::Or)
    }
    fn fetch_bit_or(
        &self,
        index: usize,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
        // println!("fetch_or");
        self.initiate_fetch_op(index,val,ArrayOpCmd::FetchOr)
    }
}

#[macro_export]
macro_rules! atomic_ops {
    ($a:ty, $name:ident) => {
        impl LocalArithmeticOps<$a> for AtomicArray<$a> {
            fn local_fetch_add(&self, index: usize, val: $a) -> $a {
                use $crate::array::AtomicOps;
                unsafe { self.local_as_mut_slice()[index].fetch_add(val) }
            }
            fn local_fetch_sub(&self, index: usize, val: $a) -> $a {
                use $crate::array::AtomicOps;
                unsafe { self.local_as_mut_slice()[index].fetch_sub(val) }
            }
            fn local_fetch_mul(&self, index: usize, val: $a) -> $a {
                use $crate::array::AtomicOps;
                unsafe { self.local_as_mut_slice()[index].fetch_mul(val) }
            }
            fn local_fetch_div(&self, index: usize, val: $a) -> $a {
                use $crate::array::AtomicOps;
                unsafe { self.local_as_mut_slice()[index].fetch_div(val) }
            }
        }

        paste::paste!{
            #[allow(non_snake_case)]
            fn [<$name local_add>](val: *mut u8, array: $crate::array::AtomicByteArray, index: usize) {
                let typed_val = unsafe { *(val as *mut $a) };
                // println!("native {}local_add func ", stringify!($name));
                let array: AtomicArray<$a> = array.into();
                use $crate::array::AtomicOps;
                unsafe { *(val as *mut $a) = array.local_as_mut_slice()[index].fetch_add(typed_val) };
            }
            $crate::atomicarray_register!{ $a,ArrayOpCmd::Add,[<$name dist_add>],[<$name local_add>] }
            $crate::atomicarray_register!{ $a,ArrayOpCmd::FetchAdd,[<$name dist_fetch_add>],[<$name local_add>] }

            #[allow(non_snake_case)]
            fn [<$name local_sub>](val: *mut u8, array: $crate::array::AtomicByteArray, index: usize) {
                let typed_val = unsafe { *(val as *mut $a) };
                // println!("native {}local_sub func ", stringify!($name));
                let array: AtomicArray<$a> = array.into();
                use $crate::array::AtomicOps;
                unsafe { *(val as *mut $a) = array.local_as_mut_slice()[index].fetch_sub(typed_val) };
            }
            $crate::atomicarray_register!{ $a,ArrayOpCmd::Sub,[<$name dist_sub>],[<$name local_sub>] }
            $crate::atomicarray_register!{ $a,ArrayOpCmd::FetchSub,[<$name dist_fetch_sub>],[<$name local_sub>] }

            #[allow(non_snake_case)]
            fn [<$name local_mul>](val: *mut u8, array: $crate::array::AtomicByteArray, index: usize) {
                let typed_val = unsafe { *(val as *mut $a) };
                // println!("native {}local_mul func ", stringify!($name));
                let array: AtomicArray<$a> = array.into();
                use $crate::array::AtomicOps;
                unsafe { *(val as *mut $a) = array.local_as_mut_slice()[index].fetch_mul(typed_val) };
            }
            $crate::atomicarray_register!{ $a,ArrayOpCmd::Mul,[<$name dist_mul>],[<$name local_mul>] }
            $crate::atomicarray_register!{ $a,ArrayOpCmd::FetchMul,[<$name dist_fetch_mul>],[<$name local_mul>] }

            #[allow(non_snake_case)]
            fn [<$name local_div>](val: *mut u8, array: $crate::array::AtomicByteArray, index: usize) {
                let typed_val = unsafe { *(val as *mut $a) };
                // println!("native {}local_mul func ", stringify!($name));
                let array: AtomicArray<$a> = array.into();
                use $crate::array::AtomicOps;
                unsafe { *(val as *mut $a) = array.local_as_mut_slice()[index].fetch_div(typed_val) };
            }
            $crate::atomicarray_register!{ $a,ArrayOpCmd::Div,[<$name dist_div>],[<$name local_div>] }
            $crate::atomicarray_register!{ $a,ArrayOpCmd::FetchDiv,[<$name dist_fetch_div>],[<$name local_div>] }
        }
    };
}
#[macro_export]
macro_rules! atomic_bitwise_ops {
    ($a:ty, $name:ident) => {
        impl LocalBitWiseOps<$a> for AtomicArray<$a> {
            // fn local_bit_and(&self, index: usize, val: $a) {
            //     self.local_fetch_bit_and(index,val);
            // }
            fn local_fetch_bit_and(&self, index: usize, val: $a) -> $a {
                use $crate::array::AtomicOps;
                unsafe { let temp = self.local_as_mut_slice()[index].fetch_bit_and(val); 
                // println!("and temp: {:?} {:?} ",temp,self.local_as_mut_slice()[index]);
                temp}
            }
            // fn local_bit_or(&self, index: usize, val: $a) {
            //     self.local_fetch_bit_or(index,val);
            // }
            fn local_fetch_bit_or(&self, index: usize, val: $a) -> $a {
                use $crate::array::AtomicOps;
                unsafe {let temp =  self.local_as_mut_slice()[index].fetch_bit_or(val);
                // println!("or temp: {:?} {:?} ",temp,self.local_as_mut_slice()[index]);
                temp}
            }
        }
        
        paste::paste!{
            #[allow(non_snake_case)]
            fn [<$name local_bit_and>](val: *mut u8, array: $crate::array::AtomicByteArray, index: usize) {
                let typed_val = unsafe { *(val as *mut $a) };
                // println!("native {}local_mul func ", stringify!($name));
                let array: AtomicArray<$a> = array.into();
                use $crate::array::AtomicOps;
                unsafe { *(val as *mut $a) = array.local_as_mut_slice()[index].fetch_bit_and(typed_val)};
            }
            $crate::atomicarray_register!{ $a,ArrayOpCmd::And,[<$name dist_bit_and>],[<$name local_bit_and>] }
            $crate::atomicarray_register!{ $a,ArrayOpCmd::FetchAnd,[<$name dist_fetch_bit_and>],[<$name local_bit_and>] }

            #[allow(non_snake_case)]
            fn [<$name local_bit_or>](val: *mut u8, array: $crate::array::AtomicByteArray, index: usize) {
                let typed_val = unsafe { *(val as *mut $a) };
                // println!("native {}local_mul func ", stringify!($name));
                let array: AtomicArray<$a> = array.into();
                use $crate::array::AtomicOps;
                unsafe { *(val as *mut $a) = array.local_as_mut_slice()[index].fetch_bit_or(typed_val)};
            }
            $crate::atomicarray_register!{ $a,ArrayOpCmd::Or,[<$name dist_bit_or>],[<$name local_bit_or>] }
            $crate::atomicarray_register!{ $a,ArrayOpCmd::FetchOr,[<$name dist_fetch_bit_or>],[<$name local_bit_or>] }
        }
    };
}

#[macro_export]
macro_rules! atomic_misc_ops {
    ($a:ty, $name:ident) => {
        impl LocalAtomicOps<$a> for AtomicArray<$a> {
            fn local_load(&self, index: usize, _val: $a) -> $a {
                use $crate::array::AtomicOps;
                unsafe { self.local_as_mut_slice()[index].load() }
            }
            fn local_store(&self,index: usize, val: $a) {
                use $crate::array::AtomicOps;
                // println!("1. local store {:?} {:?}",index, val);
                unsafe { self.local_as_mut_slice()[index].store(val); }
            }
            fn local_swap(&self,index: usize, val: $a) -> $a {
                use $crate::array::AtomicOps;
                unsafe { self.local_as_mut_slice()[index].swap(val) }
            }
        }
        paste::paste!{
            #[allow(non_snake_case)]
            fn [<$name local_load>](val: *mut u8, array: $crate::array::AtomicByteArray, index: usize) {
                // let typed_val = unsafe { *(val as *mut $a) };
                // println!("native {}local_mul func ", stringify!($name));
                let array: AtomicArray<$a> = array.into();
                use $crate::array::AtomicOps;
                unsafe { *(val as *mut $a) = array.local_as_mut_slice()[index].load() };
            }
            $crate::atomicarray_register!{ $a,ArrayOpCmd::Load,[<$name dist_load>],[<$name local_load>] }

            #[allow(non_snake_case)]
            fn [<$name local_store>](val: *mut u8, array: $crate::array::AtomicByteArray, index: usize) {
                let typed_val = unsafe { *(val as *mut $a) };
                // println!("native {}local_mul func ", stringify!($name));
                let array: AtomicArray<$a> = array.into();
                use $crate::array::AtomicOps;
                // println!("2. local store {:?} {:?}",index, typed_val);
                unsafe { array.local_as_mut_slice()[index].store(typed_val) };
            }
            $crate::atomicarray_register!{ $a,ArrayOpCmd::Store,[<$name dist_store>],[<$name local_store>] }

            #[allow(non_snake_case)]
            fn [<$name local_swap>](val: *mut u8, array: $crate::array::AtomicByteArray, index: usize) {
                let typed_val = unsafe { *(val as *mut $a) };
                // println!("native {}local_mul func ", stringify!($name));
                let array: AtomicArray<$a> = array.into();
                use $crate::array::AtomicOps;
                unsafe { *(val as *mut $a) = array.local_as_mut_slice()[index].swap(typed_val) };
            }
            $crate::atomicarray_register!{ $a,ArrayOpCmd::Swap,[<$name dist_swap>],[<$name local_swap>] }
        }
    }
}

#[macro_export]
macro_rules! non_atomic_ops {
    ($a:ty, $name:ident) => {
        impl LocalArithmeticOps<$a> for AtomicArray<$a> {
            fn local_fetch_add(&self, index: usize, val: $a) -> $a {
                // println!(
                //     "mutex local_add LocalArithmeticOps<{}> for AtomicArray<{}>  ",
                //     stringify!($a),
                //     stringify!($a)
                // );
                let _lock = self.lock_index(index);
                unsafe { 
                    let orig  = self.local_as_mut_slice()[index];
                    self.local_as_mut_slice()[index] += val;
                    orig
                }
                
            }
            fn local_fetch_sub(&self, index: usize, val: $a) -> $a {
                // println!(
                //     "mutex local_sub LocalArithmeticOps<{}> for AtomicArray<{}>  ",
                //     stringify!($a),
                //     stringify!($a)
                // );
                let _lock = self.lock_index(index);
                unsafe { 
                    let orig  = self.local_as_mut_slice()[index];
                    self.local_as_mut_slice()[index] -= val ;
                    orig
                }
            }
            fn local_fetch_mul(&self, index: usize, val: $a) -> $a {
                let _lock = self.lock_index(index);
                unsafe {
                    let orig  = self.local_as_mut_slice()[index]; 
                    self.local_as_mut_slice()[index] *= val ;
                    orig
                }
            }
            fn local_fetch_div(&self, index: usize, val: $a) -> $a {
                let _lock = self.lock_index(index);
                unsafe { 
                    let orig  = self.local_as_mut_slice()[index];
                    self.local_as_mut_slice()[index] /= val ;
                    orig
                }
            }
        }

        paste::paste!{
            #[allow(non_snake_case)]
            fn [<$name local_add>](val: *mut u8, array: $crate::array::AtomicByteArray, index: usize) {
                let typed_val = unsafe { *(val as *mut $a) };
                let array: AtomicArray<$a> = array.into();
                // println!("mutex {}local_add func", stringify!($name));
                let _lock = array.lock_index(index).expect("no lock exists!");
                unsafe {
                    *(val as *mut $a) = array.local_as_mut_slice()[index];
                    array.local_as_mut_slice()[index] += typed_val;
                }
            }
            $crate::atomicarray_register!{$a,ArrayOpCmd::Add,[<$name dist_add>],[<$name local_add>]}
            $crate::atomicarray_register!{$a,ArrayOpCmd::FetchAdd,[<$name dist_fetch_add>],[<$name local_add>]}

            #[allow(non_snake_case)]
            fn [<$name local_sub>](val: *mut u8, array: $crate::array::AtomicByteArray, index: usize) {
                let typed_val = unsafe { *(val as *mut $a) };
                let array: AtomicArray<$a> = array.into();
                // println!("mutex {}local_sub func", stringify!($name));
                let _lock = array.lock_index(index).expect("no lock exists!");
                unsafe {                     
                    *(val as *mut $a) = array.local_as_mut_slice()[index];
                    array.local_as_mut_slice()[index] -= typed_val;
                }
            }
            $crate::atomicarray_register!{$a,ArrayOpCmd::Sub,[<$name dist_sub>],[<$name local_sub>]}
            $crate::atomicarray_register!{$a,ArrayOpCmd::FetchSub,[<$name dist_fetch_sub>],[<$name local_sub>]}

            #[allow(non_snake_case)]
            fn [<$name local_mul>](val: *mut u8, array: $crate::array::AtomicByteArray, index: usize) {
                let typed_val = unsafe { *(val as *mut $a) };
                let array: AtomicArray<$a> = array.into();
                // println!("mutex {}local_mul func", stringify!($name));
                let _lock = array.lock_index(index).expect("no lock exists!");
                unsafe { 
                    *(val as *mut $a) = array.local_as_mut_slice()[index];
                    array.local_as_mut_slice()[index] *= typed_val;
                }
            }
            $crate::atomicarray_register!{$a,ArrayOpCmd::Mul,[<$name dist_mul>],[<$name local_mul>]}
            $crate::atomicarray_register!{$a,ArrayOpCmd::FetchMul,[<$name dist_fetch_mul>],[<$name local_mul>]}

            #[allow(non_snake_case)]
            fn [<$name local_div>](val: *mut u8, array: $crate::array::AtomicByteArray, index: usize) {
                let typed_val = unsafe { *(val as *mut $a) };
                let array: AtomicArray<$a> = array.into();
                // println!("mutex {}local_mul func", stringify!($name));
                let _lock = array.lock_index(index).expect("no lock exists!");
                unsafe {
                    *(val as *mut $a) = array.local_as_mut_slice()[index]; 
                    array.local_as_mut_slice()[index] /= typed_val;
                }
            }
            $crate::atomicarray_register!{$a,ArrayOpCmd::Div,[<$name dist_div>],[<$name local_div>]}
            $crate::atomicarray_register!{$a,ArrayOpCmd::FetchDiv,[<$name dist_fetch_div>],[<$name local_div>]}
        }
    };
}

#[macro_export]
macro_rules! non_atomic_bitwise_ops {
    ($a:ty, $name:ident) => {
        impl LocalBitWiseOps<$a> for AtomicArray<$a> {
            fn local_fetch_bit_and(&self, index: usize, val: $a) -> $a {
                // println!(
                //     "mutex local_add LocalArithmeticOps<{}> for AtomicArray<{}>  ",
                //     stringify!($a),
                //     stringify!($a)
                // );
                let _lock = self.lock_index(index);
                unsafe { 
                    let orig  = self.local_as_mut_slice()[index];
                    self.local_as_mut_slice()[index] &= val;
                    orig
                }
                
            }
            fn local_fetch_bit_or(&self, index: usize, val: $a) -> $a {
                // println!(
                //     "mutex local_sub LocalArithmeticOps<{}> for AtomicArray<{}>  ",
                //     stringify!($a),
                //     stringify!($a)
                // );
                let _lock = self.lock_index(index);
                unsafe { 
                    let orig  = self.local_as_mut_slice()[index];
                    self.local_as_mut_slice()[index] |= val ;
                    orig
                }
            }
        }

        paste::paste!{
            #[allow(non_snake_case)]
            fn [<$name local_bit_and>](val: *mut u8, array: $crate::array::AtomicByteArray, index: usize) {
                let typed_val = unsafe { *(val as *mut $a) };
                let array: AtomicArray<$a> = array.into();
                // println!("mutex {}local_add func", stringify!($name));
                let _lock = array.lock_index(index).expect("no lock exists!");
                unsafe {
                    *(val as *mut $a) = array.local_as_mut_slice()[index];
                    array.local_as_mut_slice()[index] &= typed_val;
                }
            }
            $crate::atomicarray_register!{$a,ArrayOpCmd::And,[<$name dist_bit_and>],[<$name local_bit_and>]}
            $crate::atomicarray_register!{$a,ArrayOpCmd::FetchAnd,[<$name dist_fetch_bit_and>],[<$name local_bit_and>]}

            #[allow(non_snake_case)]
            fn [<$name local_bit_or>](val: *mut u8, array: $crate::array::AtomicByteArray, index: usize) {
                let typed_val = unsafe { *(val as *mut $a) };
                let array: AtomicArray<$a> = array.into();
                // println!("mutex {}local_sub func", stringify!($name));
                let _lock = array.lock_index(index).expect("no lock exists!");
                unsafe {                     
                    *(val as *mut $a) = array.local_as_mut_slice()[index];
                    array.local_as_mut_slice()[index] |= typed_val;
                }
            }
            $crate::atomicarray_register!{$a,ArrayOpCmd::Or,[<$name dist_bit_or>],[<$name local_bit_or>]}
            $crate::atomicarray_register!{$a,ArrayOpCmd::FetchOr,[<$name dist_fetch_bit_or>],[<$name local_bit_or>]}
        }
    };
}

#[macro_export]
macro_rules! non_atomic_misc_ops {
    ($a:ty, $name:ident) => {
        impl LocalAtomicOps<$a> for AtomicArray<$a> {
            fn local_load(&self, index: usize, _val: $a) -> $a {
                let _lock = self.lock_index(index);
                unsafe { 
                    self.local_as_mut_slice()[index]
                }
            }
            fn local_store(&self, index: usize, val: $a) {
                let _lock = self.lock_index(index);
                unsafe { 
                    self.local_as_mut_slice()[index] = val;
                }
            }
            fn local_swap(&self, index: usize, val: $a) -> $a {
                let _lock = self.lock_index(index);
                unsafe { 
                    let orig  = self.local_as_mut_slice()[index];
                    self.local_as_mut_slice()[index] = val;
                    orig
                }
            }
        }
        paste::paste!{
            #[allow(non_snake_case)]
            fn [<$name local_load>](val: *mut u8, array: $crate::array::AtomicByteArray, index: usize) {
                // let typed_val = unsafe { *(val as *mut $a) };
                let array: AtomicArray<$a> = array.into();
                // println!("mutex {}local_mul func", stringify!($name));
                let _lock = array.lock_index(index).expect("no lock exists!");
                unsafe {
                    *(val as *mut $a) = array.local_as_mut_slice()[index]; 
                }
            }
            $crate::atomicarray_register!{$a,ArrayOpCmd::Load,[<$name dist_load>],[<$name local_load>]}

            #[allow(non_snake_case)]
            fn [<$name local_store>](val: *mut u8, array: $crate::array::AtomicByteArray, index: usize) {
                let typed_val = unsafe { *(val as *mut $a) };
                let array: AtomicArray<$a> = array.into();
                // println!("mutex {}local_mul func", stringify!($name));
                let _lock = array.lock_index(index).expect("no lock exists!");
                unsafe {
                    array.local_as_mut_slice()[index] = typed_val;
                }
            }
            $crate::atomicarray_register!{$a,ArrayOpCmd::Store,[<$name dist_store>],[<$name local_store>]}

            #[allow(non_snake_case)]
            fn [<$name local_swap>](val: *mut u8, array: $crate::array::AtomicByteArray, index: usize) {
                let typed_val = unsafe { *(val as *mut $a) };
                let array: AtomicArray<$a> = array.into();
                // println!("mutex {}local_mul func", stringify!($name));
                let _lock = array.lock_index(index).expect("no lock exists!");
                unsafe {
                    *(val as *mut $a) = array.local_as_mut_slice()[index]; 
                    array.local_as_mut_slice()[index] = typed_val;
                }
            }
            $crate::atomicarray_register!{$a,ArrayOpCmd::Swap,[<$name dist_swap>],[<$name local_swap>]}
        }
    }
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
macro_rules! AtomicArray_create_bitwise_ops {
    (u8, $name:ident) => {
        $crate::atomic_bitwise_ops!(u8, $name);
    };
    (u16, $name:ident) => {
        $crate::atomic_bitwise_ops!(u16, $name);
    };
    (u32, $name:ident) => {
        $crate::atomic_bitwise_ops!(u32, $name);
    };
    (u64, $name:ident) => {
        $crate::atomic_bitwise_ops!(u64, $name);
    };
    (usize, $name:ident) => {
        $crate::atomic_bitwise_ops!(usize, $name);
    };
    (i8, $name:ident) => {
        $crate::atomic_bitwise_ops!(i8, $name);
    };
    (i16, $name:ident) => {
        $crate::atomic_bitwise_ops!(i16, $name);
    };
    (i32, $name:ident) => {
        $crate::atomic_bitwise_ops!(i32, $name);
    };
    (i64, $name:ident) => {
        $crate::atomic_bitwise_ops!(i64, $name);
    };
    (isize, $name:ident) => {
        $crate::atomic_bitwise_ops!(isize, $name);
    };
    ($a:ty, $name:ident) => {
        $crate::non_atomic_bitwise_ops!($a, $name);
    };
}

#[macro_export]
macro_rules! AtomicArray_create_atomic_ops {
    (u8, $name:ident) => {
        $crate::atomic_misc_ops!(u8, $name);
    };
    (u16, $name:ident) => {
        $crate::atomic_misc_ops!(u16, $name);
    };
    (u32, $name:ident) => {
        $crate::atomic_misc_ops!(u32, $name);
    };
    (u64, $name:ident) => {
        $crate::atomic_misc_ops!(u64, $name);
    };
    (usize, $name:ident) => {
        $crate::atomic_misc_ops!(usize, $name);
    };
    (i8, $name:ident) => {
        $crate::atomic_misc_ops!(i8, $name);
    };
    (i16, $name:ident) => {
        $crate::atomic_misc_ops!(i16, $name);
    };
    (i32, $name:ident) => {
        $crate::atomic_misc_ops!(i32, $name);
    };
    (i64, $name:ident) => {
        $crate::atomic_misc_ops!(i64, $name);
    };
    (isize, $name:ident) => {
        $crate::atomic_misc_ops!(isize, $name);
    };
    ($a:ty, $name:ident) => {
        $crate::non_atomic_misc_ops!($a, $name);
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

