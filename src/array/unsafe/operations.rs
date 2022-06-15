use crate::active_messaging::*;
use crate::array::r#unsafe::*;
use crate::array::*;
use crate::lamellar_request::LamellarRequest;
// use crate::memregion::Dist;
use parking_lot::{Mutex, RwLock};
use std::any::TypeId;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

type OpFn = fn(*const u8, UnsafeByteArray, usize) -> LamellarArcAm;

lazy_static! {
    static ref OPS: HashMap<(ArrayOpCmd, TypeId), OpFn> = {
        let mut map = HashMap::new();
        for op in crate::inventory::iter::<UnsafeArrayOp> {
            map.insert(op.id.clone(), op.op);
        }
        map
    };
}

pub struct UnsafeArrayOp {
    pub id: (ArrayOpCmd, TypeId),
    pub op: OpFn,
}

crate::inventory::collect!(UnsafeArrayOp);

type BufFn = fn(UnsafeByteArray) -> Arc<dyn BufferOp>;

lazy_static! {
    pub(crate) static ref BUFOPS: HashMap<TypeId, BufFn> = {
        let mut map = HashMap::new();
        for op in crate::inventory::iter::<UnsafeArrayOpBuf> {
            map.insert(op.id.clone(), op.op);
        }
        map
    };
}

pub struct UnsafeArrayOpBuf {
    pub id: TypeId,
    pub op: BufFn,
}

crate::inventory::collect!(UnsafeArrayOpBuf);

impl<T: AmDist + Dist + 'static> UnsafeArray<T> {
    pub(crate) fn dummy_val(&self) -> T {
        let slice = self.inner.data.mem_region.as_slice().unwrap();
        unsafe {
            std::slice::from_raw_parts(
                slice.as_ptr() as *const T,
                slice.len() / std::mem::size_of::<T>(),
            )[0]
        }
    }

    pub(crate) fn dist_op(
        &self,
        pe: usize,
        func: LamellarArcAm,
    ) -> Box<dyn LamellarRequest<Output = ()>  > {
        // println!("dist_op for UnsafeArray<T> ");
        self.inner
            .data
            .team
            .exec_arc_am_pe(pe, func, Some(self.inner.data.array_counters.clone()))
    }
    pub(crate) fn dist_fetch_op(
        &self,
        pe: usize,
        func: LamellarArcAm,
    ) -> Box<dyn LamellarRequest<Output = T>  > {
        // println!("dist_op for UnsafeArray<T> ");
        self.inner
            .data
            .team
            .exec_arc_am_pe(pe, func, Some(self.inner.data.array_counters.clone()))
    }
    fn initiate_op(
        &self,
        index: usize,
        val: T,
        local_index: usize,
        op: ArrayOpCmd,
    ) -> Option<Box<dyn LamellarRequest<Output = ()>  >> {
        // println!("initiate_op for UnsafeArray<T> ");
        if let Some(func) = OPS.get(&(op, TypeId::of::<T>())) {
            let array: UnsafeByteArray = self.clone().into();
            let pe = self
                .inner
                .pe_for_dist_index(index)
                .expect("index out of bounds");
            let am = func(&val as *const T as *const u8, array, local_index);
            Some(self.inner.data.team.exec_arc_am_pe(
                pe,
                am,
                Some(self.inner.data.array_counters.clone()),
            ))
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

    fn initiate_fetch_op<'a>(
        &self,
        index: usize,
        val: T,
        local_index: usize,
        op: ArrayOpCmd,
    ) -> Box<dyn LamellarRequest<Output = T>  > {
        // println!("initiate_op for UnsafeArray<T> ");
        if let Some(func) = OPS.get(&(op, TypeId::of::<T>())) {
            let array: UnsafeByteArray = self.clone().into();
            let pe = self
                .inner
                .pe_for_dist_index(index)
                .expect("index out of bounds");
            let am = func(&val as *const T as *const u8, array, local_index);
            self.inner.data.team.exec_arc_am_pe(
                pe,
                am,
                Some(self.inner.data.array_counters.clone()),
            )
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

    pub(crate) fn process_ops(&self, ops: &Vec<(ArrayOpCmd, usize, T)>) {}
}

impl<T: ElementArithmeticOps + 'static> ArithmeticOps<T> for UnsafeArray<T> {
    fn add(
        &self,
        index: usize,
        val: T,
    ) -> Option<Box<dyn LamellarRequest<Output = ()>  >> {
        let pe = self
            .inner
            .pe_for_dist_index(index)
            .expect("index out of bounds");
        let local_index = self.inner.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        if pe == self.my_pe() {
            self.local_add(local_index, val);
            None
        } else {
            Some(self.initiate_op(index, val, local_index, ArrayOpCmd::Add))
        }
    }
    fn fetch_add(
        &self,
        index: usize,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = T>  > {
        let pe = self
            .inner
            .pe_for_dist_index(index)
            .expect("index out of bounds");
        let local_index = self.inner.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        if pe == self.my_pe() {
            let val = self.local_fetch_add(local_index, val);
            Box::new(LocalOpResult { val })
        } else {
            self.initiate_fetch_op(index, val, local_index, ArrayOpCmd::FetchAdd)
        }
    }
    fn sub(
        &self,
        index: usize,
        val: T,
    ) -> Option<Box<dyn LamellarRequest<Output = ()>  >> {
        let pe = self
            .inner
            .pe_for_dist_index(index)
            .expect("index out of bounds");
        let local_index = self.inner.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        if pe == self.my_pe() {
            self.local_sub(local_index, val);
            None
        } else {
            self.initiate_op_old(index, val, local_index, ArrayOpCmd::Sub)
        }
    }
    fn fetch_sub(
        &self,
        index: usize,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = T>  > {
        let pe = self
            .inner
            .pe_for_dist_index(index)
            .expect("index out of bounds");
        let local_index = self.inner.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        if pe == self.my_pe() {
            let val = self.local_fetch_sub(local_index, val);
            Box::new(LocalOpResult { val })
        } else {
            self.initiate_fetch_op(index, val, local_index, ArrayOpCmd::FetchSub)
        }
    }
    fn mul(
        &self,
        index: usize,
        val: T,
    ) -> Option<Box<dyn LamellarRequest<Output = ()>  >> {
        let pe = self
            .inner
            .pe_for_dist_index(index)
            .expect("index out of bounds");
        let local_index = self.inner.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        if pe == self.my_pe() {
            self.local_mul(local_index, val);
            None
        } else {
            self.initiate_op_old(index, val, local_index, ArrayOpCmd::Mul)
        }
    }
    fn fetch_mul(
        &self,
        index: usize,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = T>  > {
        let pe = self
            .inner
            .pe_for_dist_index(index)
            .expect("index out of bounds");
        let local_index = self.inner.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        if pe == self.my_pe() {
            let val = self.local_fetch_mul(local_index, val);
            Box::new(LocalOpResult { val })
        } else {
            self.initiate_fetch_op(index, val, local_index, ArrayOpCmd::FetchMul)
        }
    }
    fn div(
        &self,
        index: usize,
        val: T,
    ) -> Option<Box<dyn LamellarRequest<Output = ()>  >> {
        let pe = self
            .inner
            .pe_for_dist_index(index)
            .expect("index out of bounds");
        let local_index = self.inner.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        if pe == self.my_pe() {
            self.local_div(local_index, val);
            None
        } else {
            self.initiate_op_old(index, val, local_index, ArrayOpCmd::Div)
        }
    }
    fn fetch_div(
        &self,
        index: usize,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = T>  > {
        let pe = self
            .inner
            .pe_for_dist_index(index)
            .expect("index out of bounds");
        let local_index = self.inner.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        if pe == self.my_pe() {
            let val = self.local_fetch_div(local_index, val);
            Box::new(LocalOpResult { val })
        } else {
            self.initiate_fetch_op(index, val, local_index, ArrayOpCmd::FetchDiv)
        }
    }
}

impl<T: ElementBitWiseOps + 'static> BitWiseOps<T> for UnsafeArray<T> {
    fn bit_and(
        &self,
        index: usize,
        val: T,
    ) -> Option<Box<dyn LamellarRequest<Output = ()>  >> {
        let pe = self
            .inner
            .pe_for_dist_index(index)
            .expect("index out of bounds");
        let local_index = self.inner.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        if pe == self.my_pe() {
            self.local_bit_and(local_index, val);
            None
        } else {
            self.initiate_op_old(index, val, local_index, ArrayOpCmd::And)
        }
    }
    fn fetch_bit_and(
        &self,
        index: usize,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = T>  > {
        let pe = self
            .inner
            .pe_for_dist_index(index)
            .expect("index out of bounds");
        let local_index = self.inner.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        if pe == self.my_pe() {
            let val = self.local_fetch_bit_and(local_index, val);
            Box::new(LocalOpResult { val })
        } else {
            self.initiate_fetch_op(index, val, local_index, ArrayOpCmd::FetchAnd)
        }
    }

    fn bit_or(
        &self,
        index: usize,
        val: T,
    ) -> Option<Box<dyn LamellarRequest<Output = ()>  >> {
        let pe = self
            .inner
            .pe_for_dist_index(index)
            .expect("index out of bounds");
        let local_index = self.inner.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        if pe == self.my_pe() {
            self.local_bit_or(local_index, val);
            None
        } else {
            self.initiate_op_old(index, val, local_index, ArrayOpCmd::Or)
        }
    }
    fn fetch_bit_or(
        &self,
        index: usize,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = T>  > {
        let pe = self
            .inner
            .pe_for_dist_index(index)
            .expect("index out of bounds");
        let local_index = self.inner.pe_offset_for_dist_index(pe, index).unwrap(); //calculated pe above
        if pe == self.my_pe() {
            let val = self.local_fetch_bit_or(local_index, val);
            Box::new(LocalOpResult { val })
        } else {
            self.initiate_fetch_op(index, val, local_index, ArrayOpCmd::FetchOr)
        }
    }
}

// impl<T: Dist + std::ops::AddAssign> UnsafeArray<T> {
impl<T: ElementArithmeticOps> LocalArithmeticOps<T> for UnsafeArray<T> {
    fn local_fetch_add(&self, index: usize, val: T) -> T {
        // println!("local_add LocalArithmeticOps<T> for UnsafeArray<T> ");
        unsafe {
            let orig = self.local_as_mut_slice()[index];
            self.local_as_mut_slice()[index] += val;
            orig
        }
    }
    fn local_fetch_sub(&self, index: usize, val: T) -> T {
        // println!("local_sub LocalArithmeticOps<T> for UnsafeArray<T> ");
        unsafe {
            let orig = self.local_as_mut_slice()[index];
            self.local_as_mut_slice()[index] -= val;
            orig
        }
    }
    fn local_fetch_mul(&self, index: usize, val: T) -> T {
        // println!("local_sub LocalArithmeticOps<T> for UnsafeArray<T> ");
        unsafe {
            let orig = self.local_as_mut_slice()[index];
            self.local_as_mut_slice()[index] *= val;
            // println!("orig: {:?} new {:?} va; {:?}",orig,self.local_as_mut_slice()[index] ,val);
            orig
        }
    }
    fn local_fetch_div(&self, index: usize, val: T) -> T {
        // println!("local_sub LocalArithmeticOps<T> for UnsafeArray<T> ");
        unsafe {
            let orig = self.local_as_mut_slice()[index];
            self.local_as_mut_slice()[index] /= val;
            // println!("div i: {:?} {:?} {:?} {:?}",index,orig,val,self.local_as_mut_slice()[index]);
            orig
        }
    }
}
impl<T: ElementBitWiseOps> LocalBitWiseOps<T> for UnsafeArray<T> {
    fn local_fetch_bit_and(&self, index: usize, val: T) -> T {
        // println!("local_sub LocalArithmeticOps<T> for UnsafeArray<T> ");
        unsafe {
            let orig = self.local_as_mut_slice()[index];
            self.local_as_mut_slice()[index] &= val;
            orig
        }
    }
    fn local_fetch_bit_or(&self, index: usize, val: T) -> T {
        // println!("local_sub LocalArithmeticOps<T> for UnsafeArray<T> ");
        unsafe {
            let orig = self.local_as_mut_slice()[index];
            self.local_as_mut_slice()[index] |= val;
            orig
        }
    }
}
// }

#[macro_export]
macro_rules! UnsafeArray_create_ops {
    ($a:ty, $name:ident) => {
        paste::paste!{
            $crate::unsafearray_register!{$a,ArrayOpCmd::Add,[<$name dist_add>],[<$name local_add>]}
            $crate::unsafearray_register!{$a,ArrayOpCmd::FetchAdd,[<$name dist_fetch_add>],[<$name local_add>]}
            $crate::unsafearray_register!{$a,ArrayOpCmd::Sub,[<$name dist_sub>],[<$name local_sub>]}
            $crate::unsafearray_register!{$a,ArrayOpCmd::FetchSub,[<$name dist_fetch_sub>],[<$name local_sub>]}
            $crate::unsafearray_register!{$a,ArrayOpCmd::Mul,[<$name dist_mul>],[<$name local_mul>]}
            $crate::unsafearray_register!{$a,ArrayOpCmd::FetchMul,[<$name dist_fetch_mul>],[<$name local_mul>]}
            $crate::unsafearray_register!{$a,ArrayOpCmd::Div,[<$name dist_div>],[<$name local_div>]}
            $crate::unsafearray_register!{$a,ArrayOpCmd::FetchDiv,[<$name dist_fetch_div>],[<$name local_div>]}
        }
    }
}

#[macro_export]
macro_rules! UnsafeArray_create_bitwise_ops {
    ($a:ty, $name:ident) => {
        paste::paste!{
            $crate::unsafearray_register!{$a,ArrayOpCmd::And,[<$name dist_bit_and>],[<$name local_bit_and>]}
            $crate::unsafearray_register!{$a,ArrayOpCmd::FetchAnd,[<$name dist_fetch_bit_and>],[<$name local_bit_and>]}
            $crate::unsafearray_register!{$a,ArrayOpCmd::Or,[<$name dist_bit_or>],[<$name local_bit_or>]}
            $crate::unsafearray_register!{$a,ArrayOpCmd::FetchOr,[<$name dist_fetch_bit_or>],[<$name local_bit_or>]}
        }
    }
}
#[macro_export]
macro_rules! unsafearray_register {
    ($id:ident, $optype:path, $op:ident, $local:ident) => {
        inventory::submit! {
            #![crate =$crate]
            $crate::array::UnsafeArrayOp{
                id: ($optype,std::any::TypeId::of::<$id>()),
                op: $op,
            }
        }
    };
}
