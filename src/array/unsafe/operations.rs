use crate::active_messaging::*;
use crate::array::r#unsafe::*;
use crate::array::*;
use crate::lamellar_request::LamellarRequest;
use crate::memregion::Dist;
use std::any::TypeId;
use std::collections::HashMap;

type OpFn = fn(*const u8, UnsafeArray<u8>, usize) -> LamellarArcAm; 

lazy_static! {
    static ref OPS: HashMap<(ArrayOpCmd,TypeId), OpFn> = {
        let mut map = HashMap::new();
        for op in crate::inventory::iter::<UnsafeArrayOp> {
            map.insert(op.id.clone(), op.op);
        }
        map
    };
}

pub struct UnsafeArrayOp {
    pub id: (ArrayOpCmd,TypeId),
    pub op: OpFn,
}

crate::inventory::collect!(UnsafeArrayOp);

impl<T: Dist  + ElementOps + 'static> UnsafeArray<T> {
    pub(crate) fn dist_op(
        &self,
        pe: usize,
        func: LamellarArcAm,
    ) -> Box<dyn LamellarRequest<Output = ()> + Send + Sync> {
        println!("dist_op  for UnsafeArray<T> ");
        // let pe = self.pe_for_dist_index(index);
        self.inner
            .team
            .exec_arc_am_pe(pe, func, Some(self.inner.array_counters.clone()))
    }

    fn initiate_op(
        &self,
        index: usize,
        val: T,
        pe: usize,
        local_index: usize,
        op: ArrayOpCmd
    ) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>> {
        println!("initiate_op for &UnsafeArray<T> ");
        if let Some(func) = OPS.get(&(op,TypeId::of::<T>())) {
            let array: UnsafeArray<u8> = unsafe { self.as_base_inner() };
            let pe = self.pe_for_dist_index(index);
            let am = func(&val as *const T as *const u8, array, local_index);
            Some(self.inner.team.exec_arc_am_pe(
                pe,
                am,
                Some(self.inner.array_counters.clone()),
            ))
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

impl<T: Dist  + ElementOps  + 'static> ArrayOps<T> for UnsafeArray<T> {
    fn add(
        &self,
        index: usize,
        val: T,
    ) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>> {
        let pe = self.pe_for_dist_index(index);
        let local_index = self.pe_offset_for_dist_index(pe, index);
        if pe == self.my_pe() {
            self.local_add(local_index, val);
            None
        } else {
            self.initiate_op(index,val,pe,local_index,ArrayOpCmd::Add)
        }
    }
    fn sub(
        &self,
        index: usize,
        val: T,
    ) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>> {
        let pe = self.pe_for_dist_index(index);
        let local_index = self.pe_offset_for_dist_index(pe, index);
        if pe == self.my_pe() {
            self.local_sub(local_index, val);
            None
        } else {
            self.initiate_op(index,val,pe,local_index,ArrayOpCmd::Sub)
        }
    }
}

// impl<T: Dist + std::ops::AddAssign> UnsafeArray<T> {
impl<T: Dist + ElementOps> ArrayLocalOps<T> for UnsafeArray<T> {
    fn local_add(&self, index: usize, val: T) {
        // println!("local_add ArrayLocalOps<T> for UnsafeArray<T> ");
        unsafe { 
            self.local_as_mut_slice()[index] += val 
        };
    }
    fn local_sub(&self, index: usize, val: T) {
        // println!("local_sub ArrayLocalOps<T> for UnsafeArray<T> ");
        unsafe { 
            self.local_as_mut_slice()[index] -= val 
        };
    }
}
// }

#[macro_export]
macro_rules! UnsafeArray_create_ops {
    ($a:ty, $name:ident) => {
        paste::paste!{
            $crate::unsafearray_register!{$a,ArrayOpCmd::Add,[<$name dist_add>],[<$name local_add>]}
            $crate::unsafearray_register!{$a,ArrayOpCmd::Sub,[<$name dist_sub>],[<$name local_sub>]}
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
