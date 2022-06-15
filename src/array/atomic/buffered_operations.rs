use crate::active_messaging::*;
use crate::array::atomic::*;
use crate::array::*;
use crate::lamellar_request::LamellarRequest;
use crate::memregion::Dist;

use std::any::TypeId;
use std::collections::HashMap;

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

impl<T: AmDist + Dist + 'static> AtomicArray<T> {
    pub fn load<'a>(
        &self,
        index: impl OpInput<'a, usize>,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>>  > {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.load(index),
            AtomicArray::GenericAtomicArray(array) => array.load(index),
        }
    }

    pub fn store<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = ()>  > {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.store(index, val),
            AtomicArray::GenericAtomicArray(array) => array.store(index, val),
        }
    }

    pub fn swap<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>>  > {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.swap(index, val),
            AtomicArray::GenericAtomicArray(array) => array.swap(index, val),
        }
    }
}

impl<T: AmDist + Dist + std::cmp::Eq + 'static> AtomicArray<T> {
    pub fn compare_exchange<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        old: T,
        new: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<Result<T,T>>>  > {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.compare_exchange(index, old, new),
            AtomicArray::GenericAtomicArray(array) => array.compare_exchange(index, old, new),
        }
    }
}

impl<T: AmDist + Dist + std::cmp::PartialEq + 'static> AtomicArray<T> {
    pub fn compare_exchange_epsilon<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        old: T,
        new: T,
        eps: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<Result<T,T>>>  > {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.compare_exchange_epsilon(index, old, new, eps),
            AtomicArray::GenericAtomicArray(array) => array.compare_exchange_epsilon(index, old, new, eps),
        }
    }
}

impl<T: ElementArithmeticOps + 'static> ArithmeticOps<T> for AtomicArray<T> {
    fn add<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = ()>  > {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.add(index, val),
            AtomicArray::GenericAtomicArray(array) => array.add(index, val),
        }
    }
    fn fetch_add<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>>  > {
        match self{
            AtomicArray::NativeAtomicArray(array) => array.fetch_add(index, val),
            AtomicArray::GenericAtomicArray(array) => array.fetch_add(index, val),
        }
    }
    fn sub<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = ()>  > {
        match self{
            AtomicArray::NativeAtomicArray(array) => array.sub(index, val),
            AtomicArray::GenericAtomicArray(array) => array.sub(index, val),
        }
    }
    fn fetch_sub<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>>  > {
        match self{
            AtomicArray::NativeAtomicArray(array) => array.fetch_sub(index, val),
            AtomicArray::GenericAtomicArray(array) => array.fetch_sub(index, val),
        }
    }
    fn mul<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = ()>  > {
        match self{
            AtomicArray::NativeAtomicArray(array) => array.mul(index, val),
            AtomicArray::GenericAtomicArray(array) => array.mul(index, val),
        }
    }
    fn fetch_mul<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>>  > {
        match self{
            AtomicArray::NativeAtomicArray(array) => array.fetch_mul(index, val),
            AtomicArray::GenericAtomicArray(array) => array.fetch_mul(index, val),
        }
    }
    fn div<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = ()>  > {
        match self{
            AtomicArray::NativeAtomicArray(array) => array.div(index, val),
            AtomicArray::GenericAtomicArray(array) => array.div(index, val),
        }
    }
    fn fetch_div<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>>  > {
        match self{
            AtomicArray::NativeAtomicArray(array) => array.fetch_div(index, val),
            AtomicArray::GenericAtomicArray(array) => array.fetch_div(index, val),
        }
    }
}

impl<T: ElementBitWiseOps + 'static> BitWiseOps<T> for AtomicArray<T> {
    fn bit_and<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = ()>  > {
        // println!("and val {:?}",val);
        match self {
            AtomicArray::NativeAtomicArray(array) => array.bit_and(index, val),
            AtomicArray::GenericAtomicArray(array) => array.bit_and(index, val),
        }
    }
    fn fetch_bit_and<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>>  > {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.fetch_bit_and(index, val),
            AtomicArray::GenericAtomicArray(array) => array.fetch_bit_and(index, val),
        }
    }
    fn bit_or<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = ()>  > {
        // println!("or");
        match self {
            AtomicArray::NativeAtomicArray(array) => array.bit_or(index, val),
            AtomicArray::GenericAtomicArray(array) => array.bit_or(index, val),
        }
    }
    fn fetch_bit_or<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = Vec<T>>  > {
        // println!("fetch_or");
        match self {
            AtomicArray::NativeAtomicArray(array) => array.fetch_bit_or(index, val),
            AtomicArray::GenericAtomicArray(array) => array.fetch_bit_or(index, val),
        }
    }
}
