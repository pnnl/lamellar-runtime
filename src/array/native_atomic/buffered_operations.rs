use crate::active_messaging::*;
use crate::array::native_atomic::*;
use crate::array::*;
// use crate::memregion::Dist;
use std::any::TypeId;
use std::collections::HashMap;

type BufFn = fn(NativeAtomicByteArrayWeak) -> Arc<dyn BufferOp>;

lazy_static! {
    pub(crate) static ref BUFOPS: HashMap<TypeId, BufFn> = {
        let mut map = HashMap::new();
        for op in crate::inventory::iter::<NativeAtomicArrayOpBuf> {
            map.insert(op.id.clone(), op.op);
        }
        map
    };
}

pub struct NativeAtomicArrayOpBuf {
    pub id: TypeId,
    pub op: BufFn,
}

crate::inventory::collect!(NativeAtomicArrayOpBuf);

impl<T: AmDist + Dist + 'static> NativeAtomicArray<T> {
    // pub(crate) fn initiate_op<'a>(
    //     &self,
    //     val: T,
    //     index: impl OpInput<'a, usize>,
    //     op: ArrayOpCmd<T>,
    // ) -> Box<dyn LamellarRequest<Output = ()>  > {
    //     self.array.initiate_op(val, index, op)
    // }

    // pub(crate) fn initiate_fetch_op<'a>(
    //     &self,
    //     val: T,
    //     index: impl OpInput<'a, usize>,
    //     op: ArrayOpCmd<T>,
    // ) -> Box<dyn LamellarRequest<Output = Vec<T>>  > {
    //     self.array.initiate_fetch_op(val, index, op)
    // }

    pub fn store<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.array.initiate_op(val, index, ArrayOpCmd::Store)
    }

    pub fn load<'a>(
        &self,
        index: impl OpInput<'a, usize>,
    ) -> Pin<Box<dyn Future<Output = Vec<T>> + Send>> {
        let dummy_val = self.array.dummy_val(); //we dont actually do anything with this except satisfy apis;
        self.array
            .initiate_fetch_op(dummy_val, index, ArrayOpCmd::Load)
    }

    pub fn swap<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Pin<Box<dyn Future<Output = Vec<T>> + Send>> {
        self.array.initiate_fetch_op(val, index, ArrayOpCmd::Swap)
    }
}
impl<T: AmDist + Dist + std::cmp::Eq + 'static> NativeAtomicArray<T> {
    pub fn compare_exchange<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        old: T,
        new: T,
    ) -> Pin<Box<dyn Future<Output = Vec<Result<T, T>>>>> {
        self.array
            .initiate_result_op(new, index, ArrayOpCmd::CompareExchange(old))
    }
}
impl<T: AmDist + Dist + std::cmp::PartialEq + 'static> NativeAtomicArray<T> {
    pub fn compare_exchange_epsilon<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        old: T,
        new: T,
        eps: T,
    ) -> Pin<Box<dyn Future<Output = Vec<Result<T, T>>>>> {
        self.array
            .initiate_result_op(new, index, ArrayOpCmd::CompareExchangeEps(old, eps))
    }
}

impl<T: ElementArithmeticOps + 'static> ArithmeticOps<T> for NativeAtomicArray<T> {
    fn add<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.array.initiate_op(val, index, ArrayOpCmd::Add)
    }
    fn fetch_add<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Pin<Box<dyn Future<Output = Vec<T>> + Send>> {
        self.array
            .initiate_fetch_op(val, index, ArrayOpCmd::FetchAdd)
    }
    fn sub<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.array.initiate_op(val, index, ArrayOpCmd::Sub)
    }
    fn fetch_sub<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Pin<Box<dyn Future<Output = Vec<T>> + Send>> {
        self.array
            .initiate_fetch_op(val, index, ArrayOpCmd::FetchSub)
    }
    fn mul<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.array.initiate_op(val, index, ArrayOpCmd::Mul)
    }
    fn fetch_mul<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Pin<Box<dyn Future<Output = Vec<T>> + Send>> {
        self.array
            .initiate_fetch_op(val, index, ArrayOpCmd::FetchMul)
    }
    fn div<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.array.initiate_op(val, index, ArrayOpCmd::Div)
    }
    fn fetch_div<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Pin<Box<dyn Future<Output = Vec<T>> + Send>> {
        self.array
            .initiate_fetch_op(val, index, ArrayOpCmd::FetchDiv)
    }
}

impl<T: ElementBitWiseOps + 'static> BitWiseOps<T> for NativeAtomicArray<T> {
    fn bit_and<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.array.initiate_op(val, index, ArrayOpCmd::And)
    }
    fn fetch_bit_and<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Pin<Box<dyn Future<Output = Vec<T>> + Send>> {
        self.array
            .initiate_fetch_op(val, index, ArrayOpCmd::FetchAnd)
    }

    fn bit_or<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.array.initiate_op(val, index, ArrayOpCmd::Or)
    }
    fn fetch_bit_or<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: T,
    ) -> Pin<Box<dyn Future<Output = Vec<T>> + Send>> {
        self.array
            .initiate_fetch_op(val, index, ArrayOpCmd::FetchOr)
    }
}
