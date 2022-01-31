use crate::lamellar_request::LamellarRequest;
use crate::memregion::{
    local::LocalMemoryRegion, shared::SharedMemoryRegion, Dist, LamellarMemoryRegion,
};
use crate::{active_messaging::*, LamellarTeamRT};

use async_trait::async_trait;
use enum_dispatch::enum_dispatch;
use futures_lite::Future;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

pub(crate) mod r#unsafe;
pub use r#unsafe::{operations::UnsafeArrayOp, UnsafeArray, UnsafeByteArray};
pub(crate) mod read_only;
pub use read_only::{ReadOnlyArray, ReadOnlyByteArray};
pub(crate) mod local_only;
pub use local_only::LocalOnlyArray;
pub(crate) mod atomic;
pub use atomic::{operations::AtomicArrayOp, AtomicArray, AtomicByteArray, AtomicOps};

pub(crate) mod collective_atomic;
pub use collective_atomic::{
    operations::CollectiveAtomicArrayOp, CollectiveAtomicArray, CollectiveAtomicByteArray,
};

pub mod iterator;
pub use iterator::distributed_iterator::DistributedIterator;
// use iterator::distributed_iterator::{DistIter, DistIterMut, DistIteratorLauncher};
// use iterator::serial_iterator::LamellarArrayIter;
pub use iterator::serial_iterator::{SerialIterator, SerialIteratorIter};

pub(crate) type ReduceGen =
    fn(LamellarByteArray, usize) -> Arc<dyn RemoteActiveMessage + Send + Sync>;

lazy_static! {
    pub(crate) static ref REDUCE_OPS: HashMap<(std::any::TypeId, String), ReduceGen> = {
        let mut temp = HashMap::new();
        for reduction_type in crate::inventory::iter::<ReduceKey> {
            temp.insert(
                (reduction_type.id.clone(), reduction_type.name.clone()),
                reduction_type.gen,
            );
        }
        temp
    };
}

pub struct ReduceKey {
    pub id: std::any::TypeId,
    pub name: String,
    pub gen: ReduceGen,
}
crate::inventory::collect!(ReduceKey);

lamellar_impl::generate_reductions_for_type_rt!(u8, u16, u32, u64, u128, usize);
lamellar_impl::generate_ops_for_type_rt!(true, u8, u16, u32, u64, u128, usize);

lamellar_impl::generate_reductions_for_type_rt!(i8, i16, i32, i64, i128, isize);
lamellar_impl::generate_ops_for_type_rt!(true, i8, i16, i32, i64, i128, isize);

lamellar_impl::generate_reductions_for_type_rt!(f32, f64);
lamellar_impl::generate_ops_for_type_rt!(false, f32, f64);

#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug)]
pub enum Distribution {
    Block,
    Cyclic,
}

#[derive(Hash, std::cmp::PartialEq, std::cmp::Eq, Clone)]
pub enum ArrayRdmaCmd {
    Put,
    PutAm,
    Get(bool), //bool true == immediate, false = async
    GetAm,
}

#[derive(Hash, std::cmp::PartialEq, std::cmp::Eq, Clone, Debug, Copy)]
pub enum ArrayOpCmd {
    Add,
    FetchAdd,
    Sub,
    FetchSub,
    Mul,
    FetchMul,
    Div,
    FetchDiv,
    And,
    FetchAnd,
    Or,
    FetchOr,
    Store,
    Load,
    Swap,
}

pub trait ElementOps: AmDist + Dist + Sized {}
impl<T> ElementOps for T where T: AmDist + Dist {}

pub trait ElementArithmeticOps:
    std::ops::AddAssign
    + std::ops::SubAssign
    + std::ops::MulAssign
    + std::ops::DivAssign
    + AmDist
    + Dist
    + Sized
{
}
impl<T> ElementArithmeticOps for T where
    T: std::ops::AddAssign
        + std::ops::SubAssign
        + std::ops::MulAssign
        + std::ops::DivAssign
        + AmDist
        + Dist
{
}

pub trait ElementBitWiseOps:
    std::ops::BitAndAssign + std::ops::BitOrAssign + AmDist + Dist + Sized
{
}
impl<T> ElementBitWiseOps for T where
    T: std::ops::BitAndAssign + std::ops::BitOrAssign + AmDist + Dist
{
}

pub trait ArithmeticOps<T: ElementArithmeticOps> {
    fn add(
        &self,
        index: usize,
        val: T,
    ) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>>;
    fn fetch_add(&self, index: usize, val: T)
        -> Box<dyn LamellarRequest<Output = T> + Send + Sync>;

    fn sub(
        &self,
        index: usize,
        val: T,
    ) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>>;
    fn fetch_sub(&self, index: usize, val: T)
        -> Box<dyn LamellarRequest<Output = T> + Send + Sync>;

    fn mul(
        &self,
        index: usize,
        val: T,
    ) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>>;

    fn fetch_mul(&self, index: usize, val: T)
        -> Box<dyn LamellarRequest<Output = T> + Send + Sync>;

    fn div(
        &self,
        index: usize,
        val: T,
    ) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>>;

    fn fetch_div(&self, index: usize, val: T)
        -> Box<dyn LamellarRequest<Output = T> + Send + Sync>;
}
pub trait BitWiseOps<T: ElementBitWiseOps> {
    fn bit_and(
        &self,
        index: usize,
        val: T,
    ) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>>;

    fn fetch_bit_and(
        &self,
        index: usize,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = T> + Send + Sync>;

    fn bit_or(
        &self,
        index: usize,
        val: T,
    ) -> Option<Box<dyn LamellarRequest<Output = ()> + Send + Sync>>;

    fn fetch_bit_or(
        &self,
        index: usize,
        val: T,
    ) -> Box<dyn LamellarRequest<Output = T> + Send + Sync>;
}

//perform the specified operation in place, returning the original value
pub trait LocalArithmeticOps<T: Dist + ElementArithmeticOps> {
    fn local_add(&self, index: usize, val: T) {
        self.local_fetch_add(index, val);
    }
    fn local_fetch_add(&self, index: usize, val: T) -> T;
    fn local_sub(&self, index: usize, val: T) {
        self.local_fetch_sub(index, val);
    }
    fn local_fetch_sub(&self, index: usize, val: T) -> T;
    fn local_mul(&self, index: usize, val: T) {
        self.local_fetch_mul(index, val);
    }
    fn local_fetch_mul(&self, index: usize, val: T) -> T;
    fn local_div(&self, index: usize, val: T) {
        self.local_fetch_div(index, val);
    }
    fn local_fetch_div(&self, index: usize, val: T) -> T;
}
pub trait LocalBitWiseOps<T: Dist + ElementBitWiseOps> {
    fn local_bit_and(&self, index: usize, val: T) {
        self.local_fetch_bit_and(index, val);
    }
    fn local_fetch_bit_and(&self, index: usize, val: T) -> T;
    fn local_bit_or(&self, index: usize, val: T) {
        self.local_fetch_bit_or(index, val);
    }
    fn local_fetch_bit_or(&self, index: usize, val: T) -> T;
}

pub trait LocalAtomicOps<T: Dist + ElementOps> {
    fn local_load(&self, index: usize, val: T) -> T;
    fn local_store(&self, index: usize, val: T);
    fn local_swap(&self, index: usize, val: T) -> T;
}

pub struct LocalOpResult<T: Dist> {
    val: T,
}

#[async_trait]
impl<T: Dist> LamellarRequest for LocalOpResult<T> {
    type Output = T;
    async fn into_future(self: Box<Self>) -> Option<Self::Output> {
        Some(self.val)
    }
    fn get(&self) -> Option<Self::Output> {
        Some(self.val)
    }
    fn get_all(&self) -> Vec<Option<Self::Output>> {
        vec![Some(self.val)]
    }
}

#[enum_dispatch(RegisteredMemoryRegion<T>, SubRegion<T>, MyFrom<T>,MemoryRegionRDMA<T>,AsBase)]
#[derive(Clone)]
pub enum LamellarArrayInput<T: Dist> {
    LamellarMemRegion(LamellarMemoryRegion<T>),
    SharedMemRegion(SharedMemoryRegion<T>), //when used as input/output we are only using the local data
    LocalMemRegion(LocalMemoryRegion<T>),
    // Unsafe(UnsafeArray<T>),
    // Vec(Vec<T>),
}

pub trait LamellarWrite {}
pub trait LamellarRead {}

impl<T: Dist> LamellarRead for T {}

impl<T: Dist> MyFrom<&T> for LamellarArrayInput<T> {
    fn my_from(val: &T, team: &Pin<Arc<LamellarTeamRT>>) -> Self {
        let buf: LocalMemoryRegion<T> = team.alloc_local_mem_region(1);
        unsafe {
            buf.as_mut_slice().unwrap()[0] = val.clone();
        }
        LamellarArrayInput::LocalMemRegion(buf)
    }
}

impl<T: Dist> MyFrom<T> for LamellarArrayInput<T> {
    fn my_from(val: T, team: &Pin<Arc<LamellarTeamRT>>) -> Self {
        let buf: LocalMemoryRegion<T> = team.alloc_local_mem_region(1);
        unsafe {
            buf.as_mut_slice().unwrap()[0] = val;
        }
        LamellarArrayInput::LocalMemRegion(buf)
    }
}

// impl<T: AmDist+ Clone + 'static> MyFrom<T> for LamellarArrayInput<T> {
//     fn my_from(val: T, team: &Arc<LamellarTeamRT>) -> Self {
//         let buf: LocalMemoryRegion<T> = team.alloc_local_mem_region(1);
//         unsafe {
//             buf.as_mut_slice().unwrap()[0] = val;
//         }
//         LamellarArrayInput::LocalMemRegion(buf)
//     }
// }

pub trait MyFrom<T: ?Sized> {
    fn my_from(val: T, team: &Pin<Arc<LamellarTeamRT>>) -> Self;
}

pub trait MyInto<T: ?Sized> {
    fn my_into(self, team: &Pin<Arc<LamellarTeamRT>>) -> T;
}

impl<T, U> MyInto<U> for T
where
    U: MyFrom<T>,
{
    fn my_into(self, team: &Pin<Arc<LamellarTeamRT>>) -> U {
        U::my_from(self, team)
    }
}

impl<T: Dist> MyFrom<&LamellarArrayInput<T>> for LamellarArrayInput<T> {
    fn my_from(lai: &LamellarArrayInput<T>, _team: &Pin<Arc<LamellarTeamRT>>) -> Self {
        lai.clone()
    }
}

#[enum_dispatch]
#[derive(serde::Serialize, serde::Deserialize, Clone)]
#[serde(bound = "T: Dist + serde::Serialize + serde::de::DeserializeOwned + 'static")]
pub enum LamellarReadArray<T: Dist + 'static> {
    UnsafeArray(UnsafeArray<T>),
    ReadOnlyArray(ReadOnlyArray<T>),
    AtomicArray(AtomicArray<T>),
    CollectiveAtomicArray(CollectiveAtomicArray<T>),
}

#[enum_dispatch]
#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub enum LamellarByteArray {
    //we intentially do not include "byte" in the variant name to ease construciton in the proc macros
    UnsafeArray(UnsafeByteArray),
    ReadOnlyArray(ReadOnlyByteArray),
    AtomicArray(AtomicByteArray),
    CollectiveAtomicArray(CollectiveAtomicByteArray),
}

impl<T: Dist + 'static> crate::DarcSerde for LamellarReadArray<T> {
    fn ser(&self, num_pes: usize, cur_pe: Result<usize, crate::IdError>) {
        // println!("in shared ser");
        match self {
            LamellarReadArray::UnsafeArray(array) => array.ser(num_pes, cur_pe),
            LamellarReadArray::ReadOnlyArray(array) => array.ser(num_pes, cur_pe),
            LamellarReadArray::AtomicArray(array) => array.ser(num_pes, cur_pe),
            LamellarReadArray::CollectiveAtomicArray(array) => array.ser(num_pes, cur_pe),
        }
    }
    fn des(&self, cur_pe: Result<usize, crate::IdError>) {
        // println!("in shared des");
        match self {
            LamellarReadArray::UnsafeArray(array) => array.des(cur_pe),
            LamellarReadArray::ReadOnlyArray(array) => array.des(cur_pe),
            LamellarReadArray::AtomicArray(array) => array.des(cur_pe),
            LamellarReadArray::CollectiveAtomicArray(array) => array.des(cur_pe),
        }
    }
}

#[enum_dispatch]
#[derive(serde::Serialize, serde::Deserialize, Clone)]
#[serde(bound = "T: Dist + serde::Serialize + serde::de::DeserializeOwned")]
pub enum LamellarWriteArray<T: Dist> {
    UnsafeArray(UnsafeArray<T>),
    AtomicArray(AtomicArray<T>),
    CollectiveAtomicArray(CollectiveAtomicArray<T>),
}

impl<T: Dist + 'static> crate::DarcSerde for LamellarWriteArray<T> {
    fn ser(&self, num_pes: usize, cur_pe: Result<usize, crate::IdError>) {
        // println!("in shared ser");
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.ser(num_pes, cur_pe),
            LamellarWriteArray::AtomicArray(array) => array.ser(num_pes, cur_pe),
            LamellarWriteArray::CollectiveAtomicArray(array) => array.ser(num_pes, cur_pe),
        }
    }
    fn des(&self, cur_pe: Result<usize, crate::IdError>) {
        // println!("in shared des");
        match self {
            LamellarWriteArray::UnsafeArray(array) => array.des(cur_pe),
            LamellarWriteArray::AtomicArray(array) => array.des(cur_pe),
            LamellarWriteArray::CollectiveAtomicArray(array) => array.des(cur_pe),
        }
    }
}

pub(crate) mod private {
    use crate::active_messaging::*;
    use crate::array::{
        AtomicArray, CollectiveAtomicArray, LamellarReadArray, LamellarWriteArray, ReadOnlyArray,
        UnsafeArray,
    };
    use crate::lamellar_request::LamellarRequest;
    use crate::memregion::Dist;
    use crate::LamellarTeamRT;
    use enum_dispatch::enum_dispatch;
    use std::pin::Pin;
    use std::sync::Arc;
    #[enum_dispatch(LamellarReadArray<T>,LamellarWriteArray<T>)]
    pub trait LamellarArrayPrivate<T: Dist> {
        // // fn my_pe(&self) -> usize;

        fn local_as_ptr(&self) -> *const T;
        fn local_as_mut_ptr(&self) -> *mut T;
        fn pe_for_dist_index(&self, index: usize) -> Option<usize>;
        fn pe_offset_for_dist_index(&self, pe: usize, index: usize) -> Option<usize>;
        unsafe fn into_inner(self) -> UnsafeArray<T>;
    }

    #[enum_dispatch(LamellarReadArray<T>,LamellarWriteArray<T>)]
    pub(crate) trait ArrayExecAm<T: Dist> {
        fn team(&self) -> Pin<Arc<LamellarTeamRT>>;
        fn team_counters(&self) -> Arc<AMCounters>;
        fn exec_am_local<F>(
            &self,
            am: F,
        ) -> Box<dyn LamellarRequest<Output = F::Output> + Send + Sync>
        where
            F: LamellarActiveMessage + LocalAM + Send + Sync + 'static,
        {
            self.team().exec_am_local_tg(am, Some(self.team_counters()))
        }
        fn exec_am_pe<F>(
            &self,
            pe: usize,
            am: F,
        ) -> Box<dyn LamellarRequest<Output = F::Output> + Send + Sync>
        where
            F: RemoteActiveMessage + LamellarAM + AmDist,
        {
            self.team()
                .exec_am_pe_tg(pe, am, Some(self.team_counters()))
        }
        fn exec_arc_am_pe<F>(
            &self,
            pe: usize,
            am: LamellarArcAm,
        ) -> Box<dyn LamellarRequest<Output = F> + Send + Sync>
        where
            F: AmDist,
        {
            self.team()
                .exec_arc_am_pe(pe, am, Some(self.team_counters()))
        }
        fn exec_am_all<F>(
            &self,
            am: F,
        ) -> Box<dyn LamellarRequest<Output = F::Output> + Send + Sync>
        where
            F: RemoteActiveMessage + LamellarAM + AmDist,
        {
            self.team().exec_am_all_tg(am, Some(self.team_counters()))
        }
    }
}

#[enum_dispatch(LamellarReadArray<T>,LamellarWriteArray<T>)]
pub trait LamellarArray<T: Dist>: private::LamellarArrayPrivate<T> {
    fn team(&self) -> Pin<Arc<LamellarTeamRT>>;
    fn my_pe(&self) -> usize;
    fn num_elems_local(&self) -> usize;
    fn len(&self) -> usize;
    fn barrier(&self);
    fn wait_all(&self);

    // /// Returns a distributed iterator for the LamellarArray
    // /// must be called accross all pes containing data in the array
    // /// iteration on a pe only occurs on the data which is locally present
    // /// with all pes iterating concurrently
    // /// blocking: true
    // pub fn dist_iter(&self) -> DistIter<'static, T>;

    // /// Returns a distributed iterator for the LamellarArray
    // /// must be called accross all pes containing data in the array
    // /// iteration on a pe only occurs on the data which is locally present
    // /// with all pes iterating concurrently
    // pub fn dist_iter_mut(&self) -> DistIterMut<'static, T>;

    // /// Returns an iterator for the LamellarArray, all iteration occurs on the PE
    // /// where this was called, data that is not local to the PE is automatically
    // /// copied and transferred
    // pub fn ser_iter(&self) -> LamellarArrayIter<'_, T> ;

    // /// Returns an iterator for the LamellarArray, all iteration occurs on the PE
    // /// where this was called, data that is not local to the PE is automatically
    // /// copied and transferred, array data is buffered to more efficiently make
    // /// use of network buffers
    // pub fn buffered_iter(&self, buf_size: usize) -> LamellarArrayIter<'_, T> ;
}

// pub trait ArrayIterator{

// }
// #[doc(hidden)]
// // #[enum_dispatch(LamellarReadArray<T>,LamellarWriteArray<T>)]
// pub trait AsBytes<T: Dist, B: Dist>: LamellarArray<T> {
//     // #[doc(hidden)]
//     // unsafe fn to_base_inner<B: Dist>(self) -> LamellarArray<B>;
//     type Array: LamellarArray<B>;
//     #[doc(hidden)]
//     unsafe fn as_bytes(&self) -> Self::Array;
// }

// #[doc(hidden)]
// // #[enum_dispatch(LamellarReadArray<T>,LamellarWriteArray<T>)]
// pub trait FromBytes<T: Dist,B: Dist>: LamellarArray<B> {
//     // #[doc(hidden)]
//     // unsafe fn to_base_inner<B: Dist>(self) -> LamellarArray<B>;
//     type Array: LamellarArray<T>;
//     #[doc(hidden)]
//     unsafe fn from_bytes(self) -> Self::Array;
// }

// impl <T: Dist> FromBytes<T,u8> for LamellarReadArray<u8>{
//     type Array = LamellarReadArray<T>;
//     #[doc(hidden)]
//     unsafe fn from_bytes(self) -> Self::Array{
//         match self {
//             LamellarReadArray::UnsafeArray(array) => array.from_bytes().into(),
//             LamellarReadArray::ReadOnlyArray(array) => array.from_bytes().into(),
//             LamellarReadArray::AtomicArray(array) => array.from_bytes().into(),
//             LamellarReadArray::CollectiveAtomicArray(array) => array.from_bytes().into(),
//         }
//     }
// }

pub trait SubArray<T: Dist>: LamellarArray<T> {
    type Array: LamellarArray<T>;
    fn sub_array<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self::Array;
    fn global_index(&self, sub_index: usize) -> usize;
}

#[enum_dispatch(LamellarReadArray<T>,LamellarWriteArray<T>)]
pub trait LamellarArrayGet<T: Dist + 'static>: LamellarArray<T> + Sync + Send {
    // this is non blocking call
    // the runtime does not manage checking for completion of message transmission
    // the user is responsible for ensuring the buffer remains valid
    // unsafe fn get_unchecked<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(
    //     &self,
    //     index: usize,
    //     buf: U,
    // );

    // a safe synchronous call that blocks untils the data as all been transfered
    // get data from self and write into buf
    fn iget<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(&self, index: usize, dst: U);

    // async get
    // get data from self and write into buf
    fn get<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(&self, index: usize, dst: U);

    // blocking call that gets the value stored and the provided index
    fn iat(&self, index: usize) -> T;
}

#[enum_dispatch(LamellarWriteArray<T>)]
pub trait LamellarArrayPut<T: Dist>: LamellarArray<T> + Sync + Send {
    //put data from buf into self
    fn iput<U: MyInto<LamellarArrayInput<T>> + LamellarRead>(&self, index: usize, src: U);
}

pub trait ArrayPrint<T: Dist + std::fmt::Debug>: LamellarArray<T> {
    fn print(&self);
}

pub trait LamellarArrayReduce<T>: LamellarArrayGet<T>
where
    T: Dist + serde::Serialize + serde::de::DeserializeOwned + 'static,
{
    fn get_reduction_op(&self, op: String) -> LamellarArcAm;
    fn reduce(&self, op: &str) -> Box<dyn LamellarRequest<Output = T> + Send + Sync>;
    fn sum(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync>;
    fn max(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync>;
    fn prod(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync>;
}

// impl<'a, T: AmDist + 'static> IntoIterator
//     for &'a LamellarArray<T>
// {
//     type Item = &'a T;
//     type IntoIter = SerialIteratorIter<LamellarArrayIter<'a, T>>;
//     fn into_iter(self) -> Self::IntoIter {
//         SerialIteratorIter {
//             iter: self.ser_iter(),
//         }
//     }
// }
