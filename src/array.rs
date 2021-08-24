use crate::{active_messaging::*, LamellarTeam, RemoteMemoryRegion}; //{ActiveMessaging,AMCounters,Cmd,Msg,LamellarAny,LamellarLocal};
                                                                    // use crate::lamellae::Lamellae;
                                                                    // use crate::lamellar_arch::LamellarArchRT;
use crate::memregion::{
    local::LocalMemoryRegion, shared::SharedMemoryRegion, AsBase, Dist, LamellarMemoryRegion,
};
// use crate::lamellar_request::{AmType, LamellarRequest, LamellarRequestHandle};
// use crate::lamellar_team::LamellarTeam;
// use crate::scheduler::{Scheduler,SchedulerQueue};

// use log::trace;
// use std::hash::{Hash, Hasher};
// use std::ops::{Index, IndexMut, Range, RangeFrom, RangeFull, RangeInclusive, RangeTo, RangeToInclusive};
// use std::any;
// use core::marker::PhantomData;
use std::collections::HashMap;
// use std::sync::atomic::Ordering;
use std::sync::Arc;
// use std::time::{Duration, Instant};
use enum_dispatch::enum_dispatch;

pub(crate) mod r#unsafe;
pub use r#unsafe::UnsafeArray;

pub(crate) type ReduceGen =
    fn(LamellarArray<u8>, usize) -> Arc<dyn RemoteActiveMessage + Send + Sync>;

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

// lamellar_impl::generate_reductions_for_type_rt!(u8, u16, u32, u64, u128, usize);
// lamellar_impl::generate_reductions_for_type_rt!(i8, i16, i32, i64, i128, isize);
// lamellar_impl::generate_reductions_for_type_rt!(f32, f64);

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum Distribution {
    Block,
    Cyclic,
}

#[enum_dispatch(RegisteredMemoryRegion<T>, SubRegion<T>, MyFrom<T>)]
// #[derive(serde::Serialize, serde::Deserialize, Clone)]
#[derive(Clone)]
pub enum LamellarArrayInput<T: Dist + 'static> {
    LamellarMemRegion(LamellarMemoryRegion<T>),
    SharedMemRegion(SharedMemoryRegion<T>),
    LocalMemRegion(LocalMemoryRegion<T>),
    // Unsafe(UnsafeArray<T>),
    // Vec(Vec<T>),
}

impl<T: Dist + 'static> MyFrom<&T> for LamellarArrayInput<T> {
    fn my_from(val: &T, team: &Arc<LamellarTeam>) -> Self {
        let buf: LocalMemoryRegion<T> = team.alloc_local_mem_region(1);
        unsafe {
            buf.as_mut_slice().unwrap()[0] = val.clone();
        }
        LamellarArrayInput::LocalMemRegion(buf)
    }
}

impl<T: Dist + 'static> MyFrom<T> for LamellarArrayInput<T> {
    fn my_from(val: T, team: &Arc<LamellarTeam>) -> Self {
        let buf: LocalMemoryRegion<T> = team.alloc_local_mem_region(1);
        unsafe {
            buf.as_mut_slice().unwrap()[0] = val;
        }
        LamellarArrayInput::LocalMemRegion(buf)
    }
}

pub trait MyFrom<T: ?Sized> {
    fn my_from(val: T, team: &Arc<LamellarTeam>) -> Self;
}

pub trait MyInto<T: ?Sized> {
    fn my_into(self, team: &Arc<LamellarTeam>) -> T;
}

impl<T, U> MyInto<U> for T
where
    U: MyFrom<T>,
{
    fn my_into(self, team: &Arc<LamellarTeam>) -> U {
        U::my_from(self, team)
    }
}

#[enum_dispatch(LamellarArrayRDMA<T>)]
#[derive(serde::Serialize, serde::Deserialize)]
pub enum LamellarArray<T: Dist + 'static> {
    Unsafe(UnsafeArray<T>),
}

#[enum_dispatch]
pub trait LamellarArrayRDMA<T>
where
    T: Dist + 'static,
{
    fn put<U: MyInto<LamellarArrayInput<T>>>(&self, index: usize, buf: U);
    // fn put<U: Into<LamellarArrayInput<T>>>(&self, index: usize, buf: U);
    // pub fn put_indirect(self, index: usize, buf: &impl LamellarBuffer<T>);
    fn get<U: MyInto<LamellarArrayInput<T>>>(&self, index: usize, buf: U);
    // fn get_raw_mem_region(&self) -> LamellarMemoryRegion<T>;
    // pub fn get_indirect(self, index: usize, buf: &mut impl LamellarBuffer<T>); //do we need a LamellarBufferMut?
    fn local_as_slice(&self) -> &[T];
}

// pub enum LamellarArrayElem<T: Dist +'static> {
//     UnsafeElem(UnsafeElem<T>),
// }

// impl<T> LamellarArrayElem<T>
// where T: Dist +'static{
//     pub fn set(&self, index)
// }

// pub trait LamellarArrayReduce<T>: LamellarArrayRDMA<T>
// // + std::ops::Index + std::ops::IndexMut
// where
//     T: serde::ser::Serialize
//         + serde::de::DeserializeOwned
//         + std::clone::Clone
//         + Send
//         + Sync
//         + std::fmt::Debug,
// {
//     fn wait_all(&self);
//     fn get_reduction_op(&self, op: String) -> LamellarArcAm {
//         unsafe {
//             REDUCE_OPS
//                 .get(&(std::any::TypeId::of::<T>(), op))
//                 .expect("unexpected reduction type")(
//                 self.rmr.clone().as_base::<u8>(), self.num_pes
//             )
//         }
//     }
// }

// pub(crate) trait LamellarBuffer<T>
// where T: serde::ser::Serialize
// + serde::de::DeserializeOwned
// + std::clone::Clone
// + Send
// + Sync
// + std::fmt::Debug{
//     pub(crate) fn to_rmr(self)-> impl RegisteredMemoryRegion;
//     pub(crate) fn from_rmr(self, data: impl RegisteredMemoryRegion);
// }

// pub trait LamellarArrayIndex<T>
// where T: ?Sized,{
//     type Output: ?Sized;
//     // fn get(self, array: &T) -> Option<&Self::Output>;
//     fn index(self, array: &T) -> &Self::Output;
// }

// //#[prof]
// impl<
//         T: serde::ser::Serialize
//             + serde::de::DeserializeOwned
//             + std::clone::Clone
//             + Send
//             + Sync
//             + std::fmt::Debug
//             + 'static,
//     > UnsafeArray<T>
// {
//     pub(crate) fn new(
//         team: Arc<LamellarTeam>,
//         array_size: usize,
//     ) -> UnsafeArray<T> {
//         let rmr = team.alloc_shared_mem_region(array_size);
//         UnsafeArray {
//             mem_region: Darc::new(team,rmr),
//         }
//     }

//     pub fn get_raw_mem_region(&self) -> &LamellarMemoryRegion<T> {
//         self.mem_region
//     }

//     pub fn wait_all(&self) {
//         let mut temp_now = Instant::now();
//         while self.team_counters.outstanding_reqs.load(Ordering::SeqCst) > 0 {
//             std::thread::yield_now();
//             if temp_now.elapsed() > Duration::new(60, 0) {
//                 println!(
//                     "in lamellar_array wait_all mype: {:?} cnt: {:?} {:?}",
//                     self.my_pe,
//                     self.team_counters.send_req_cnt.load(Ordering::SeqCst),
//                     self.team_counters.outstanding_reqs.load(Ordering::SeqCst),
//                     // self.counters.recv_req_cnt.load(Ordering::SeqCst),
//                 );
//                 // self.lamellae.print_stats();
//                 temp_now = Instant::now();
//             }
//         }
//     }

//     fn get_reduction_op(&self, op: String) -> LamellarArcAm {
//         unsafe {
//             REDUCE_OPS
//                 .get(&(std::any::TypeId::of::<T>(), op))
//                 .expect("unexpected reduction type")(
//                 self.rmr.clone().as_base::<u8>(), self.num_pes
//             )
//         }
//     }

//     fn reduce_inner(
//         &self,
//         func: LamellarArcAm,
//     ) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
//         trace!("[{:?}] team exec am all request", self.my_pe);
//         let (my_req, ireq) = LamellarRequestHandle::new(
//             1,
//             AmType::RegisteredFunction,
//             self.arch.clone(),
//             self.team_counters.outstanding_reqs.clone(),
//             self.world_counters.outstanding_reqs.clone(),
//             self.team.team.team_hash,
//             self.team.clone(),
//         );
//         self.world_counters.add_send_req(1);
//         self.team_counters.add_send_req(1);
//         let world = if let Some(world) = &self.team.world{
//             world.clone()
//         }
//         else{
//             self.team.clone()
//         };
//         self.scheduler.submit_req_new(
//             self.my_pe,
//             Some(self.my_pe),
//             ExecType::Am(Cmd::Exec),
//             my_req.id,
//             LamellarFunc::Am(func),
//             self.lamellae.clone(),
//             world,
//             self.team.clone(),
//             self.my_hash,
//             Some(ireq),
//         );
//         Box::new(my_req)
//     }
//     pub fn reduce(&self, op: &str) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
//         self.reduce_inner(self.get_reduction_op(op.to_string()))
//     }
//     pub fn sum(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
//         // let my_any: LamellarAny = Box::new(self.get_reduction_op("sum".to_string()) as LamellarBoxedAm);
//         self.reduce("sum")
//     }
//     pub fn prod(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
//         // let my_any: LamellarAny = Box::new(self.get_reduction_op("prod".to_string()) as LamellarBoxedAm);
//         self.reduce("prod")
//     }
//     pub fn max(&self) -> Box<dyn LamellarRequest<Output = T> + Send + Sync> {
//         // let my_any: LamellarAny = Box::new(self.get_reduction_op("max".to_string()) as LamellarBoxedAm);
//         self.reduce("max")
//     }
// }

// //#[prof]
// impl<
//         T: serde::ser::Serialize
//             + serde::de::DeserializeOwned
//             + std::clone::Clone
//             + Send
//             + Sync
//             + 'static,
//     > Drop for LamellarArray<T>
// {
//     fn drop(&mut self) {
//         // println!(
//         //     "[{:?}] team handle dropping {:?}",
//         //     self.team.my_pe,
//         //     self.team.get_pes()
//         // );
//         // self.team.wait_all();
//         // self.team.barrier();
//         // self.team.put_dropped();
//         // if let Ok(_my_index) = self.team.arch.team_pe(self.team.my_pe) {
//         //     self.team.drop_barrier();
//         // }
//         // if let Some(parent) = &self.team.parent {
//         //     parent.sub_teams.write().remove(&self.team.id);
//         // }
//         // self.teams.write().remove(&self.team.my_hash);
//     }
// }

/*
//todo something like RefCell
//so have a sub_array(), sub_array_mut(), sub_array_unchecked(), sub_array_mut_unchecked()
//
pub trait LamellarSubArray<T> {
    type Output;
    fn sub_array(&self, range: T) -> Self::Output;
}

// #[derive(serde::Serialize, serde::Deserialize)]
pub struct LamellarLocalArray<
    T: serde::ser::Serialize + serde::de::DeserializeOwned + std::clone::Clone + Send + Sync + 'static,
> {
    pub(crate) addr: usize,
    pub(crate) size: usize,
    cnt: Arc<AtomicUsize>,
    rdma: Arc<dyn LamellaeRDMA>,
    phantom: PhantomData<T>,
}

impl<
        T: serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + Send
            + Sync
            + 'static,
    > LamellarLocalArray<T>
{
    pub(crate) fn new(
        //maybe add a new_uninit
        size: usize,
        init: T,
        rdma: Arc<dyn LamellaeRDMA>,
    ) -> LamellarLocalArray<T> {
        let cnt = Arc::new(AtomicUsize::new(1));
        let addr = rdma
            .rt_alloc(size * std::mem::size_of::<T>())
            .unwrap()
            + rdma.base_addr();
        let temp = LamellarLocalArray {
            addr: addr,
            size: size,
            cnt: cnt,
            rdma: rdma,
            phantom: PhantomData,
        };
        unsafe {std::slice::from_raw_parts_mut(addr as *mut T, size).iter_mut().for_each(|x| *x = init.clone());}
        // unsafe { std::slice::from_raw_parts_mut(addr as *mut T, size)[size - 1] = init }

        // println!("new local array: addr {:?} size {:?}",temp.addr,temp.size);
        temp
    }
    pub fn as_slice(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.addr as *const T, self.size) }
    }

    // pub unsafe fn as_mut_slice(&self) -> &mut [T] {
    //     std::slice::from_raw_parts_mut(self.addr as *mut T, self.size)
    // }
}

impl<
        T: serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + Send
            + Sync
            + 'static,
    > RegisteredMemoryRegion for LamellarLocalArray<T>
{
    type Output = T;
    fn len(&self) -> usize {
        self.size
    }
    fn addr(&self) ->usize {
        self.addr
    }
    unsafe fn as_mut_slice(&self) -> &mut [T] {
        std::slice::from_raw_parts_mut(self.addr as *mut T, self.size)
    }
    fn as_ptr(&self) -> *const T{
        self.addr as *const T
    }
    fn as_mut_ptr(&self) -> *mut T{
        self.addr as *mut T
    }
}

impl<
        T: serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + Send
            + Sync
            + 'static,
    > Clone for LamellarLocalArray<T>
{
    fn clone(&self) -> Self {
        self.cnt.fetch_add(1, Ordering::SeqCst);
        let lla = LamellarLocalArray {
            addr: self.addr,
            size: self.size,
            cnt: self.cnt.clone(),
            rdma: self.rdma.clone(),
            phantom: self.phantom,
        };
        lla
    }
}

use std::ops::{Index, IndexMut, Range, RangeFrom, RangeFull, RangeInclusive, RangeTo, RangeToInclusive};
impl<T> LamellarSubArray<usize> for LamellarLocalArray<T>
where
    T: serde::ser::Serialize
        + serde::de::DeserializeOwned
        + std::clone::Clone
        + Send
        + Sync
        + 'static,
{
    type Output = LamellarLocalArray<T>;

    fn sub_array(&self, idx: usize) -> Self::Output {
        assert!(
            idx < self.size,
            "index out of bounds"
        );
        self.cnt.fetch_add(1, Ordering::SeqCst);
        let lla = LamellarLocalArray {
            addr: self.addr + idx*std::mem::size_of::<T>(),
            size: 1,
            cnt: self.cnt.clone(),
            rdma: self.rdma.clone(),
            phantom: self.phantom,
        };
        lla
    }
}
impl<T> LamellarSubArray<Range<usize>> for LamellarLocalArray<T>
where
    T: serde::ser::Serialize
        + serde::de::DeserializeOwned
        + std::clone::Clone
        + Send
        + Sync
        + 'static,
{
    type Output = LamellarLocalArray<T>;

    fn sub_array(&self, range: Range<usize>) -> Self::Output {
        assert!(
            range.start < range.end,
            "empty LamellarArrays not yet supported"
        );
        self.cnt.fetch_add(1, Ordering::SeqCst);
        let lla = LamellarLocalArray {
            addr: self.addr + range.start*std::mem::size_of::<T>(),
            size: range.end - range.start,
            cnt: self.cnt.clone(),
            rdma: self.rdma.clone(),
            phantom: self.phantom,
        };
        lla
    }
}

impl<T> LamellarSubArray<RangeFrom<usize>> for LamellarLocalArray<T>
where
    T: serde::ser::Serialize
        + serde::de::DeserializeOwned
        + std::clone::Clone
        + Send
        + Sync
        + 'static,
{
    type Output = LamellarLocalArray<T>;

    fn sub_array(&self, range: RangeFrom<usize>) -> Self::Output {
        assert!(range.start < self.size, "invalid start index");
        self.cnt.fetch_add(1, Ordering::SeqCst);
        let lla = LamellarLocalArray {
            addr: self.addr + range.start*std::mem::size_of::<T>(),
            size: self.size - range.start,
            cnt: self.cnt.clone(),
            rdma: self.rdma.clone(),
            phantom: self.phantom,
        };
        lla
    }
}

impl<T> LamellarSubArray<RangeFull> for LamellarLocalArray<T>
where
    T: serde::ser::Serialize
        + serde::de::DeserializeOwned
        + std::clone::Clone
        + Send
        + Sync
        + 'static,
{
    type Output = LamellarLocalArray<T>;

    fn sub_array(&self, _range: RangeFull) -> Self::Output {
        self.clone()
    }
}

impl<T> LamellarSubArray<RangeInclusive<usize>> for LamellarLocalArray<T>
where
    T: serde::ser::Serialize
        + serde::de::DeserializeOwned
        + std::clone::Clone
        + Send
        + Sync
        + 'static,
{
    type Output = LamellarLocalArray<T>;

    fn sub_array(&self, range: RangeInclusive<usize>) -> Self::Output {
        assert!(range.start() <= range.end(), "invalid start index");
        self.cnt.fetch_add(1, Ordering::SeqCst);
        let lla = LamellarLocalArray {
            addr: self.addr + range.start()+std::mem::size_of::<T>(),
            size: (range.end() - range.start()) + 1,
            cnt: self.cnt.clone(),
            rdma: self.rdma.clone(),
            phantom: self.phantom,
        };
        lla
    }
}

impl<T> LamellarSubArray<RangeTo<usize>> for LamellarLocalArray<T>
where
    T: serde::ser::Serialize
        + serde::de::DeserializeOwned
        + std::clone::Clone
        + Send
        + Sync
        + 'static,
{
    type Output = LamellarLocalArray<T>;

    fn sub_array(&self, range: RangeTo<usize>) -> Self::Output {
        assert!(range.end <= self.size, "invalid end index");
        self.cnt.fetch_add(1, Ordering::SeqCst);
        let lla = LamellarLocalArray {
            addr: self.addr,
            size: range.end,
            cnt: self.cnt.clone(),
            rdma: self.rdma.clone(),
            phantom: self.phantom,
        };
        lla
    }
}

impl<T> LamellarSubArray<RangeToInclusive<usize>> for LamellarLocalArray<T>
where
    T: serde::ser::Serialize
        + serde::de::DeserializeOwned
        + std::clone::Clone
        + Send
        + Sync
        + 'static,
{
    type Output = LamellarLocalArray<T>;

    fn sub_array(&self, range: RangeToInclusive<usize>) -> Self::Output {
        assert!(range.end < self.size, "invalid end index");
        self.cnt.fetch_add(1, Ordering::SeqCst);
        let lla = LamellarLocalArray {
            addr: self.addr,
            size: range.end + 1,
            cnt: self.cnt.clone(),
            rdma: self.rdma.clone(),
            phantom: self.phantom,
        };
        lla
    }
}

impl<T, Idx> Index<Idx> for LamellarLocalArray<T>
where
    T: serde::ser::Serialize
        + serde::de::DeserializeOwned
        + std::clone::Clone
        + Send
        + Sync
        + 'static,
    Idx: std::slice::SliceIndex<[T]>,
{
    type Output = Idx::Output;

    fn index(&self, index: Idx) -> &Self::Output {
        unsafe { &std::slice::from_raw_parts(self.addr as *mut T, self.size)[index] }
    }
}

impl<T, Idx> IndexMut<Idx> for LamellarLocalArray<T>
where
    T: serde::ser::Serialize
        + serde::de::DeserializeOwned
        + std::clone::Clone
        + Send
        + Sync
        + 'static,
    Idx: std::slice::SliceIndex<[T]>,
{
    fn index_mut(&mut self, index: Idx) -> &mut Self::Output {
        unsafe { &mut std::slice::from_raw_parts_mut(self.addr as *mut T, self.size)[index] }
    }
}

impl<
        T: serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + Send
            + Sync
            + 'static,
    > Drop for LamellarLocalArray<T>
{
    fn drop(&mut self) {
        let cnt = self.cnt.fetch_sub(1, Ordering::SeqCst);
        // //println!("drop: {:?}",self);
        if cnt == 1 {
            // println!("trying to dropping mem region {:?}",self);
            self.rdma.rt_free(self.addr - self.rdma.base_addr());
            // //println!("dropping mem region {:?}",self);
        }
    }
}




#[cfg(test)]
mod tests {
    use super::*;
    use crate::lamellae::{Backend, create_lamellae};

    #[test]
    fn single_element_sub_array(){
        let lamellae = create_lamellae(Backend::Local);
        let array = LamellarLocalArray::new(100,0usize,lamellae.get_rdma());
        let sub_array = array.sub_array(43);
        assert_eq!(sub_array.len(), 1);
    }

    #[test]
    fn range_sub_array(){
        let lamellae = create_lamellae(Backend::Local);
        let array = LamellarLocalArray::new(100,0usize,lamellae.get_rdma());
        let start = 20;
        let end = 60;
        let sub_array = array.sub_array(start..end);
        assert_eq!(sub_array.len(), end-start);
    }

    #[test]
    fn range_from_sub_array(){
        let lamellae = create_lamellae(Backend::Local);
        let array = LamellarLocalArray::new(100,0usize,lamellae.get_rdma());
        let start = 20;
        let sub_array = array.sub_array(start..);
        assert_eq!(sub_array.len(), array.len()-start);
    }

    #[test]
    fn range_full_sub_array(){
        let lamellae = create_lamellae(Backend::Local);
        let array = LamellarLocalArray::new(100,0usize,lamellae.get_rdma());
        let sub_array = array.sub_array(..);
        assert_eq!(sub_array.len(), array.len());
    }

    #[test]
    fn range_inclusive_sub_array(){
        let lamellae = create_lamellae(Backend::Local);
        let array = LamellarLocalArray::new(100,0usize,lamellae.get_rdma());
        let start = 20;
        let end = 60;
        let sub_array = array.sub_array(start..=end);
        assert_eq!(sub_array.len(), 1+end-start);
    }

    #[test]
    fn range_to_sub_array(){
        let lamellae = create_lamellae(Backend::Local);
        let array = LamellarLocalArray::new(100,0usize,lamellae.get_rdma());
        let end = 60;
        let sub_array = array.sub_array(..end);
        assert_eq!(sub_array.len(), end);
    }

    #[test]
    fn range_to_inclusive_sub_array(){
        let lamellae = create_lamellae(Backend::Local);
        let array = LamellarLocalArray::new(100,0usize,lamellae.get_rdma());
        let end = 60;
        let sub_array = array.sub_array(..=end);
        assert_eq!(sub_array.len(), end+1);
    }
}
*/
