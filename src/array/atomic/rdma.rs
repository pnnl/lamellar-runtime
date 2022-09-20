use crate::array::atomic::*;
// use crate::array::private::ArrayExecAm;
use crate::array::LamellarWrite;
use crate::array::*;
use crate::memregion::Dist;

type GetFn = fn(AtomicByteArray, usize, usize) -> LamellarArcAm;
pub struct AtomicArrayGet {
    pub id: TypeId,
    pub op: GetFn,
}
crate::inventory::collect!(AtomicArrayGet);
lazy_static! {
    pub(crate) static ref GET_OPS: HashMap<TypeId, GetFn> = {
        let mut map = HashMap::new();
        for get in crate::inventory::iter::<AtomicArrayGet> {
            map.insert(get.id.clone(),get.op);
        }
        map
        // map.insert(TypeId::of::<f64>(), f64_add::add as AddFn );
    };
}

type PutFn = fn(AtomicByteArray, usize, usize, Vec<u8>) -> LamellarArcAm;
pub struct AtomicArrayPut {
    pub id: TypeId,
    pub op: PutFn,
}
crate::inventory::collect!(AtomicArrayPut);
lazy_static! {
    pub(crate) static ref PUT_OPS: HashMap<TypeId, PutFn> = {
        let mut map = HashMap::new();
        for put in crate::inventory::iter::<AtomicArrayPut> {
            map.insert(put.id.clone(),put.op);
        }
        map
        // map.insert(TypeId::of::<f64>(), f64_add::add as AddFn );
    };
}

impl<T: Dist> AtomicArray<T> {
    // pub fn iget<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(&self, index: usize, buf: U) {
    //     self.exec_am_local(InitGetAm {
    //         array: self.clone(),
    //         index: index,
    //         buf: buf.my_into(&self.array.team()),
    //     })
    //     .get();
    // }
    pub(crate) fn internal_get<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(
        &self,
        index: usize,
        buf: U,
    ) -> Box<dyn LamellarArrayRequest<Output = ()>> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.internal_get(index, buf),
            AtomicArray::GenericAtomicArray(array) => array.internal_get(index, buf),
        }
    }
    pub fn get<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(
        &self,
        index: usize,
        buf: U,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.internal_get(index, buf).into_future()
    }
    // pub fn iput<U: MyInto<LamellarArrayInput<T>> + LamellarRead>(&self, index: usize, buf: U) {
    //     self.exec_am_local(InitPutAm {
    //         array: self.clone(),
    //         index: index,
    //         buf: buf.my_into(&self.array.team()),
    //     })
    //     .get();
    // }
    pub(crate) fn internal_put<U: MyInto<LamellarArrayInput<T>> + LamellarRead>(
        &self,
        index: usize,
        buf: U,
    ) -> Box<dyn LamellarArrayRequest<Output = ()>> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.internal_put(index, buf),
            AtomicArray::GenericAtomicArray(array) => array.internal_put(index, buf),
        }
    }

    pub fn put<U: MyInto<LamellarArrayInput<T>> + LamellarRead>(
        &self,
        index: usize,
        buf: U,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.internal_put(index, buf).into_future()
    }

    pub fn internal_at(&self, index: usize) -> Box<dyn LamellarArrayRequest<Output = T>> {
        match self {
            AtomicArray::NativeAtomicArray(array) => array.internal_at(index),
            AtomicArray::GenericAtomicArray(array) => array.internal_at(index),
        }
    }

    pub fn at(&self, index: usize) -> Pin<Box<dyn Future<Output = T> + Send>> {
        self.internal_at(index).into_future()
    }
}

// impl<T: Dist + 'static> LamellarArrayGet<T> for AtomicArray<T> {
//     // unsafe fn get_unchecked<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(
//     //     &self,
//     //     index: usize,
//     //     buf: U,
//     // ) {
//     //     self.array.get_unchecked(index, buf)
//     // }
//     // fn iget<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(&self, index: usize, buf: U) {
//     //     self.iget(index, buf);
//     // }
//     fn get<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(
//         &self,
//         index: usize,
//         buf: U,
//     ) -> Box<dyn LamellarArrayRequest<Output = ()>  > {
//         self.get(index, buf)
//     }
//     fn at(&self, index: usize) -> Box<dyn LamellarArrayRequest<Output = T>  > {
//         self.at(index)
//     }
// }

// impl<T: Dist> LamellarArrayPut<T> for AtomicArray<T> {
//     // fn put_unchecked<U: MyInto<LamellarArrayInput<T>> + LamellarRead>(&self, index: usize, buf: U) {
//     //     self.array.put_unchecked(index, buf)
//     // }
//     fn put<U: MyInto<LamellarArrayInput<T>> + LamellarRead>(
//         &self,
//         index: usize,
//         buf: U,
//     ) -> Box<dyn LamellarArrayRequest<Output = ()>  > {
//         self.put(index, buf)
//     }
// }

// #[lamellar_impl::AmLocalDataRT(Debug)]
// struct InitGetAm<T: Dist> {
//     array: AtomicArray<T>, //inner of the indices we need to place data into
//     index: usize,
//     buf: LamellarArrayInput<T>,
// }

// #[lamellar_impl::rt_am_local]
// impl<T: Dist + 'static> LamellarAm for InitGetAm<T> {
//     async fn exec(self) {
//         let mut reqs = vec![];
//         // println!("initgetam {:?} {:?}",self.index,self.index+self.buf.len());
//         for pe in self
//             .array
//             .array
//             .pes_for_range(self.index, self.buf.len())
//             .into_iter()
//         {
//             // println!("pe {:?}",pe);
//             //get the proper remote am from the ops map
//             if let Some(remote_am_gen) = GET_OPS.get(&TypeId::of::<T>()) {
//                 let am: LamellarArcAm =
//                     remote_am_gen(self.array.clone().into(), self.index, self.buf.len());
//                 reqs.push(self.array.exec_arc_am_pe::<Vec<u8>>(pe, am).into_future());
//             } else {
//                 let name = std::any::type_name::<T>().split("::").last().unwrap();
//                 panic!("the type {:?} has not been registered for atomic rdma operations, this typically means you need to derive \"AtomicRdma\" for the type,
//                 you can use the lamellar::AmData attribute proc macro to automatically derive it, e.g.
//                 #[lamellar::AMData(AtomicRdma, any other traits you derive)]
//                 struct {:?}{{
//                     ....
//                 }}",name,name);
//             }
//         }
//         unsafe {
//             match self.array.array.inner.distribution {
//                 Distribution::Block => {
//                     let u8_buf = self.buf.clone().to_base::<u8>();
//                     let mut cur_index = 0;
//                     for req in reqs.drain(..) {
//                         let data = req.await.unwrap();
//                         u8_buf.put_slice(lamellar::current_pe, cur_index, &data);
//                         cur_index += data.len();
//                     }
//                 }
//                 Distribution::Cyclic => {
//                     let buf_slice = self.buf.as_mut_slice().unwrap();
//                     let num_pes = reqs.len();
//                     for (start_index, req) in reqs.drain(..).enumerate() {
//                         let data = req.await.unwrap();
//                         let data_t_ptr = data.as_ptr() as *const T;
//                         let data_t_len = if data.len() % std::mem::size_of::<T>() == 0 {
//                             data.len() / std::mem::size_of::<T>()
//                         } else {
//                             panic!("memory align error");
//                         };
//                         // println!("start_index {:?}  data {:?} {:?} {:?}",start_index,data, data.len(),data_t_len);
//                         let data_t_slice = std::slice::from_raw_parts(data_t_ptr, data_t_len);
//                         for (i, val) in data_t_slice.iter().enumerate() {
//                             buf_slice[start_index + i * num_pes] = *val;
//                         }
//                     }
//                 }
//             }
//         }
//     }
// }

// //the remote get am is implemented in lamellar_impl

// #[lamellar_impl::AmLocalDataRT(Debug)]
// struct InitPutAm<T: Dist> {
//     array: AtomicArray<T>, //inner of the indices we need to place data into
//     index: usize,
//     buf: LamellarArrayInput<T>,
// }

// #[lamellar_impl::rt_am_local]
// impl<T: Dist + 'static> LamellarAm for InitPutAm<T> {
//     async fn exec(self) {
//         // println!("initputam {:?} {:?}",self.index,self.buf.len());
//         unsafe {
//             let u8_buf = self.buf.clone().to_base::<u8>();
//             let mut reqs = vec![];
//             match self.array.array.inner.distribution {
//                 Distribution::Block => {
//                     let mut cur_index = 0;
//                     for pe in self
//                         .array
//                         .array
//                         .pes_for_range(self.index, self.buf.len())
//                         .into_iter()
//                     {
//                         // println!("pe {:?}",pe);
//                         if let Some(len) = self.array.array.num_elements_on_pe_for_range(
//                             pe,
//                             self.index,
//                             self.buf.len(),
//                         ) {
//                             // println!("pe {:?} len {:?} bytes: {:?}",pe,len,u8_buf);
//                             let u8_buf_len = len * std::mem::size_of::<T>();
//                             //get the proper remote am from the ops map
//                             if let Some(remote_am_gen) = PUT_OPS.get(&TypeId::of::<T>()) {
//                                 let am: LamellarArcAm = remote_am_gen(
//                                     self.array.clone().into(),
//                                     self.index,
//                                     self.buf.len(),
//                                     u8_buf.as_slice().unwrap()[cur_index..(cur_index + u8_buf_len)]
//                                         .to_vec(),
//                                 );
//                                 reqs.push(
//                                     self.array.exec_arc_am_pe::<Vec<u8>>(pe, am).into_future(),
//                                 );
//                             } else {
//                                 let name = std::any::type_name::<T>().split("::").last().unwrap();
//                                 panic!("the type {:?} has not been registered for atomic rdma operations, this typically means you need to derive \"AtomicRdma\" for the type,
//                                 you can use the lamellar::AmData attribute proc macro to automatically derive it, e.g.
//                                 #[lamellar::AMData(AtomicRdma, any other traits you derive)]
//                                 struct {:?}{{
//                                     ....
//                                 }}",name,name);
//                             }
//                             cur_index += u8_buf_len;
//                         } else {
//                             panic!("this should not be possible");
//                         }
//                     }
//                 }
//                 Distribution::Cyclic => {
//                     let num_pes = ArrayExecAm::team(&self.array).num_pes();
//                     let mut pe_u8_vecs: HashMap<usize, Vec<u8>> = HashMap::new();
//                     let mut pe_t_slices: HashMap<usize, &mut [T]> = HashMap::new();
//                     let buf_slice = self.buf.as_slice().unwrap();
//                     for pe in self
//                         .array
//                         .array
//                         .pes_for_range(self.index, self.buf.len())
//                         .into_iter()
//                     {
//                         if let Some(len) = self.array.array.num_elements_on_pe_for_range(
//                             pe,
//                             self.index,
//                             self.buf.len(),
//                         ) {
//                             let mut u8_vec = vec![0u8; len * std::mem::size_of::<T>()];
//                             let t_slice =
//                                 std::slice::from_raw_parts_mut(u8_vec.as_mut_ptr() as *mut T, len);
//                             pe_u8_vecs.insert(pe, u8_vec);
//                             pe_t_slices.insert(pe, t_slice);
//                         }
//                     }
//                     for (buf_index, index) in
//                         (self.index..(self.index + self.buf.len())).enumerate()
//                     {
//                         let pe = self.array.array.pe_for_dist_index(index).unwrap() % num_pes;
//                         pe_t_slices.get_mut(&pe).unwrap()[buf_index / num_pes] =
//                             buf_slice[buf_index];
//                     }
//                     for (pe, vec) in pe_u8_vecs.drain() {
//                         if let Some(remote_am_gen) = PUT_OPS.get(&TypeId::of::<T>()) {
//                             let am: LamellarArcAm = remote_am_gen(
//                                 self.array.clone().into(),
//                                 self.index,
//                                 self.buf.len(),
//                                 vec,
//                             );
//                             reqs.push(self.array.exec_arc_am_pe::<Vec<u8>>(pe, am).into_future());
//                         } else {
//                             let name = std::any::type_name::<T>().split("::").last().unwrap();
//                             panic!("the type {:?} has not been registered for atomic rdma operations, this typically means you need to derive \"AtomicRdma\" for the type,
//                             you can use the lamellar::AmData attribute proc macro to automatically derive it, e.g.
//                             #[lamellar::AMData(AtomicRdma, any other traits you derive)]
//                             struct {:?}{{
//                                 ....
//                             }}",name,name);
//                         }
//                     }
//                 }
//             }
//             for req in reqs.drain(..) {
//                 req.await;
//             }
//         }
//         // println!("done initput am");
//     }
// }
