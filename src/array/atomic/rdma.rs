use crate::array::atomic::*;
use crate::array::LamellarWrite;
use crate::array::*;
use crate::array::private::ArrayExecAm;
use crate::lamellar_request::LamellarRequest;
use crate::memregion::{
    AsBase, Dist, MemoryRegionRDMA, RTMemoryRegionRDMA, RegisteredMemoryRegion, SubRegion,
};

type GetFn = fn(AtomicArray<u8>, usize, usize) -> LamellarArcAm;
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

type PutFn = fn(AtomicArray<u8>, usize, usize, Vec<u8>) -> LamellarArcAm;
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
    pub  fn iget<U: MyInto<LamellarArrayInput<T>> + LamellarWrite >(&self, index: usize, buf: U) {
        self.exec_am_local(InitGetAm{
            array: self.clone(),
            index: index,
            buf: buf.my_into(&self.array.team()),
        }).get();
    }
    pub  fn get<U: MyInto<LamellarArrayInput<T>> + LamellarWrite >(&self, index: usize, buf: U) {
        self.exec_am_local(InitGetAm{
            array: self.clone(),
            index: index,
            buf: buf.my_into(&self.array.team()),
        });
    }
    pub  fn put<U: MyInto<LamellarArrayInput<T>> + LamellarWrite >(&self, index: usize, buf: U) {
        self.exec_am_local(InitPutAm{
            array: self.clone(),
            index: index,
            buf: buf.my_into(&self.array.team()),
        });
    }
}


impl<T: Dist + 'static> LamellarArrayRead<T> for AtomicArray<T> {
    // unsafe fn get_unchecked<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(
    //     &self,
    //     index: usize,
    //     buf: U,
    // ) {
    //     self.array.get_unchecked(index, buf)
    // }
    fn iget<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(&self, index: usize, buf: U) {
        self.iget(index,buf);
    }
    fn get<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(&self, index: usize, buf: U) {
        self.get(index,buf);
    }
    fn iat(&self, index: usize) -> T {
        self.array.iat(index)
    }
}

impl<T: Dist> LamellarArrayWrite<T> for AtomicArray<T> {
    // fn put_unchecked<U: MyInto<LamellarArrayInput<T>> + LamellarRead>(&self, index: usize, buf: U) {
    //     self.array.put_unchecked(index, buf)
    // }
}


#[lamellar_impl::AmLocalDataRT]
struct InitGetAm<T: Dist > {
    array: AtomicArray<T>, //subarray of the indices we need to place data into
    index: usize,
    buf: LamellarArrayInput<T>
}

#[lamellar_impl::rt_am_local]
impl<T: Dist + 'static > LamellarAm for InitGetAm<T> {
    fn exec(self) {
        let mut reqs = vec![];
        for pe in self.array.array.pes_for_range(self.index,self.index+self.buf.len()).into_iter(){
            //get the proper remote am from the ops map
            if let Some(remote_am_gen) = GET_OPS.get(&TypeId::of::<T>()) {
                unsafe{
                    let am: LamellarArcAm = remote_am_gen(self.array.as_bytes(), self.index,self.index+self.buf.len());
                    reqs.push(self.array.exec_arc_am_pe::<Vec<u8>>(pe,am).into_future());
                }
            }else{
                let name = std::any::type_name::<T>().split("::").last().unwrap();
                panic!("the type {:?} has not been registered for atomic rdma operations, this typically means you need to derive \"AtomicRdma\" for the type,
                you can use the lamellar::AmData attribute proc macro to automatically derive it, e.g.
                #[lamellar::AMData(AtomicRdma, any other traits you derive)]
                struct {:?}{{
                    ....
                }}",name,name);
            }
        }
        unsafe {
            match self.array.array.distribution{
                Distribution::Block => {
                    let u8_buf = self.buf.clone().to_base::<u8>();
                    let mut cur_index = 0;
                    for req in reqs.drain(..){
                        let data = req.await.unwrap();
                        u8_buf.put_slice(lamellar::current_pe,cur_index,&data);
                        cur_index += data.len();
                    }
                }
                Distribution::Cyclic => {
                    let buf_slice = self.buf.as_mut_slice().unwrap();
                    let num_pes = reqs.len(); 
                    for (start_index,req) in reqs.drain(..).enumerate(){
                        let data = req.await.unwrap();
                        let data_t_ptr = data.as_ptr() as *const T;
                        let data_t_len = if data.len() % std::mem::size_of::<T>() == 0 {
                            data.len()/std::mem::size_of::<T>()
                        }
                        else{
                            panic!("memory align error");
                        };
                        let data_t_slice = std::slice::from_raw_parts(data_t_ptr,data_t_len);
                        for (i,val) in data_t_slice.iter().enumerate(){
                            buf_slice[start_index+i*num_pes] = *val;
                        }
                    }
                }   
            }
        }
    }
}

//the remote get am is implemented in lamellar_impl

#[lamellar_impl::AmLocalDataRT]
struct InitPutAm<T: Dist > {
    array: AtomicArray<T>, //subarray of the indices we need to place data into
    index: usize,
    buf: LamellarArrayInput<T>
}

#[lamellar_impl::rt_am_local]
impl<T: Dist + 'static > LamellarAm for InitPutAm<T> {
    fn exec(self) {
        let u8_index = self.index * std::mem::size_of::<T>();
        let u8_len = self.buf.len() * std::mem::size_of::<T>(); 

        unsafe{
            let u8_buf = self.buf.clone().to_base::<u8>();
            let mut reqs = vec![];
            match self.array.array.distribution{
                Distribution::Block => {
                    let mut cur_index = 0;
                    for pe in self.array.array.pes_for_range(self.index,self.index+self.buf.len()).into_iter(){
                        if let Some(len) = self.array.array.elements_on_pe_for_range(pe,self.index,self.index+self.buf.len()) {
                            let u8_buf_len = len * std::mem::size_of::<T>();
                             //get the proper remote am from the ops map
                            if let Some(remote_am_gen) = PUT_OPS.get(&TypeId::of::<T>()) {
                                unsafe{
                                    let am: LamellarArcAm = remote_am_gen(self.array.as_bytes(), self.index,self.index+self.buf.len(), u8_buf.as_slice().unwrap()[cur_index..(cur_index+u8_buf_len)].to_vec());
                                    reqs.push(self.array.exec_arc_am_pe::<Vec<u8>>(pe,am).into_future());
                                }
                            }else{
                                let name = std::any::type_name::<T>().split("::").last().unwrap();
                                panic!("the type {:?} has not been registered for atomic rdma operations, this typically means you need to derive \"AtomicRdma\" for the type,
                                you can use the lamellar::AmData attribute proc macro to automatically derive it, e.g.
                                #[lamellar::AMData(AtomicRdma, any other traits you derive)]
                                struct {:?}{{
                                    ....
                                }}",name,name);
                            }
                            cur_index += u8_buf_len;
                        }   
                        else{   
                            panic!("this should not be possible");
                        }
                    }
                }
                Distribution::Cyclic => {
                    let num_pes = ArrayExecAm::team(&self.array).num_pes();
                    let mut pe_u8_vecs: HashMap<usize,Vec<u8>> = HashMap::new();
                    let mut pe_t_slices: HashMap<usize,&mut [T]> = HashMap::new();
                    let buf_slice = self.buf.as_slice().unwrap();
                    for pe in self.array.array.pes_for_range(self.index,self.index+self.buf.len()).into_iter(){
                        if let Some(len) = self.array.array.elements_on_pe_for_range(pe,self.index,self.index+self.buf.len()) {
                            let mut u8_vec = Vec::with_capacity(len * std::mem::size_of::<T>());
                            let t_slice = std::slice::from_raw_parts_mut(u8_vec.as_mut_ptr() as *mut T, len);
                            pe_u8_vecs.insert(pe,u8_vec);
                            pe_t_slices.insert(pe,t_slice);
                        }
                    }
                    for (buf_index,global_index) in (self.index..(self.index + self.buf.len())).enumerate(){
                        let pe = global_index%num_pes;
                        pe_t_slices.get_mut(&pe).unwrap()[buf_index/num_pes] = buf_slice[buf_index];
                    }
                    for (pe,vec) in pe_u8_vecs.drain(){
                        if let Some(remote_am_gen) = PUT_OPS.get(&TypeId::of::<T>()) {
                            unsafe{
                                let am: LamellarArcAm = remote_am_gen(self.array.as_bytes(), self.index,self.index+self.buf.len(),vec);
                                reqs.push(self.array.exec_arc_am_pe::<Vec<u8>>(pe,am).into_future());
                            }
                        }else{
                            let name = std::any::type_name::<T>().split("::").last().unwrap();
                            panic!("the type {:?} has not been registered for atomic rdma operations, this typically means you need to derive \"AtomicRdma\" for the type,
                            you can use the lamellar::AmData attribute proc macro to automatically derive it, e.g.
                            #[lamellar::AMData(AtomicRdma, any other traits you derive)]
                            struct {:?}{{
                                ....
                            }}",name,name);
                        }
                    }                    
                }
            }
        }
    }
}
