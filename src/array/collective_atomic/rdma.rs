use crate::array::collective_atomic::*;
use crate::array::LamellarWrite;
use crate::array::*;
use crate::array::private::ArrayExecAm;
use crate::lamellar_request::LamellarRequest;
use crate::memregion::{
    AsBase, Dist, MemoryRegionRDMA, RTMemoryRegionRDMA, RegisteredMemoryRegion, SubRegion,
};


impl<T: Dist + 'static> CollectiveAtomicArray<T> {
    pub fn iget<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(&self, index: usize, buf: U) {
        
        self.exec_am_local(InitGetAm{
            array: self.clone(),
            index: index,
            buf: buf.my_into(&self.array.team()),
        }).get();
    }

    pub fn get<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(&self, index: usize, buf: U) {
        
        self.exec_am_local(InitGetAm{
            array: self.clone(),
            index: index,
            buf: buf.my_into(&self.array.team()),
        });
    }

    pub fn put<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(&self, index: usize, buf: U) {
        self.exec_am_local(InitPutAm{
            array: self.clone(),
            index: index,
            buf: buf.my_into(&self.array.team()),
        });
    }
}

impl<T: Dist + 'static> LamellarArrayRead<T> for CollectiveAtomicArray<T> {
    fn iget<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(&self, index: usize, buf: U) {
        self.iget(index, buf)
    }
    fn get<U: MyInto<LamellarArrayInput<T>> + LamellarWrite>(&self, index: usize, buf: U) {
        self.array.get(index, buf)
    }
    fn iat(&self, index: usize) -> T {
        self.array.iat(index)
    }
}

impl<T: Dist> LamellarArrayWrite<T> for CollectiveAtomicArray<T> {
    // fn put_unchecked<U: MyInto<LamellarArrayInput<T>> + LamellarRead>(&self, index: usize, buf: U) {
    //     self.array.put_unchecked(index, buf)
    // }
}


#[lamellar_impl::AmLocalDataRT]
struct InitGetAm<T: Dist > {
    array: CollectiveAtomicArray<T>, //subarray of the indices we need to place data into
    index: usize,
    buf: LamellarArrayInput<T>
}

#[lamellar_impl::rt_am_local]
impl<T: Dist + 'static > LamellarAm for InitGetAm<T> {
    fn exec(self) {
        // let buf = self.buf.into();
        let u8_index = self.index * std::mem::size_of::<T>();
        let u8_len = self.buf.len() * std::mem::size_of::<T>(); 
        // println!("in InitGetAm {:?} {:?}",u8_index,u8_index + u8_len);
        let mut reqs = vec![];
        for pe in self.array.array.pes_for_range(self.index,self.index+self.buf.len()).into_iter(){
            let remote_am = RemoteGetAm{
                array: unsafe {self.array.as_bytes()},
                start_index: u8_index,
                end_index: u8_index + u8_len,
            };
            reqs.push(self.array.exec_am_pe(pe,remote_am).into_future());
        }
        unsafe {
            match self.array.array.distribution{
                Distribution::Block => {
                    let u8_buf = self.buf.clone().to_base::<u8>();
                    let mut cur_index = 0;
                    for req in reqs.drain(..){
                        let data = req.await.unwrap();
                        // println!("data recv {:?}",data.len());
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

#[lamellar_impl::AmDataRT]
struct RemoteGetAm {
    array: CollectiveAtomicArray<u8>, //subarray of the indices we need to place data into
    start_index: usize,
    end_index: usize,

}

#[lamellar_impl::rt_am]
impl LamellarAm for RemoteGetAm { //we cant directly do a put from the array in to the data buf
                                  //because we need to guarantee the put operation is atomic (maybe iput would work?)
    fn exec(self) -> Vec<u8> {
        // println!("in remotegetam {:?} {:?}",self.start_index,self.end_index);
        let _lock = self.array.lock.read();
        unsafe {
            match self.array.array.local_elements_for_range(self.start_index,self.end_index){
                Some((elems,_)) => elems.to_vec(),
                None => vec![],
            }
        }
    }
}

#[lamellar_impl::AmLocalDataRT]
struct InitPutAm<T: Dist > {
    array: CollectiveAtomicArray<T>, //subarray of the indices we need to place data into
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
                            let remote_am = RemotePutAm {
                                array: self.array.as_bytes(), //subarray of the indices we need to place data into
                                start_index: u8_index,
                                end_index: u8_index + u8_len,
                                data: u8_buf.as_slice().unwrap()[cur_index..(cur_index+u8_buf_len)].to_vec(),
                            };
                            reqs.push(self.array.exec_am_pe(pe,remote_am).into_future());
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
                        let remote_am = RemotePutAm {
                            array: self.array.as_bytes(), //subarray of the indices we need to place data into
                            start_index: u8_index,
                            end_index: u8_index + u8_len,
                            data: vec,
                        };
                        reqs.push(self.array.exec_am_pe(pe,remote_am).into_future());
                    }                    
                
                }
            }
        }
    }
}

#[lamellar_impl::AmDataRT]
struct RemotePutAm {
    array: CollectiveAtomicArray<u8>, //subarray of the indices we need to place data into
    start_index: usize,
    end_index: usize,
    data: Vec<u8>,
}

#[lamellar_impl::rt_am]
impl LamellarAm for RemotePutAm { 
    fn exec(self) {
        // println!("in remotegetam {:?} {:?}",self.start_index,self.end_index);
        let _lock = self.array.lock.write();
        unsafe {
            match self.array.array.local_elements_for_range(self.start_index,self.end_index){
                Some((elems,_)) => std::ptr::copy_nonoverlapping(self.data.as_ptr(),elems.as_mut_ptr(),elems.len()),
                None => {},
            }
        }
    }
}
