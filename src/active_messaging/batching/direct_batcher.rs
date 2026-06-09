use crate::{
    active_messaging::{registered_active_message::*, *},
    lamellar_arch::LamellarArchRT,
    lamellae::{Lamellae,LamellaeUtil,SerializeHeader,CommInfo},
    scheduler::Scheduler,
};
use batching::*;

use zerocopy_derive::*;
use zerocopy::*;

use async_trait::async_trait;

use parking_lot::{Mutex, MutexGuard};

use tracing::{debug, trace};

const MAX_BATCH_SIZE: usize = 100_000;

#[repr(C)]
// #[derive( Debug, Copy, Clone,CheckedBitPattern, NoUninit, Zeroable)]
#[derive( Debug, Copy, Clone,IntoBytes, FromBytes, KnownLayout,Immutable,Unaligned)]
pub(crate) struct MyAmHeader {
    pub(crate) req_id: U64<NativeEndian>,
    pub(crate) req_sub_id: U64<NativeEndian>,
    pub(crate) team_addr: U64<NativeEndian>,
    pub(crate) data_len: U64<NativeEndian>,
    pub(crate) am_id: I32<NativeEndian>,
}

#[repr(C)]
// #[derive( Debug, Copy, Clone,Pod, Zeroable)]
#[derive( Debug, Copy, Clone,IntoBytes,FromBytes,KnownLayout,Immutable,Unaligned)]
pub(crate) struct MyDataHeader {
    pub(crate) req_id: U64<NativeEndian>,
    pub(crate) req_sub_id: U64<NativeEndian>,
    pub(crate) size: U64<NativeEndian>,
    pub(crate) darc_list_size: U64<NativeEndian>,
}

#[repr(C)]
// #[derive( Debug, Copy, Clone,Pod, Zeroable)]
#[derive( Debug, Copy, Clone,IntoBytes,FromBytes,KnownLayout,Immutable,Unaligned)]
pub(crate) struct MyUnitHeader {
    pub(crate) req_id: U64<NativeEndian>,
    pub(crate) req_sub_id: U64<NativeEndian>,
}

#[derive(Debug)]
struct DirectBatcherInner{
    pe_batch: Vec<Mutex<Vec<u8>>>,
    team_all_batch: Mutex<HashMap<Arc<LamellarArchRT>, Vec<u8>>>,
    header_bytes: Vec<u8>,
    stall_mark: Arc<AtomicUsize>,
}

#[derive(Debug, Clone)]
pub struct DirectBatcher {
    inner: Arc<DirectBatcherInner>,
    executor: Arc<Executor>,
}

#[lamellar_prof::prof]
impl DirectBatcher {
    pub fn new(num_pes: usize, my_pe: usize, stall_mark: Arc<AtomicUsize>, executor: Arc<Executor>) -> Self {
        let mut batch = Vec::with_capacity(num_pes + 1);
        let header = Some(SerializeHeader{msg: Msg{
            src: my_pe as u16,
            cmd: Cmd::BatchedMsg,
            padding: [0; 1],
        }});
        let header_bytes = crate::serialize(&header,false).unwrap();
        for _ in 0..num_pes {
            batch.push(Mutex::new(header_bytes.clone()));
        }
        let inner = Arc::new(DirectBatcherInner {
            pe_batch: batch,
            team_all_batch: Mutex::new(HashMap::new()),
            header_bytes: header_bytes,
            stall_mark,
        });
        
        Self {
            inner,
            executor,
        }
    }

    pub(crate) fn send_batch_if_needed(&self, lamellae: &Arc<Lamellae>, pe: usize, batch: &mut MutexGuard<Vec<u8>>) {
        if batch.len() > self.inner.header_bytes.len() {
            trace!("Checking if need to send batch for PE {:?}, batch size: {:?}, stall_mark: {:?} available_to_send: {:?}", pe, batch.len(), self.inner.stall_mark.load(Ordering::Relaxed), lamellae.available_to_send(pe));
        }
       if batch.len() > self.inner.header_bytes.len() && batch.len() > MAX_BATCH_SIZE && lamellae.available_to_send(pe) {
            
            let mut new_batch = self.inner.header_bytes.clone();
            std::mem::swap(&mut new_batch, batch);
            trace!("Sending batch if neededto PE {:?} with size {:?}", pe, new_batch.len());
            self.executor.block_on(lamellae.send_vec_to_pe_async(pe, new_batch));
        }
    }
    

    pub(crate) fn init_batcher_task(&self,scheduler: Arc<Scheduler>, lamellae: &Arc<Lamellae>) { 
        let inner = self.inner.clone();
        let executor = self.executor.clone();
        let stall_mark = self.inner.stall_mark.clone();
        let lamellae = lamellae.clone();
        let header_bytes_len = self.inner.header_bytes.len();
        let same_stall_threshold: usize = 1;
        debug!("Initializing batcher task");
        scheduler.clone().submit_long_task(async move {
            let mut  prev_stall_mark = 0;
            let mut previous_batch_size = vec![0; lamellae.comm().num_pes()];
            let mut same_stall_count = 0;
            let mut same_batch_size_count = vec![0; lamellae.comm().num_pes()];
            while scheduler.active(1) {
                let current_stall_mark = stall_mark.load(Ordering::Acquire);
                for (pe, batch_lock) in inner.pe_batch.iter().enumerate().filter(|(pe, _)| *pe != lamellae.comm().my_pe() && lamellae.available_to_send(*pe) ) {
                    let mut batch_lock = batch_lock.lock();
                    // if prev_stall_mark != current_stall_mark || previous_batch_size[pe] != batch_lock.len() {
                    //      trace!("Stall mark  {:?} to {:?}, previous batch size: {:?}, current batch size: {:?}, sending batch for PE {:?} with size {:?}", prev_stall_mark, current_stall_mark, previous_batch_size[pe], batch_lock.len(), pe, batch_lock.len());
                    // }
                    if previous_batch_size[pe] == batch_lock.len() {
                        same_batch_size_count[pe] += 1;
                    } else {
                        same_batch_size_count[pe] = 0;
                    }
                    
                    if  batch_lock.len()>header_bytes_len &&(batch_lock.len() > MAX_BATCH_SIZE  || same_stall_count > same_stall_threshold || same_batch_size_count[pe] > same_stall_threshold) {
                        let mut new_batch = inner.header_bytes.clone();
                        std::mem::swap(&mut new_batch, &mut *batch_lock);
                        debug!("Sending batch to PE {:?} with size {:?} same_stall_count: {:?}", pe, new_batch.len(), same_stall_count);
                        let lamellae_clone = lamellae.clone();
                        scheduler.submit_immediate_task(async move {
                            lamellae_clone.send_vec_to_pe_async(pe, new_batch).await
                        });
                        same_stall_count = 0;
                        same_batch_size_count[pe] = 0;
                    }
                    previous_batch_size[pe] = batch_lock.len();
                }
                // for (arch, batch) in inner.team_all_batch.lock().extract_if(|_arch, batch| lamellae.available_to_send(_arch) && (batch.len() > MAX_BATCH_SIZE || (current_stall_mark == prev_stall_mark && !batch.is_empty()))) {
                //     lamellae.send_vec_to_team_pes_async(arch, batch).await;
                // }
                if prev_stall_mark == current_stall_mark {
                        same_stall_count += 1;
                } else {
                    same_stall_count = 0;
                }
                prev_stall_mark = current_stall_mark;
                async_std::task::yield_now().await;
            }
            debug!("Batcher task exiting!!!!!!");

        });
    }

    // fn get_pe_batch(&self, pe: Option<usize>) -> Vec<u8> {
    //     let mut new_batch = self.inner.header_bytes.clone();
    //     if let Some(pe) = pe {
    //         std::mem::swap(&mut new_batch, &mut *self.inner.batch[pe].lock());
    //     } else {
    //         std::mem::swap(&mut new_batch, &mut *self.inner.batch.last().unwrap().lock());
    //     }
    //     new_batch
    // }
}


 #[lamellar_prof::prof]
 #[async_trait]
impl Batcher for DirectBatcher {
    async fn add_remote_am_to_batch(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        am_id: AmId,
        am_size: usize,
        stall_mark: usize,
    ){
        trace!("Adding remote AM to batch for req_id: {:?}, sub_id: {:?}, dst: {:?}, team: {:?}", req_data.id.id, req_data.id.sub_id, req_data.dst, req_data.team.darc_addr());
        let mut bytes = am.serialize();
        let cmd_bytes = Cmd::Am.as_bytes();
        let am_header = MyAmHeader {
            am_id: I32::new(am_id),
            req_id: U64::new(req_data.id.id as u64),
            req_sub_id: U64::new(req_data.id.sub_id as u64),
            data_len: U64::new(bytes.len() as u64),
            team_addr: U64::new(req_data.team.darc_addr() as u64),
        };

        let am_header_bytes = am_header.as_bytes();

        let add_to_batch = |cmd_bytes: &[u8], am_header_bytes: &[u8], mut bytes: Vec<u8>, batch: &mut Vec<u8>| {
            debug!("Adding remote AM to batch: cmd_bytes len: {:?}, am_header_bytes len: {:?}, bytes len: {:?}", cmd_bytes.len(), am_header_bytes.len(), bytes.len());
            batch.extend_from_slice(cmd_bytes);
            batch.extend_from_slice(am_header_bytes);
            batch.append(&mut bytes);
            
        };

        
        
        // let mut batch_lock =
        if let Some(pe) = req_data.dst{
            let mut darcs = vec![];
            am.ser(1, &mut darcs);
            // self.inner.pe_batch[pe].lock()
            let mut batch = &mut self.inner.pe_batch[pe].lock();
            add_to_batch(cmd_bytes,am_header_bytes,bytes,&mut batch);
            // self.send_batch_if_needed(&req_data.lamellae, pe, batch);
        }
        else{
            let darc_ser_cnt = match req_data.team.team_pe_id() {
                Ok(_) => req_data.team.num_pes() - 1,
                Err(_) => req_data.team.num_pes(),
            };
            let mut darcs = vec![];
            am.ser(darc_ser_cnt, &mut darcs);
            // self.inner.team_all_batch.lock()
            //     .entry(req_data.team.arch.clone())
            //     .or_insert_with(|| self.inner.batch_msg_bytes.clone())
            for pe in req_data.team.arch.team_iter().filter(|pe| pe != &req_data.team.lamellae.comm().my_pe()) {
                let mut batch = &mut self.inner.pe_batch[pe].lock();
                add_to_batch(cmd_bytes,am_header_bytes,bytes.clone(),&mut batch);
                // self.send_batch_if_needed(&req_data.lamellae, pe, batch);
            }
        };
        // batch_lock.extend_from_slice(cmd_bytes);
        // batch_lock.extend_from_slice(am_header_bytes);
        // batch_lock.append(&mut bytes);
    }
    async fn add_return_am_to_batch(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        am_id: AmId,
        am_size: usize,
        stall_mark: usize,
    ){
        trace!("Adding return AM to batch for req_id: {:?}, sub_id: {:?}, dst: {:?}, team: {:?}", req_data.id.id, req_data.id.sub_id, req_data.dst, req_data.team.darc_addr());
        let mut bytes = am.serialize();
        let cmd_bytes = Cmd::ReturnAm.as_bytes();
        let am_header = MyAmHeader {
            am_id: I32::new(am_id),
            req_id: U64::new(req_data.id.id as u64),
            req_sub_id: U64::new(req_data.id.sub_id as u64),
            data_len: U64::new(bytes.len() as u64),
            team_addr: U64::new(req_data.team.darc_addr() as u64),
        };

        let am_header_bytes = am_header.as_bytes();
        let add_to_batch = |cmd_bytes: &[u8], am_header_bytes: &[u8], mut bytes: Vec<u8>, batch: &mut Vec<u8>| {
                debug!("Adding return AM to batch: cmd_bytes len: {:?}, am_header_bytes len: {:?}, bytes len: {:?}", cmd_bytes.len(), am_header_bytes.len(), bytes.len());
            batch.extend_from_slice(cmd_bytes);
            batch.extend_from_slice(am_header_bytes);
            batch.append(&mut bytes);
        };
        
        // let mut batch_lock =
        if let Some(pe) = req_data.dst{
            let mut darcs = vec![];
            am.ser(1, &mut darcs);
            //self.inner.pe_batch[pe].lock()
            let mut batch = &mut self.inner.pe_batch[pe].lock();
            add_to_batch(cmd_bytes,am_header_bytes,bytes,&mut batch);
            // self.send_batch_if_needed(&req_data.lamellae, pe, batch);
        }
        else{
            let darc_ser_cnt = match req_data.team.team_pe_id() {
                Ok(_) => req_data.team.num_pes() - 1,
                Err(_) => req_data.team.num_pes(),
            };
            let mut darcs = vec![];
            am.ser(darc_ser_cnt, &mut darcs);
            // self.inner.team_all_batch.lock()
            //     .entry(req_data.team.arch.clone())
            //     .or_insert_with(|| self.inner.batch_msg_bytes.clone())
            for pe in req_data.team.arch.team_iter().filter(|pe| pe != &req_data.team.lamellae.comm().my_pe()) {
                let mut batch = &mut self.inner.pe_batch[pe].lock();
                add_to_batch(cmd_bytes,am_header_bytes,bytes.clone(),&mut batch);
                // self.send_batch_if_needed(&req_data.lamellae, pe, batch);
            }
        };
        // batch_lock.extend_from_slice(cmd_bytes);
        // batch_lock.extend_from_slice(am_header_bytes);
        // batch_lock.append(&mut bytes);
    }
    async fn add_data_am_to_batch(
        &self,
        req_data: ReqMetaData,
        data: LamellarResultArc,
        data_size: usize,
        stall_mark: usize,
    ){
        trace!("Adding data AM to batch for req_id: {:?}, sub_id: {:?}, dst: {:?}, team: {:?}", req_data.id.id, req_data.id.sub_id, req_data.dst, req_data.team.darc_addr());
        let mut darcs = Vec::new();
        data.ser(1, &mut darcs); //1 because we are only sending back to the original PE
        let mut serialized_darcs = crate::serialize(&darcs,false).unwrap();
        let mut bytes = data.serialize();
        let cmd_bytes = Cmd::Data.as_bytes();

        let data_header = MyDataHeader {
            req_id: U64::new(req_data.id.id as u64),
            req_sub_id: U64::new(req_data.id.sub_id as u64),
            size: U64::new(bytes.len() as u64),
            darc_list_size: U64::new(serialized_darcs.len() as u64),
        };

        let data_header_bytes = data_header.as_bytes();
        
        let add_to_batch = |cmd_bytes: &[u8], data_header_bytes: &[u8], mut serialized_darcs: Vec<u8>, mut bytes: Vec<u8>, batch: &mut Vec<u8>| {
                debug!("Adding data am to batch: cmd_bytes len: {:?}, data_header_bytes len: {:?}, serialized_darcs len: {:?}, bytes len: {:?}", cmd_bytes.len(), data_header_bytes.len(), serialized_darcs.len(), bytes.len());
                batch.extend_from_slice(cmd_bytes);
                batch.extend_from_slice(data_header_bytes);
                batch.append(&mut serialized_darcs);
                batch.append(&mut bytes);
        };

        // let mut batch_lock = 
        if let Some(pe) = req_data.dst{
            // self.inner.pe_batch[pe].lock()
            let mut batch = &mut self.inner.pe_batch[pe].lock();
            add_to_batch(cmd_bytes,data_header_bytes,serialized_darcs,bytes,&mut batch);
            // self.send_batch_if_needed(&req_data.lamellae, pe, batch);
        }
        else{
            // self.inner.team_all_batch.lock()
            //     .entry(req_data.team.arch.clone())
            //     .or_insert_with(|| self.inner.batch_msg_bytes.clone())
            for pe in req_data.team.arch.team_iter().filter(|pe| pe != &req_data.team.lamellae.comm().my_pe()){
                let mut batch = &mut self.inner.pe_batch[pe].lock();
                add_to_batch(cmd_bytes,data_header_bytes,serialized_darcs.clone(),bytes.clone(),&mut batch);
                // self.send_batch_if_needed(&req_data.lamellae, pe, batch);
            }
        };
        // batch_lock.extend_from_slice(cmd_bytes);
        // batch_lock.extend_from_slice(data_header_bytes);
        // batch_lock.append(&mut serialized_darcs);
        // batch_lock.append(&mut bytes);
    }
    async fn add_unit_am_to_batch(&self, req_data: ReqMetaData, stall_mark: usize){
            trace!("Adding unit AM to batch for req_id: {:?}, sub_id: {:?}, dst: {:?}, team: {:?}", req_data.id.id, req_data.id.sub_id, req_data.dst, req_data.team.darc_addr());
        let unit_header = MyUnitHeader {
            req_id: U64::new(req_data.id.id as u64),
            req_sub_id: U64::new(req_data.id.sub_id as u64),
        };
        let cmd_bytes = Cmd::Unit.as_bytes();
        
        let unit_header_bytes = unit_header.as_bytes();

        let add_to_batch = |cmd_bytes: &[u8], unit_header_bytes: &[u8], batch: &mut Vec<u8>| {
                debug!("Adding unit am to batch: cmd_bytes len: {:?}, unit_header_bytes len: {:?}, batch_len_before: {:?}", cmd_bytes.len(), unit_header_bytes.len(), batch.len());
            batch.extend_from_slice(cmd_bytes);
            batch.extend_from_slice(unit_header_bytes);
        };

        // let mut batch_lock = 
        if let Some(pe) = req_data.dst{
            // self.inner.pe_batch[pe].lock()
            let mut batch = &mut self.inner.pe_batch[pe].lock();
            add_to_batch(cmd_bytes,unit_header_bytes,&mut batch);
            // self.send_batch_if_needed(&req_data.lamellae, pe, batch);
        }
        else{
            // self.inner.team_all_batch.lock()
            //     .entry(req_data.team.arch.clone())
            //     .or_insert_with(|| self.inner.batch_msg_bytes.clone())
            for pe in req_data.team.arch.team_iter().filter(|pe| pe != &req_data.team.lamellae.comm().my_pe()){
                let mut batch = &mut self.inner.pe_batch[pe].lock();
                add_to_batch(cmd_bytes,unit_header_bytes,&mut batch);
                // self.send_batch_if_needed(&req_data.lamellae, pe, batch);
            }
        };
        // batch_lock.extend_from_slice(cmd_bytes);
        // batch_lock.extend_from_slice(unit_header_bytes);
    }

    async fn exec_batched_msg(
        &self,
        msg: Msg,
        mut ser_data: SerializedData,
        lamellae: Arc<Lamellae>,
        ame: &RegisteredActiveMessages,
    ){
        debug!("Executing batched message from src: {:?} with data size: {:?}", msg.src, ser_data.data_len());
        let mut offset = 0;
        let data_bytes = ser_data.data_as_bytes();
        let src = msg.src as usize;
        while offset < data_bytes.len() {
            trace!("Processing batch at offset: {:?}, remaining data size: {:?}", offset, data_bytes.len() - offset);
            let cmd_bytes = &data_bytes[offset..offset + std::mem::size_of::<Cmd>()];
            let cmd = Cmd::try_ref_from_bytes(cmd_bytes).expect("Failed to parse Cmd from bytes");
            offset += std::mem::size_of::<Cmd>();
            offset += match cmd {
                Cmd::Am => {
                   self.exec_am(src, &data_bytes[offset..], &lamellae, ame)
                },
                Cmd::ReturnAm => {
                    self.exec_return_am(src, &data_bytes[offset..], &lamellae, ame).await
                },
                Cmd::Data => {
                    self.exec_data_am(src, &data_bytes[offset..], ame)
                },
                Cmd::Unit => {
                   self.exec_unit_am(src, &data_bytes[offset..], ame)
                },
                _ => unreachable!(),
            };
        }
    }
}

 #[lamellar_prof::prof]
impl DirectBatcher {
    fn exec_am(&self,src: usize,  data_bytes: &[u8], lamellae: &Arc<Lamellae>, ame: &RegisteredActiveMessages) -> usize{
        debug!("Executing AM from src: {:?} with data size: {:?}", src, data_bytes.len());
        let mut offset = 0;
        let header_bytes = &data_bytes[offset..offset + std::mem::size_of::<MyAmHeader>()];
        let am_header = MyAmHeader::ref_from_bytes(header_bytes).expect("Failed to parse MyAmHeader from bytes");
        offset += std::mem::size_of::<MyAmHeader>();
        let data_len = am_header.data_len.get() as usize;
        let am_data_bytes = &data_bytes[offset..offset + data_len];
        offset += data_len;
        let (team, world) = ame.get_team_and_world(src, am_header.team_addr.get() as usize, lamellae);
        let am = AMS_EXECS.get(&am_header.am_id.get()).unwrap()(am_data_bytes, team.team.team_pe);
        let req_data = ReqMetaData {
            src: team.team.world_pe,
            dst: Some(src),
            id: ReqId {
                id: am_header.req_id.get() as usize,
                sub_id: am_header.req_sub_id.get() as usize,
            },
            lamellae: lamellae.clone(),
            world: world.team.clone(),
            team: team.team.clone(),
        };
        let ame = ame.clone();
        world.team.world_counters.inc_outstanding(1);
        team.team.team_counters.inc_outstanding(1);
        self.executor.submit_task(async move {
            trace!("simple batcher exec_am submit task");
            let am = match am
                .exec(
                    team.team.world_pe,
                    team.team.num_world_pes,
                    false,
                    world.clone(),
                    team.clone(),
                )
                .await
            {
                LamellarReturn::Unit => Am::Unit(req_data),
                LamellarReturn::RemoteData(data) => Am::Data(req_data, data),
                LamellarReturn::RemoteAm(am) => Am::Return(req_data, am),
                LamellarReturn::LocalData(_) | LamellarReturn::LocalAm(_) => {
                    panic!("Should not be returning local data or AM from remote  am");
                }
            };
            world.team.world_counters.dec_outstanding(1);
            team.team.team_counters.dec_outstanding(1);
            ame.process_msg(am, 0, false).await;
        });
        offset
    }

    async fn exec_return_am(&self, src: usize,  data_bytes: &[u8], lamellae: &Arc<Lamellae>, ame: &RegisteredActiveMessages) -> usize{
        debug!("Executing return AM from src: {:?} with data size: {:?}", src, data_bytes.len());
        let mut offset = 0;
        let header_bytes = &data_bytes[offset..offset + std::mem::size_of::<MyAmHeader>()];
        let am_header = MyAmHeader::ref_from_bytes(header_bytes).expect("Failed to parse MyAmHeader from bytes");
        offset += std::mem::size_of::<MyAmHeader>();
        let data_len = am_header.data_len.get() as usize;
        let am_data_bytes = &data_bytes[offset..offset + data_len];
        offset += data_len;
        let (team, world) = ame.get_team_and_world(src, am_header.team_addr.get() as usize, lamellae);
        let am = AMS_EXECS.get(&am_header.am_id.get()).unwrap()(am_data_bytes, team.team.team_pe);
        let req_data = ReqMetaData {
            src: src,
            dst: Some(team.team.world_pe),
            id: ReqId {
                id: am_header.req_id.get() as usize,
                sub_id: am_header.req_sub_id.get() as usize,
            },
            lamellae: lamellae.clone(),
            world: world.team.clone(),
            team: team.team.clone(),
        };
        ame.clone().exec_local_am(req_data, am.as_local(), world,team ).await;
        offset
    }

    fn exec_data_am(&self, src: usize, data_bytes: &[u8],  ame: &RegisteredActiveMessages)-> usize{
        debug!("Executing data AM from src: {:?} with data size: {:?}", src, data_bytes.len());
        let mut offset = 0;
        let data_header_bytes = &data_bytes[offset..offset + std::mem::size_of::<MyDataHeader>()];
        let data_header= MyDataHeader::ref_from_bytes(data_header_bytes).expect("Failed to parse MyDataHeader from bytes");
        offset += std::mem::size_of::<MyDataHeader>();

        let darc_list_size = data_header.darc_list_size.get() as usize;
        let data_darc_bytes = &data_bytes[offset..offset + darc_list_size];
        let darc_list: Vec<RemotePtr> = crate::deserialize(data_darc_bytes,false).unwrap();
        offset += darc_list_size;

        let data_size = data_header.size.get() as usize;
        let data_bytes = &data_bytes[offset..offset + data_size];
        offset += data_size;

        let req_id = ReqId {
            id: data_header.req_id.get() as usize,
            sub_id: data_header.req_sub_id.get() as usize,
        };
        ame.send_data_to_user_handle(req_id, src, InternalResult::NewRemote(data_bytes.to_vec(), darc_list));
        offset

    }

    fn exec_unit_am(&self, src: usize, data_bytes: &[u8], ame: &RegisteredActiveMessages)-> usize{
        debug!("Executing unit AM from src: {:?} with data size: {:?}", src, data_bytes.len());
        let mut offset = 0;
        let unit_header_bytes = &data_bytes[offset..offset + std::mem::size_of::<MyUnitHeader>()];
        let unit_header = MyUnitHeader::ref_from_bytes(unit_header_bytes).expect("Failed to parse MyUnitHeader from bytes");
        offset += std::mem::size_of::<MyUnitHeader>();
        let req_id = ReqId {
            id: unit_header.req_id.get() as usize,
            sub_id: unit_header.req_sub_id.get() as usize,
        };
        ame.send_data_to_user_handle(req_id, src, InternalResult::Unit);
        offset
    }
}