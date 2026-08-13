use crate::{
    active_messaging::{registered_active_message::*, *},
    lamellae::{
        comm::{error::AllocError, CommInfo},
        CommSlice, Des, Lamellae, LamellaeUtil, Ser, SerializeHeader,
    },
    utils::stats,
};
use batching::*;

use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::HashMap;
use tracing::debug;

const MAX_BATCH_SIZE: usize = 1_000_000;

lazy_static! {
    pub(crate) static ref IO_TASK_SPAWN: Vec<AtomicUsize> = {
        let mut v = Vec::new();
        for _ in 0..4 {
            v.push(AtomicUsize::new(0));
        }
        v
    };
    pub(crate) static ref IO_TASK_START: Vec<AtomicUsize> = {
        let mut v = Vec::new();
        for _ in 0..4 {
            v.push(AtomicUsize::new(0));
        }
        v
    };
    pub(crate) static ref IO_TASK_FINISH: Vec<AtomicUsize> = {
        let mut v = Vec::new();
        for _ in 0..4 {
            v.push(AtomicUsize::new(0));
        }
        v
    };
    pub(crate) static ref IO_TASK_TOO_BIG: Vec<AtomicUsize> = {
        let mut v = Vec::new();
        for _ in 0..4 {
            v.push(AtomicUsize::new(0));
        }
        v
    };
    pub(crate) static ref IO_TASK_TOO_BIG_FINISH: Vec<AtomicUsize> = {
        let mut v = Vec::new();
        for _ in 0..4 {
            v.push(AtomicUsize::new(0));
        }
        v
    };
}

pub(crate) fn io_task_stats() -> String {
    let mut stats = String::new();
    stats!(for i in 0..4 {
        stats.push_str(&format!("IO Task Type {}: Spawned {}, Started {}, Finished {}, Too Big {}, Too Big Finished {}\n", i, IO_TASK_SPAWN[i].load(std::sync::atomic::Ordering::Relaxed), IO_TASK_START[i].load(std::sync::atomic::Ordering::Relaxed), IO_TASK_FINISH[i].load(std::sync::atomic::Ordering::Relaxed), IO_TASK_TOO_BIG[i].load(std::sync::atomic::Ordering::Relaxed), IO_TASK_TOO_BIG_FINISH[i].load(std::sync::atomic::Ordering::Relaxed)));
    });
    stats
}

#[derive(Clone, Debug)]
struct SimpleBatcherInner {
    batch: Arc<Mutex<Vec<(ReqMetaData, LamellarData, usize)>>>, //reqid,data,data size,team addr
    size: Arc<AtomicUsize>,
    batch_id: Arc<AtomicUsize>,
    pe: Option<usize>,
}

impl SimpleBatcherInner {
    //#[tracing::instrument(skip_all, level = "debug")]
    fn new(pe: Option<usize>) -> SimpleBatcherInner {
        SimpleBatcherInner {
            batch: Arc::new(Mutex::new(Vec::new())),
            size: Arc::new(AtomicUsize::new(0)),
            batch_id: Arc::new(AtomicUsize::new(0)),
            pe,
        }
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    fn add(
        &self,
        req_data: ReqMetaData,
        data: LamellarData,
        payload_size: usize,
        header_size: usize,
    ) -> usize {
        // println!("adding to batch");
        //return true if this is the first am in the batch
        let mut batch = self.batch.lock();
        let size = *CMD_LEN + payload_size + header_size;
        batch.push((req_data, data, size));
        // batch.len() == 1
        self.size.fetch_add(size, Ordering::SeqCst)
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    fn swap(&self) -> (Vec<(ReqMetaData, LamellarData, usize)>, usize, usize) {
        let mut batch = self.batch.lock();
        let size = self.size.load(Ordering::SeqCst);
        self.size.store(0, Ordering::SeqCst);
        let batch_id = self.batch_id.fetch_add(1, Ordering::SeqCst);
        // println!("batch_id {_batch_id} swapped");
        let mut new_vec = Vec::new();
        std::mem::swap(&mut *batch, &mut new_vec);
        (new_vec, size, batch_id)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SimpleBatcher {
    batched_ams: Arc<Vec<SimpleBatcherInner>>,
    stall_mark: Arc<AtomicUsize>,
    executor: Arc<Executor>,
}

#[async_trait]
impl Batcher for SimpleBatcher {
    // //#[tracing::instrument(skip_all)]
    async fn add_remote_am_to_batch(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        am_id: AmId,
        am_size: usize,
        mut stall_mark: usize,
    ) {
        // println!("add_remote_am_to_batch");
        //let dst =req_data.dst;
        let batch = match req_data.dst {
            Some(dst) => {
                stats!(
                    BATCHER_AM_PE_SEND_CNTS.0[&StatType::Orig][&dst][&StatCmd::Am]
                        .fetch_add(1, Ordering::Relaxed)
                );
                stats!(
                    BATCHER_AM_PE_SEND_CNTS.0[&StatType::Orig][&dst][&StatCmd::Multi]
                        .fetch_add(1, Ordering::Relaxed)
                );
                self.batched_ams[dst].clone()
            }
            None => {
                stats!(BATCHER_AM_PE_SEND_CNTS.0[&StatType::Orig]
                    .iter()
                    .for_each(|(pe, c)| {
                        if pe < &req_data.team.lamellae.comm().num_pes()
                            && pe != &req_data.team.lamellae.comm().my_pe()
                        {
                            c[&StatCmd::Am].fetch_add(1, Ordering::Relaxed);
                            c[&StatCmd::Multi].fetch_add(1, Ordering::Relaxed);
                        }
                    }));
                self.batched_ams.last().unwrap().clone()
            }
        };
        if stall_mark == 0 {
            self.stall_mark.fetch_add(1, Ordering::Relaxed);
        }
        let size = batch.add(
            req_data,
            LamellarData::Am(am, am_id, am_size),
            am_size,
            *AM_HEADER_LEN.get().expect("am header size not calculated"),
        );
        let batch_id = batch.batch_id.load(Ordering::SeqCst);
        if size == 0 {
            //first data in batch, schedule a transfer task
            let cur_stall_mark = self.stall_mark.clone();
            stats!(IO_TASK_SPAWN[0].fetch_add(1, Ordering::Relaxed));
            self.executor.submit_io_task(async move {
                stats!(IO_TASK_START[0].fetch_add(1, Ordering::Relaxed));
                let mut timer = std::time::Instant::now();
                while stall_mark != cur_stall_mark.load(Ordering::Acquire)
                    && batch.size.load(Ordering::SeqCst) < MAX_BATCH_SIZE
                    && batch_id == batch.batch_id.load(Ordering::SeqCst)
                {
                    if timer.elapsed().as_secs_f32() > 10.0 {
                        debug!(
                            "[{:?}] remote_am waiting to send batch_id {} stall_mark {} cur_stall_mark {} size {}",
                            std::thread::current().id(),
                            batch_id,
                            stall_mark,
                            cur_stall_mark.load(Ordering::Relaxed),
                            batch.size.load(Ordering::SeqCst)
                        );
                        timer = std::time::Instant::now();
                    }
                    stall_mark = cur_stall_mark.load(Ordering::Relaxed);
                    async_std::task::yield_now().await;
                }
                if batch_id == batch.batch_id.load(Ordering::SeqCst) {
                    //this batch is still valid
                    debug!(
                        "[{:?}] remote_am spawning tx task {} of size {:?}  to pe {:?} ",
                        std::thread::current().id(),
                        batch_id,
                        size,
                        batch.pe,
                    );
                    SimpleBatcher::create_tx_task(batch).await;
                } else {
                    debug!("remote am Someone else is transmitting the batch {batch_id} already");
                }
                // in_tx_task.store(false, Ordering::SeqCst);
                stats!(IO_TASK_FINISH[0].fetch_add(1, Ordering::Relaxed));
            });
        } else if size >= MAX_BATCH_SIZE {
            stats!(IO_TASK_TOO_BIG[0].fetch_add(1, Ordering::Relaxed));
            if batch_id == batch.batch_id.load(Ordering::SeqCst) {
                //this batch is still valid
                debug!(
                    "[{:?}] remote_am spawning to big tx task {} of size {:?}  to pe {:?} ",
                    std::thread::current().id(),
                    batch_id,
                    size,
                    batch.pe,
                );
                SimpleBatcher::create_tx_task(batch).await;
            } else {
                debug!("remote am Someone else is transmitting the batch {batch_id} already");
            }
            stats!(IO_TASK_TOO_BIG_FINISH[0].fetch_add(1, Ordering::Relaxed));
        }
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    async fn add_return_am_to_batch(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        am_id: AmId,
        am_size: usize,
        mut stall_mark: usize,
    ) {
        // trace!("add_return_am_to_batch");
        //let dst =req_data.dst;
        let batch = match req_data.dst {
            Some(dst) => {
                stats!(
                    BATCHER_AM_PE_SEND_CNTS.0[&StatType::Remote][&dst][&StatCmd::Return]
                        .fetch_add(1, Ordering::Relaxed)
                );
                stats!(
                    BATCHER_AM_PE_SEND_CNTS.0[&StatType::Remote][&dst][&StatCmd::Multi]
                        .fetch_add(1, Ordering::Relaxed)
                );
                self.batched_ams[dst].clone()
            }
            None => {
                stats!(BATCHER_AM_PE_SEND_CNTS.0[&StatType::Remote]
                    .iter()
                    .for_each(|(pe, c)| {
                        if pe < &req_data.team.lamellae.comm().num_pes()
                            && pe != &req_data.team.lamellae.comm().my_pe()
                        {
                            c[&StatCmd::Return].fetch_add(1, Ordering::Relaxed);
                            c[&StatCmd::Multi].fetch_add(1, Ordering::Relaxed);
                        }
                    }));
                self.batched_ams.last().unwrap().clone()
            }
        };
        if stall_mark == 0 {
            self.stall_mark.fetch_add(1, Ordering::Relaxed);
        }
        let size = batch.add(
            req_data,
            LamellarData::Return(am, am_id, am_size),
            am_size,
            *AM_HEADER_LEN.get().expect("am header size not calculated"),
        );
        let batch_id = batch.batch_id.load(Ordering::SeqCst);
        if size == 0 {
            //first data in batch, schedule a transfer task
            let cur_stall_mark = self.stall_mark.clone();
            stats!(IO_TASK_SPAWN[1].fetch_add(1, Ordering::Relaxed));
            self.executor.submit_io_task(async move {
                stats!(IO_TASK_START[1].fetch_add(1, Ordering::Relaxed));
                let mut timer = std::time::Instant::now();
                while stall_mark != cur_stall_mark.load(Ordering::Acquire)
                    && batch.size.load(Ordering::SeqCst) < MAX_BATCH_SIZE
                    && batch_id == batch.batch_id.load(Ordering::SeqCst)
                {
                    if timer.elapsed().as_secs_f32() > 10.0 {
                        debug!(
                            "[{:?}] return am waiting to send batch_id {} stall_mark {} cur_stall_mark {} size {}",
                            std::thread::current().id(),
                            batch_id,
                            stall_mark,
                            cur_stall_mark.load(Ordering::Relaxed),
                            batch.size.load(Ordering::SeqCst)
                        );
                        timer = std::time::Instant::now();
                    }
                    stall_mark = cur_stall_mark.load(Ordering::Relaxed);
                    async_std::task::yield_now().await;
                }
                if batch_id == batch.batch_id.load(Ordering::SeqCst) {
                    //this batch is still valid
                     debug!(
                        "[{:?}] return_am spawning tx task {} of size {:?}  to pe {:?} ",
                        std::thread::current().id(),
                        batch_id,
                        size,
                        batch.pe,
                    );
                    SimpleBatcher::create_tx_task(batch).await;
                } else {
                    debug!("return am Someone else is transmitting the batch {batch_id} already");
                }
                stats!(IO_TASK_FINISH[1].fetch_add(1, Ordering::Relaxed));
            });
        } else if size >= MAX_BATCH_SIZE {
            stats!(IO_TASK_TOO_BIG[1].fetch_add(1, Ordering::Relaxed));
            if batch_id == batch.batch_id.load(Ordering::SeqCst) {
                //this batch is still valid
                debug!(
                    "[{:?}] return_am too big spawning tx task {} of size {:?}  to pe {:?} ",
                    std::thread::current().id(),
                    batch_id,
                    size,
                    batch.pe,
                );
                SimpleBatcher::create_tx_task(batch).await;
            } else {
                debug!(
                    "return am too big Someone else is transmitting the batch {batch_id} already"
                );
            }
            stats!(IO_TASK_TOO_BIG_FINISH[1].fetch_add(1, Ordering::Relaxed));
        }
    }

    // //#[tracing::instrument(skip_all)]
    async fn add_data_am_to_batch(
        &self,
        req_data: ReqMetaData,
        data: LamellarResultArc,
        data_size: usize,
        mut stall_mark: usize,
    ) {
        let batch = match req_data.dst {
            Some(dst) => {
                stats!(
                    BATCHER_AM_PE_SEND_CNTS.0[&StatType::Remote][&dst][&StatCmd::Data]
                        .fetch_add(1, Ordering::Relaxed)
                );
                stats!(
                    BATCHER_AM_PE_SEND_CNTS.0[&StatType::Remote][&dst][&StatCmd::Multi]
                        .fetch_add(1, Ordering::Relaxed)
                );
                self.batched_ams[dst].clone()
            }
            None => {
                stats!(BATCHER_AM_PE_SEND_CNTS.0[&StatType::Remote]
                    .iter()
                    .for_each(|(pe, c)| {
                        if pe < &req_data.team.lamellae.comm().num_pes()
                            && pe != &req_data.team.lamellae.comm().my_pe()
                        {
                            c[&StatCmd::Data].fetch_add(1, Ordering::Relaxed);
                            c[&StatCmd::Multi].fetch_add(1, Ordering::Relaxed);
                        }
                    }));
                self.batched_ams.last().unwrap().clone()
            }
        };
        if stall_mark == 0 {
            self.stall_mark.fetch_add(1, Ordering::Relaxed);
        }
        let mut darcs = vec![];
        data.ser(1, &mut darcs); //1 because we are only sending back to the original PE
        let darc_list_size = crate::serialized_size(&darcs, false);
        let size = batch.add(
            req_data,
            LamellarData::Data(data, darcs, data_size, darc_list_size),
            data_size,
            darc_list_size + *DATA_HEADER_LEN,
        );
        let batch_id = batch.batch_id.load(Ordering::SeqCst);
        if size == 0 {
            //first data in batch, schedule a transfer task
            let cur_stall_mark = self.stall_mark.clone();
            stats!(IO_TASK_SPAWN[2].fetch_add(1, Ordering::Relaxed));

            self.executor.submit_io_task(async move {
                    stats!(IO_TASK_START[2].fetch_add(1, Ordering::Relaxed));
                let mut timer = std::time::Instant::now();
                while stall_mark != cur_stall_mark.load(Ordering::Acquire)
                    && batch.size.load(Ordering::SeqCst) < MAX_BATCH_SIZE
                    && batch_id == batch.batch_id.load(Ordering::SeqCst)
                {
                    if timer.elapsed().as_secs_f32() > 10.0 {
                        debug!(
                            "[{:?}] data am waiting to send batch_id {} stall_mark {} cur_stall_mark {} size {}",
                            std::thread::current().id(),
                            batch_id,
                            stall_mark,
                            cur_stall_mark.load(Ordering::Relaxed),
                            batch.size.load(Ordering::SeqCst)
                        );
                        timer = std::time::Instant::now();
                    }
                    stall_mark = cur_stall_mark.load(Ordering::Relaxed);
                    async_std::task::yield_now().await;
                }
                if batch_id == batch.batch_id.load(Ordering::SeqCst) {
                    //this batch is still valid
                     debug!(
                        "[{:?}] data spawning tx task {} of size {:?}  to pe {:?} ",
                        std::thread::current().id(),
                        batch_id,
                        size,
                        batch.pe,
                    );
                    SimpleBatcher::create_tx_task(batch).await;
                } else {
                   debug!("data am Someone else is transmitting the batch {batch_id} already");
                }
                stats!(IO_TASK_FINISH[2].fetch_add(1, Ordering::Relaxed));
            });
        } else if size >= MAX_BATCH_SIZE {
            stats!(IO_TASK_TOO_BIG[2].fetch_add(1, Ordering::Relaxed));
            if batch_id == batch.batch_id.load(Ordering::SeqCst) {
                //this batch is still valid
                debug!(
                    "[{:?}] data too big spawning tx task {} of size {:?}  to pe {:?} ",
                    std::thread::current().id(),
                    batch_id,
                    size,
                    batch.pe,
                );
                SimpleBatcher::create_tx_task(batch).await;
            } else {
                debug!("data am too big Someone else is transmitting the batch {batch_id} already");
            }
            stats!(IO_TASK_TOO_BIG_FINISH[2].fetch_add(1, Ordering::Relaxed));
        }
    }

    // //#[tracing::instrument(skip_all)]
    async fn add_unit_am_to_batch(&self, req_data: ReqMetaData, mut stall_mark: usize) {
        let batch = match req_data.dst {
            Some(dst) => {
                stats!(
                    BATCHER_AM_PE_SEND_CNTS.0[&StatType::Remote][&dst][&StatCmd::Unit]
                        .fetch_add(1, Ordering::Relaxed)
                );
                stats!(
                    BATCHER_AM_PE_SEND_CNTS.0[&StatType::Remote][&dst][&StatCmd::Multi]
                        .fetch_add(1, Ordering::Relaxed)
                );
                self.batched_ams[dst].clone()
            }
            None => {
                stats!(BATCHER_AM_PE_SEND_CNTS.0[&StatType::Remote]
                    .iter()
                    .for_each(|(pe, c)| {
                        if pe < &req_data.team.lamellae.comm().num_pes()
                            && pe != &req_data.team.lamellae.comm().my_pe()
                        {
                            c[&StatCmd::Unit].fetch_add(1, Ordering::Relaxed);
                            c[&StatCmd::Multi].fetch_add(1, Ordering::Relaxed);
                        }
                    }));
                self.batched_ams.last().unwrap().clone()
            }
        };
        if stall_mark == 0 {
            self.stall_mark.fetch_add(1, Ordering::Relaxed);
        }
        let size = batch.add(req_data, LamellarData::Unit, 0, *UNIT_HEADER_LEN);
        let batch_id = batch.batch_id.load(Ordering::SeqCst);
        if size == 0 {
            //first data in batch, schedule a transfer task

            let cur_stall_mark = self.stall_mark.clone();
            stats!(IO_TASK_SPAWN[3].fetch_add(1, Ordering::Relaxed));
            self.executor.submit_io_task(async move {
                stats!(IO_TASK_START[3].fetch_add(1, Ordering::Relaxed));
                let mut timer = std::time::Instant::now();
                while stall_mark != cur_stall_mark.load(Ordering::Acquire)
                    && batch.size.load(Ordering::SeqCst) < MAX_BATCH_SIZE
                    && batch_id == batch.batch_id.load(Ordering::SeqCst)
                {
                    if timer.elapsed().as_secs_f32() > 10.0 {
                        debug!(
                            "[{:?}] unit am waiting to send batch_id {} stall_mark {} cur_stall_mark {} size {}",
                            std::thread::current().id(),
                            batch_id,
                            stall_mark,
                            cur_stall_mark.load(Ordering::Relaxed),
                            batch.size.load(Ordering::SeqCst)
                        );
                        timer = std::time::Instant::now();
                    }
                    stall_mark = cur_stall_mark.load(Ordering::Relaxed);
                    async_std::task::yield_now().await;
                }
                if batch_id == batch.batch_id.load(Ordering::SeqCst) {
                    //this batch is still valid
                     debug!(
                        "[{:?}] unit spawning tx task {} of size {:?}  to pe {:?} ",
                        std::thread::current().id(),
                        batch_id,
                        size,
                        batch.pe,
                    );
                    SimpleBatcher::create_tx_task(batch).await;
                } else {
                   debug!("unit am Someone else is transmitting the batch {batch_id} already");
                }
                stats!(IO_TASK_FINISH[3].fetch_add(1, Ordering::Relaxed ));
            });
        } else if size >= MAX_BATCH_SIZE {
            stats!(IO_TASK_TOO_BIG[3].fetch_add(1, Ordering::Relaxed));
            if batch_id == batch.batch_id.load(Ordering::SeqCst) {
                //this batch is still valid
                debug!(
                    "[{:?}] unit too big spawning tx task {} of size {:?}  to pe {:?} ",
                    std::thread::current().id(),
                    batch_id,
                    size,
                    batch.pe,
                );
                SimpleBatcher::create_tx_task(batch).await;
            } else {
                debug!("unit am too big Someone else is transmitting the batch {batch_id} already");
            }
            stats!(IO_TASK_TOO_BIG_FINISH[3].fetch_add(1, Ordering::Relaxed));
        }
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    async fn exec_batched_msg(
        &self,
        msg: Msg,
        mut ser_data: SerializedData,
        lamellae: &Arc<Lamellae>,
        ame: &RegisteredActiveMessages,
    ) {
        let mut i = 0;
        trace!("executing batched msg {:?}", ser_data.data_len());
        let mut cnts = HashMap::new();
        // let mut cnt =0;
        while i < ser_data.data_len() {
            let cmd: Cmd = ser_data
                .sub_data(i, i + *CMD_LEN)
                .deserialize_data()
                .unwrap();
            i += *CMD_LEN;
            // print!("{cnt} ");
            // cnt+=1;
            match cmd {
                Cmd::Am => {
                    *cnts.entry(Cmd::Am).or_insert(0) += 1;
                    let data = ser_data.data_as_bytes();
                    self.exec_am_serde(msg.src as usize, &data, &mut i, lamellae, ame);
                    stats!(
                        BATCHER_AM_PE_RECV_CNTS.0[&StatType::Remote][&(msg.src as usize)]
                            [&StatCmd::Am]
                            .fetch_add(1, Ordering::Relaxed)
                    );
                    stats!(
                        BATCHER_AM_PE_RECV_CNTS.0[&StatType::Remote][&(msg.src as usize)]
                            [&StatCmd::Multi]
                            .fetch_add(1, Ordering::Relaxed)
                    );
                }
                Cmd::ReturnAm => {
                    *cnts.entry(Cmd::ReturnAm).or_insert(0) += 1;
                    let data = ser_data.data_as_bytes();
                    self.exec_return_am_serde(msg.src as usize, &data, &mut i, lamellae, ame)
                        .await;
                    stats!(
                        BATCHER_AM_PE_RECV_CNTS.0[&StatType::Orig][&(msg.src as usize)]
                            [&StatCmd::Return]
                            .fetch_add(1, Ordering::Relaxed)
                    );
                    stats!(
                        BATCHER_AM_PE_RECV_CNTS.0[&StatType::Orig][&(msg.src as usize)]
                            [&StatCmd::Multi]
                            .fetch_add(1, Ordering::Relaxed)
                    );
                }
                Cmd::Data => {
                    *cnts.entry(Cmd::Data).or_insert(0) += 1;
                    let data = ser_data.data_as_bytes();
                    exec_data_am_serde(msg.src as usize, &data, &mut i, ame);
                    stats!(
                        BATCHER_AM_PE_RECV_CNTS.0[&StatType::Orig][&(msg.src as usize)]
                            [&StatCmd::Data]
                            .fetch_add(1, Ordering::Relaxed)
                    );
                    stats!(
                        BATCHER_AM_PE_RECV_CNTS.0[&StatType::Orig][&(msg.src as usize)]
                            [&StatCmd::Multi]
                            .fetch_add(1, Ordering::Relaxed)
                    );
                }
                Cmd::Unit => {
                    *cnts.entry(Cmd::Unit).or_insert(0) += 1;
                    let data = ser_data.data_as_bytes();
                    exec_unit_am_serde(msg.src as usize, &data, &mut i, ame);
                    stats!(
                        BATCHER_AM_PE_RECV_CNTS.0[&StatType::Orig][&(msg.src as usize)]
                            [&StatCmd::Unit]
                            .fetch_add(1, Ordering::Relaxed)
                    );
                    stats!(
                        BATCHER_AM_PE_RECV_CNTS.0[&StatType::Orig][&(msg.src as usize)]
                            [&StatCmd::Multi]
                            .fetch_add(1, Ordering::Relaxed)
                    );
                }
                Cmd::BatchedMsg => {
                    panic!("should not recieve a batched msg within a Simple Batcher batched msg")
                }
            }
        }
        trace!(
            "finished batched msg from {:?} {:?} {:?}",
            msg.src,
            cnts,
            ser_data.data_len(),
        );
        trace!(target: "lamellae_debug",
            "finished batched msg from {:?} {:?} {:?}, lamellae cnt: {:?}",
            msg.src,
            cnts,
            ser_data.data_len(),
            Arc::strong_count(&lamellae)
        );
    }

    async fn send_am(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        am_id: AmId,
        _am_size: usize,
        cmd: Cmd,
    ) {
        send_am_serde(req_data, am, am_id, cmd).await;
    }

    async fn send_data_am(&self, req_data: ReqMetaData, data: LamellarResultArc, data_size: usize) {
        send_data_am_serde(req_data, data, data_size).await;
    }

    async fn send_unit_am(&self, req_data: ReqMetaData) {
        send_unit_am_serde(req_data).await;
    }

    async fn exec_am(
        &self,
        src: usize,
        data: &[u8],
        i: &mut usize,
        lamellae: &Arc<Lamellae>,
        ame: &RegisteredActiveMessages,
        _executor: &Arc<Executor>,
    ) {
        self.exec_am_serde(src, data, i, lamellae, ame);
    }

    async fn exec_return_am(
        &self,
        src: usize,
        data: &[u8],
        i: &mut usize,
        lamellae: &Arc<Lamellae>,
        ame: &RegisteredActiveMessages,
    ) {
        self.exec_return_am_serde(src, data, i, lamellae, ame).await;
    }

    fn exec_data_am(&self, src: usize, data: &[u8], i: &mut usize, ame: &RegisteredActiveMessages) {
        exec_data_am_serde(src, data, i, ame);
    }

    fn exec_unit_am(&self, src: usize, data: &[u8], i: &mut usize, ame: &RegisteredActiveMessages) {
        exec_unit_am_serde(src, data, i, ame);
    }
}

impl SimpleBatcher {
    //#[tracing::instrument(skip_all, level = "debug")]
    pub(crate) fn new(
        num_pes: usize,
        stall_mark: Arc<AtomicUsize>,
        executor: Arc<Executor>,
    ) -> SimpleBatcher {
        let mut batched_ams = Vec::new();
        for pe in 0..num_pes {
            batched_ams.push(SimpleBatcherInner::new(Some(pe)));
        }
        batched_ams.push(SimpleBatcherInner::new(None));
        SimpleBatcher {
            batched_ams: Arc::new(batched_ams),
            stall_mark,
            executor,
        }
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    async fn create_tx_task(batch: SimpleBatcherInner) {
        let old_batch_id = batch.batch_id.load(Ordering::SeqCst);

        let (buf, size, batch_id) = batch.swap();
        debug!(
            "[{:?}] create_tx_task for batch ({}) {} {} {:?}",
            std::thread::current().id(),
            old_batch_id,
            batch_id,
            size,
            batch.pe
        );

        if size > 0 {
            let lamellae = buf[0].0.lamellae.clone();
            let arch = buf[0].0.team.arch.clone();
            let header = SimpleBatcher::create_header(buf[0].0.team.world_pe);
            let mut data_buf = SimpleBatcher::create_data_buf(header, size, &lamellae).await;
            let data_slice = data_buf.data_as_bytes_mut();

            let mut cnts = HashMap::new();

            let mut i = 0;
            let batched_cnt = buf.len();
            for (req_data, data, _size) in buf {
                let req_data_slice = data_slice.sub_slice(i..);
                match data {
                    LamellarData::Am(am, id, am_size) => {
                        i += SimpleBatcher::serialize_am(
                            req_data,
                            am_size,
                            am,
                            id,
                            req_data_slice,
                            Cmd::Am,
                        );
                        cnts.entry(Cmd::Am).and_modify(|e| *e += 1).or_insert(1);
                    }
                    LamellarData::Return(am, id, am_size) => {
                        i += SimpleBatcher::serialize_am(
                            req_data,
                            am_size,
                            am,
                            id,
                            req_data_slice,
                            Cmd::ReturnAm,
                        );
                        cnts.entry(Cmd::ReturnAm)
                            .and_modify(|e| *e += 1)
                            .or_insert(1);
                    }
                    LamellarData::Data(data, darcs, data_size, darc_list_size) => {
                        i += SimpleBatcher::serialize_data(
                            req_data,
                            data_size,
                            data,
                            req_data_slice,
                            darcs,
                            darc_list_size,
                        );
                        cnts.entry(Cmd::Data).and_modify(|e| *e += 1).or_insert(1);
                    }
                    LamellarData::Unit => {
                        i += SimpleBatcher::serialize_unit(req_data, req_data_slice);
                        cnts.entry(Cmd::Unit).and_modify(|e| *e += 1).or_insert(1);
                    }
                }
            }
            debug!(
                "[{:?}] sending batch {batch_id} of size {} {:?} to pe {:?} {:?}",
                std::thread::current().id(),
                i,
                data_buf,
                batch.pe,
                cnts
            );
            match batch.pe {
                Some(pe) => {
                    stats!(
                        BATCHER_AM_PE_SEND_CNTS.0[&StatType::Orig][&pe][&StatCmd::Batched]
                            .fetch_add(1, Ordering::Relaxed)
                    );
                    stats!(
                        BATCHER_AM_PE_SEND_CNTS.0[&StatType::Orig][&pe][&StatCmd::MultiBatched]
                            .fetch_add(batched_cnt, Ordering::Relaxed)
                    );
                }
                None => {
                    stats!(BATCHER_AM_PE_SEND_CNTS.0[&StatType::Orig].iter().for_each(
                        |(pe, c)| {
                            if pe < &lamellae.comm().num_pes() && pe != &lamellae.comm().my_pe() {
                                c[&StatCmd::Batched].fetch_add(1, Ordering::Relaxed);
                                c[&StatCmd::MultiBatched].fetch_add(batched_cnt, Ordering::Relaxed);
                            }
                        }
                    ));
                }
            }

            lamellae.send_to_pes_async(batch.pe, arch, data_buf).await;
        } else {
            debug!(
                "[{:?}] skipping send of empty batch { }",
                std::thread::current().id(),
                batch_id
            );
        }
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    fn serialize_am(
        req_data: ReqMetaData,
        am_size: usize,
        am: LamellarArcAm,
        am_id: AmId,
        mut data_buf: CommSlice<u8>,
        cmd: Cmd,
    ) -> usize {
        // println!("serialize_am");
        let mut i = 0;
        crate::serialize_into(&mut data_buf[i..i + *CMD_LEN], &cmd, false).unwrap();
        i += *CMD_LEN;

        // if req_data.dst.is_some() {
        //     req_data.team.ser(1, &mut vec![]); //ensure team is serialized for am header
        // } else {
        //     req_data.team.ser(req_data.team.num_pes(), &mut vec![]); //ensure team is serialized for am header
        // }
        let am_header = AmHeader {
            am_id,
            req_id: req_data.id,
            // team_addr: req_data.team_addr.into(),
            team_addr: req_data.team.darc_addr(),
            // team: req_data.team.clone(),
        };
        trace!(
            "serializing am header for {:?} ,req: {:?}",
            am_header,
            req_data
        );
        crate::serialize_into(
            &mut data_buf[i..i + *AM_HEADER_LEN.get().expect("am header size not calculated")],
            &am_header,
            false,
        )
        .unwrap();
        i += *AM_HEADER_LEN.get().expect("am header size not calculated");

        let darc_ser_cnt = match req_data.dst {
            Some(_) => 1,
            None => {
                match req_data.team.team_pe_id() {
                    Ok(_) => req_data.team.num_pes() - 1, //we dont send an am to ourself here
                    Err(_) => req_data.team.num_pes(), //this means we have a handle to a team but are not in the team
                }
            }
        };
        let mut darcs = vec![];
        am.ser(darc_ser_cnt, &mut darcs);
        {
            let _mrg = crate::memregion::one_sided::MemRegionSendGuard::new(darc_ser_cnt);
            am.serialize_into(&mut data_buf[i..i + am_size]);
        }
        i + am_size
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    fn serialize_data(
        req_data: ReqMetaData,
        data_size: usize,
        data: LamellarResultArc,
        mut data_buf: CommSlice<u8>,
        darcs: Vec<RemotePtr>,
        darc_list_size: usize,
    ) -> usize {
        // println!("serialize_data");
        let mut i = 0;
        crate::serialize_into(&mut data_buf[i..i + *CMD_LEN], &Cmd::Data, false).unwrap();
        i += *CMD_LEN;
        let data_header = DataHeader {
            size: data_size,
            req_id: req_data.id,
            darc_list_size,
        };
        crate::serialize_into(&mut data_buf[i..i + *DATA_HEADER_LEN], &data_header, false).unwrap();
        i += *DATA_HEADER_LEN;

        crate::serialize_into(&mut data_buf[i..(i + darc_list_size)], &darcs, false).unwrap();
        i += darc_list_size;

        {
            let _mrg = crate::memregion::one_sided::MemRegionSendGuard::new(1);
            data.serialize_into(&mut data_buf[i..i + data_size]);
        }
        i + data_size
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    fn serialize_unit(req_data: ReqMetaData, mut data_buf: CommSlice<u8>) -> usize {
        // println!("serialize_unit");
        let mut i = 0;
        crate::serialize_into(&mut data_buf[i..i + *CMD_LEN], &Cmd::Unit, false).unwrap();
        i += *CMD_LEN;

        let unit_header = UnitHeader {
            req_id: req_data.id,
        };
        crate::serialize_into(&mut data_buf[i..i + *UNIT_HEADER_LEN], &unit_header, false).unwrap();
        i + *UNIT_HEADER_LEN
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    fn create_header(src: usize) -> SerializeHeader {
        // println!("create_header");
        let msg = Msg {
            src: src as u16,
            cmd: Cmd::BatchedMsg,
            padding: [0; 1],
        };
        SerializeHeader { msg }
    }

    //#[tracing::instrument(skip_all, level = "debug")]
    async fn create_data_buf(
        header: SerializeHeader,
        size: usize,
        lamellae: &Arc<Lamellae>,
    ) -> SerializedData {
        let header = Some(header);
        let mut data = lamellae.serialize_header(header.clone(), size);
        while let Err(err) = data {
            async_std::task::yield_now().await;
            match err.downcast_ref::<AllocError>() {
                Some(AllocError::OutOfMemoryError(_)) => {
                    lamellae.request_new_alloc(size * 2).await;
                }
                _ => panic!("unhanlded error!! {:?}", err),
            }
            data = lamellae.serialize_header(header.clone(), size);
        }
        trace!(target: "lamellae_debug",
            "created data buf of size {} with header {:?}, lamellae cnt: {:?}",
            size,
            header,
            Arc::strong_count(&lamellae)
        );
        data.unwrap()
    }

    fn exec_am_serde(
        &self,
        src: usize,
        data: &[u8],
        i: &mut usize,
        lamellae: &Arc<Lamellae>,
        ame: &RegisteredActiveMessages,
    ) {
        exec_am_serde(src, data, i, lamellae, ame, &self.executor);
    }

    async fn exec_return_am_serde(
        &self,
        src: usize,
        data: &[u8],
        i: &mut usize,
        lamellae: &Arc<Lamellae>,
        ame: &RegisteredActiveMessages,
    ) {
        exec_return_am_serde(src, data, i, lamellae, ame).await;
    }
}
