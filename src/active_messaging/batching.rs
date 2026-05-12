use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;

use crate::active_messaging::registered_active_message::AmId;
use crate::active_messaging::*;

pub(crate) mod simple_batcher;
use simple_batcher::SimpleBatcher;

pub(crate) mod team_am_batcher;
use team_am_batcher::TeamAmBatcher;

use async_trait::async_trait;

#[derive(Clone, Copy, Hash, PartialEq, Eq, Debug)]
pub(crate) enum StatCmd {
    Am,
    Return,
    Data,
    Unit,
    Batched,
    Single,
    Multi,
    MultiBatched,
}
#[derive(Clone, Copy, Hash, PartialEq, Eq, Debug)]
pub(crate) enum StatType {
    Orig,
    Remote,
}

pub(crate) struct BatcherStatMap(
    pub(crate) HashMap<StatType, HashMap<usize, HashMap<StatCmd, AtomicUsize>>>,
);

lazy_static! {
    pub(crate) static ref BATCHER_AM_PE_SEND_CNTS: BatcherStatMap = {
        let mut m = HashMap::new();
        for stat_type in &[StatType::Orig, StatType::Remote] {
            let mut pe_map = HashMap::new();
            for i in 0..32 {
                let mut t = HashMap::new();
                for cmd in &[
                    StatCmd::Am,
                    StatCmd::Return,
                    StatCmd::Data,
                    StatCmd::Unit,
                    StatCmd::Batched,
                    StatCmd::Single,
                    StatCmd::Multi,
                    StatCmd::MultiBatched,
                ] {
                    t.insert(*cmd, AtomicUsize::new(0));
                }
                pe_map.insert(i, t);
            }
            m.insert(stat_type.clone(), pe_map);
        }
        BatcherStatMap(m)
    };
    pub(crate) static ref BATCHER_AM_PE_RECV_CNTS: BatcherStatMap = {
        let mut m = HashMap::new();
        for stat_type in &[StatType::Orig, StatType::Remote] {
            let mut pe_map = HashMap::new();
            for i in 0..32 {
                let mut t = HashMap::new();
                for cmd in &[
                    StatCmd::Am,
                    StatCmd::Return,
                    StatCmd::Data,
                    StatCmd::Unit,
                    StatCmd::Batched,
                    StatCmd::Single,
                    StatCmd::Multi,
                    StatCmd::MultiBatched,
                ] {
                    t.insert(*cmd, AtomicUsize::new(0));
                }
                pe_map.insert(i, t);
            }
            m.insert(stat_type.clone(), pe_map);
        }
        BatcherStatMap(m)
    };
}

impl std::fmt::Debug for BatcherStatMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (stat_type, pe_map) in &self.0 {
            writeln!(f, "{:?}:", stat_type)?;
            for (pe, cmd_map) in pe_map {
                if cmd_map
                    .values()
                    .map(|cnt| cnt.load(std::sync::atomic::Ordering::Relaxed))
                    .any(|x| x > 0)
                {
                    writeln!(f, "  PE {}:", pe)?;
                    for (cmd, cnt) in cmd_map {
                        writeln!(
                            f,
                            "    {:?}: {}",
                            cmd,
                            cnt.load(std::sync::atomic::Ordering::Relaxed)
                        )?;
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
enum LamellarData {
    Am(LamellarArcAm, AmId, usize),
    Return(LamellarArcAm, AmId, usize),
    Data(LamellarResultArc, Vec<RemotePtr>, usize, usize),
    Unit,
}

impl std::fmt::Debug for LamellarData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LamellarData::Am(_, _, _) => write!(f, "Am"),
            LamellarData::Return(_, _, _) => write!(f, "Return"),
            LamellarData::Data(_, _, _, _) => write!(f, "Data"),
            LamellarData::Unit => write!(f, "Unit"),
        }
    }
}

#[async_trait]
pub(crate) trait Batcher {
    async fn add_remote_am_to_batch(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        am_id: AmId,
        am_size: usize,
        stall_mark: usize,
    );
    async fn add_return_am_to_batch(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        am_id: AmId,
        am_size: usize,
        stall_mark: usize,
    );
    async fn add_data_am_to_batch(
        &self,
        req_data: ReqMetaData,
        data: LamellarResultArc,
        data_size: usize,
        stall_mark: usize,
    );
    async fn add_unit_am_to_batch(&self, req_data: ReqMetaData, stall_mark: usize);

    async fn exec_batched_msg(
        &self,
        msg: Msg,
        ser_data: SerializedData,
        lamellae: Arc<Lamellae>,
        ame: &RegisteredActiveMessages,
    );
}

#[derive(Debug, Clone)]
pub(crate) enum BatcherType {
    Simple(SimpleBatcher),
    TeamAm(TeamAmBatcher),
}

#[async_trait]
impl Batcher for BatcherType {
    // //#[tracing::instrument(skip_all)]
    async fn add_remote_am_to_batch(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        am_id: AmId,
        am_size: usize,
        stall_mark: usize,
    ) {
        match self {
            BatcherType::Simple(batcher) => {
                batcher
                    .add_remote_am_to_batch(req_data, am, am_id, am_size, stall_mark)
                    .await
            }
            BatcherType::TeamAm(batcher) => {
                batcher
                    .add_remote_am_to_batch(req_data, am, am_id, am_size, stall_mark)
                    .await
            }
        }
    }
    // //#[tracing::instrument(skip_all)]
    async fn add_return_am_to_batch(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        am_id: AmId,
        am_size: usize,
        stall_mark: usize,
    ) {
        match self {
            BatcherType::Simple(batcher) => {
                batcher
                    .add_return_am_to_batch(req_data, am, am_id, am_size, stall_mark)
                    .await
            }
            BatcherType::TeamAm(batcher) => {
                batcher
                    .add_return_am_to_batch(req_data, am, am_id, am_size, stall_mark)
                    .await
            }
        }
    }
    // //#[tracing::instrument(skip_all)]
    async fn add_data_am_to_batch(
        &self,
        req_data: ReqMetaData,
        data: LamellarResultArc,
        data_size: usize,
        stall_mark: usize,
    ) {
        match self {
            BatcherType::Simple(batcher) => {
                batcher
                    .add_data_am_to_batch(req_data, data, data_size, stall_mark)
                    .await
            }
            BatcherType::TeamAm(batcher) => {
                batcher
                    .add_data_am_to_batch(req_data, data, data_size, stall_mark)
                    .await
            }
        }
    }
    // //#[tracing::instrument(skip_all)]
    async fn add_unit_am_to_batch(&self, req_data: ReqMetaData, stall_mark: usize) {
        match self {
            BatcherType::Simple(batcher) => {
                batcher.add_unit_am_to_batch(req_data, stall_mark).await
            }
            BatcherType::TeamAm(batcher) => {
                batcher.add_unit_am_to_batch(req_data, stall_mark).await
            }
        }
    }
    //#[tracing::instrument(skip_all, level = "debug")]
    async fn exec_batched_msg(
        &self,
        msg: Msg,
        ser_data: SerializedData,
        lamellae: Arc<Lamellae>,
        ame: &RegisteredActiveMessages,
    ) {
        match self {
            BatcherType::Simple(batcher) => {
                batcher.exec_batched_msg(msg, ser_data, lamellae, ame).await
            }
            BatcherType::TeamAm(batcher) => {
                batcher.exec_batched_msg(msg, ser_data, lamellae, ame).await
            }
        }
    }
}
