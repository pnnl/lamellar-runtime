use std::collections::HashMap;
use std::sync::LazyLock;
use std::sync::atomic::AtomicUsize;

use crate::active_messaging::registered_active_message::{AMS_EXECS, AmId};
use crate::active_messaging::*;
use crate::lamellae::{
    Lamellae, LamellaeUtil, Ser, SerializeHeader, SerializedData,
    comm::{CommInfo, error::AllocError},
};
use direct_batcher::{MyAmHeader, MyDataHeader, MyUnitHeader};
use zerocopy::*;

pub(crate) mod simple_batcher;
use simple_batcher::SimpleBatcher;

pub(crate) mod direct_batcher;
use direct_batcher::DirectBatcher;

pub(crate) mod vec_simple_batcher;
use vec_simple_batcher::VecSimpleBatcher;

pub(crate) mod team_am_batcher;
use team_am_batcher::TeamAmBatcher;

pub(crate) mod vec_team_am_batcher;
use vec_team_am_batcher::VecTeamAmBatcher;

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

pub(crate) static BATCHER_AM_PE_SEND_CNTS: LazyLock<BatcherStatMap> = LazyLock::new(|| {
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
});
pub(crate) static BATCHER_AM_PE_RECV_CNTS: LazyLock<BatcherStatMap> = LazyLock::new(|| {
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
});

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
    Am(LamellarArcAm, AmId, Vec<u8>),
    Return(LamellarArcAm, AmId, Vec<u8>),
    Data(Vec<u8>, Vec<u8>),
    Unit,
}

impl std::fmt::Debug for LamellarData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LamellarData::Am(_, _, _) => write!(f, "Am"),
            LamellarData::Return(_, _, _) => write!(f, "Return"),
            LamellarData::Data(_, _) => write!(f, "Data"),
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
        am_bytes: Vec<u8>,
        stall_mark: usize,
    );
    async fn add_return_am_to_batch(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        am_id: AmId,
        am_bytes: Vec<u8>,
        stall_mark: usize,
    );
    async fn add_data_am_to_batch(
        &self,
        req_data: ReqMetaData,
        darc_bytes: Vec<u8>,
        data_bytes: Vec<u8>,
        stall_mark: usize,
    );
    async fn add_unit_am_to_batch(&self, req_data: ReqMetaData, stall_mark: usize);

    async fn exec_batched_msg(
        &self,
        msg: Msg,
        ser_data: SerializedData,
        lamellae: &Arc<Lamellae>,
        ame: &RegisteredActiveMessages,
    );

    // Single-message send: batcher owns the wire format for over-threshold AMs.
    async fn send_am(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        am_id: AmId,
        am_bytes: Vec<u8>,
        cmd: Cmd,
    ) {
        send_am_zerocopy(req_data, am, am_id, am_bytes, cmd).await;
    }
    async fn send_data_am(&self, req_data: ReqMetaData, darc_bytes: Vec<u8>, data_bytes: Vec<u8>) {
        send_data_am_zerocopy(req_data, darc_bytes, data_bytes).await;
    }
    async fn send_unit_am(&self, req_data: ReqMetaData) {
        send_unit_am_zerocopy(req_data).await;
    }

    // Single-message exec: called from exec_msg for Cmd::Am/ReturnAm/Data/Unit.
    async fn exec_am(
        &self,
        src: usize,
        data: &[u8],
        i: &mut usize,
        lamellae: &Arc<Lamellae>,
        ame: &RegisteredActiveMessages,
        executor: &Arc<Executor>,
    ) {
        exec_am_zerocopy(src, data, i, lamellae, ame, executor).await;
    }
    async fn exec_return_am(
        &self,
        src: usize,
        data: &[u8],
        i: &mut usize,
        lamellae: &Arc<Lamellae>,
        ame: &RegisteredActiveMessages,
    ) {
        exec_return_am_zerocopy(src, data, i, lamellae, ame).await;
    }
    fn exec_data_am(&self, src: usize, data: &[u8], i: &mut usize, ame: &RegisteredActiveMessages) {
        exec_data_am_zerocopy(src, data, i, ame);
    }
    fn exec_unit_am(&self, src: usize, data: &[u8], i: &mut usize, ame: &RegisteredActiveMessages) {
        exec_unit_am_zerocopy(src, data, i, ame);
    }
}

#[derive(Debug, Clone)]
pub(crate) enum BatcherType {
    Simple(SimpleBatcher),
    Direct(DirectBatcher),
    VecSimple(VecSimpleBatcher),
    TeamAm(TeamAmBatcher),
    VecTeamAm(VecTeamAmBatcher),
}

#[async_trait]
impl Batcher for BatcherType {
    // //#[tracing::instrument(skip_all)]
    async fn add_remote_am_to_batch(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        am_id: AmId,
        am_bytes: Vec<u8>,
        stall_mark: usize,
    ) {
        match self {
            BatcherType::Simple(batcher) => {
                batcher
                    .add_remote_am_to_batch(req_data, am, am_id, am_bytes, stall_mark)
                    .await
            }
            BatcherType::Direct(batcher) => {
                batcher
                    .add_remote_am_to_batch(req_data, am, am_id, am_bytes, stall_mark)
                    .await
            }
            BatcherType::VecSimple(batcher) => {
                batcher
                    .add_remote_am_to_batch(req_data, am, am_id, am_bytes, stall_mark)
                    .await
            }
            BatcherType::TeamAm(batcher) => {
                batcher
                    .add_remote_am_to_batch(req_data, am, am_id, am_bytes, stall_mark)
                    .await
            }
            BatcherType::VecTeamAm(batcher) => {
                batcher
                    .add_remote_am_to_batch(req_data, am, am_id, am_bytes, stall_mark)
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
        am_bytes: Vec<u8>,
        stall_mark: usize,
    ) {
        match self {
            BatcherType::Simple(batcher) => {
                batcher
                    .add_return_am_to_batch(req_data, am, am_id, am_bytes, stall_mark)
                    .await
            }
            BatcherType::Direct(batcher) => {
                batcher
                    .add_return_am_to_batch(req_data, am, am_id, am_bytes, stall_mark)
                    .await
            }
            BatcherType::VecSimple(batcher) => {
                batcher
                    .add_return_am_to_batch(req_data, am, am_id, am_bytes, stall_mark)
                    .await
            }
            BatcherType::TeamAm(batcher) => {
                batcher
                    .add_return_am_to_batch(req_data, am, am_id, am_bytes, stall_mark)
                    .await
            }
            BatcherType::VecTeamAm(batcher) => {
                batcher
                    .add_return_am_to_batch(req_data, am, am_id, am_bytes, stall_mark)
                    .await
            }
        }
    }
    // //#[tracing::instrument(skip_all)]
    async fn add_data_am_to_batch(
        &self,
        req_data: ReqMetaData,
        darc_bytes: Vec<u8>,
        data_bytes: Vec<u8>,
        stall_mark: usize,
    ) {
        match self {
            BatcherType::Simple(batcher) => {
                batcher
                    .add_data_am_to_batch(req_data, darc_bytes, data_bytes, stall_mark)
                    .await
            }
            BatcherType::Direct(batcher) => {
                batcher
                    .add_data_am_to_batch(req_data, darc_bytes, data_bytes, stall_mark)
                    .await
            }
            BatcherType::VecSimple(batcher) => {
                batcher
                    .add_data_am_to_batch(req_data, darc_bytes, data_bytes, stall_mark)
                    .await
            }
            BatcherType::TeamAm(batcher) => {
                batcher
                    .add_data_am_to_batch(req_data, darc_bytes, data_bytes, stall_mark)
                    .await
            }
            BatcherType::VecTeamAm(batcher) => {
                batcher
                    .add_data_am_to_batch(req_data, darc_bytes, data_bytes, stall_mark)
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
            BatcherType::Direct(batcher) => {
                batcher.add_unit_am_to_batch(req_data, stall_mark).await
            }
            BatcherType::VecSimple(batcher) => {
                batcher.add_unit_am_to_batch(req_data, stall_mark).await
            }
            BatcherType::TeamAm(batcher) => {
                batcher.add_unit_am_to_batch(req_data, stall_mark).await
            }
            BatcherType::VecTeamAm(batcher) => {
                batcher.add_unit_am_to_batch(req_data, stall_mark).await
            }
        }
    }
    //#[tracing::instrument(skip_all, level = "debug")]
    async fn exec_batched_msg(
        &self,
        msg: Msg,
        ser_data: SerializedData,
        lamellae: &Arc<Lamellae>,
        ame: &RegisteredActiveMessages,
    ) {
        match self {
            BatcherType::Simple(batcher) => {
                batcher.exec_batched_msg(msg, ser_data, lamellae, ame).await
            }
            BatcherType::Direct(batcher) => {
                batcher.exec_batched_msg(msg, ser_data, lamellae, ame).await
            }
            BatcherType::VecSimple(batcher) => {
                batcher.exec_batched_msg(msg, ser_data, lamellae, ame).await
            }
            BatcherType::TeamAm(batcher) => {
                batcher.exec_batched_msg(msg, ser_data, lamellae, ame).await
            }
            BatcherType::VecTeamAm(batcher) => {
                batcher.exec_batched_msg(msg, ser_data, lamellae, ame).await
            }
        }
    }

    async fn send_am(
        &self,
        req_data: ReqMetaData,
        am: LamellarArcAm,
        am_id: AmId,
        am_bytes: Vec<u8>,
        cmd: Cmd,
    ) {
        match self {
            BatcherType::Simple(b) => b.send_am(req_data, am, am_id, am_bytes, cmd).await,
            BatcherType::Direct(b) => b.send_am(req_data, am, am_id, am_bytes, cmd).await,
            BatcherType::VecSimple(b) => b.send_am(req_data, am, am_id, am_bytes, cmd).await,
            BatcherType::TeamAm(b) => b.send_am(req_data, am, am_id, am_bytes, cmd).await,
            BatcherType::VecTeamAm(b) => b.send_am(req_data, am, am_id, am_bytes, cmd).await,
        }
    }
    async fn send_data_am(&self, req_data: ReqMetaData, darc_bytes: Vec<u8>, data_bytes: Vec<u8>) {
        match self {
            BatcherType::Simple(b) => b.send_data_am(req_data, darc_bytes, data_bytes).await,
            BatcherType::Direct(b) => b.send_data_am(req_data, darc_bytes, data_bytes).await,
            BatcherType::VecSimple(b) => b.send_data_am(req_data, darc_bytes, data_bytes).await,
            BatcherType::TeamAm(b) => b.send_data_am(req_data, darc_bytes, data_bytes).await,
            BatcherType::VecTeamAm(b) => b.send_data_am(req_data, darc_bytes, data_bytes).await,
        }
    }
    async fn send_unit_am(&self, req_data: ReqMetaData) {
        match self {
            BatcherType::Simple(b) => b.send_unit_am(req_data).await,
            BatcherType::Direct(b) => b.send_unit_am(req_data).await,
            BatcherType::VecSimple(b) => b.send_unit_am(req_data).await,
            BatcherType::TeamAm(b) => b.send_unit_am(req_data).await,
            BatcherType::VecTeamAm(b) => b.send_unit_am(req_data).await,
        }
    }

    async fn exec_am(
        &self,
        src: usize,
        data: &[u8],
        i: &mut usize,
        lamellae: &Arc<Lamellae>,
        ame: &RegisteredActiveMessages,
        executor: &Arc<Executor>,
    ) {
        match self {
            BatcherType::Simple(b) => b.exec_am(src, data, i, lamellae, ame, executor).await,
            BatcherType::Direct(b) => b.exec_am(src, data, i, lamellae, ame, executor).await,
            BatcherType::VecSimple(b) => b.exec_am(src, data, i, lamellae, ame, executor).await,
            BatcherType::TeamAm(b) => b.exec_am(src, data, i, lamellae, ame, executor).await,
            BatcherType::VecTeamAm(b) => b.exec_am(src, data, i, lamellae, ame, executor).await,
        }
    }
    async fn exec_return_am(
        &self,
        src: usize,
        data: &[u8],
        i: &mut usize,
        lamellae: &Arc<Lamellae>,
        ame: &RegisteredActiveMessages,
    ) {
        match self {
            BatcherType::Simple(b) => b.exec_return_am(src, data, i, lamellae, ame).await,
            BatcherType::Direct(b) => b.exec_return_am(src, data, i, lamellae, ame).await,
            BatcherType::VecSimple(b) => b.exec_return_am(src, data, i, lamellae, ame).await,
            BatcherType::TeamAm(b) => b.exec_return_am(src, data, i, lamellae, ame).await,
            BatcherType::VecTeamAm(b) => b.exec_return_am(src, data, i, lamellae, ame).await,
        }
    }
    fn exec_data_am(&self, src: usize, data: &[u8], i: &mut usize, ame: &RegisteredActiveMessages) {
        match self {
            BatcherType::Simple(b) => b.exec_data_am(src, data, i, ame),
            BatcherType::Direct(b) => b.exec_data_am(src, data, i, ame),
            BatcherType::VecSimple(b) => b.exec_data_am(src, data, i, ame),
            BatcherType::TeamAm(b) => b.exec_data_am(src, data, i, ame),
            BatcherType::VecTeamAm(b) => b.exec_data_am(src, data, i, ame),
        }
    }
    fn exec_unit_am(&self, src: usize, data: &[u8], i: &mut usize, ame: &RegisteredActiveMessages) {
        match self {
            BatcherType::Simple(b) => b.exec_unit_am(src, data, i, ame),
            BatcherType::Direct(b) => b.exec_unit_am(src, data, i, ame),
            BatcherType::VecSimple(b) => b.exec_unit_am(src, data, i, ame),
            BatcherType::TeamAm(b) => b.exec_unit_am(src, data, i, ame),
            BatcherType::VecTeamAm(b) => b.exec_unit_am(src, data, i, ame),
        }
    }
}

// ---------------------------------------------------------------------------
// Shared zerocopy wire-format free functions — used by the default impls above
// and by any batcher that doesn't need to override the wire format.
// ---------------------------------------------------------------------------

pub(crate) async fn send_am_zerocopy(
    req_data: ReqMetaData,
    am: LamellarArcAm,
    am_id: AmId,
    am_bytes: Vec<u8>,
    cmd: Cmd,
) {
    let my_pe = req_data.team.world_pe;
    let header = SerializeHeader {
        msg: Msg {
            src: my_pe as u16,
            cmd,
            padding: [0; 1],
        },
    };
    let mut bytes = header.as_bytes().to_vec();
    let darc_ser_cnt = match req_data.dst {
        Some(_) => 1,
        None => match req_data.team.team_pe_id() {
            Ok(_) => req_data.team.num_pes() - 1,
            Err(_) => req_data.team.num_pes(),
        },
    };
    let am_header = MyAmHeader {
        am_id: I32::new(am_id),
        req_id: U64::new(req_data.id.id as u64),
        req_sub_id: U64::new(req_data.id.sub_id as u64),
        data_len: U64::new(am_bytes.len() as u64),
        team_addr: U64::new(req_data.team.darc_addr() as u64),
    };
    bytes.extend_from_slice(am_header.as_bytes());
    bytes.extend_from_slice(&am_bytes);
    let mut darcs = vec![];
    match req_data.dst {
        Some(pe) => {
            am.ser(1, &mut darcs);
            req_data.lamellae.send_vec_to_pe_async(pe, bytes).await;
        }
        None => {
            am.ser(darc_ser_cnt, &mut darcs);
            for pe in req_data
                .team
                .arch
                .team_iter()
                .filter(|pe| pe != &req_data.team.world_pe)
                .collect::<Vec<_>>()
            {
                req_data
                    .lamellae
                    .send_vec_to_pe_async(pe, bytes.clone())
                    .await;
            }
        }
    }
}

pub(crate) async fn send_data_am_zerocopy(
    req_data: ReqMetaData,
    darc_bytes: Vec<u8>,
    data_bytes: Vec<u8>,
) {
    let my_pe = req_data.team.world_pe;
    let header = SerializeHeader {
        msg: Msg {
            src: my_pe as u16,
            cmd: Cmd::Data,
            padding: [0; 1],
        },
    };
    let mut bytes = header.as_bytes().to_vec();
    let data_header = MyDataHeader {
        req_id: U64::new(req_data.id.id as u64),
        req_sub_id: U64::new(req_data.id.sub_id as u64),
        size: U64::new(data_bytes.len() as u64),
        darc_list_size: U64::new(darc_bytes.len() as u64),
    };
    bytes.extend_from_slice(data_header.as_bytes());
    bytes.extend_from_slice(&darc_bytes);
    bytes.extend_from_slice(&data_bytes);
    let pe = req_data.dst.expect("send_data_am always has a dst");
    req_data.lamellae.send_vec_to_pe_async(pe, bytes).await;
}

pub(crate) async fn send_unit_am_zerocopy(req_data: ReqMetaData) {
    let my_pe = req_data.team.world_pe;
    let header = SerializeHeader {
        msg: Msg {
            src: my_pe as u16,
            cmd: Cmd::Unit,
            padding: [0; 1],
        },
    };
    let mut bytes = header.as_bytes().to_vec();
    let unit_header = MyUnitHeader {
        req_id: U64::new(req_data.id.id as u64),
        req_sub_id: U64::new(req_data.id.sub_id as u64),
    };
    bytes.extend_from_slice(unit_header.as_bytes());
    match req_data.dst {
        Some(pe) => {
            req_data.lamellae.send_vec_to_pe_async(pe, bytes).await;
        }
        None => {
            for pe in req_data
                .team
                .arch
                .team_iter()
                .filter(|pe| pe != &req_data.team.world_pe)
                .collect::<Vec<_>>()
            {
                req_data
                    .lamellae
                    .send_vec_to_pe_async(pe, bytes.clone())
                    .await;
            }
        }
    }
}

pub(crate) async fn exec_am_zerocopy(
    src: usize,
    data: &[u8],
    i: &mut usize,
    lamellae: &Arc<Lamellae>,
    ame: &RegisteredActiveMessages,
    executor: &Arc<Executor>,
) {
    let am_header = MyAmHeader::ref_from_bytes(&data[*i..*i + std::mem::size_of::<MyAmHeader>()])
        .expect("failed to parse MyAmHeader");
    *i += std::mem::size_of::<MyAmHeader>();
    let data_len = am_header.data_len.get() as usize;
    let am_data_bytes = &data[*i..*i + data_len];
    *i += data_len;
    let (team, world) = ame.get_team_and_world(src, am_header.team_addr.get() as usize, lamellae);
    let am = {
        let _mrg = crate::memregion::one_sided::MemRegionRecvGuard::new();
        AMS_EXECS.get(&am_header.am_id.get()).unwrap()(am_data_bytes, team.team.team_pe)
    };
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
    trace!(target: "lamellae_debug", "exec_am_zerocopy:  lamellae cnt: {:?}", Arc::strong_count(&lamellae));
    executor.submit_task(async move {
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
                panic!("Should not be returning local data or AM from remote am");
            }
        };
        world.team.world_counters.dec_outstanding(1);
        team.team.team_counters.dec_outstanding(1);
        ame.process_msg(am, 0, false).await;
    });
}

pub(crate) async fn exec_return_am_zerocopy(
    src: usize,
    data: &[u8],
    i: &mut usize,
    lamellae: &Arc<Lamellae>,
    ame: &RegisteredActiveMessages,
) {
    let am_header = MyAmHeader::ref_from_bytes(&data[*i..*i + std::mem::size_of::<MyAmHeader>()])
        .expect("failed to parse MyAmHeader");
    *i += std::mem::size_of::<MyAmHeader>();
    let data_len = am_header.data_len.get() as usize;
    let am_data_bytes = &data[*i..*i + data_len];
    *i += data_len;
    let (team, world) = ame.get_team_and_world(src, am_header.team_addr.get() as usize, lamellae);
    let am = {
        let _mrg = crate::memregion::one_sided::MemRegionRecvGuard::new();
        AMS_EXECS.get(&am_header.am_id.get()).unwrap()(am_data_bytes, team.team.team_pe)
    };
    let req_data = ReqMetaData {
        src,
        dst: Some(team.team.world_pe),
        id: ReqId {
            id: am_header.req_id.get() as usize,
            sub_id: am_header.req_sub_id.get() as usize,
        },
        lamellae: lamellae.clone(),
        world: world.team.clone(),
        team: team.team.clone(),
    };
    ame.clone()
        .exec_local_am(req_data, am.as_local(), world, team)
        .await;
    trace!(target: "lamellae_debug", "finished processing return am in exec_return_am_zerocopy, lamellae cnt: {:?}", Arc::strong_count(&lamellae));
}

pub(crate) fn exec_data_am_zerocopy(
    src: usize,
    data: &[u8],
    i: &mut usize,
    ame: &RegisteredActiveMessages,
) {
    let data_header =
        MyDataHeader::ref_from_bytes(&data[*i..*i + std::mem::size_of::<MyDataHeader>()])
            .expect("failed to parse MyDataHeader");
    *i += std::mem::size_of::<MyDataHeader>();
    let darc_list_size = data_header.darc_list_size.get() as usize;
    let darcs: Vec<RemotePtr> = crate::deserialize(&data[*i..*i + darc_list_size], false).unwrap();
    *i += darc_list_size;
    let data_size = data_header.size.get() as usize;
    let payload = &data[*i..*i + data_size];
    *i += data_size;
    let req_id = ReqId {
        id: data_header.req_id.get() as usize,
        sub_id: data_header.req_sub_id.get() as usize,
    };
    ame.send_data_to_user_handle(
        req_id,
        src,
        InternalResult::NewRemote(payload.to_vec(), darcs),
    );
}

pub(crate) fn exec_unit_am_zerocopy(
    src: usize,
    data: &[u8],
    i: &mut usize,
    ame: &RegisteredActiveMessages,
) {
    let unit_header =
        MyUnitHeader::ref_from_bytes(&data[*i..*i + std::mem::size_of::<MyUnitHeader>()])
            .expect("failed to parse MyUnitHeader");
    *i += std::mem::size_of::<MyUnitHeader>();
    let req_id = ReqId {
        id: unit_header.req_id.get() as usize,
        sub_id: unit_header.req_sub_id.get() as usize,
    };
    ame.send_data_to_user_handle(req_id, src, InternalResult::Unit);
}

// ---------------------------------------------------------------------------
// Serde wire-format exec helpers — used by SimpleBatcher and TeamAmBatcher
// ---------------------------------------------------------------------------

pub(crate) fn exec_am_serde(
    src: usize,
    data: &[u8],
    i: &mut usize,
    lamellae: &Arc<Lamellae>,
    ame: &RegisteredActiveMessages,
    executor: &Arc<Executor>,
) {
    use crate::active_messaging::registered_active_message::{AM_HEADER_LEN, AmHeader};
    let am_header = AmHeader::try_read_from_bytes(&data[*i..*i + AM_HEADER_LEN])
        .expect("failed to parse AmHeader");
    let (team, world) = ame.get_team_and_world(src, am_header.team_addr, lamellae);
    *i += AM_HEADER_LEN;
    let am = {
        let _mrg = crate::memregion::one_sided::MemRegionRecvGuard::new();
        AMS_EXECS.get(&am_header.am_id).unwrap()(
            &data[*i..*i + am_header.data_len],
            team.team.team_pe,
        )
    };
    *i += am_header.data_len;
    let req_data = ReqMetaData {
        src: team.team.world_pe,
        dst: Some(src),
        id: am_header.req_id,
        lamellae: lamellae.clone(),
        world: world.team.clone(),
        team: team.team.clone(),
    };
    let ame = ame.clone();
    world.team.world_counters.inc_outstanding(1);
    team.team.team_counters.inc_outstanding(1);
    trace!(target: "lamellae_debug", "exec_am_serde:  lamellae cnt: {:?}", Arc::strong_count(lamellae));
    executor.submit_task(async move {
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
                panic!("Should not be returning local data or AM from remote am");
            }
        };
        world.team.world_counters.dec_outstanding(1);
        team.team.team_counters.dec_outstanding(1);
        ame.process_msg(am, 0, false).await;
    });
}

pub(crate) async fn exec_return_am_serde(
    src: usize,
    data: &[u8],
    i: &mut usize,
    lamellae: &Arc<Lamellae>,
    ame: &RegisteredActiveMessages,
) {
    use crate::active_messaging::registered_active_message::{AM_HEADER_LEN, AmHeader};
    let am_header = AmHeader::try_read_from_bytes(&data[*i..*i + AM_HEADER_LEN])
        .expect("failed to parse AmHeader");
    let (team, world) = ame.get_team_and_world(src, am_header.team_addr, lamellae);
    *i += AM_HEADER_LEN;
    let am = {
        let _mrg = crate::memregion::one_sided::MemRegionRecvGuard::new();
        AMS_EXECS.get(&am_header.am_id).unwrap()(
            &data[*i..*i + am_header.data_len],
            team.team.team_pe,
        )
    };
    *i += am_header.data_len;
    let req_data = ReqMetaData {
        src,
        dst: Some(team.team.world_pe),
        id: am_header.req_id,
        lamellae: lamellae.clone(),
        world: world.team.clone(),
        team: team.team.clone(),
    };
    ame.clone()
        .exec_local_am(req_data, am.as_local(), world, team)
        .await;
    trace!(target: "lamellae_debug", "finished processing return am in exec_return_am_serde, lamellae cnt: {:?}", Arc::strong_count(&lamellae));
}

// ---------------------------------------------------------------------------
// Serde wire-format send helpers — used by SimpleBatcher and TeamAmBatcher
// ---------------------------------------------------------------------------

async fn create_serde_buf(
    header: SerializeHeader,
    size: usize,
    lamellae: &Arc<Lamellae>,
) -> SerializedData {
    let mut data = lamellae.serialize_header(header, size);
    while let Err(err) = data {
        async_std::task::yield_now().await;
        match err.downcast_ref::<AllocError>() {
            Some(AllocError::OutOfMemoryError(_)) => {
                lamellae.request_new_alloc(size * 2).await;
            }
            _ => panic!("unhandled error: {:?}", err),
        }
        data = lamellae.serialize_header(header, size);
    }
    trace!(target: "lamellae_debug", "create_serde_buf:  lamellae cnt: {:?}, requested size: {}", Arc::strong_count(lamellae), size);
    data.unwrap()
}

pub(crate) async fn send_am_serde(
    req_data: ReqMetaData,
    am: LamellarArcAm,
    am_id: AmId,
    am_bytes: Vec<u8>,
    cmd: Cmd,
) {
    use crate::active_messaging::registered_active_message::{AM_HEADER_LEN, AmHeader};
    let my_pe = req_data.team.world_pe;
    let am_size = am_bytes.len();
    let header = SerializeHeader {
        msg: Msg {
            src: my_pe as u16,
            cmd,
            padding: [0; 1],
        },
    };
    let header_len = AM_HEADER_LEN;
    let mut data_buf = create_serde_buf(header, am_size + header_len, &req_data.lamellae).await;
    let mut data_slice = data_buf.data_as_bytes_mut();
    let am_header = AmHeader {
        am_id,
        req_id: req_data.id,
        team_addr: req_data.team.darc_addr(),
        data_len: am_size,
        _pad: [0; 4],
    };
    data_slice[0..header_len].copy_from_slice(am_header.as_bytes());
    let darc_ser_cnt = match req_data.dst {
        Some(_) => 1,
        None => match req_data.team.team_pe_id() {
            Ok(_) => req_data.team.num_pes() - 1,
            Err(_) => req_data.team.num_pes(),
        },
    };
    let mut darcs = vec![];
    am.ser(darc_ser_cnt, &mut darcs);
    data_slice[header_len..header_len + am_size].copy_from_slice(&am_bytes);
    req_data
        .lamellae
        .send_to_pes_async(req_data.dst, req_data.team.arch.clone(), data_buf)
        .await;
    trace!(target: "lamellae_debug", "send_am_serde:  lamellae cnt: {:?}, am size: {}", Arc::strong_count(&req_data.lamellae), am_size);
}

pub(crate) async fn send_data_am_serde(
    req_data: ReqMetaData,
    darc_bytes: Vec<u8>,
    data_bytes: Vec<u8>,
) {
    use crate::active_messaging::registered_active_message::{DATA_HEADER_LEN, DataHeader};
    let my_pe = req_data.team.world_pe;
    let header = SerializeHeader {
        msg: Msg {
            src: my_pe as u16,
            cmd: Cmd::Data,
            padding: [0; 1],
        },
    };
    let darc_list_size = darc_bytes.len();
    let data_size = data_bytes.len();
    let data_header = DataHeader {
        size: data_size,
        req_id: req_data.id,
        darc_list_size,
    };
    let mut data_buf = create_serde_buf(
        header,
        data_size + darc_list_size + DATA_HEADER_LEN,
        &req_data.lamellae,
    )
    .await;
    let mut data_slice = data_buf.data_as_bytes_mut();
    data_slice[0..DATA_HEADER_LEN].copy_from_slice(data_header.as_bytes());
    let mut i = DATA_HEADER_LEN;
    data_slice[i..i + darc_list_size].copy_from_slice(&darc_bytes);
    i += darc_list_size;
    data_slice[i..i + data_size].copy_from_slice(&data_bytes);
    req_data
        .lamellae
        .send_to_pes_async(req_data.dst, req_data.team.arch.clone(), data_buf)
        .await;
    trace!(target: "lamellae_debug", "send_data_am_serde:  lamellae cnt: {:?}, data size: {}", Arc::strong_count(&req_data.lamellae), data_size);
}

pub(crate) async fn send_unit_am_serde(req_data: ReqMetaData) {
    use crate::active_messaging::registered_active_message::{UNIT_HEADER_LEN, UnitHeader};
    let my_pe = req_data.team.world_pe;
    let header = SerializeHeader {
        msg: Msg {
            src: my_pe as u16,
            cmd: Cmd::Unit,
            padding: [0; 1],
        },
    };
    let mut data_buf = create_serde_buf(header, UNIT_HEADER_LEN, &req_data.lamellae).await;
    let mut data_slice = data_buf.data_as_bytes_mut();
    let unit_header = UnitHeader {
        req_id: req_data.id,
    };
    data_slice[0..UNIT_HEADER_LEN].copy_from_slice(unit_header.as_bytes());
    req_data
        .lamellae
        .send_to_pes_async(req_data.dst, req_data.team.arch.clone(), data_buf)
        .await;
    trace!(target: "lamellae_debug", "send_unit_am_serde:  lamellae cnt: {:?}, sent unit am", Arc::strong_count(&req_data.lamellae));
}

pub(crate) fn exec_data_am_serde(
    src: usize,
    data: &[u8],
    i: &mut usize,
    ame: &RegisteredActiveMessages,
) {
    use crate::active_messaging::registered_active_message::{DATA_HEADER_LEN, DataHeader};
    let data_header = DataHeader::try_read_from_bytes(&data[*i..*i + DATA_HEADER_LEN])
        .expect("failed to parse DataHeader");
    *i += DATA_HEADER_LEN;
    let darcs: Vec<RemotePtr> =
        crate::deserialize(&data[*i..*i + data_header.darc_list_size], false).unwrap();
    *i += data_header.darc_list_size;
    let payload = data[*i..*i + data_header.size].to_vec();
    *i += data_header.size;
    ame.send_data_to_user_handle(
        data_header.req_id,
        src,
        InternalResult::NewRemote(payload, darcs),
    );
}

pub(crate) fn exec_unit_am_serde(
    src: usize,
    data: &[u8],
    i: &mut usize,
    ame: &RegisteredActiveMessages,
) {
    use crate::active_messaging::registered_active_message::{UNIT_HEADER_LEN, UnitHeader};
    let unit_header = UnitHeader::try_read_from_bytes(&data[*i..*i + UNIT_HEADER_LEN])
        .expect("failed to parse UnitHeader");
    *i += UNIT_HEADER_LEN;
    ame.send_data_to_user_handle(unit_header.req_id, src, InternalResult::Unit);
}
