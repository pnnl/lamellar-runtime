// Receive-side dispatch for Cmd::Stream frames (see wire.rs for the record format).
//
// Unlike the legacy batchers, remote AMs are polled once inline (noop waker) before
// falling back to a spawned task, so trivial AMs finish without a task hop. Unit
// replies generated inline are handed to the batcher in one call at frame end.

use std::sync::Arc;
use std::sync::LazyLock;
use std::task::{Context, Poll};

use super::wire::{Record, RecordIter};
use super::Batcher;
use crate::active_messaging::registered_active_message::{RegisteredActiveMessages, AMS_EXECS};
use crate::active_messaging::*;
use crate::lamellae::Lamellae;

/// Max remote AMs polled inline per frame (`LAMELLAR_AM_INLINE`, 0 disables).
static AM_INLINE_BUDGET: LazyLock<usize> = LazyLock::new(|| {
    std::env::var("LAMELLAR_AM_INLINE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(32)
});

/// Per-frame cache of resolved (team, world) handles; frames rarely touch more than a
/// couple of teams, so a linear scan beats hashing.
struct TeamCache(Vec<(usize, Arc<LamellarTeam>, Arc<LamellarTeam>)>);

impl TeamCache {
    #[inline]
    fn get(
        &mut self,
        src: usize,
        team_addr: usize,
        lamellae: &Arc<Lamellae>,
        ame: &RegisteredActiveMessages,
    ) -> (Arc<LamellarTeam>, Arc<LamellarTeam>) {
        if let Some((_, t, w)) = self.0.iter().find(|(a, _, _)| *a == team_addr) {
            return (t.clone(), w.clone());
        }
        let (t, w) = ame.get_team_and_world(src, team_addr, lamellae);
        self.0.push((team_addr, t.clone(), w.clone()));
        (t, w)
    }
}

#[inline]
fn finish_am(ret: LamellarReturn, req_data: ReqMetaData) -> Am {
    match ret {
        LamellarReturn::Unit => Am::Unit(req_data),
        LamellarReturn::RemoteData(data) => Am::Data(req_data, data),
        LamellarReturn::RemoteAm(am) => Am::Return(req_data, am),
        LamellarReturn::LocalData(_) | LamellarReturn::LocalAm(_) => {
            panic!("Should not be returning local data or AM from remote am");
        }
    }
}

pub(crate) async fn exec_stream_frame(
    src: usize,
    data: &[u8],
    lamellae: &Arc<Lamellae>,
    ame: &RegisteredActiveMessages,
) {
    let mut teams = TeamCache(Vec::new());
    let mut units: Vec<ReqMetaData> = Vec::new();
    let mut inline_budget = *AM_INLINE_BUDGET;

    for rec in RecordIter::new(data) {
        match rec {
            Record::Am {
                ret: false,
                am_id,
                req_id,
                team_addr,
                payload,
            } => {
                let (team, world) = teams.get(src, team_addr, lamellae, ame);
                let am = {
                    let _mrg = crate::memregion::one_sided::MemRegionRecvGuard::new();
                    AMS_EXECS.get(&am_id).unwrap()(payload, team.team.team_pe)
                };
                let req_data = ReqMetaData {
                    src: team.team.world_pe,
                    dst: Some(src),
                    id: req_id,
                    lamellae: lamellae.clone(),
                    world: world.team.clone(),
                    team: team.team.clone(),
                };
                world.team.world_counters.inc_outstanding(1);
                team.team.team_counters.inc_outstanding(1);
                let mut fut = am.exec(
                    team.team.world_pe,
                    team.team.num_world_pes,
                    false,
                    world.clone(),
                    team.clone(),
                );
                if inline_budget > 0 {
                    inline_budget -= 1;
                    // Context is !Send: keep it out of scope across the awaits below
                    let polled = fut
                        .as_mut()
                        .poll(&mut Context::from_waker(futures_util::task::noop_waker_ref()));
                    if let Poll::Ready(ret) = polled {
                        world.team.world_counters.dec_outstanding(1);
                        team.team.team_counters.dec_outstanding(1);
                        match finish_am(ret, req_data) {
                            Am::Unit(req_data) => units.push(req_data),
                            am => ame.clone().process_msg(am, 0, false).await,
                        }
                        continue;
                    }
                }
                let ame = ame.clone();
                ame.executor.clone().submit_task(async move {
                    let am = finish_am(fut.await, req_data);
                    world.team.world_counters.dec_outstanding(1);
                    team.team.team_counters.dec_outstanding(1);
                    ame.process_msg(am, 0, false).await;
                });
            }
            Record::Am {
                ret: true,
                am_id,
                req_id,
                team_addr,
                payload,
            } => {
                let (team, world) = teams.get(src, team_addr, lamellae, ame);
                let am = {
                    let _mrg = crate::memregion::one_sided::MemRegionRecvGuard::new();
                    AMS_EXECS.get(&am_id).unwrap()(payload, team.team.team_pe)
                };
                let req_data = ReqMetaData {
                    src,
                    dst: Some(team.team.world_pe),
                    id: req_id,
                    lamellae: lamellae.clone(),
                    world: world.team.clone(),
                    team: team.team.clone(),
                };
                ame.clone()
                    .exec_local_am(req_data, am.as_local(), world, team)
                    .await;
            }
            Record::Data {
                req_id,
                darcs,
                payload,
            } => {
                let darcs: Vec<RemotePtr> = crate::deserialize(darcs, false).unwrap();
                ame.send_data_to_user_handle(
                    req_id,
                    src,
                    InternalResult::NewRemote(payload.to_vec(), darcs),
                );
            }
            Record::Units(it) => {
                for req_id in it {
                    ame.send_data_to_user_handle(req_id, src, InternalResult::Unit);
                }
            }
            Record::Pad => {}
        }
    }
    if !units.is_empty() {
        ame.batcher.add_units_to_batch(units, 0).await;
    }
}
