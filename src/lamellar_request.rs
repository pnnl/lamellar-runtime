use crate::active_messaging::{
    AmHandleInner, DarcSerde, LamellarAny, MultiAmHandleInner, RemotePtr,
};
use crate::darc::Darc;
use crate::lamellae::SerializedData;
use crate::lamellar_task_group::{TaskGroupAmHandleInner, TaskGroupMultiAmHandleInner};
use crate::memregion::one_sided::MemRegionHandleInner;

use std::future::Future;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::Waker;

#[derive(Debug)]
pub(crate) enum InternalResult {
    Local(LamellarAny), // a local result from a local am (possibly a returned one)
    Remote(SerializedData, Vec<RemotePtr>), // a remote result from a remote am
    Unit,
}

//#[doc(hidden)]
// #[enum_dispatch]
pub(crate) trait LamellarRequest: Future {
    fn launch(&mut self);
    fn blocking_wait(self) -> Self::Output;
    fn ready_or_set_waker(&mut self, waker: &Waker) -> bool;
    fn val(&self) -> Self::Output;
}

// //#[doc(hidden)]
// #[async_trait]
// pub trait LamellarMultiRequest: Sync + Send {
//     type Output;
//     async fn into_future(mut self: Box<Self>) -> Vec<Self::Output>;
//     fn blocking_wait(&self) -> Vec<Self::Output>;
// }

pub(crate) trait LamellarRequestAddResult: Sync + Send {
    fn user_held(&self) -> bool;
    fn add_result(&self, pe: usize, sub_id: usize, data: InternalResult);
    fn update_counters(&self, sub_id: usize);
}

pub(crate) enum LamellarRequestResult {
    Am(Arc<AmHandleInner>),
    MultiAm(Arc<MultiAmHandleInner>),
    TgAm(Arc<TaskGroupAmHandleInner>),
    TgMultiAm(Arc<TaskGroupMultiAmHandleInner>),
}
//todo make this an enum instead...
// will need to include the task group requests as well...
// pub(crate) struct LamellarRequestResult {
//     pub(crate) req: Arc<dyn LamellarRequestAddResult>,
// }
impl std::fmt::Debug for LamellarRequestResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Am(_) => write!(f, "Am"),
            Self::MultiAm(_) => write!(f, "MultiAm"),
            Self::TgAm(_) => write!(f, "TgAm"),
            Self::TgMultiAm(_) => write!(f, "TgMultiAm"),
        }
    }
}

impl LamellarRequestResult {
    //#[tracing::instrument(skip_all)]
    pub(crate) fn add_result_inner<T: LamellarRequestAddResult>(
        req: &Arc<T>,
        pe: usize,
        sub_id: usize,
        data: InternalResult,
    ) -> bool {
        let mut added = false;

        if req.user_held() {
            req.add_result(pe as usize, sub_id, data);
            added = true;
        } else {
            // if the user dopped the handle we still need to handle if Darcs are returned
            if let InternalResult::Remote(_, darcs) = data {
                // we need to appropriately set the reference counts if the returned data contains any Darcs
                // we "cheat" in that we don't actually care what the Darc wraps (hence the cast to ()) we just care
                // that the reference count is updated.
                for darc in darcs {
                    match darc {
                        RemotePtr::NetworkDarc(darc) => {
                            let temp: Darc<()> = darc.into();
                            temp.des(Ok(0));
                        }
                        RemotePtr::NetMemRegionHandle(mr) => {
                            let temp: Arc<MemRegionHandleInner> = mr.into();
                            temp.local_ref.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                }
            }
        }
        req.update_counters(sub_id);
        added
    }

    pub(crate) fn add_result(&self, pe: usize, sub_id: usize, data: InternalResult) -> bool {
        match self {
            Self::Am(req) => Self::add_result_inner(req, pe, sub_id, data),
            Self::MultiAm(req) => Self::add_result_inner(req, pe, sub_id, data),
            Self::TgAm(req) => Self::add_result_inner(req, pe, sub_id, data),
            Self::TgMultiAm(req) => Self::add_result_inner(req, pe, sub_id, data),
        }
    }
}
