use lamellar_ucc_sys::*;
use std::{mem::MaybeUninit, os::raw::c_void, sync::{atomic::AtomicUsize, Arc}};
use crate::lamellae::{collective::AllReduceOp, ucx_lamellae_mt::fabric::UcxMtAlloc};

#[derive(Debug)]
pub(crate) struct LibConfig {
    handle: ucc_lib_config_h,
}

impl Default for LibConfig {
    fn default() -> Self {
        let mut handle = MaybeUninit::uninit();
        let status =
            unsafe { ucc_lib_config_read(std::ptr::null_mut(), std::ptr::null_mut(), handle.as_mut_ptr()) };
        Error::from_status(status).unwrap();

        LibConfig {
            handle: unsafe { handle.assume_init() },
        }
    }
}

impl Drop for LibConfig {
    fn drop(&mut self) {
        unsafe { ucc_lib_config_release(self.handle) };
    }
}


#[derive(Debug)]
struct CtxConfig {
    handle: ucc_context_config_h,
}


impl CtxConfig {
    fn new(lib: &UccLib) -> Self {
        let mut handle = MaybeUninit::uninit();
        let status =
            unsafe { ucc_context_config_read(lib.handle, std::ptr::null_mut(), handle.as_mut_ptr()) };
        Error::from_status(status).unwrap();
        CtxConfig {
            handle: unsafe { handle.assume_init() },
        }
    }
}

impl Drop for CtxConfig {
    fn drop(&mut self) {
        unsafe { ucc_context_config_release(self.handle) };
    }
}


#[derive(Debug)]
pub(crate) struct UccLib {
    handle: ucc_lib_h,
}

impl UccLib {
    pub(crate) fn new() -> Self {
        Self::new_with_config(&LibConfig::default())
    }

    pub(crate) fn new_with_config(config: &LibConfig) -> Self {
        let requested_coll_types = ucc_coll_type_t_UCC_COLL_TYPE_ALLGATHER as u64
                                | ucc_coll_type_t_UCC_COLL_TYPE_ALLREDUCE as u64
                                | ucc_coll_type_t_UCC_COLL_TYPE_ALLTOALL as u64
                                | ucc_coll_type_t_UCC_COLL_TYPE_BARRIER as u64
                                | ucc_coll_type_t_UCC_COLL_TYPE_BCAST as u64
                                | ucc_coll_type_t_UCC_COLL_TYPE_GATHER as u64
                                | ucc_coll_type_t_UCC_COLL_TYPE_REDUCE as u64
                                | ucc_coll_type_t_UCC_COLL_TYPE_SCATTER as u64
                                | ucc_coll_type_t_UCC_COLL_TYPE_REDUCE_SCATTER as u64 ;
        
        let requested_reduction_types = ucc_reduction_op_t_UCC_OP_SUM as u64
                                | ucc_reduction_op_t_UCC_OP_PROD as u64
                                | ucc_reduction_op_t_UCC_OP_MIN as u64
                                | ucc_reduction_op_t_UCC_OP_MAX as u64
                                | ucc_reduction_op_t_UCC_OP_LAND as u64
                                | ucc_reduction_op_t_UCC_OP_LOR as u64
                                | ucc_reduction_op_t_UCC_OP_LXOR as u64
                                | ucc_reduction_op_t_UCC_OP_BAND as u64
                                | ucc_reduction_op_t_UCC_OP_BOR as u64
                                | ucc_reduction_op_t_UCC_OP_BXOR as u64
                                | ucc_reduction_op_t_UCC_OP_MAXLOC as u64
                                | ucc_reduction_op_t_UCC_OP_MINLOC as u64;
        let requested_sync_types = ucc_coll_sync_type_t_UCC_SYNC_COLLECTIVES
                                    | ucc_coll_sync_type_t_UCC_NO_SYNC_COLLECTIVES;

        let params = ucc_lib_params_t {
            mask: ucc_lib_params_field_UCC_LIB_PARAM_FIELD_THREAD_MODE as u64
                | ucc_lib_params_field_UCC_LIB_PARAM_FIELD_COLL_TYPES as u64
                | ucc_lib_params_field_UCC_LIB_PARAM_FIELD_REDUCTION_TYPES as u64
                | ucc_lib_params_field_UCC_LIB_PARAM_FIELD_SYNC_TYPE as u64,
            thread_mode: ucc_thread_mode_t_UCC_THREAD_MULTIPLE,
            coll_types: requested_coll_types,
            reduction_types: requested_reduction_types,
            sync_type: requested_sync_types,
        };

        let mut handle = MaybeUninit::uninit();
        let status = unsafe { 
            ucc_init_version(
            UCC_API_MAJOR,
            UCC_API_MINOR,
            &params,
            config.handle,
            handle.as_mut_ptr(),
            )
        };
        assert_eq!(status, ucc_status_t_UCC_OK);
        Self {
            handle: unsafe { handle.assume_init() },
        }
    }
}

impl Drop for UccLib {
    fn drop(&mut self) {
        unsafe { ucc_finalize(self.handle) };
    }
}

pub(crate) struct UccContext {
    _lib: Arc<UccLib>,
    handle: ucc_context_h,
    _params: Box<UccTeamParams>, // pin?
    progress_lock: std::sync::Mutex<()>,
}

unsafe impl Send for UccContext {}
unsafe impl Sync for UccContext {}

impl UccContext {
    pub(crate) fn new(ucc_lib: Arc<UccLib>, ucx_alloc: Arc<UcxMtAlloc>) -> Result<Self, Error> {
        let config = CtxConfig::new(&ucc_lib);
        let mut params = Box::new(UccTeamParams {
            my_pe: ucx_alloc.my_pe,
            pes: (0..ucx_alloc.num_pes).collect(),
            ucx_alloc: Some(ucx_alloc.clone()),
        });
        let ctx_params = ucc_context_params_t {
            mask: (ucc_context_params_field_UCC_CONTEXT_PARAM_FIELD_TYPE
                | ucc_context_params_field_UCC_CONTEXT_PARAM_FIELD_OOB) as u64,
            type_: ucc_context_type_t_UCC_CONTEXT_SHARED,
            sync_type: 0,
            oob: ucc_oob_coll_t {
                allgather: Some(oob_collective),
                req_test: Some(req_test),
                req_free: Some(req_free),
                n_oob_eps: ucx_alloc.num_pes as u32,
                oob_ep: ucx_alloc.my_pe as u32,
                coll_info: &mut *params as *mut UccTeamParams as *mut c_void,
            },
            ctx_id: ucx_alloc.my_pe as u64,
            mem_params: ucc_mem_map_params { 
                segments: std::ptr::null_mut(), 
                n_segments: 0 
            },
        };
        // let oob_info = unsafe {Box::from_raw(ctx_params.oob.coll_info as *mut oob_info)};

        let mut ctx = MaybeUninit::uninit();

        let status = unsafe { ucc_context_create(ucc_lib.handle, &ctx_params, config.handle, ctx.as_mut_ptr()) };
        Error::from_status(status)?;

        Ok(Self {
            _lib: ucc_lib.clone(),
            handle: unsafe { ctx.assume_init() },
            _params: params,
            progress_lock: std::sync::Mutex::new(()),
        })
    }

    pub(crate) fn progress(&self) -> Result<(), Error> {
        let _guard = self.progress_lock.lock().unwrap();
        Error::from_status( unsafe { ucc_context_progress(self.handle) })
    }
}

impl Drop for UccContext {
    fn drop(&mut self) {
        unsafe { ucc_context_destroy(self.handle) };
    }
}


unsafe impl Send for UccTeam {}
unsafe impl Sync for UccTeam {}
pub(crate) struct UccTeam {
    handle: ucc_team_h,
    pub(crate) context: Arc<UccContext>,
    #[allow(dead_code)] // WIP: pinning params required for UCC safety, not yet read directly
    params: Box<UccTeamParams>, // pin?
    pub(crate) req_pending: Arc<AtomicUsize>,
    pub(crate) req_completed: Arc<AtomicUsize>,
}

pub(crate) struct UccTeamParams {
    pub(crate) my_pe: usize,
    pub(crate) pes: Vec<usize>,
    pub(crate) ucx_alloc: Option<Arc<UcxMtAlloc>>,
}

impl UccTeam {
    pub(crate) fn new(my_pe: usize, pes: &[usize], ctx: Arc<UccContext>, ucx_alloc: Arc<UcxMtAlloc>) -> Result<Self, Error> {
        let mut handle: MaybeUninit<ucc_team_h> = MaybeUninit::uninit();
        let team_rank = pes
            .iter()
            .position(|&pe| pe == my_pe)
            .ok_or(Error::InvalidParam)?;
        let mut params = Box::new(UccTeamParams {
            my_pe,
            pes: pes.to_vec(),
            ucx_alloc: Some(ucx_alloc.clone()),
        });
        let ucc_coll_info = ucc_oob_coll_t {
            allgather: Some(oob_collective),    
            req_test: Some(req_test),
            req_free: Some(req_free),
            n_oob_eps: pes.len() as u32,
            oob_ep: team_rank as u32,
            coll_info: &mut *params as *mut UccTeamParams as *mut c_void,
        };

        // let is_contiguous = ucx_alloc.num_pes).windows(2).all(|w| w[1] == w[0] + 1);
        // let range = if is_contiguous {
        //     ucc_ep_range_type_t_UCC_COLLECTIVE_EP_RANGE_CONTIG
        // } else {
        let range=    ucc_ep_range_type_t_UCC_COLLECTIVE_EP_RANGE_NONCONTIG;
        // };

        // let rank_in_team = pes
        //                             .iter()
        //                             .position(|&r| r == my_rank)
        //                             .expect("My rank must be in the list of ranks for the team");


        let team_params = ucc_team_params {
            mask: (ucc_team_params_field_UCC_TEAM_PARAM_FIELD_EP
                | ucc_team_params_field_UCC_TEAM_PARAM_FIELD_TEAM_SIZE
                | ucc_team_params_field_UCC_TEAM_PARAM_FIELD_EP_RANGE
                | ucc_team_params_field_UCC_TEAM_PARAM_FIELD_OOB) as u64,
            flags: 0,
            ordering: 0,
            outstanding_colls: 0,
            ep: ucx_alloc.my_pe as u64,
            ep_list: std::ptr::null_mut(),
            ep_range: range,
            team_size:  ucx_alloc.num_pes as u64,
            sync_type: 0,
            oob: ucc_coll_info,
            p2p_conn: ucc_team_p2p_conn { 
                conn_info_lookup: None, 
                conn_info_release: None, 
                conn_ctx: std::ptr::null_mut(), 
                req_test: None, 
                req_free: None 
            },
            mem_params: ucc_mem_map_params { segments: std::ptr::null_mut(), n_segments: 0 },
            ep_map: ucc_ep_map_t {
                type_: 0,
                ep_num: 0,
                __bindgen_anon_1: ucc_ep_map_t__bindgen_ty_1{
                    array: ucc_ep_map_array{
                        map: std::ptr::null_mut(),
                        elem_size: 0,
                    },
                },
            },
            id: 0,
        };
        let mut contexts = [ctx.handle];
        let status = unsafe {
            ucc_team_create_post(
                contexts.as_mut_ptr(),
                contexts.len() as u32,
                &team_params,
                handle.as_mut_ptr(),
            )
        };
        Error::from_status(status)?;
        let handle = unsafe { handle.assume_init() };
        assert_ne!(handle, std::ptr::null_mut());
        loop {
            ctx.progress()?;
            let team_status = unsafe { ucc_team_create_test(handle) };
            if team_status == ucc_status_t_UCC_INPROGRESS {
                continue;
            }
            Error::from_status(team_status)?;
            break;
        }
        Ok(Self {
            handle,
            context: ctx.clone(),
            params,
            req_pending: Arc::new(AtomicUsize::new(0)),
            req_completed: Arc::new(AtomicUsize::new(0)),
        })
    }

    // Creates a sub-team using the context's inherited OOB (no per-team OOB needed).
    // `pes` is the ordered list of global PE ids in the sub-team.
    pub(crate) fn new_sub_team(my_pe: usize, pes: &[usize], ctx: Arc<UccContext>) -> Result<Self, Error> {
        let mut handle: MaybeUninit<ucc_team_h> = MaybeUninit::uninit();

        // The map must stay alive until ucc_team_create_test returns.
        let global_ranks: Vec<u64> = pes.iter().map(|&p| p as u64).collect();

        let team_rank = pes.iter().position(|&p| p == my_pe).ok_or(Error::InvalidParam)?;

        // Stable team ID derived from sorted PE list — consistent across all team members.
        // Avoids UCC's service-team allreduce for ID allocation (which requires all world PEs).
        // UCC_TEAM_ID_MAX = 0x7FFF; use lower 14 bits (nonzero) to avoid reserved values.
        let team_id: u64 = {
            let mut h: u64 = 0xcbf29ce484222325; // FNV-1a offset basis
            for &pe in pes.iter() {
                h ^= pe as u64;
                h = h.wrapping_mul(0x100000001b3);
            }
            let id = h & 0x3FFF; // 14 bits
            if id == 0 { 1 } else { id }
        };

        let team_params = ucc_team_params {
            mask: (ucc_team_params_field_UCC_TEAM_PARAM_FIELD_EP
                | ucc_team_params_field_UCC_TEAM_PARAM_FIELD_EP_MAP
                | ucc_team_params_field_UCC_TEAM_PARAM_FIELD_EP_RANGE
                | ucc_team_params_field_UCC_TEAM_PARAM_FIELD_TEAM_SIZE
                | ucc_team_params_field_UCC_TEAM_PARAM_FIELD_ID) as u64,
            flags: 0,
            ordering: 0,
            outstanding_colls: 0,
            // CONTIG ep_range: UCC derives team_rank directly from ep (team-local rank).
            // The ep_map then maps team-local rank -> global endpoint for transport.
            ep: team_rank as u64,
            ep_list: std::ptr::null_mut(),
            ep_range: ucc_ep_range_type_t_UCC_COLLECTIVE_EP_RANGE_CONTIG,
            team_size: pes.len() as u64,
            sync_type: 0,
            oob: ucc_oob_coll_t {
                allgather: None,
                req_test: None,
                req_free: None,
                n_oob_eps: 0,
                oob_ep: 0,
                coll_info: std::ptr::null_mut(),
            },
            p2p_conn: ucc_team_p2p_conn {
                conn_info_lookup: None,
                conn_info_release: None,
                conn_ctx: std::ptr::null_mut(),
                req_test: None,
                req_free: None,
            },
            mem_params: ucc_mem_map_params { segments: std::ptr::null_mut(), n_segments: 0 },
            ep_map: ucc_ep_map_t {
                type_: ucc_ep_map_type_t_UCC_EP_MAP_ARRAY,
                ep_num: pes.len() as u64,
                __bindgen_anon_1: ucc_ep_map_t__bindgen_ty_1 {
                    array: ucc_ep_map_array {
                        map: global_ranks.as_ptr() as *mut c_void,
                        elem_size: std::mem::size_of::<u64>(),
                    },
                },
            },
            id: team_id,
        };

        let mut contexts = [ctx.handle];
        let status = unsafe {
            ucc_team_create_post(
                contexts.as_mut_ptr(),
                contexts.len() as u32,
                &team_params,
                handle.as_mut_ptr(),
            )
        };
        Error::from_status(status)?;
        let handle = unsafe { handle.assume_init() };
        assert_ne!(handle, std::ptr::null_mut());
        loop {
            ctx.progress()?;
            let team_status = unsafe { ucc_team_create_test(handle) };
            if team_status == ucc_status_t_UCC_INPROGRESS {
                continue;
            }
            Error::from_status(team_status)?;
            break;
        }
        // global_ranks kept alive through the create_test loop above; safe to drop now.
        drop(global_ranks);

        let params = Box::new(UccTeamParams {
            my_pe,
            pes: pes.to_vec(),
            ucx_alloc: None, // OOB is at context level; no per-team ucx_alloc needed
        });
        Ok(Self {
            handle,
            context: ctx.clone(),
            params,
            req_pending: Arc::new(AtomicUsize::new(0)),
            req_completed: Arc::new(AtomicUsize::new(0)),
        })
    }
}

fn reduce_op_to_ucc_op(op: AllReduceOp) -> ucc_reduction_op_t {
    match op {
        AllReduceOp::Sum => ucc_reduction_op_t_UCC_OP_SUM,
        AllReduceOp::Prod => ucc_reduction_op_t_UCC_OP_PROD,
        AllReduceOp::Min => ucc_reduction_op_t_UCC_OP_MIN,
        AllReduceOp::Max => ucc_reduction_op_t_UCC_OP_MAX,
        // AllReduceOp::Land => ucc_reduction_op_t_UCC_OP_LAND,
        // AllReduceOp::Lor => ucc_reduction_op_t_UCC_OP_LOR,
        // AllReduceOp::Lxor => ucc_reduction_op_t_UCC_OP_LXOR,
        AllReduceOp::BitAnd => ucc_reduction_op_t_UCC_OP_BAND,
        AllReduceOp::BitOr => ucc_reduction_op_t_UCC_OP_BOR,
        AllReduceOp::BitXor => ucc_reduction_op_t_UCC_OP_BXOR,
    }
}

fn rust_type_to_ucc_dtype<T: 'static>() -> ucc_datatype_t {
    if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i8>() {
        UCC_DT_INT8
    } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u8>() {
        UCC_DT_UINT8
    } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i16>() {
        UCC_DT_INT16
    } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u16>() {
        UCC_DT_UINT16
    } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i32>() {
        UCC_DT_INT32
    } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u32>() {
        UCC_DT_UINT32
    } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i64>() {
        UCC_DT_INT64
    } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u64>() {
        UCC_DT_UINT64
    } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<i128>() {
        UCC_DT_INT128
    } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<u128>() {
        UCC_DT_UINT128
    } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<isize>() {
        if cfg!(target_pointer_width = "64") {
            UCC_DT_INT64
        } else {
            UCC_DT_INT32
        }
    } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<usize>() {
        if cfg!(target_pointer_width = "64") {
            UCC_DT_UINT64
        } else {
            UCC_DT_UINT32
        }
    } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<f32>() {
        UCC_DT_FLOAT32
    } else if std::any::TypeId::of::<T>() == std::any::TypeId::of::<f64>() {
        UCC_DT_FLOAT64
    } else {
        panic!("Unsupported type");
    }
}

unsafe impl Send for UccRequest {}
unsafe impl Sync for UccRequest {}

pub(crate) struct UccRequest {
    req_handle: ucc_coll_req_h,
    req_completed: Arc<AtomicUsize>,
}

impl UccRequest {
    pub(crate) fn new(req_handle: ucc_coll_req_h, req_completed: Arc<AtomicUsize>) -> Self {
        Self { 
            req_handle,
            req_completed,
        }
    }

    pub(crate) fn test(&self) -> Result<(), Error> {
        Error::from_status(unsafe { ucc_collective_test_wrapper(self.req_handle) })?;
        self.req_completed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

impl Drop for UccRequest {
    fn drop(&mut self) {
        unsafe { ucc_collective_finalize(self.req_handle) };
    }
}

fn generate_coll_args<T: 'static>(buff: &[T], res: &mut [T], coll_type: ucc_coll_type_t, root: Option<usize>, op: Option<AllReduceOp>) -> ucc_coll_args_t {
    let op = if let Some(op) = op {
        reduce_op_to_ucc_op(op)
    }else {
        0
    };
    ucc_coll_args_t {
        mask: 0,
        flags: 0,
        coll_type,
        src: ucc_coll_args__bindgen_ty_1 {
            info: ucc_coll_buffer_info_t {
                buffer: buff.as_ptr() as *const ::std::os::raw::c_void as *mut ::std::os::raw::c_void,
                datatype: rust_type_to_ucc_dtype::<T>(),
                mem_type: ucc_memory_type_UCC_MEMORY_TYPE_HOST,
                count: buff.len() as u64,
            }
        },
        dst: ucc_coll_args__bindgen_ty_2 {
            info: ucc_coll_buffer_info_t {
                buffer: res.as_mut_ptr().cast(),
                datatype: rust_type_to_ucc_dtype::<T>(),
                mem_type: ucc_memory_type_UCC_MEMORY_TYPE_HOST,
                count: res.len() as u64,
            }
        },
        op,
        tag: 0,
        root: root.unwrap_or(0) as u64,
        error_type: 0,
        global_work_buffer: std::ptr::null_mut(),
        cb: ucc_coll_callback { 
            cb: None, 
            data: std::ptr::null_mut() 
        },
        timeout: 0.0,
        active_set: ucc_coll_args__bindgen_ty_3 {
            start: 0,
            stride: 0,
            size: 0,
        },
        src_memh: ucc_coll_args__bindgen_ty_4 { 
            local_memh: std::ptr::null_mut() 
        },
        dst_memh: ucc_coll_args__bindgen_ty_5 { 
            local_memh: std::ptr::null_mut() 
        },
    }
}

impl UccTeam {

    fn post_coll_req(&self, mut coll_args: ucc_coll_args_t) -> Result<UccRequest, Error> {
        let mut coll_req= MaybeUninit::uninit();
        let err = unsafe {ucc_collective_init(&mut coll_args, coll_req.as_mut_ptr(), self.handle)};
        let coll_req = unsafe { coll_req.assume_init() };
        self.req_pending.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let req = UccRequest::new(coll_req, self.req_completed.clone());
        Error::from_status(err)?; // we check here to make sure the request will be freed even if init failed.
        let err = unsafe {ucc_collective_post(req.req_handle)};
        Error::from_status(err)?;
        Ok(req)
    }

    pub(crate) fn allreduce<T: 'static>(
        &self,
        buff: &[T],
        res: &mut [T],
        op: AllReduceOp,
    ) -> Result<UccRequest, Error> {
        let allreduce_args = generate_coll_args::<T>(buff, res, ucc_coll_type_t_UCC_COLL_TYPE_ALLREDUCE, None, Some(op));
        self.post_coll_req(allreduce_args)
    }

    pub(crate) fn allgather<T: 'static>(
        &self,
        buff: &[T],
        res: &mut [T],
    ) -> Result<UccRequest, Error> {
        
        let allgather_args = generate_coll_args::<T>(buff, res, ucc_coll_type_t_UCC_COLL_TYPE_ALLGATHER, None, None);
        self.post_coll_req(allgather_args)
    }

    pub(crate) fn alltoall<T: 'static>(
        &self,
        buff: &[T],
        res: &mut [T],
    ) -> Result<UccRequest, Error> {
        
        let alltoall_args = generate_coll_args::<T>(buff, res, ucc_coll_type_t_UCC_COLL_TYPE_ALLTOALL, None, None);
        self.post_coll_req(alltoall_args)
    }

    // pub(crate) fn barrier(&self) -> Result<UccRequest, Error> {
    //     let barrier_args = generate_coll_args::<i32>(&[], &mut [], ucc_coll_type_t_UCC_COLL_TYPE_BARRIER, None, None);
    //     self.post_coll_req(barrier_args)
    // }

    pub(crate) fn broadcast<T: 'static>(
        &self,
        buff: &[T],
        res: &mut [T],
        root: usize,
    ) -> Result<UccRequest, Error> {
        
        let bcast_args = generate_coll_args::<T>(buff, res, ucc_coll_type_t_UCC_COLL_TYPE_BCAST, Some(root), None);
        self.post_coll_req(bcast_args)
    }

    pub(crate) fn reduce<T: 'static>(
        &self,
        buff: &[T],
        res: &mut [T],
        root: usize,
        op: AllReduceOp,
    ) -> Result<UccRequest, Error> {
        
        let reduce_args = generate_coll_args::<T>(buff, res, ucc_coll_type_t_UCC_COLL_TYPE_REDUCE, Some(root), Some(op));
        self.post_coll_req(reduce_args)
    }

    pub(crate) fn gather<T: 'static>(
        &self,
        buff: &[T],
        res: &mut [T],
        root: usize,
    ) -> Result<UccRequest, Error> {
        let gather_args = generate_coll_args::<T>(buff, res, ucc_coll_type_t_UCC_COLL_TYPE_GATHER, Some(root), None);
        self.post_coll_req(gather_args)
    }

    pub(crate) fn scatter<T: 'static>(
        &self,
        buff: &[T],
        res: &mut [T],
        root: usize,
    ) -> Result<UccRequest, Error> {
        let scatter_args = generate_coll_args::<T>(buff, res, ucc_coll_type_t_UCC_COLL_TYPE_SCATTER, Some(root), None);
        self.post_coll_req(scatter_args)
    }

    pub(crate) fn reduce_scatter<T: 'static>(
        &self,
        buff: &[T],
        res: &mut [T],
        op: AllReduceOp,
    ) -> Result<UccRequest, Error> {
        let reduce_scatter_args = generate_coll_args::<T>(buff, res, ucc_coll_type_t_UCC_COLL_TYPE_REDUCE_SCATTER, None, Some(op));
        self.post_coll_req(reduce_scatter_args)
    }
}

impl Drop for UccTeam {
    fn drop(&mut self) {
        unsafe { ucc_team_destroy(self.handle) };
    }
}


unsafe extern "C" fn oob_collective(
    src_buf: *mut ::std::os::raw::c_void,
    recv_buf: *mut ::std::os::raw::c_void,
    size: usize,
    allgather_info: *mut ::std::os::raw::c_void,
    _request: *mut *mut ::std::os::raw::c_void,
) -> ucc_status_t {
    let params = unsafe { &*(allgather_info as *const UccTeamParams) };
    let my_team_idx = match params.pes.iter().position(|&pe| pe == params.my_pe) {
        Some(idx) => idx,
        None => return ucc_status_t_UCC_ERR_INVALID_PARAM,
    };
    // println!("[{}] size: {}", params.my_pe, size);
    
    let dst_addr = unsafe { std::slice::from_raw_parts_mut(recv_buf as *mut u8, size * params.pes.len()) };
    let src_buf = unsafe { std::slice::from_raw_parts(src_buf as *const u8, size) };
    // println!("[{}] Source bufer: len:{}, data: {:x?}", params.my_pe, src_buf.len(), src_buf);

    if my_team_idx == 0 {
        let alloc = params.ucx_alloc.as_ref().unwrap();
        for i in 1..params.pes.len() {
            // println!("PE[{}]: Waiting for PE: {}", params.my_pe, i);
            while alloc.as_mut_slice::<u8>()[i] == u8::MAX {
                alloc.wait();
                std::thread::yield_now();
            }
            // println!("PE[{}]: Done Waiting for PE: {}", params.my_pe, i);
        }
        // println!("PE[{}]: Putting data to self", params.my_pe);
        alloc.put_inner(params.my_pe, 1, src_buf, false, false);
        // println!("PE[{}]: Done Putting data to self", params.my_pe);
        // alloc.wait_all();

        for (i, pe) in params.pes.iter().enumerate() {
            // println!("PE[{}]: Getting from PE: {}", params.my_pe, i);

            alloc.inner_get(*pe, 1, true, &mut dst_addr[i*size..(i+1)*size]);
            // println!("PE[{}]: Done getting from PE: {}", params.my_pe, i);
        }
        // alloc.wait_all();

        let one = [1u8];
        for (i, pe) in params.pes.iter().skip(1).enumerate() {
            alloc.as_mut_slice::<u8>()[i+1] = u8::MAX;
            // println!("PE[{}]: Putting data to PE: {}", params.my_pe, pe);
            // must complete before the "done" flag put below, else remote may
            // read the flag and consume stale data (races at high PE counts)
            alloc.put_inner(*pe, 1, &dst_addr, true, false);
            // println!("PE[{}]: Done data Putting  to PE: {}", params.my_pe, pe);
        }
        // alloc.wait_all();

        for pe in params.pes.iter().skip(1) {
            // println!("PE[{}]: Putting done to PE: {}", params.my_pe, pe);
            alloc.put_inner(*pe, 0, &one, false, false);
            // println!("PE[{}]: Done Putting done to PE: {}", params.my_pe, pe);
        }
        // alloc.wait_all();
    }
    else {
        let alloc = params.ucx_alloc.as_ref().unwrap();
        // println!("PE[{}]: Putting data to self", params.my_pe);
        alloc.as_mut_slice::<u8>()[1..src_buf.len()+1].copy_from_slice(src_buf);
        // println!("PE[{}]: Done Putting data to self", params.my_pe);
        let one = [1u8];
        // println!("PE[{}]: Putting data to Root", params.my_pe);
        alloc.put_inner(params.pes[0], my_team_idx, &one, false, false);
        // println!("PE[{}]: Done Putting data to Root", params.my_pe);
        // println!("PE[{}]: Waiting data from root", params.my_pe);
        while alloc.as_mut_slice::<u8>()[0] == u8::MAX {
            alloc.wait();
            std::thread::yield_now();
        }
        // println!("PE[{}]: Done waiting data from root", params.my_pe);
        alloc.as_mut_slice::<u8>()[0] = u8::MAX;
        dst_addr.copy_from_slice(&alloc.as_mut_slice::<u8>()[1..(1+size*params.pes.len())]);
    }
    // println!("PE[{}] Completed allgather in oob_collective", params.my_pe);
    // println!("[{}]recv_buf: len:{}, data: {:x?}", params.my_pe, dst_addr.len(), dst_addr);

    return ucc_status_t_UCC_OK;
}

unsafe extern "C" fn req_test(
    _request: *mut ::std::os::raw::c_void,
) -> ucc_status_t {
    return ucc_status_t_UCC_OK;
}

unsafe extern "C" fn req_free(
    _request: *mut ::std::os::raw::c_void,
) -> ucc_status_t {
    return ucc_status_t_UCC_OK;
}



#[allow(missing_docs)]
#[repr(i8)]
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub(crate) enum Error {
    #[error("Operation in progress")]
    Inprogress,
    #[error("Operation initialized")]
    Initialized,
    #[error("Not supported")]
    NotSupported,
    #[error("Function not implemented")]
    NotImplemented,
    #[error("Invalid parameter")]
    InvalidParam,
    #[error("Out of memory")]
    NoMemory,
    #[error("Out of resources")]
    NoResource,
    #[error("No messages")]
    NoMessage,
    #[error("Not found")]
    NotFound,
    #[error("Operation timed out")]
    Timeout,
    #[error("IO error")]
    IoError,
    #[error("Unknown error")]
    Unknown,
}

impl Error {
    #[allow(non_upper_case_globals)]
    pub(crate) fn from_error(status: ucc_status_t) -> Self {
        debug_assert_ne!(status, ucc_status_t_UCC_OK);
        match status {
            ucc_status_t_UCC_INPROGRESS => Self::Inprogress,
            ucc_status_t_UCC_OPERATION_INITIALIZED => Self::Initialized,
            ucc_status_t_UCC_ERR_NOT_SUPPORTED => Self::NotSupported,
            ucc_status_t_UCC_ERR_NOT_IMPLEMENTED => Self::NotImplemented,
            ucc_status_t_UCC_ERR_INVALID_PARAM => Self::InvalidParam,
            ucc_status_t_UCC_ERR_NO_MEMORY => Self::NoMemory,
            ucc_status_t_UCC_ERR_NO_RESOURCE => Self::NoResource,
            ucc_status_t_UCC_ERR_NO_MESSAGE => Self::NoMessage,
            ucc_status_t_UCC_ERR_NOT_FOUND => Self::NotFound,
            ucc_status_t_UCC_ERR_TIMED_OUT => Self::Timeout,
            ucc_status_t_UCC_ERR_IO_ERROR => Self::IoError,
            _ => Self::Unknown,
        }
    }

    pub(crate) fn from_status(status: ucc_status_t) -> Result<(), Self> {
        if status == ucc_status_t_UCC_OK {
            Ok(())
        } else {
            Err(Self::from_error(status))
        }
    }
}