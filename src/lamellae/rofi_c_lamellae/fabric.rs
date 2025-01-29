extern crate libc;

use crate::lamellae::{AllocError,AllocResult,RdmaError,RdmaResult,AllocationType};

use std::any::type_name;
use std::ffi::CString;
use std::os::raw::c_ulong;
use tracing::error;

pub(crate) fn rofi_c_init(provider: &str, domain: &str) -> Result<(), &'static str> {
    let prov_str = CString::new(provider).unwrap();
    let domain_str = CString::new(domain).unwrap();
    let retval = unsafe {
        rofisys::rofi_init(prov_str.as_ptr() as *mut _, domain_str.as_ptr() as *mut _) as i32
    };
    if retval == 0 {
        Ok(())
    } else {
        Err("unable to initialize rofi_c")
    }
}

//currently shows unused warning as we are debugging a hang in rofi_c_finit

pub(crate) fn rofi_c_finit() -> Result<(), &'static str> {
    let retval = unsafe { rofisys::rofi_finit() as i32 };
    if retval == 0 {
        Ok(())
    } else {
        Err("unable to finit rofi_c")
    }
}

pub(crate) fn rofi_c_get_size() -> usize {
    unsafe { rofisys::rofi_get_size() as usize }
}

pub(crate) fn rofi_c_get_id() -> usize {
    unsafe { rofisys::rofi_get_id() as usize }
}

pub(crate) fn rofi_c_barrier() {
    unsafe { rofisys::rofi_barrier() };
}

pub(crate) fn rofi_c_alloc(size: usize, alloc: AllocationType) -> AllocResult<*mut u8> {
    let mut base_ptr: *mut u8 = std::ptr::null_mut();
    let base_ptr_ptr = (&mut base_ptr as *mut _) as *mut *mut std::ffi::c_void;
    // println!("rofi_c_alloc");
    unsafe {
        let ret = match alloc {
            AllocationType::Sub(pes) => rofisys::rofi_sub_alloc(
                size,
                0x0,
                base_ptr_ptr,
                pes.as_ptr() as *mut _,
                pes.len() as u64,
            ),
            AllocationType::Global => rofisys::rofi_alloc(size, 0x0, base_ptr_ptr),
            _ => return Err(AllocError::UnexpectedAllocationType(alloc)),
        };

        if ret != 0 {
           return Err(AllocError::FabricAllocationError(ret));
        }
    }
    //println!("[{:?}] ({:?}:{:?}) rofi_c_alloc addr: {:x} size {:?}",rofi_c_get_id(),file!(),line!(),base_ptr as usize, size);

    Ok(base_ptr)
}

#[allow(dead_code)]
pub(crate) fn rofi_c_release(addr: usize) {
    let base_ptr = addr as *mut u8;
    // println!("rofi_c_release");
    unsafe {
        if rofisys::rofi_release(base_ptr as *mut std::ffi::c_void) != 0 {
            panic!("unable to release memory region");
        }
    }
    //println!("[{:?}] ({:?}:{:?}) rofi_c_release addr: {:x} ",rofi_c_get_id(),file!(),line!(),base_ptr as usize);
}

pub(crate) fn rofi_c_local_addr(remote_pe: usize, remote_addr: usize) -> AllocResult<usize> {
    let addr = unsafe {
        // println!("{:x} {:?} {:?} {:?}",remote_addr,(remote_addr as *mut u8) as *mut std::ffi::c_void,remote_pe,remote_pe as u32);
        rofisys::rofi_get_local_addr_from_remote_addr(
            (remote_addr as *mut u8) as *mut std::ffi::c_void,
            remote_pe as u32,
        ) as usize
    };

    if addr == 0 {
        error!("remote_pe: {remote_pe:?} {remote_addr:x}");
        Err(AllocError::LocalNotFound(remote_addr.into()))
    }
    else{
        Ok(addr)
    }
}

pub(crate) fn rofi_c_remote_addr(pe: usize, local_addr: usize) ->  AllocResult<usize>  {
    let addr = unsafe {
        // println!("{:x} {:?} {:?} {:?}",local_addr,(local_addr as *mut u8) as *mut std::ffi::c_void,pe,pe as u32);
        rofisys::rofi_get_remote_addr((local_addr as *mut u8) as *mut std::ffi::c_void, pe as u32) as usize
    };
    // println!("remote addr {:?} 0x{:x}", addr as *mut u8 ,addr as usize);
    if addr == 0 {
        error!("unable to locate local memory addr");
        Err(AllocError::RemoteNotFound(local_addr.into()))
    }
    else{
        Ok(addr)
    }
}

pub(crate) fn rofi_c_flush() -> i32 {
    unsafe { rofisys::rofi_flush() as i32 }
}

pub(crate) fn rofi_c_wait() -> i32 {
    unsafe { rofisys::rofi_wait() }
}

// data is a reference, user must ensure lifetime is valid until underlying put is complete, thus is unsafe
pub(crate) unsafe fn rofi_c_put<T>(src: &[T], dst: usize, pe: usize) ->  RdmaResult {
    let src_addr = src.as_ptr() as *mut std::ffi::c_void;
    let size = src.len() * std::mem::size_of::<T>();

    let mut ret = rofisys::rofi_put(dst as *mut std::ffi::c_void, src_addr, size, pe as u32, 0); 
                                                                                                 //FI_EAGAIN should this be handled here, at c-rofi_c layer, or application layer?

    while ret == -11 {
        std::thread::yield_now();
        ret = rofisys::rofi_put(dst as *mut std::ffi::c_void, src_addr, size, pe as u32, 0);
        // println!("[{:?}] ({:?}:{:?}) rofi_c_put src_addr {:?} dst_addr 0x{:x} {:?}",rofi_c_get_id(),file!(),line!(),src.as_ptr(),dst,ret);
    }
    if ret == 0 {
        Ok(())
    } else {
        Err(RdmaError::FabricPutError(ret))
    }
}

#[allow(dead_code)]
pub(crate) fn rofi_c_iput<T>(src: &[T], dst: usize, pe: usize) -> RdmaResult {
    let src_addr = src.as_ptr() as *mut std::ffi::c_void;
    let size = src.len() * std::mem::size_of::<T>();
    let mut ret =
        unsafe { rofisys::rofi_iput(dst as *mut std::ffi::c_void, src_addr, size, pe as u32, 0) };
    //FI_EAGAIN should this be handled here, at c-rofi_c layer, or application layer?
    // println!("putting {:?} to {:?} @ {:x}",src,pe,dst);
    while ret == -11 {
        std::thread::yield_now();
        ret = unsafe {
            rofisys::rofi_iput(dst as *mut std::ffi::c_void, src_addr, size, pe as u32, 0)
        };
        //println!("[{:?}] ({:?}:{:?}) rofi_c_iput src_addr {:?} dst_addr 0x{:x} pe {:?} {:?}",rofi_c_get_id(),file!(),line!(),src_addr,dst,pe,ret);
    }
    if ret == 0 {
        Ok(())
    } else {
        Err(RdmaError::FabricPutError(ret))
    }
}

#[allow(dead_code)]
pub(crate) unsafe fn rofi_c_get<T>(src: usize, dst: &mut [T], pe: usize) -> RdmaResult {
    let src_addr = src as *mut std::ffi::c_void;
    let dst_addr = dst.as_ptr() as *mut std::ffi::c_void;
    let size = dst.len() * std::mem::size_of::<T>();
    let mut ret = rofisys::rofi_get(dst_addr, src_addr, size, pe as u32, 0); 
    while ret == -11 {
        std::thread::yield_now();
        ret = rofisys::rofi_get(dst_addr, src_addr, size, pe as u32, 0); 
                                                                         //println!("[{:?}] ({:?}:{:?}) rofi_c_get src_addr {:?} dst_addr{:?} pe {:?} {:?}",rofi_c_get_id(),file!(),line!(),src_addr, dst_addr,pe, ret);
    }
    if ret == 0 {
        Ok(())
    } else {
        Err(RdmaError::FabricGetError(ret))
    }
}

#[allow(dead_code)]
pub(crate) fn rofi_c_iget<T>(src: usize, dst: &mut [T], pe: usize) -> RdmaResult {
    let src_addr = src as *mut std::ffi::c_void;
    let dst_addr = dst.as_ptr() as *mut std::ffi::c_void;
    let size = dst.len() * std::mem::size_of::<T>();
    // println!(
    //     "[{:?}] ({:?}{:?}) rofi_c_iget src: {:x} dst: {:?} pe: {:?} size: {:?}",
    //     rofi_c_get_id(),
    //     file!(),
    //     line!(),
    //     src,
    //     dst.as_ptr(),
    //     pe,
    //     size
    // );
    let mut ret = unsafe { rofisys::rofi_iget(dst_addr, src_addr, size, pe as u32, 0) }; //, &mut txid) };
    while ret == -11 {
        std::thread::yield_now();
        ret = unsafe { rofisys::rofi_iget(dst_addr, src_addr, size, pe as u32, 0) };
        // println!("[{:?}] ({:?}:{:?}) rofi_c_get src_addr {:?} dst_addr{:?} pe {:?} {:?}",rofi_c_get_id(),file!(),line!(),src_addr, dst_addr,pe, ret);
    }
    if ret == 0 {
        Ok(())
    } else {
        Err(RdmaError::FabricGetError(ret))
    }
}

// fn get_rofi_c_dt<T>() -> Option<rofisys::rofi_datatype> {
//     let dt = type_name::<T>();
//     // println!("T type name:  {} {}",dt, type_name::<usize>());
//     if dt == type_name::<u8>() {
//         Some(rofisys::rofi_datatype_ROFI_U8)
//     } else if dt == type_name::<u16>() {
//         Some(rofisys::rofi_datatype_ROFI_U16)
//     } else if dt == type_name::<u32>() {
//         Some(rofisys::rofi_datatype_ROFI_U32)
//     } else if dt == type_name::<u64>() {
//         Some(rofisys::rofi_datatype_ROFI_U64)
//     } else if dt == type_name::<usize>() {
//         Some(rofisys::rofi_datatype_ROFI_U64)
//     } else if dt == type_name::<i8>() {
//         Some(rofisys::rofi_datatype_ROFI_I8)
//     } else if dt == type_name::<i16>() {
//         Some(rofisys::rofi_datatype_ROFI_I16)
//     } else if dt == type_name::<i32>() {
//         Some(rofisys::rofi_datatype_ROFI_I32)
//     } else if dt == type_name::<i64>() {
//         Some(rofisys::rofi_datatype_ROFI_I64)
//     } else if dt == type_name::<isize>() {
//         Some(rofisys::rofi_datatype_ROFI_I64)
//     } else {
//         None
//     }
// }

// pub(crate) fn rofi_c_atomic_avail<T>() -> bool {
//     get_rofi_c_dt::<T>().is_some()
// }

// pub(crate) fn rofi_c_atomic_store<T>(local: &[T], remote: usize, pe: usize) -> Result<c_ulong, i32> {
//     let local_addr = local.as_ptr() as *mut std::ffi::c_void;
//     let remote_addr = remote as *mut std::ffi::c_void;
//     let size = local.len();
//     let txid: c_ulong = 0;
//     let dt = get_rofi_c_dt::<T>().expect("type should be atomic");

//     let mut ret = unsafe {
//         rofisys::rofi_atomic(
//             rofisys::rofi_atomic_op_ROFI_STORE,
//             dt,
//             local_addr,
//             remote_addr,
//             size,
//             pe as u32,
//             0,
//         )
//     }; 
//     while ret == -11 {
//         std::thread::yield_now();
//         ret = unsafe {
//             rofisys::rofi_atomic(
//                 rofisys::rofi_atomic_op_ROFI_STORE,
//                 dt,
//                 local_addr,
//                 remote_addr,
//                 size,
//                 pe as u32,
//                 0,
//             )
//         };
//     }
//     if ret == 0 {
//         Ok(txid)
//     } else {
//         Err(ret)
//     }
// }

// pub(crate) fn rofi_c_atomic_load<T>(
//     result: &mut [T],
//     remote: usize,
//     pe: usize,
// ) -> Result<c_ulong, i32> {
//     let result_addr = result.as_ptr() as *mut std::ffi::c_void;
//     let remote_addr = remote as *mut std::ffi::c_void;
//     let size = result.len();
//     let txid: c_ulong = 0;
//     let dt = get_rofi_c_dt::<T>().expect("type should be atomic");

//     let mut ret = unsafe {
//         rofisys::rofi_atomic_fetch(
//             rofisys::rofi_atomic_op_ROFI_LOAD,
//             dt,
//             std::ptr::null_mut(),
//             remote_addr,
//             result_addr,
//             size,
//             pe as u32,
//             0,
//         )
//     }; 
//     while ret == -11 {
//         std::thread::yield_now();
//         ret = unsafe {
//             rofisys::rofi_atomic_fetch(
//                 rofisys::rofi_atomic_op_ROFI_LOAD,
//                 dt,
//                 std::ptr::null_mut(),
//                 remote_addr,
//                 result_addr,
//                 size,
//                 pe as u32,
//                 0,
//             )
//         }; 
//     }
//     if ret == 0 {
//         Ok(txid)
//     } else {
//         Err(ret)
//     }
// }

// pub(crate) fn rofi_c_atomic_swap<T>(
//     operand: &[T],
//     result: &mut [T],
//     remote: usize,
//     pe: usize,
// ) -> Result<c_ulong, i32> {
//     let operand_addr = operand.as_ptr() as *mut std::ffi::c_void;
//     let result_addr = result.as_ptr() as *mut std::ffi::c_void;
//     let remote_addr = remote as *mut std::ffi::c_void;
//     let size = result.len();
//     let txid: c_ulong = 0;
//     let dt = get_rofi_c_dt::<T>().expect("type should be atomic");

//     let mut ret = unsafe {
//         rofisys::rofi_atomic_fetch(
//             rofisys::rofi_atomic_op_ROFI_STORE,
//             dt,
//             operand_addr,
//             remote_addr,
//             result_addr,
//             size,
//             pe as u32,
//             0,
//         )
//     };
//     while ret == -11 {
//         std::thread::yield_now();
//         ret = unsafe {
//             rofisys::rofi_atomic_fetch(
//                 rofisys::rofi_atomic_op_ROFI_STORE,
//                 dt,
//                 operand_addr,
//                 remote_addr,
//                 result_addr,
//                 size,
//                 pe as u32,
//                 0,
//             )
//         }; 
//     }
//     if ret == 0 {
//         Ok(txid)
//     } else {
//         Err(ret)
//     }
// }

// pub(crate) fn rofi_c_atomic_compare_exchange<T>(
//     local: &[T],
//     remote: usize,
//     compare: &[T],
//     result: &mut [T],
//     pe: usize,
// ) -> Result<c_ulong, i32> {
//     let local_addr = local.as_ptr() as *mut std::ffi::c_void;
//     let compare_addr = compare.as_ptr() as *mut std::ffi::c_void;
//     let result_addr = result.as_ptr() as *mut std::ffi::c_void;
//     let remote_addr = remote as *mut std::ffi::c_void;
//     let size = local.len();
//     let txid: c_ulong = 0;
//     let dt = get_rofi_c_dt::<T>().expect("type should be atomic");

//     let mut ret = unsafe {
//         rofisys::rofi_atomic_compare_exchange(
//             dt,
//             local_addr,
//             remote_addr,
//             compare_addr,
//             result_addr,
//             size,
//             pe as u32,
//             0,
//         )
//     }; 
//     while ret == -11 {
//         std::thread::yield_now();
//         ret = unsafe {
//             rofisys::rofi_atomic_compare_exchange(
//                 dt,
//                 local_addr,
//                 remote_addr,
//                 compare_addr,
//                 result_addr,
//                 size,
//                 pe as u32,
//                 0,
//             )
//         };
//     }
//     if ret == 0 {
//         Ok(txid)
//     } else {
//         Err(ret)
//     }
// }

// pub(crate) fn rofi_c_iatomic_store<T>(local: &[T], remote: usize, pe: usize) -> Result<c_ulong, i32> {
//     let local_addr = local.as_ptr() as *mut std::ffi::c_void;
//     let remote_addr = remote as *mut std::ffi::c_void;
//     let size = local.len();
//     let txid: c_ulong = 0;
//     let dt = get_rofi_c_dt::<T>().expect("type should be atomic");

//     let mut ret = unsafe {
//         rofisys::rofi_iatomic(
//             rofisys::rofi_atomic_op_ROFI_STORE,
//             dt,
//             local_addr,
//             remote_addr,
//             size,
//             pe as u32,
//             0,
//         )
//     }; 
//     while ret == -11 {
//         std::thread::yield_now();
//         ret = unsafe {
//             rofisys::rofi_iatomic(
//                 rofisys::rofi_atomic_op_ROFI_STORE,
//                 dt,
//                 local_addr,
//                 remote_addr,
//                 size,
//                 pe as u32,
//                 0,
//             )
//         }; 
//     }
//     if ret == 0 {
//         Ok(txid)
//     } else {
//         Err(ret)
//     }
// }

// pub(crate) fn rofi_c_iatomic_load<T>(
//     result: &mut [T],
//     remote: usize,
//     pe: usize,
// ) -> Result<c_ulong, i32> {
//     let result_addr = result.as_ptr() as *mut std::ffi::c_void;
//     let remote_addr = remote as *mut std::ffi::c_void;
//     let size = result.len();
//     let txid: c_ulong = 0;
//     let dt = get_rofi_c_dt::<T>().expect("type should be atomic");

//     let mut ret = unsafe {
//         rofisys::rofi_iatomic_fetch(
//             rofisys::rofi_atomic_op_ROFI_LOAD,
//             dt,
//             std::ptr::null_mut(),
//             remote_addr,
//             result_addr,
//             size,
//             pe as u32,
//             0,
//         )
//     }; 
//     while ret == -11 {
//         std::thread::yield_now();
//         ret = unsafe {
//             rofisys::rofi_iatomic_fetch(
//                 rofisys::rofi_atomic_op_ROFI_LOAD,
//                 dt,
//                 std::ptr::null_mut(),
//                 remote_addr,
//                 result_addr,
//                 size,
//                 pe as u32,
//                 0,
//             )
//         }; 
//     }
//     if ret == 0 {
//         Ok(txid)
//     } else {
//         Err(ret)
//     }
// }

// pub(crate) fn rofi_c_iatomic_swap<T>(
//     operand: &[T],
//     result: &mut [T],
//     remote: usize,
//     pe: usize,
// ) -> Result<c_ulong, i32> {
//     let operand_addr = operand.as_ptr() as *mut std::ffi::c_void;
//     let result_addr = result.as_ptr() as *mut std::ffi::c_void;
//     let remote_addr = remote as *mut std::ffi::c_void;
//     let size = result.len();
//     let txid: c_ulong = 0;
//     let dt = get_rofi_c_dt::<T>().expect("type should be atomic");

//     let mut ret = unsafe {
//         rofisys::rofi_iatomic_fetch(
//             rofisys::rofi_atomic_op_ROFI_STORE,
//             dt,
//             operand_addr,
//             remote_addr,
//             result_addr,
//             size,
//             pe as u32,
//             0,
//         )
//     };
//     while ret == -11 {
//         std::thread::yield_now();
//         ret = unsafe {
//             rofisys::rofi_iatomic_fetch(
//                 rofisys::rofi_atomic_op_ROFI_STORE,
//                 dt,
//                 operand_addr,
//                 remote_addr,
//                 result_addr,
//                 size,
//                 pe as u32,
//                 0,
//             )
//         }; 
//     }
//     if ret == 0 {
//         Ok(txid)
//     } else {
//         Err(ret)
//     }
// }

// pub(crate) fn rofi_c_iatomic_compare_exchange<T>(
//     local: &[T],
//     remote: usize,
//     compare: &[T],
//     result: &mut [T],
//     pe: usize,
// ) -> Result<c_ulong, i32> {
//     let local_addr = local.as_ptr() as *mut std::ffi::c_void;
//     let compare_addr = compare.as_ptr() as *mut std::ffi::c_void;
//     let result_addr = result.as_ptr() as *mut std::ffi::c_void;
//     let remote_addr = remote as *mut std::ffi::c_void;
//     let size = local.len();
//     let txid: c_ulong = 0;
//     let dt = get_rofi_c_dt::<T>().expect("type should be atomic");

//     let mut ret = unsafe {
//         rofisys::rofi_iatomic_compare_exchange(
//             dt,
//             local_addr,
//             remote_addr,
//             compare_addr,
//             result_addr,
//             size,
//             pe as u32,
//             0,
//         )
//     }; 
//     while ret == -11 {
//         std::thread::yield_now();
//         ret = unsafe {
//             rofisys::rofi_iatomic_compare_exchange(
//                 dt,
//                 local_addr,
//                 remote_addr,
//                 compare_addr,
//                 result_addr,
//                 size,
//                 pe as u32,
//                 0,
//             )
//         }; 
//     }
//     if ret == 0 {
//         Ok(txid)
//     } else {
//         Err(ret)
//     }
// }
