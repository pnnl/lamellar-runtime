extern crate libc;

use std::ffi::CString;

pub(crate) fn rofi_init(provider: &str) -> Result<(), &'static str> {
    let c_str = CString::new(provider).unwrap();
    let retval = unsafe { rofisys::rofi_init(c_str.as_ptr() as *mut _) as i32 };
    if retval == 0 {
        Ok(())
    } else {
        Err("unable to initialize rofi")
    }
}

pub(crate) fn rofi_finit() -> Result<(), &'static str> {
    let retval = unsafe { rofisys::rofi_finit() as i32 };
    if retval == 0 {
        Ok(())
    } else {
        Err("unable to finit rofi")
    }
}

pub(crate) fn rofi_get_size() -> usize {
    unsafe { rofisys::rofi_get_size() as usize }
}

pub(crate) fn rofi_get_id() -> usize {
    unsafe { rofisys::rofi_get_id() as usize }
}

pub(crate) fn rofi_barrier() {
    unsafe { rofisys::rofi_barrier() };
}

pub(crate) fn rofi_alloc(size: usize) -> *mut u8 {
    let mut base_ptr: *mut u8 = std::ptr::null_mut();
    let base_ptr_ptr = (&mut base_ptr as *mut _) as *mut *mut std::ffi::c_void;
    // let mut key = 0u64;
    // let key_ptr = &mut key as *mut c_ulonglong;
    unsafe {
        if rofisys::rofi_alloc(size, 0x0, base_ptr_ptr) != 0 {
            panic!("unable to allocate memory region");
        }
    }
    // trace!("[{:?}] ({:?}:{:?}) rofi_alloc addr: {:x} size {:?}",rofi_get_id(),file!(),line!(),base_ptr as usize, size);

    // let _rofi_slice = unsafe { std::slice::from_raw_parts(base_ptr as *const u8, size) };
    rofi_barrier();
    base_ptr
}

#[allow(dead_code)]
pub(crate) fn rofi_release(addr: usize) {
    let base_ptr = addr as *mut u8;
    unsafe {
        if rofisys::rofi_release(base_ptr as *mut std::ffi::c_void) != 0 {
            panic!("unable to release memory region");
        }
    }
    // trace!("[{:?}] ({:?}:{:?}) rofi_release addr: {:x} ",rofi_get_id(),file!(),line!(),base_ptr as usize);
}

pub(crate) fn rofi_local_addr(remote_pe: usize, remote_addr: usize) -> usize {
    let addr = unsafe {
        // println!("{:x} {:?} {:?} {:?}",remote_addr,(remote_addr as *mut u8) as *mut std::ffi::c_void,remote_pe,remote_pe as u32);
        rofisys::rofi_get_local_addr_from_remote_addr(
            (remote_addr as *mut u8) as *mut std::ffi::c_void,
            remote_pe as u32,
        ) as usize
    };
    if addr == 0 {
        panic!("unable to locate local memory addr");
    }
    addr
}

// pub(crate) fn rofi_start_addr(id: u64) -> usize {
//     let mut addr_ptr: *mut u8 = std::ptr::null_mut();
//     let addr_ptr_ptr = (&mut addr_ptr as *mut _) as *mut *mut std::ffi::c_void;
//     unsafe {
//         if rofisys::rofi_start_addr(id as c_ulonglong, addr_ptr_ptr) != 0 {
//             panic!("unable to locate remote memory addr");
//         }
//     }
//     addr_ptr as usize
// }

pub(crate) fn rofi_remote_addr(pe: usize, local_addr: usize) -> usize {
    let addr = unsafe {
        rofisys::rofi_get_remote_addr((local_addr as *mut u8) as *mut std::ffi::c_void, pe as u32)
            as usize
    };
    if addr == 0 {
        panic!("unable to locate local memory addr");
    }
    addr
}

// data is a reference, user must ensure lifetime is valid until underlying put is complete, thus is unsafe
pub(crate) unsafe fn rofi_put<T>(src: &[T], dst: usize, pe: usize) -> Result<(), i32> {
    let src_addr = src.as_ptr() as *mut std::ffi::c_void;
    let size = src.len() * std::mem::size_of::<T>();
    let mut ret = -11;
    //FI_EAGAIN should this be handled here, at c-rofi layer, or application layer?
    while ret == -11 {
        ret = rofisys::rofi_put(dst as *mut std::ffi::c_void, src_addr, size, pe as u32, 0);
        // println!("[{:?}] ({:?}:{:?}) rofi_put src_addr {:?} {:?}",rofi_get_id(),file!(),line!(),src.as_ptr(),ret);
    }
    if ret == 0 {
        Ok(())
    } else {
        Err(ret)
    }
}

#[allow(dead_code)]
pub(crate) fn rofi_iput<T>(src: &[T], dst: usize, pe: usize) -> Result<(), i32> {
    let src_addr = src.as_ptr() as *mut std::ffi::c_void;
    let size = src.len() * std::mem::size_of::<T>();
    let mut ret = -11;
    //FI_EAGAIN should this be handled here, at c-rofi layer, or application layer?
    // println!("putting {:?} to {:?} @ {:x}",src,pe,dst);
    while ret == -11 {
        ret = unsafe {
            rofisys::rofi_iput(dst as *mut std::ffi::c_void, src_addr, size, pe as u32, 0)
        };
        // println!("[{:?}] ({:?}:{:?}) rofi_iput src_addr {:?} {:?}",rofi_get_id(),file!(),line!(),src.as_ptr(),ret);
    }
    if ret == 0 {
        Ok(())
    } else {
        Err(ret)
    }
}
#[allow(dead_code)]
pub(crate) unsafe fn rofi_get<T>(src: usize, dst: &mut [T], pe: usize) -> Result<(), i32> {
    let src_addr = src as *mut std::ffi::c_void;
    let dst_addr = dst.as_ptr() as *mut std::ffi::c_void;
    let size = dst.len() * std::mem::size_of::<T>();
    // let mut ret = -11;
    // while ret == -11 {
    let ret = rofisys::rofi_iget(dst_addr, src_addr, size, pe as u32, 0);
    // }
    if ret == 0 {
        Ok(())
    } else {
        Err(ret)
    }
}

#[allow(dead_code)]
pub(crate) fn rofi_iget<T>(src: usize, dst: &mut [T], pe: usize) -> Result<(), i32> {
    let src_addr = src as *mut std::ffi::c_void;
    let dst_addr = dst.as_ptr() as *mut std::ffi::c_void;
    let size = dst.len() * std::mem::size_of::<T>();
    // println!("[{:?}] ({:?}{:?}) rofi_iget src: {:x} dst: {:?} pe: {:?} size: {:?}",rofi_get_id(),file!(),line!(),src, dst.as_ptr(),pe, size);
    // let mut ret = -11;
    // while ret == -11 {
    let ret = unsafe { rofisys::rofi_iget(dst_addr, src_addr, size, pe as u32, 0) };
    // }
    if ret == 0 {
        Ok(())
    } else {
        Err(ret)
    }
}

#[allow(dead_code)]
pub(crate) fn rofi_isend<T: std::fmt::Debug>(src: &[T], pe: usize) -> Result<(), i32> {
    println!("sending {:?}", src);
    let src_addr = src.as_ptr() as *mut std::ffi::c_void;
    let size = src.len() * std::mem::size_of::<T>();
    println!("sending {:?} {:?}", src, size);
    let mut ret = -11;
    while ret == -11 {
        ret = unsafe { rofisys::rofi_isend(pe as u64, src_addr, size, 0) };
        // println!("src_addr {:?} {:?}",src.as_ptr(),ret);
    }
    if ret == 0 {
        Ok(())
    } else {
        Err(ret)
    }
}

#[allow(dead_code)]
pub(crate) fn rofi_irecv<T>(dst: &mut [T], pe: usize) -> Result<(), i32> {
    let dst_addr = dst.as_ptr() as *mut std::ffi::c_void;
    let size = dst.len() * std::mem::size_of::<T>();
    println!("waiting to receive {:?} from {:?}", size, pe);
    let mut ret = -11;
    while ret == -11 {
        ret = unsafe { rofisys::rofi_irecv(pe as u64, dst_addr, size, 0) };
        println!("recv ret {:?}", ret);
    }
    if ret == 0 {
        Ok(())
    } else {
        Err(ret)
    }
}
