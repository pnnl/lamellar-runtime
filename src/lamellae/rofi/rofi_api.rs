extern crate libc;

use crate::lamellae::AllocationType;
use std::ffi::CString;
use std::os::raw::c_ulong;

pub(crate) fn rofi_init(provider: &str) -> Result<(), &'static str> {
    let c_str = CString::new(provider).unwrap();
    let retval = unsafe { rofisys::rofi_init(c_str.as_ptr() as *mut _) as i32 };
    if retval == 0 {
        Ok(())
    } else {
        Err("unable to initialize rofi")
    }
}

//currently shows unused warning as we are debugging a hang in rofi_finit

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

pub(crate) fn rofi_alloc(size: usize, alloc: AllocationType) -> *mut u8 {
    let mut base_ptr: *mut u8 = std::ptr::null_mut();
    let base_ptr_ptr = (&mut base_ptr as *mut _) as *mut *mut std::ffi::c_void;
    // println!("rofi_alloc");
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
            _ => panic!("unexpected allocation type {:?} in rofi_alloc", alloc),
        };

        if ret != 0 {
            panic!("unable to allocate memory region");
        }
    }
    //println!("[{:?}] ({:?}:{:?}) rofi_alloc addr: {:x} size {:?}",rofi_get_id(),file!(),line!(),base_ptr as usize, size);

    base_ptr
}

#[allow(dead_code)]
pub(crate) fn rofi_release(addr: usize) {
    let base_ptr = addr as *mut u8;
    // println!("rofi_release");
    unsafe {
        if rofisys::rofi_release(base_ptr as *mut std::ffi::c_void) != 0 {
            panic!("unable to release memory region");
        }
    }
    //println!("[{:?}] ({:?}:{:?}) rofi_release addr: {:x} ",rofi_get_id(),file!(),line!(),base_ptr as usize);
}

pub(crate) fn rofi_local_addr(remote_pe: usize, remote_addr: usize) -> usize {
    let addr = unsafe {
        // println!("{:x} {:?} {:?} {:?}",remote_addr,(remote_addr as *mut u8) as *mut std::ffi::c_void,remote_pe,remote_pe as u32);
        rofisys::rofi_get_local_addr_from_remote_addr(
            (remote_addr as *mut u8) as *mut std::ffi::c_void,
            remote_pe as u32,
        ) as usize
    };
    // println!("local addr: {:x}",addr);
    if addr == 0 {
        panic!("unable to locate local memory addr");
    }
    addr
}

pub(crate) fn rofi_remote_addr(pe: usize, local_addr: usize) -> usize {
    let addr = unsafe {
        // println!("{:x} {:?} {:?} {:?}",local_addr,(local_addr as *mut u8) as *mut std::ffi::c_void,pe,pe as u32);
        rofisys::rofi_get_remote_addr((local_addr as *mut u8) as *mut std::ffi::c_void, pe as u32)
    };
    // println!("remote addr {:?} 0x{:x}", addr as *mut u8 ,addr as usize);
    if addr as usize == 0 {
        panic!("unable to locate local memory addr");
    }
    addr as usize
}

// data is a reference, user must ensure lifetime is valid until underlying put is complete, thus is unsafe
pub(crate) unsafe fn rofi_put<T>(src: &[T], dst: usize, pe: usize) -> Result<c_ulong, i32> {
    let src_addr = src.as_ptr() as *mut std::ffi::c_void;
    let size = src.len() * std::mem::size_of::<T>();
    let txid: c_ulong = 0;
    let mut ret = rofisys::rofi_put(dst as *mut std::ffi::c_void, src_addr, size, pe as u32, 0); //, &mut txid);
                                                                                                 //FI_EAGAIN should this be handled here, at c-rofi layer, or application layer?

    while ret == -11 {
        std::thread::yield_now();
        ret = rofisys::rofi_put(dst as *mut std::ffi::c_void, src_addr, size, pe as u32, 0);
        //, &mut txid);
        // println!("[{:?}] ({:?}:{:?}) rofi_put src_addr {:?} dst_addr 0x{:x} {:?}",rofi_get_id(),file!(),line!(),src.as_ptr(),dst,ret);
    }
    if ret == 0 {
        Ok(txid)
    } else {
        Err(ret)
    }
}

#[allow(dead_code)]
pub(crate) fn rofi_iput<T>(src: &[T], dst: usize, pe: usize) -> Result<c_ulong, i32> {
    let src_addr = src.as_ptr() as *mut std::ffi::c_void;
    let size = src.len() * std::mem::size_of::<T>();
    let txid: c_ulong = 0;
    let mut ret =
        unsafe { rofisys::rofi_iput(dst as *mut std::ffi::c_void, src_addr, size, pe as u32, 0) };
    //FI_EAGAIN should this be handled here, at c-rofi layer, or application layer?
    // println!("putting {:?} to {:?} @ {:x}",src,pe,dst);
    while ret == -11 {
        std::thread::yield_now();
        ret = unsafe {
            rofisys::rofi_iput(dst as *mut std::ffi::c_void, src_addr, size, pe as u32, 0)
            //, &mut txid)
        };
        //println!("[{:?}] ({:?}:{:?}) rofi_iput src_addr {:?} dst_addr 0x{:x} pe {:?} {:?}",rofi_get_id(),file!(),line!(),src_addr,dst,pe,ret);
    }
    if ret == 0 {
        Ok(txid)
    } else {
        Err(ret)
    }
}

#[allow(dead_code)]
pub(crate) unsafe fn rofi_get<T>(src: usize, dst: &mut [T], pe: usize) -> Result<c_ulong, i32> {
    let src_addr = src as *mut std::ffi::c_void;
    let dst_addr = dst.as_ptr() as *mut std::ffi::c_void;
    let size = dst.len() * std::mem::size_of::<T>();
    let txid: c_ulong = 0;
    let mut ret = rofisys::rofi_get(dst_addr, src_addr, size, pe as u32, 0); //, &mut txid);
    while ret == -11 {
        std::thread::yield_now();
        ret = rofisys::rofi_get(dst_addr, src_addr, size, pe as u32, 0); //, &mut txid);
                                                                         //println!("[{:?}] ({:?}:{:?}) rofi_get src_addr {:?} dst_addr{:?} pe {:?} {:?}",rofi_get_id(),file!(),line!(),src_addr, dst_addr,pe, ret);
    }
    if ret == 0 {
        Ok(txid)
    } else {
        Err(ret)
    }
}

#[allow(dead_code)]
pub(crate) fn rofi_iget<T>(src: usize, dst: &mut [T], pe: usize) -> Result<c_ulong, i32> {
    let src_addr = src as *mut std::ffi::c_void;
    let dst_addr = dst.as_ptr() as *mut std::ffi::c_void;
    let size = dst.len() * std::mem::size_of::<T>();
    let txid: c_ulong = 0;
    //println!("[{:?}] ({:?}{:?}) rofi_iget src: {:x} dst: {:?} pe: {:?} size: {:?}",rofi_get_id(),file!(),line!(),src, dst.as_ptr(),pe, size);
    let mut ret = unsafe { rofisys::rofi_iget(dst_addr, src_addr, size, pe as u32, 0) }; //, &mut txid) };
    while ret == -11 {
        std::thread::yield_now();
        ret = unsafe { rofisys::rofi_iget(dst_addr, src_addr, size, pe as u32, 0) };
        //, &mut txid) };

        // println!("[{:?}] ({:?}:{:?}) rofi_get src_addr {:?} dst_addr{:?} pe {:?} {:?}",rofi_get_id(),file!(),line!(),src_addr, dst_addr,pe, ret);
    }
    if ret == 0 {
        Ok(txid)
    } else {
        Err(ret)
    }
}

#[allow(dead_code)]
pub(crate) fn rofi_isend<T: std::fmt::Debug>(src: &[T], pe: usize) -> Result<(), i32> {
    // println!("sending {:?}", src);
    let src_addr = src.as_ptr() as *mut std::ffi::c_void;
    let size = src.len() * std::mem::size_of::<T>();
    // println!("sending {:?} {:?}", src, size);
    let mut ret = unsafe { rofisys::rofi_isend(pe as u64, src_addr, size, 0) };
    while ret == -11 {
        std::thread::yield_now();
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
    // println!("waiting to receive {:?} from {:?}", size, pe);
    let mut ret = unsafe { rofisys::rofi_irecv(pe as u64, dst_addr, size, 0) };
    while ret == -11 {
        std::thread::yield_now();
        ret = unsafe { rofisys::rofi_irecv(pe as u64, dst_addr, size, 0) };
        // println!("recv ret {:?}", ret);
    }
    if ret == 0 {
        Ok(())
    } else {
        Err(ret)
    }
}

// pub(crate) fn rofi_wait_set(txids: Vec<c_ulong>) {
//     let ptr = txids.as_ptr() as *mut c_ulong;
//     unsafe {rofisys::rofi_wait_set(ptr,txids.len()as u64)};
// }

// pub(crate) fn rofi_drop_set(txids: Vec<c_ulong>) {
//     let ptr = txids.as_ptr() as *mut c_ulong;
//     unsafe {rofisys::rofi_drop_set(ptr,txids.len()as u64)};
// }
