pub(crate) fn rofi_init() -> Result<(), &'static str> {
    let retval = unsafe { rofisys::rofi_init() as i32 };
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
    let base_ptr_ptr = &mut base_ptr as *mut _;
    unsafe {
        if rofisys::rofi_alloc(size, 0x0, base_ptr_ptr as *mut *mut std::ffi::c_void) != 0 {
            panic!("unable to allocate memory region");
        }
    }
    base_ptr
}

// data is a reference, user must ensure lifetime is valid until underlying put is complete, thus is unsafe
pub(crate) unsafe fn rofi_put<T>(src: &[T], dst: usize, pe: usize) -> Result<(), i32> {
    let src_addr = src.as_ptr() as *mut std::ffi::c_void;
    let size = src.len() * std::mem::size_of::<T>();
    let mut ret = -11;
    //FI_EAGAIN should this be handled here, at c-rofi layer, or application layer?
    while ret == -11 {
        ret = rofisys::rofi_put(dst as *mut std::ffi::c_void, src_addr, size, pe as u32, 0);
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
    while ret == -11 {
        ret = unsafe {
            rofisys::rofi_iput(dst as *mut std::ffi::c_void, src_addr, size, pe as u32, 0)
        };
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
    let ret = rofisys::rofi_get(dst_addr, src_addr, size, pe as u32, 0);
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
    let mut ret = -11;
    while ret == -11 {
        unsafe {
            ret = rofisys::rofi_iget(dst_addr, src_addr, size, pe as u32, 0);
        }
    }
    if ret == 0 {
        Ok(())
    } else {
        Err(ret)
    }
}
