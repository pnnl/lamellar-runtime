#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

pub fn ucp_dt_make_contig(element_size: usize) -> ucp_datatype_t {
    ((element_size as ucp_datatype_t) << (ucp_dt_type::UCP_DATATYPE_SHIFT as ucp_datatype_t))
        | (ucp_dt_type::UCP_DATATYPE_CONTIG as ucp_datatype_t)
}

pub fn UCS_PTR_IS_ERR(ptr: ucs_status_ptr_t) -> bool {
    ptr as usize >= ucs_status_t::UCS_ERR_LAST as usize
}

pub fn UCS_PTR_IS_PTR(ptr: ucs_status_ptr_t) -> bool {
    ptr as usize - 1 < ucs_status_t::UCS_ERR_LAST as usize - 1
}

pub fn UCS_PTR_RAW_STATUS(ptr: ucs_status_ptr_t) -> ucs_status_t {
    unsafe { std::mem::transmute(ptr as i8) }
}

pub fn UCS_PTR_STATUS(ptr: ucs_status_ptr_t) -> ucs_status_t {
    if UCS_PTR_IS_PTR(ptr) {
        ucs_status_t::UCS_INPROGRESS
    } else {
        UCS_PTR_RAW_STATUS(ptr)
    }
}
