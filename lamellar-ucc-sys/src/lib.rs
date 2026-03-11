#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

pub const fn ucc_predefined_dt(id: u64) -> ucc_datatype_t {
    ((id as ucc_datatype_t) << (ucc_dt_type_t_UCC_DATATYPE_SHIFT as ucc_datatype_t)) as ucc_datatype_t | (ucc_dt_type_t_UCC_DATATYPE_PREDEFINED as ucc_datatype_t) 
}

pub const UCC_DT_INT8: ucc_datatype_t = ucc_predefined_dt(0);
pub const UCC_DT_INT16: ucc_datatype_t = ucc_predefined_dt(1);
pub const UCC_DT_INT32: ucc_datatype_t = ucc_predefined_dt(2);
pub const UCC_DT_INT64: ucc_datatype_t = ucc_predefined_dt(3);
pub const UCC_DT_INT128: ucc_datatype_t = ucc_predefined_dt(4);
pub const UCC_DT_UINT8: ucc_datatype_t = ucc_predefined_dt(5);
pub const UCC_DT_UINT16: ucc_datatype_t = ucc_predefined_dt(6);
pub const UCC_DT_UINT32: ucc_datatype_t = ucc_predefined_dt(7);
pub const UCC_DT_UINT64: ucc_datatype_t = ucc_predefined_dt(8);
pub const UCC_DT_UINT128: ucc_datatype_t = ucc_predefined_dt(9);
pub const UCC_DT_FLOAT16: ucc_datatype_t = ucc_predefined_dt(10);
pub const UCC_DT_FLOAT32: ucc_datatype_t = ucc_predefined_dt(11);
pub const UCC_DT_FLOAT64: ucc_datatype_t = ucc_predefined_dt(12);
pub const UCC_DT_BFLOAT16: ucc_datatype_t = ucc_predefined_dt(13);
pub const UCC_DT_FLOAT128: ucc_datatype_t = ucc_predefined_dt(14);
pub const UCC_DT_FLOAT32_COMPLEX: ucc_datatype_t = ucc_predefined_dt(15);
pub const UCC_DT_FLOAT64_COMPLEX: ucc_datatype_t = ucc_predefined_dt(16);
pub const UCC_DT_FLOAT128_COMPLEX: ucc_datatype_t = ucc_predefined_dt(17);