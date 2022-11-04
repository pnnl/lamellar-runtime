use crate::array::atomic::*;
use crate::array::generic_atomic::*;
use crate::array::local_lock_atomic::*;
use crate::array::native_atomic::*;
use crate::array::r#unsafe::*;
use crate::array::read_only::*;

use crate::array::*;

use crate::lamellar_request::LamellarRequest;
use crate::memregion::{
    local::LocalMemoryRegion, shared::SharedMemoryRegion, Dist, LamellarMemoryRegion,
};
// use crate::Darc;
use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::u8;

pub static OPS_BUFFER_SIZE: usize = 10_000_000;

#[derive(
    serde::Serialize,
    serde::Deserialize,
    Hash,
    std::cmp::PartialEq,
    std::cmp::Eq,
    Clone,
    Debug,
    Copy,
)]
#[serde(bound = "T: Dist + serde::Serialize + serde::de::DeserializeOwned")]
pub enum ArrayOpCmd<T: Dist> {
    Add,
    FetchAdd,
    Sub,
    FetchSub,
    Mul,
    FetchMul,
    Div,
    FetchDiv,
    And,
    FetchAnd,
    Or,
    FetchOr,
    Store,
    Load,
    Swap,
    Put,
    Get,
    CompareExchange(T),
    CompareExchangeEps(T, T),
}

impl<T: Dist> ArrayOpCmd<T> {
    #[tracing::instrument(skip_all)]
    pub fn result_size(&self) -> usize {
        match self {
            ArrayOpCmd::CompareExchange(_) | ArrayOpCmd::CompareExchangeEps(_, _) => {
                std::mem::size_of::<T>() + 1
            } //plus one to indicate this requires a result (0 for okay, 1 for error)
            ArrayOpCmd::FetchAdd
            | ArrayOpCmd::FetchSub
            | ArrayOpCmd::FetchMul
            | ArrayOpCmd::FetchDiv
            | ArrayOpCmd::FetchAnd
            | ArrayOpCmd::FetchOr
            | ArrayOpCmd::Load
            | ArrayOpCmd::Swap
            | ArrayOpCmd::Get => std::mem::size_of::<T>(), //just return value, assume never fails
            ArrayOpCmd::Add
            | ArrayOpCmd::Sub
            | ArrayOpCmd::Mul
            | ArrayOpCmd::Div
            | ArrayOpCmd::And
            | ArrayOpCmd::Or
            | ArrayOpCmd::Store
            | ArrayOpCmd::Put => 0, //we dont return anything
        }
    }

    pub fn to_bytes(&self, buf: &mut [u8]) -> usize {
        match self {
            ArrayOpCmd::Add => {
                buf[0] = 0;
                1
            }
            ArrayOpCmd::FetchAdd => {
                buf[0] = 1;
                1
            }
            ArrayOpCmd::Sub => {
                buf[0] = 2;
                1
            }
            ArrayOpCmd::FetchSub => {
                buf[0] = 3;
                1
            }
            ArrayOpCmd::Mul => {
                buf[0] = 4;
                1
            }
            ArrayOpCmd::FetchMul => {
                buf[0] = 5;
                1
            }
            ArrayOpCmd::Div => {
                buf[0] = 6;
                1
            }
            ArrayOpCmd::FetchDiv => {
                buf[0] = 7;
                1
            }
            ArrayOpCmd::And => {
                buf[0] = 8;
                1
            }
            ArrayOpCmd::FetchAnd => {
                buf[0] = 9;
                1
            }
            ArrayOpCmd::Or => {
                buf[0] = 10;
                1
            }
            ArrayOpCmd::FetchOr => {
                buf[0] = 11;
                1
            }
            ArrayOpCmd::Store => {
                buf[0] = 12;
                1
            }
            ArrayOpCmd::Load => {
                buf[0] = 13;
                1
            }
            ArrayOpCmd::Swap => {
                buf[0] = 14;
                1
            }
            ArrayOpCmd::Put => {
                buf[0] = 15;
                1
            }
            ArrayOpCmd::Get => {
                buf[0] = 16;
                1
            }
            ArrayOpCmd::CompareExchange(val) => {
                buf[0] = 17;
                unsafe {
                    std::ptr::copy_nonoverlapping(val as *const T, buf[1..].as_ptr() as *mut T, 1);
                }
                1 + std::mem::size_of::<T>()
            }
            ArrayOpCmd::CompareExchangeEps(val, eps) => {
                buf[0] = 18;
                let t_size = std::mem::size_of::<T>();
                unsafe {
                    std::ptr::copy_nonoverlapping(val as *const T, buf[1..].as_ptr() as *mut T, 1);
                    std::ptr::copy_nonoverlapping(
                        eps as *const T,
                        buf[(1 + t_size)..].as_ptr() as *mut T,
                        1,
                    );
                }
                1 + 2 * std::mem::size_of::<T>()
            }
        }
    }

    pub fn num_bytes(&self) -> usize {
        match self {
            ArrayOpCmd::Add => 1,
            ArrayOpCmd::FetchAdd => 1,
            ArrayOpCmd::Sub => 1,
            ArrayOpCmd::FetchSub => 1,
            ArrayOpCmd::Mul => 1,
            ArrayOpCmd::FetchMul => 1,
            ArrayOpCmd::Div => 1,
            ArrayOpCmd::FetchDiv => 1,
            ArrayOpCmd::And => 1,
            ArrayOpCmd::FetchAnd => 1,
            ArrayOpCmd::Or => 1,
            ArrayOpCmd::FetchOr => 1,
            ArrayOpCmd::Store => 1,
            ArrayOpCmd::Load => 1,
            ArrayOpCmd::Swap => 1,
            ArrayOpCmd::Put => 1,
            ArrayOpCmd::Get => 1,
            ArrayOpCmd::CompareExchange(_val) => 1 + std::mem::size_of::<T>(),
            ArrayOpCmd::CompareExchangeEps(_val, _eps) => 1 + 2 * std::mem::size_of::<T>(),
        }
    }

    pub fn from_bytes(buf: &[u8]) -> (Self, usize) {
        let variant = buf[0];
        match variant {
            0 => (ArrayOpCmd::Add, 1),
            1 => (ArrayOpCmd::FetchAdd, 1),
            2 => (ArrayOpCmd::Sub, 1),
            3 => (ArrayOpCmd::FetchSub, 1),
            4 => (ArrayOpCmd::Mul, 1),
            5 => (ArrayOpCmd::FetchMul, 1),
            6 => (ArrayOpCmd::Div, 1),
            7 => (ArrayOpCmd::FetchDiv, 1),
            8 => (ArrayOpCmd::And, 1),
            9 => (ArrayOpCmd::FetchAnd, 1),
            10 => (ArrayOpCmd::Or, 1),
            11 => (ArrayOpCmd::FetchOr, 1),
            12 => (ArrayOpCmd::Store, 1),
            13 => (ArrayOpCmd::Load, 1),
            14 => (ArrayOpCmd::Swap, 1),
            15 => (ArrayOpCmd::Put, 1),
            16 => (ArrayOpCmd::Get, 1),
            17 => {
                let val = unsafe { *(buf[1..].as_ptr() as *const T) };
                (
                    ArrayOpCmd::CompareExchange(val),
                    1 + std::mem::size_of::<T>(),
                )
            }
            18 => {
                let t_size = std::mem::size_of::<T>();
                let val = unsafe { *(buf[1..].as_ptr() as *const T) };
                let eps = unsafe { *(buf[(1 + t_size)..].as_ptr() as *const T) };
                (ArrayOpCmd::CompareExchangeEps(val, eps), 1 + 2 * t_size)
            }
            _ => {
                panic!("unrecognized Array Op Type");
            }
        }
    }
}

#[derive(serde::Serialize, Clone, Debug)]
pub enum InputToValue<'a, T: Dist> {
    OneToOne(usize, T),
    OneToMany(usize, OpInputEnum<'a, T>),
    ManyToOne(OpInputEnum<'a, usize>, T),
    ManyToMany(OpInputEnum<'a, usize>, OpInputEnum<'a, T>),
}

impl<'a, T: Dist> InputToValue<'a, T> {
    #[tracing::instrument(skip_all)]
    pub(crate) fn len(&self) -> usize {
        match self {
            InputToValue::OneToOne(_, _) => 1,
            InputToValue::OneToMany(_, vals) => vals.len(),
            InputToValue::ManyToOne(indices, _) => indices.len(),
            InputToValue::ManyToMany(indices, _) => indices.len(),
        }
    }
    // fn num_bytes(&self) -> usize{
    //     match self{
    //         InputToValue::OneToOne(_,_) => std::mem::size_of::<(usize,T)>(),
    //         InputToValue::OneToMany(_,vals) => std::mem::size_of::<usize>()+ vals.len() * std::mem::size_of::<T>(),
    //         InputToValue::ManyToOne(indices,_) => indices.len() * std::mem::size_of::<usize>() + std::mem::size_of::<T>(),
    //         InputToValue::ManyToMany(indices,vals) => indices.len() * std::mem::size_of::<usize>() +  vals.len() * std::mem::size_of::<T>(),
    //     }
    // }
    #[tracing::instrument(skip_all)]
    pub(crate) fn to_pe_offsets(
        self,
        array: &UnsafeArray<T>,
    ) -> (
        HashMap<usize, InputToValue<'a, T>>,
        HashMap<usize, Vec<usize>>,
        usize,
    ) {
        let mut pe_offsets = HashMap::new();
        let mut req_ids = HashMap::new();
        match self {
            InputToValue::OneToOne(index, value) => {
                let (pe, local_index) = array.calc_pe_and_offset(index);
                pe_offsets.insert(pe, InputToValue::OneToOne(local_index, value));
                req_ids.insert(pe, vec![0]);
                (pe_offsets, req_ids, 1)
            }
            InputToValue::OneToMany(index, values) => {
                let (pe, local_index) = array.calc_pe_and_offset(index);
                let vals_len = values.len();
                req_ids.insert(pe, (0..vals_len).collect());
                pe_offsets.insert(pe, InputToValue::OneToMany(local_index, values));

                (pe_offsets, req_ids, vals_len)
            }
            InputToValue::ManyToOne(indices, value) => {
                let mut temp_pe_offsets = HashMap::new();
                let mut req_cnt = 0;
                for index in indices.iter() {
                    let (pe, local_index) = array.calc_pe_and_offset(index);
                    temp_pe_offsets
                        .entry(pe)
                        .or_insert(vec![])
                        .push(local_index);
                    req_ids.entry(pe).or_insert(vec![]).push(req_cnt);
                    req_cnt += 1;
                }

                for (pe, local_indices) in temp_pe_offsets {
                    pe_offsets.insert(
                        pe,
                        InputToValue::ManyToOne(OpInputEnum::Vec(local_indices), value),
                    );
                }

                (pe_offsets, req_ids, indices.len())
            }
            InputToValue::ManyToMany(indices, values) => {
                let mut temp_pe_offsets = HashMap::new();
                let mut req_cnt = 0;
                for (index, val) in indices.iter().zip(values.iter()) {
                    let (pe, local_index) = array.calc_pe_and_offset(index);
                    let data = temp_pe_offsets.entry(pe).or_insert((vec![], vec![]));
                    data.0.push(local_index);
                    data.1.push(val);
                    req_ids.entry(pe).or_insert(vec![]).push(req_cnt);
                    req_cnt += 1;
                }
                for (pe, (local_indices, vals)) in temp_pe_offsets {
                    pe_offsets.insert(
                        pe,
                        InputToValue::ManyToMany(
                            OpInputEnum::Vec(local_indices),
                            OpInputEnum::Vec(vals),
                        ),
                    );
                }
                (pe_offsets, req_ids, indices.len())
            }
        }
    }
}
impl<'a, T: Dist + serde::Serialize + serde::de::DeserializeOwned> InputToValue<'a, T> {
    #[tracing::instrument(skip_all)]
    pub fn as_op_am_input(&self) -> OpAmInputToValue<T> {
        match self {
            InputToValue::OneToOne(index, value) => OpAmInputToValue::OneToOne(*index, *value),
            InputToValue::OneToMany(index, values) => {
                OpAmInputToValue::OneToMany(*index, values.iter().collect())
            }
            InputToValue::ManyToOne(indices, value) => {
                OpAmInputToValue::ManyToOne(indices.iter().collect(), *value)
            }
            InputToValue::ManyToMany(indices, values) => {
                OpAmInputToValue::ManyToMany(indices.iter().collect(), values.iter().collect())
            }
        }
    }
}

impl<T: Dist> OpAmInputToValue<T> {
    #[tracing::instrument(skip_all)]
    pub fn len(&self) -> usize {
        match self {
            OpAmInputToValue::OneToOne(_, _) => 1,
            OpAmInputToValue::OneToMany(_, vals) => vals.len(),
            OpAmInputToValue::ManyToOne(indices, _) => indices.len(),
            OpAmInputToValue::ManyToMany(indices, _) => indices.len(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[serde(bound = "T: Dist + serde::Serialize + serde::de::DeserializeOwned")]
pub enum OpAmInputToValue<T: Dist> {
    OneToOne(usize, T),
    OneToMany(usize, Vec<T>),
    ManyToOne(Vec<usize>, T),
    ManyToMany(Vec<usize>, Vec<T>),
}

impl<T: Dist> OpAmInputToValue<T> {
    pub fn embed_vec<U>(data: &Vec<U>, buf: &mut [u8]) -> usize {
        let mut size = 0;
        // embed the data length
        let len = data.len();
        unsafe {
            std::ptr::copy_nonoverlapping(
                &len as *const usize,
                buf[size..].as_ptr() as *mut usize,
                1,
            )
        };
        size += std::mem::size_of::<usize>();
        // ---- end data length ----
        // embed the data
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), buf[size..].as_ptr() as *mut U, len)
        };
        size += len * std::mem::size_of::<U>();
        // ---- end data ====
        size
    }
    pub fn embed_single_val<U>(val: U, buf: &mut [u8]) -> usize {
        // embed the val
        unsafe { std::ptr::copy_nonoverlapping(&val as *const U, buf.as_ptr() as *mut U, 1) };
        std::mem::size_of::<U>()
        // ---- end val ----
    }
    pub fn to_bytes(self, buf: &mut [u8]) -> usize {
        match self {
            OpAmInputToValue::OneToOne(idx, val) => {
                // embed the enum type
                let mut size = 0;
                buf[size] = 0;
                size += 1;
                // ----- end type -----
                // embed the index
                size += OpAmInputToValue::<T>::embed_single_val(idx, &mut buf[size..]);
                // ---- end index ----
                // embed the value
                size += OpAmInputToValue::<T>::embed_single_val(val, &mut buf[size..]);
                // -- end value --
                size
            }
            OpAmInputToValue::OneToMany(idx, vals) => {
                // embed the enum type
                let mut size = 0;
                buf[size] = 1;
                size += 1;
                // ----- end type -----
                // embed the index
                size += OpAmInputToValue::<T>::embed_single_val(idx, &mut buf[size..]);
                // ---- end index ----
                // embed the vals
                size += OpAmInputToValue::<T>::embed_vec(&vals, &mut buf[size..]);
                // ---- end vals ----
                size
            }
            OpAmInputToValue::ManyToOne(idxs, val) => {
                // embed the enum type
                let mut size = 0;
                buf[size] = 2;
                size += 1;
                // ----- end type -----
                // embed the indices
                size += OpAmInputToValue::<T>::embed_vec(&idxs, &mut buf[size..]);
                // ---- end indices ----
                // embed the val
                size += OpAmInputToValue::<T>::embed_single_val(val, &mut buf[size..]);
                // ---- end val ----
                size
            }
            OpAmInputToValue::ManyToMany(idxs, vals) => {
                // embed the enum type
                let mut size = 0;
                buf[size] = 3;
                size += 1;
                // ----- end type -----
                // embed the indices
                size += OpAmInputToValue::<T>::embed_vec(&idxs, &mut buf[size..]);
                // ---- end indices ----
                // embed the vals
                size += OpAmInputToValue::<T>::embed_vec(&vals, &mut buf[size..]);
                // ---- end vals ----
                size
            }
        }
    }
    pub fn vec_size<U>(data: &Vec<U>) -> usize {
        let mut size = 0;
        let len = data.len();
        size += std::mem::size_of::<usize>(); //the length
        size += len * std::mem::size_of::<U>();
        size
    }
    pub fn single_val_size<U>(_val: U) -> usize {
        std::mem::size_of::<U>()
    }
    pub fn num_bytes(&self) -> usize {
        match self {
            OpAmInputToValue::OneToOne(idx, val) => {
                let mut size = 0;
                size += 1;
                size += OpAmInputToValue::<T>::single_val_size(idx);
                size += OpAmInputToValue::<T>::single_val_size(val);
                size
            }
            OpAmInputToValue::OneToMany(idx, vals) => {
                let mut size = 0;
                size += 1;
                size += OpAmInputToValue::<T>::single_val_size(idx);
                size += OpAmInputToValue::<T>::vec_size(&vals);
                size
            }
            OpAmInputToValue::ManyToOne(idxs, val) => {
                let mut size = 0;
                size += 1;
                size += OpAmInputToValue::<T>::vec_size(&idxs);
                size += OpAmInputToValue::<T>::single_val_size(val);
                size
            }
            OpAmInputToValue::ManyToMany(idxs, vals) => {
                let mut size = 0;
                size += 1;
                size += OpAmInputToValue::<T>::vec_size(&idxs);
                size += OpAmInputToValue::<T>::vec_size(&vals);
                size
            }
        }
    }
}

pub enum RemoteOpAmInputToValue<'a, T: Dist> {
    OneToOne(&'a usize, &'a T),
    OneToMany(&'a usize, &'a [T]),
    ManyToOne(&'a [usize], &'a T),
    ManyToMany(&'a [usize], &'a [T]),
}

impl<'a, T: Dist> RemoteOpAmInputToValue<'a, T> {
    pub fn unpack_slice<U>(buf: &[u8]) -> (&[U], usize) {
        let mut size = 0;
        let len = unsafe { &*(buf[size..].as_ptr() as *const usize) };
        size += std::mem::size_of::<usize>();
        let vals = unsafe { std::slice::from_raw_parts(buf[size..].as_ptr() as *const U, *len) };
        size += len * std::mem::size_of::<T>();
        (vals, size)
    }
    pub fn from_bytes(buf: &'a [u8]) -> (Self, usize) {
        let mut size = 0;
        let variant = buf[size];
        size += 1;
        match variant {
            0 => {
                let idx = unsafe { &*(buf[size..].as_ptr() as *const usize) };
                size += std::mem::size_of::<usize>();
                let val = unsafe { &*(buf[size..].as_ptr() as *const T) };
                size += std::mem::size_of::<T>();
                (RemoteOpAmInputToValue::OneToOne(idx, val), size)
            }
            1 => {
                let idx = unsafe { &*(buf[size..].as_ptr() as *const usize) };
                size += std::mem::size_of::<usize>();
                let (vals, vals_bytes) = RemoteOpAmInputToValue::<T>::unpack_slice(&buf[size..]);
                size += vals_bytes;
                (RemoteOpAmInputToValue::OneToMany(idx, vals), size)
            }
            2 => {
                let (idxs, idxs_bytes) = RemoteOpAmInputToValue::<T>::unpack_slice(&buf[size..]);
                size += idxs_bytes;
                let val = unsafe { &*(buf[size..].as_ptr() as *const T) };
                size += std::mem::size_of::<T>();

                (RemoteOpAmInputToValue::ManyToOne(idxs, val), size)
            }
            3 => {
                let (idxs, idxs_bytes) = RemoteOpAmInputToValue::<T>::unpack_slice(&buf[size..]);
                size += idxs_bytes;
                let (vals, vals_bytes) = RemoteOpAmInputToValue::<T>::unpack_slice(&buf[size..]);
                size += vals_bytes;

                (RemoteOpAmInputToValue::ManyToMany(idxs, vals), size)
            }
            _ => {
                panic!("unrecognized OpAmInputToValue Type");
            }
        }
    }
}

#[derive(Clone, serde::Serialize, Debug)]
pub enum OpInputEnum<'a, T: Dist> {
    Val(T),
    Slice(&'a [T]),
    Vec(Vec<T>),
    #[serde(serialize_with = "LamellarMemoryRegion::serialize_local_data")]
    MemoryRegion(LamellarMemoryRegion<T>),
    // UnsafeArray(UnsafeArray<T>),
    // ReadOnlyArray(ReadOnlyArray<T>),
    // AtomicArray(AtomicArray<T>),
    NativeAtomicArray(NativeAtomicLocalData<T>),
    GenericAtomicArray(GenericAtomicLocalData<T>),
    LocalLockAtomicArray(LocalLockAtomicLocalData<'a, T>),
}

impl<'a, T: Dist> OpInputEnum<'_, T> {
    #[tracing::instrument(skip_all)]
    pub(crate) fn iter(&self) -> Box<dyn Iterator<Item = T> + '_> {
        match self {
            OpInputEnum::Val(v) => Box::new(std::iter::repeat(v).map(|elem| *elem)),
            OpInputEnum::Slice(s) => Box::new(s.iter().map(|elem| *elem)),
            OpInputEnum::Vec(v) => Box::new(v.iter().map(|elem| *elem)),
            OpInputEnum::MemoryRegion(mr) => Box::new(
                unsafe { mr.as_slice() }
                    .expect("memregion not local")
                    .iter()
                    .map(|elem| *elem),
            ),
            // OpInputEnum::UnsafeArray(a) => Box::new(unsafe{a.local_data()}.iter().map(|elem| *elem)),
            // OpInputEnum::ReadOnlyArray(a) => Box::new(a.local_data().iter().map(|elem| *elem)),
            // OpInputEnum::AtomicArray(a) => Box::new(a.local_data().iter().map(|elem| elem.load())),
            OpInputEnum::NativeAtomicArray(a) => Box::new(a.iter().map(|elem| elem.load())),
            OpInputEnum::GenericAtomicArray(a) => Box::new(a.iter().map(|elem| elem.load())),
            OpInputEnum::LocalLockAtomicArray(a) => Box::new(a.iter().map(|elem| *elem)),
        }
    }
    #[tracing::instrument(skip_all)]
    pub(crate) fn len(&self) -> usize {
        match self {
            OpInputEnum::Val(_) => 1,
            OpInputEnum::Slice(s) => s.len(),
            OpInputEnum::Vec(v) => v.len(),
            OpInputEnum::MemoryRegion(mr) => {
                unsafe { mr.as_slice() }.expect("memregion not local").len()
            }
            // OpInputEnum::UnsafeArray(a) => unsafe{a.local_data()}.len(),
            // OpInputEnum::ReadOnlyArray(a) => a.local_data().len(),
            // OpInputEnum::AtomicArray(a) => unsafe{a.__local_as_slice().len()},
            OpInputEnum::NativeAtomicArray(a) => a.len(),
            OpInputEnum::GenericAtomicArray(a) => a.len(),
            OpInputEnum::LocalLockAtomicArray(a) => a.len(),
        }
    }

    #[tracing::instrument(skip_all)]
    pub(crate) fn first(&self) -> T {
        match self {
            OpInputEnum::Val(v) => *v,
            OpInputEnum::Slice(s) => *s.first().expect("slice is empty"),
            OpInputEnum::Vec(v) => *v.first().expect("vec is empty"),
            OpInputEnum::MemoryRegion(mr) => *unsafe { mr.as_slice() }
                .expect("memregion not local")
                .first()
                .expect("memregion is empty"),
            OpInputEnum::NativeAtomicArray(a) => a.at(0).load(),
            OpInputEnum::GenericAtomicArray(a) => a.at(0).load(),
            OpInputEnum::LocalLockAtomicArray(a) => *a.first().expect("array is empty"),
        }
    }
}

pub trait OpInput<'a, T: Dist> {
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize); //(Vec<(Box<dyn Iterator<Item = T>    + '_>,usize)>,usize);
}

impl<'a, T: Dist> OpInput<'a, T> for T {
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        (vec![OpInputEnum::Val(self)], 1)
    }
}
impl<'a, T: Dist> OpInput<'a, T> for &T {
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        (vec![OpInputEnum::Val(*self)], 1)
    }
}

impl<'a, T: Dist> OpInput<'a, T> for &'a [T] {
    #[tracing::instrument(skip_all)]
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        let len = self.len();
        let mut iters = vec![];
        let num_per_batch = match std::env::var("LAMELLAR_OP_BATCH") {
            Ok(n) => n.parse::<usize>().unwrap(), //+ 1 to account for main thread
            Err(_) => 10000,                      //+ 1 to account for main thread
        };
        let num = len / num_per_batch;
        for i in 0..num {
            let temp = &self[(i * num_per_batch)..((i + 1) * num_per_batch)];
            iters.push(OpInputEnum::Slice(temp));
        }
        let rem = len % num_per_batch;
        if rem > 0 {
            let temp = &self[(num * num_per_batch)..(num * num_per_batch) + rem];
            iters.push(OpInputEnum::Slice(temp));
        }
        (iters, len)
    }
}

impl<'a, T: Dist> OpInput<'a, T> for &'a Vec<T> {
    #[tracing::instrument(skip_all)]
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        (&self[..]).as_op_input()
    }
}

impl<'a, T: Dist> OpInput<'a, T> for Vec<T> {
    #[tracing::instrument(skip_all)]
    fn as_op_input(mut self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        let len = self.len();
        let mut iters = vec![];
        let num_per_batch = match std::env::var("LAMELLAR_OP_BATCH") {
            Ok(n) => n.parse::<usize>().unwrap(),
            Err(_) => 10000,
        };
        let num = (len as f32 / num_per_batch as f32).ceil() as usize;
        // println!("num: {}", num);
        for i in (1..num).rev() {
            let temp = self.split_off(i * num_per_batch);
            // println!("temp: {:?} {:?} {:?}", temp,i ,i * num_per_batch);
            iters.push(OpInputEnum::Vec(temp));
        }
        let rem = len % num_per_batch;
        // println!("rem: {} {:?}", rem,self);
        if rem > 0 || num == 1 {
            iters.push(OpInputEnum::Vec(self));
        }
        iters.reverse(); //the indice slices get pushed in from the back, but we want to return in order
        (iters, len)
    }
}

impl<'a, T: Dist> OpInput<'a, T> for &LamellarMemoryRegion<T> {
    #[tracing::instrument(skip_all)]
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        let slice = unsafe { self.as_slice() }.expect("mem region not local");
        let len = slice.len();
        let mut iters = vec![];
        let num_per_batch = match std::env::var("LAMELLAR_OP_BATCH") {
            Ok(n) => n.parse::<usize>().unwrap(),
            Err(_) => 10000,
        };
        let num = len / num_per_batch;
        for i in 0..num {
            let sub_region = self.sub_region((i * num_per_batch)..((i + 1) * num_per_batch));
            iters.push(OpInputEnum::MemoryRegion(sub_region));
        }
        let rem = len % num_per_batch;
        if rem > 0 {
            let sub_region = self.sub_region((num * num_per_batch)..(num * num_per_batch) + rem);
            iters.push(OpInputEnum::MemoryRegion(sub_region));
        }
        (iters, len)
    }
}

impl<'a, T: Dist> OpInput<'a, T> for &LocalMemoryRegion<T> {
    #[tracing::instrument(skip_all)]
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        LamellarMemoryRegion::from(self).as_op_input()
    }
}

impl<'a, T: Dist> OpInput<'a, T> for LocalMemoryRegion<T> {
    #[tracing::instrument(skip_all)]
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        LamellarMemoryRegion::from(self).as_op_input()
    }
}

impl<'a, T: Dist> OpInput<'a, T> for &SharedMemoryRegion<T> {
    #[tracing::instrument(skip_all)]
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        LamellarMemoryRegion::from(self).as_op_input()
    }
}

impl<'a, T: Dist> OpInput<'a, T> for SharedMemoryRegion<T> {
    #[tracing::instrument(skip_all)]
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        LamellarMemoryRegion::from(self).as_op_input()
    }
}

impl<'a, T: Dist> OpInput<'a, T> for &'a UnsafeArray<T> {
    #[tracing::instrument(skip_all)]
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        let slice = unsafe { self.local_as_slice() };
        // let slice = unsafe { std::mem::transmute::<&'_ [T], &'a [T]>(slice) }; //this is safe in the context of buffered_ops because we know we wait for all the requests to submit before we return
        slice.as_op_input()
    }
}

// impl<'a, T: Dist> OpInput<'a, T> for UnsafeArray<T>{
//     fn  as_op_input(self) -> (Vec<OpInputEnum<'a,T>>,usize) {
//         unsafe{self.clone().local_as_slice().as_op_input()}
//     }
// }

impl<'a, T: Dist> OpInput<'a, T> for &'a ReadOnlyArray<T> {
    #[tracing::instrument(skip_all)]
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        let slice = self.local_as_slice();
        // let slice = unsafe { std::mem::transmute::<&'_ [T], &'a [T]>(slice) }; //this is safe in the context of buffered_ops because we know we wait for all the requests to submit before we return
        slice.as_op_input()
    }
}

// impl<'a, T: Dist> OpInput<'a, T> for ReadOnlyArray<T>{
//     fn  as_op_input(self) -> (Vec<OpInputEnum<'a,T>>,usize) {
//         (&self).as_op_input()
//     }
// }

impl<'a, T: Dist> OpInput<'a, T> for &'a LocalLockAtomicArray<T> {
    #[tracing::instrument(skip_all)]
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        // let slice=unsafe{self.__local_as_slice()};
        let slice = self.read_local_data();
        let len = slice.len();
        let mut iters = vec![];
        let my_pe = self.my_pe();
        if let Some(_start_index) = self.array.inner.start_index_for_pe(my_pe) {
            //just to check that the array is local
            let num_per_batch = match std::env::var("LAMELLAR_OP_BATCH") {
                Ok(n) => n.parse::<usize>().unwrap(), //+ 1 to account for main thread
                Err(_) => 10000,                      //+ 1 to account for main thread
            };
            let num = len / num_per_batch;
            // println!("num: {} len {:?} npb {:?}", num, len, num_per_batch);
            for i in 0..num {
                // let sub_array = self.sub_array((start_index+(i*num_per_batch))..(start_index+((i+1)*num_per_batch)));
                let sub_data = self
                    .read_local_data()
                    .into_sub_data(i * num_per_batch, (i + 1) * num_per_batch);
                iters.push(OpInputEnum::LocalLockAtomicArray(sub_data));
            }
            let rem = len % num_per_batch;
            if rem > 0 {
                // let sub_array = self.sub_array((start_index+(num*num_per_batch))..(start_index+(num*num_per_batch) + rem));
                let sub_data = self
                    .read_local_data()
                    .into_sub_data(num * num_per_batch, num * num_per_batch + rem);
                iters.push(OpInputEnum::LocalLockAtomicArray(sub_data));
            }
        }
        (iters, len)
    }
}

// impl<'a, T: Dist> OpInput<'a, T> for LocalLockAtomicArray<T>{
//     fn  as_op_input(self) -> (Vec<OpInputEnum<'a,T>>,usize) {
//         (&self).as_op_input()
//     }
// }

impl<'a, T: Dist + ElementOps> OpInput<'a, T> for &AtomicArray<T> {
    #[tracing::instrument(skip_all)]
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        match self {
            &AtomicArray::GenericAtomicArray(ref a) => a.as_op_input(),
            &AtomicArray::NativeAtomicArray(ref a) => a.as_op_input(),
        }
    }
}

// impl<'a, T: Dist + ElementOps> OpInput<'a, T> for AtomicArray<T>{
//     fn  as_op_input(self) -> (Vec<OpInputEnum<'a,T>>,usize) {
//         (&self).as_op_input()
//     }
// }

impl<'a, T: Dist + ElementOps> OpInput<'a, T> for &GenericAtomicArray<T> {
    #[tracing::instrument(skip_all)]
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        let slice = unsafe { self.__local_as_slice() };
        let len = slice.len();
        let local_data = self.local_data();
        let mut iters = vec![];
        let my_pe = self.my_pe();
        if let Some(_start_index) = self.array.inner.start_index_for_pe(my_pe) {
            //just to check that the array is local
            let num_per_batch = match std::env::var("LAMELLAR_OP_BATCH") {
                Ok(n) => n.parse::<usize>().unwrap(), //+ 1 to account for main thread
                Err(_) => 10000,                      //+ 1 to account for main thread
            };
            let num = len / num_per_batch;
            for i in 0..num {
                // let sub_array = self.sub_array((start_index+(i*num_per_batch))..(start_index+((i+1)*num_per_batch)));
                let sub_data = local_data.sub_data(i * num_per_batch, (i + 1) * num_per_batch);
                iters.push(OpInputEnum::GenericAtomicArray(sub_data));
            }
            let rem = len % num_per_batch;
            if rem > 0 {
                // let sub_array = self.sub_array((start_index+(num*num_per_batch))..(start_index+(num*num_per_batch) + rem));
                let sub_data =
                    local_data.sub_data(num * num_per_batch, (num * num_per_batch) + rem);
                iters.push(OpInputEnum::GenericAtomicArray(sub_data));
            }
        }
        (iters, len)
    }
}

// impl<'a, T: Dist + ElementOps> OpInput<'a, T> for GenericAtomicArray<T>{
//     fn  as_op_input(self) -> (Vec<OpInputEnum<'a,T>>,usize) {
//         (&self).as_op_input()
//     }
// }

impl<'a, T: Dist + ElementOps> OpInput<'a, T> for &NativeAtomicArray<T> {
    #[tracing::instrument(skip_all)]
    fn as_op_input(self) -> (Vec<OpInputEnum<'a, T>>, usize) {
        let slice = unsafe { self.__local_as_slice() };
        let len = slice.len();
        let local_data = self.local_data();
        let mut iters = vec![];
        let my_pe = self.my_pe();
        if let Some(_start_index) = self.array.inner.start_index_for_pe(my_pe) {
            //just to check that the array is local
            let num_per_batch = match std::env::var("LAMELLAR_OP_BATCH") {
                Ok(n) => n.parse::<usize>().unwrap(), //+ 1 to account for main thread
                Err(_) => 10000,                      //+ 1 to account for main thread
            };
            let num = len / num_per_batch;
            // println!("num: {} len {:?} npb {:?}", num, len, num_per_batch);
            for i in 0..num {
                // let sub_array = self.sub_array((start_index+(i*num_per_batch))..(start_index+((i+1)*num_per_batch)));
                let sub_data = local_data.sub_data(i * num_per_batch, (i + 1) * num_per_batch);
                // for j in sub_data.clone().into_iter() {
                //     println!("{:?} {:?}",i, j);
                // }
                iters.push(OpInputEnum::NativeAtomicArray(sub_data));
            }
            let rem = len % num_per_batch;
            if rem > 0 {
                // let sub_array = self.sub_array((start_index+(num*num_per_batch))..(start_index+(num*num_per_batch) + rem));
                let sub_data =
                    local_data.sub_data(num * num_per_batch, (num * num_per_batch) + rem);
                iters.push(OpInputEnum::NativeAtomicArray(sub_data));
            }
        }
        (iters, len)
    }
}

// impl<'a, T: Dist + ElementOps> OpInput<'a, T> for NativeAtomicArray<T>{
//     fn  as_op_input(self) -> (Vec<OpInputEnum<'a,T>>,usize) {
//         (&self).as_op_input()
//     }
// }

pub trait BufferOp: Sync + Send {
    fn add_ops(
        &self,
        op: *const u8,
        op_data: *const u8,
        team: Pin<Arc<LamellarTeamRT>>,
    ) -> (bool, Arc<AtomicBool>);
    fn add_fetch_ops(
        &self,
        pe: usize,
        op: *const u8,
        op_data: *const u8,
        req_ids: &Vec<usize>,
        res_map: OpResults,
        team: Pin<Arc<LamellarTeamRT>>,
    ) -> (bool, Arc<AtomicBool>, Option<OpResultOffsets>);

    fn into_arc_am(
        &self,
        pe: usize,
        sub_array: std::ops::Range<usize>,
    ) -> (
        Vec<LamellarArcAm>,
        usize,
        Arc<AtomicBool>,
        Arc<Mutex<Vec<u8>>>,
    );
}

pub type OpResultOffsets = Vec<(usize, usize, usize)>; //reqid,offset,len
pub struct OpReqOffsets(Arc<Mutex<HashMap<usize, OpResultOffsets>>>); //pe
impl OpReqOffsets {
    #[tracing::instrument(skip_all)]
    pub(crate) fn new() -> Self {
        OpReqOffsets(Arc::new(Mutex::new(HashMap::new())))
    }
    #[tracing::instrument(skip_all)]
    pub fn insert(&self, index: usize, indices: OpResultOffsets) {
        let mut map = self.0.lock();
        map.insert(index, indices);
    }
    #[tracing::instrument(skip_all)]
    pub(crate) fn lock(&self) -> parking_lot::MutexGuard<HashMap<usize, OpResultOffsets>> {
        self.0.lock()
    }
}

impl Clone for OpReqOffsets {
    fn clone(&self) -> Self {
        OpReqOffsets(self.0.clone())
    }
}

impl std::fmt::Debug for OpReqOffsets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let map = self.0.lock();
        write!(f, "{:?} {:?}", map.len(), map)
    }
}

pub type PeOpResults = Arc<Mutex<Vec<u8>>>;
pub struct OpResults(Arc<Mutex<HashMap<usize, PeOpResults>>>);
impl OpResults {
    #[tracing::instrument(skip_all)]
    pub(crate) fn new() -> Self {
        OpResults(Arc::new(Mutex::new(HashMap::new())))
    }
    #[tracing::instrument(skip_all)]
    pub fn insert(&self, index: usize, val: PeOpResults) {
        let mut map = self.0.lock();
        map.insert(index, val);
    }
    #[tracing::instrument(skip_all)]
    pub(crate) fn lock(&self) -> parking_lot::MutexGuard<HashMap<usize, PeOpResults>> {
        self.0.lock()
    }
}

impl Clone for OpResults {
    fn clone(&self) -> Self {
        OpResults(self.0.clone())
    }
}

impl std::fmt::Debug for OpResults {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let map = self.0.lock();
        write!(f, "{:?} {:?}", map.len(), map)
    }
}

pub(crate) struct ArrayOpHandle {
    pub(crate) reqs: Vec<Box<ArrayOpHandleInner>>,
}

#[derive(Debug)]
pub(crate) struct ArrayOpHandleInner {
    pub(crate) complete: Vec<Arc<AtomicBool>>,
}

pub(crate) struct ArrayOpFetchHandle<T: Dist> {
    pub(crate) req: Box<ArrayOpFetchHandleInner<T>>,
}

pub(crate) struct ArrayOpBatchFetchHandle<T: Dist> {
    pub(crate) reqs: Vec<Box<ArrayOpFetchHandleInner<T>>>,
}

#[derive(Debug)]
pub(crate) struct ArrayOpFetchHandleInner<T: Dist> {
    pub(crate) indices: OpReqOffsets,
    pub(crate) complete: Vec<Arc<AtomicBool>>,
    pub(crate) results: OpResults,
    pub(crate) req_cnt: usize,
    pub(crate) _phantom: PhantomData<T>,
}

pub(crate) struct ArrayOpResultHandle<T: Dist> {
    pub(crate) req: Box<ArrayOpResultHandleInner<T>>,
}
pub(crate) struct ArrayOpBatchResultHandle<T: Dist> {
    pub(crate) reqs: Vec<Box<ArrayOpResultHandleInner<T>>>,
}

#[derive(Debug)]
pub(crate) struct ArrayOpResultHandleInner<T> {
    pub(crate) indices: OpReqOffsets,
    pub(crate) complete: Vec<Arc<AtomicBool>>,
    pub(crate) results: OpResults,
    pub(crate) req_cnt: usize,
    pub(crate) _phantom: PhantomData<T>,
}

#[async_trait]
impl LamellarRequest for ArrayOpHandle {
    type Output = ();
    #[tracing::instrument(skip_all)]
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        for req in self.reqs.drain(..) {
            req.into_future().await;
        }
        ()
    }
    #[tracing::instrument(skip_all)]
    fn get(&self) -> Self::Output {
        for req in &self.reqs {
            req.get();
        }
        ()
    }
}

#[async_trait]
impl LamellarRequest for ArrayOpHandleInner {
    type Output = ();
    #[tracing::instrument(skip_all)]
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        for comp in self.complete {
            while comp.load(Ordering::Relaxed) == false {
                async_std::task::yield_now().await;
            }
        }
        ()
    }
    #[tracing::instrument(skip_all)]
    fn get(&self) -> Self::Output {
        for comp in &self.complete {
            while comp.load(Ordering::Relaxed) == false {
                std::thread::yield_now();
            }
        }
        ()
    }
}

#[async_trait]
impl<T: Dist> LamellarRequest for ArrayOpFetchHandle<T> {
    type Output = T;
    #[tracing::instrument(skip_all)]
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        self.req
            .into_future()
            .await
            .pop()
            .expect("should have a single request")
    }
    #[tracing::instrument(skip_all)]
    fn get(&self) -> Self::Output {
        self.req.get().pop().expect("should have a single request")
    }
}

#[async_trait]
impl<T: Dist> LamellarRequest for ArrayOpBatchFetchHandle<T> {
    type Output = Vec<T>;
    #[tracing::instrument(skip_all)]
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        let mut res = vec![];
        for req in self.reqs.drain(..) {
            res.extend(req.into_future().await);
        }
        res
    }
    #[tracing::instrument(skip_all)]
    fn get(&self) -> Self::Output {
        let mut res = vec![];
        for req in &self.reqs {
            res.extend(req.get());
        }
        // println!("res: {:?}",res);
        res
    }
}

impl<T: Dist> ArrayOpFetchHandleInner<T> {
    #[tracing::instrument(skip_all)]
    fn get_result(&self) -> Vec<T> {
        if self.req_cnt > 0 {
            let mut res_vec = Vec::with_capacity(self.req_cnt);
            unsafe {
                res_vec.set_len(self.req_cnt);
            }
            // println!("req_cnt: {:?}", self.req_cnt);

            for (pe, res) in self.results.lock().iter() {
                let res = res.lock();
                for (rid, offset, len) in self.indices.lock().get(pe).unwrap().iter() {
                    let len = *len;
                    if len == std::mem::size_of::<T>() + 1 {
                        panic!(
                            "unexpected results len {:?} {:?}",
                            len,
                            std::mem::size_of::<T>() + 1
                        );
                    }
                    let res_t = unsafe {
                        std::slice::from_raw_parts(
                            res.as_ptr().offset(*offset as isize) as *const T,
                            len / std::mem::size_of::<T>(),
                        )
                    };
                    // println!("rid {:?} offset {:?} len {:?} {:?}",rid,offset,len,res.len());
                    // println!("res {:?} {:?}",res.len(),&res[offset..offset+len]);
                    // println!("res {:?} {:?}",res_t,res_t.len());
                    res_vec[*rid] = res_t[0];
                }
            }
            res_vec
        } else {
            vec![]
        }
    }
}

#[async_trait]
impl<T: Dist> LamellarRequest for ArrayOpFetchHandleInner<T> {
    type Output = Vec<T>;
    #[tracing::instrument(skip_all)]
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        for comp in &self.complete {
            while comp.load(Ordering::Relaxed) == false {
                async_std::task::yield_now().await;
            }
        }
        self.get_result()
    }
    #[tracing::instrument(skip_all)]
    fn get(&self) -> Self::Output {
        for comp in &self.complete {
            while comp.load(Ordering::Relaxed) == false {
                std::thread::yield_now();
            }
        }
        self.get_result()
    }
}

#[async_trait]
impl<T: Dist> LamellarRequest for ArrayOpResultHandle<T> {
    type Output = Result<T, T>;
    #[tracing::instrument(skip_all)]
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        self.req
            .into_future()
            .await
            .pop()
            .expect("should have a single request")
    }
    #[tracing::instrument(skip_all)]
    fn get(&self) -> Self::Output {
        self.req.get().pop().expect("should have a single request")
    }
}

#[async_trait]
impl<T: Dist> LamellarRequest for ArrayOpBatchResultHandle<T> {
    type Output = Vec<Result<T, T>>;
    #[tracing::instrument(skip_all)]
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        // println!("num_reqs: {}",self.reqs.len());
        let mut res = vec![];
        for req in self.reqs.drain(..) {
            res.extend(req.into_future().await);
        }
        res
    }
    #[tracing::instrument(skip_all)]
    fn get(&self) -> Self::Output {
        let mut res = vec![];
        for req in &self.reqs {
            res.extend(req.get());
        }
        res
    }
}

impl<T: Dist> ArrayOpResultHandleInner<T> {
    #[tracing::instrument(skip_all)]
    fn get_result(&self) -> Vec<Result<T, T>> {
        // println!("req_cnt: {:?}", self.req_cnt);
        if self.req_cnt > 0 {
            let mut res_vec = Vec::with_capacity(self.req_cnt);
            unsafe {
                res_vec.set_len(self.req_cnt);
            }

            for (pe, res) in self.results.lock().iter() {
                let res = res.lock();
                // println!("{pe} {:?}",res.len());
                // let mut rids = std::collections::HashSet::new();
                let res_offsets_lock = self.indices.lock();
                let res_offsets =res_offsets_lock.get(pe).unwrap();
                // println!("{pe} {:?} {:?}",res_offsets[0],res_offsets.last());
                for (rid, offset, len) in res_offsets.iter() {
                    // if rids.contains(rid){
                    //     println!("uhhh ohhhhh not sure this should be possible {:?}",rid);
                    // }
                    // else{
                    //     rids.insert(rid);
                    // }
                    let ok: bool;
                    let mut offset = *offset;
                    let mut len = *len;
                    if len == std::mem::size_of::<T>() + 1 {
                        ok = res[offset] == 0;
                        offset += 1;
                        len -= 1;
                    } else {
                        panic!(
                            "unexpected results len {:?} {:?}",
                            len,
                            std::mem::size_of::<T>() + 1
                        );
                    };
                    let res_t = unsafe {
                        std::slice::from_raw_parts(
                            res.as_ptr().offset(offset as isize) as *const T,
                            len / std::mem::size_of::<T>(),
                        )
                    };

                    if ok {
                        res_vec[*rid] = Ok(res_t[0]);
                    } else {
                        res_vec[*rid] = Err(res_t[0]);
                    }
                }
            }
            res_vec
        } else {
            vec![]
        }
    }
}

#[async_trait]
impl<T: Dist> LamellarRequest for ArrayOpResultHandleInner<T> {
    type Output = Vec<Result<T, T>>;
    #[tracing::instrument(skip_all)]
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        // println!("comp size: {}",self.complete.len());
        for comp in &self.complete {
            while comp.load(Ordering::Relaxed) == false {
                async_std::task::yield_now().await;
            }
        }
        self.get_result()
    }
    #[tracing::instrument(skip_all)]
    fn get(&self) -> Self::Output {
        for comp in &self.complete {
            while comp.load(Ordering::Relaxed) == false {
                std::thread::yield_now();
            }
        }
        self.get_result()
    }
}

pub trait ElementOps: AmDist + Dist + Sized {}
impl<T> ElementOps for T where T: AmDist + Dist {}

pub trait ElementArithmeticOps:
    std::ops::AddAssign
    + std::ops::SubAssign
    + std::ops::MulAssign
    + std::ops::DivAssign
    + AmDist
    + Dist
    + Sized
{
}
impl<T> ElementArithmeticOps for T where
    T: std::ops::AddAssign
        + std::ops::SubAssign
        + std::ops::MulAssign
        + std::ops::DivAssign
        + AmDist
        + Dist
{
}

pub trait ElementBitWiseOps:
    std::ops::BitAndAssign + std::ops::BitOrAssign + AmDist + Dist + Sized
{
}
impl<T> ElementBitWiseOps for T where
    T: std::ops::BitAndAssign + std::ops::BitOrAssign + AmDist + Dist
{
}

pub trait ElementCompareEqOps: std::cmp::Eq + AmDist + Dist + Sized {}
impl<T> ElementCompareEqOps for T where T: std::cmp::Eq + AmDist + Dist {}

pub trait ElementComparePartialEqOps: std::cmp::PartialEq + AmDist + Dist + Sized {}
impl<T> ElementComparePartialEqOps for T where T: std::cmp::PartialEq + AmDist + Dist {}

pub trait ReadOnlyOps<T: ElementOps>: private::LamellarArrayPrivate<T> {
    #[tracing::instrument(skip_all)]
    fn load<'a>(&self, index: usize) -> Pin<Box<dyn Future<Output = T> + Send>> {
        let dummy_val = self.inner_array().dummy_val(); //we dont actually do anything with this except satisfy apis;
        self.inner_array()
            .initiate_fetch_op(dummy_val, index, ArrayOpCmd::Load)
    }
    #[tracing::instrument(skip_all)]
    fn batch_load<'a>(
        &self,
        index: impl OpInput<'a, usize>,
    ) -> Pin<Box<dyn Future<Output = Vec<T>> + Send>> {
        let dummy_val = self.inner_array().dummy_val(); //we dont actually do anything with this except satisfy apis;
        self.inner_array()
            .initiate_batch_fetch_op(dummy_val, index, ArrayOpCmd::Load)
    }
}

pub trait AccessOps<T: ElementOps>: private::LamellarArrayPrivate<T> {
    #[tracing::instrument(skip_all)]
    fn store<'a>(&self, index: usize, val: T) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.inner_array()
            .initiate_op(val, index, ArrayOpCmd::Store)
    }
    #[tracing::instrument(skip_all)]
    fn batch_store<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.inner_array()
            .initiate_op(val, index, ArrayOpCmd::Store)
    }
    #[tracing::instrument(skip_all)]
    fn swap<'a>(&self, index: usize, val: T) -> Pin<Box<dyn Future<Output = T> + Send>> {
        self.inner_array()
            .initiate_fetch_op(val, index, ArrayOpCmd::Swap)
    }
    #[tracing::instrument(skip_all)]
    fn batch_swap<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> Pin<Box<dyn Future<Output = Vec<T>> + Send>> {
        self.inner_array()
            .initiate_batch_fetch_op(val, index, ArrayOpCmd::Swap)
    }
}

pub trait ArithmeticOps<T: Dist + ElementArithmeticOps>: private::LamellarArrayPrivate<T> {
    #[tracing::instrument(skip_all)]
    fn add(&self, index: usize, val: T) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.inner_array().initiate_op(val, index, ArrayOpCmd::Add)
    }
    #[tracing::instrument(skip_all)]
    fn batch_add<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.inner_array().initiate_op(val, index, ArrayOpCmd::Add)
    }
    #[tracing::instrument(skip_all)]
    fn fetch_add(&self, index: usize, val: T) -> Pin<Box<dyn Future<Output = T> + Send>> {
        self.inner_array()
            .initiate_fetch_op(val, index, ArrayOpCmd::FetchAdd)
    }
    #[tracing::instrument(skip_all)]
    fn batch_fetch_add<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> Pin<Box<dyn Future<Output = Vec<T>> + Send>> {
        self.inner_array()
            .initiate_batch_fetch_op(val, index, ArrayOpCmd::FetchAdd)
    }

    #[tracing::instrument(skip_all)]
    fn sub<'a>(&self, index: usize, val: T) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.inner_array().initiate_op(val, index, ArrayOpCmd::Sub)
    }
    #[tracing::instrument(skip_all)]
    fn batch_sub<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.inner_array().initiate_op(val, index, ArrayOpCmd::Sub)
    }
    #[tracing::instrument(skip_all)]
    fn fetch_sub<'a>(&self, index: usize, val: T) -> Pin<Box<dyn Future<Output = T> + Send>> {
        self.inner_array()
            .initiate_fetch_op(val, index, ArrayOpCmd::FetchSub)
    }
    #[tracing::instrument(skip_all)]
    fn batch_fetch_sub<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> Pin<Box<dyn Future<Output = Vec<T>> + Send>> {
        self.inner_array()
            .initiate_batch_fetch_op(val, index, ArrayOpCmd::FetchSub)
    }
    #[tracing::instrument(skip_all)]
    fn mul<'a>(&self, index: usize, val: T) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.inner_array().initiate_op(val, index, ArrayOpCmd::Mul)
    }
    #[tracing::instrument(skip_all)]
    fn batch_mul<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.inner_array().initiate_op(val, index, ArrayOpCmd::Mul)
    }
    #[tracing::instrument(skip_all)]
    fn fetch_mul<'a>(&self, index: usize, val: T) -> Pin<Box<dyn Future<Output = T> + Send>> {
        self.inner_array()
            .initiate_fetch_op(val, index, ArrayOpCmd::FetchMul)
    }
    #[tracing::instrument(skip_all)]
    fn batch_fetch_mul<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> Pin<Box<dyn Future<Output = Vec<T>> + Send>> {
        self.inner_array()
            .initiate_batch_fetch_op(val, index, ArrayOpCmd::FetchMul)
    }
    #[tracing::instrument(skip_all)]
    fn div<'a>(&self, index: usize, val: T) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.inner_array().initiate_op(val, index, ArrayOpCmd::Div)
    }
    #[tracing::instrument(skip_all)]
    fn batch_div<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.inner_array().initiate_op(val, index, ArrayOpCmd::Div)
    }
    #[tracing::instrument(skip_all)]
    fn fetch_div<'a>(&self, index: usize, val: T) -> Pin<Box<dyn Future<Output = T> + Send>> {
        self.inner_array()
            .initiate_fetch_op(val, index, ArrayOpCmd::FetchDiv)
    }
    #[tracing::instrument(skip_all)]
    fn batch_fetch_div<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> Pin<Box<dyn Future<Output = Vec<T>> + Send>> {
        self.inner_array()
            .initiate_batch_fetch_op(val, index, ArrayOpCmd::FetchDiv)
    }
}

pub trait BitWiseOps<T: ElementBitWiseOps>: private::LamellarArrayPrivate<T> {
    #[tracing::instrument(skip_all)]
    fn bit_and<'a>(&self, index: usize, val: T) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.inner_array().initiate_op(val, index, ArrayOpCmd::And)
    }
    #[tracing::instrument(skip_all)]
    fn batch_bit_and<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.inner_array().initiate_op(val, index, ArrayOpCmd::And)
    }
    #[tracing::instrument(skip_all)]
    fn fetch_bit_and<'a>(&self, index: usize, val: T) -> Pin<Box<dyn Future<Output = T> + Send>> {
        self.inner_array()
            .initiate_fetch_op(val, index, ArrayOpCmd::FetchAnd)
    }
    #[tracing::instrument(skip_all)]
    fn batch_fetch_bit_and<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> Pin<Box<dyn Future<Output = Vec<T>> + Send>> {
        self.inner_array()
            .initiate_batch_fetch_op(val, index, ArrayOpCmd::FetchAnd)
    }
    #[tracing::instrument(skip_all)]
    fn bit_or<'a>(&self, index: usize, val: T) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.inner_array().initiate_op(val, index, ArrayOpCmd::Or)
    }
    #[tracing::instrument(skip_all)]
    fn batch_bit_or<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        self.inner_array().initiate_op(val, index, ArrayOpCmd::Or)
    }
    #[tracing::instrument(skip_all)]
    fn fetch_bit_or<'a>(&self, index: usize, val: T) -> Pin<Box<dyn Future<Output = T> + Send>> {
        self.inner_array()
            .initiate_fetch_op(val, index, ArrayOpCmd::FetchOr)
    }
    #[tracing::instrument(skip_all)]
    fn batch_fetch_bit_or<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        val: impl OpInput<'a, T>,
    ) -> Pin<Box<dyn Future<Output = Vec<T>> + Send>> {
        self.inner_array()
            .initiate_batch_fetch_op(val, index, ArrayOpCmd::FetchOr)
    }
}

pub trait CompareExchangeOps<T: ElementCompareEqOps>: private::LamellarArrayPrivate<T> {
    #[tracing::instrument(skip_all)]
    fn compare_exchange<'a>(
        &self,
        index: usize,
        old: T,
        new: T,
    ) -> Pin<Box<dyn Future<Output = Result<T, T>> + Send>> {
        self.inner_array()
            .initiate_result_op(new, index, ArrayOpCmd::CompareExchange(old))
    }
    #[tracing::instrument(skip_all)]
    fn batch_compare_exchange<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        old: T,
        new: impl OpInput<'a, T>,
    ) -> Pin<Box<dyn Future<Output = Vec<Result<T, T>>> + Send>> {
        self.inner_array()
            .initiate_batch_result_op(new, index, ArrayOpCmd::CompareExchange(old))
    }
}

pub trait CompareExchangeEpsilonOps<T: ElementComparePartialEqOps>:
    private::LamellarArrayPrivate<T>
{
    #[tracing::instrument(skip_all)]
    fn compare_exchange_epsilon<'a>(
        &self,
        index: usize,
        old: T,
        new: T,
        eps: T,
    ) -> Pin<Box<dyn Future<Output = Result<T, T>> + Send>> {
        self.inner_array()
            .initiate_result_op(new, index, ArrayOpCmd::CompareExchangeEps(old, eps))
    }
    #[tracing::instrument(skip_all)]
    fn batch_compare_exchange_epsilon<'a>(
        &self,
        index: impl OpInput<'a, usize>,
        old: T,
        new: impl OpInput<'a, T>,
        eps: T,
    ) -> Pin<Box<dyn Future<Output = Vec<Result<T, T>>> + Send>> {
        self.inner_array().initiate_batch_result_op(
            new,
            index,
            ArrayOpCmd::CompareExchangeEps(old, eps),
        )
    }
}

// pub trait BufferOps<T: ElementOps>: private::LamellarArrayPrivate<T> {
//     #[tracing::instrument(skip_all)]
//     fn copy_from_buffer<'a, U: MyInto<LamellarArrayInput<T>>>(
//         &self,
//         index: usize,
//         buf: U,
//     ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
//         self.inner_array()
//             .initiate_op(buf, index, ArrayOpCmd::CopyFromBuff);
//     }
//     #[tracing::instrument(skip_all)]
//     fn batch_copy_from_buffer<'a, U: MyInto<LamellarArrayInput<T>>>(
//         &self,
//         index: impl OpInput<'a, usize>,
//         old: T,
//         buf: U,
//     ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
//         self.inner_array()
//             .initiate_batch_op(buf, index, ArrayOpCmd::CopyFromBuff)
//     }
// }
//perform the specified operation in place, returning the original value
pub trait LocalArithmeticOps<T: Dist + ElementArithmeticOps> {
    fn local_add(&self, index: usize, val: T) {
        self.local_fetch_add(index, val);
    }
    fn local_fetch_add(&self, index: usize, val: T) -> T;
    fn local_sub(&self, index: usize, val: T) {
        self.local_fetch_sub(index, val);
    }
    fn local_fetch_sub(&self, index: usize, val: T) -> T;
    fn local_mul(&self, index: usize, val: T) {
        self.local_fetch_mul(index, val);
    }
    fn local_fetch_mul(&self, index: usize, val: T) -> T;
    fn local_div(&self, index: usize, val: T) {
        self.local_fetch_div(index, val);
    }
    fn local_fetch_div(&self, index: usize, val: T) -> T;
}

pub trait LocalBitWiseOps<T: Dist + ElementBitWiseOps> {
    fn local_bit_and(&self, index: usize, val: T) {
        self.local_fetch_bit_and(index, val);
    }
    fn local_fetch_bit_and(&self, index: usize, val: T) -> T;
    fn local_bit_or(&self, index: usize, val: T) {
        self.local_fetch_bit_or(index, val);
    }
    fn local_fetch_bit_or(&self, index: usize, val: T) -> T;
}

pub trait LocalAtomicOps<T: Dist + ElementOps> {
    fn local_load(&self, index: usize, val: T) -> T;
    fn local_store(&self, index: usize, val: T);
    fn local_swap(&self, index: usize, val: T) -> T;
}

pub struct LocalOpResult<T: Dist> {
    val: T,
}

#[async_trait]
impl<T: Dist> LamellarArrayRequest for LocalOpResult<T> {
    type Output = T;
    async fn into_future(mut self: Box<Self>) -> Self::Output {
        self.val
    }
    fn wait(self: Box<Self>) -> Self::Output {
        self.val
    }
}

impl<T: ElementArithmeticOps> ArithmeticOps<T> for LamellarWriteArray<T> {}