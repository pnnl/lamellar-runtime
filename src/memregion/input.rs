use std::{ops::Range, sync::Arc};

use crate::{
    array::{LamellarArrayRdmaInput, TeamFrom},
    lamellae::{CommSlice, Remote},
    memregion::{
        Dist, LamellarMemoryRegion, OneSidedMemoryRegion, RegisteredMemoryRegion,
        RemoteMemoryRegion, SharedMemoryRegion, SubRegion,
    },
    LamellarTeam,
};

#[derive(Clone)]
pub enum MemregionRdmaInput<T: Remote> {
    LamellarMemRegion(LamellarMemoryRegion<T>),
    SharedMemRegion(SharedMemoryRegion<T>),
    LocalMemRegion(OneSidedMemoryRegion<T>),
    Owned(T),
    ArcVec((Arc<Vec<T>>, Range<usize>)),
}

#[derive(Clone)]
pub(crate) enum MemregionRdmaInputInner<T: Remote> {
    LamellarMemRegion(LamellarMemoryRegion<T>),
    SharedMemRegion(SharedMemoryRegion<T>),
    LocalMemRegion(OneSidedMemoryRegion<T>),
    Slice(CommSlice<T>),
    Owned(T),
    ArcVec((Arc<Vec<T>>, Range<usize>)),
}

impl<T: Remote> From<MemregionRdmaInput<T>> for MemregionRdmaInputInner<T> {
    fn from(input: MemregionRdmaInput<T>) -> Self {
        match input {
            MemregionRdmaInput::LamellarMemRegion(mr) => {
                MemregionRdmaInputInner::LamellarMemRegion(mr)
            }
            MemregionRdmaInput::SharedMemRegion(mr) => MemregionRdmaInputInner::SharedMemRegion(mr),
            MemregionRdmaInput::LocalMemRegion(mr) => MemregionRdmaInputInner::LocalMemRegion(mr),
            MemregionRdmaInput::Owned(v) => MemregionRdmaInputInner::Owned(v),
            MemregionRdmaInput::ArcVec((v, r)) => MemregionRdmaInputInner::ArcVec((v, r)),
        }
    }
}

impl<T: Dist> From<LamellarArrayRdmaInput<T>> for MemregionRdmaInput<T> {
    fn from(input: LamellarArrayRdmaInput<T>) -> Self {
        match input {
            LamellarArrayRdmaInput::LamellarMemRegion(mr) => {
                MemregionRdmaInput::LamellarMemRegion(mr)
            }
            LamellarArrayRdmaInput::SharedMemRegion(mr) => MemregionRdmaInput::SharedMemRegion(mr),
            LamellarArrayRdmaInput::LocalMemRegion(mr) => MemregionRdmaInput::LocalMemRegion(mr),
            LamellarArrayRdmaInput::Owned(v) => MemregionRdmaInput::Owned(v),
            LamellarArrayRdmaInput::OwnedVec(v) => {
                let len = v.len();
                MemregionRdmaInput::ArcVec((Arc::new(v), 0..len))
            }
        }
    }
}

impl<T: Remote> From<LamellarMemoryRegion<T>> for MemregionRdmaInput<T> {
    fn from(mem_region: LamellarMemoryRegion<T>) -> Self {
        MemregionRdmaInput::LamellarMemRegion(mem_region)
    }
}

impl<T: Remote> From<&LamellarMemoryRegion<T>> for MemregionRdmaInput<T> {
    fn from(mem_region: &LamellarMemoryRegion<T>) -> Self {
        MemregionRdmaInput::LamellarMemRegion(mem_region.clone())
    }
}

impl<T: Remote> From<OneSidedMemoryRegion<T>> for MemregionRdmaInput<T> {
    fn from(mem_region: OneSidedMemoryRegion<T>) -> Self {
        MemregionRdmaInput::LocalMemRegion(mem_region)
    }
}
impl<T: Remote> From<&OneSidedMemoryRegion<T>> for MemregionRdmaInput<T> {
    fn from(mem_region: &OneSidedMemoryRegion<T>) -> Self {
        MemregionRdmaInput::LocalMemRegion(mem_region.clone())
    }
}

impl<T: Remote> From<SharedMemoryRegion<T>> for MemregionRdmaInput<T> {
    fn from(mem_region: SharedMemoryRegion<T>) -> Self {
        MemregionRdmaInput::SharedMemRegion(mem_region)
    }
}

impl<T: Remote> From<&SharedMemoryRegion<T>> for MemregionRdmaInput<T> {
    fn from(mem_region: &SharedMemoryRegion<T>) -> Self {
        MemregionRdmaInput::SharedMemRegion(mem_region.clone())
    }
}

impl<T: Remote> From<&[T]> for MemregionRdmaInput<T> {
    fn from(slice: &[T]) -> Self {
        MemregionRdmaInput::ArcVec((Arc::new(slice.to_vec()), 0..slice.len()))
    }
}
impl<T: Remote> From<&Vec<T>> for MemregionRdmaInput<T> {
    fn from(slice: &Vec<T>) -> Self {
        MemregionRdmaInput::ArcVec((Arc::new(slice.clone()), 0..slice.len()))
    }
}
impl<T: Remote> From<Vec<T>> for MemregionRdmaInput<T> {
    fn from(slice: Vec<T>) -> Self {
        let len = slice.len();
        MemregionRdmaInput::ArcVec((Arc::new(slice), 0..len))
    }
}

impl<T: Remote> From<&T> for MemregionRdmaInput<T> {
    fn from(slice: &T) -> Self {
        MemregionRdmaInput::Owned(*slice)
    }
}
impl<T: Remote> From<T> for MemregionRdmaInput<T> {
    fn from(val: T) -> Self {
        MemregionRdmaInput::Owned(val)
    }
}

impl<T: Remote> MemregionRdmaInputInner<T> {
    pub(crate) fn as_slice(&self) -> &[T] {
        match self {
            MemregionRdmaInputInner::LamellarMemRegion(m) => unsafe { m.as_slice() },
            MemregionRdmaInputInner::SharedMemRegion(m) => unsafe { m.as_slice() },
            MemregionRdmaInputInner::LocalMemRegion(m) => unsafe { m.as_slice() },
            MemregionRdmaInputInner::Slice(s) => s.as_slice(),
            MemregionRdmaInputInner::Owned(v) => std::slice::from_ref(v),
            MemregionRdmaInputInner::ArcVec((v, r)) => &v[r.clone()],
        }
    }

    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        let byte_len = self.len() * std::mem::size_of::<T>();
        let mut bytes = Vec::with_capacity(byte_len);
        unsafe {
            bytes.set_len(byte_len);
            std::ptr::copy_nonoverlapping(self.as_ptr() as *const u8, bytes.as_mut_ptr(), byte_len);
        }
        bytes
    }

    pub(crate) fn as_ptr(&self) -> *const T {
        match self {
            MemregionRdmaInputInner::LamellarMemRegion(m) => unsafe { m.as_ptr().unwrap() },
            MemregionRdmaInputInner::SharedMemRegion(m) => unsafe { m.as_ptr().unwrap() },
            MemregionRdmaInputInner::LocalMemRegion(m) => unsafe { m.as_ptr().unwrap() },
            MemregionRdmaInputInner::Slice(s) => s.as_ptr(),
            MemregionRdmaInputInner::Owned(v) => v as *const T,
            MemregionRdmaInputInner::ArcVec((v, r)) => v[r.clone()].as_ptr(),
        }
    }

    pub(crate) fn contains(&self, addr: &usize) -> bool {
        match self {
            MemregionRdmaInputInner::LamellarMemRegion(m) => {
                let start = unsafe { m.as_ptr().unwrap() as usize };
                let end = start + m.len() * std::mem::size_of::<T>();
                (*addr >= start) && (*addr < end)
            }
            MemregionRdmaInputInner::SharedMemRegion(m) => {
                let start = unsafe { m.as_ptr().unwrap() as usize };
                let end = start + m.len() * std::mem::size_of::<T>();
                (*addr >= start) && (*addr < end)
            }
            MemregionRdmaInputInner::LocalMemRegion(m) => {
                let start = unsafe { m.as_ptr().unwrap() as usize };
                let end = start + m.len() * std::mem::size_of::<T>();
                (*addr >= start) && (*addr < end)
            }
            MemregionRdmaInputInner::Slice(s) => s.contains(addr),
            MemregionRdmaInputInner::Owned(v) => v as *const T as usize == *addr,
            MemregionRdmaInputInner::ArcVec((_v, r)) => r.contains(addr),
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            MemregionRdmaInputInner::LamellarMemRegion(m) => m.len(),
            MemregionRdmaInputInner::SharedMemRegion(m) => m.len(),
            MemregionRdmaInputInner::LocalMemRegion(m) => m.len(),
            MemregionRdmaInputInner::Slice(s) => s.len(),
            MemregionRdmaInputInner::Owned(_) => 1,
            MemregionRdmaInputInner::ArcVec((_v, r)) => r.len(),
        }
    }

    pub(crate) fn num_bytes(&self) -> usize {
        self.len() * std::mem::size_of::<T>()
    }

    pub(crate) fn sub_region<R: std::ops::RangeBounds<usize>>(&self, range: R) -> Self {
        match self {
            MemregionRdmaInputInner::LamellarMemRegion(m) => {
                MemregionRdmaInputInner::LamellarMemRegion(m.sub_region(range))
            }
            MemregionRdmaInputInner::SharedMemRegion(m) => {
                MemregionRdmaInputInner::SharedMemRegion(m.sub_region(range))
            }
            MemregionRdmaInputInner::LocalMemRegion(m) => {
                MemregionRdmaInputInner::LocalMemRegion(m.sub_region(range))
            }
            MemregionRdmaInputInner::Slice(s) => MemregionRdmaInputInner::Slice(s.sub_slice(range)),
            MemregionRdmaInputInner::Owned(v) => {
                if range.start_bound() == std::ops::Bound::Included(&0)
                    && (range.end_bound() == std::ops::Bound::Included(&0)
                        || range.end_bound() == std::ops::Bound::Excluded(&1))
                {
                    MemregionRdmaInputInner::Owned(*v)
                } else {
                    panic!("range out of bounds")
                }
            }
            MemregionRdmaInputInner::ArcVec((v, _r)) => {
                let start = match range.start_bound() {
                    std::ops::Bound::Included(b) => *b,
                    std::ops::Bound::Excluded(b) => *b + 1,
                    std::ops::Bound::Unbounded => 0,
                };
                let end = match range.end_bound() {
                    std::ops::Bound::Included(b) => *b + 1,
                    std::ops::Bound::Excluded(b) => *b,
                    std::ops::Bound::Unbounded => v.len(),
                };
                MemregionRdmaInputInner::ArcVec((v.clone(), start..end))
            }
        }
    }
}

impl<T: Dist> From<LamellarArrayRdmaInput<T>> for MemregionRdmaInputInner<T> {
    fn from(input: LamellarArrayRdmaInput<T>) -> Self {
        match input {
            LamellarArrayRdmaInput::LamellarMemRegion(mr) => {
                MemregionRdmaInputInner::LamellarMemRegion(mr)
            }
            LamellarArrayRdmaInput::SharedMemRegion(mr) => {
                MemregionRdmaInputInner::SharedMemRegion(mr)
            }
            LamellarArrayRdmaInput::LocalMemRegion(mr) => {
                MemregionRdmaInputInner::LocalMemRegion(mr)
            }
            LamellarArrayRdmaInput::Owned(v) => MemregionRdmaInputInner::Owned(v),
            LamellarArrayRdmaInput::OwnedVec(v) => {
                let len = v.len();
                MemregionRdmaInputInner::ArcVec((Arc::new(v), 0..len))
            }
        }
    }
}

impl<T: Remote> From<LamellarMemoryRegion<T>> for MemregionRdmaInputInner<T> {
    fn from(mem_region: LamellarMemoryRegion<T>) -> Self {
        MemregionRdmaInputInner::LamellarMemRegion(mem_region)
    }
}

impl<T: Remote> From<&LamellarMemoryRegion<T>> for MemregionRdmaInputInner<T> {
    fn from(mem_region: &LamellarMemoryRegion<T>) -> Self {
        MemregionRdmaInputInner::LamellarMemRegion(mem_region.clone())
    }
}

impl<T: Remote> From<OneSidedMemoryRegion<T>> for MemregionRdmaInputInner<T> {
    fn from(mem_region: OneSidedMemoryRegion<T>) -> Self {
        MemregionRdmaInputInner::LocalMemRegion(mem_region)
    }
}

impl<T: Remote> From<SharedMemoryRegion<T>> for MemregionRdmaInputInner<T> {
    fn from(mem_region: SharedMemoryRegion<T>) -> Self {
        MemregionRdmaInputInner::SharedMemRegion(mem_region)
    }
}

impl<T: Remote> From<CommSlice<T>> for MemregionRdmaInputInner<T> {
    fn from(slice: CommSlice<T>) -> Self {
        MemregionRdmaInputInner::Slice(slice)
    }
}

impl<T: Remote> From<&[T]> for MemregionRdmaInputInner<T> {
    fn from(slice: &[T]) -> Self {
        MemregionRdmaInputInner::ArcVec((Arc::new(slice.to_vec()), 0..slice.len()))
    }
}
impl<T: Remote> From<&Vec<T>> for MemregionRdmaInputInner<T> {
    fn from(slice: &Vec<T>) -> Self {
        MemregionRdmaInputInner::ArcVec((Arc::new(slice.clone()), 0..slice.len()))
    }
}
impl<T: Remote> From<Vec<T>> for MemregionRdmaInputInner<T> {
    fn from(slice: Vec<T>) -> Self {
        let len = slice.len();
        MemregionRdmaInputInner::ArcVec((Arc::new(slice), 0..len))
    }
}

impl<T: Remote> From<&T> for MemregionRdmaInputInner<T> {
    fn from(slice: &T) -> Self {
        MemregionRdmaInputInner::Owned(*slice)
    }
}
impl<T: Remote> From<T> for MemregionRdmaInputInner<T> {
    fn from(val: T) -> Self {
        MemregionRdmaInputInner::Owned(val)
    }
}

impl<T: Remote> TeamFrom<MemregionRdmaInputInner<T>> for LamellarMemoryRegion<T> {
    fn team_from(input: MemregionRdmaInputInner<T>, team: &Arc<LamellarTeam>) -> Self {
        match input {
            MemregionRdmaInputInner::LamellarMemRegion(mr) => mr,
            MemregionRdmaInputInner::SharedMemRegion(mr) => LamellarMemoryRegion::Shared(mr),
            MemregionRdmaInputInner::LocalMemRegion(mr) => LamellarMemoryRegion::Local(mr),
            MemregionRdmaInputInner::Slice(s) => {
                let mem_region = team.alloc_one_sided_mem_region(s.len());
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        s.as_ptr(),
                        mem_region.as_mut_ptr().unwrap(),
                        s.len(),
                    )
                };
                mem_region.into()
            }
            MemregionRdmaInputInner::Owned(v) => {
                let mem_region = team.alloc_one_sided_mem_region(std::mem::size_of::<T>());
                unsafe { mem_region.as_mut_slice()[0] = v };
                mem_region.into()
            }
            MemregionRdmaInputInner::ArcVec((v, r)) => {
                let len = r.len();
                let mem_region = team.alloc_one_sided_mem_region(len);
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        v[r].as_ptr(),
                        mem_region.as_mut_ptr().unwrap(),
                        len,
                    )
                };
                mem_region.into()
            }
        }
    }
}
