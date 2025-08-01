use crate::array::native_atomic::*;
use crate::array::operations::read_only::LocalReadOnlyOps;
// use crate::array::native_atomic::rdma::atomic_store;
// use crate::array::operations::handle::{ArrayFetchOpHandle, BatchOpState, FetchOpState};
use crate::array::*;
// use std::collections::VecDeque;

impl<T: ElementOps + 'static> ReadOnlyOps<T> for NativeAtomicArray<T> {
    // fn load<'a>(&self, index: usize) -> ArrayFetchOpHandle<T> {
    //     if self.array.team_rt().lamellae.atomic_avail::<T>() {
    //         let buf: OneSidedMemoryRegion<T> = self.array.team_rt().alloc_one_sided_mem_region(1);
    //         self.network_iatomic_load(index, &buf);
    //         let team = self.team_rt();
    //         team.inc_outstanding(1);
    //         let task = self.spawn(async move {
    //             // team.lamellae.comm().wait();
    //             team.dec_outstanding(1);
    //             vec![unsafe { buf.as_slice().unwrap()[0] }]
    //         });

    //         ArrayFetchOpHandle {
    //             array: self.clone().into(),
    //             state: FetchOpState::Launched(task),
    //         }
    //     } else {
    //         let dummy_val = self.array.dummy_val(); //we dont actually do anything with this except satisfy apis;
    //         self.array
    //             .initiate_batch_fetch_op_2(dummy_val, index, ArrayOpCmd::Load, self.clone().into())
    //             .into()
    //     }
    // }
}

impl<T: ElementOps + 'static> AccessOps<T> for NativeAtomicArray<T> {
    // fn store<'a>(&self, index: usize, val: T) -> ArrayOpHandle {
    //     // println!("in native atomic store");
    //     if self.array.team_rt().lamellae.atomic_avail::<T>() {
    //         // println!("performing network atomic");
    //         self.network_atomic_store(index, val);
    //         let mut req = VecDeque::new();
    //         let team = self.team_rt();
    //         team.inc_outstanding(1);
    //         let task = self.spawn(async move {
    //             team.lamellae.comm().wait();
    //             team.dec_outstanding(1);
    //         });
    //         req.push_back((task, vec![index]));
    //         ArrayOpHandle {
    //             array: self.clone().into(),
    //             state: BatchOpState::Launched(req),
    //         }
    //     } else {
    //         // println!("performing am atomic");
    //         self.array
    //             .initiate_batch_op(val, index, ArrayOpCmd::Store, self.clone().into())
    //             .into()
    //     }
    // }
}

impl<T: ElementArithmeticOps + 'static> ArithmeticOps<T> for NativeAtomicArray<T> {}

impl<T: ElementBitWiseOps + 'static> BitWiseOps<T> for NativeAtomicArray<T> {}

impl<T: ElementShiftOps + 'static> ShiftOps<T> for NativeAtomicArray<T> {}

impl<T: ElementCompareEqOps + 'static> CompareExchangeOps<T> for NativeAtomicArray<T> {}

impl<T: ElementComparePartialEqOps + 'static> CompareExchangeEpsilonOps<T>
    for NativeAtomicArray<T>
{
}

trait NativeAtomicOps<T, A> {
    unsafe fn local_load(&self, _val: *const T, res: *mut T);
    unsafe fn local_store(&self, val: *const T, _res: *mut T);
    unsafe fn local_swap(&self, val: *const T, res: *mut T);
    unsafe fn local_fetch_add(&self, val: *const T, res: *mut T);
    unsafe fn local_fetch_sub(&self, val: *const T, res: *mut T);
    unsafe fn local_fetch_mul(&self, val: *const T, res: *mut T);
    unsafe fn local_fetch_div(&self, val: *const T, res: *mut T);
    unsafe fn local_fetch_rem(&self, val: *const T, res: *mut T);
    unsafe fn local_fetch_bit_and(&self, val: *const T, res: *mut T);
    unsafe fn local_fetch_bit_or(&self, val: *const T, res: *mut T);
    unsafe fn local_fetch_bit_xor(&self, val: *const T, res: *mut T);
    unsafe fn local_fetch_shl(&self, val: *const T, res: *mut T);
    unsafe fn local_fetch_shr(&self, val: *const T, res: *mut T);
    unsafe fn local_compare_exchange(&self, val: *const T, res: *mut T, current: *const T) -> bool;
    unsafe fn local_compare_exchange_epsilon(
        &self,
        val: *const T,
        res: *mut T,
        current: *const T,
        eps: *const T,
    ) -> bool;
}

macro_rules! ImplNativeAtomicOps {
    ($t:ty,$at:ty) => {
        impl<T: Dist  > NativeAtomicOps<T,$at> for $at {
            unsafe fn local_load(&self, _val: *const T, res: *mut T){
                *res = *(&self.load(Ordering::SeqCst) as *const $t as *const T);
            }
            unsafe fn local_store(&self, val: *const T,_res: *mut T){
                self.store(*(val as *const $t), Ordering::SeqCst);
            }
            unsafe fn local_swap(&self, val: *const T, res: *mut T){
                let mut old = self.load(Ordering::SeqCst);
                let val_t = *(val as *const $t);
                while self.compare_exchange(old, val_t, Ordering::SeqCst, Ordering::SeqCst).is_err(){
                    old = self.load(Ordering::SeqCst);
                }
                *res = *(&old as *const $t as *const T);
            }
            unsafe fn local_fetch_add(&self, val: *const T,res: *mut T){
                *res = *(&self.fetch_add(*(val as *const $t), Ordering::SeqCst) as *const $t as *const T);
            }
            unsafe fn local_fetch_sub(&self, val: *const T,res: *mut T){
                *res = *(&self.fetch_sub(*(val as *const $t), Ordering::SeqCst) as *const $t as *const T);
            }
            unsafe fn local_fetch_mul(&self, val: *const T,res: *mut T){
                let mut old = self.load(Ordering::SeqCst);
                let val_t = *(val as *const $t);
                let mut new = old * val_t;
                while self.compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                    std::thread::yield_now();
                    old = self.load(Ordering::SeqCst);
                    new = old * val_t;
                }
                *res = *(&old as *const $t as *const T);
            }
            unsafe fn local_fetch_div(&self, val: *const T,res: *mut T){
                let mut old = self.load(Ordering::SeqCst);
                let val_t = *(val as *const $t);
                let mut new = old / val_t;
                while self.compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                    std::thread::yield_now();
                    old = self.load(Ordering::SeqCst);
                    new = old / val_t;
                }
                *res = *(&old as *const $t as *const T);
            }
            unsafe fn local_fetch_rem(&self, val: *const T,res: *mut T){
                let mut old = self.load(Ordering::SeqCst);
                let val_t = *(val as *const $t);
                let mut new = old % val_t;
                while self.compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                    std::thread::yield_now();
                    old = self.load(Ordering::SeqCst);
                    new = old % val_t;
                }
                *res = *(&old as *const $t as *const T);
            }
            unsafe fn local_fetch_bit_and(&self, val: *const T,res: *mut T){
                let mut old = self.load(Ordering::SeqCst);
                let val_t = *(val as *const $t);
                let mut new = old & val_t;
                while self.compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                    std::thread::yield_now();
                    old = self.load(Ordering::SeqCst);
                    new = old & val_t;
                }
                *res = *(&old as *const $t as *const T);
            }
            unsafe fn local_fetch_bit_or(&self, val: *const T,res: *mut T){
                let mut old = self.load(Ordering::SeqCst);
                let val_t = *(val as *const $t);
                let mut new = old | val_t;
                while self.compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                    std::thread::yield_now();
                    old = self.load(Ordering::SeqCst);
                    new = old | val_t;
                }
                *res = *(&old as *const $t as *const T);
            }
            unsafe fn local_fetch_bit_xor(&self, val: *const T,res: *mut T){
                let mut old = self.load(Ordering::SeqCst);
                let val_t = *(val as *const $t);
                let mut new = old ^ val_t;
                while self.compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                    std::thread::yield_now();
                    old = self.load(Ordering::SeqCst);
                    new = old ^ val_t;
                }
                *res = *(&old as *const $t as *const T);
            }
            unsafe fn local_fetch_shl(&self, val: *const T,res: *mut T){
                let mut old = self.load(Ordering::SeqCst);
                let val_t = *(val as *const $t);
                let mut new = old << val_t;
                while self.compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                    std::thread::yield_now();
                    old = self.load(Ordering::SeqCst);
                    new = old << val_t;
                }
                *res = *(&old as *const $t as *const T);
            }
            unsafe fn local_fetch_shr(&self, val: *const T,res: *mut T){
                let mut old = self.load(Ordering::SeqCst);
                let val_t = *(val as *const $t);
                let mut new = old >> val_t;
                while self.compare_exchange(old, new, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                    std::thread::yield_now();
                    old = self.load(Ordering::SeqCst);
                    new = old >> val_t;
                }
                *res = *(&old as *const $t as *const T);
            }
            unsafe fn local_compare_exchange(&self, val: *const T,res: *mut T, current: *const T) -> bool {
                let current_t = *(current as *const $t);
                let val_t = *(val as *const $t);
                match self.compare_exchange(current_t, val_t, Ordering::SeqCst, Ordering::SeqCst){
                    Ok(old) => {
                        *res = *(&old as *const $t as *const T);
                        true
                    },
                    Err(new) => {
                        *res = *(&new as *const $t as *const T);
                        false
                    }
                }
            }
            unsafe fn local_compare_exchange_epsilon(&self, val: *const T,res: *mut T, current: *const T, epsilon: *const T) -> bool {
                let current_t = *(current as *const $t);
                let epsilon_t = *(epsilon as *const $t);
                let val_t = *(val as *const $t);
                match self.compare_exchange(current_t, val_t, Ordering::SeqCst, Ordering::SeqCst){
                    Ok(old) => {
                        *res = *(&old as *const $t as *const T);
                        true
                    },
                    Err(new) => {
                        let mut done = false;
                        let mut cur = new;
                        while cur.abs_diff(current_t) as $t < epsilon_t && !done{
                            cur = match self.compare_exchange(new, val_t, Ordering::SeqCst, Ordering::SeqCst){
                                Ok(cur) => {
                                    done = true;
                                    cur
                                },
                                Err(cur) => {
                                    std::thread::yield_now();
                                    cur
                                }
                            }
                        }
                        *res = *(&cur as *const $t as *const T);
                        done
                    }
                }

            }
        }
    };
}

ImplNativeAtomicOps!(u8, AtomicU8);
ImplNativeAtomicOps!(u16, AtomicU16);
ImplNativeAtomicOps!(u32, AtomicU32);
ImplNativeAtomicOps!(u64, AtomicU64);
ImplNativeAtomicOps!(usize, AtomicUsize);
ImplNativeAtomicOps!(i8, AtomicI8);
ImplNativeAtomicOps!(i16, AtomicI16);
ImplNativeAtomicOps!(i32, AtomicI32);
ImplNativeAtomicOps!(i64, AtomicI64);
ImplNativeAtomicOps!(isize, AtomicIsize);

impl<T: Dist> NativeAtomicLocalData<T> {
    fn inner_op<A: NativeAtomicOps<T, A>>(
        &self,
        idx_vals: impl Iterator<Item = (usize, T)>,
        results: &mut Option<Vec<T>>,
        op: unsafe fn(&A, *const T, *mut T),
    ) {
        let data = self
            .as_slice::<A>()
            .expect("Slice to be equivalent of underlying native atomic type");
        if let Some(results) = results.as_mut() {
            let mut new_res = idx_vals
                .map(|(i, val)| {
                    let mut res = val;
                    unsafe { op(&data[i], &val as *const T, &mut res as *mut T) }
                    res
                })
                .collect();
            std::mem::swap(results, &mut new_res);
        } else {
            idx_vals.for_each(|(i, val)| {
                let mut res = val;
                unsafe { op(&data[i], &val as *const T, &mut res as *mut T) }
            });
        }
    }
    fn inner_compare_exchange<A: NativeAtomicOps<T, A>>(
        &self,
        idx_vals: impl Iterator<Item = (usize, T)>,
        current: T,
        op: unsafe fn(&A, *const T, *mut T, *const T) -> bool,
    ) -> Vec<Result<T, T>> {
        let data = self
            .as_slice::<A>()
            .expect("Slice to be equivalent of underlying native atomic type");
        idx_vals
            .map(|(i, val)| {
                let mut res = val;
                match unsafe {
                    op(
                        &data[i],
                        &val as *const T,
                        &mut res as *mut T,
                        &current as *const T,
                    )
                } {
                    true => Ok(res),
                    false => Err(res),
                }
            })
            .collect()
    }
    fn inner_compare_exchange_epsilon<A: NativeAtomicOps<T, A>>(
        &self,
        idx_vals: impl Iterator<Item = (usize, T)>,
        current: T,
        epsilon: T,
        op: unsafe fn(&A, *const T, *mut T, *const T, *const T) -> bool,
    ) -> Vec<Result<T, T>> {
        let data = self
            .as_slice::<A>()
            .expect("Slice to be equivalent of underlying native atomic type");
        idx_vals
            .map(|(i, val)| {
                let mut res = val;
                match unsafe {
                    op(
                        &data[i],
                        &val as *const T,
                        &mut res as *mut T,
                        &current as *const T,
                        &epsilon as *const T,
                    )
                } {
                    true => Ok(res),
                    false => Err(res),
                }
            })
            .collect()
    }
}

macro_rules! local_op {
    ($self:expr, $idx_vals:expr, $fetch:expr, $op:ident) => {{
        let mut results = match $fetch {
            true => Some(Vec::new()),
            false => None,
        };
        let t = std::any::TypeId::of::<T>();
        paste::paste! {
            if t == std::any::TypeId::of::<i8>() {
                $self.inner_op::<AtomicI8>($idx_vals, &mut results, AtomicI8::[<local_ $op>]);
            } else if t == std::any::TypeId::of::<i16>() {
                $self.inner_op::<AtomicI16>($idx_vals, &mut results, AtomicI16::[<local_ $op>]);
            } else if t == std::any::TypeId::of::<i32>() {
                $self.inner_op::<AtomicI32>($idx_vals, &mut results, AtomicI32::[<local_ $op>]);
            } else if t == std::any::TypeId::of::<i64>() {
                $self.inner_op::<AtomicI64>($idx_vals, &mut results, AtomicI64::[<local_ $op>]);
            } else if t == std::any::TypeId::of::<isize>() {
                $self.inner_op::<AtomicIsize>($idx_vals, &mut results, AtomicIsize::[<local_ $op>]);
            } else if t == std::any::TypeId::of::<u8>() {
                $self.inner_op::<AtomicU8>($idx_vals, &mut results, AtomicU8::[<local_ $op>]);
            } else if t == std::any::TypeId::of::<u16>() {
                $self.inner_op::<AtomicU16>($idx_vals, &mut results, AtomicU16::[<local_ $op>]);
            } else if t == std::any::TypeId::of::<u32>() {
                $self.inner_op::<AtomicU32>($idx_vals, &mut results, AtomicU32::[<local_ $op>]);
            } else if t == std::any::TypeId::of::<u64>() {
                $self.inner_op::<AtomicU64>($idx_vals, &mut results, AtomicU64::[<local_ $op>]);
            } else if t == std::any::TypeId::of::<usize>() {
                $self.inner_op::<AtomicUsize>($idx_vals, &mut results, AtomicUsize::[<local_ $op>]);
            }
            else {
                panic!("invalid native atomic type!");
            }
        }

        results
    }};
}

impl<T: Dist + ElementOps> LocalAccessOps<T> for NativeAtomicLocalData<T> {
    fn local_store(&mut self, idx_vals: impl Iterator<Item = (usize, T)>) {
        local_op!(self, idx_vals, false, store);
    }

    fn local_swap(&mut self, idx_vals: impl Iterator<Item = (usize, T)>) -> Vec<T> {
        local_op!(self, idx_vals, true, swap).unwrap()
    }
}

impl<T: Dist + ElementArithmeticOps> LocalArithmeticOps<T> for NativeAtomicLocalData<T> {
    fn local_fetch_add(
        &mut self,
        idx_vals: impl Iterator<Item = (usize, T)>,
        fetch: bool,
    ) -> Option<Vec<T>> {
        local_op!(self, idx_vals, fetch, fetch_add)
    }

    fn local_fetch_sub(
        &mut self,
        idx_vals: impl Iterator<Item = (usize, T)>,
        fetch: bool,
    ) -> Option<Vec<T>> {
        local_op!(self, idx_vals, fetch, fetch_sub)
    }

    fn local_fetch_mul(
        &mut self,
        idx_vals: impl Iterator<Item = (usize, T)>,
        fetch: bool,
    ) -> Option<Vec<T>> {
        local_op!(self, idx_vals, fetch, fetch_mul)
    }

    fn local_fetch_div(
        &mut self,
        idx_vals: impl Iterator<Item = (usize, T)>,
        fetch: bool,
    ) -> Option<Vec<T>> {
        local_op!(self, idx_vals, fetch, fetch_div)
    }

    fn local_fetch_rem(
        &mut self,
        idx_vals: impl Iterator<Item = (usize, T)>,
        fetch: bool,
    ) -> Option<Vec<T>> {
        local_op!(self, idx_vals, fetch, fetch_rem)
    }
}

impl<T: Dist + ElementBitWiseOps> LocalBitWiseOps<T> for NativeAtomicLocalData<T> {
    fn local_fetch_bit_and(
        &mut self,
        idx_vals: impl Iterator<Item = (usize, T)>,
        fetch: bool,
    ) -> Option<Vec<T>> {
        local_op!(self, idx_vals, fetch, fetch_bit_and)
    }

    fn local_fetch_bit_or(
        &mut self,
        idx_vals: impl Iterator<Item = (usize, T)>,
        fetch: bool,
    ) -> Option<Vec<T>> {
        local_op!(self, idx_vals, fetch, fetch_bit_or)
    }

    fn local_fetch_bit_xor(
        &mut self,
        idx_vals: impl Iterator<Item = (usize, T)>,
        fetch: bool,
    ) -> Option<Vec<T>> {
        local_op!(self, idx_vals, fetch, fetch_bit_xor)
    }
}

impl<T: ElementOps> LocalReadOnlyOps<T> for NativeAtomicLocalData<T> {
    fn local_load<'a>(&self, idx_vals: impl Iterator<Item = (usize, T)>) -> Vec<T> {
        local_op!(self, idx_vals, true, load).unwrap()
    }
}

impl<T: Dist + ElementShiftOps> LocalShiftOps<T> for NativeAtomicLocalData<T> {
    fn local_fetch_shl(
        &mut self,
        idx_vals: impl Iterator<Item = (usize, T)>,
        fetch: bool,
    ) -> Option<Vec<T>> {
        local_op!(self, idx_vals, fetch, fetch_shl)
    }

    fn local_fetch_shr(
        &mut self,
        idx_vals: impl Iterator<Item = (usize, T)>,
        fetch: bool,
    ) -> Option<Vec<T>> {
        local_op!(self, idx_vals, fetch, fetch_shr)
    }
}

impl<T: Dist + ElementCompareEqOps> LocalCompareExchangeOps<T> for NativeAtomicLocalData<T> {
    fn local_compare_exchange(
        &mut self,
        idx_vals: impl Iterator<Item = (usize, T)>,
        current: T,
    ) -> Vec<Result<T, T>> {
        let t = std::any::TypeId::of::<T>();
        if t == std::any::TypeId::of::<i8>() {
            self.inner_compare_exchange::<AtomicI8>(
                idx_vals,
                current,
                AtomicI8::local_compare_exchange,
            )
        } else if t == std::any::TypeId::of::<i16>() {
            self.inner_compare_exchange::<AtomicI16>(
                idx_vals,
                current,
                AtomicI16::local_compare_exchange,
            )
        } else if t == std::any::TypeId::of::<i32>() {
            self.inner_compare_exchange::<AtomicI32>(
                idx_vals,
                current,
                AtomicI32::local_compare_exchange,
            )
        } else if t == std::any::TypeId::of::<i64>() {
            self.inner_compare_exchange::<AtomicI64>(
                idx_vals,
                current,
                AtomicI64::local_compare_exchange,
            )
        } else if t == std::any::TypeId::of::<isize>() {
            self.inner_compare_exchange::<AtomicIsize>(
                idx_vals,
                current,
                AtomicIsize::local_compare_exchange,
            )
        } else if t == std::any::TypeId::of::<u8>() {
            self.inner_compare_exchange::<AtomicU8>(
                idx_vals,
                current,
                AtomicU8::local_compare_exchange,
            )
        } else if t == std::any::TypeId::of::<u16>() {
            self.inner_compare_exchange::<AtomicU16>(
                idx_vals,
                current,
                AtomicU16::local_compare_exchange,
            )
        } else if t == std::any::TypeId::of::<u32>() {
            self.inner_compare_exchange::<AtomicU32>(
                idx_vals,
                current,
                AtomicU32::local_compare_exchange,
            )
        } else if t == std::any::TypeId::of::<u64>() {
            self.inner_compare_exchange::<AtomicU64>(
                idx_vals,
                current,
                AtomicU64::local_compare_exchange,
            )
        } else if t == std::any::TypeId::of::<usize>() {
            self.inner_compare_exchange::<AtomicUsize>(
                idx_vals,
                current,
                AtomicUsize::local_compare_exchange,
            )
        } else {
            panic!("invalid native atomic type!");
        }
    }
}

impl<T: Dist + ElementComparePartialEqOps> LocalCompareExchangeOpsEpsilon<T>
    for NativeAtomicLocalData<T>
{
    fn local_compare_exchange_epsilon(
        &mut self,
        idx_vals: impl Iterator<Item = (usize, T)>,
        current: T,
        eps: T,
    ) -> Vec<Result<T, T>> {
        let t = std::any::TypeId::of::<T>();
        if t == std::any::TypeId::of::<i8>() {
            self.inner_compare_exchange_epsilon::<AtomicI16>(
                idx_vals,
                current,
                eps,
                AtomicI16::local_compare_exchange_epsilon,
            )
        } else if t == std::any::TypeId::of::<i32>() {
            self.inner_compare_exchange_epsilon::<AtomicI32>(
                idx_vals,
                current,
                eps,
                AtomicI32::local_compare_exchange_epsilon,
            )
        } else if t == std::any::TypeId::of::<i64>() {
            self.inner_compare_exchange_epsilon::<AtomicI64>(
                idx_vals,
                current,
                eps,
                AtomicI64::local_compare_exchange_epsilon,
            )
        } else if t == std::any::TypeId::of::<isize>() {
            self.inner_compare_exchange_epsilon::<AtomicIsize>(
                idx_vals,
                current,
                eps,
                AtomicIsize::local_compare_exchange_epsilon,
            )
        } else if t == std::any::TypeId::of::<u8>() {
            self.inner_compare_exchange_epsilon::<AtomicU8>(
                idx_vals,
                current,
                eps,
                AtomicU8::local_compare_exchange_epsilon,
            )
        } else if t == std::any::TypeId::of::<u16>() {
            self.inner_compare_exchange_epsilon::<AtomicU16>(
                idx_vals,
                current,
                eps,
                AtomicU16::local_compare_exchange_epsilon,
            )
        } else if t == std::any::TypeId::of::<u32>() {
            self.inner_compare_exchange_epsilon::<AtomicU32>(
                idx_vals,
                current,
                eps,
                AtomicU32::local_compare_exchange_epsilon,
            )
        } else if t == std::any::TypeId::of::<u64>() {
            self.inner_compare_exchange_epsilon::<AtomicU64>(
                idx_vals,
                current,
                eps,
                AtomicU64::local_compare_exchange_epsilon,
            )
        } else if t == std::any::TypeId::of::<usize>() {
            self.inner_compare_exchange_epsilon::<AtomicUsize>(
                idx_vals,
                current,
                eps,
                AtomicUsize::local_compare_exchange_epsilon,
            )
        } else {
            panic!("invalid native atomic type!");
        }
    }
}
