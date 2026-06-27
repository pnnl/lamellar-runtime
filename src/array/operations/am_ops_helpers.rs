
use crate::array::{ Dist};

// ── Byte-erased transmute helpers ────────────────────────────────────────────

// #[doc(hidden)]
// pub fn vec_t_to_bytes<T: Copy>(v: Vec<T>) -> Vec<u8> {
//     let len = v.len() * std::mem::size_of::<T>();
//     let cap = v.capacity() * std::mem::size_of::<T>();
//     let ptr = v.as_ptr() as *mut u8;
//     std::mem::forget(v);
//     unsafe { Vec::from_raw_parts(ptr, len, cap) }
// }

// #[doc(hidden)]
// pub fn bytes_to_vec_t<T: Copy>(bytes: Vec<u8>) -> Vec<T> {
//     let elem_size = std::mem::size_of::<T>();
//     if elem_size == 0 || bytes.is_empty() {
//         return Vec::new();
//     }
//     let len = bytes.len() / elem_size;
//     let cap = bytes.capacity() / elem_size;
//     let ptr = bytes.as_ptr() as *mut T;
//     std::mem::forget(bytes);
//     unsafe { Vec::from_raw_parts(ptr, len, cap) }
// }

// #[doc(hidden)]
// pub fn result_vec_to_bytes<T: Dist>(v: Vec<Result<T, T>>) -> Vec<u8> {
//     crate::serialize(&v, true).expect("failed to serialize result vec")
// }

// #[doc(hidden)]
// pub fn bytes_to_result_vec<T: Dist>(bytes: Vec<u8>) -> Vec<Result<T, T>> {
//     crate::deserialize(&bytes, true).expect("failed to deserialize result vec")
// }

// ── MultiVal + MultiIdx (integer: full bounds) ────────────────────────────────

// pub(crate) async fn exec_mv_mi_void<T>(
//     data: &mut LamellarByteArray,
//     op: ArrayOpCmd<T>,
//     idxs_vals: &[u8],
//     index_size: u8,
// ) where
//     T: Dist + AmDist + ElementArithmeticOps + ElementBitWiseOps + ElementShiftOps + ElementOps,
// {
//     let mut local_data = data.mut_local_data::<T>().await;
//     let idx_vals = IdxVal::<u8, T>::iter_from_bytes(index_size as usize, idxs_vals);
//     match op {
//         ArrayOpCmd::Store => local_data.local_store(idx_vals),
//         ArrayOpCmd::Add => local_data.local_add(idx_vals),
//         ArrayOpCmd::Sub => local_data.local_sub(idx_vals),
//         ArrayOpCmd::Mul => local_data.local_mul(idx_vals),
//         ArrayOpCmd::Div => local_data.local_div(idx_vals),
//         ArrayOpCmd::Rem => local_data.local_rem(idx_vals),
//         ArrayOpCmd::And => local_data.local_bit_and(idx_vals),
//         ArrayOpCmd::Or => local_data.local_bit_or(idx_vals),
//         ArrayOpCmd::Xor => local_data.local_bit_xor(idx_vals),
//         ArrayOpCmd::Shl => local_data.local_shl(idx_vals),
//         ArrayOpCmd::Shr => local_data.local_shr(idx_vals),
//         _ => panic!("Invalid ArrayOpCmd for MultiValMultiIdx void AM"),
//     }
// }

// pub(crate) async fn exec_mv_mi_fetch<T>(
//     data: &mut LamellarByteArray,
//     op: ArrayOpCmd<T>,
//     idxs_vals: &[u8],
//     index_size: u8,
// ) -> Vec<T>
// where
//     T: Dist + AmDist + ElementArithmeticOps + ElementBitWiseOps + ElementShiftOps + ElementOps,
// {
//     let mut local_data = data.mut_local_data::<T>().await;
//     let idx_vals = IdxVal::<u8, T>::iter_from_bytes(index_size as usize, idxs_vals);
//     match op {
//         ArrayOpCmd::Load => local_data.local_load(idx_vals),
//         ArrayOpCmd::Swap => local_data.local_swap(idx_vals),
//         ArrayOpCmd::FetchAdd => local_data.local_fetch_add(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchSub => local_data.local_fetch_sub(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchMul => local_data.local_fetch_mul(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchDiv => local_data.local_fetch_div(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchRem => local_data.local_fetch_rem(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchAnd => local_data.local_fetch_bit_and(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchOr => local_data.local_fetch_bit_or(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchXor => local_data.local_fetch_bit_xor(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchShl => local_data.local_fetch_shl(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchShr => local_data.local_fetch_shr(idx_vals, true).unwrap(),
//         _ => panic!("Invalid ArrayOpCmd for MultiValMultiIdx fetch AM"),
//     }
// }

// pub(crate) async fn exec_mv_mi_result_ceq<T>(
//     data: &mut LamellarByteArray,
//     op: ArrayOpCmd<T>,
//     idxs_vals: &[u8],
//     index_size: u8,
// ) -> Vec<Result<T, T>>
// where
//     T: Dist + AmDist + ElementCompareEqOps,
// {
//     let mut local_data = data.mut_local_data::<T>().await;
//     let idx_vals = IdxVal::<u8, T>::iter_from_bytes(index_size as usize, idxs_vals);
//     match op {
//         ArrayOpCmd::CompareExchange(cur) => local_data.local_compare_exchange(idx_vals, cur),
//         _ => panic!("Invalid ArrayOpCmd for MultiValMultiIdx result AM"),
//     }
// }

// pub(crate) async fn exec_mv_mi_result_cex<T>(
//     data: &mut LamellarByteArray,
//     op: ArrayOpCmd<T>,
//     idxs_vals: &[u8],
//     index_size: u8,
// ) -> Vec<Result<T, T>>
// where
//     T: Dist + AmDist + ElementComparePartialEqOps,
// {
//     let mut local_data = data.mut_local_data::<T>().await;
//     let idx_vals = IdxVal::<u8, T>::iter_from_bytes(index_size as usize, idxs_vals);
//     match op {
//         ArrayOpCmd::CompareExchangeEps(cur, eps) => {
//             local_data.local_compare_exchange_epsilon(idx_vals, cur, eps)
//         }
//         _ => panic!("Invalid ArrayOpCmd for MultiValMultiIdx result-eps AM"),
//     }
// }

// // ── MultiVal + MultiIdx (arith-only: f32/f64) ────────────────────────────────

// pub(crate) async fn exec_mv_mi_void_arith<T>(
//     data: &mut LamellarByteArray,
//     op: ArrayOpCmd<T>,
//     idxs_vals: &[u8],
//     index_size: u8,
// ) where
//     T: Dist + AmDist + ElementArithmeticOps + ElementOps,
// {
//     let mut local_data = data.mut_local_data::<T>().await;
//     let idx_vals = IdxVal::<u8, T>::iter_from_bytes(index_size as usize, idxs_vals);
//     match op {
//         ArrayOpCmd::Store => local_data.local_store(idx_vals),
//         ArrayOpCmd::Add => local_data.local_add(idx_vals),
//         ArrayOpCmd::Sub => local_data.local_sub(idx_vals),
//         ArrayOpCmd::Mul => local_data.local_mul(idx_vals),
//         ArrayOpCmd::Div => local_data.local_div(idx_vals),
//         ArrayOpCmd::Rem => local_data.local_rem(idx_vals),
//         _ => panic!("Invalid ArrayOpCmd for MultiValMultiIdx arith void AM"),
//     }
// }

// pub(crate) async fn exec_mv_mi_fetch_arith<T>(
//     data: &mut LamellarByteArray,
//     op: ArrayOpCmd<T>,
//     idxs_vals: &[u8],
//     index_size: u8,
// ) -> Vec<T>
// where
//     T: Dist + AmDist + ElementArithmeticOps + ElementOps,
// {
//     let mut local_data = data.mut_local_data::<T>().await;
//     let idx_vals = IdxVal::<u8, T>::iter_from_bytes(index_size as usize, idxs_vals);
//     match op {
//         ArrayOpCmd::Load => local_data.local_load(idx_vals),
//         ArrayOpCmd::Swap => local_data.local_swap(idx_vals),
//         ArrayOpCmd::FetchAdd => local_data.local_fetch_add(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchSub => local_data.local_fetch_sub(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchMul => local_data.local_fetch_mul(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchDiv => local_data.local_fetch_div(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchRem => local_data.local_fetch_rem(idx_vals, true).unwrap(),
//         _ => panic!("Invalid ArrayOpCmd for MultiValMultiIdx arith fetch AM"),
//     }
// }

// // ── MultiVal + MultiIdx (access-only: Option<T>) ─────────────────────────────

// pub(crate) async fn exec_mv_mi_void_access<T>(
//     data: &mut LamellarByteArray,
//     op: ArrayOpCmd<T>,
//     idxs_vals: &[u8],
//     index_size: u8,
// ) where
//     T: Dist + AmDist + ElementOps,
// {
//     let mut local_data = data.mut_local_data::<T>().await;
//     let idx_vals = IdxVal::<u8, T>::iter_from_bytes(index_size as usize, idxs_vals);
//     match op {
//         ArrayOpCmd::Store => local_data.local_store(idx_vals),
//         _ => panic!("Invalid ArrayOpCmd for MultiValMultiIdx access void AM"),
//     }
// }

// pub(crate) async fn exec_mv_mi_fetch_access<T>(
//     data: &mut LamellarByteArray,
//     op: ArrayOpCmd<T>,
//     idxs_vals: &[u8],
//     index_size: u8,
// ) -> Vec<T>
// where
//     T: Dist + AmDist + ElementOps,
// {
//     let mut local_data = data.mut_local_data::<T>().await;
//     let idx_vals = IdxVal::<u8, T>::iter_from_bytes(index_size as usize, idxs_vals);
//     match op {
//         ArrayOpCmd::Load => local_data.local_load(idx_vals),
//         ArrayOpCmd::Swap => local_data.local_swap(idx_vals),
//         _ => panic!("Invalid ArrayOpCmd for MultiValMultiIdx access fetch AM"),
//     }
// }

// // ── SingleVal + MultiIdx (integer) ───────────────────────────────────────────

// pub(crate) async fn exec_sv_mi_void<T>(
//     data: &mut LamellarByteArray,
//     op: ArrayOpCmd<T>,
//     val: T,
//     idxs: &[u8],
//     index_size: u8,
// ) where
//     T: Dist + AmDist + ElementArithmeticOps + ElementBitWiseOps + ElementShiftOps + ElementOps + Copy,
// {
//     let mut local_data = data.mut_local_data::<T>().await;
//     let idx_vals = sv_mi_idx_vals(val, idxs, index_size);
//     match op {
//         ArrayOpCmd::Store => local_data.local_store(idx_vals),
//         ArrayOpCmd::Add => local_data.local_add(idx_vals),
//         ArrayOpCmd::Sub => local_data.local_sub(idx_vals),
//         ArrayOpCmd::Mul => local_data.local_mul(idx_vals),
//         ArrayOpCmd::Div => local_data.local_div(idx_vals),
//         ArrayOpCmd::Rem => local_data.local_rem(idx_vals),
//         ArrayOpCmd::And => local_data.local_bit_and(idx_vals),
//         ArrayOpCmd::Or => local_data.local_bit_or(idx_vals),
//         ArrayOpCmd::Xor => local_data.local_bit_xor(idx_vals),
//         ArrayOpCmd::Shl => local_data.local_shl(idx_vals),
//         ArrayOpCmd::Shr => local_data.local_shr(idx_vals),
//         _ => panic!("Invalid ArrayOpCmd for SingleValMultiIdx void AM"),
//     }
// }

// pub(crate) async fn exec_sv_mi_fetch<T>(
//     data: &mut LamellarByteArray,
//     op: ArrayOpCmd<T>,
//     val: T,
//     idxs: &[u8],
//     index_size: u8,
// ) -> Vec<T>
// where
//     T: Dist + AmDist + ElementArithmeticOps + ElementBitWiseOps + ElementShiftOps + ElementOps + Copy,
// {
//     let mut local_data = data.mut_local_data::<T>().await;
//     let idx_vals = sv_mi_idx_vals(val, idxs, index_size);
//     match op {
//         ArrayOpCmd::Load => local_data.local_load(idx_vals),
//         ArrayOpCmd::Swap => local_data.local_swap(idx_vals),
//         ArrayOpCmd::FetchAdd => local_data.local_fetch_add(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchSub => local_data.local_fetch_sub(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchMul => local_data.local_fetch_mul(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchDiv => local_data.local_fetch_div(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchRem => local_data.local_fetch_rem(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchAnd => local_data.local_fetch_bit_and(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchOr => local_data.local_fetch_bit_or(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchXor => local_data.local_fetch_bit_xor(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchShl => local_data.local_fetch_shl(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchShr => local_data.local_fetch_shr(idx_vals, true).unwrap(),
//         _ => panic!("Invalid ArrayOpCmd for SingleValMultiIdx fetch AM"),
//     }
// }

// pub(crate) async fn exec_sv_mi_result_ceq<T>(
//     data: &mut LamellarByteArray,
//     op: ArrayOpCmd<T>,
//     val: T,
//     idxs: &[u8],
//     index_size: u8,
// ) -> Vec<Result<T, T>>
// where
//     T: Dist + AmDist + ElementCompareEqOps + Copy,
// {
//     let mut local_data = data.mut_local_data::<T>().await;
//     let idx_vals = sv_mi_idx_vals(val, idxs, index_size);
//     match op {
//         ArrayOpCmd::CompareExchange(cur) => local_data.local_compare_exchange(idx_vals, cur),
//         _ => panic!("Invalid ArrayOpCmd for SingleValMultiIdx result AM"),
//     }
// }

// pub(crate) async fn exec_sv_mi_result_cex<T>(
//     data: &mut LamellarByteArray,
//     op: ArrayOpCmd<T>,
//     val: T,
//     idxs: &[u8],
//     index_size: u8,
// ) -> Vec<Result<T, T>>
// where
//     T: Dist + AmDist + ElementComparePartialEqOps + Copy,
// {
//     let mut local_data = data.mut_local_data::<T>().await;
//     let idx_vals = sv_mi_idx_vals(val, idxs, index_size);
//     match op {
//         ArrayOpCmd::CompareExchangeEps(cur, eps) => {
//             local_data.local_compare_exchange_epsilon(idx_vals, cur, eps)
//         }
//         _ => panic!("Invalid ArrayOpCmd for SingleValMultiIdx result-eps AM"),
//     }
// }

// // ── SingleVal + MultiIdx (arith-only) ────────────────────────────────────────

// pub(crate) async fn exec_sv_mi_void_arith<T>(
//     data: &mut LamellarByteArray,
//     op: ArrayOpCmd<T>,
//     val: T,
//     idxs: &[u8],
//     index_size: u8,
// ) where
//     T: Dist + AmDist + ElementArithmeticOps + ElementOps + Copy,
// {
//     let mut local_data = data.mut_local_data::<T>().await;
//     let idx_vals = sv_mi_idx_vals(val, idxs, index_size);
//     match op {
//         ArrayOpCmd::Store => local_data.local_store(idx_vals),
//         ArrayOpCmd::Add => local_data.local_add(idx_vals),
//         ArrayOpCmd::Sub => local_data.local_sub(idx_vals),
//         ArrayOpCmd::Mul => local_data.local_mul(idx_vals),
//         ArrayOpCmd::Div => local_data.local_div(idx_vals),
//         ArrayOpCmd::Rem => local_data.local_rem(idx_vals),
//         _ => panic!("Invalid ArrayOpCmd for SingleValMultiIdx arith void AM"),
//     }
// }

// pub(crate) async fn exec_sv_mi_fetch_arith<T>(
//     data: &mut LamellarByteArray,
//     op: ArrayOpCmd<T>,
//     val: T,
//     idxs: &[u8],
//     index_size: u8,
// ) -> Vec<T>
// where
//     T: Dist + AmDist + ElementArithmeticOps + ElementOps + Copy,
// {
//     let mut local_data = data.mut_local_data::<T>().await;
//     let idx_vals = sv_mi_idx_vals(val, idxs, index_size);
//     match op {
//         ArrayOpCmd::Load => local_data.local_load(idx_vals),
//         ArrayOpCmd::Swap => local_data.local_swap(idx_vals),
//         ArrayOpCmd::FetchAdd => local_data.local_fetch_add(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchSub => local_data.local_fetch_sub(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchMul => local_data.local_fetch_mul(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchDiv => local_data.local_fetch_div(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchRem => local_data.local_fetch_rem(idx_vals, true).unwrap(),
//         _ => panic!("Invalid ArrayOpCmd for SingleValMultiIdx arith fetch AM"),
//     }
// }

// // ── SingleVal + MultiIdx (access-only) ───────────────────────────────────────

// pub(crate) async fn exec_sv_mi_void_access<T>(
//     data: &mut LamellarByteArray,
//     op: ArrayOpCmd<T>,
//     val: T,
//     idxs: &[u8],
//     index_size: u8,
// ) where
//     T: Dist + AmDist + ElementOps + Copy,
// {
//     let mut local_data = data.mut_local_data::<T>().await;
//     let idx_vals = sv_mi_idx_vals(val, idxs, index_size);
//     match op {
//         ArrayOpCmd::Store => local_data.local_store(idx_vals),
//         _ => panic!("Invalid ArrayOpCmd for SingleValMultiIdx access void AM"),
//     }
// }

// pub(crate) async fn exec_sv_mi_fetch_access<T>(
//     data: &mut LamellarByteArray,
//     op: ArrayOpCmd<T>,
//     val: T,
//     idxs: &[u8],
//     index_size: u8,
// ) -> Vec<T>
// where
//     T: Dist + AmDist + ElementOps + Copy,
// {
//     let mut local_data = data.mut_local_data::<T>().await;
//     let idx_vals = sv_mi_idx_vals(val, idxs, index_size);
//     match op {
//         ArrayOpCmd::Load => local_data.local_load(idx_vals),
//         ArrayOpCmd::Swap => local_data.local_swap(idx_vals),
//         _ => panic!("Invalid ArrayOpCmd for SingleValMultiIdx access fetch AM"),
//     }
// }

// // ── MultiVal + SingleIdx (integer) ───────────────────────────────────────────

// pub(crate) async fn exec_mv_si_void<T>(
//     data: &mut LamellarByteArray,
//     op: ArrayOpCmd<T>,
//     idx: usize,
//     vals: &[T],
// ) where
//     T: Dist + AmDist + ElementArithmeticOps + ElementBitWiseOps + ElementShiftOps + ElementOps + Copy,
// {
//     let mut local_data = data.mut_local_data::<T>().await;
//     let idx_vals = std::iter::repeat(idx).zip(vals.iter().copied());
//     match op {
//         ArrayOpCmd::Store => local_data.local_store(idx_vals),
//         ArrayOpCmd::Add => local_data.local_add(idx_vals),
//         ArrayOpCmd::Sub => local_data.local_sub(idx_vals),
//         ArrayOpCmd::Mul => local_data.local_mul(idx_vals),
//         ArrayOpCmd::Div => local_data.local_div(idx_vals),
//         ArrayOpCmd::Rem => local_data.local_rem(idx_vals),
//         ArrayOpCmd::And => local_data.local_bit_and(idx_vals),
//         ArrayOpCmd::Or => local_data.local_bit_or(idx_vals),
//         ArrayOpCmd::Xor => local_data.local_bit_xor(idx_vals),
//         ArrayOpCmd::Shl => local_data.local_shl(idx_vals),
//         ArrayOpCmd::Shr => local_data.local_shr(idx_vals),
//         _ => panic!("Invalid ArrayOpCmd for MultiValSingleIdx void AM"),
//     }
// }

// pub(crate) async fn exec_mv_si_fetch<T>(
//     data: &mut LamellarByteArray,
//     op: ArrayOpCmd<T>,
//     idx: usize,
//     vals: &[T],
// ) -> Vec<T>
// where
//     T: Dist + AmDist + ElementArithmeticOps + ElementBitWiseOps + ElementShiftOps + ElementOps + Copy,
// {
//     let mut local_data = data.mut_local_data::<T>().await;
//     let idx_vals = std::iter::repeat(idx).zip(vals.iter().copied());
//     match op {
//         ArrayOpCmd::Load => local_data.local_load(idx_vals),
//         ArrayOpCmd::Swap => local_data.local_swap(idx_vals),
//         ArrayOpCmd::FetchAdd => local_data.local_fetch_add(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchSub => local_data.local_fetch_sub(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchMul => local_data.local_fetch_mul(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchDiv => local_data.local_fetch_div(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchRem => local_data.local_fetch_rem(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchAnd => local_data.local_fetch_bit_and(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchOr => local_data.local_fetch_bit_or(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchXor => local_data.local_fetch_bit_xor(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchShl => local_data.local_fetch_shl(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchShr => local_data.local_fetch_shr(idx_vals, true).unwrap(),
//         _ => panic!("Invalid ArrayOpCmd for MultiValSingleIdx fetch AM"),
//     }
// }

// pub(crate) async fn exec_mv_si_result_ceq<T>(
//     data: &mut LamellarByteArray,
//     op: ArrayOpCmd<T>,
//     idx: usize,
//     vals: &[T],
// ) -> Vec<Result<T, T>>
// where
//     T: Dist + AmDist + ElementCompareEqOps + Copy,
// {
//     let mut local_data = data.mut_local_data::<T>().await;
//     let idx_vals = std::iter::repeat(idx).zip(vals.iter().copied());
//     match op {
//         ArrayOpCmd::CompareExchange(cur) => local_data.local_compare_exchange(idx_vals, cur),
//         _ => panic!("Invalid ArrayOpCmd for MultiValSingleIdx result AM"),
//     }
// }

// pub(crate) async fn exec_mv_si_result_cex<T>(
//     data: &mut LamellarByteArray,
//     op: ArrayOpCmd<T>,
//     idx: usize,
//     vals: &[T],
// ) -> Vec<Result<T, T>>
// where
//     T: Dist + AmDist + ElementComparePartialEqOps + Copy,
// {
//     let mut local_data = data.mut_local_data::<T>().await;
//     let idx_vals = std::iter::repeat(idx).zip(vals.iter().copied());
//     match op {
//         ArrayOpCmd::CompareExchangeEps(cur, eps) => {
//             local_data.local_compare_exchange_epsilon(idx_vals, cur, eps)
//         }
//         _ => panic!("Invalid ArrayOpCmd for MultiValSingleIdx result-eps AM"),
//     }
// }

// // ── MultiVal + SingleIdx (arith-only) ────────────────────────────────────────

// pub(crate) async fn exec_mv_si_void_arith<T>(
//     data: &mut LamellarByteArray,
//     op: ArrayOpCmd<T>,
//     idx: usize,
//     vals: &[T],
// ) where
//     T: Dist + AmDist + ElementArithmeticOps + ElementOps + Copy,
// {
//     let mut local_data = data.mut_local_data::<T>().await;
//     let idx_vals = std::iter::repeat(idx).zip(vals.iter().copied());
//     match op {
//         ArrayOpCmd::Store => local_data.local_store(idx_vals),
//         ArrayOpCmd::Add => local_data.local_add(idx_vals),
//         ArrayOpCmd::Sub => local_data.local_sub(idx_vals),
//         ArrayOpCmd::Mul => local_data.local_mul(idx_vals),
//         ArrayOpCmd::Div => local_data.local_div(idx_vals),
//         ArrayOpCmd::Rem => local_data.local_rem(idx_vals),
//         _ => panic!("Invalid ArrayOpCmd for MultiValSingleIdx arith void AM"),
//     }
// }

// pub(crate) async fn exec_mv_si_fetch_arith<T>(
//     data: &mut LamellarByteArray,
//     op: ArrayOpCmd<T>,
//     idx: usize,
//     vals: &[T],
// ) -> Vec<T>
// where
//     T: Dist + AmDist + ElementArithmeticOps + ElementOps + Copy,
// {
//     let mut local_data = data.mut_local_data::<T>().await;
//     let idx_vals = std::iter::repeat(idx).zip(vals.iter().copied());
//     match op {
//         ArrayOpCmd::Load => local_data.local_load(idx_vals),
//         ArrayOpCmd::Swap => local_data.local_swap(idx_vals),
//         ArrayOpCmd::FetchAdd => local_data.local_fetch_add(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchSub => local_data.local_fetch_sub(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchMul => local_data.local_fetch_mul(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchDiv => local_data.local_fetch_div(idx_vals, true).unwrap(),
//         ArrayOpCmd::FetchRem => local_data.local_fetch_rem(idx_vals, true).unwrap(),
//         _ => panic!("Invalid ArrayOpCmd for MultiValSingleIdx arith fetch AM"),
//     }
// }

// // ── MultiVal + SingleIdx (access-only) ───────────────────────────────────────

// pub(crate) async fn exec_mv_si_void_access<T>(
//     data: &mut LamellarByteArray,
//     op: ArrayOpCmd<T>,
//     idx: usize,
//     vals: &[T],
// ) where
//     T: Dist + AmDist + ElementOps + Copy,
// {
//     let mut local_data = data.mut_local_data::<T>().await;
//     let idx_vals = std::iter::repeat(idx).zip(vals.iter().copied());
//     match op {
//         ArrayOpCmd::Store => local_data.local_store(idx_vals),
//         _ => panic!("Invalid ArrayOpCmd for MultiValSingleIdx access void AM"),
//     }
// }

// pub(crate) async fn exec_mv_si_fetch_access<T>(
//     data: &mut LamellarByteArray,
//     op: ArrayOpCmd<T>,
//     idx: usize,
//     vals: &[T],
// ) -> Vec<T>
// where
//     T: Dist + AmDist + ElementOps + Copy,
// {
//     let mut local_data = data.mut_local_data::<T>().await;
//     let idx_vals = std::iter::repeat(idx).zip(vals.iter().copied());
//     match op {
//         ArrayOpCmd::Load => local_data.local_load(idx_vals),
//         ArrayOpCmd::Swap => local_data.local_swap(idx_vals),
//         _ => panic!("Invalid ArrayOpCmd for MultiValSingleIdx access fetch AM"),
//     }
// }

// // ── Private helpers ───────────────────────────────────────────────────────────

// fn sv_mi_idx_vals<'a, T: Copy + 'a>(
//     val: T,
//     idxs: &'a [u8],
//     index_size: u8,
// ) -> Box<dyn Iterator<Item = (usize, T)> + 'a> {
//     unsafe {
//         match index_size {
//             1 => Box::new(
//                 idxs.iter()
//                     .map(move |&idx| (idx as usize, val)),
//             ),
//             2 => Box::new(
//                 std::slice::from_raw_parts(idxs.as_ptr() as *const u16, idxs.len() / 2)
//                     .iter()
//                     .map(move |&idx| (idx as usize, val)),
//             ),
//             4 => Box::new(
//                 std::slice::from_raw_parts(idxs.as_ptr() as *const u32, idxs.len() / 4)
//                     .iter()
//                     .map(move |&idx| (idx as usize, val)),
//             ),
//             8 => Box::new(
//                 std::slice::from_raw_parts(idxs.as_ptr() as *const u64, idxs.len() / 8)
//                     .iter()
//                     .map(move |&idx| (idx as usize, val)),
//             ),
//             _ => Box::new(
//                 std::slice::from_raw_parts(
//                     idxs.as_ptr() as *const usize,
//                     idxs.len() / std::mem::size_of::<usize>(),
//                 )
//                 .iter()
//                 .map(move |&idx| (idx as usize, val)),
//             ),
//         }
//     }
// }

// ── Runtime type tags ─────────────────────────────────────────────────────────

/// Runtime tag for the 14 primitive scalar types that support array ops.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub enum PodPrimType {
    U8, U16, U32, U64, Usize,
    U128,
    I8, I16, I32, I64, Isize,
    I128,
    F32, F64,
}

/// Runtime tag for Option-wrapped primitive types.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub enum OptionPodType {
    OptionU8,    OptionU16,   OptionU32,   OptionU64,   OptionUsize,
    OptionU128,
    OptionI8,    OptionI16,   OptionI32,   OptionI64,   OptionIsize,
    OptionI128,
    OptionF32,   OptionF64,
}

// ── Macros for dispatching void/fetch/result by pod type ─────────────────────

// macro_rules! dispatch_prim_void {
//     ($pod:expr, $data:expr, $op:expr, $idxs_vals:expr, $index_size:expr) => {
//         match $pod {
//             PodPrimType::U8    => exec_mv_mi_void::<u8>   ($data, $op.into(), $idxs_vals, $index_size).await,
//             PodPrimType::U16   => exec_mv_mi_void::<u16>  ($data, $op.into(), $idxs_vals, $index_size).await,
//             PodPrimType::U32   => exec_mv_mi_void::<u32>  ($data, $op.into(), $idxs_vals, $index_size).await,
//             PodPrimType::U64   => exec_mv_mi_void::<u64>  ($data, $op.into(), $idxs_vals, $index_size).await,
//             PodPrimType::Usize => exec_mv_mi_void::<usize>($data, $op.into(), $idxs_vals, $index_size).await,
//             PodPrimType::U128  => exec_mv_mi_void::<u128> ($data, $op.into(), $idxs_vals, $index_size).await,
//             PodPrimType::I8    => exec_mv_mi_void::<i8>   ($data, $op.into(), $idxs_vals, $index_size).await,
//             PodPrimType::I16   => exec_mv_mi_void::<i16>  ($data, $op.into(), $idxs_vals, $index_size).await,
//             PodPrimType::I32   => exec_mv_mi_void::<i32>  ($data, $op.into(), $idxs_vals, $index_size).await,
//             PodPrimType::I64   => exec_mv_mi_void::<i64>  ($data, $op.into(), $idxs_vals, $index_size).await,
//             PodPrimType::Isize => exec_mv_mi_void::<isize>($data, $op.into(), $idxs_vals, $index_size).await,
//             PodPrimType::I128  => exec_mv_mi_void::<i128> ($data, $op.into(), $idxs_vals, $index_size).await,
//             PodPrimType::F32   => exec_mv_mi_void_arith::<f32>($data, $op.into(), $idxs_vals, $index_size).await,
//             PodPrimType::F64   => exec_mv_mi_void_arith::<f64>($data, $op.into(), $idxs_vals, $index_size).await,
//         }
//     };
// }

// macro_rules! dispatch_prim_fetch {
//     ($pod:expr, $data:expr, $op:expr, $idxs_vals:expr, $index_size:expr) => {
//         match $pod {
//             PodPrimType::U8    => vec_t_to_bytes(exec_mv_mi_fetch::<u8>   ($data, $op.into(), $idxs_vals, $index_size).await),
//             PodPrimType::U16   => vec_t_to_bytes(exec_mv_mi_fetch::<u16>  ($data, $op.into(), $idxs_vals, $index_size).await),
//             PodPrimType::U32   => vec_t_to_bytes(exec_mv_mi_fetch::<u32>  ($data, $op.into(), $idxs_vals, $index_size).await),
//             PodPrimType::U64   => vec_t_to_bytes(exec_mv_mi_fetch::<u64>  ($data, $op.into(), $idxs_vals, $index_size).await),
//             PodPrimType::Usize => vec_t_to_bytes(exec_mv_mi_fetch::<usize>($data, $op.into(), $idxs_vals, $index_size).await),
//             PodPrimType::U128  => vec_t_to_bytes(exec_mv_mi_fetch::<u128> ($data, $op.into(), $idxs_vals, $index_size).await),
//             PodPrimType::I8    => vec_t_to_bytes(exec_mv_mi_fetch::<i8>   ($data, $op.into(), $idxs_vals, $index_size).await),
//             PodPrimType::I16   => vec_t_to_bytes(exec_mv_mi_fetch::<i16>  ($data, $op.into(), $idxs_vals, $index_size).await),
//             PodPrimType::I32   => vec_t_to_bytes(exec_mv_mi_fetch::<i32>  ($data, $op.into(), $idxs_vals, $index_size).await),
//             PodPrimType::I64   => vec_t_to_bytes(exec_mv_mi_fetch::<i64>  ($data, $op.into(), $idxs_vals, $index_size).await),
//             PodPrimType::Isize => vec_t_to_bytes(exec_mv_mi_fetch::<isize>($data, $op.into(), $idxs_vals, $index_size).await),
//             PodPrimType::I128  => vec_t_to_bytes(exec_mv_mi_fetch::<i128> ($data, $op.into(), $idxs_vals, $index_size).await),
//             PodPrimType::F32   => vec_t_to_bytes(exec_mv_mi_fetch_arith::<f32>($data, $op.into(), $idxs_vals, $index_size).await),
//             PodPrimType::F64   => vec_t_to_bytes(exec_mv_mi_fetch_arith::<f64>($data, $op.into(), $idxs_vals, $index_size).await),
//         }
//     };
// }

// macro_rules! dispatch_prim_result {
//     ($pod:expr, $data:expr, $op:expr, $idxs_vals:expr, $index_size:expr) => {
//         match $pod {
//             PodPrimType::U8    => result_vec_to_bytes(exec_mv_mi_result_ceq::<u8>   ($data, $op.into(), $idxs_vals, $index_size).await),
//             PodPrimType::U16   => result_vec_to_bytes(exec_mv_mi_result_ceq::<u16>  ($data, $op.into(), $idxs_vals, $index_size).await),
//             PodPrimType::U32   => result_vec_to_bytes(exec_mv_mi_result_ceq::<u32>  ($data, $op.into(), $idxs_vals, $index_size).await),
//             PodPrimType::U64   => result_vec_to_bytes(exec_mv_mi_result_ceq::<u64>  ($data, $op.into(), $idxs_vals, $index_size).await),
//             PodPrimType::Usize => result_vec_to_bytes(exec_mv_mi_result_ceq::<usize>($data, $op.into(), $idxs_vals, $index_size).await),
//             PodPrimType::U128  => result_vec_to_bytes(exec_mv_mi_result_ceq::<u128> ($data, $op.into(), $idxs_vals, $index_size).await),
//             PodPrimType::I8    => result_vec_to_bytes(exec_mv_mi_result_ceq::<i8>   ($data, $op.into(), $idxs_vals, $index_size).await),
//             PodPrimType::I16   => result_vec_to_bytes(exec_mv_mi_result_ceq::<i16>  ($data, $op.into(), $idxs_vals, $index_size).await),
//             PodPrimType::I32   => result_vec_to_bytes(exec_mv_mi_result_ceq::<i32>  ($data, $op.into(), $idxs_vals, $index_size).await),
//             PodPrimType::I64   => result_vec_to_bytes(exec_mv_mi_result_ceq::<i64>  ($data, $op.into(), $idxs_vals, $index_size).await),
//             PodPrimType::Isize => result_vec_to_bytes(exec_mv_mi_result_ceq::<isize>($data, $op.into(), $idxs_vals, $index_size).await),
//             PodPrimType::I128  => result_vec_to_bytes(exec_mv_mi_result_ceq::<i128> ($data, $op.into(), $idxs_vals, $index_size).await),
//             PodPrimType::F32   => result_vec_to_bytes(exec_mv_mi_result_cex::<f32>  ($data, $op.into(), $idxs_vals, $index_size).await),
//             PodPrimType::F64   => result_vec_to_bytes(exec_mv_mi_result_cex::<f64>  ($data, $op.into(), $idxs_vals, $index_size).await),
//         }
//     };
// }

// macro_rules! dispatch_prim_sv_mi_void {
//     ($pod:expr, $data:expr, $op:expr, $val_bytes:expr, $idxs:expr, $index_size:expr) => {
//         match $pod {
//             PodPrimType::U8    => { let v = unsafe{*($val_bytes.as_ptr() as *const u8)};    exec_sv_mi_void::<u8>   ($data, $op.into(), v, $idxs, $index_size).await }
//             PodPrimType::U16   => { let v = unsafe{*($val_bytes.as_ptr() as *const u16)};   exec_sv_mi_void::<u16>  ($data, $op.into(), v, $idxs, $index_size).await }
//             PodPrimType::U32   => { let v = unsafe{*($val_bytes.as_ptr() as *const u32)};   exec_sv_mi_void::<u32>  ($data, $op.into(), v, $idxs, $index_size).await }
//             PodPrimType::U64   => { let v = unsafe{*($val_bytes.as_ptr() as *const u64)};   exec_sv_mi_void::<u64>  ($data, $op.into(), v, $idxs, $index_size).await }
//             PodPrimType::Usize => { let v = unsafe{*($val_bytes.as_ptr() as *const usize)}; exec_sv_mi_void::<usize>($data, $op.into(), v, $idxs, $index_size).await }
//             PodPrimType::U128  => { let v = unsafe{*($val_bytes.as_ptr() as *const u128)};  exec_sv_mi_void::<u128> ($data, $op.into(), v, $idxs, $index_size).await }
//             PodPrimType::I8    => { let v = unsafe{*($val_bytes.as_ptr() as *const i8)};    exec_sv_mi_void::<i8>   ($data, $op.into(), v, $idxs, $index_size).await }
//             PodPrimType::I16   => { let v = unsafe{*($val_bytes.as_ptr() as *const i16)};   exec_sv_mi_void::<i16>  ($data, $op.into(), v, $idxs, $index_size).await }
//             PodPrimType::I32   => { let v = unsafe{*($val_bytes.as_ptr() as *const i32)};   exec_sv_mi_void::<i32>  ($data, $op.into(), v, $idxs, $index_size).await }
//             PodPrimType::I64   => { let v = unsafe{*($val_bytes.as_ptr() as *const i64)};   exec_sv_mi_void::<i64>  ($data, $op.into(), v, $idxs, $index_size).await }
//             PodPrimType::Isize => { let v = unsafe{*($val_bytes.as_ptr() as *const isize)}; exec_sv_mi_void::<isize>($data, $op.into(), v, $idxs, $index_size).await }
//             PodPrimType::I128  => { let v = unsafe{*($val_bytes.as_ptr() as *const i128)};  exec_sv_mi_void::<i128> ($data, $op.into(), v, $idxs, $index_size).await }
//             PodPrimType::F32   => { let v = unsafe{*($val_bytes.as_ptr() as *const f32)};   exec_sv_mi_void_arith::<f32>($data, $op.into(), v, $idxs, $index_size).await }
//             PodPrimType::F64   => { let v = unsafe{*($val_bytes.as_ptr() as *const f64)};   exec_sv_mi_void_arith::<f64>($data, $op.into(), v, $idxs, $index_size).await }
//         }
//     };
// }

// macro_rules! dispatch_prim_sv_mi_fetch {
//     ($pod:expr, $data:expr, $op:expr, $val_bytes:expr, $idxs:expr, $index_size:expr) => {
//         match $pod {
//             PodPrimType::U8    => { let v = unsafe{*($val_bytes.as_ptr() as *const u8)};    vec_t_to_bytes(exec_sv_mi_fetch::<u8>   ($data, $op.into(), v, $idxs, $index_size).await) }
//             PodPrimType::U16   => { let v = unsafe{*($val_bytes.as_ptr() as *const u16)};   vec_t_to_bytes(exec_sv_mi_fetch::<u16>  ($data, $op.into(), v, $idxs, $index_size).await) }
//             PodPrimType::U32   => { let v = unsafe{*($val_bytes.as_ptr() as *const u32)};   vec_t_to_bytes(exec_sv_mi_fetch::<u32>  ($data, $op.into(), v, $idxs, $index_size).await) }
//             PodPrimType::U64   => { let v = unsafe{*($val_bytes.as_ptr() as *const u64)};   vec_t_to_bytes(exec_sv_mi_fetch::<u64>  ($data, $op.into(), v, $idxs, $index_size).await) }
//             PodPrimType::Usize => { let v = unsafe{*($val_bytes.as_ptr() as *const usize)}; vec_t_to_bytes(exec_sv_mi_fetch::<usize>($data, $op.into(), v, $idxs, $index_size).await) }
//             PodPrimType::U128  => { let v = unsafe{*($val_bytes.as_ptr() as *const u128)};  vec_t_to_bytes(exec_sv_mi_fetch::<u128> ($data, $op.into(), v, $idxs, $index_size).await) }
//             PodPrimType::I8    => { let v = unsafe{*($val_bytes.as_ptr() as *const i8)};    vec_t_to_bytes(exec_sv_mi_fetch::<i8>   ($data, $op.into(), v, $idxs, $index_size).await) }
//             PodPrimType::I16   => { let v = unsafe{*($val_bytes.as_ptr() as *const i16)};   vec_t_to_bytes(exec_sv_mi_fetch::<i16>  ($data, $op.into(), v, $idxs, $index_size).await) }
//             PodPrimType::I32   => { let v = unsafe{*($val_bytes.as_ptr() as *const i32)};   vec_t_to_bytes(exec_sv_mi_fetch::<i32>  ($data, $op.into(), v, $idxs, $index_size).await) }
//             PodPrimType::I64   => { let v = unsafe{*($val_bytes.as_ptr() as *const i64)};   vec_t_to_bytes(exec_sv_mi_fetch::<i64>  ($data, $op.into(), v, $idxs, $index_size).await) }
//             PodPrimType::Isize => { let v = unsafe{*($val_bytes.as_ptr() as *const isize)}; vec_t_to_bytes(exec_sv_mi_fetch::<isize>($data, $op.into(), v, $idxs, $index_size).await) }
//             PodPrimType::I128  => { let v = unsafe{*($val_bytes.as_ptr() as *const i128)};  vec_t_to_bytes(exec_sv_mi_fetch::<i128> ($data, $op.into(), v, $idxs, $index_size).await) }
//             PodPrimType::F32   => { let v = unsafe{*($val_bytes.as_ptr() as *const f32)};   vec_t_to_bytes(exec_sv_mi_fetch_arith::<f32>($data, $op.into(), v, $idxs, $index_size).await) }
//             PodPrimType::F64   => { let v = unsafe{*($val_bytes.as_ptr() as *const f64)};   vec_t_to_bytes(exec_sv_mi_fetch_arith::<f64>($data, $op.into(), v, $idxs, $index_size).await) }
//         }
//     };
// }

// macro_rules! dispatch_prim_sv_mi_result {
//     ($pod:expr, $data:expr, $op:expr, $val_bytes:expr, $idxs:expr, $index_size:expr) => {
//         match $pod {
//             PodPrimType::U8    => { let v = unsafe{*($val_bytes.as_ptr() as *const u8)};    result_vec_to_bytes(exec_sv_mi_result_ceq::<u8>   ($data, $op.into(), v, $idxs, $index_size).await) }
//             PodPrimType::U16   => { let v = unsafe{*($val_bytes.as_ptr() as *const u16)};   result_vec_to_bytes(exec_sv_mi_result_ceq::<u16>  ($data, $op.into(), v, $idxs, $index_size).await) }
//             PodPrimType::U32   => { let v = unsafe{*($val_bytes.as_ptr() as *const u32)};   result_vec_to_bytes(exec_sv_mi_result_ceq::<u32>  ($data, $op.into(), v, $idxs, $index_size).await) }
//             PodPrimType::U64   => { let v = unsafe{*($val_bytes.as_ptr() as *const u64)};   result_vec_to_bytes(exec_sv_mi_result_ceq::<u64>  ($data, $op.into(), v, $idxs, $index_size).await) }
//             PodPrimType::Usize => { let v = unsafe{*($val_bytes.as_ptr() as *const usize)}; result_vec_to_bytes(exec_sv_mi_result_ceq::<usize>($data, $op.into(), v, $idxs, $index_size).await) }
//             PodPrimType::U128  => { let v = unsafe{*($val_bytes.as_ptr() as *const u128)};  result_vec_to_bytes(exec_sv_mi_result_ceq::<u128> ($data, $op.into(), v, $idxs, $index_size).await) }
//             PodPrimType::I8    => { let v = unsafe{*($val_bytes.as_ptr() as *const i8)};    result_vec_to_bytes(exec_sv_mi_result_ceq::<i8>   ($data, $op.into(), v, $idxs, $index_size).await) }
//             PodPrimType::I16   => { let v = unsafe{*($val_bytes.as_ptr() as *const i16)};   result_vec_to_bytes(exec_sv_mi_result_ceq::<i16>  ($data, $op.into(), v, $idxs, $index_size).await) }
//             PodPrimType::I32   => { let v = unsafe{*($val_bytes.as_ptr() as *const i32)};   result_vec_to_bytes(exec_sv_mi_result_ceq::<i32>  ($data, $op.into(), v, $idxs, $index_size).await) }
//             PodPrimType::I64   => { let v = unsafe{*($val_bytes.as_ptr() as *const i64)};   result_vec_to_bytes(exec_sv_mi_result_ceq::<i64>  ($data, $op.into(), v, $idxs, $index_size).await) }
//             PodPrimType::Isize => { let v = unsafe{*($val_bytes.as_ptr() as *const isize)}; result_vec_to_bytes(exec_sv_mi_result_ceq::<isize>($data, $op.into(), v, $idxs, $index_size).await) }
//             PodPrimType::I128  => { let v = unsafe{*($val_bytes.as_ptr() as *const i128)};  result_vec_to_bytes(exec_sv_mi_result_ceq::<i128> ($data, $op.into(), v, $idxs, $index_size).await) }
//             PodPrimType::F32   => { let v = unsafe{*($val_bytes.as_ptr() as *const f32)};   result_vec_to_bytes(exec_sv_mi_result_cex::<f32>  ($data, $op.into(), v, $idxs, $index_size).await) }
//             PodPrimType::F64   => { let v = unsafe{*($val_bytes.as_ptr() as *const f64)};   result_vec_to_bytes(exec_sv_mi_result_cex::<f64>  ($data, $op.into(), v, $idxs, $index_size).await) }
//         }
//     };
// }

// macro_rules! dispatch_prim_mv_si_void {
//     ($pod:expr, $data:expr, $op:expr, $vals_bytes:expr, $idx:expr) => {
//         match $pod {
//             PodPrimType::U8    => exec_mv_si_void::<u8>   ($data, $op.into(), $idx, &bytes_to_vec_t::<u8>   ($vals_bytes.clone())).await,
//             PodPrimType::U16   => exec_mv_si_void::<u16>  ($data, $op.into(), $idx, &bytes_to_vec_t::<u16>  ($vals_bytes.clone())).await,
//             PodPrimType::U32   => exec_mv_si_void::<u32>  ($data, $op.into(), $idx, &bytes_to_vec_t::<u32>  ($vals_bytes.clone())).await,
//             PodPrimType::U64   => exec_mv_si_void::<u64>  ($data, $op.into(), $idx, &bytes_to_vec_t::<u64>  ($vals_bytes.clone())).await,
//             PodPrimType::Usize => exec_mv_si_void::<usize>($data, $op.into(), $idx, &bytes_to_vec_t::<usize>($vals_bytes.clone())).await,
//             PodPrimType::U128  => exec_mv_si_void::<u128> ($data, $op.into(), $idx, &bytes_to_vec_t::<u128> ($vals_bytes.clone())).await,
//             PodPrimType::I8    => exec_mv_si_void::<i8>   ($data, $op.into(), $idx, &bytes_to_vec_t::<i8>   ($vals_bytes.clone())).await,
//             PodPrimType::I16   => exec_mv_si_void::<i16>  ($data, $op.into(), $idx, &bytes_to_vec_t::<i16>  ($vals_bytes.clone())).await,
//             PodPrimType::I32   => exec_mv_si_void::<i32>  ($data, $op.into(), $idx, &bytes_to_vec_t::<i32>  ($vals_bytes.clone())).await,
//             PodPrimType::I64   => exec_mv_si_void::<i64>  ($data, $op.into(), $idx, &bytes_to_vec_t::<i64>  ($vals_bytes.clone())).await,
//             PodPrimType::Isize => exec_mv_si_void::<isize>($data, $op.into(), $idx, &bytes_to_vec_t::<isize>($vals_bytes.clone())).await,
//             PodPrimType::I128  => exec_mv_si_void::<i128> ($data, $op.into(), $idx, &bytes_to_vec_t::<i128> ($vals_bytes.clone())).await,
//             PodPrimType::F32   => exec_mv_si_void_arith::<f32>($data, $op.into(), $idx, &bytes_to_vec_t::<f32>($vals_bytes.clone())).await,
//             PodPrimType::F64   => exec_mv_si_void_arith::<f64>($data, $op.into(), $idx, &bytes_to_vec_t::<f64>($vals_bytes.clone())).await,
//         }
//     };
// }

// macro_rules! dispatch_prim_mv_si_fetch {
//     ($pod:expr, $data:expr, $op:expr, $vals_bytes:expr, $idx:expr) => {
//         match $pod {
//             PodPrimType::U8    => vec_t_to_bytes(exec_mv_si_fetch::<u8>   ($data, $op.into(), $idx, &bytes_to_vec_t::<u8>   ($vals_bytes.clone())).await),
//             PodPrimType::U16   => vec_t_to_bytes(exec_mv_si_fetch::<u16>  ($data, $op.into(), $idx, &bytes_to_vec_t::<u16>  ($vals_bytes.clone())).await),
//             PodPrimType::U32   => vec_t_to_bytes(exec_mv_si_fetch::<u32>  ($data, $op.into(), $idx, &bytes_to_vec_t::<u32>  ($vals_bytes.clone())).await),
//             PodPrimType::U64   => vec_t_to_bytes(exec_mv_si_fetch::<u64>  ($data, $op.into(), $idx, &bytes_to_vec_t::<u64>  ($vals_bytes.clone())).await),
//             PodPrimType::Usize => vec_t_to_bytes(exec_mv_si_fetch::<usize>($data, $op.into(), $idx, &bytes_to_vec_t::<usize>($vals_bytes.clone())).await),
//             PodPrimType::U128  => vec_t_to_bytes(exec_mv_si_fetch::<u128> ($data, $op.into(), $idx, &bytes_to_vec_t::<u128> ($vals_bytes.clone())).await),
//             PodPrimType::I8    => vec_t_to_bytes(exec_mv_si_fetch::<i8>   ($data, $op.into(), $idx, &bytes_to_vec_t::<i8>   ($vals_bytes.clone())).await),
//             PodPrimType::I16   => vec_t_to_bytes(exec_mv_si_fetch::<i16>  ($data, $op.into(), $idx, &bytes_to_vec_t::<i16>  ($vals_bytes.clone())).await),
//             PodPrimType::I32   => vec_t_to_bytes(exec_mv_si_fetch::<i32>  ($data, $op.into(), $idx, &bytes_to_vec_t::<i32>  ($vals_bytes.clone())).await),
//             PodPrimType::I64   => vec_t_to_bytes(exec_mv_si_fetch::<i64>  ($data, $op.into(), $idx, &bytes_to_vec_t::<i64>  ($vals_bytes.clone())).await),
//             PodPrimType::Isize => vec_t_to_bytes(exec_mv_si_fetch::<isize>($data, $op.into(), $idx, &bytes_to_vec_t::<isize>($vals_bytes.clone())).await),
//             PodPrimType::I128  => vec_t_to_bytes(exec_mv_si_fetch::<i128> ($data, $op.into(), $idx, &bytes_to_vec_t::<i128> ($vals_bytes.clone())).await),
//             PodPrimType::F32   => vec_t_to_bytes(exec_mv_si_fetch_arith::<f32>($data, $op.into(), $idx, &bytes_to_vec_t::<f32>($vals_bytes.clone())).await),
//             PodPrimType::F64   => vec_t_to_bytes(exec_mv_si_fetch_arith::<f64>($data, $op.into(), $idx, &bytes_to_vec_t::<f64>($vals_bytes.clone())).await),
//         }
//     };
// }

// macro_rules! dispatch_prim_mv_si_result {
//     ($pod:expr, $data:expr, $op:expr, $vals_bytes:expr, $idx:expr) => {
//         match $pod {
//             PodPrimType::U8    => result_vec_to_bytes(exec_mv_si_result_ceq::<u8>   ($data, $op.into(), $idx, &bytes_to_vec_t::<u8>   ($vals_bytes.clone())).await),
//             PodPrimType::U16   => result_vec_to_bytes(exec_mv_si_result_ceq::<u16>  ($data, $op.into(), $idx, &bytes_to_vec_t::<u16>  ($vals_bytes.clone())).await),
//             PodPrimType::U32   => result_vec_to_bytes(exec_mv_si_result_ceq::<u32>  ($data, $op.into(), $idx, &bytes_to_vec_t::<u32>  ($vals_bytes.clone())).await),
//             PodPrimType::U64   => result_vec_to_bytes(exec_mv_si_result_ceq::<u64>  ($data, $op.into(), $idx, &bytes_to_vec_t::<u64>  ($vals_bytes.clone())).await),
//             PodPrimType::Usize => result_vec_to_bytes(exec_mv_si_result_ceq::<usize>($data, $op.into(), $idx, &bytes_to_vec_t::<usize>($vals_bytes.clone())).await),
//             PodPrimType::U128  => result_vec_to_bytes(exec_mv_si_result_ceq::<u128> ($data, $op.into(), $idx, &bytes_to_vec_t::<u128> ($vals_bytes.clone())).await),
//             PodPrimType::I8    => result_vec_to_bytes(exec_mv_si_result_ceq::<i8>   ($data, $op.into(), $idx, &bytes_to_vec_t::<i8>   ($vals_bytes.clone())).await),
//             PodPrimType::I16   => result_vec_to_bytes(exec_mv_si_result_ceq::<i16>  ($data, $op.into(), $idx, &bytes_to_vec_t::<i16>  ($vals_bytes.clone())).await),
//             PodPrimType::I32   => result_vec_to_bytes(exec_mv_si_result_ceq::<i32>  ($data, $op.into(), $idx, &bytes_to_vec_t::<i32>  ($vals_bytes.clone())).await),
//             PodPrimType::I64   => result_vec_to_bytes(exec_mv_si_result_ceq::<i64>  ($data, $op.into(), $idx, &bytes_to_vec_t::<i64>  ($vals_bytes.clone())).await),
//             PodPrimType::Isize => result_vec_to_bytes(exec_mv_si_result_ceq::<isize>($data, $op.into(), $idx, &bytes_to_vec_t::<isize>($vals_bytes.clone())).await),
//             PodPrimType::I128  => result_vec_to_bytes(exec_mv_si_result_ceq::<i128> ($data, $op.into(), $idx, &bytes_to_vec_t::<i128> ($vals_bytes.clone())).await),
//             PodPrimType::F32   => result_vec_to_bytes(exec_mv_si_result_cex::<f32>  ($data, $op.into(), $idx, &bytes_to_vec_t::<f32>  ($vals_bytes.clone())).await),
//             PodPrimType::F64   => result_vec_to_bytes(exec_mv_si_result_cex::<f64>  ($data, $op.into(), $idx, &bytes_to_vec_t::<f64>  ($vals_bytes.clone())).await),
//         }
//     };
// }

// // Option<T> dispatch macros — only Access (Store) + ReadOnly (Load/Swap) + CompEx

// macro_rules! dispatch_opt_void {
//     ($pod:expr, $data:expr, $op:expr, $idxs_vals:expr, $index_size:expr) => {
//         match $pod {
//             OptionPodType::OptionU8    => exec_mv_mi_void_access::<Option<u8>>   ($data, $op.into(), $idxs_vals, $index_size).await,
//             OptionPodType::OptionU16   => exec_mv_mi_void_access::<Option<u16>>  ($data, $op.into(), $idxs_vals, $index_size).await,
//             OptionPodType::OptionU32   => exec_mv_mi_void_access::<Option<u32>>  ($data, $op.into(), $idxs_vals, $index_size).await,
//             OptionPodType::OptionU64   => exec_mv_mi_void_access::<Option<u64>>  ($data, $op.into(), $idxs_vals, $index_size).await,
//             OptionPodType::OptionUsize => exec_mv_mi_void_access::<Option<usize>>($data, $op.into(), $idxs_vals, $index_size).await,
//             OptionPodType::OptionU128  => exec_mv_mi_void_access::<Option<u128>> ($data, $op.into(), $idxs_vals, $index_size).await,
//             OptionPodType::OptionI8    => exec_mv_mi_void_access::<Option<i8>>   ($data, $op.into(), $idxs_vals, $index_size).await,
//             OptionPodType::OptionI16   => exec_mv_mi_void_access::<Option<i16>>  ($data, $op.into(), $idxs_vals, $index_size).await,
//             OptionPodType::OptionI32   => exec_mv_mi_void_access::<Option<i32>>  ($data, $op.into(), $idxs_vals, $index_size).await,
//             OptionPodType::OptionI64   => exec_mv_mi_void_access::<Option<i64>>  ($data, $op.into(), $idxs_vals, $index_size).await,
//             OptionPodType::OptionIsize => exec_mv_mi_void_access::<Option<isize>>($data, $op.into(), $idxs_vals, $index_size).await,
//             OptionPodType::OptionI128  => exec_mv_mi_void_access::<Option<i128>> ($data, $op.into(), $idxs_vals, $index_size).await,
//             OptionPodType::OptionF32   => exec_mv_mi_void_access::<Option<f32>>  ($data, $op.into(), $idxs_vals, $index_size).await,
//             OptionPodType::OptionF64   => exec_mv_mi_void_access::<Option<f64>>  ($data, $op.into(), $idxs_vals, $index_size).await,
//         }
//     };
// }

// macro_rules! dispatch_opt_fetch {
//     ($pod:expr, $data:expr, $op:expr, $idxs_vals:expr, $index_size:expr) => {
//         match $pod {
//             OptionPodType::OptionU8    => vec_t_to_bytes(exec_mv_mi_fetch_access::<Option<u8>>   ($data, $op.into(), $idxs_vals, $index_size).await),
//             OptionPodType::OptionU16   => vec_t_to_bytes(exec_mv_mi_fetch_access::<Option<u16>>  ($data, $op.into(), $idxs_vals, $index_size).await),
//             OptionPodType::OptionU32   => vec_t_to_bytes(exec_mv_mi_fetch_access::<Option<u32>>  ($data, $op.into(), $idxs_vals, $index_size).await),
//             OptionPodType::OptionU64   => vec_t_to_bytes(exec_mv_mi_fetch_access::<Option<u64>>  ($data, $op.into(), $idxs_vals, $index_size).await),
//             OptionPodType::OptionUsize => vec_t_to_bytes(exec_mv_mi_fetch_access::<Option<usize>>($data, $op.into(), $idxs_vals, $index_size).await),
//             OptionPodType::OptionU128  => vec_t_to_bytes(exec_mv_mi_fetch_access::<Option<u128>> ($data, $op.into(), $idxs_vals, $index_size).await),
//             OptionPodType::OptionI8    => vec_t_to_bytes(exec_mv_mi_fetch_access::<Option<i8>>   ($data, $op.into(), $idxs_vals, $index_size).await),
//             OptionPodType::OptionI16   => vec_t_to_bytes(exec_mv_mi_fetch_access::<Option<i16>>  ($data, $op.into(), $idxs_vals, $index_size).await),
//             OptionPodType::OptionI32   => vec_t_to_bytes(exec_mv_mi_fetch_access::<Option<i32>>  ($data, $op.into(), $idxs_vals, $index_size).await),
//             OptionPodType::OptionI64   => vec_t_to_bytes(exec_mv_mi_fetch_access::<Option<i64>>  ($data, $op.into(), $idxs_vals, $index_size).await),
//             OptionPodType::OptionIsize => vec_t_to_bytes(exec_mv_mi_fetch_access::<Option<isize>>($data, $op.into(), $idxs_vals, $index_size).await),
//             OptionPodType::OptionI128  => vec_t_to_bytes(exec_mv_mi_fetch_access::<Option<i128>> ($data, $op.into(), $idxs_vals, $index_size).await),
//             OptionPodType::OptionF32   => vec_t_to_bytes(exec_mv_mi_fetch_access::<Option<f32>>  ($data, $op.into(), $idxs_vals, $index_size).await),
//             OptionPodType::OptionF64   => vec_t_to_bytes(exec_mv_mi_fetch_access::<Option<f64>>  ($data, $op.into(), $idxs_vals, $index_size).await),
//         }
//     };
// }

// macro_rules! dispatch_opt_result {
//     ($pod:expr, $data:expr, $op:expr, $idxs_vals:expr, $index_size:expr) => {
//         match $pod {
//             OptionPodType::OptionU8    => result_vec_to_bytes(exec_mv_mi_result_ceq::<Option<u8>>   ($data, $op.into(), $idxs_vals, $index_size).await),
//             OptionPodType::OptionU16   => result_vec_to_bytes(exec_mv_mi_result_ceq::<Option<u16>>  ($data, $op.into(), $idxs_vals, $index_size).await),
//             OptionPodType::OptionU32   => result_vec_to_bytes(exec_mv_mi_result_ceq::<Option<u32>>  ($data, $op.into(), $idxs_vals, $index_size).await),
//             OptionPodType::OptionU64   => result_vec_to_bytes(exec_mv_mi_result_ceq::<Option<u64>>  ($data, $op.into(), $idxs_vals, $index_size).await),
//             OptionPodType::OptionUsize => result_vec_to_bytes(exec_mv_mi_result_ceq::<Option<usize>>($data, $op.into(), $idxs_vals, $index_size).await),
//             OptionPodType::OptionU128  => result_vec_to_bytes(exec_mv_mi_result_ceq::<Option<u128>> ($data, $op.into(), $idxs_vals, $index_size).await),
//             OptionPodType::OptionI8    => result_vec_to_bytes(exec_mv_mi_result_ceq::<Option<i8>>   ($data, $op.into(), $idxs_vals, $index_size).await),
//             OptionPodType::OptionI16   => result_vec_to_bytes(exec_mv_mi_result_ceq::<Option<i16>>  ($data, $op.into(), $idxs_vals, $index_size).await),
//             OptionPodType::OptionI32   => result_vec_to_bytes(exec_mv_mi_result_ceq::<Option<i32>>  ($data, $op.into(), $idxs_vals, $index_size).await),
//             OptionPodType::OptionI64   => result_vec_to_bytes(exec_mv_mi_result_ceq::<Option<i64>>  ($data, $op.into(), $idxs_vals, $index_size).await),
//             OptionPodType::OptionIsize => result_vec_to_bytes(exec_mv_mi_result_ceq::<Option<isize>>($data, $op.into(), $idxs_vals, $index_size).await),
//             OptionPodType::OptionI128  => result_vec_to_bytes(exec_mv_mi_result_ceq::<Option<i128>> ($data, $op.into(), $idxs_vals, $index_size).await),
//             OptionPodType::OptionF32 | OptionPodType::OptionF64 =>
//                 panic!("CompareExchange not supported for Option<f32>/Option<f64>"),
//         }
//     };
// }

// macro_rules! dispatch_opt_sv_mi_void {
//     ($pod:expr, $data:expr, $op:expr, $val_bytes:expr, $idxs:expr, $index_size:expr) => {
//         match $pod {
//             OptionPodType::OptionU8    => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<u8>)};    exec_sv_mi_void_access($data, $op.into(), v, $idxs, $index_size).await }
//             OptionPodType::OptionU16   => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<u16>)};   exec_sv_mi_void_access($data, $op.into(), v, $idxs, $index_size).await }
//             OptionPodType::OptionU32   => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<u32>)};   exec_sv_mi_void_access($data, $op.into(), v, $idxs, $index_size).await }
//             OptionPodType::OptionU64   => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<u64>)};   exec_sv_mi_void_access($data, $op.into(), v, $idxs, $index_size).await }
//             OptionPodType::OptionUsize => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<usize>)}; exec_sv_mi_void_access($data, $op.into(), v, $idxs, $index_size).await }
//             OptionPodType::OptionU128  => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<u128>)};  exec_sv_mi_void_access($data, $op.into(), v, $idxs, $index_size).await }
//             OptionPodType::OptionI8    => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<i8>)};    exec_sv_mi_void_access($data, $op.into(), v, $idxs, $index_size).await }
//             OptionPodType::OptionI16   => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<i16>)};   exec_sv_mi_void_access($data, $op.into(), v, $idxs, $index_size).await }
//             OptionPodType::OptionI32   => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<i32>)};   exec_sv_mi_void_access($data, $op.into(), v, $idxs, $index_size).await }
//             OptionPodType::OptionI64   => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<i64>)};   exec_sv_mi_void_access($data, $op.into(), v, $idxs, $index_size).await }
//             OptionPodType::OptionIsize => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<isize>)}; exec_sv_mi_void_access($data, $op.into(), v, $idxs, $index_size).await }
//             OptionPodType::OptionI128  => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<i128>)};  exec_sv_mi_void_access($data, $op.into(), v, $idxs, $index_size).await }
//             OptionPodType::OptionF32   => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<f32>)};   exec_sv_mi_void_access($data, $op.into(), v, $idxs, $index_size).await }
//             OptionPodType::OptionF64   => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<f64>)};   exec_sv_mi_void_access($data, $op.into(), v, $idxs, $index_size).await }
//         }
//     };
// }

// macro_rules! dispatch_opt_sv_mi_fetch {
//     ($pod:expr, $data:expr, $op:expr, $val_bytes:expr, $idxs:expr, $index_size:expr) => {
//         match $pod {
//             OptionPodType::OptionU8    => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<u8>)};    vec_t_to_bytes(exec_sv_mi_fetch_access($data, $op.into(), v, $idxs, $index_size).await) }
//             OptionPodType::OptionU16   => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<u16>)};   vec_t_to_bytes(exec_sv_mi_fetch_access($data, $op.into(), v, $idxs, $index_size).await) }
//             OptionPodType::OptionU32   => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<u32>)};   vec_t_to_bytes(exec_sv_mi_fetch_access($data, $op.into(), v, $idxs, $index_size).await) }
//             OptionPodType::OptionU64   => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<u64>)};   vec_t_to_bytes(exec_sv_mi_fetch_access($data, $op.into(), v, $idxs, $index_size).await) }
//             OptionPodType::OptionUsize => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<usize>)}; vec_t_to_bytes(exec_sv_mi_fetch_access($data, $op.into(), v, $idxs, $index_size).await) }
//             OptionPodType::OptionU128  => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<u128>)};  vec_t_to_bytes(exec_sv_mi_fetch_access($data, $op.into(), v, $idxs, $index_size).await) }
//             OptionPodType::OptionI8    => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<i8>)};    vec_t_to_bytes(exec_sv_mi_fetch_access($data, $op.into(), v, $idxs, $index_size).await) }
//             OptionPodType::OptionI16   => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<i16>)};   vec_t_to_bytes(exec_sv_mi_fetch_access($data, $op.into(), v, $idxs, $index_size).await) }
//             OptionPodType::OptionI32   => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<i32>)};   vec_t_to_bytes(exec_sv_mi_fetch_access($data, $op.into(), v, $idxs, $index_size).await) }
//             OptionPodType::OptionI64   => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<i64>)};   vec_t_to_bytes(exec_sv_mi_fetch_access($data, $op.into(), v, $idxs, $index_size).await) }
//             OptionPodType::OptionIsize => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<isize>)}; vec_t_to_bytes(exec_sv_mi_fetch_access($data, $op.into(), v, $idxs, $index_size).await) }
//             OptionPodType::OptionI128  => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<i128>)};  vec_t_to_bytes(exec_sv_mi_fetch_access($data, $op.into(), v, $idxs, $index_size).await) }
//             OptionPodType::OptionF32   => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<f32>)};   vec_t_to_bytes(exec_sv_mi_fetch_access($data, $op.into(), v, $idxs, $index_size).await) }
//             OptionPodType::OptionF64   => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<f64>)};   vec_t_to_bytes(exec_sv_mi_fetch_access($data, $op.into(), v, $idxs, $index_size).await) }
//         }
//     };
// }

// macro_rules! dispatch_opt_sv_mi_result {
//     ($pod:expr, $data:expr, $op:expr, $val_bytes:expr, $idxs:expr, $index_size:expr) => {
//         match $pod {
//             OptionPodType::OptionU8    => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<u8>)};    result_vec_to_bytes(exec_sv_mi_result_ceq($data, $op.into(), v, $idxs, $index_size).await) }
//             OptionPodType::OptionU16   => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<u16>)};   result_vec_to_bytes(exec_sv_mi_result_ceq($data, $op.into(), v, $idxs, $index_size).await) }
//             OptionPodType::OptionU32   => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<u32>)};   result_vec_to_bytes(exec_sv_mi_result_ceq($data, $op.into(), v, $idxs, $index_size).await) }
//             OptionPodType::OptionU64   => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<u64>)};   result_vec_to_bytes(exec_sv_mi_result_ceq($data, $op.into(), v, $idxs, $index_size).await) }
//             OptionPodType::OptionUsize => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<usize>)}; result_vec_to_bytes(exec_sv_mi_result_ceq($data, $op.into(), v, $idxs, $index_size).await) }
//             OptionPodType::OptionU128  => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<u128>)};  result_vec_to_bytes(exec_sv_mi_result_ceq($data, $op.into(), v, $idxs, $index_size).await) }
//             OptionPodType::OptionI8    => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<i8>)};    result_vec_to_bytes(exec_sv_mi_result_ceq($data, $op.into(), v, $idxs, $index_size).await) }
//             OptionPodType::OptionI16   => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<i16>)};   result_vec_to_bytes(exec_sv_mi_result_ceq($data, $op.into(), v, $idxs, $index_size).await) }
//             OptionPodType::OptionI32   => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<i32>)};   result_vec_to_bytes(exec_sv_mi_result_ceq($data, $op.into(), v, $idxs, $index_size).await) }
//             OptionPodType::OptionI64   => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<i64>)};   result_vec_to_bytes(exec_sv_mi_result_ceq($data, $op.into(), v, $idxs, $index_size).await) }
//             OptionPodType::OptionIsize => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<isize>)}; result_vec_to_bytes(exec_sv_mi_result_ceq($data, $op.into(), v, $idxs, $index_size).await) }
//             OptionPodType::OptionI128  => { let v = unsafe{*($val_bytes.as_ptr() as *const Option<i128>)};  result_vec_to_bytes(exec_sv_mi_result_ceq($data, $op.into(), v, $idxs, $index_size).await) }
//             OptionPodType::OptionF32 | OptionPodType::OptionF64 =>
//                 panic!("CompareExchange not supported for Option<f32>/Option<f64>"),
//         }
//     };
// }

// macro_rules! dispatch_opt_mv_si_void {
//     ($pod:expr, $data:expr, $op:expr, $vals_bytes:expr, $idx:expr) => {
//         match $pod {
//             OptionPodType::OptionU8    => exec_mv_si_void_access($data, $op.into(), $idx, &bytes_to_vec_t::<Option<u8>>   ($vals_bytes.clone())).await,
//             OptionPodType::OptionU16   => exec_mv_si_void_access($data, $op.into(), $idx, &bytes_to_vec_t::<Option<u16>>  ($vals_bytes.clone())).await,
//             OptionPodType::OptionU32   => exec_mv_si_void_access($data, $op.into(), $idx, &bytes_to_vec_t::<Option<u32>>  ($vals_bytes.clone())).await,
//             OptionPodType::OptionU64   => exec_mv_si_void_access($data, $op.into(), $idx, &bytes_to_vec_t::<Option<u64>>  ($vals_bytes.clone())).await,
//             OptionPodType::OptionUsize => exec_mv_si_void_access($data, $op.into(), $idx, &bytes_to_vec_t::<Option<usize>>($vals_bytes.clone())).await,
//             OptionPodType::OptionU128  => exec_mv_si_void_access($data, $op.into(), $idx, &bytes_to_vec_t::<Option<u128>> ($vals_bytes.clone())).await,
//             OptionPodType::OptionI8    => exec_mv_si_void_access($data, $op.into(), $idx, &bytes_to_vec_t::<Option<i8>>   ($vals_bytes.clone())).await,
//             OptionPodType::OptionI16   => exec_mv_si_void_access($data, $op.into(), $idx, &bytes_to_vec_t::<Option<i16>>  ($vals_bytes.clone())).await,
//             OptionPodType::OptionI32   => exec_mv_si_void_access($data, $op.into(), $idx, &bytes_to_vec_t::<Option<i32>>  ($vals_bytes.clone())).await,
//             OptionPodType::OptionI64   => exec_mv_si_void_access($data, $op.into(), $idx, &bytes_to_vec_t::<Option<i64>>  ($vals_bytes.clone())).await,
//             OptionPodType::OptionIsize => exec_mv_si_void_access($data, $op.into(), $idx, &bytes_to_vec_t::<Option<isize>>($vals_bytes.clone())).await,
//             OptionPodType::OptionI128  => exec_mv_si_void_access($data, $op.into(), $idx, &bytes_to_vec_t::<Option<i128>> ($vals_bytes.clone())).await,
//             OptionPodType::OptionF32   => exec_mv_si_void_access($data, $op.into(), $idx, &bytes_to_vec_t::<Option<f32>>  ($vals_bytes.clone())).await,
//             OptionPodType::OptionF64   => exec_mv_si_void_access($data, $op.into(), $idx, &bytes_to_vec_t::<Option<f64>>  ($vals_bytes.clone())).await,
//         }
//     };
// }

// macro_rules! dispatch_opt_mv_si_fetch {
//     ($pod:expr, $data:expr, $op:expr, $vals_bytes:expr, $idx:expr) => {
//         match $pod {
//             OptionPodType::OptionU8    => vec_t_to_bytes(exec_mv_si_fetch_access($data, $op.into(), $idx, &bytes_to_vec_t::<Option<u8>>   ($vals_bytes.clone())).await),
//             OptionPodType::OptionU16   => vec_t_to_bytes(exec_mv_si_fetch_access($data, $op.into(), $idx, &bytes_to_vec_t::<Option<u16>>  ($vals_bytes.clone())).await),
//             OptionPodType::OptionU32   => vec_t_to_bytes(exec_mv_si_fetch_access($data, $op.into(), $idx, &bytes_to_vec_t::<Option<u32>>  ($vals_bytes.clone())).await),
//             OptionPodType::OptionU64   => vec_t_to_bytes(exec_mv_si_fetch_access($data, $op.into(), $idx, &bytes_to_vec_t::<Option<u64>>  ($vals_bytes.clone())).await),
//             OptionPodType::OptionUsize => vec_t_to_bytes(exec_mv_si_fetch_access($data, $op.into(), $idx, &bytes_to_vec_t::<Option<usize>>($vals_bytes.clone())).await),
//             OptionPodType::OptionU128  => vec_t_to_bytes(exec_mv_si_fetch_access($data, $op.into(), $idx, &bytes_to_vec_t::<Option<u128>> ($vals_bytes.clone())).await),
//             OptionPodType::OptionI8    => vec_t_to_bytes(exec_mv_si_fetch_access($data, $op.into(), $idx, &bytes_to_vec_t::<Option<i8>>   ($vals_bytes.clone())).await),
//             OptionPodType::OptionI16   => vec_t_to_bytes(exec_mv_si_fetch_access($data, $op.into(), $idx, &bytes_to_vec_t::<Option<i16>>  ($vals_bytes.clone())).await),
//             OptionPodType::OptionI32   => vec_t_to_bytes(exec_mv_si_fetch_access($data, $op.into(), $idx, &bytes_to_vec_t::<Option<i32>>  ($vals_bytes.clone())).await),
//             OptionPodType::OptionI64   => vec_t_to_bytes(exec_mv_si_fetch_access($data, $op.into(), $idx, &bytes_to_vec_t::<Option<i64>>  ($vals_bytes.clone())).await),
//             OptionPodType::OptionIsize => vec_t_to_bytes(exec_mv_si_fetch_access($data, $op.into(), $idx, &bytes_to_vec_t::<Option<isize>>($vals_bytes.clone())).await),
//             OptionPodType::OptionI128  => vec_t_to_bytes(exec_mv_si_fetch_access($data, $op.into(), $idx, &bytes_to_vec_t::<Option<i128>> ($vals_bytes.clone())).await),
//             OptionPodType::OptionF32   => vec_t_to_bytes(exec_mv_si_fetch_access($data, $op.into(), $idx, &bytes_to_vec_t::<Option<f32>>  ($vals_bytes.clone())).await),
//             OptionPodType::OptionF64   => vec_t_to_bytes(exec_mv_si_fetch_access($data, $op.into(), $idx, &bytes_to_vec_t::<Option<f64>>  ($vals_bytes.clone())).await),
//         }
//     };
// }

// macro_rules! dispatch_opt_mv_si_result {
//     ($pod:expr, $data:expr, $op:expr, $vals_bytes:expr, $idx:expr) => {
//         match $pod {
//             OptionPodType::OptionU8    => result_vec_to_bytes(exec_mv_si_result_ceq($data, $op.into(), $idx, &bytes_to_vec_t::<Option<u8>>   ($vals_bytes.clone())).await),
//             OptionPodType::OptionU16   => result_vec_to_bytes(exec_mv_si_result_ceq($data, $op.into(), $idx, &bytes_to_vec_t::<Option<u16>>  ($vals_bytes.clone())).await),
//             OptionPodType::OptionU32   => result_vec_to_bytes(exec_mv_si_result_ceq($data, $op.into(), $idx, &bytes_to_vec_t::<Option<u32>>  ($vals_bytes.clone())).await),
//             OptionPodType::OptionU64   => result_vec_to_bytes(exec_mv_si_result_ceq($data, $op.into(), $idx, &bytes_to_vec_t::<Option<u64>>  ($vals_bytes.clone())).await),
//             OptionPodType::OptionUsize => result_vec_to_bytes(exec_mv_si_result_ceq($data, $op.into(), $idx, &bytes_to_vec_t::<Option<usize>>($vals_bytes.clone())).await),
//             OptionPodType::OptionU128  => result_vec_to_bytes(exec_mv_si_result_ceq($data, $op.into(), $idx, &bytes_to_vec_t::<Option<u128>> ($vals_bytes.clone())).await),
//             OptionPodType::OptionI8    => result_vec_to_bytes(exec_mv_si_result_ceq($data, $op.into(), $idx, &bytes_to_vec_t::<Option<i8>>   ($vals_bytes.clone())).await),
//             OptionPodType::OptionI16   => result_vec_to_bytes(exec_mv_si_result_ceq($data, $op.into(), $idx, &bytes_to_vec_t::<Option<i16>>  ($vals_bytes.clone())).await),
//             OptionPodType::OptionI32   => result_vec_to_bytes(exec_mv_si_result_ceq($data, $op.into(), $idx, &bytes_to_vec_t::<Option<i32>>  ($vals_bytes.clone())).await),
//             OptionPodType::OptionI64   => result_vec_to_bytes(exec_mv_si_result_ceq($data, $op.into(), $idx, &bytes_to_vec_t::<Option<i64>>  ($vals_bytes.clone())).await),
//             OptionPodType::OptionIsize => result_vec_to_bytes(exec_mv_si_result_ceq($data, $op.into(), $idx, &bytes_to_vec_t::<Option<isize>>($vals_bytes.clone())).await),
//             OptionPodType::OptionI128  => result_vec_to_bytes(exec_mv_si_result_ceq($data, $op.into(), $idx, &bytes_to_vec_t::<Option<i128>> ($vals_bytes.clone())).await),
//             OptionPodType::OptionF32 | OptionPodType::OptionF64 =>
//                 panic!("CompareExchange not supported for Option<f32>/Option<f64>"),
//         }
//     };
// }

// // ── Static AM structs: 9 for primitive types ──────────────────────────────────

// #[doc(hidden)]
// #[lamellar_impl::AmDataRT(AmGroup(false))]
// pub struct PodMvMiVoidAm {
//     pub data: LamellarByteArray,
//     pub op: ArrayOpCmd<Vec<u8>>,
//     pub idxs_vals: Vec<u8>,
//     pub index_size: u8,
//     pub pod_type: PodPrimType,
// }
// #[lamellar_impl::rt_am]
// impl LamellarAM for PodMvMiVoidAm {
//     async fn exec(&self) {
//         let mut data = self.data.clone();
//         dispatch_prim_void!(self.pod_type, &mut data, self.op.clone(), &self.idxs_vals, self.index_size);
//     }
// }

// #[doc(hidden)]
// #[lamellar_impl::AmDataRT(AmGroup(false))]
// pub struct PodMvMiFetchAm {
//     pub data: LamellarByteArray,
//     pub op: ArrayOpCmd<Vec<u8>>,
//     pub idxs_vals: Vec<u8>,
//     pub index_size: u8,
//     pub pod_type: PodPrimType,
// }
// #[lamellar_impl::rt_am]
// impl LamellarAM for PodMvMiFetchAm {
//     async fn exec(&self) -> Vec<u8> {
//         let mut data = self.data.clone();
//         dispatch_prim_fetch!(self.pod_type, &mut data, self.op.clone(), &self.idxs_vals, self.index_size)
//     }
// }

// #[doc(hidden)]
// #[lamellar_impl::AmDataRT(AmGroup(false))]
// pub struct PodMvMiResultAm {
//     pub data: LamellarByteArray,
//     pub op: ArrayOpCmd<Vec<u8>>,
//     pub idxs_vals: Vec<u8>,
//     pub index_size: u8,
//     pub pod_type: PodPrimType,
// }
// #[lamellar_impl::rt_am]
// impl LamellarAM for PodMvMiResultAm {
//     async fn exec(&self) -> Vec<u8> {
//         let mut data = self.data.clone();
//         dispatch_prim_result!(self.pod_type, &mut data, self.op.clone(), &self.idxs_vals, self.index_size)
//     }
// }

// #[doc(hidden)]
// #[lamellar_impl::AmDataRT(AmGroup(false))]
// pub struct PodSvMiVoidAm {
//     pub data: LamellarByteArray,
//     pub op: ArrayOpCmd<Vec<u8>>,
//     pub val_bytes: Vec<u8>,
//     pub idxs: Vec<u8>,
//     pub index_size: u8,
//     pub pod_type: PodPrimType,
// }
// #[lamellar_impl::rt_am]
// impl LamellarAM for PodSvMiVoidAm {
//     async fn exec(&self) {
//         let mut data = self.data.clone();
//         dispatch_prim_sv_mi_void!(self.pod_type, &mut data, self.op.clone(), &self.val_bytes, &self.idxs, self.index_size);
//     }
// }

// #[doc(hidden)]
// #[lamellar_impl::AmDataRT(AmGroup(false))]
// pub struct PodSvMiFetchAm {
//     pub data: LamellarByteArray,
//     pub op: ArrayOpCmd<Vec<u8>>,
//     pub val_bytes: Vec<u8>,
//     pub idxs: Vec<u8>,
//     pub index_size: u8,
//     pub pod_type: PodPrimType,
// }
// #[lamellar_impl::rt_am]
// impl LamellarAM for PodSvMiFetchAm {
//     async fn exec(&self) -> Vec<u8> {
//         let mut data = self.data.clone();
//         dispatch_prim_sv_mi_fetch!(self.pod_type, &mut data, self.op.clone(), &self.val_bytes, &self.idxs, self.index_size)
//     }
// }

// #[doc(hidden)]
// #[lamellar_impl::AmDataRT(AmGroup(false))]
// pub struct PodSvMiResultAm {
//     pub data: LamellarByteArray,
//     pub op: ArrayOpCmd<Vec<u8>>,
//     pub val_bytes: Vec<u8>,
//     pub idxs: Vec<u8>,
//     pub index_size: u8,
//     pub pod_type: PodPrimType,
// }
// #[lamellar_impl::rt_am]
// impl LamellarAM for PodSvMiResultAm {
//     async fn exec(&self) -> Vec<u8> {
//         let mut data = self.data.clone();
//         dispatch_prim_sv_mi_result!(self.pod_type, &mut data, self.op.clone(), &self.val_bytes, &self.idxs, self.index_size)
//     }
// }

// #[doc(hidden)]
// #[lamellar_impl::AmDataRT(AmGroup(false))]
// pub struct PodMvSiVoidAm {
//     pub data: LamellarByteArray,
//     pub op: ArrayOpCmd<Vec<u8>>,
//     pub idx: usize,
//     pub vals_bytes: Vec<u8>,
//     pub pod_type: PodPrimType,
// }
// #[lamellar_impl::rt_am]
// impl LamellarAM for PodMvSiVoidAm {
//     async fn exec(&self) {
//         let mut data = self.data.clone();
//         dispatch_prim_mv_si_void!(self.pod_type, &mut data, self.op.clone(), self.vals_bytes.clone(), self.idx);
//     }
// }

// #[doc(hidden)]
// #[lamellar_impl::AmDataRT(AmGroup(false))]
// pub struct PodMvSiFetchAm {
//     pub data: LamellarByteArray,
//     pub op: ArrayOpCmd<Vec<u8>>,
//     pub idx: usize,
//     pub vals_bytes: Vec<u8>,
//     pub pod_type: PodPrimType,
// }
// #[lamellar_impl::rt_am]
// impl LamellarAM for PodMvSiFetchAm {
//     async fn exec(&self) -> Vec<u8> {
//         let mut data = self.data.clone();
//         dispatch_prim_mv_si_fetch!(self.pod_type, &mut data, self.op.clone(), self.vals_bytes.clone(), self.idx)
//     }
// }

// #[doc(hidden)]
// #[lamellar_impl::AmDataRT(AmGroup(false))]
// pub struct PodMvSiResultAm {
//     pub data: LamellarByteArray,
//     pub op: ArrayOpCmd<Vec<u8>>,
//     pub idx: usize,
//     pub vals_bytes: Vec<u8>,
//     pub pod_type: PodPrimType,
// }
// #[lamellar_impl::rt_am]
// impl LamellarAM for PodMvSiResultAm {
//     async fn exec(&self) -> Vec<u8> {
//         let mut data = self.data.clone();
//         dispatch_prim_mv_si_result!(self.pod_type, &mut data, self.op.clone(), self.vals_bytes.clone(), self.idx)
//     }
// }

// // ── Static AM structs: 9 for Option<prim> types ───────────────────────────────

// #[doc(hidden)]
// #[lamellar_impl::AmDataRT(AmGroup(false))]
// pub struct OptMvMiVoidAm {
//     pub data: LamellarByteArray,
//     pub op: ArrayOpCmd<Vec<u8>>,
//     pub idxs_vals: Vec<u8>,
//     pub index_size: u8,
//     pub pod_type: OptionPodType,
// }
// #[lamellar_impl::rt_am]
// impl LamellarAM for OptMvMiVoidAm {
//     async fn exec(&self) {
//         let mut data = self.data.clone();
//         dispatch_opt_void!(self.pod_type, &mut data, self.op.clone(), &self.idxs_vals, self.index_size);
//     }
// }

// #[doc(hidden)]
// #[lamellar_impl::AmDataRT(AmGroup(false))]
// pub struct OptMvMiFetchAm {
//     pub data: LamellarByteArray,
//     pub op: ArrayOpCmd<Vec<u8>>,
//     pub idxs_vals: Vec<u8>,
//     pub index_size: u8,
//     pub pod_type: OptionPodType,
// }
// #[lamellar_impl::rt_am]
// impl LamellarAM for OptMvMiFetchAm {
//     async fn exec(&self) -> Vec<u8> {
//         let mut data = self.data.clone();
//         dispatch_opt_fetch!(self.pod_type, &mut data, self.op.clone(), &self.idxs_vals, self.index_size)
//     }
// }

// #[doc(hidden)]
// #[lamellar_impl::AmDataRT(AmGroup(false))]
// pub struct OptMvMiResultAm {
//     pub data: LamellarByteArray,
//     pub op: ArrayOpCmd<Vec<u8>>,
//     pub idxs_vals: Vec<u8>,
//     pub index_size: u8,
//     pub pod_type: OptionPodType,
// }
// #[lamellar_impl::rt_am]
// impl LamellarAM for OptMvMiResultAm {
//     async fn exec(&self) -> Vec<u8> {
//         let mut data = self.data.clone();
//         dispatch_opt_result!(self.pod_type, &mut data, self.op.clone(), &self.idxs_vals, self.index_size)
//     }
// }

// #[doc(hidden)]
// #[lamellar_impl::AmDataRT(AmGroup(false))]
// pub struct OptSvMiVoidAm {
//     pub data: LamellarByteArray,
//     pub op: ArrayOpCmd<Vec<u8>>,
//     pub val_bytes: Vec<u8>,
//     pub idxs: Vec<u8>,
//     pub index_size: u8,
//     pub pod_type: OptionPodType,
// }
// #[lamellar_impl::rt_am]
// impl LamellarAM for OptSvMiVoidAm {
//     async fn exec(&self) {
//         let mut data = self.data.clone();
//         dispatch_opt_sv_mi_void!(self.pod_type, &mut data, self.op.clone(), &self.val_bytes, &self.idxs, self.index_size);
//     }
// }

// #[doc(hidden)]
// #[lamellar_impl::AmDataRT(AmGroup(false))]
// pub struct OptSvMiFetchAm {
//     pub data: LamellarByteArray,
//     pub op: ArrayOpCmd<Vec<u8>>,
//     pub val_bytes: Vec<u8>,
//     pub idxs: Vec<u8>,
//     pub index_size: u8,
//     pub pod_type: OptionPodType,
// }
// #[lamellar_impl::rt_am]
// impl LamellarAM for OptSvMiFetchAm {
//     async fn exec(&self) -> Vec<u8> {
//         let mut data = self.data.clone();
//         dispatch_opt_sv_mi_fetch!(self.pod_type, &mut data, self.op.clone(), &self.val_bytes, &self.idxs, self.index_size)
//     }
// }

// #[doc(hidden)]
// #[lamellar_impl::AmDataRT(AmGroup(false))]
// pub struct OptSvMiResultAm {
//     pub data: LamellarByteArray,
//     pub op: ArrayOpCmd<Vec<u8>>,
//     pub val_bytes: Vec<u8>,
//     pub idxs: Vec<u8>,
//     pub index_size: u8,
//     pub pod_type: OptionPodType,
// }
// #[lamellar_impl::rt_am]
// impl LamellarAM for OptSvMiResultAm {
//     async fn exec(&self) -> Vec<u8> {
//         let mut data = self.data.clone();
//         dispatch_opt_sv_mi_result!(self.pod_type, &mut data, self.op.clone(), &self.val_bytes, &self.idxs, self.index_size)
//     }
// }

// #[doc(hidden)]
// #[lamellar_impl::AmDataRT(AmGroup(false))]
// pub struct OptMvSiVoidAm {
//     pub data: LamellarByteArray,
//     pub op: ArrayOpCmd<Vec<u8>>,
//     pub idx: usize,
//     pub vals_bytes: Vec<u8>,
//     pub pod_type: OptionPodType,
// }
// #[lamellar_impl::rt_am]
// impl LamellarAM for OptMvSiVoidAm {
//     async fn exec(&self) {
//         let mut data = self.data.clone();
//         dispatch_opt_mv_si_void!(self.pod_type, &mut data, self.op.clone(), self.vals_bytes.clone(), self.idx);
//     }
// }

// #[doc(hidden)]
// #[lamellar_impl::AmDataRT(AmGroup(false))]
// pub struct OptMvSiFetchAm {
//     pub data: LamellarByteArray,
//     pub op: ArrayOpCmd<Vec<u8>>,
//     pub idx: usize,
//     pub vals_bytes: Vec<u8>,
//     pub pod_type: OptionPodType,
// }
// #[lamellar_impl::rt_am]
// impl LamellarAM for OptMvSiFetchAm {
//     async fn exec(&self) -> Vec<u8> {
//         let mut data = self.data.clone();
//         dispatch_opt_mv_si_fetch!(self.pod_type, &mut data, self.op.clone(), self.vals_bytes.clone(), self.idx)
//     }
// }

// #[doc(hidden)]
// #[lamellar_impl::AmDataRT(AmGroup(false))]
// pub struct OptMvSiResultAm {
//     pub data: LamellarByteArray,
//     pub op: ArrayOpCmd<Vec<u8>>,
//     pub idx: usize,
//     pub vals_bytes: Vec<u8>,
//     pub pod_type: OptionPodType,
// }
// #[lamellar_impl::rt_am]
// impl LamellarAM for OptMvSiResultAm {
//     async fn exec(&self) -> Vec<u8> {
//         let mut data = self.data.clone();
//         dispatch_opt_mv_si_result!(self.pod_type, &mut data, self.op.clone(), self.vals_bytes.clone(), self.idx)
//     }
// }
