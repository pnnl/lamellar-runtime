use crate::array::*;
use crate::OneSidedMemoryRegion;
use crate::Remote;

#[crabtime::function]
fn impl_ops_scalar_type_match(crabtime::pattern!($bytes:expr, $bytes_type:ty,$result:expr): _) {
    let mut match_arms = Vec::new();
    let bytes = stringify!($bytes);
    let bytes_type = stringify!($bytes_type);
    let result = stringify!($result);

    let (
        aritmetic_ops,
        bitwise_ops,
        shift_ops,
        read_ops,
        write_ops,
        comp_ex_op,
        comp_ex_eps_op,
        return_type,
    ) = if result == "false" {
        //false means not returning anything, true means returning a OneSidedMemoryRegion<u8>
        let aritmetic_ops = crabtime::quote! {
            ArrayOpCmd::Add => {array.mut_local_data().await.local_add(idx_vals);},
            ArrayOpCmd::Sub => {array.mut_local_data().await.local_sub(idx_vals);},
            ArrayOpCmd::Mul => {array.mut_local_data().await.local_mul(idx_vals);},
            ArrayOpCmd::Div => {array.mut_local_data().await.local_div(idx_vals);},
            ArrayOpCmd::Rem => {array.mut_local_data().await.local_rem(idx_vals);},
        };
        let bitwise_ops = crabtime::quote! {
            ArrayOpCmd::And => {array.mut_local_data().await.local_bit_and(idx_vals);},
            ArrayOpCmd::Or => {array.mut_local_data().await.local_bit_or(idx_vals);},
            ArrayOpCmd::Xor => {array.mut_local_data().await.local_bit_xor(idx_vals);},
        };
        let shift_ops = crabtime::quote! {
            ArrayOpCmd::Shl => {array.mut_local_data().await.local_shl(idx_vals);},
            ArrayOpCmd::Shr => {array.mut_local_data().await.local_shr(idx_vals);},
        };
        let read_ops = crabtime::quote! {};
        let write_ops = crabtime::quote! {
            ArrayOpCmd::Store => {array.mut_local_data().await.local_store(idx_vals);},
            ArrayOpCmd::Put => {array.mut_local_data().await.local_store(idx_vals);},
        };
        let comp_ex_op = crabtime::quote! {};
        let comp_ex_eps_op = crabtime::quote! {};
        let return_type = crabtime::quote! {};
        (
            aritmetic_ops,
            bitwise_ops,
            shift_ops,
            read_ops,
            write_ops,
            comp_ex_op,
            comp_ex_eps_op,
            return_type,
        )
    } else {
        let aritmetic_ops = crabtime::quote! {
            ArrayOpCmd::FetchAdd => {self.vec_to_bytes(array.mut_local_data().await.local_fetch_add(idx_vals,true).unwrap()).await},
            ArrayOpCmd::FetchSub => {self.vec_to_bytes(array.mut_local_data().await.local_fetch_sub(idx_vals,true).unwrap()).await},
            ArrayOpCmd::FetchMul => {self.vec_to_bytes(array.mut_local_data().await.local_fetch_mul(idx_vals,true).unwrap()).await},
            ArrayOpCmd::FetchDiv => {self.vec_to_bytes(array.mut_local_data().await.local_fetch_div(idx_vals,true).unwrap()).await},
            ArrayOpCmd::FetchRem => {self.vec_to_bytes(array.mut_local_data().await.local_fetch_rem(idx_vals,true).unwrap()).await},
        };
        let bitwise_ops = crabtime::quote! {
            ArrayOpCmd::FetchAnd => {self.vec_to_bytes(array.mut_local_data().await.local_fetch_bit_and(idx_vals,true).unwrap()).await},
            ArrayOpCmd::FetchOr => {self.vec_to_bytes(array.mut_local_data().await.local_fetch_bit_or(idx_vals,true).unwrap()).await},
            ArrayOpCmd::FetchXor => {self.vec_to_bytes(array.mut_local_data().await.local_fetch_bit_xor(idx_vals,true).unwrap()).await},
        };
        let shift_ops = crabtime::quote! {
            ArrayOpCmd::FetchShl => {self.vec_to_bytes(array.mut_local_data().await.local_fetch_shl(idx_vals,true).unwrap()).await},
            ArrayOpCmd::FetchShr => {self.vec_to_bytes(array.mut_local_data().await.local_fetch_shr(idx_vals,true).unwrap()).await},
        };
        let read_ops = crabtime::quote! {
            ArrayOpCmd::Load => {self.vec_to_bytes(array.local_data().await.local_load(idx_vals)).await},
            ArrayOpCmd::Get => {self.vec_to_bytes(array.local_data().await.local_load(idx_vals)).await},
        };
        let write_ops = crabtime::quote! {
            ArrayOpCmd::Swap => {self.vec_to_bytes(array.mut_local_data().await.local_swap(idx_vals)).await},
        };
        let comp_ex_op = crabtime::quote! {ArrayOpCmd::CompareExchange(old)         => {self.vec_to_bytes(array.mut_local_data().await.local_compare_exchange(idx_vals, old)).await}};
        let comp_ex_eps_op = crabtime::quote! {ArrayOpCmd::CompareExchangeEps(old, eps) => {self.vec_to_bytes(array.mut_local_data().await.local_compare_exchange_epsilon(idx_vals , old, eps)).await}};
        let return_type = crabtime::quote! { -> OneSidedMemoryRegion<u8>};
        (
            aritmetic_ops,
            bitwise_ops,
            shift_ops,
            read_ops,
            write_ops,
            comp_ex_op,
            comp_ex_eps_op,
            return_type,
        )
    };

    for ty in [
        "u8", "u16", "u32", "u64", "usize", "u128", "i8", "i16", "i32", "i64", "isize", "i128",
        "bool", "f32", "f64",
    ]
    .iter()
    {
        let mut op_arms = crabtime::quote! {};
        if *ty == "bool" {
            op_arms = crabtime::quote! {
                {{bitwise_ops}}
                {{read_ops}}
                {{write_ops}}
                {{comp_ex_op}}
            };
        } else if *ty == "f32" || *ty == "f64" {
            op_arms = crabtime::quote! {
                {{aritmetic_ops}}
                {{read_ops}}
                {{write_ops}}
                {{comp_ex_eps_op}}
            };
        } else {
            op_arms = crabtime::quote! {
                {{aritmetic_ops}}
                {{bitwise_ops}}
                {{shift_ops}}
                {{read_ops}}
                {{write_ops}}
                {{comp_ex_op}}
                {{comp_ex_eps_op}}
            };
        }
        match_arms.push(crabtime::quote! {
            ScalarType::{{ty}} => {
                let idx_vals = self.idx_vals::<{{ty}}>({{bytes}});
                let op: ArrayOpCmd<{{ty}}> = self.op.clone().into();
                match op {
                    {{op_arms}}
                    _ => panic!("invalid op cmd"),
                }
            }
        });
    }

    let match_arms = match_arms.join("\n");
    crabtime::output! {
        async fn bare_type(&self, mut bytes: {{bytes_type}}, mut array: LamellarByteArray) {{return_type}} {
            match self.scalar_type {
                {{match_arms}}
                _ => panic!("invalid type for bare array")
            }
        }
    }
}

#[crabtime::function]
fn impl_ops_option_scalar_type_match(
    crabtime::pattern!($bytes:expr, $bytes_type:ty, $result:expr): _,
) {
    let mut match_arms = Vec::new();
    let bytes = stringify!($bytes);
    let bytes_type = stringify!($bytes_type);
    let result = stringify!($result);
    let (write_ops, comp_ex_ops, return_type) = if result == "false" {
        let write_ops = crabtime::quote! {
            ArrayOpCmd::Store => {array.mut_local_data().await.local_store(idx_vals);},
            ArrayOpCmd::Put => {array.mut_local_data().await.local_store(idx_vals);},
        };
        let comp_ex_ops = crabtime::quote! {};
        let return_type = crabtime::quote! {};
        (write_ops, comp_ex_ops, return_type)
    } else {
        let write_ops = crabtime::quote! {
            ArrayOpCmd::Swap => {self.vec_to_bytes(array.mut_local_data().await.local_swap(idx_vals)).await},
        };
        let comp_ex_ops = crabtime::quote! {ArrayOpCmd::CompareExchange(old) => {self.vec_to_bytes(array.mut_local_data().await.local_compare_exchange(idx_vals, old)).await}};
        let return_type = crabtime::quote! { -> OneSidedMemoryRegion<u8>};
        (write_ops, comp_ex_ops, return_type)
    };
    for ty in [
        "u8", "u16", "u32", "u64", "usize", "u128", "i8", "i16", "i32", "i64", "isize", "i128",
        "f32", "f64", "bool",
    ]
    .iter()
    {
        let mut comp_ex_arm = crabtime::quote! {};
        if *ty != "f32" && *ty != "f64" {
            comp_ex_arm = comp_ex_ops.clone();
        }
        match_arms.push(crabtime::quote! {
            ScalarType::{{ty}} => {
                let idx_vals = self.idx_vals::<Option<{{ty}}>>({{bytes}});
                let op: ArrayOpCmd<Option<{{ty}}>> = self.op.clone().into();
                match op {
                    {{write_ops}}
                    {{comp_ex_arm}}
                    _ => panic!("invalid op cmd"),
                }
            }
        });
    }
    let match_arms = match_arms.join("\n");
    crabtime::output! {
        async fn option_type(&self, mut bytes: {{bytes_type}}, mut array: LamellarByteArray) {{return_type}} {
            match self.scalar_type {
                {{match_arms}}
                _ => panic!("invalid type for option array")
            }
        }
    }
}

macro_rules! impl_vec_to_bytes {
    () => {
        async fn vec_to_bytes<T: Remote>(&self, result: Vec<T>) -> OneSidedMemoryRegion<u8> {
            let mut mem_region = self
                .array
                .team()
                .try_alloc_one_sided_mem_region::<T>(result.len());
            while let None = mem_region {
                async_std::task::yield_now().await;
                mem_region = self
                    .array
                    .team()
                    .try_alloc_one_sided_mem_region::<T>(result.len());
            }
            let mem_region = mem_region.unwrap();
            unsafe {
                mem_region.local_copy_from_slice(&result);
                mem_region.to_base::<u8>()
            }
        }
    };
}

#[lamellar_impl::AmDataRT(Clone, AmGroup(false))]
pub(crate) struct PackedIndicies {
    pub(crate) data: Vec<u8>,
    byte_width: usize,
    estimated_elems: usize,
}

impl PackedIndicies {
    // pub(crate) fn new(byte_width: usize) -> PackedIndicies {
    //     PackedIndicies { data: Vec::new(), byte_width , estimated_elems: 0}
    // }
    pub(crate) fn new_with_capacity(byte_width: usize, estimated_elems: usize) -> PackedIndicies {
        let data = Vec::with_capacity(byte_width * estimated_elems);
        PackedIndicies {
            data,
            byte_width,
            estimated_elems,
        }
    }
    pub(crate) fn push<T>(&mut self, value: T) {
        let byte_width = self.byte_width;
        let value_bytes =
            unsafe { std::slice::from_raw_parts((&value as *const T) as *const u8, byte_width) };
        self.data.extend_from_slice(value_bytes);
    }
    pub(crate) fn as_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }
    pub(crate) fn len(&self) -> usize {
        self.data.len()
    }
    pub(crate) fn clear(&mut self) {
        self.data.clear();
    }
    // pub(crate) fn slice_as_iter<T>(byte_width: usize, data: &[u8]) -> impl Iterator<Item=T> + '_ {
    //     data.chunks_exact(byte_width).map(move |chunk| {
    //         let mut data = vec![0; std::mem::size_of::<T>()];
    //         data[..chunk.len()].copy_from_slice(chunk);
    //         unsafe { std::ptr::read_unaligned(data.as_ptr() as *const T) }
    //     })
    // }
}

#[lamellar_impl::AmDataRT(Clone, AmGroup(false))]
pub(crate) struct PackedIdxVal {
    data: Vec<u8>,
    idx_byte_width: usize,
}

impl PackedIdxVal {
    // pub(crate) fn new(idx_byte_width: usize) -> PackedIdxVal {
    //     PackedIdxVal { data: Vec::new(), idx_byte_width }
    // }
    pub(crate) fn new_with_capacity<T>(
        idx_byte_width: usize,
        estimated_elems: usize,
    ) -> PackedIdxVal {
        let data = Vec::with_capacity(
            idx_byte_width * estimated_elems + std::mem::size_of::<T>() * estimated_elems,
        );
        PackedIdxVal {
            data,
            idx_byte_width,
        }
    }
    pub(crate) fn push<T>(&mut self, idx: usize, value: T) {
        let idx_byte_width = self.idx_byte_width;
        let idx_bytes = unsafe {
            std::slice::from_raw_parts((&idx as *const usize) as *const u8, idx_byte_width)
        };
        self.data.extend_from_slice(idx_bytes);
        let value_bytes = unsafe {
            std::slice::from_raw_parts((&value as *const T) as *const u8, std::mem::size_of::<T>())
        };
        self.data.extend_from_slice(value_bytes);
    }
    pub(crate) fn as_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }
    pub(crate) fn len(&self) -> usize {
        self.data.len()
    }
    pub(crate) fn clear(&mut self) {
        self.data.clear();
    }
    pub(crate) fn slice_as_iter<T>(
        idx_byte_width: usize,
        data: &[u8],
    ) -> impl Iterator<Item = (usize, T)> + '_ {
        data.chunks_exact(idx_byte_width + std::mem::size_of::<T>())
            .map(move |chunk| {
                let mut idx_data = vec![0; std::mem::size_of::<usize>()];
                let mut val_data = vec![0; std::mem::size_of::<T>()];
                idx_data[..idx_byte_width].copy_from_slice(&chunk[..idx_byte_width]);
                val_data.copy_from_slice(&chunk[idx_byte_width..]);
                let idx = unsafe { std::ptr::read_unaligned(idx_data.as_ptr() as *const usize) };
                let val = unsafe { std::ptr::read_unaligned(val_data.as_ptr() as *const T) };
                (idx, val)
            })
    }
}

#[lamellar_impl::AmDataRT(AmGroup(false))]
pub(crate) struct ScalarSingleIdxMultiValAm {
    pub(crate) array: LamellarByteArray,
    pub(crate) index: usize,
    pub(crate) vals: OneSidedMemoryRegion<u8>,
    pub(crate) scalar_type: ScalarType,
    pub(crate) opt: bool,
    pub(crate) op: ArrayOpCmd<Vec<u8>>,
}

impl ScalarSingleIdxMultiValAm {
    fn idx_vals<'a, T: Dist + Sized>(
        &self,
        bytes: &'a [u8],
    ) -> impl Iterator<Item = (usize, T)> + 'a {
        let vals = bytes.chunks_exact(std::mem::size_of::<T>()).map(|chunk| {
            let data = chunk.to_vec();
            unsafe { std::ptr::read_unaligned(data.as_ptr() as *const T) }
        });
        std::iter::repeat(self.index).zip(vals)
    }
    impl_ops_option_scalar_type_match!(&bytes, Vec<u8>, false);
    impl_ops_scalar_type_match!(&bytes, Vec<u8>, false);
}

#[lamellar_impl::rt_am]
impl LamellarAM for ScalarSingleIdxMultiValAm {
    async fn exec(&self) {
        let num_bytes = self.vals.len();
        let bytes = unsafe { self.vals.get_buffer(0, num_bytes).await };
        let array = self.array.clone();

        if self.opt {
            self.option_type(bytes, array).await;
        } else {
            self.bare_type(bytes, array).await;
        }
    }
}

#[lamellar_impl::AmDataRT(AmGroup(false))]
pub(crate) struct ScalarSingleIdxMultiValAmReturn {
    pub(crate) array: LamellarByteArray,
    pub(crate) index: usize,
    pub(crate) vals: OneSidedMemoryRegion<u8>,
    pub(crate) scalar_type: ScalarType,
    pub(crate) opt: bool,
    pub(crate) op: ArrayOpCmd<Vec<u8>>,
}

impl ScalarSingleIdxMultiValAmReturn {
    fn idx_vals<'a, T: Dist + Sized>(
        &self,
        bytes: &'a [u8],
    ) -> impl Iterator<Item = (usize, T)> + 'a {
        let vals = bytes.chunks_exact(std::mem::size_of::<T>()).map(|chunk| {
            let data = chunk.to_vec();
            unsafe { std::ptr::read_unaligned(data.as_ptr() as *const T) }
        });
        std::iter::repeat(self.index).zip(vals)
    }
    impl_vec_to_bytes!();
    impl_ops_option_scalar_type_match!(&bytes, Vec<u8>, true);
    impl_ops_scalar_type_match!(&bytes, Vec<u8>, true);
}

#[lamellar_impl::rt_am]
impl LamellarAM for ScalarSingleIdxMultiValAmReturn {
    async fn exec(&self) -> OneSidedMemoryRegion<u8> {
        let num_bytes = self.vals.len();
        let bytes = unsafe { self.vals.get_buffer(0, num_bytes).await };
        let array = self.array.clone();

        if self.opt {
            self.option_type(bytes, array).await
        } else {
            self.bare_type(bytes, array).await
        }
    }
}

#[lamellar_impl::AmDataRT(AmGroup(false))]
pub(crate) struct ScalarMultiIdxSingleValAm {
    pub(crate) array: LamellarByteArray,
    pub(crate) val: Vec<u8>,
    pub(crate) indices: OneSidedMemoryRegion<u8>,
    pub(crate) idx_size: usize,
    pub(crate) scalar_type: ScalarType,
    pub(crate) opt: bool,
    pub(crate) op: ArrayOpCmd<Vec<u8>>,
}

impl ScalarMultiIdxSingleValAm {
    fn idx_vals<'a, T: Dist + Sized>(
        &self,
        indices: impl Iterator<Item = usize> + 'a,
    ) -> impl Iterator<Item = (usize, T)> + 'a {
        let val = unsafe { std::ptr::read_unaligned(self.val.as_ptr() as *const T) };
        indices.zip(std::iter::repeat(val))
    }
    impl_ops_option_scalar_type_match!(bytes, impl Iterator<Item = usize>, false);
    impl_ops_scalar_type_match!(bytes, impl Iterator<Item = usize>, false);
}

#[lamellar_impl::rt_am]
impl LamellarAM for ScalarMultiIdxSingleValAm {
    async fn exec(&self) {
        let num_bytes = self.indices.len();
        let array = self.array.clone();

        let indices: Box<dyn Iterator<Item = usize> + Send + '_> = match self.idx_size {
            1 => Box::new(
                unsafe { self.indices.get_buffer(0, num_bytes).await }
                    .into_iter()
                    .map(|b| b as usize),
            ),
            2 => Box::new(
                unsafe {
                    self.indices
                        .clone()
                        .to_base::<u16>()
                        .get_buffer(0, num_bytes / 2)
                        .await
                }
                .into_iter()
                .map(|b| b as usize),
            ),
            4 => Box::new(
                unsafe {
                    self.indices
                        .clone()
                        .to_base::<u32>()
                        .get_buffer(0, num_bytes / 4)
                        .await
                }
                .into_iter()
                .map(|b| b as usize),
            ),
            _ => Box::new(
                unsafe {
                    self.indices
                        .clone()
                        .to_base::<u64>()
                        .get_buffer(0, num_bytes / 8)
                        .await
                }
                .into_iter()
                .map(|b| b as usize),
            ),
        };
        if self.opt {
            self.option_type(indices, array).await;
        } else {
            self.bare_type(indices, array).await;
        }
    }
}

#[lamellar_impl::AmDataRT(AmGroup(false))]
pub(crate) struct ScalarMultiIdxSingleValAmReturn {
    pub(crate) array: LamellarByteArray,
    pub(crate) val: Vec<u8>,
    pub(crate) indices: OneSidedMemoryRegion<u8>,
    pub(crate) idx_size: usize,
    pub(crate) scalar_type: ScalarType,
    pub(crate) opt: bool,
    pub(crate) op: ArrayOpCmd<Vec<u8>>,
}

impl ScalarMultiIdxSingleValAmReturn {
    fn idx_vals<'a, T: Dist + Sized>(
        &self,
        indices: impl Iterator<Item = usize> + 'a,
    ) -> impl Iterator<Item = (usize, T)> + 'a {
        let val = unsafe { std::ptr::read_unaligned(self.val.as_ptr() as *const T) };
        indices.zip(std::iter::repeat(val))
    }
    impl_vec_to_bytes!();
    impl_ops_option_scalar_type_match!(bytes, impl Iterator<Item = usize>, true);
    impl_ops_scalar_type_match!(bytes, impl Iterator<Item = usize>, true);
}

#[lamellar_impl::rt_am]
impl LamellarAM for ScalarMultiIdxSingleValAmReturn {
    async fn exec(&self) -> OneSidedMemoryRegion<u8> {
        let num_bytes = self.indices.len();
        let array = self.array.clone();

        let indices: Box<dyn Iterator<Item = usize> + Send + '_> = match self.idx_size {
            1 => Box::new(
                unsafe { self.indices.get_buffer(0, num_bytes).await }
                    .into_iter()
                    .map(|b| b as usize),
            ),
            2 => Box::new(
                unsafe {
                    self.indices
                        .clone()
                        .to_base::<u16>()
                        .get_buffer(0, num_bytes / 2)
                        .await
                }
                .into_iter()
                .map(|b| b as usize),
            ),
            4 => Box::new(
                unsafe {
                    self.indices
                        .clone()
                        .to_base::<u32>()
                        .get_buffer(0, num_bytes / 4)
                        .await
                }
                .into_iter()
                .map(|b| b as usize),
            ),
            _ => Box::new(
                unsafe {
                    self.indices
                        .clone()
                        .to_base::<u64>()
                        .get_buffer(0, num_bytes / 8)
                        .await
                }
                .into_iter()
                .map(|b| b as usize),
            ),
        };
        if self.opt {
            self.option_type(indices, array).await
        } else {
            self.bare_type(indices, array).await
        }
    }
}

#[lamellar_impl::AmDataRT(AmGroup(false))]
pub(crate) struct ScalarMultiIdxMultiValAm {
    pub(crate) array: LamellarByteArray,
    pub(crate) idx_val: OneSidedMemoryRegion<u8>,
    pub(crate) idx_size: usize,
    pub(crate) scalar_type: ScalarType,
    pub(crate) opt: bool,
    pub(crate) op: ArrayOpCmd<Vec<u8>>,
}

impl ScalarMultiIdxMultiValAm {
    fn idx_vals<'a, T: Dist + Sized>(
        &self,
        bytes: &'a [u8],
    ) -> impl Iterator<Item = (usize, T)> + 'a {
        PackedIdxVal::slice_as_iter::<T>(self.idx_size, bytes)
    }

    impl_ops_option_scalar_type_match!(&bytes, Vec<u8>, false);
    impl_ops_scalar_type_match!(&bytes, Vec<u8>, false);
}

#[lamellar_impl::rt_am]
impl LamellarAM for ScalarMultiIdxMultiValAm {
    async fn exec(&self) {
        let num_bytes = self.idx_val.len();
        let bytes = unsafe { self.idx_val.get_buffer(0, num_bytes).await };
        let array = self.array.clone();

        if self.opt {
            self.option_type(bytes, array).await;
        } else {
            self.bare_type(bytes, array).await;
        }
    }
}

#[lamellar_impl::AmDataRT(AmGroup(false))]
pub(crate) struct ScalarMultiIdxMultiValAmReturn {
    pub(crate) array: LamellarByteArray,
    pub(crate) idx_val: OneSidedMemoryRegion<u8>,
    pub(crate) idx_size: usize,
    pub(crate) scalar_type: ScalarType,
    pub(crate) opt: bool,
    pub(crate) op: ArrayOpCmd<Vec<u8>>,
}

impl ScalarMultiIdxMultiValAmReturn {
    fn idx_vals<'a, T: Dist + Sized>(
        &self,
        bytes: &'a [u8],
    ) -> impl Iterator<Item = (usize, T)> + 'a {
        PackedIdxVal::slice_as_iter::<T>(self.idx_size, bytes)
    }

    impl_vec_to_bytes!();
    impl_ops_option_scalar_type_match!(&bytes, Vec<u8>, true);
    impl_ops_scalar_type_match!(&bytes, Vec<u8>, true);
}

#[lamellar_impl::rt_am]
impl LamellarAM for ScalarMultiIdxMultiValAmReturn {
    async fn exec(&self) -> OneSidedMemoryRegion<u8> {
        let num_bytes = self.idx_val.len();
        let bytes = unsafe { self.idx_val.get_buffer(0, num_bytes).await };
        let array = self.array.clone();

        if self.opt {
            self.option_type(bytes, array).await
        } else {
            self.bare_type(bytes, array).await
        }
    }
}

#[lamellar_impl::AmDataRT(AmGroup(false))]
pub(crate) struct UsizeSingleValMultiIdxAm {
    pub(crate) array: UnsafeArray<usize>,
    pub(crate) indices: OneSidedMemoryRegion<u8>,
    pub(crate) idx_size: usize,
    pub(crate) val: usize,
}

#[lamellar_impl::rt_am]
impl LamellarAM for UsizeSingleValMultiIdxAm {
    async fn exec(&self) {
        let num_bytes = self.indices.len();
        let indices = unsafe {
            self.indices
                .clone()
                .to_base::<u32>()
                .get_buffer(0, num_bytes / 4)
                .await
        };
        // let bytes = unsafe { self.indices.get_buffer(0, num_bytes).await };
        // // let indices = PackedIndicies::slice_as_iter::<usize>(self.idx_size, bytes);
        // let indices= unsafe {
        //     std::slice::from_raw_parts(bytes.as_ptr() as *const u32, bytes.len() / 4)
        // };
        let local_data = unsafe { self.array.mut_local_data() };
        indices.iter().for_each(|i| {
            local_data[*i as usize] += self.val;
        });
    }
}
