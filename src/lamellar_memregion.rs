use crate::lamellae::Lamellae;
use crate::runtime::LAMELLAR_RT;
use core::marker::PhantomData;

use serde::de::{self};
use serde::ser::Serializer;

#[derive(serde::Serialize, serde::Deserialize, Clone, Copy)]
pub struct LamellarMemoryRegion<
    T: serde::ser::Serialize + serde::de::DeserializeOwned + std::clone::Clone + Send + Sync + 'static,
> {
    #[serde(
        serialize_with = "serialize_addr",
        deserialize_with = "deserialize_addr"
    )]
    addr: usize,
    size: usize,

    phantom: PhantomData<T>,
}

impl<
        T: serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + Send
            + Sync
            + 'static,
            
    > LamellarMemoryRegion<T>
{
    pub fn new(size: usize) -> LamellarMemoryRegion<T> {
        LamellarMemoryRegion {
            addr: LAMELLAR_RT
                .lamellae
                .alloc(size * std::mem::size_of::<T>())
                .unwrap(), //TODO: out of memory...
            size: size,
            phantom: PhantomData,
        }
    }

    pub fn delete(&self) {
        LAMELLAR_RT.lamellae.free(self.addr);
    }
    //currently unsafe because user must ensure data exists utill put is finished
    pub unsafe fn put(&self, pe: usize, index: usize, data: &[T]) {
        if index + data.len() <= self.size {
            LAMELLAR_RT.lamellae.put(pe, data, self.addr + index*std::mem::size_of::<T>());
        } else {
            println!(
                "{:?} {:?} {:?}",
                self.size,
                index,
                std::mem::size_of_val(data)
            );
            panic!("index out of bounds");
        }
    }
    //currently unsafe because user must ensure data exists utill put is finished
    pub unsafe fn put_all(&self, index: usize, data: &[T]) {
        if index + data.len() <= self.size {
            LAMELLAR_RT.lamellae.put_all(data, self.addr + index*std::mem::size_of::<T>());
        } else {
            panic!("index out of bounds");
        }
    }

    //TODO: once we have a reliable asynchronos get wait mechanism, we return a request handle,
    //data probably needs to be referenced count or lifespan controlled so we know it exists when the get trys to complete
    //in the handle drop method we will wait until the request completes before dropping...  ensuring the data has a place to go
    /// copy data from remote memory location into provided data buffer
    ///
    /// # Arguments
    ///
    /// * `pe` - id of remote PE to grab data from
    /// * `index` - offset into the remote memory window
    /// * `dst` - address of destination buffer
    ///
    pub unsafe fn get(&self, pe: usize, index: usize, data: &mut [T]) {
        if index + data.len() <= self.size {
            LAMELLAR_RT.lamellae.get(pe, self.addr + index*std::mem::size_of::<T>(), data); //(remote pe, src, dst)
        } else {
            println!(
                "{:?} {:?} {:?} {:?}",
                self.size,
                index,
                data.len(),
                std::mem::size_of_val(data)
            );
            panic!("index out of bounds");
        }
    }

    // pub fn as_bytes(&self) -> &[u8] {
    //     let pointer = self as *const T as *const u8;
    //     let size = std::mem::size_of::<T>();
    //     let slice: &[u8] = unsafe { std::slice::from_raw_parts(pointer, size) };
    //     slice
    // }
    pub fn as_slice(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.addr as *const T, self.size) }
    }

    pub unsafe fn as_mut_slice(&self) -> &mut [T] {
        std::slice::from_raw_parts_mut(self.addr as *mut T, self.size)
    }
}



fn serialize_addr<S>(addr: &usize, serialzer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serialzer.serialize_u64(*addr as u64 - LAMELLAR_RT.lamellae.base_addr() as u64)
}

fn deserialize_addr<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: de::Deserializer<'de>,
{
    struct AddrVisitor;
    impl<'de> de::Visitor<'de> for AddrVisitor {
        type Value = usize;
        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a usize representing relative offset")
        }

        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(LAMELLAR_RT.lamellae.base_addr() + value as usize)
        }
    }
    deserializer.deserialize_u64(AddrVisitor)
}

impl<
        T: std::fmt::Debug
            + serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + Send
            + Sync
            + 'static,
    > std::fmt::Debug for LamellarMemoryRegion<T>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let slice = unsafe { std::slice::from_raw_parts(self.addr as *const T, self.size) };
        write!(f, "{:?}", slice)
    }
}
