use crate::runtime::Arch;
#[cfg(feature = "RofiBackend")]
pub(crate) mod rofi_lamellae;
#[cfg(feature = "SocketsBackend")]
pub(crate) mod sockets_lamellae;

pub(crate) trait Lamellae {
    // fn new() -> Self;
    fn init(&mut self) -> Arch;
    fn finit(&self);
    fn barrier(&self);
    fn send_to_pe(&self, pe: usize, data: std::vec::Vec<u8>);
    fn send_to_all(&self, data: std::vec::Vec<u8>);
    fn put<
        T: serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + Send
            + Sync
            + 'static,
    >(
        &self,
        pe: usize,
        src: &[T],
        dst: usize,
    );
    fn put_all<
        T: serde::ser::Serialize
            + serde::de::DeserializeOwned
            + std::clone::Clone
            + Send
            + Sync
            + 'static,
    >(
        &self,
        src: &[T],
        dst: usize,
    );
    /// copy data from remote memory location into provided data buffer
    ///
    /// # Arguments
    ///
    /// * `pe` - id of remote PE to grab data from
    /// * `src` - address of source (remote) buffer
    /// * `dst` - address of destination buffer
    ///
    fn get<T: serde::ser::Serialize + serde::de::DeserializeOwned + std::clone::Clone + 'static>(
        &self,
        pe: usize,
        src: usize,
        dst: &mut [T],
    );
    fn alloc(&self, size: usize) -> Option<usize>;
    fn free(&self, addr: usize);
    fn base_addr(&self) -> usize;
    #[allow(non_snake_case)]
    fn MB_sent(&self) -> f64;
    fn print_stats(&self);
}
