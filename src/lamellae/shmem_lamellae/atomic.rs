impl CommAtomic for ShmemComm {
    fn atomic_avail<T>(&self) -> bool {
        false
    }
    fn atomic_op<T: NetworkAtomic>(
        &self,
        op: AtomicOp<T>,
        pe: usize,
        remote_addr: usize,
    ) -> RdmaFuture {
        async move { Ok(()) }
    }
    fn atomic_fetch_op<T: NetworkAtomic>(
        &self,
        op: AtomicOp<T>,
        pe: usize,
        remote_addr: usize,
        result: &mut [T],
    ) -> RdmaFuture {
        async move { Ok(()) }
    }
}
