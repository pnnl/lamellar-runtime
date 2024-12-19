pub(crate) struct LocalFuture {}

impl Future for LocalFuture {
    type Output = RdmaResult;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Ok(()))
    }
}

impl CommRdma for LocalComm {
    fn put<T: Remote>(&self, pe: usize, src: &[T], dst: usize) -> RdmaFuture {
        let src_ptr = src.as_ptr();
        let src_addr = src_ptr as usize;
        async move {
            if !((src_addr <= dst
            && dst < src_addr + src.len()) //dst start overlaps src
            || (src_addr <= dst + src.len()
            && dst + src.len() < src_addr + src.len()))
            //dst end overlaps src
            {
                unsafe {
                    std::ptr::copy_nonoverlapping(src_addr as *mut u8, dst as *mut u8, src.len());
                }
            } else {
                unsafe {
                    std::ptr::copy(src_addr as *mut u8, dst as *mut u8, src.len());
                }
            }
            Ok(())
        }
    }
    fn put_all<T: Remote>(&self, src: &[T], dst: usize) -> RdmaFuture {
        let src_ptr = src.as_ptr();
        if !((src_ptr as usize <= dst
            && dst < src_ptr as usize + src.len()) //dst start overlaps src
            || (src_ptr as usize <= dst + src.len()
            && dst + src.len() < src_ptr as usize + src.len()))
        //dst end overlaps src
        {
            async {
                unsafe {
                    std::ptr::copy_nonoverlapping(src.as_ptr(), dst as *mut u8, src.len());
                }
                Ok(())
            }
        } else {
            async {
                unsafe {
                    std::ptr::copy(src.as_ptr(), dst as *mut u8, src.len());
                }
                Ok(())
            }
        }
    }
    fn get<T: Remote>(&self, pe: usize, src: usize, dst: &mut [T]) -> RdmaFuture {
        let dst_ptr = dst.as_mut_ptr();
        if !((dst_ptr as usize <= src && src < dst_ptr as usize + dst.len())
            || (dst_ptr as usize <= src + dst.len()
                && src + dst.len() < dst_ptr as usize + dst.len()))
        {
            unsafe {
                std::ptr::copy_nonoverlapping(src as *mut u8, dst.as_mut_ptr(), dst.len());
            }
        } else {
            unsafe {
                std::ptr::copy(src as *mut u8, dst.as_mut_ptr(), dst.len());
            }
        }
    }
}
