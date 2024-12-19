impl CommMem for ShmemComm {
    fn alloc(&self, size: usize, alloc_type: AllocationType) -> AllocResult<usize> {
        //shared memory segments are aligned on page boundaries so no need to pass in alignment constraint
        let mut alloc = self.alloc_lock.write();
        let (ret, index, remote_addrs) = match alloc_type {
            AllocationType::Sub(pes) => {
                // println!("pes: {:?}",pes);
                if pes.contains(&self.my_pe) {
                    let ret = unsafe { alloc.1.alloc(size, pes.iter().cloned()) };
                    // println!("{:?}",ret.2);
                    ret
                } else {
                    return Err(AllocError::IdError(self.my_pe));
                }
            }
            AllocationType::Global => unsafe { alloc.1.alloc(size, 0..self.num_pes) },
            _ => panic!("unexpected allocation type {:?} in rofi_alloc", alloc_type),
        };
        let mut addr_map = HashMap::new();
        let mut relative_index = 0;
        for pe in 0..self.num_pes {
            if remote_addrs[pe] > 0 {
                // let local_addr = ret.as_ptr() as usize + size*relative_index;
                addr_map.insert(pe, (remote_addrs[pe], relative_index));
                relative_index += 1;
            }
        }
        let addr = ret.as_ptr() as usize + size * index;
        alloc.0.insert(addr, (ret, size, addr_map));
        Ok(addr)
    }

    fn free(&self, addr: usize) {
        //maybe need to do something more intelligent on the drop of the shmem_alloc
        let mut alloc = self.alloc_lock.write();
        alloc.0.remove(&addr);
    }

    fn rt_alloc(&self, size: usize, align: usize) -> AllocResult<usize> {
        let allocs = self.alloc.read();
        for alloc in allocs.iter() {
            if let Some(addr) = alloc.try_malloc(size, align) {
                return Ok(addr);
            }
        }
        Err(AllocError::OutOfMemoryError(size))
    }

    fn rt_check_alloc(&self, size: usize, align: usize) -> bool {
        let allocs = self.alloc.read();
        for alloc in allocs.iter() {
            if alloc.fake_malloc(size, align) {
                return true;
            }
        }
        false
    }

    fn rt_free(&self, addr: usize) {
        let allocs = self.alloc.read();
        for alloc in allocs.iter() {
            if let Ok(_) = alloc.free(addr) {
                return;
            }
        }
        panic!("Error invalid free! {:?}", addr);
    }

    fn occupied(&self) -> usize {
        let mut occupied = 0;
        let allocs = self.alloc.read();
        for alloc in allocs.iter() {
            occupied += alloc.occupied();
        }
        occupied
    }

    fn alloc_pool(&self, min_size: usize) {
        let mut allocs = self.alloc.write();
        let size = std::cmp::max(
            min_size * 2 * self.num_pes,
            SHMEM_SIZE.load(Ordering::SeqCst),
        ) / self.num_pes;
        if let Ok(addr) = self.alloc(size, AllocationType::Global) {
            // println!("addr: {:x} - {:x}",addr, addr+size);
            let mut new_alloc = BTreeAlloc::new("shmem".to_string());
            new_alloc.init(addr, size);
            allocs.push(new_alloc)
        } else {
            panic!("[Error] out of system memory");
        }
    }
    fn num_pool_allocs(&self) -> usize {
        self.alloc.read().len()
    }

    fn print_pools(&self) {
        let allocs = self.alloc.read();
        println!("num_pools {:?}", allocs.len());
        for alloc in allocs.iter() {
            println!(
                "{:x} {:?} {:?} {:?}",
                alloc.start_addr,
                alloc.max_size,
                alloc.occupied(),
                alloc.space_avail()
            );
        }
    }

    fn base_addr(&self) -> usize {
        *self.base_address.read()
    }
    fn local_addr(&self, remote_pe: usize, remote_addr: usize) -> usize {
        let alloc = self.alloc_lock.read();
        for (addr, (shmem, size, addrs)) in alloc.0.iter() {
            if let Some(data) = addrs.get(&remote_pe) {
                if data.0 <= remote_addr && remote_addr < data.0 + shmem.len() {
                    let remote_offset = remote_addr - (data.0 + size * data.1);
                    return addr + remote_offset;
                }
            }
        }
        panic!("not sure i should be here...means address not found");
    }
    fn remote_addr(&self, pe: usize, local_addr: usize) -> usize {
        let alloc = self.alloc_lock.read();
        for (addr, (shmem, size, addrs)) in alloc.0.iter() {
            if shmem.contains(local_addr) {
                let local_offset = local_addr - addr;
                return addrs[&pe].0 + size * addrs[&pe].1 + local_offset;
            }
        }
        panic!("not sure i should be here...means address not found");
    }
}
