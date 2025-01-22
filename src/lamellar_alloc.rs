use crate::env_var::config;

use core::marker::PhantomData;
use indexmap::IndexSet;
// use log::trace;
use parking_lot::{Condvar, Mutex};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub(crate) trait LamellarAlloc {
    fn new(id: String) -> Self;
    fn init(&mut self, start_addr: usize, size: usize); //size in bytes
    #[allow(dead_code)]
    fn malloc(&self, size: usize, align: usize) -> usize;
    fn try_malloc(&self, size: usize, align: usize) -> Option<usize>;
    fn fake_malloc(&self, size: usize, align: usize) -> bool;
    fn free(&self, addr: usize) -> Result<(), usize>;
    fn find(&self, addr: usize) -> Option<usize>;
    fn space_avail(&self) -> usize;
    fn occupied(&self) -> usize;
}

#[derive(Debug)]
#[allow(dead_code)]
struct Vma {
    addr: usize,
    padding: usize,
    size: usize,
}

#[derive(Clone)]
#[allow(dead_code)]
pub(crate) struct LinearAlloc {
    entries: Arc<(Mutex<Vec<Vma>>, Condvar)>,
    start_addr: usize,
    max_size: usize,
    last_idx: Arc<AtomicUsize>,
    _id: String,
    free_space: Arc<AtomicUsize>,
}

fn calc_padding(addr: usize, align: usize) -> usize {
    let rem = addr % align;
    if rem == 0 {
        0
    } else {
        align - rem
    }
}

impl LamellarAlloc for LinearAlloc {
    fn new(id: String) -> LinearAlloc {
        // trace!("new linear alloc: {:?}", id);
        LinearAlloc {
            entries: Arc::new((Mutex::new(Vec::new()), Condvar::new())),
            start_addr: 0,
            max_size: 0,
            last_idx: Arc::new(AtomicUsize::new(0)),
            _id: id,
            free_space: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn init(&mut self, start_addr: usize, size: usize) {
        // trace!("init: {:?} {:x} {:?}", self.id, start_addr, size);
        self.start_addr = start_addr;
        self.max_size = size;
        self.free_space.store(size, Ordering::SeqCst);
    }

    fn malloc(&self, size: usize, align: usize) -> usize {
        let mut val = self.try_malloc(size, align);
        while let None = val {
            val = self.try_malloc(size, align);
        }
        val.unwrap()
    }

    fn try_malloc(&self, size: usize, align: usize) -> Option<usize> {
        let &(ref lock, ref cvar) = &*self.entries;
        let mut entries = lock.lock();

        if entries.len() > 0 {
            // let padding = align - (self.start_addr % align);
            // let mut prev_end = self.start_addr + padding;
            let mut prev_end = self.start_addr;
            let mut padding = calc_padding(prev_end, align);
            let mut idx = 0;
            for i in 0..entries.len() {
                if entries[i].addr - prev_end >= size + padding {
                    break;
                }
                prev_end = entries[i].addr + entries[i].size + entries[i].padding;
                padding = calc_padding(prev_end, align);
                idx = i + 1;
            }

            if prev_end + size + padding <= self.start_addr + self.max_size {
                let n_vma = Vma {
                    addr: prev_end,
                    padding: padding,
                    size: size,
                };
                entries.insert(idx, n_vma);
                self.free_space.fetch_sub(size + padding, Ordering::SeqCst);
                return Some(prev_end + padding);
            } else {
                cvar.wait_for(&mut entries, std::time::Duration::from_millis(1));
                return None;
            }
        } else {
            let padding = calc_padding(self.start_addr, align);
            if size + padding <= self.start_addr + self.max_size {
                let n_vma = Vma {
                    addr: self.start_addr,
                    padding: padding,
                    size: size,
                };
                entries.push(n_vma);
                self.free_space.fetch_sub(size + padding, Ordering::SeqCst);
                return Some(self.start_addr + padding);
            } else {
                cvar.wait_for(&mut entries, std::time::Duration::from_millis(1));
                return None;
            }
        }
    }

    fn fake_malloc(&self, size: usize, align: usize) -> bool {
        let &(ref lock, ref _cvar) = &*self.entries;
        let entries = lock.lock();

        if entries.len() > 0 {
            let mut prev_end = self.start_addr;
            let mut padding = calc_padding(prev_end, align);
            for i in 0..entries.len() {
                if entries[i].addr - prev_end >= size + padding {
                    break;
                }
                prev_end = entries[i].addr + entries[i].size + entries[i].padding;
                padding = calc_padding(prev_end, align);
            }

            if prev_end + size + padding <= self.start_addr + self.max_size {
                return true;
            } else {
                return false;
            }
        } else {
            let padding = calc_padding(self.start_addr, align);
            if size + padding <= self.start_addr + self.max_size {
                return true;
            } else {
                return false;
            }
        }
    }

    fn free(&self, addr: usize) -> Result<(), usize> {
        let &(ref lock, ref cvar) = &*self.entries;
        let mut entries = lock.lock();
        for i in 0..entries.len() {
            if addr - entries[i].padding as usize == entries[i].addr as usize {
                self.free_space
                    .fetch_add(entries[i].size + entries[i].padding, Ordering::SeqCst);
                entries.remove(i);
                let last_idx = self.last_idx.load(Ordering::SeqCst);
                if last_idx >= entries.len() && last_idx != 0 {
                    self.last_idx.fetch_sub(1, Ordering::SeqCst);
                }
                cvar.notify_all();

                return Ok(());
            }
        }
        Err(addr)
    }

    fn find(&self, addr: usize) -> Option<usize> {
        let &(ref lock, ref _cvar) = &*self.entries;
        let entries = lock.lock();
        for i in 0..entries.len() {
            if addr - entries[i].padding as usize == entries[i].addr as usize {
                return Some(entries[i].size);
            }
        }
        None
    }
    fn space_avail(&self) -> usize {
        self.free_space.load(Ordering::SeqCst)
    }
    fn occupied(&self) -> usize {
        self.max_size - self.free_space.load(Ordering::SeqCst)
    }
}

#[derive(Clone, Debug)]
struct FreeEntries {
    sizes: BTreeMap<usize, IndexSet<usize>>, //<size,<Vec<addr>>
    addrs: BTreeMap<usize, (usize, usize)>,  //<addr,(size,padding)>
}

impl FreeEntries {
    fn new() -> FreeEntries {
        FreeEntries {
            sizes: BTreeMap::new(),
            addrs: BTreeMap::new(),
        }
    }

    fn merge(&mut self) {
        let mut i = 0;
        while i < self.addrs.len() - 1 {
            let (faddr, (fsize, fpadding)) = self.addrs.pop_first().unwrap();
            let (naddr, (nsize, npadding)) = self.addrs.pop_first().unwrap();
            if faddr + fsize + fpadding == naddr {
                //merge
                let new_size = fsize + nsize;
                let new_padding = fpadding + npadding;
                assert!(new_padding == 0);
                let new_addr = faddr;
                self.remove_size(naddr, nsize);
                self.remove_size(faddr, fsize);
                self.addrs.insert(new_addr, (new_size, new_padding));
                self.sizes
                    .entry(new_size)
                    .or_insert(IndexSet::new())
                    .insert(new_addr);
            } else {
                self.addrs.insert(faddr, (fsize, fpadding));
                self.addrs.insert(naddr, (nsize, npadding));
                i += 1;
            }
        }
    }

    fn remove_size(&mut self, addr: usize, size: usize) {
        let mut remove_size = false;
        if let Some(addrs) = self.sizes.get_mut(&size) {
            addrs.remove(&addr);
            if addrs.is_empty() {
                remove_size = true;
            }
        }
        if remove_size {
            self.sizes.remove(&size);
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct BTreeAlloc {
    free_entries: Arc<(Mutex<FreeEntries>, Condvar)>,
    allocated_addrs: Arc<(Mutex<BTreeMap<usize, (usize, usize)>>, Condvar)>, //<addr,(size,padding)>
    pub(crate) start_addr: usize,
    pub(crate) max_size: usize,
    id: String,
    free_space: Arc<AtomicUsize>,
}

impl BTreeAlloc {}

impl LamellarAlloc for BTreeAlloc {
    fn new(id: String) -> BTreeAlloc {
        // trace!("new BTreeAlloc: {:?}", id);
        BTreeAlloc {
            free_entries: Arc::new((Mutex::new(FreeEntries::new()), Condvar::new())),
            allocated_addrs: Arc::new((Mutex::new(BTreeMap::new()), Condvar::new())),
            start_addr: 0,
            max_size: 0,
            id: id,
            free_space: Arc::new(AtomicUsize::new(0)),
        }
    }
    fn init(&mut self, start_addr: usize, size: usize) {
        // println!("init: {:?} {:x} {:?}", self.id, start_addr, size);
        self.start_addr = start_addr;
        self.max_size = size;
        let &(ref lock, ref _cvar) = &*self.free_entries;
        let mut free_entries = lock.lock();
        let mut temp = IndexSet::new();
        temp.insert(start_addr);
        free_entries.sizes.insert(size, temp);
        free_entries.addrs.insert(start_addr, (size, 0));
        self.free_space.store(size, Ordering::SeqCst);
    }

    fn malloc(&self, size: usize, align: usize) -> usize {
        let mut val = self.try_malloc(size, align);
        let mut timer = std::time::Instant::now();
        while let None = val {
            val = self.try_malloc(size, align);
            if timer.elapsed().as_secs_f64() > config().deadlock_timeout {
                println!("[WARNING]  Potential deadlock detected when trying to allocate more memory.\n\
                The deadlock timeout can be set via the LAMELLAR_DEADLOCK_WARNING_TIMEOUT environment variable, the current timeout is {} seconds\n\
                To view backtrace set RUST_LIB_BACKTRACE=1\n\
                {}",config().deadlock_timeout,std::backtrace::Backtrace::capture());
                timer = std::time::Instant::now();
            }
        }
        val.unwrap()
    }

    fn try_malloc(&self, size: usize, align: usize) -> Option<usize> {
        let &(ref lock, ref cvar) = &*self.free_entries;
        let mut free_entries = lock.lock();
        // println!("before: {:?}", free_entries);
        let mut addr: Option<usize> = None;
        let mut remove_size: Option<usize> = None;
        let upper_size = size + align - 1; //max size we would need

        let mut try_again = true;

        while try_again {
            //find smallest memory segment greater than or equal to size
            if let Some((free_size, addrs)) = free_entries.sizes.range_mut(upper_size..).next() {
                //the actual size, and list of addrs with that size
                addr = addrs.pop(); // get the first addr

                //if no more address exist with this size, mark it as removable from the free_entries size map
                if addrs.is_empty() {
                    remove_size = Some(free_size.clone());
                }

                if let Some(a) = addr {
                    let padding = calc_padding(a, align);
                    let full_size = size + padding;

                    if let Some((fsize, fpadding)) = free_entries.addrs.remove(&a) {
                        // remove the address from free_entries address map
                        // if full_size > fsize {
                        //     println!("{full_size} {fsize} {size} {align} {padding}  {fpadding}");
                        // }
                        if fsize + fpadding != full_size {
                            let remaining = (fsize + fpadding) - full_size;
                            let new_addr = a + full_size;
                            free_entries
                                .sizes
                                .entry(remaining)
                                .or_insert(IndexSet::new()) //insert new entry of remaining size if not already present
                                .insert(new_addr); //insert address into size map
                            free_entries.addrs.insert(new_addr, (remaining, 0));
                        }
                    } else {
                        // println!("{full_size}  {size} {align} {padding}");
                        // println!("{:?}", free_entries.sizes);
                        // println!("{:?}", free_entries.addrs);
                        panic!("{:?} addr {:?} not found in free_entries", self.id, a);
                    }
                }
                try_again = false;
            } else {
                cvar.wait_for(&mut free_entries, std::time::Duration::from_millis(1));
                free_entries.merge();
                if free_entries.sizes.range_mut(upper_size..).next().is_none() {
                    try_again = false;
                }
            }
        }

        if let Some(rsize) = remove_size {
            free_entries.sizes.remove(&rsize);
        }
        // println!("after: free_entries: {:?}", free_entries);
        drop(free_entries);
        addr = if let Some(a) = addr {
            let padding = calc_padding(a, align);
            let full_size = size + padding;
            let &(ref lock, ref _cvar) = &*self.allocated_addrs;
            let mut allocated_addrs = lock.lock();
            allocated_addrs.insert(a + padding, (size, padding));
            // println!("allocated_addrs: {:?}", allocated_addrs);
            self.free_space.fetch_sub(full_size, Ordering::SeqCst);
            // println!(
            //     "alloc addr {:?} = {a:?} + padding {padding} (size: {size}, align: {align}) {:?}",
            //     a + padding,
            //     self.free_space.load(Ordering::SeqCst)
            // );
            let new_addr = a + padding;
            // let rem = new_addr % align;
            // let rem_16 = new_addr % 16;
            // println!(
            //     "alloc addr {:x?} {:x?} {new_addr} {a} {padding} {rem} {align} {rem_16}",
            //     a + padding,
            //     new_addr,
            // );
            Some(new_addr)
        } else {
            None
        };

        addr
    }

    fn fake_malloc(&self, size: usize, align: usize) -> bool {
        let &(ref lock, ref _cvar) = &*self.free_entries;
        let mut free_entries = lock.lock();
        let upper_size = size + align - 1; //max size we would need
                                           //find smallest memory segment greater than or equal to size
        if let Some((_, _)) = free_entries.sizes.range_mut(upper_size..).next() {
            return true;
        } else {
            free_entries.merge();
            if let Some((_, _)) = free_entries.sizes.range_mut(upper_size..).next() {
                return true;
            }
            return false;
        }
    }

    fn free(&self, addr: usize) -> Result<(), usize> {
        let &(ref lock, ref _cvar) = &*self.allocated_addrs;
        let mut allocated_addrs = lock.lock();
        // println!("trying to free: {:x?} {:?}", addr, addr);
        if let Some((size, padding)) = allocated_addrs.remove(&addr) {
            // println!("allocated_addrs: {:?}", allocated_addrs);
            let full_size = size + padding;
            self.free_space.fetch_add(full_size, Ordering::SeqCst);
            drop(allocated_addrs);
            let unpadded_addr = addr - padding;
            let full_size = size + padding;
            let mut temp_addr = unpadded_addr;
            let mut temp_size = full_size;
            let mut remove = Vec::new();
            let &(ref lock, ref cvar) = &*self.free_entries;
            let mut free_entries = lock.lock();
            if let Some((faddr, (fsize, fpadding))) =
                free_entries.addrs.range(..temp_addr).next_back()
            {
                //look at address before addr
                if faddr + fsize + fpadding == addr {
                    //they are next to eachother...
                    temp_addr = *faddr;
                    temp_size += fsize + fpadding;
                    remove.push((*faddr, *fsize, *fpadding));
                }
                // else {
                //     break;
                // }
            }

            if let Some((faddr, (fsize, fpadding))) = free_entries.addrs.range(addr..).next() {
                //look at address after addr
                if temp_addr + temp_size == *faddr {
                    //they are next to eachother...
                    temp_size += fsize + fpadding;
                    remove.push((*faddr, *fsize, *fpadding));
                }
                // else {
                //     break;
                // }
            }
            for (raddr, rsize, rpadding) in remove {
                let rfull_size = rsize + rpadding;
                free_entries.addrs.remove(&raddr);
                free_entries.remove_size(raddr, rfull_size);
            }
            free_entries.addrs.insert(temp_addr, (temp_size, 0));
            free_entries
                .sizes
                .entry(temp_size)
                .or_insert(IndexSet::new())
                .insert(temp_addr);

            // free_entries.addrs.insert(unpadded_addr, (full_size, 0));
            // free_entries
            //     .sizes
            //     .entry(full_size)
            //     .or_insert(IndexSet::new())
            //     .insert(unpadded_addr);

            // println!("before: {:?}", free_entries);

            // let mut i = 0;
            // while i < free_entries.addrs.len() - 1 {
            //     let (faddr, (fsize, fpadding)) = free_entries.addrs.pop_first().unwrap();
            //     let (naddr, (nsize, npadding)) = free_entries.addrs.pop_first().unwrap();
            //     if faddr + fsize + fpadding == naddr {
            //         //merge
            //         let new_size = fsize + nsize;
            //         let new_padding = fpadding + npadding;
            //         assert!(new_padding == 0);
            //         let new_addr = faddr;
            //         self.remove_size(&mut free_entries, naddr, nsize);
            //         self.remove_size(&mut free_entries, faddr, fsize);
            //         free_entries.addrs.insert(new_addr, (new_size, new_padding));
            //         free_entries
            //             .sizes
            //             .entry(new_size)
            //             .or_insert(IndexSet::new())
            //             .insert(new_addr);
            //     } else {
            //         free_entries.addrs.insert(faddr, (fsize, fpadding));
            //         free_entries.addrs.insert(naddr, (nsize, npadding));
            //         i += 1;
            //     }
            // }

            // println!("after: free_entries: {:?}", free_entries);
            cvar.notify_all();
            Ok(())
        } else {
            // println!(
            //     "{:?} illegal free, addr not currently allocated: {:?}",
            //     self.id, addr
            // );
            Err(addr)
        }
    }

    fn find(&self, addr: usize) -> Option<usize> {
        let &(ref lock, ref _cvar) = &*self.allocated_addrs;
        let allocated_addrs = lock.lock();
        // println!("trying to free: {:x?} {:?}", addr, addr);
        if let Some((size, _padding)) = allocated_addrs.get(&addr) {
            return Some(*size);
        }
        None
    }
    fn space_avail(&self) -> usize {
        self.free_space.load(Ordering::SeqCst)
    }
    fn occupied(&self) -> usize {
        self.max_size - self.free_space.load(Ordering::SeqCst)
    }
}

#[derive(Clone)]
#[allow(dead_code)]
pub(crate) struct ObjAlloc<T: Copy> {
    free_entries: Arc<(Mutex<Vec<usize>>, Condvar)>,
    start_addr: usize,
    max_size: usize,
    num_entries: usize,
    _id: String,
    phantom: PhantomData<T>,
}

impl<T: Copy> LamellarAlloc for ObjAlloc<T> {
    fn new(id: String) -> ObjAlloc<T> {
        // trace!("new ObjAlloc: {:?}", id);
        ObjAlloc {
            free_entries: Arc::new((Mutex::new(Vec::new()), Condvar::new())),
            start_addr: 0,
            max_size: 0,
            num_entries: 0,
            _id: id,
            phantom: PhantomData,
        }
    }
    fn init(&mut self, start_addr: usize, size: usize) {
        // trace!("init: {:?} {:x} {:?}", self.id, start_addr, size);
        let align = std::mem::align_of::<T>();
        let padding = calc_padding(start_addr, align);
        self.start_addr = start_addr + padding;
        self.max_size = size;
        let &(ref lock, ref _cvar) = &*self.free_entries;
        let mut free_entries = lock.lock();
        *free_entries = ((start_addr + padding)..(start_addr + size))
            .step_by(std::mem::size_of::<T>())
            .collect();
        self.num_entries = free_entries.len();
    }

    fn malloc(&self, size: usize, align: usize) -> usize {
        let mut val = self.try_malloc(size, align);
        while let None = val {
            val = self.try_malloc(size, align);
        }
        val.unwrap()
    }

    fn try_malloc(&self, size: usize, _align: usize) -> Option<usize> {
        //dont need to worry about align here as we handle padding in init
        assert_eq!(
            size, 1,
            "ObjAlloc does not currently support multiobject allocations"
        );
        let &(ref lock, ref cvar) = &*self.free_entries;
        let mut free_entries = lock.lock();
        if let Some(addr) = free_entries.pop() {
            return Some(addr);
        } else {
            cvar.wait_for(&mut free_entries, std::time::Duration::from_millis(1));
            return None;
        }
    }

    fn fake_malloc(&self, size: usize, _align: usize) -> bool {
        //dont need to worry about align here as we handle padding in init
        assert_eq!(
            size, 1,
            "ObjAlloc does not currently support multiobject allocations"
        );
        let &(ref lock, ref _cvar) = &*self.free_entries;
        let free_entries = lock.lock();
        if free_entries.len() > 1 {
            true
        } else {
            false
        }
    }

    fn free(&self, addr: usize) -> Result<(), usize> {
        let &(ref lock, ref cvar) = &*self.free_entries;
        let mut free_entries = lock.lock();
        free_entries.push(addr);
        cvar.notify_all();
        Ok(())
    }

    fn find(&self, addr: usize) -> Option<usize> {
        assert!(addr < self.num_entries);
        let &(ref lock, ref _cvar) = &*self.free_entries;
        let free_entries = lock.lock();
        match free_entries.iter().position(|x| *x == addr) {
            Some(_i) => None, //means its available to be allocated
            None => Some(1),
        }
    }

    fn space_avail(&self) -> usize {
        let &(ref lock, ref _cvar) = &*self.free_entries;
        let free_entries = lock.lock();
        free_entries.len()
    }
    fn occupied(&self) -> usize {
        self.max_size - self.space_avail()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::seq::SliceRandom;
    use rand::{rngs::StdRng, Rng, SeedableRng};

    fn test_malloc(alloc: &mut impl LamellarAlloc) {
        alloc.init(0, 100);
        let mut rng = StdRng::seed_from_u64(0 as u64);
        let mut shuffled: Vec<usize> = (1..11).collect();
        shuffled.shuffle(&mut rng);

        let mut cnt = 0;
        for i in shuffled {
            assert_eq!(alloc.malloc(i, 1), cnt);
            cnt += i;
        }
        for i in (cnt..100).step_by(5) {
            assert_eq!(alloc.malloc(5, 1), i);
        }
    }

    fn stress<T: LamellarAlloc + Clone + Send + 'static>(alloc: T) {
        let mut threads = Vec::new();
        let start = std::time::Instant::now();
        for _i in 0..10 {
            let alloc_clone = alloc.clone();
            let t = std::thread::spawn(move || {
                let mut rng = rand::thread_rng();
                let mut addrs: Vec<usize> = Vec::new();
                let mut i = 0;
                while i < 100000 {
                    if rng.gen_range(0..2) == 0 || addrs.len() == 0 {
                        if let Some(addr) = alloc_clone.try_malloc(1, 1) {
                            addrs.push(addr);
                            i += 1;
                        }
                    } else {
                        let index = rng.gen_range(0..addrs.len());
                        let addr = addrs.remove(index);
                        alloc_clone
                            .free(addr)
                            .expect("Address should have been found and freed");
                    }
                }
                for addr in addrs {
                    alloc_clone
                        .free(addr)
                        .expect("Address should have been found and freed");
                }
            });
            threads.push(t);
        }
        for t in threads {
            t.join().unwrap();
        }
        let time = start.elapsed().as_secs_f64();

        println!("time: {:?}", time);
    }

    #[test]
    fn test_linear_malloc() {
        let mut alloc = LinearAlloc::new("linear_malloc".to_string());
        test_malloc(&mut alloc);
    }

    #[test]
    fn test_linear_stress() {
        let mut alloc = LinearAlloc::new("linear_stress".to_string());
        alloc.init(0, 100000);
        stress(alloc.clone());
        let &(ref lock, ref _cvar) = &*alloc.entries;
        let entries = lock.lock();
        assert_eq!(entries.len(), 0);
    }

    #[test]
    fn test_bttreealloc_malloc() {
        let mut alloc = BTreeAlloc::new("bttree_malloc".to_string());
        test_malloc(&mut alloc);
    }

    #[test]
    fn test_bttreealloc_stress() {
        let mut alloc = BTreeAlloc::new("bttree_stress".to_string());
        alloc.init(0, 100000);
        stress(alloc.clone());
        let &(ref lock, ref _cvar) = &*alloc.free_entries;
        let free_entries = lock.lock();
        assert_eq!(free_entries.sizes.len(), 1);
        assert_eq!(free_entries.addrs.len(), 1);
        // assert_eq!(free_entries.addrs.get(&0), Some(&100000usize));
        let &(ref lock, ref _cvar) = &*alloc.allocated_addrs;
        let allocated_addrs = lock.lock();
        assert_eq!(allocated_addrs.len(), 0);
    }

    #[test]
    fn test_obj_malloc() {
        let mut alloc = ObjAlloc::<u8>::new("obj_malloc_u8".to_string());
        alloc.init(0, 10);
        for i in 0..10 {
            assert_eq!(alloc.malloc(1, 1), 9 - i); //object allocator returns last address first...
        }

        let mut alloc = ObjAlloc::<u16>::new("obj_malloc_u16".to_string());
        alloc.init(0, 10); //only 5 slots
        for i in 0..5 {
            assert_eq!(alloc.malloc(1, 1), 8 - (i * std::mem::size_of::<u16>()));
        }
        assert_eq!(alloc.try_malloc(1, 1), None);
    }

    #[test]
    fn test_obj_u8_stress() {
        let mut alloc = ObjAlloc::<u8>::new("obj_malloc_u8".to_string());
        alloc.init(0, 100000);
        stress(alloc.clone());
        let &(ref lock, ref _cvar) = &*alloc.free_entries;
        let free_entries = lock.lock();
        assert_eq!(free_entries.len(), 100000 / std::mem::size_of::<u8>());
    }
    #[test]
    fn test_obj_f64_stress() {
        let mut alloc = ObjAlloc::<f64>::new("obj_malloc_u8".to_string());
        alloc.init(0, 100000);
        stress(alloc.clone());
        let &(ref lock, ref _cvar) = &*alloc.free_entries;
        let free_entries = lock.lock();
        assert_eq!(free_entries.len(), 100000 / std::mem::size_of::<f64>());
    }
}
