use core::marker::PhantomData;
use indexmap::IndexSet;
use lamellar_prof::*;
// use log::trace;
use parking_lot::{Condvar, Mutex};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub(crate) trait LamellarAlloc {
    fn new(id: String) -> Self;
    fn init(&mut self, start_addr: usize, size: usize); //size in bytes
    fn malloc(&self, size: usize) -> usize;
    fn try_malloc(&self, size: usize) -> Option<usize>;
    fn fake_malloc(&self, size: usize) -> bool;
    fn free(&self, addr: usize) -> Result<(), usize>;
    fn space_avail(&self) -> usize;
    fn occupied(&self) -> usize;
}

#[derive(Debug)]
struct Vma {
    addr: usize,
    size: usize,
}

#[derive(Clone)]
pub(crate) struct LinearAlloc {
    entries: Arc<(Mutex<Vec<Vma>>, Condvar)>,
    start_addr: usize,
    max_size: usize,
    last_idx: Arc<AtomicUsize>,
    _id: String,
    free_space: Arc<AtomicUsize>,
}

#[prof]
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

    fn malloc(&self, size: usize) -> usize {
        let mut val = self.try_malloc(size);
        while let None = val {
            val = self.try_malloc(size);
        }
        val.unwrap()
    }

    fn try_malloc(&self, size: usize) -> Option<usize> {
        let &(ref lock, ref cvar) = &*self.entries;
        let mut entries = lock.lock();

        if entries.len() > 0 {
            let mut prev_end = self.start_addr;
            let mut idx = 0;
            for i in 0..entries.len() {
                if entries[i].addr - prev_end >= size {
                    break;
                }
                prev_end = entries[i].addr + entries[i].size;
                idx = i + 1;
            }

            if prev_end + size <= self.start_addr + self.max_size {
                let n_vma = Vma {
                    addr: prev_end,
                    size: size,
                };
                entries.insert(idx, n_vma);
                self.free_space.fetch_sub(size, Ordering::SeqCst);
                return Some(prev_end);
            } else {
                prof_start!(waiting);
                cvar.wait_for(&mut entries, std::time::Duration::from_millis(1));
                prof_end!(waiting);
                return None;
            }
        } else {
            if size <= self.start_addr + self.max_size {
                let n_vma = Vma {
                    addr: self.start_addr,
                    size: size,
                };
                entries.push(n_vma);
                self.free_space.fetch_sub(size, Ordering::SeqCst);
                return Some(self.start_addr);
            } else {
                prof_start!(waiting);
                cvar.wait_for(&mut entries, std::time::Duration::from_millis(1));
                prof_end!(waiting);
                return None;
            }
        }
    }

    fn fake_malloc(&self, size: usize) -> bool {
        let &(ref lock, ref _cvar) = &*self.entries;
        let entries = lock.lock();

        if entries.len() > 0 {
            let mut prev_end = self.start_addr;
            for i in 0..entries.len() {
                if entries[i].addr - prev_end >= size {
                    break;
                }
                prev_end = entries[i].addr + entries[i].size;
            }

            if prev_end + size <= self.start_addr + self.max_size {
                return true;
            } else {
                return false;
            }
        } else {
            if size <= self.start_addr + self.max_size {
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
            if addr as usize == entries[i].addr as usize {
                self.free_space.fetch_add(entries[i].size, Ordering::SeqCst);
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
    addrs: BTreeMap<usize, usize>,           //<addr,size>
}
//#[prof]
impl FreeEntries {
    fn new() -> FreeEntries {
        FreeEntries {
            sizes: BTreeMap::new(),
            addrs: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct BTreeAlloc {
    free_entries: Arc<(Mutex<FreeEntries>, Condvar)>,
    allocated_addrs: Arc<(Mutex<BTreeMap<usize, usize>>, Condvar)>, //<addr,size>
    pub(crate) start_addr: usize,
    pub(crate) max_size: usize,
    id: String,
    free_space: Arc<AtomicUsize>,
}

#[prof]
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
        free_entries.addrs.insert(start_addr, size);
        self.free_space.store(size, Ordering::SeqCst);
    }

    fn malloc(&self, size: usize) -> usize {
        let mut val = self.try_malloc(size);
        let mut timer = std::time::Instant::now();
        while let None = val {
            val = self.try_malloc(size);
            if timer.elapsed().as_secs_f64() > 10.0 {
                println!("probably out of memory");
                timer = std::time::Instant::now();
            }
        }
        val.unwrap()
    }

    fn try_malloc(&self, size: usize) -> Option<usize> {
        prof_start!(locking);
        let &(ref lock, ref cvar) = &*self.free_entries;
        let mut free_entries = lock.lock();
        prof_end!(locking);
        let mut addr: Option<usize> = None;
        let mut remove_size: Option<usize> = None;
        //find smallest memory segment greater than or equal to size
        if let Some((free_size, addrs)) = free_entries.sizes.range_mut(size..).next() {
            //the actual size, and list of addrs with that size
            addr = addrs.pop(); // get the first addr

            //if no more address exist with this size, mark it as removable from the free_entries size map
            if addrs.is_empty() {
                remove_size = Some(free_size.clone());
            }

            if let Some(a) = addr {
                if let Some(fsize) = free_entries.addrs.remove(&a) {
                    // remove the address from free_entries address map
                    if fsize != size {
                        let remaining = fsize - size;
                        let new_addr = a + size;
                        free_entries
                            .sizes
                            .entry(remaining)
                            .or_insert(IndexSet::new()) //insert new entry of remaining size if not already present
                            .insert(new_addr); //insert address into size map
                        free_entries.addrs.insert(new_addr, remaining);
                    }
                } else {
                    panic!("{:?} addr {:?} not found in free_entries", self.id, a);
                }
            }
        } else {
            prof_start!(waiting);
            cvar.wait_for(&mut free_entries, std::time::Duration::from_millis(1));
            prof_end!(waiting);
        }
        if let Some(rsize) = remove_size {
            free_entries.sizes.remove(&rsize);
        }
        drop(free_entries);
        if let Some(a) = addr {
            let &(ref lock, ref _cvar) = &*self.allocated_addrs;
            let mut allocated_addrs = lock.lock();
            allocated_addrs.insert(a, size);
            self.free_space.fetch_sub(size, Ordering::SeqCst);
        }
        addr
    }

    fn fake_malloc(&self, size: usize) -> bool {
        prof_start!(locking);
        let &(ref lock, ref _cvar) = &*self.free_entries;
        let mut free_entries = lock.lock();
        prof_end!(locking);
        //find smallest memory segment greater than or equal to size
        if let Some((_, _)) = free_entries.sizes.range_mut(size..).next() {
            return true;
        } else {
            return false;
        }
    }

    fn free(&self, addr: usize) -> Result<(), usize> {
        let &(ref lock, ref _cvar) = &*self.allocated_addrs;
        let mut allocated_addrs = lock.lock();
        if let Some(size) = allocated_addrs.remove(&addr) {
            self.free_space.fetch_add(size, Ordering::SeqCst);
            drop(allocated_addrs);
            let mut temp_addr = addr;
            let mut temp_size = size;
            let mut remove: Vec<(usize, usize)> = vec![];
            let &(ref lock, ref cvar) = &*self.free_entries;
            let mut free_entries = lock.lock();
            if let Some((faddr, fsize)) = free_entries.addrs.range(..addr).next_back() {
                //look at address before addr
                if faddr + fsize == addr {
                    //they are next to eachother...
                    temp_addr = *faddr;
                    temp_size = fsize + size;
                    remove.push((faddr.clone(), fsize.clone()));
                }
            }

            if let Some((faddr, fsize)) = free_entries.addrs.range(addr..).next() {
                //look at address after addr
                if temp_addr + temp_size == *faddr {
                    //they are next to eachother...
                    temp_size += fsize;
                    remove.push((faddr.clone(), fsize.clone()));
                }
            }
            for (raddr, rsize) in remove {
                free_entries.addrs.remove(&raddr);
                let mut remove_size = false;
                if let Some(addrs) = free_entries.sizes.get_mut(&rsize) {
                    addrs.remove(&raddr);
                    if addrs.is_empty() {
                        remove_size = true;
                    }
                }
                if remove_size {
                    free_entries.sizes.remove(&rsize);
                }
            }
            free_entries.addrs.insert(temp_addr, temp_size);
            free_entries
                .sizes
                .entry(temp_size)
                .or_insert(IndexSet::new())
                .insert(temp_addr);
            cvar.notify_all();
            Ok(())
        } else {
            // panic!(
            //     "{:?} illegal free, addr not currently allocated: {:?}",
            //     self.id, addr
            // )
            Err(addr)
        }
    }

    fn space_avail(&self) -> usize {
        self.free_space.load(Ordering::SeqCst)
    }
    fn occupied(&self) -> usize {
        self.max_size - self.free_space.load(Ordering::SeqCst)
    }
}

#[derive(Clone)]
pub(crate) struct ObjAlloc<T: Copy> {
    free_entries: Arc<(Mutex<Vec<usize>>, Condvar)>,
    start_addr: usize,
    max_size: usize,
    _id: String,
    phantom: PhantomData<T>,
}

#[prof]
impl<T: Copy> LamellarAlloc for ObjAlloc<T> {
    fn new(id: String) -> ObjAlloc<T> {
        // trace!("new ObjAlloc: {:?}", id);
        ObjAlloc {
            free_entries: Arc::new((Mutex::new(Vec::new()), Condvar::new())),
            start_addr: 0,
            max_size: 0,
            _id: id,
            phantom: PhantomData,
        }
    }
    fn init(&mut self, start_addr: usize, size: usize) {
        // trace!("init: {:?} {:x} {:?}", self.id, start_addr, size);
        self.start_addr = start_addr;
        self.max_size = size;
        let &(ref lock, ref _cvar) = &*self.free_entries;
        let mut free_entries = lock.lock();
        *free_entries = (start_addr..(start_addr + size))
            .step_by(std::mem::size_of::<T>())
            .collect();
    }

    fn malloc(&self, size: usize) -> usize {
        let mut val = self.try_malloc(size);
        while let None = val {
            val = self.try_malloc(size);
        }
        val.unwrap()
    }

    fn try_malloc(&self, size: usize) -> Option<usize> {
        assert_eq!(
            size, 1,
            "ObjAlloc does not currently support multiobject allocations"
        );
        prof_start!(locking);
        let &(ref lock, ref cvar) = &*self.free_entries;
        let mut free_entries = lock.lock();
        prof_end!(locking);
        if let Some(addr) = free_entries.pop() {
            return Some(addr);
        } else {
            prof_start!(waiting);
            cvar.wait_for(&mut free_entries, std::time::Duration::from_millis(1));
            prof_end!(waiting);
            return None;
        }
    }

    fn fake_malloc(&self, size: usize) -> bool {
        assert_eq!(
            size, 1,
            "ObjAlloc does not currently support multiobject allocations"
        );
        prof_start!(locking);
        let &(ref lock, ref _cvar) = &*self.free_entries;
        let free_entries = lock.lock();
        prof_end!(locking);
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
            assert_eq!(alloc.malloc(i), cnt);
            cnt += i;
        }
        for i in (cnt..100).step_by(5) {
            assert_eq!(alloc.malloc(5), i);
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
                    if rng.gen_range(0, 2) == 0 || addrs.len() == 0 {
                        if let Some(addr) = alloc_clone.try_malloc(1) {
                            addrs.push(addr);
                            i += 1;
                        }
                    } else {
                        let index = rng.gen_range(0, addrs.len());
                        let addr = addrs.remove(index);
                        alloc_clone.free(addr).unwrap();
                    }
                }
                for addr in addrs {
                    alloc_clone.free(addr).unwrap();
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
        assert_eq!(free_entries.addrs.get(&0), Some(&100000usize));
        let &(ref lock, ref _cvar) = &*alloc.allocated_addrs;
        let allocated_addrs = lock.lock();
        assert_eq!(allocated_addrs.len(), 0);
    }

    #[test]
    fn test_obj_malloc() {
        let mut alloc = ObjAlloc::<u8>::new("obj_malloc_u8".to_string());
        alloc.init(0, 10);
        for i in 0..10 {
            assert_eq!(alloc.malloc(1), 9 - i); //object allocator returns last address first...
        }

        let mut alloc = ObjAlloc::<u16>::new("obj_malloc_u16".to_string());
        alloc.init(0, 10); //only 5 slots
        for i in 0..5 {
            assert_eq!(alloc.malloc(1), 8 - (i * std::mem::size_of::<u16>()));
        }
        assert_eq!(alloc.try_malloc(1), None);
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
