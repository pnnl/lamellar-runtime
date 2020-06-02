use indexmap::IndexSet;
use parking_lot::{Condvar, Mutex};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub(crate) trait LamellarAlloc {
    fn new(id: String) -> Self;
    fn init(&mut self, start_addr: usize, size: usize);
    fn malloc(&mut self, size: usize) -> usize;
    fn try_malloc(&self, size: usize) -> Option<usize>;
    fn free(&self, addr: usize);
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
    id: String,
}

impl LamellarAlloc for LinearAlloc {
    fn new(id: String) -> LinearAlloc {
        LinearAlloc {
            entries: Arc::new((Mutex::new(Vec::new()), Condvar::new())),
            start_addr: 0,
            max_size: 0,
            last_idx: Arc::new(AtomicUsize::new(0)),
            id: id,
        }
    }

    fn init(&mut self, start_addr: usize, size: usize) {
        self.start_addr = start_addr;
        self.max_size = size;
    }

    fn malloc(&mut self, size: usize) -> usize {
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
                return Some(prev_end);
            } else {
                cvar.wait(&mut entries);
                return None;
            }
        } else {
            if size <= self.start_addr + self.max_size {
                let n_vma = Vma {
                    addr: self.start_addr,
                    size: size,
                };
                entries.push(n_vma);
                return Some(self.start_addr);
            } else {
                cvar.wait(&mut entries);
                return None;
            }
        }
    }

    fn free(&self, addr: usize) {
        let &(ref lock, ref cvar) = &*self.entries;
        let mut entries = lock.lock();
        for i in 0..entries.len() {
            if addr as usize == entries[i].addr as usize {
                entries.remove(i);
                let last_idx = self.last_idx.load(Ordering::SeqCst);
                if last_idx >= entries.len() && last_idx != 0 {
                    self.last_idx.fetch_sub(1, Ordering::SeqCst);
                }
                cvar.notify_all();

                return;
            }
        }
    }
}

#[derive(Clone, Debug)]
struct FreeEntries {
    sizes: BTreeMap<usize, IndexSet<usize>>, //<size,<Vec<addr>>
    addrs: BTreeMap<usize, usize>,           //<addr,size>
}
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
    start_addr: usize,
    max_size: usize,
    id: String,
}

impl LamellarAlloc for BTreeAlloc {
    fn new(id: String) -> BTreeAlloc {
        BTreeAlloc {
            free_entries: Arc::new((Mutex::new(FreeEntries::new()), Condvar::new())),
            allocated_addrs: Arc::new((Mutex::new(BTreeMap::new()), Condvar::new())),
            start_addr: 0,
            max_size: 0,
            id: id,
        }
    }
    fn init(&mut self, start_addr: usize, size: usize) {
        self.start_addr = start_addr;
        self.max_size = size;
        let &(ref lock, ref _cvar) = &*self.free_entries;
        let mut free_entries = lock.lock();
        let mut temp = IndexSet::new();
        temp.insert(start_addr);
        free_entries.sizes.insert(size, temp);
        free_entries.addrs.insert(start_addr, size);
    }

    fn malloc(&mut self, size: usize) -> usize {
        let mut val = self.try_malloc(size);
        while let None = val {
            val = self.try_malloc(size);
        }
        val.unwrap()
    }

    fn try_malloc(&self, size: usize) -> Option<usize> {
        let &(ref lock, ref cvar) = &*self.free_entries;
        let mut free_entries = lock.lock();
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
            cvar.wait(&mut free_entries);
        }
        if let Some(rsize) = remove_size {
            free_entries.sizes.remove(&rsize);
        }
        drop(free_entries);
        if let Some(a) = addr {
            let &(ref lock, ref _cvar) = &*self.allocated_addrs;
            let mut allocated_addrs = lock.lock();
            allocated_addrs.insert(a, size);
        }
        addr
    }

    fn free(&self, addr: usize) {
        let &(ref lock, ref _cvar) = &*self.allocated_addrs;
        let mut allocated_addrs = lock.lock();
        if let Some(size) = allocated_addrs.remove(&addr) {
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
        } else {
            panic!(
                "{:?} illegal free, addr not currently allocated: {:?}",
                self.id, addr
            )
        }
    }
}

#[derive(Clone)]
pub(crate) struct ObjAlloc {
    free_entries: Arc<(Mutex<Vec<usize>>, Condvar)>,
    start_addr: usize,
    max_size: usize,
    id: String,
}

impl LamellarAlloc for ObjAlloc {
    fn new(id: String) -> ObjAlloc {
        ObjAlloc {
            free_entries: Arc::new((Mutex::new(Vec::new()), Condvar::new())),
            start_addr: 0,
            max_size: 0,
            id: id,
        }
    }
    fn init(&mut self, start_addr: usize, size: usize) {
        self.start_addr = start_addr;
        self.max_size = size;
        let &(ref lock, ref _cvar) = &*self.free_entries;
        let mut free_entries = lock.lock();
        for i in start_addr..(start_addr + size) {
            free_entries.push(i);
        }
    }

    fn malloc(&mut self, size: usize) -> usize {
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
        let &(ref lock, ref cvar) = &*self.free_entries;
        let mut free_entries = lock.lock();
        if let Some(addr) = free_entries.pop() {
            return Some(addr);
        } else {
            cvar.wait(&mut free_entries);
            return None;
        }
    }

    fn free(&self, addr: usize) {
        let &(ref lock, ref cvar) = &*self.free_entries;
        let mut free_entries = lock.lock();
        free_entries.push(addr);
        cvar.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_linear_malloc() {
        let mut alloc = LinearAlloc::new("test_linear_malloc".to_string());
        alloc.init(0, 10);

        // for i in 0..10{
        //     assert_eq!(alloc.malloc_fast(1),Some(i));
        // }
    }

    #[test]
    fn test_malloc_free() {
        let mut alloc = LinearAlloc::new("test_malloc_free".to_string());
        alloc.init(0, 10);
        // assert_eq!(alloc.malloc_fast(5),Some(0));
        // assert_eq!(alloc.malloc_fast(1),Some(5));
        // assert_eq!(alloc.malloc_fast(1),Some(6));
        // alloc.free(0);
        // assert_eq!(alloc.malloc_fast(3),Some(0));
        // assert_eq!(alloc.malloc_fast(3),Some(7));
        // assert_eq!(alloc.malloc_fast(2),Some(3));
    }

    #[test]
    fn test_bttreealloc_malloc() {
        let mut alloc = BTreeAlloc::new("test_bttree_malloc".to_string());
        alloc.init(0, 1024);
        let mut sum = 1;
        for i in 0..10 {
            let addr = alloc.malloc(sum);
            println!("{:?} {:?} {:?}", sum, addr, alloc);
            // assert_eq!(addr.unwrap()+sum,sum);
            sum += addr.unwrap();
        }
        println!("{:?}", alloc);
        assert_eq!(0, 1);
    }

    #[test]
    fn test_bttreealloc_free() {
        let mut alloc = BTreeAlloc::new("test_malloc_free".to_string());
        alloc.init(0, 1000);
        assert_eq!(alloc.malloc(50), Some(0));
        println!("{:?}", alloc);
        assert_eq!(alloc.malloc(10), Some(50));
        println!("{:?}", alloc);
        assert_eq!(alloc.malloc(600), Some(60));
        println!("{:?}", alloc);
        assert_eq!(alloc.malloc(10), Some(660));
        println!("{:?}", alloc);
        alloc.free(0);
        println!("{:?}", alloc);
        assert_eq!(alloc.malloc(30), Some(0));
        println!("{:?}", alloc);
        alloc.free(50);
        println!("{:?}", alloc);
        assert_eq!(alloc.malloc(10), Some(30));
        println!("{:?}", alloc);
        assert_eq!(alloc.malloc(10), Some(40));
        println!("{:?}", alloc);
        alloc.free(660);
        println!("{:?}", alloc);
        alloc.free(30);
        println!("{:?}", alloc);
        alloc.malloc(10);
        println!("{:?}", alloc);
        alloc.free(30);
        println!("{:?}", alloc);
        assert_eq!(alloc.malloc(200), Some(660));
        println!("{:?}", alloc);
        alloc.free(60);
        println!("{:?}", alloc);
        alloc.free(660);
        println!("{:?}", alloc);
        alloc.free(40);
        println!("{:?}", alloc);
        alloc.free(0);
        println!("{:?}", alloc);
        assert_eq!(0, 1);
        // assert_eq!(alloc.malloc_fast(2),Some(3));
    }

    #[test]
    fn test_obj_malloc_free() {
        let mut alloc = ObjAlloc::new("test_obj_malloc_free".to_string());
        alloc.init(0, 10);
        for i in 0..10 {
            println!("alloc: {:?}", alloc.malloc());
        }
        for i in 0..10 {
            alloc.free(i);
        }

        for i in 0..6 {
            println!("alloc: {:?}", alloc.malloc());
        }
        alloc.free(8);
        alloc.free(6);
        alloc.free(9);
        for i in 0..6 {
            println!("alloc: {:?}", alloc.malloc());
        }
        assert_eq!(0, 1);
    }
}
