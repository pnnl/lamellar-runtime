pub(crate) mod collect;
pub(crate) mod count;
pub(crate) mod for_each;
pub(crate) mod reduce;

use crate::array::iterator::local_iterator::LocalIterator;
use collect::*;
use count::*;
use for_each::*;
use reduce::*;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize,Ordering};
use parking_lot::Mutex;
use rand::thread_rng;
use rand::prelude::SliceRandom;


#[derive(Clone, Debug)]
pub(crate) struct IterWorkStealer {
    pub(crate) range: Arc<Mutex<(usize, usize)>>, //start, end
}

impl IterWorkStealer {
    fn set_range(&self, start: usize, end: usize) {
        let mut range = self.range.lock();
        range.0 = start;
        range.1 = end;
    }

    fn next(&self) -> Option<usize> {
        let mut range = self.range.lock();
        range.0 += 1;
        if range.0 <= range.1 {
            Some(range.0)
        } else {
            None
        }
    }
    fn set_done(&self) {
        let mut range = self.range.lock();
        range.0 = range.1;
    }

    fn steal(&self) -> Option<(usize, usize)> {
        let mut range = self.range.lock();
        let start = range.0;
        let end = range.1;
        if end > start && end - start > 2 {
            let new_end = (start + end) / 2;
            range.1 = new_end;
            Some((new_end, end))
        } else {
            None
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum IterSchedule<I>{
    Static(I,usize,usize),
    Dynamic(I,Arc<AtomicUsize>,usize),
    Chunk(I,Vec<(usize, usize)>, Arc<AtomicUsize>,),
    WorkStealing(I,IterWorkStealer, Vec<IterWorkStealer>)
}

impl<I: LocalIterator> IntoIterator for &IterSchedule<I> {
    type Item = I::Item;
    type IntoIter = IterScheduleIter<I>;
    fn into_iter(self) -> Self::IntoIter {
        match self {
            IterSchedule::Static(iter, start, end) => {
                let iter = iter.init(*start,*end-*start);
                IterScheduleIter::Static(iter)
            }
            IterSchedule::Dynamic(iter, cur_i, max_i) => {
                IterScheduleIter::Dynamic(iter.clone(), cur_i.clone(), *max_i)
            }
            IterSchedule::Chunk(iter, ranges, range_i) => {
                IterScheduleIter::Chunk(iter.init(0,0), ranges.clone(),range_i.clone())
            }
            IterSchedule::WorkStealing(iter, range, siblings) => {
                let (start, end) = *range.range.lock();
                IterScheduleIter::WorkStealing(iter.init(start, end-start), range.clone(), siblings.clone())
            }
        }
    }
    fn enumerate_iter(self){
        match self {
            IterSchedule::Static(iter, start, end) => {
                let iter = iter.enumerate().init(*start,*end-*start);
                IterScheduleIter::Static(iter)
            }
            IterSchedule::Dynamic(iter, cur_i, max_i) => {
                IterScheduleIter::Dynamic(iter.enumerate(), cur_i.clone(), *max_i)
            }
            IterSchedule::Chunk(iter, ranges, range_i) => {
                IterScheduleIter::Chunk(iter.enumerate().init(0,0), ranges.clone(),range_i.clone())
            }
            IterSchedule::WorkStealing(iter, range, siblings) => {
                let (start, end) = *range.range.lock();
                IterScheduleIter::WorkStealing(iter.enumerate().init(start, end-start), range.clone(), siblings.clone())            }
        }
    }
}

pub(crate) enum IterScheduleIter<I>{
    Static(I),
    Dynamic(I,Arc<AtomicUsize>,usize),
    Chunk(I,Vec<(usize, usize)>, Arc<AtomicUsize>),
    WorkStealing(I,IterWorkStealer, Vec<IterWorkStealer>)
}

impl<I: LocalIterator> Iterator for IterScheduleIter<I> {
    type Item = I::Item;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            IterScheduleIter::Static(iter) => {
                iter.next()
            }
            IterScheduleIter::Dynamic(iter, cur_i, max_i) => {
                let ci = cur_i.fetch_add(1, Ordering::Relaxed);
                if ci < *max_i {
                    *iter = iter.init(ci,1);
                    iter.next()
                }
                else{
                    None
                }
            }
            IterScheduleIter::Chunk(iter, ranges, range_i) => {
                let mut next = iter.next();
                if next.is_none(){
                    let ri = range_i.fetch_add(1, Ordering::Relaxed);
                    if ri < ranges.len() {
                        *iter = iter.init(ranges[ri].0, ranges[ri].1);
                        next = iter.next();
                    }
                }
                next
            }
            IterScheduleIter::WorkStealing(iter, range, siblings) => {
                let mut inner_next = |iter: &mut I| {
                    if range.next().is_some() {
                        if let Some(elem) = iter.next() {
                            return Some(elem);
                        }
                        else{
                            range.set_done();
                        }
                    }
                    None
                };  
                let mut next = inner_next(iter);
                if next.is_none() {
                    let mut rng = thread_rng();
                    let mut workers = (0..siblings.len()).collect::<Vec<usize>>();
                    workers.shuffle(&mut rng);
                    if let Some(worker) = workers.pop() {
                        if let Some((start, end)) = siblings[worker].steal() {
                            *iter = iter.init(start, end - start);
                            range.set_range(start, end);
                            next = inner_next(iter);
                        }
                    }
                }
                next
            }
        }
    }
}
