//! Represents iterators that consume the elements.
//! Iterator Consumers are what end up "driving" the iterator
//!

use crate::active_messaging::{LamellarArcLocalAm, SyncSend};
use crate::lamellar_task_group::TaskGroupLocalAmHandle;
use crate::lamellar_team::LamellarTeamRT;

use parking_lot::Mutex;
use rand::prelude::SliceRandom;
use rand::thread_rng;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

// trait Consumer{
//     type Item;
//     fn init(&self, start: usize, cnt: usize, _s: Sealed) -> Self;
//     fn monotonic(&self) -> Self;
//     fn next(&self) -> Self::Item;
// }

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
        let index = range.0;
        range.0 += 1;
        if range.0 <= range.1 {
            Some(index)
        } else {
            None
        }
    }
    // fn set_done(&self) {
    //     let mut range = self.range.lock();
    //     range.0 = range.1;
    // }

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
pub(crate) enum IterSchedule {
    Static(usize, usize),
    Dynamic(Arc<AtomicUsize>, usize),
    Chunk(Vec<(usize, usize)>, Arc<AtomicUsize>),
    WorkStealing(IterWorkStealer, Vec<IterWorkStealer>),
}

impl IterSchedule {
    pub(crate) fn init_iter<I: IterConsumer>(&self, iter: I) -> IterScheduleIter<I> {
        match self {
            IterSchedule::Static(start, end) => {
                IterScheduleIter::Static(iter.init(*start, end - start))
            }
            IterSchedule::Dynamic(cur_i, max_i) => {
                IterScheduleIter::Dynamic(iter, cur_i.clone(), *max_i)
            }
            IterSchedule::Chunk(ranges, range_i) => {
                IterScheduleIter::Chunk(iter.init(0, 0), ranges.clone(), range_i.clone())
            }
            IterSchedule::WorkStealing(range, siblings) => {
                let (start, end) = *range.range.lock();
                IterScheduleIter::WorkStealing(
                    iter.init(start, end - start),
                    range.clone(),
                    siblings.clone(),
                )
            }
        }
    }
}

pub(crate) enum IterScheduleIter<I: IterConsumer> {
    Static(I),
    Dynamic(I, Arc<AtomicUsize>, usize),
    Chunk(I, Vec<(usize, usize)>, Arc<AtomicUsize>),
    WorkStealing(I, IterWorkStealer, Vec<IterWorkStealer>),
}

impl<I: IterConsumer> Iterator for IterScheduleIter<I> {
    type Item = I::Item;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            IterScheduleIter::Static(iter) => iter.next(),
            IterScheduleIter::Dynamic(iter, cur_i, max_i) => {
                let mut ci = cur_i.fetch_add(1, Ordering::Relaxed);
                while ci < *max_i {
                    // println!("ci {:?} maxi {:?} {:?}", ci, *max_i, std::thread::current().id());
                    *iter = iter.init(ci, 1);
                    if let Some(elem) = iter.next() {
                        return Some(elem);
                    }
                    ci = cur_i.fetch_add(1, Ordering::Relaxed);
                }
                None
            }
            IterScheduleIter::Chunk(iter, ranges, range_i) => {
                let mut next = iter.next();
                // println!("next {:?} {:?}", next.is_none(), std::thread::current().id());
                if next.is_none() {
                    let ri = range_i.fetch_add(1, Ordering::Relaxed);
                    // println!("range {:?} {:?}", ri, std::thread::current().id());
                    if ri < ranges.len() {
                        *iter = iter.init(ranges[ri].0, ranges[ri].1 - ranges[ri].0);
                        next = iter.next();
                    }
                }
                next
            }
            IterScheduleIter::WorkStealing(iter, range, siblings) => {
                let inner_next = |iter: &mut I| {
                    while let Some(ri) = range.next() {
                        *iter = iter.init(ri, 1);
                        if let Some(elem) = iter.next() {
                            return Some(elem);
                        }
                        // else{
                        //     range.set_done();
                        // }
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

pub(crate) trait IterConsumer: SyncSend {
    type AmOutput;
    type Output;
    type Item;
    type Handle;
    fn init(&self, start: usize, cnt: usize) -> Self;
    fn next(&mut self) -> Option<Self::Item>;
    fn into_am(&self, schedule: IterSchedule) -> LamellarArcLocalAm;
    fn create_handle(
        self,
        team: Pin<Arc<LamellarTeamRT>>,
        reqs: VecDeque<TaskGroupLocalAmHandle<Self::AmOutput>>,
    ) -> Self::Handle;
    fn max_elems(&self, in_elems: usize) -> usize;
}
