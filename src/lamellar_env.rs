use crate::AtomicArray;
use crate::Dist;
use crate::LamellarTeam;
use enum_dispatch::enum_dispatch;
use std::sync::Arc;

/// A trait for accessing various data about the current lamellar envrionment
#[enum_dispatch]
pub trait LamellarEnv {
    /// Return the PE id of the calling PE,
    /// if called on a team instance, the PE id will be with respect to the team
    /// (not to the world)
    fn my_pe(&self) -> usize;

    /// Return the number of PEs in the execution
    fn num_pes(&self) -> usize;

    /// Return the number of threads per PE
    fn num_threads_per_pe(&self) -> usize;

    /// Return a pointer the world team
    fn world(&self) -> Arc<LamellarTeam>;

    /// Return a pointer to the LamellarTeam
    fn team(&self) -> Arc<LamellarTeam>;
}
