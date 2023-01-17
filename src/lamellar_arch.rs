use lamellar_prof::prof;

use std::sync::Arc;
// use std::collections::hash_map::DefaultHasher;

/// An abstraction which represents the PEs that are associated with a Lamellar team
pub trait LamellarArch: Send + Sync {
    /// The number of  PEs in the team defined by this LamellarArch
    fn num_pes(&self) -> usize;
    /// The id of the first (lowest numbered) PE in the team
    fn start_pe(&self) -> usize; //with respect to parent (maybe this should be min possible pe?)
    /// The id of the first (highest numbered) PE in the team
    fn end_pe(&self) -> usize; //with respect to parent (maybe this should be max possible pe?)
                               //TODO expand example
    /// Converts a (sub)team PE id into the id space of the Parent team
    ///
    /// Returns an error if the pe does not exist in the team
    fn parent_pe_id(&self, team_pe: &usize) -> ArchResult<usize>; // need global id so the lamellae knows who to communicate -- this should this be parent pe?
    /// Converts a Parent team PE id into the id space of the team specified by this LamellarArch
    ///
    /// Returns an error if the pe does not exist in the team
    fn team_pe_id(&self, parent_pe: &usize) -> ArchResult<usize>; // team id is for user convenience, ids == 0..num_pes-1
}

/// An error that occurs when trying to access a PE that does not exist on a team/subteam
#[derive(Debug, Clone, Copy)]
pub struct IdError {
    pub parent_pe: usize,
    pub team_pe: usize,
}

type ArchResult<T> = Result<T, IdError>;

#[prof]
impl std::fmt::Display for IdError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "Invalid Id => parent_pe:{} team_pe => {}",
            self.parent_pe, self.team_pe
        )
    }
}

#[prof]
impl std::error::Error for IdError {}

#[derive(Clone)] //, Hash)]
pub(crate) enum LamellarArchEnum {
    GlobalArch(GlobalArch),
    StridedArch(StridedArch),
    BlockedArch(BlockedArch),
    Dynamic(Arc<dyn LamellarArch>),
}

impl std::fmt::Debug for LamellarArchEnum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LamellarArchEnum::GlobalArch(_) => write!(f, "GlobalArch"),
            LamellarArchEnum::StridedArch(_) => write!(f, "StridedArch"),
            LamellarArchEnum::BlockedArch(_) => write!(f, "BlockedArch"),
            LamellarArchEnum::Dynamic(_) => write!(f, "Dynamic"),
        }
    }
}

impl LamellarArchEnum {
    pub fn new<A>(arch: A) -> LamellarArchEnum
    where
        A: LamellarArch + 'static,
    {
        assert!(arch.num_pes() > 0);
        let any_arch = &arch as &dyn std::any::Any;
        let arch_enum = if let Some(strided) = any_arch.downcast_ref::<StridedArch>() {
            LamellarArchEnum::StridedArch(*strided)
        } else if let Some(blocked) = any_arch.downcast_ref::<BlockedArch>() {
            LamellarArchEnum::BlockedArch(*blocked)
        } else if let Some(global) = any_arch.downcast_ref::<GlobalArch>() {
            LamellarArchEnum::GlobalArch(*global)
        } else {
            LamellarArchEnum::Dynamic(Arc::new(arch))
        };
        arch_enum
    }
}
// It might be worth using the enum_dispatch crate for this?
// https://gitlab.com/antonok/enum_dispatch
#[prof]
impl LamellarArch for LamellarArchEnum {
    fn num_pes(&self) -> usize {
        match self {
            LamellarArchEnum::GlobalArch(arch) => arch.num_pes(),
            LamellarArchEnum::StridedArch(arch) => arch.num_pes(),
            LamellarArchEnum::BlockedArch(arch) => arch.num_pes(),
            LamellarArchEnum::Dynamic(arch) => arch.num_pes(),
        }
    }
    fn start_pe(&self) -> usize {
        match self {
            LamellarArchEnum::GlobalArch(arch) => arch.start_pe(),
            LamellarArchEnum::StridedArch(arch) => arch.start_pe(),
            LamellarArchEnum::BlockedArch(arch) => arch.start_pe(),
            LamellarArchEnum::Dynamic(arch) => arch.start_pe(),
        }
    }
    fn end_pe(&self) -> usize {
        match self {
            LamellarArchEnum::GlobalArch(arch) => arch.end_pe(),
            LamellarArchEnum::StridedArch(arch) => arch.end_pe(),
            LamellarArchEnum::BlockedArch(arch) => arch.end_pe(),
            LamellarArchEnum::Dynamic(arch) => arch.end_pe(),
        }
    }
    fn parent_pe_id(&self, team_pe: &usize) -> ArchResult<usize> {
        match self {
            LamellarArchEnum::GlobalArch(arch) => arch.parent_pe_id(team_pe),
            LamellarArchEnum::StridedArch(arch) => arch.parent_pe_id(team_pe),
            LamellarArchEnum::BlockedArch(arch) => arch.parent_pe_id(team_pe),
            LamellarArchEnum::Dynamic(arch) => arch.parent_pe_id(team_pe),
        }
    }
    fn team_pe_id(&self, world_pe: &usize) -> ArchResult<usize> {
        match self {
            LamellarArchEnum::GlobalArch(arch) => arch.team_pe_id(world_pe),
            LamellarArchEnum::StridedArch(arch) => arch.team_pe_id(world_pe),
            LamellarArchEnum::BlockedArch(arch) => arch.team_pe_id(world_pe),
            LamellarArchEnum::Dynamic(arch) => arch.team_pe_id(world_pe),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct LamellarArchRT {
    pub(crate) parent: Option<Arc<LamellarArchRT>>,
    pub(crate) arch: LamellarArchEnum,
    pub(crate) num_pes: usize,
}

impl LamellarArchRT {
    pub fn new<A>(parent: Arc<LamellarArchRT>, arch: A) -> LamellarArchRT
    where
        A: LamellarArch + 'static,
    {
        assert!(
            arch.num_pes() <= parent.num_pes(),
            "cannot have more pes in subteam than parent"
        );

        let arch_first = arch.start_pe();
        let arch_last = arch.end_pe();
        let first = parent.arch.start_pe();
        let last = parent.arch.end_pe();
        assert!(
            first <= arch_first && arch_first <= last && first <= arch_last && arch_last <= last,
            "subteam PEs must be subset of parent PEs"
        );

        LamellarArchRT {
            parent: Some(parent),
            num_pes: arch.num_pes(),
            arch: LamellarArchEnum::new(arch),
        }
    }
    pub fn num_pes(&self) -> usize {
        self.num_pes
    }
    pub fn world_pe(&self, team_pe: usize) -> ArchResult<usize> {
        let parent_pe = self.arch.parent_pe_id(&team_pe)?;
        if let Some(parent) = &self.parent {
            parent.world_pe(parent_pe)
        } else {
            Ok(parent_pe)
        }
    }

    pub fn team_pe(&self, world_pe: usize) -> ArchResult<usize> {
        if let Some(parent) = &self.parent {
            let parent_pe = parent.team_pe(world_pe)?;
            // println!("world_pe {:?}   parent_pe {:?}  self: {:?}",world_pe, parent_pe,self);
            let res = self.arch.team_pe_id(&parent_pe);
            // println!("team_pe {:?}",res);
            res
        } else {
            // println!("root world_pe {:?}",world_pe);
            let res = self.arch.team_pe_id(&world_pe);
            // println!("team_pe {:?}",res);
            res
        }
    }

    pub fn team_iter(&self) -> Box<dyn Iterator<Item = usize>> {
        //return an iterator of the teams global pe ids
        Box::new(LamellarArchRTiter {
            arch: self.clone(),
            cur_pe: 0,
            single: false,
        })
    }
    #[allow(dead_code)]
    pub fn single_iter(&self, pe: usize) -> Box<dyn Iterator<Item = usize>> {
        //a single element iterator returning the global id of pe
        Box::new(LamellarArchRTiter {
            arch: self.clone(),
            cur_pe: pe,
            single: true,
        })
    }
}

pub(crate) struct LamellarArchRTiter {
    arch: LamellarArchRT,
    cur_pe: usize, //pe in team based ids
    single: bool,
}
#[prof]
impl Iterator for LamellarArchRTiter {
    type Item = usize;
    fn next(&mut self) -> Option<usize> {
        let res = if self.cur_pe < self.arch.num_pes() {
            if let Ok(pe) = self.arch.world_pe(self.cur_pe) {
                Some(pe)
            } else {
                None
            }
        } else {
            return None;
        };
        if self.single {
            self.cur_pe = self.arch.num_pes();
        } else {
            self.cur_pe += 1;
        }
        res
    }
}

#[doc(hidden)]
#[derive(Copy, Clone, std::hash::Hash, Debug)]
pub struct GlobalArch {
    pub(crate) num_pes: usize,
}

impl GlobalArch {
    pub fn new(num_pes: usize) -> GlobalArch {
        GlobalArch { num_pes: num_pes }
    }
}

#[prof]
impl LamellarArch for GlobalArch {
    fn num_pes(&self) -> usize {
        self.num_pes
    }
    fn start_pe(&self) -> usize {
        0
    }
    fn end_pe(&self) -> usize {
        self.num_pes - 1
    }

    fn parent_pe_id(&self, team_pe: &usize) -> ArchResult<usize> {
        if *team_pe < self.num_pes {
            Ok(*team_pe)
        } else {
            Err(IdError {
                parent_pe: *team_pe,
                team_pe: *team_pe,
            })
        }
    }
    fn team_pe_id(&self, parent_pe: &usize) -> ArchResult<usize> {
        if *parent_pe < self.num_pes {
            Ok(*parent_pe)
        } else {
            Err(IdError {
                parent_pe: *parent_pe,
                team_pe: *parent_pe,
            })
        }
    }
}

/// A grouping of PE's forming a team using a "strided" based distribution pattern.
///
/// # examples
///
///```
/// use lamellar::{LamellarWorldBuilder,StridedArch};
///
/// let world = LamellarWorldBuilder::new().build();
/// let num_pes = world.num_pes();
///
/// //create a team consisting of the "even" PEs in the world
/// let first_half_team = world.create_team_from_arch(StridedArch::new(
///    0,                                      // start pe
///    2,                                      // stride
///    (num_pes as f64 / 2.0).ceil() as usize, //num_pes in team
/// ));
#[derive(Copy, Clone, std::hash::Hash, Debug)]
pub struct StridedArch {
    pub(crate) num_pes: usize,
    pub(crate) start_pe: usize, //this is with respect to the parent arch
    pub(crate) end_pe: usize,   //this is with respect to the parent arch
    pub(crate) stride: usize, //this is with respect to the parent arch, if all arches were stided that this is multiplicative...(possibly an avenue for optiization)
}

#[prof]
impl StridedArch {
    /// Construct a new StrideArch using a starting PE, the stride length, and the number of PEs to include in the Block
    ///
    /// # Examples
    ///
    ///```
    /// use lamellar::StridedArch;
    ///
    /// StridedArch::new(
    ///    0, //start pe
    ///    4, //stride
    ///    5, //num_pes in team
    /// );
    /// // the team will consist of the 5 pes => 0,4,8,12,16
    pub fn new(start_pe: usize, stride: usize, num_team_pes: usize) -> StridedArch {
        let mut end_pe = start_pe;
        for _i in 1..num_team_pes {
            end_pe += stride;
        }
        StridedArch {
            num_pes: num_team_pes,
            start_pe: start_pe,
            end_pe: end_pe,
            stride: stride,
        }
    }
}

#[prof]
impl LamellarArch for StridedArch {
    fn num_pes(&self) -> usize {
        self.num_pes
    }
    fn start_pe(&self) -> usize {
        self.start_pe
    }
    fn end_pe(&self) -> usize {
        self.end_pe
    }
    fn parent_pe_id(&self, team_pe: &usize) -> ArchResult<usize> {
        let parent_pe = self.start_pe + team_pe * self.stride;
        if parent_pe >= self.start_pe && parent_pe <= self.end_pe && *team_pe < self.num_pes {
            Ok(parent_pe)
        } else {
            Err(IdError {
                parent_pe: parent_pe,
                team_pe: *team_pe,
            })
        }
    }
    fn team_pe_id(&self, parent_pe: &usize) -> ArchResult<usize> {
        if *parent_pe >= self.start_pe
            && *parent_pe <= self.end_pe
            && (parent_pe - self.start_pe) % self.stride == 0
        {
            let team_pe = (parent_pe - self.start_pe) / self.stride;
            if team_pe < self.num_pes {
                Ok(team_pe)
            } else {
                Err(IdError {
                    parent_pe: *parent_pe,
                    team_pe: team_pe,
                })
            }
        } else {
            Err(IdError {
                parent_pe: *parent_pe,
                team_pe: 0,
            })
        }
    }
}

/// A grouping of PE's forming a team using a "block" based distribution pattern.
///
/// PEs in the group are contiguous (with respect to their PE id, not necessarily their pyhsical location in the distributed envrionment).
///
/// # examples
///
///```
/// use lamellar::{LamellarWorldBuilder,BlockedArch};
///
/// let world = LamellarWorldBuilder::new().build();
/// let num_pes = world.num_pes();
///
/// //create a team consisting of the first half of PEs in the world
/// let first_half_team = world.create_team_from_arch(BlockedArch::new(
///    0,                                      //start pe
///    (num_pes as f64 / 2.0).ceil() as usize, //num_pes in team
/// ));
#[derive(Copy, Clone, std::hash::Hash, Debug)]
pub struct BlockedArch {
    pub(crate) num_pes: usize,
    pub(crate) start_pe: usize, //this is with respect to the parent arch (inclusive)
    pub(crate) end_pe: usize,   //this is with respect to the parent arch (inclusive)
}

#[prof]
impl BlockedArch {
    /// Construct a new Block using a starting PE and the number of PEs to include in the Block
    ///
    /// # Examples
    ///
    ///```
    /// use lamellar::BlockedArch;
    ///
    /// BlockedArch::new(
    ///    4, //start pe
    ///    5, //num_pes in team
    /// );
    /// // the team will consist of the 5 pes => 4,5,6,7,8
    pub fn new(start_pe: usize, num_team_pes: usize) -> BlockedArch {
        BlockedArch {
            num_pes: num_team_pes,
            start_pe: start_pe,
            end_pe: start_pe + num_team_pes - 1,
        }
    }
}

#[prof]
impl LamellarArch for BlockedArch {
    fn num_pes(&self) -> usize {
        self.num_pes
    }
    fn start_pe(&self) -> usize {
        self.start_pe
    }
    fn end_pe(&self) -> usize {
        self.end_pe
    }
    fn parent_pe_id(&self, team_pe: &usize) -> ArchResult<usize> {
        let parent_pe = self.start_pe + team_pe;
        if parent_pe >= self.start_pe && parent_pe <= self.end_pe && *team_pe < self.num_pes {
            Ok(parent_pe)
        } else {
            Err(IdError {
                parent_pe: parent_pe,
                team_pe: *team_pe,
            })
        }
    }
    fn team_pe_id(&self, parent_pe: &usize) -> ArchResult<usize> {
        if *parent_pe >= self.start_pe && *parent_pe <= self.end_pe {
            let team_pe = parent_pe - self.start_pe;
            if team_pe < self.num_pes {
                Ok(team_pe)
            } else {
                Err(IdError {
                    parent_pe: *parent_pe,
                    team_pe: team_pe,
                })
            }
        } else {
            Err(IdError {
                parent_pe: *parent_pe,
                team_pe: 0,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn global_arch() {
        let garch = Arc::new(LamellarArchRT {
            parent: None,
            arch: LamellarArchEnum::GlobalArch(GlobalArch::new(10)),
            num_pes: 10,
        });
        // assert_eq!(0, arch.my_pe());
        assert_eq!(10, garch.num_pes());
        assert_eq!(vec![0], garch.single_iter(0).collect::<Vec<usize>>());
        assert_eq!(vec![3], garch.single_iter(3).collect::<Vec<usize>>());
        assert_eq!(
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            garch.team_iter().collect::<Vec<usize>>()
        );
    }
    #[test]
    fn sub_arch_stride_1() {
        let garch = Arc::new(LamellarArchRT {
            parent: None,
            arch: LamellarArchEnum::GlobalArch(GlobalArch::new(10)),
            num_pes: 10,
        });
        // let arch = Arc::new(LamellarArchRT {
        //     parent: Some(garch.clone()),
        //     arch: LamellarArchEnum::new(StridedArch::new(0, 1, 5)),
        // });
        let arch = Arc::new(LamellarArchRT::new(
            garch.clone(),
            LamellarArchEnum::new(StridedArch::new(0, 1, 5)),
        ));
        // LamellarArchRT::new(garch.clone(),StridedArch::new(0, 1, 5));
        // assert_eq!(0, arch.my_pe());
        assert_eq!(5, arch.num_pes());
        assert_eq!(vec![0], arch.single_iter(0).collect::<Vec<usize>>());
        assert_eq!(vec![3], arch.single_iter(3).collect::<Vec<usize>>());
        assert_eq!(
            Vec::<usize>::new(),
            arch.single_iter(7).collect::<Vec<usize>>()
        );
        assert_eq!(
            vec![0, 1, 2, 3, 4],
            arch.team_iter().collect::<Vec<usize>>()
        );
    }
    #[test]
    fn sub_arch_stride_2() {
        let garch = Arc::new(LamellarArchRT {
            parent: None,
            arch: LamellarArchEnum::GlobalArch(GlobalArch::new(10)),
            num_pes: 10,
        });

        // let arch = Arc::new(LamellarArchRT {
        //     parent: Some(garch.clone()),
        //     arch: LamellarArchEnum::new(StridedArch::new(0, 2, 5)),
        // });
        let arch = Arc::new(LamellarArchRT::new(
            garch.clone(),
            LamellarArchEnum::new(StridedArch::new(0, 2, 5)),
        ));
        // assert_eq!(0, arch.my_pe());
        assert_eq!(5, arch.num_pes());
        assert_eq!(vec![0], arch.single_iter(0).collect::<Vec<usize>>());
        assert_eq!(vec![4], arch.single_iter(2).collect::<Vec<usize>>());
        assert_eq!(
            Vec::<usize>::new(),
            arch.single_iter(7).collect::<Vec<usize>>()
        );
        assert_eq!(
            vec![0, 2, 4, 6, 8],
            arch.team_iter().collect::<Vec<usize>>()
        );

        // let arch = Arc::new(LamellarArchRT {
        //     parent: Some(garch.clone()),
        //     arch: LamellarArchEnum::new(StridedArch::new(1, 2, 5)),
        // });
        let arch = Arc::new(LamellarArchRT::new(
            garch.clone(),
            LamellarArchEnum::new(StridedArch::new(1, 2, 5)),
        ));
        // assert_eq!(1, arch.my_pe());
        assert_eq!(5, arch.num_pes());
        assert_eq!(vec![1], arch.single_iter(0).collect::<Vec<usize>>());
        assert_eq!(vec![5], arch.single_iter(2).collect::<Vec<usize>>());
        assert_eq!(
            Vec::<usize>::new(),
            arch.single_iter(7).collect::<Vec<usize>>()
        );
        assert_eq!(
            vec![1, 3, 5, 7, 9],
            arch.team_iter().collect::<Vec<usize>>()
        );
    }
    #[test]
    fn sub_arch_stride_3() {
        let garch = Arc::new(LamellarArchRT {
            parent: None,
            arch: LamellarArchEnum::GlobalArch(GlobalArch::new(11)),
            num_pes: 11,
        });
        // let arch = Arc::new(LamellarArchRT {
        //     parent: Some(garch.clone()),
        //     arch: LamellarArchEnum::new(StridedArch::new(0, 3, 4)),
        // });
        let arch = Arc::new(LamellarArchRT::new(
            garch.clone(),
            LamellarArchEnum::new(StridedArch::new(0, 3, 4)),
        ));
        // assert_eq!(0, arch.my_pe());
        assert_eq!(4, arch.num_pes());
        assert_eq!(vec![0], arch.single_iter(0).collect::<Vec<usize>>());
        assert_eq!(vec![6], arch.single_iter(2).collect::<Vec<usize>>());
        assert_eq!(
            Vec::<usize>::new(),
            arch.single_iter(7).collect::<Vec<usize>>()
        );
        assert_eq!(vec![0, 3, 6, 9], arch.team_iter().collect::<Vec<usize>>());

        // let arch = Arc::new(LamellarArchRT {
        //     parent: Some(garch.clone()),
        //     arch: LamellarArchEnum::new(StridedArch::new(1, 3, 4)),
        // });
        let arch = Arc::new(LamellarArchRT::new(
            garch.clone(),
            LamellarArchEnum::new(StridedArch::new(1, 3, 4)),
        ));
        // assert_eq!(1, arch.my_pe());
        assert_eq!(4, arch.num_pes());
        assert_eq!(vec![1], arch.single_iter(0).collect::<Vec<usize>>());
        assert_eq!(vec![7], arch.single_iter(2).collect::<Vec<usize>>());
        assert_eq!(
            Vec::<usize>::new(),
            arch.single_iter(7).collect::<Vec<usize>>()
        );
        assert_eq!(vec![1, 4, 7, 10], arch.team_iter().collect::<Vec<usize>>());

        // let arch = Arc::new(LamellarArchRT {
        //     parent: Some(garch.clone()),
        //     arch: LamellarArchEnum::new(StridedArch::new(2, 3, 3)),
        // });
        let arch = Arc::new(LamellarArchRT::new(
            garch.clone(),
            LamellarArchEnum::new(StridedArch::new(2, 3, 3)),
        ));
        // assert_eq!(1, arch.my_pe());
        assert_eq!(3, arch.num_pes());
        assert_eq!(vec![2], arch.single_iter(0).collect::<Vec<usize>>());
        assert_eq!(vec![8], arch.single_iter(2).collect::<Vec<usize>>());
        assert_eq!(
            Vec::<usize>::new(),
            arch.single_iter(7).collect::<Vec<usize>>()
        );
        assert_eq!(vec![2, 5, 8], arch.team_iter().collect::<Vec<usize>>());
    }

    #[test]
    fn multi_level_sub_arches() {
        let garch = Arc::new(LamellarArchRT {
            parent: None,
            arch: LamellarArchEnum::GlobalArch(GlobalArch::new(20)),
            num_pes: 20,
        });
        // let arch1 = Arc::new(LamellarArchRT {
        //     parent: Some(garch.clone()),
        //     arch: LamellarArchEnum::new(StridedArch::new(0, 2, 10)),
        // });
        let arch1 = Arc::new(LamellarArchRT::new(
            garch.clone(),
            StridedArch::new(0, 2, 10),
        ));
        // let arch1_1 = Arc::new(LamellarArchRT {
        //     parent: Some(arch1.clone()),
        //     arch: LamellarArchEnum::new(StridedArch::new(0, 2, 5)),
        // });
        let arch1_1 = Arc::new(LamellarArchRT::new(
            arch1.clone(),
            StridedArch::new(0, 2, 5),
        ));
        assert_eq!(
            vec![0, 4, 8, 12, 16],
            arch1_1.team_iter().collect::<Vec<usize>>()
        );
        // let arch1_2 = Arc::new(LamellarArchRT {
        //     parent: Some(arch1.clone()),
        //     arch: LamellarArchEnum::new(StridedArch::new(1, 2, 5)),
        // });
        let arch1_2 = Arc::new(LamellarArchRT::new(
            arch1.clone(),
            StridedArch::new(1, 2, 5),
        ));
        assert_eq!(
            vec![2, 6, 10, 14, 18],
            arch1_2.team_iter().collect::<Vec<usize>>()
        );
    }
}
