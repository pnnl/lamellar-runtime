use super::CommAllocAddr;
// use crate::lamellae::AllocationType;

#[derive(Debug, Clone)]
pub(crate) enum AllocError {
    OutOfMemoryError(usize),
    IdError(usize),
    LocalNotFound(CommAllocAddr),
    // RemoteNotFound(CommAllocAddr),
    #[cfg(any(feature = "enable-libfabric", feature = "enable-libfabric-mt", feature = "enable-libfabric-async",feature="enable-rofi-c"))]
    UnexpectedAllocationType( crate::lamellae::AllocationType),
    #[cfg(any(feature = "enable-libfabric", feature = "enable-libfabric-mt", feature = "enable-libfabric-async",feature="enable-rofi-c"))]
    FabricAllocationError(i32),
    InvalidSubAlloc(usize, usize),
    // NotRTAlloc(usize),
}

impl std::fmt::Display for AllocError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            AllocError::OutOfMemoryError(size) => {
                write!(f, "not enough memory for to allocate {} bytes", size)
            }
            AllocError::IdError(pe) => {
                write!(f, "pe {} must be part of team of sub allocation", pe)
            }
            AllocError::LocalNotFound(addr) => {
                write!(
                    f,
                    "Allocation not found locally for given address {:x}",
                    addr
                )
            }
            // AllocError::RemoteNotFound(addr) => {
            //     write!(
            //         f,
            //         "Allocation not found remotely for given address {:x}",
            //         addr
            //     )
            // }
             #[cfg(any(feature = "enable-libfabric", feature = "enable-libfabric-mt", feature = "enable-libfabric-async",feature="enable-rofi-c"))]
            AllocError::UnexpectedAllocationType(alloc_type) => {
                write!(f, "Unexpected allocation type {:?}", alloc_type)
            }
             #[cfg(any(feature = "enable-libfabric", feature = "enable-libfabric-mt", feature = "enable-libfabric-async",feature="enable-rofi-c"))]
            AllocError::FabricAllocationError(err_no) => {
                write!(f, "Fabric allocation error: {:?}", err_no)
            }
            AllocError::InvalidSubAlloc(size, align) => {
                write!(
                    f,
                    "Invalid sub allocation size {} with alignment {}",
                    size, align
                )
            } // AllocError::NotRTAlloc(addr) => {
              //     write!(f, "Address {:x} is not part of the runtime heap", addr)
              // }
        }
    }
}

impl std::error::Error for AllocError {}

pub(crate) type AllocResult<T> = Result<T, AllocError>;


#[cfg(any(feature = "enable-libfabric", feature = "enable-libfabric-mt", feature = "enable-libfabric-async",feature="enable-rofi-c", feature = "enable-ucx", feature = "enable-ucx-mt"))]
#[derive(Debug, Clone, Copy)]
pub(crate) enum FabricError {
    InitError(u32),
    #[cfg(feature = "enable-libfabric-async")]
    BarrierError(u32),
    // FabricError(u32),
}

#[cfg(any(feature = "enable-libfabric", feature = "enable-libfabric-mt", feature = "enable-libfabric-async",feature="enable-rofi-c", feature = "enable-ucx", feature = "enable-ucx-mt"))]

impl std::fmt::Display for FabricError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            FabricError::InitError(err_no) => {
                write!(f, "Fabric initialization error: {}", err_no)
            }
            #[cfg(feature = "enable-libfabric-async")]
            FabricError::BarrierError(err_no) => {
                write!(f, "Barrier error: {}", err_no)
            } // FabricError::FabricError(err_no) => {
              //     write!(f, "Fabric error: {}", err_no)
              // }
        }
    }
}

#[cfg(any(feature = "enable-libfabric", feature = "enable-libfabric-mt", feature = "enable-libfabric-async",feature="enable-rofi-c", feature = "enable-ucx", feature = "enable-ucx-mt"))]

impl std::error::Error for FabricError {}

#[cfg(any(feature = "enable-libfabric", feature = "enable-libfabric-mt", feature = "enable-libfabric-async",feature="enable-rofi-c"))]
pub(crate) type FabricResult<T> = Result<T, FabricError>;

#[cfg(feature="enable-rofi-c")]
#[derive(Debug, Clone, Copy)]
pub(crate) enum RdmaError {
    FabricPutError(i32),
    FabricGetError(i32),
    #[allow(dead_code)]
    FabricWaitError(i32),
}

#[cfg(feature="enable-rofi-c")]
pub(crate) type RdmaResult = Result<(), RdmaError>;

#[cfg(feature="enable-rofi-c")]
impl std::fmt::Display for RdmaError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            RdmaError::FabricPutError(err_no) => write!(f, "Fabric put error: {}", err_no),
            RdmaError::FabricGetError(err_no) => write!(f, "Fabric get error: {}", err_no),
            RdmaError::FabricWaitError(err_no) => write!(f, "Fabric wait error: {}", err_no),
        }
    }
}

#[cfg(feature="enable-rofi-c")]
impl std::error::Error for RdmaError {}

// #[cfg(feature = "rofi-c")]
// #[derive(Debug, Clone, Copy)]
// pub(crate) enum TxError {
//     GetError,
// }
// #[cfg(feature = "rofi-c")]
// impl std::fmt::Display for TxError {
//     fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
//         match self {
//             TxError::GetError => {
//                 write!(f, "error performing get")
//             }
//         }
//     }
// }
// #[cfg(feature = "rofi-c")]
// impl std::error::Error for TxError {}
// #[cfg(feature = "rofi-c")]
// pub(crate) type TxResult<T> = Result<T, TxError>;
