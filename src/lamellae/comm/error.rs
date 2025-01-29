use super::CommAllocAddr;
use crate::lamellae::AllocationType;

#[derive(Debug, Clone)]
pub(crate) enum AllocError {
    OutOfMemoryError(usize),
    IdError(usize),
    LocalNotFound(CommAllocAddr),
    RemoteNotFound(CommAllocAddr),
    UnexpectedAllocationType(AllocationType),
    FabricAllocationError(i32),
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
            AllocError::RemoteNotFound(addr) => {
                write!(
                    f,
                    "Allocation not found remotely for given address {:x}",
                    addr
                )
            }
            AllocError::UnexpectedAllocationType(alloc_type) => {
                write!(f, "Unexpected allocation type {:?}", alloc_type)
            }
            AllocError::FabricAllocationError(err_no) => {
                write!(f, "Fabric allocation error: {:?}", err_no)
            }
        }
    }
}

impl std::error::Error for AllocError {}

pub(crate) type AllocResult<T> = Result<T, AllocError>;

#[derive(Debug, Clone, Copy)]
pub(crate) enum RdmaError {
    FabricPutError(i32),
    FabricGetError(i32),
}

pub(crate) type RdmaResult = Result<(), RdmaError>;

impl std::fmt::Display for RdmaError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            RdmaError::FabricPutError(err_no) => {
                write!(f, "Fabric put error: {}", err_no)
            }
            RdmaError::FabricGetError(err_no) => {
                write!(f, "Fabric get error: {}", err_no)
            }
        }
    }
}

impl std::error::Error for RdmaError {}

#[cfg(feature = "rofi-c")]
#[derive(Debug, Clone, Copy)]
pub(crate) enum TxError {
    GetError,
}
#[cfg(feature = "rofi-c")]
impl std::fmt::Display for TxError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            TxError::GetError => {
                write!(f, "error performing get")
            }
        }
    }
}
#[cfg(feature = "rofi-c")]
impl std::error::Error for TxError {}
#[cfg(feature = "rofi-c")]
pub(crate) type TxResult<T> = Result<T, TxError>;
