#[derive(Debug, Clone, Copy)]
pub(crate) enum AllocError {
    OutOfMemoryError(usize),
    IdError(usize),
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
        }
    }
}

impl std::error::Error for AllocError {}

pub(crate) type AllocResult<T> = Result<T, AllocError>;

#[derive(Debug, Clone, Copy)]
pub struct RdmaError {}

pub(crate) type RdmaResult = Result<(), RdmaError>;

impl std::fmt::Display for RdmaError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "RDMA Error")
    }
}

impl std::error::Error for RdmaError {}

#[cfg(feature = "rofi")]
#[derive(Debug, Clone, Copy)]
pub(crate) enum TxError {
    GetError,
}
#[cfg(feature = "rofi")]
impl std::fmt::Display for TxError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            TxError::GetError => {
                write!(f, "error performing get")
            }
        }
    }
}
#[cfg(feature = "rofi")]
impl std::error::Error for TxError {}
#[cfg(feature = "rofi")]
pub(crate) type TxResult<T> = Result<T, TxError>;
