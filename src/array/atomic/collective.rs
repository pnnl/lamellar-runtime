use crate::{array::{collective::reduce_handle::{ArrayCollectiveAllReduceHandle, ArrayCollectiveAllReduceInPlaceHandle, ArrayCollectiveAllReduceInPlaceState, ArrayCollectiveAllReduceIntoBufferHandle, ArrayCollectiveAllReduceIntoBufferState, ArrayCollectiveAllReduceState}, private::LamellarArrayPrivate}, lamellae::collective::ReduceOp, AsLamellarBuffer, AtomicArray, Dist, LamellarBuffer};

impl<T: Dist> AtomicArray<T> {
    pub unsafe fn sum_all(&self) -> ArrayCollectiveAllReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce_all(ReduceOp::Sum);

                ArrayCollectiveAllReduceHandle {
                    array: array.as_lamellar_byte_array(),
                    state: ArrayCollectiveAllReduceState::CollectiveAllReduce(req),
                    spawned: false,
                }
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn max_all(&self) -> ArrayCollectiveAllReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce_all(ReduceOp::Max);

                ArrayCollectiveAllReduceHandle {
                    array: self.as_lamellar_byte_array(),
                    state: ArrayCollectiveAllReduceState::CollectiveAllReduce(req),
                    spawned: false,
                }
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn min_all(&self) -> ArrayCollectiveAllReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce_all(ReduceOp::Min);

                ArrayCollectiveAllReduceHandle {
                    array: array.as_lamellar_byte_array(),
                    state: ArrayCollectiveAllReduceState::CollectiveAllReduce(req),
                    spawned: false,
                }
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn prod_all(&self) -> ArrayCollectiveAllReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce_all(ReduceOp::Prod);

                ArrayCollectiveAllReduceHandle {
                    array: array.as_lamellar_byte_array(),
                    state: ArrayCollectiveAllReduceState::CollectiveAllReduce(req),
                    spawned: false,
                }
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_and_all(&self) -> ArrayCollectiveAllReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce_all(ReduceOp::BitAnd);

                ArrayCollectiveAllReduceHandle {
                    array: self.as_lamellar_byte_array(),
                    state: ArrayCollectiveAllReduceState::CollectiveAllReduce(req),
                    spawned: false,
                }
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_xor_all(&self) -> ArrayCollectiveAllReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce_all(ReduceOp::BitXor);

                ArrayCollectiveAllReduceHandle {
                    array: self.as_lamellar_byte_array(),
                    state: ArrayCollectiveAllReduceState::CollectiveAllReduce(req),
                    spawned: false,
                }
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_or_all(&self) -> ArrayCollectiveAllReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce_all(ReduceOp::BitOr);

                ArrayCollectiveAllReduceHandle {
                    array: self.as_lamellar_byte_array(),
                    state: ArrayCollectiveAllReduceState::CollectiveAllReduce(req),
                    spawned: false,
                }
            }
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
         }
    }
}

impl<T: Dist> AtomicArray<T> {
    pub unsafe fn sum_all_into_buffer<B: AsLamellarBuffer<T>>(&self, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce_all_into_buffer(ReduceOp::Sum, buffer);

                ArrayCollectiveAllReduceIntoBufferHandle {
                    array: array.as_lamellar_byte_array(),
                    state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req),
                    spawned: false,
                }
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn max_all_into_buffer<B: AsLamellarBuffer<T>> (&self, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce_all_into_buffer(ReduceOp::Max, buffer);

                ArrayCollectiveAllReduceIntoBufferHandle {
                    array: self.as_lamellar_byte_array(),
                    state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req),
                    spawned: false,
                }
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn min_all_into_buffer<B: AsLamellarBuffer<T>> (&self, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce_all_into_buffer(ReduceOp::Min, buffer);

                ArrayCollectiveAllReduceIntoBufferHandle {
                    array: array.as_lamellar_byte_array(),
                    state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req),
                    spawned: false,
                }
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn prod_all_into_buffer<B: AsLamellarBuffer<T>> (&self, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce_all_into_buffer(ReduceOp::Prod, buffer);

                ArrayCollectiveAllReduceIntoBufferHandle {
                    array: array.as_lamellar_byte_array(),
                    state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req),
                    spawned: false,
                }
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_and_all_into_buffer<B: AsLamellarBuffer<T>> (&self, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce_all_into_buffer(ReduceOp::BitAnd, buffer);

                ArrayCollectiveAllReduceIntoBufferHandle {
                    array: self.as_lamellar_byte_array(),
                    state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req),
                    spawned: false,
                }
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_xor_all_into_buffer<B: AsLamellarBuffer<T>> (&self, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce_all_into_buffer(ReduceOp::BitXor, buffer);

                ArrayCollectiveAllReduceIntoBufferHandle {
                    array: self.as_lamellar_byte_array(),
                    state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req),
                    spawned: false,
                }
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_or_all_into_buffer<B: AsLamellarBuffer<T>> (&self, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce_all_into_buffer(ReduceOp::BitOr, buffer);

                ArrayCollectiveAllReduceIntoBufferHandle {
                    array: self.as_lamellar_byte_array(),
                    state: ArrayCollectiveAllReduceIntoBufferState::CollectiveAllReduceIntoBuffer(req),
                    spawned: false,
                }
            }
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
         }
    }
}

impl<T: Dist> AtomicArray<T> {
    pub unsafe fn sum_all_in_place(&self) -> ArrayCollectiveAllReduceInPlaceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce_all_in_place(ReduceOp::Sum);

                ArrayCollectiveAllReduceInPlaceHandle {
                    array: array.as_lamellar_byte_array(),
                    state: ArrayCollectiveAllReduceInPlaceState::CollectiveAllReduceInPlace(req),
                    spawned: false,
                }
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn max_all_in_place(&self) -> ArrayCollectiveAllReduceInPlaceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce_all_in_place(ReduceOp::Max);

                ArrayCollectiveAllReduceInPlaceHandle {
                    array: self.as_lamellar_byte_array(),
                    state: ArrayCollectiveAllReduceInPlaceState::CollectiveAllReduceInPlace(req),
                    spawned: false,
                }
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn min_all_in_place(&self) -> ArrayCollectiveAllReduceInPlaceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce_all_in_place(ReduceOp::Min);

                ArrayCollectiveAllReduceInPlaceHandle {
                    array: array.as_lamellar_byte_array(),
                    state: ArrayCollectiveAllReduceInPlaceState::CollectiveAllReduceInPlace(req),
                    spawned: false,
                }
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn prod_all_in_place(&self) -> ArrayCollectiveAllReduceInPlaceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce_all_in_place(ReduceOp::Prod);

                ArrayCollectiveAllReduceInPlaceHandle {
                    array: array.as_lamellar_byte_array(),
                    state: ArrayCollectiveAllReduceInPlaceState::CollectiveAllReduceInPlace(req),
                    spawned: false,
                }
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_and_all_in_place(&self) -> ArrayCollectiveAllReduceInPlaceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce_all_in_place(ReduceOp::BitAnd);

                ArrayCollectiveAllReduceInPlaceHandle {
                    array: self.as_lamellar_byte_array(),
                    state: ArrayCollectiveAllReduceInPlaceState::CollectiveAllReduceInPlace(req),
                    spawned: false,
                }
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_xor_all_in_place(&self) -> ArrayCollectiveAllReduceInPlaceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce_all_in_place(ReduceOp::BitXor);

                ArrayCollectiveAllReduceInPlaceHandle {
                    array: self.as_lamellar_byte_array(),
                    state: ArrayCollectiveAllReduceInPlaceState::CollectiveAllReduceInPlace(req),
                    spawned: false,
                }
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_or_all_in_place(&self) -> ArrayCollectiveAllReduceInPlaceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce_all_in_place(ReduceOp::BitOr);

                ArrayCollectiveAllReduceInPlaceHandle {
                    array: self.as_lamellar_byte_array(),
                    state: ArrayCollectiveAllReduceInPlaceState::CollectiveAllReduceInPlace(req),
                    spawned: false,
                }
            }
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
         }
    }
}