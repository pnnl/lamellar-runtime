use crate::{array::{collective::{broadcast_handle::{ArrayCollectiveAllBroadcastHandle, ArrayCollectiveAllBroadcastIntoBufferHandle, ArrayCollectiveAllBroadcastIntoBufferState, ArrayCollectiveAllBroadcastState, ArrayCollectiveBroadcastHandle, ArrayCollectiveBroadcastIntoBufferHandle}, gather_handle::{ArrayCollectiveAllGatherHandle, ArrayCollectiveAllGatherIntoBufferHandle, ArrayCollectiveAllGatherIntoBufferState, ArrayCollectiveAllGatherState, ArrayCollectiveGatherHandle, ArrayCollectiveGatherIntoBufferHandle, ArrayCollectiveGatherIntoBufferState, ArrayCollectiveGatherState}, reduce_handle::{ArrayCollectiveAllReduceHandle, ArrayCollectiveAllReduceInPlaceHandle, ArrayCollectiveAllReduceInPlaceState, ArrayCollectiveAllReduceIntoBufferHandle, ArrayCollectiveAllReduceIntoBufferState, ArrayCollectiveAllReduceState, ArrayCollectiveReduceHandle, ArrayCollectiveReduceInPlaceHandle, ArrayCollectiveReduceInPlaceState, ArrayCollectiveReduceIntoBufferHandle, ArrayCollectiveReduceIntoBufferState, ArrayCollectiveReduceState}}, private::LamellarArrayPrivate}, lamellae::collective::{ReduceOp, RootOrBuffer, RootOrLamellarBuffer, RootSrcOrLamellarBuffer}, AsLamellarBuffer, AtomicArray, Dist, LamellarBuffer};

impl<T: Dist> AtomicArray<T> {
    pub unsafe fn sum_all(&self) -> ArrayCollectiveAllReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .sum_all()
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn max_all(&self) -> ArrayCollectiveAllReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .max_all()
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn min_all(&self) -> ArrayCollectiveAllReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .min_all()
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn prod_all(&self) -> ArrayCollectiveAllReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .prod_all()
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_and_all(&self) -> ArrayCollectiveAllReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .bit_and_all()
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


impl<T: Dist> AtomicArray<T> {
    pub unsafe fn sum_at_pe(&self, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce(ReduceOp::Sum, pe);

                ArrayCollectiveReduceHandle {
                    array: array.as_lamellar_byte_array(),
                    state: ArrayCollectiveReduceState::CollectiveReduce(req),
                    spawned: false,
                }
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn max_at_pe(&self, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce(ReduceOp::Max, pe);

                ArrayCollectiveReduceHandle {
                    array: self.as_lamellar_byte_array(),
                    state: ArrayCollectiveReduceState::CollectiveReduce(req),
                    spawned: false,
                }
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn min_at_pe(&self, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce(ReduceOp::Min, pe);

                ArrayCollectiveReduceHandle {
                    array: array.as_lamellar_byte_array(),
                    state: ArrayCollectiveReduceState::CollectiveReduce(req),
                    spawned: false,
                }
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn prod_at_pe(&self, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce(ReduceOp::Prod, pe);

                ArrayCollectiveReduceHandle {
                    array: array.as_lamellar_byte_array(),
                    state: ArrayCollectiveReduceState::CollectiveReduce(req),
                    spawned: false,
                }
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_and_at_pe(&self, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce(ReduceOp::BitAnd, pe);

                ArrayCollectiveReduceHandle {
                    array: self.as_lamellar_byte_array(),
                    state: ArrayCollectiveReduceState::CollectiveReduce(req),
                    spawned: false,
                }
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_xor_at_pe(&self, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce(ReduceOp::BitXor, pe);

                ArrayCollectiveReduceHandle {
                    array: self.as_lamellar_byte_array(),
                    state: ArrayCollectiveReduceState::CollectiveReduce(req),
                    spawned: false,
                }
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_or_at_pe(&self, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce(ReduceOp::BitOr, pe);

                ArrayCollectiveReduceHandle {
                    array: self.as_lamellar_byte_array(),
                    state: ArrayCollectiveReduceState::CollectiveReduce(req),
                    spawned: false,
                }
            }
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
         }
    }
}

impl<T: Dist> AtomicArray<T> {
    pub unsafe fn sum_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce_into_buffer(ReduceOp::Sum, target);

                ArrayCollectiveReduceIntoBufferHandle {
                    array: array.as_lamellar_byte_array(),
                    state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req),
                    spawned: false,
                }
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn max_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce_into_buffer(ReduceOp::Max, target);

                ArrayCollectiveReduceIntoBufferHandle {
                    array: self.as_lamellar_byte_array(),
                    state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req),
                    spawned: false,
                }
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn min_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce_into_buffer(ReduceOp::Min, target);

                ArrayCollectiveReduceIntoBufferHandle {
                    array: array.as_lamellar_byte_array(),
                    state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req),
                    spawned: false,
                }
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn prod_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce_into_buffer(ReduceOp::Prod, target);

                ArrayCollectiveReduceIntoBufferHandle {
                    array: array.as_lamellar_byte_array(),
                    state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req),
                    spawned: false,
                }
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_and_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce_into_buffer(ReduceOp::BitAnd, target);

                ArrayCollectiveReduceIntoBufferHandle {
                    array: self.as_lamellar_byte_array(),
                    state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req),
                    spawned: false,
                }
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_xor_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce_into_buffer(ReduceOp::BitXor, target);

                ArrayCollectiveReduceIntoBufferHandle {
                    array: self.as_lamellar_byte_array(),
                    state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req),
                    spawned: false,
                }
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_or_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce_into_buffer(ReduceOp::BitOr, target);

                ArrayCollectiveReduceIntoBufferHandle {
                    array: self.as_lamellar_byte_array(),
                    state: ArrayCollectiveReduceIntoBufferState::CollectiveReduceIntoBuffer(req),
                    spawned: false,
                }
            }
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
         }
    }
}

impl<T: Dist> AtomicArray<T> {
    pub unsafe fn sum_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce_in_place(ReduceOp::Sum, pe);

                ArrayCollectiveReduceInPlaceHandle {
                    array: array.as_lamellar_byte_array(),
                    state: ArrayCollectiveReduceInPlaceState::CollectiveReduceInPlace(req),
                    spawned: false,
                }
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn max_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce_in_place(ReduceOp::Max, pe);

                ArrayCollectiveReduceInPlaceHandle {
                    array: self.as_lamellar_byte_array(),
                    state: ArrayCollectiveReduceInPlaceState::CollectiveReduceInPlace(req),
                    spawned: false,
                }
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn min_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce_in_place(ReduceOp::Min, pe);

                ArrayCollectiveReduceInPlaceHandle {
                    array: array.as_lamellar_byte_array(),
                    state: ArrayCollectiveReduceInPlaceState::CollectiveReduceInPlace(req),
                    spawned: false,
                }
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn prod_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce_in_place(ReduceOp::Prod, pe);

                ArrayCollectiveReduceInPlaceHandle {
                    array: array.as_lamellar_byte_array(),
                    state: ArrayCollectiveReduceInPlaceState::CollectiveReduceInPlace(req),
                    spawned: false,
                }
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_and_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce_in_place(ReduceOp::BitAnd, pe);

                ArrayCollectiveReduceInPlaceHandle {
                    array: self.as_lamellar_byte_array(),
                    state: ArrayCollectiveReduceInPlaceState::CollectiveReduceInPlace(req),
                    spawned: false,
                }
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_xor_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce_in_place(ReduceOp::BitXor, pe);

                ArrayCollectiveReduceInPlaceHandle {
                    array: self.as_lamellar_byte_array(),
                    state: ArrayCollectiveReduceInPlaceState::CollectiveReduceInPlace(req),
                    spawned: false,
                }
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_or_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                let req = array
                    .array
                    .inner
                    .data
                    .mem_region
                    .as_base::<T>()
                    .reduce_in_place(ReduceOp::BitOr, pe);

                ArrayCollectiveReduceInPlaceHandle {
                    array: self.as_lamellar_byte_array(),
                    state: ArrayCollectiveReduceInPlaceState::CollectiveReduceInPlace(req),
                    spawned: false,
                }
            }
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
         }
    }
}


impl<T: Dist> AtomicArray<T> {
    pub unsafe fn gather_all(&self) -> ArrayCollectiveAllGatherHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .gather_all()
            }
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
         }
    }

    pub unsafe fn gather_all_into_buffer<B: AsLamellarBuffer<T>>(&self, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllGatherIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .gather_all_into_buffer(buffer)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }
}

impl<T: Dist> AtomicArray<T> {
    pub unsafe fn gather_at_pe(&self, pe: usize) -> ArrayCollectiveGatherHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .gather_at_pe(pe)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn gather_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveGatherIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .gather_at_pe_into_buffer(target)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }
}

impl<T: Dist> AtomicArray<T> {
    pub unsafe fn broadcast_all(&self) -> ArrayCollectiveAllBroadcastHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .broadcast_all()
            }
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
         }
    }

    pub unsafe fn broadcast_all_into_buffer<B: AsLamellarBuffer<T>>(&self, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllBroadcastIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .broadcast_all_into_buffer(buffer)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }
}

impl<T: Dist> AtomicArray<T> {
    pub unsafe fn broadcast_from_pe(&self, pe: usize) -> ArrayCollectiveBroadcastHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .broadcast_from_pe(pe)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn broadcast_from_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, target: RootSrcOrLamellarBuffer<T, B>) -> ArrayCollectiveBroadcastIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .broadcast_from_pe_into_buffer(target)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }
}