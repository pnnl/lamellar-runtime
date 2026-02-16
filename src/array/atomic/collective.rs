use crate::{array::{collective::{broadcast_handle::{ArrayCollectiveAllBroadcastHandle, ArrayCollectiveAllBroadcastIntoBufferHandle, ArrayCollectiveBroadcastHandle, ArrayCollectiveBroadcastIntoBufferHandle, ArrayCollectiveScatterHandle, ArrayCollectiveScatterIntoBufferHandle}, gather_handle::{ArrayCollectiveAllGatherHandle, ArrayCollectiveAllGatherIntoBufferHandle, ArrayCollectiveGatherHandle, ArrayCollectiveGatherIntoBufferHandle}, reduce_handle::{ArrayCollectiveAllReduceHandle, ArrayCollectiveAllReduceInPlaceHandle, ArrayCollectiveAllReduceIntoBufferHandle, ArrayCollectiveAllReduceState, ArrayCollectiveReduceHandle, ArrayCollectiveReduceInPlaceHandle, ArrayCollectiveReduceIntoBufferHandle}}, private::LamellarArrayPrivate}, lamellae::collective::{ReduceOp, RootOrLamellarBuffer, RootSrcOrLamellarBuffer}, AsLamellarBuffer, AtomicArray, Dist, LamellarBuffer};

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
                array
                    .array
                    .bit_or_all()
            }
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
         }
    }
}

impl<T: Dist> AtomicArray<T> {
    pub unsafe fn sum_all_into_buffer<B: AsLamellarBuffer<T>>(&self, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .sum_all_into_buffer(buffer)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn max_all_into_buffer<B: AsLamellarBuffer<T>> (&self, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .max_all_into_buffer(buffer)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn min_all_into_buffer<B: AsLamellarBuffer<T>> (&self, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .min_all_into_buffer(buffer)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn prod_all_into_buffer<B: AsLamellarBuffer<T>> (&self, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .prod_all_into_buffer(buffer)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_and_all_into_buffer<B: AsLamellarBuffer<T>> (&self, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .bit_and_all_into_buffer(buffer)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_xor_all_into_buffer<B: AsLamellarBuffer<T>> (&self, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .bit_xor_all_into_buffer(buffer)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_or_all_into_buffer<B: AsLamellarBuffer<T>> (&self, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .bit_or_all_into_buffer(buffer)
            }
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
         }
    }
}

impl<T: Dist> AtomicArray<T> {
    pub unsafe fn sum_all_in_place(&self) -> ArrayCollectiveAllReduceInPlaceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .sum_all_in_place()
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn max_all_in_place(&self) -> ArrayCollectiveAllReduceInPlaceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .max_all_in_place()

            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn min_all_in_place(&self) -> ArrayCollectiveAllReduceInPlaceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .min_all_in_place()
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn prod_all_in_place(&self) -> ArrayCollectiveAllReduceInPlaceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .prod_all_in_place()
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_and_all_in_place(&self) -> ArrayCollectiveAllReduceInPlaceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .bit_and_all_in_place()
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_xor_all_in_place(&self) -> ArrayCollectiveAllReduceInPlaceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .bit_xor_all_in_place()
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_or_all_in_place(&self) -> ArrayCollectiveAllReduceInPlaceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .bit_or_all_in_place()
            }
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
         }
    }
}


impl<T: Dist> AtomicArray<T> {
    pub unsafe fn sum_at_pe(&self, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .sum_at_pe(pe)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn max_at_pe(&self, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .max_at_pe(pe)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn min_at_pe(&self, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .min_at_pe(pe)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn prod_at_pe(&self, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .prod_at_pe(pe)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_and_at_pe(&self, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .bit_and_at_pe(pe)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_xor_at_pe(&self, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .bit_xor_at_pe(pe)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_or_at_pe(&self, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .bit_or_at_pe(pe)
            }
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
         }
    }
}

impl<T: Dist> AtomicArray<T> {
    pub unsafe fn sum_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .sum_at_pe_into_buffer(target)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn max_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .max_at_pe_into_buffer(target)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn min_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .min_at_pe_into_buffer(target)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn prod_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .prod_at_pe_into_buffer(target)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_and_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .bit_and_at_pe_into_buffer(target)

            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_xor_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .bit_xor_at_pe_into_buffer(target)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_or_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .bit_or_at_pe_into_buffer(target)
            }
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
         }
    }
}

impl<T: Dist> AtomicArray<T> {
    pub unsafe fn sum_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .sum_at_pe_in_place(pe)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn max_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .max_at_pe_in_place(pe)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn min_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .min_at_pe_in_place(pe)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn prod_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .prod_at_pe_in_place(pe)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_and_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .bit_and_at_pe_in_place(pe)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_xor_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .bit_xor_at_pe_in_place(pe)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_or_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .bit_or_at_pe_in_place(pe)
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

impl<T: Dist> AtomicArray<T> {
    pub unsafe fn scatter_from_pe(&self, pe: usize) -> ArrayCollectiveScatterHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .scatter_from_pe(pe)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn scatter_from_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, buf: LamellarBuffer<T, B>, root_pe: usize) -> ArrayCollectiveScatterIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .scatter_from_pe_into_buffer(buf, root_pe)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }
}