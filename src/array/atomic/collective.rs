use crate::{array::{collective::{broadcast_handle::{ArrayCollectiveAllBroadcastHandle, ArrayCollectiveAllBroadcastIntoBufferHandle, ArrayCollectiveBroadcastHandle, ArrayCollectiveBroadcastIntoBufferHandle, ArrayCollectiveScatterHandle, ArrayCollectiveScatterIntoBufferHandle}, gather_handle::{ArrayCollectiveAllGatherHandle, ArrayCollectiveAllGatherIntoBufferHandle, ArrayCollectiveGatherHandle, ArrayCollectiveGatherIntoBufferHandle}, reduce_handle::{ArrayCollectiveAllReduceHandle, ArrayCollectiveAllReduceInPlaceHandle, ArrayCollectiveAllReduceIntoBufferHandle, ArrayCollectiveAllReduceState, ArrayCollectiveReduceHandle, ArrayCollectiveReduceInPlaceHandle, ArrayCollectiveReduceIntoBufferHandle}, reduce_scatter_handle::{ArrayCollectiveReduceScatterHandle, ArrayCollectiveReduceScatterIntoBufferHandle}}, private::LamellarArrayPrivate}, lamellae::collective::{BroadcastInput, ReduceOp, RootOrLamellarBuffer, RootSrcOrLamellarBuffer, ScatterInput}, memregion::MemregionRdmaInput, AsLamellarBuffer, AtomicArray, Dist, LamellarBuffer};

impl<T: Dist> AtomicArray<T> {
    pub unsafe fn sum_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .sum_all(index, len)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn max_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .max_all(index, len)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn min_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .min_all(index, len)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn prod_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .prod_all(index, len)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_and_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .bit_and_all(index, len)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_xor_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .bit_or_all(index, len)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_or_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .bit_or_all(index, len)
            }
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
         }
    }
}

impl<T: Dist> AtomicArray<T> {
    pub unsafe fn sum_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .sum_all_into_buffer(index, len, buffer)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn max_all_into_buffer<B: AsLamellarBuffer<T>> (&self, index: usize, len: usize, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .max_all_into_buffer(index, len, buffer)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn min_all_into_buffer<B: AsLamellarBuffer<T>> (&self, index: usize, len: usize, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .min_all_into_buffer(index, len, buffer)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn prod_all_into_buffer<B: AsLamellarBuffer<T>> (&self, index: usize, len: usize, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .prod_all_into_buffer(index, len, buffer)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_and_all_into_buffer<B: AsLamellarBuffer<T>> (&self, index: usize, len: usize, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .bit_and_all_into_buffer(index, len, buffer)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_xor_all_into_buffer<B: AsLamellarBuffer<T>> (&self, index: usize, len: usize, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .bit_xor_all_into_buffer(index, len, buffer)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_or_all_into_buffer<B: AsLamellarBuffer<T>> (&self, index: usize, len: usize, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .bit_or_all_into_buffer(index, len, buffer)
            }
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
         }
    }
}

impl<T: Dist> AtomicArray<T> {
    pub unsafe fn sum_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T,B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .sum_all_in_place(src_and_dst)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn max_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T,B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .max_all_in_place(src_and_dst)

            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn min_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T,B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .min_all_in_place(src_and_dst)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn prod_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T,B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .prod_all_in_place(src_and_dst)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_and_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T,B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .bit_and_all_in_place(src_and_dst)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_xor_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T,B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .bit_xor_all_in_place(src_and_dst)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_or_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T,B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .bit_or_all_in_place(src_and_dst)
            }
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
         }
    }
}


impl<T: Dist> AtomicArray<T> {
    pub unsafe fn sum_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .sum_at_pe(index, len, pe)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn max_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .max_at_pe(index, len, pe)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn min_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .min_at_pe(index, len, pe)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn prod_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .prod_at_pe(index, len, pe)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_and_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .bit_and_at_pe(index, len, pe)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_xor_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .bit_xor_at_pe(index, len, pe)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_or_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .bit_or_at_pe(index, len, pe)
            }
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
         }
    }
}

impl<T: Dist> AtomicArray<T> {
    pub unsafe fn sum_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .sum_at_pe_into_buffer(index, len, target)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn max_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .max_at_pe_into_buffer(index, len, target)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn min_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .min_at_pe_into_buffer(index, len, target)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn prod_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .prod_at_pe_into_buffer(index, len, target)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_and_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .bit_and_at_pe_into_buffer(index, len, target)

            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_xor_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .bit_xor_at_pe_into_buffer(index, len, target)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_or_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .bit_or_at_pe_into_buffer(index, len, target)
            }
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
         }
    }
}

// impl<T: Dist> AtomicArray<T> {
//     pub unsafe fn sum_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         match self {
//             AtomicArray::NetworkAtomicArray(array) => {
//                 array
//                     .array
//                     .sum_at_pe_in_place(pe)
//             },
//             _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
//         }
//     }

//     pub unsafe fn max_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         match self {
//             AtomicArray::NetworkAtomicArray(array) => {
//                 array
//                     .array
//                     .max_at_pe_in_place(pe)
//             },
//             _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
//         }
//     }

//     pub unsafe fn min_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         match self {
//             AtomicArray::NetworkAtomicArray(array) => {
//                 array
//                     .array
//                     .min_at_pe_in_place(pe)
//             },
//             _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
//         }
//     }

//     pub unsafe fn prod_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         match self {
//             AtomicArray::NetworkAtomicArray(array) => {
//                 array
//                     .array
//                     .prod_at_pe_in_place(pe)
//             },
//             _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
//         }
//     }

//     pub unsafe fn bit_and_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         match self {
//             AtomicArray::NetworkAtomicArray(array) => {
//                 array
//                     .array
//                     .bit_and_at_pe_in_place(pe)
//             },
//             _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
//         }
//     }

//     pub unsafe fn bit_xor_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         match self {
//             AtomicArray::NetworkAtomicArray(array) => {
//                 array
//                     .array
//                     .bit_xor_at_pe_in_place(pe)
//             },
//             _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
//         }
//     }

//     pub unsafe fn bit_or_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         match self {
//             AtomicArray::NetworkAtomicArray(array) => {
//                 array
//                     .array
//                     .bit_or_at_pe_in_place(pe)
//             }
//             _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
//          }
//     }
// }


impl<T: Dist> AtomicArray<T> {
    pub unsafe fn gather_all(&self, index: usize, len: usize) -> ArrayCollectiveAllGatherHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .gather_all(index, len)
            }
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
         }
    }

    pub unsafe fn gather_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllGatherIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .gather_all_into_buffer(index, len, buffer)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }
}

impl<T: Dist> AtomicArray<T> {
    pub unsafe fn gather_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveGatherHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .gather_at_pe(index, len, pe)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn gather_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveGatherIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .gather_at_pe_into_buffer(index, len, target)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }
}

impl<T: Dist> AtomicArray<T> {
    pub unsafe fn broadcast_all(&self,  src: impl Into<MemregionRdmaInput<T>>) -> ArrayCollectiveAllBroadcastHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .broadcast_all(src)
            }
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
         }
    }

    pub unsafe fn broadcast_all_into_buffer<B: AsLamellarBuffer<T>>(&self, src: impl Into<MemregionRdmaInput<T>>,  buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllBroadcastIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .broadcast_all_into_buffer(src, buffer)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }
}

impl<T: Dist> AtomicArray<T> {
    pub unsafe fn broadcast_from_pe(&self, src_or_root_pe: BroadcastInput, len: usize) -> ArrayCollectiveBroadcastHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .broadcast_from_pe(src_or_root_pe, len)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn broadcast_from_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, target: RootSrcOrLamellarBuffer<T, B>, len: usize) -> ArrayCollectiveBroadcastIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .broadcast_from_pe_into_buffer(target, len)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }
}

impl<T: Dist> AtomicArray<T> {
    pub unsafe fn scatter_from_pe(&self, src_or_root_pe: ScatterInput, len: usize) -> ArrayCollectiveScatterHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .scatter_from_pe(src_or_root_pe, len)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn scatter_from_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, buf: LamellarBuffer<T, B>, src_or_root_pe: ScatterInput, len: usize) -> ArrayCollectiveScatterIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .scatter_from_pe_into_buffer(buf, src_or_root_pe, len)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }
}


impl<T: Dist> AtomicArray<T> {
    pub unsafe fn sum_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .sum_scatter(index, len)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn max_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .max_scatter(index, len)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn min_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .min_scatter(index, len)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn prod_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .prod_scatter(index, len)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_and_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .bit_and_scatter(index, len)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_xor_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .bit_xor_scatter(index, len)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_or_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .bit_or_scatter(index, len)
            }
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
         }
    }
}

impl<T: Dist> AtomicArray<T> {
    pub unsafe fn sum_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .sum_scatter_into_buffer(index, len, buffer)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn max_scatter_into_buffer<B: AsLamellarBuffer<T>> (&self, index: usize, len: usize, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .max_scatter_into_buffer(index, len, buffer)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn min_scatter_into_buffer<B: AsLamellarBuffer<T>> (&self, index: usize, len: usize, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .min_scatter_into_buffer(index, len, buffer)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn prod_scatter_into_buffer<B: AsLamellarBuffer<T>> (&self, index: usize, len: usize, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .prod_scatter_into_buffer(index, len, buffer)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_and_scatter_into_buffer<B: AsLamellarBuffer<T>> (&self, index: usize, len: usize, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .bit_and_scatter_into_buffer(index, len, buffer)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_xor_scatter_into_buffer<B: AsLamellarBuffer<T>> (&self, index: usize, len: usize, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .bit_xor_scatter_into_buffer(index, len, buffer)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_or_scatter_into_buffer<B: AsLamellarBuffer<T>> (&self, index: usize, len: usize, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .array
                    .bit_or_scatter_into_buffer(index, len, buffer)
            }
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
         }
    }
}