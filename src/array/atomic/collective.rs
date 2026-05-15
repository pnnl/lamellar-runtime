use crate::{AsLamellarBuffer, AtomicArray, Dist, ElementArithmeticOps, ElementBitWiseOps, LamellarBuffer, array::{collective::{broadcast_handle::{ArrayCollectiveAllToAllHandle, ArrayCollectiveAllToAllIntoBufferHandle, ArrayCollectiveBroadcastHandle, ArrayCollectiveBroadcastIntoBufferHandle, ArrayCollectiveScatterHandle, ArrayCollectiveScatterIntoBufferHandle}, gather_handle::{ArrayCollectiveAllGatherHandle, ArrayCollectiveAllGatherIntoBufferHandle, ArrayCollectiveGatherHandle, ArrayCollectiveGatherIntoBufferHandle}, reduce_handle::{ArrayCollectiveAllReduceHandle, ArrayCollectiveAllReduceInPlaceHandle, ArrayCollectiveAllReduceIntoBufferHandle, ArrayCollectiveReduceHandle, ArrayCollectiveReduceIntoBufferHandle}, reduce_scatter_handle::{ArrayCollectiveReduceScatterHandle, ArrayCollectiveReduceScatterIntoBufferHandle}}}, lamellae::collective::{BroadcastInput, RootOrLamellarBuffer, RootSrcOrLamellarBuffer, ScatterInput}};

impl<T: ElementArithmeticOps> AtomicArray<T> {
    pub unsafe fn sum_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .sum_all(index, len)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .sum_all(index, len)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .sum_all(index, len)
            },
        }
    }

    pub unsafe fn max_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .max_all(index, len)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .max_all(index, len)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .max_all(index, len)
            },
        }
    }

    pub unsafe fn min_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .min_all(index, len)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .min_all(index, len)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .min_all(index, len)
            },
        }
    }

    pub unsafe fn prod_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .prod_all(index, len)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .prod_all(index, len)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .prod_all(index, len)
            },
        }
    }
}
impl<T: ElementBitWiseOps> AtomicArray<T> {
    pub unsafe fn bit_and_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .bit_and_all(index, len)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .bit_and_all(index, len)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .bit_and_all(index, len)
            },
        }
    }

    pub unsafe fn bit_xor_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .bit_xor_all(index, len)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .bit_xor_all(index, len)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .bit_xor_all(index, len)
            },
        }
    }

    pub unsafe fn bit_or_all(&self, index: usize, len: usize) -> ArrayCollectiveAllReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .bit_or_all(index, len)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .bit_or_all(index, len)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .bit_or_all(index, len)
            },
         }
    }
}

impl<T: ElementArithmeticOps> AtomicArray<T> {
    pub unsafe fn sum_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .sum_all_into_buffer(index, len, buffer)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .sum_all_into_buffer(index, len, buffer)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .sum_all_into_buffer(index, len, buffer)
            },
        }
    }

    pub unsafe fn max_all_into_buffer<B: AsLamellarBuffer<T>> (&self, index: usize, len: usize, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .max_all_into_buffer(index, len, buffer)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .max_all_into_buffer(index, len, buffer)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .max_all_into_buffer(index, len, buffer)
            },
        }
    }

    pub unsafe fn min_all_into_buffer<B: AsLamellarBuffer<T>> (&self, index: usize, len: usize, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .min_all_into_buffer(index, len, buffer)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .min_all_into_buffer(index, len, buffer)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .min_all_into_buffer(index, len, buffer)
            },
        }
    }

    pub unsafe fn prod_all_into_buffer<B: AsLamellarBuffer<T>> (&self, index: usize, len: usize, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .prod_all_into_buffer(index, len, buffer)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .prod_all_into_buffer(index, len, buffer)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .prod_all_into_buffer(index, len, buffer)
            },
        }
    }
}

impl<T: ElementBitWiseOps> AtomicArray<T> {
     
    pub unsafe fn bit_and_all_into_buffer<B: AsLamellarBuffer<T>> (&self, index: usize, len: usize, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .bit_and_all_into_buffer(index, len, buffer)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .bit_and_all_into_buffer(index, len, buffer)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .bit_and_all_into_buffer(index, len, buffer)
            },
        }
    }

    pub unsafe fn bit_xor_all_into_buffer<B: AsLamellarBuffer<T>> (&self, index: usize, len: usize, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .bit_xor_all_into_buffer(index, len, buffer)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .bit_xor_all_into_buffer(index, len, buffer)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .bit_xor_all_into_buffer(index, len, buffer)
            },
        }
    }

    pub unsafe fn bit_or_all_into_buffer<B: AsLamellarBuffer<T>> (&self, index: usize, len: usize, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveAllReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .bit_or_all_into_buffer(index, len, buffer)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .bit_or_all_into_buffer(index, len, buffer)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .bit_or_all_into_buffer(index, len, buffer)
            },
        }
    }
}

impl<T: ElementArithmeticOps> AtomicArray<T> {
    pub unsafe fn sum_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T,B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .sum_all_in_place(src_and_dst)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}

            // AtomicArray::NativeAtomicArray(array) => {
            //     array
            //         .sum_all_in_place(src_and_dst)
            // },
            // AtomicArray::GenericAtomicArray(array) => {
            //     array
            //         .sum_all_in_place(src_and_dst)
            // },
        }
    }

    pub unsafe fn max_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T,B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .max_all_in_place(src_and_dst)

            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
            // AtomicArray::NativeAtomicArray(array) => {
            //     array
            //         .max_all_in_place(src_and_dst)
            // },
            // AtomicArray::GenericAtomicArray(array) => {
            //     array
            //         .max_all_in_place(src_and_dst)
            // },
        }
    }

    pub unsafe fn min_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T,B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .min_all_in_place(src_and_dst)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
            // AtomicArray::NativeAtomicArray(array) => {
            //     array
            //         .min_all_in_place(src_and_dst)
            // },
            // AtomicArray::GenericAtomicArray(array) => {
            //     array
            //         .min_all_in_place(src_and_dst)
            // },
        }
    }

    pub unsafe fn prod_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T,B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .prod_all_in_place(src_and_dst)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}

            // AtomicArray::NativeAtomicArray(array) => {
            //     array
            //         .prod_all_in_place(src_and_dst)
            // },
            // AtomicArray::GenericAtomicArray(array) => {
            //     array
            //         .prod_all_in_place(src_and_dst)
            // },
        }
    }
}

impl<T: ElementBitWiseOps> AtomicArray<T> {

    pub unsafe fn bit_and_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T,B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .bit_and_all_in_place(src_and_dst)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_xor_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T,B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .bit_xor_all_in_place(src_and_dst)
            },
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
        }
    }

    pub unsafe fn bit_or_all_in_place<B: AsLamellarBuffer<T>>(&self, src_and_dst: LamellarBuffer<T,B>) -> ArrayCollectiveAllReduceInPlaceHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .bit_or_all_in_place(src_and_dst)
            }
            _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
         }
    }
}


impl<T: ElementArithmeticOps> AtomicArray<T> {
    pub unsafe fn sum_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .sum_at_pe(index, len, pe)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .sum_at_pe(index, len, pe)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .sum_at_pe(index, len, pe)
            },
        }
    }

    pub unsafe fn max_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .max_at_pe(index, len, pe)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .max_at_pe(index, len, pe)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .max_at_pe(index, len, pe)
            },
        }
    }

    pub unsafe fn min_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .min_at_pe(index, len, pe)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .min_at_pe(index, len, pe)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .min_at_pe(index, len, pe)
            },
        }
    }

    pub unsafe fn prod_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .prod_at_pe(index, len, pe)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .prod_at_pe(index, len, pe)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .prod_at_pe(index, len, pe)
            },
        }
    }
}

impl<T: ElementBitWiseOps> AtomicArray<T> {
    pub unsafe fn bit_and_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .bit_and_at_pe(index, len, pe)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .bit_and_at_pe(index, len, pe)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .bit_and_at_pe(index, len, pe)
            },
        }
    }

    pub unsafe fn bit_xor_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .bit_xor_at_pe(index, len, pe)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .bit_xor_at_pe(index, len, pe)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .bit_xor_at_pe(index, len, pe)
            },
        }
    }

    pub unsafe fn bit_or_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveReduceHandle<T> {

        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .bit_or_at_pe(index, len, pe)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .bit_or_at_pe(index, len, pe)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .bit_or_at_pe(index, len, pe)
            },
        }
    }
}

impl<T: ElementArithmeticOps> AtomicArray<T> {
    pub unsafe fn sum_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .sum_at_pe_into_buffer(index, len, target)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .sum_at_pe_into_buffer(index, len, target)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .sum_at_pe_into_buffer(index, len, target)
            },
        }
    }

    pub unsafe fn max_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .max_at_pe_into_buffer(index, len, target)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .max_at_pe_into_buffer(index, len, target)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .max_at_pe_into_buffer(index, len, target)
            },
        }
    }

    pub unsafe fn min_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .min_at_pe_into_buffer(index, len, target)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .min_at_pe_into_buffer(index, len, target)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .min_at_pe_into_buffer(index, len, target)
            },
        }
    }

    pub unsafe fn prod_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .prod_at_pe_into_buffer(index, len, target)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .prod_at_pe_into_buffer(index, len, target)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .prod_at_pe_into_buffer(index, len, target)
            },
        }
    }
}

impl<T: ElementBitWiseOps> AtomicArray<T> {
     
    pub unsafe fn bit_and_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .bit_and_at_pe_into_buffer(index, len, target)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .bit_and_at_pe_into_buffer(index, len, target)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .bit_and_at_pe_into_buffer(index, len, target)
            },
        }
    }

    pub unsafe fn bit_xor_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .bit_xor_at_pe_into_buffer(index, len, target)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .bit_xor_at_pe_into_buffer(index, len, target)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .bit_xor_at_pe_into_buffer(index, len, target)
            },
        }
    }

    pub unsafe fn bit_or_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveReduceIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .bit_or_at_pe_into_buffer(index, len, target)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .bit_or_at_pe_into_buffer(index, len, target)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .bit_or_at_pe_into_buffer(index, len, target)
            },
        }
    }
}

// impl<T: Dist> AtomicArray<T> {
//     pub unsafe fn sum_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         match self {
//             AtomicArray::NetworkAtomicArray(array) => {
//                 array
//                     .sum_at_pe_in_place(pe)
//             },
//             _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
//         }
//     }

//     pub unsafe fn max_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         match self {
//             AtomicArray::NetworkAtomicArray(array) => {
//                 array
//                     .max_at_pe_in_place(pe)
//             },
//             _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
//         }
//     }

//     pub unsafe fn min_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         match self {
//             AtomicArray::NetworkAtomicArray(array) => {
//                 array
//                     .min_at_pe_in_place(pe)
//             },
//             _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
//         }
//     }

//     pub unsafe fn prod_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         match self {
//             AtomicArray::NetworkAtomicArray(array) => {
//                 array
//                     .prod_at_pe_in_place(pe)
//             },
//             _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
//         }
//     }

//     pub unsafe fn bit_and_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         match self {
//             AtomicArray::NetworkAtomicArray(array) => {
//                 array
//                     .bit_and_at_pe_in_place(pe)
//             },
//             _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
//         }
//     }

//     pub unsafe fn bit_xor_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         match self {
//             AtomicArray::NetworkAtomicArray(array) => {
//                 array
//                     .bit_xor_at_pe_in_place(pe)
//             },
//             _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
//         }
//     }

//     pub unsafe fn bit_or_at_pe_in_place(&self, pe: usize) -> ArrayCollectiveReduceInPlaceHandle<T> {
//         match self {
//             AtomicArray::NetworkAtomicArray(array) => {
//                 array
//                     .bit_or_at_pe_in_place(pe)
//             },
//             _ => {todo!("collective reduce operations currently only supported on network atomic arrays")}
//          }
//     }
// }


impl<T: Dist> AtomicArray<T> {
    pub unsafe fn gather_all(&self, index: usize, len: usize) -> ArrayCollectiveAllGatherHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .gather_all(index, len)
            }
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .gather_all(index, len)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .gather_all(index, len)
            },
        }
    }

    pub unsafe fn gather_all_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllGatherIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .gather_all_into_buffer(index, len, buffer)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .gather_all_into_buffer(index, len, buffer)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .gather_all_into_buffer(index, len, buffer)
            },
        }
    }
}

impl<T: Dist> AtomicArray<T> {
    pub unsafe fn gather_at_pe(&self, index: usize, len: usize, pe: usize) -> ArrayCollectiveGatherHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .gather_at_pe(index, len, pe)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .gather_at_pe(index, len, pe)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .gather_at_pe(index, len, pe)
            },
        }
    }

    pub unsafe fn gather_at_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, target: RootOrLamellarBuffer<T, B>) -> ArrayCollectiveGatherIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .gather_at_pe_into_buffer(index, len, target)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .gather_at_pe_into_buffer(index, len, target)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .gather_at_pe_into_buffer(index, len, target)
            },
        }
    }
}

impl<T: Dist> AtomicArray<T> {
    pub unsafe fn alltoall(&self,  index: usize, len: usize) -> ArrayCollectiveAllToAllHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .alltoall(index, len)
            }
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .alltoall(index, len)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .alltoall(index, len)
            },
         }
    }

    pub unsafe fn alltoall_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize,  buffer: LamellarBuffer<T, B>) -> ArrayCollectiveAllToAllIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .alltoall_into_buffer(index, len, buffer)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .alltoall_into_buffer(index, len, buffer)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .alltoall_into_buffer(index, len, buffer)
            },
        }
    }
}

impl<T: Dist> AtomicArray<T> {
    pub unsafe fn broadcast_from_pe(&self, src_or_root_pe: BroadcastInput, len: usize) -> ArrayCollectiveBroadcastHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .broadcast_from_pe(src_or_root_pe, len)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .broadcast_from_pe(src_or_root_pe, len)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .broadcast_from_pe(src_or_root_pe, len)
            },
        }
    }

    pub unsafe fn broadcast_from_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, target: RootSrcOrLamellarBuffer<T, B>, len: usize) -> ArrayCollectiveBroadcastIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .broadcast_from_pe_into_buffer(target, len)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .broadcast_from_pe_into_buffer(target, len)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .broadcast_from_pe_into_buffer(target, len)
            },
        }
    }
}

impl<T: Dist> AtomicArray<T> {
    pub unsafe fn scatter_from_pe(&self, src_or_root_pe: ScatterInput, len: usize) -> ArrayCollectiveScatterHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .scatter_from_pe(src_or_root_pe, len)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .scatter_from_pe(src_or_root_pe, len)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .scatter_from_pe(src_or_root_pe, len)
            }
        }
    }

    pub unsafe fn scatter_from_pe_into_buffer<B: AsLamellarBuffer<T>>(&self, buf: LamellarBuffer<T, B>, src_or_root_pe: ScatterInput, len: usize) -> ArrayCollectiveScatterIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .scatter_from_pe_into_buffer(buf, src_or_root_pe, len)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .scatter_from_pe_into_buffer(buf, src_or_root_pe, len)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .scatter_from_pe_into_buffer(buf, src_or_root_pe, len)
            },
        }
    }
}


impl<T: ElementArithmeticOps> AtomicArray<T> {
    pub unsafe fn sum_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .sum_scatter(index, len)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .sum_scatter(index, len)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .sum_scatter(index, len)
            },
        }
    }

    pub unsafe fn max_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .max_scatter(index, len)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .max_scatter(index, len)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .max_scatter(index, len)
            },
        }
    }

    pub unsafe fn min_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .min_scatter(index, len)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .min_scatter(index, len)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .min_scatter(index, len)
            },
        }
    }

    pub unsafe fn prod_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .prod_scatter(index, len)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .prod_scatter(index, len)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .prod_scatter(index, len)
            },
        }
    }
}

impl<T: ElementBitWiseOps> AtomicArray<T> {
    pub unsafe fn bit_and_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T>
    {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .bit_and_scatter(index, len)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .bit_and_scatter(index, len)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .bit_and_scatter(index, len)
            },
        }
    }

    pub unsafe fn bit_xor_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T>
    {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .bit_xor_scatter(index, len)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .bit_xor_scatter(index, len)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .bit_xor_scatter(index, len)
            },
        }
    }

    pub unsafe fn bit_or_scatter(&self, index: usize, len: usize) -> ArrayCollectiveReduceScatterHandle<T>
    {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .bit_or_scatter(index, len)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .bit_or_scatter(index, len)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .bit_or_scatter(index, len)
            },
        }
    }
}

impl<T: ElementArithmeticOps> AtomicArray<T> {
    pub unsafe fn sum_scatter_into_buffer<B: AsLamellarBuffer<T>>(&self, index: usize, len: usize, buffer: LamellarBuffer<T, B>) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .sum_scatter_into_buffer(index, len, buffer)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .sum_scatter_into_buffer(index, len, buffer)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .sum_scatter_into_buffer(index, len, buffer)
            },
        }
    }

    pub unsafe fn max_scatter_into_buffer<B: AsLamellarBuffer<T>> (&self, index: usize, len: usize, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .max_scatter_into_buffer(index, len, buffer)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .max_scatter_into_buffer(index, len, buffer)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .max_scatter_into_buffer(index, len, buffer)
            },
        }
    }

    pub unsafe fn min_scatter_into_buffer<B: AsLamellarBuffer<T>> (&self, index: usize, len: usize, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .min_scatter_into_buffer(index, len, buffer)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .min_scatter_into_buffer(index, len, buffer)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .min_scatter_into_buffer(index, len, buffer)
            },
        }
    }

    pub unsafe fn prod_scatter_into_buffer<B: AsLamellarBuffer<T>> (&self, index: usize, len: usize, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .prod_scatter_into_buffer(index, len, buffer)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .prod_scatter_into_buffer(index, len, buffer)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .prod_scatter_into_buffer(index, len, buffer)
            },
        }
    }
}

impl<T: ElementBitWiseOps> AtomicArray<T> {
    pub unsafe fn bit_and_scatter_into_buffer<B: AsLamellarBuffer<T>> (&self, index: usize, len: usize, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .bit_and_scatter_into_buffer(index, len, buffer)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .bit_and_scatter_into_buffer(index, len, buffer)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .bit_and_scatter_into_buffer(index, len, buffer)
            },
        }
    }

    pub unsafe fn bit_xor_scatter_into_buffer<B: AsLamellarBuffer<T>> (&self, index: usize, len: usize, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .bit_xor_scatter_into_buffer(index, len, buffer)
            },
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .bit_xor_scatter_into_buffer(index, len, buffer)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .bit_xor_scatter_into_buffer(index, len, buffer)
            },
        }
    }

    pub unsafe fn bit_or_scatter_into_buffer<B: AsLamellarBuffer<T>> (&self, index: usize, len: usize, buffer: LamellarBuffer<T, B> ) -> ArrayCollectiveReduceScatterIntoBufferHandle<T, B> {
        match self {
            AtomicArray::NetworkAtomicArray(array) => {
                array
                    .bit_or_scatter_into_buffer(index, len, buffer)
            }
            AtomicArray::NativeAtomicArray(array) => {
                array
                    .bit_or_scatter_into_buffer(index, len, buffer)
            },
            AtomicArray::GenericAtomicArray(array) => {
                array
                    .bit_or_scatter_into_buffer(index, len, buffer)
            },
         }
    }
}