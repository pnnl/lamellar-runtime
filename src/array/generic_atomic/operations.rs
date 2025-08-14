use crate::array::generic_atomic::*;
use crate::array::*;

impl<T: ElementOps + 'static> ReadOnlyOps<T> for GenericAtomicArray<T> {}

impl<T: ElementOps + 'static> AccessOps<T> for GenericAtomicArray<T> {}

impl<T: ElementArithmeticOps + 'static> ArithmeticOps<T> for GenericAtomicArray<T> {}

impl<T: ElementBitWiseOps + 'static> BitWiseOps<T> for GenericAtomicArray<T> {}

impl<T: ElementShiftOps + 'static> ShiftOps<T> for GenericAtomicArray<T> {}

impl<T: ElementCompareEqOps + 'static> CompareExchangeOps<T> for GenericAtomicArray<T> {}

impl<T: ElementComparePartialEqOps + 'static> CompareExchangeEpsilonOps<T>
    for GenericAtomicArray<T>
{
}

impl<T: Dist + ElementOps> LocalAccessOps<T> for GenericAtomicLocalData<T> {
    fn local_store(&mut self, idx_vals: impl Iterator<Item = (usize, T)>) {
        idx_vals.for_each(|(i, val)| {
            self.at(i).store(val);
        });
    }
    fn local_swap(&mut self, idx_vals: impl Iterator<Item = (usize, T)>) -> Vec<T> {
        idx_vals.map(|(i, val)| self.at(i).swap(val)).collect()
    }
}

impl<T: Dist + ElementArithmeticOps> LocalArithmeticOps<T> for GenericAtomicLocalData<T> {
    fn local_fetch_add(
        &mut self,
        idx_vals: impl Iterator<Item = (usize, T)>,
        fetch: bool,
    ) -> Option<Vec<T>> {
        if fetch {
            Some(idx_vals.map(|(i, val)| self.at(i).fetch_add(val)).collect())
        } else {
            idx_vals.for_each(|(i, val)| {
                self.at(i).fetch_add(val);
            });
            None
        }
    }

    fn local_fetch_sub(
        &mut self,
        idx_vals: impl Iterator<Item = (usize, T)>,
        fetch: bool,
    ) -> Option<Vec<T>> {
        if fetch {
            Some(idx_vals.map(|(i, val)| self.at(i).fetch_sub(val)).collect())
        } else {
            idx_vals.for_each(|(i, val)| {
                self.at(i).fetch_sub(val);
            });
            None
        }
    }

    fn local_fetch_mul(
        &mut self,
        idx_vals: impl Iterator<Item = (usize, T)>,
        fetch: bool,
    ) -> Option<Vec<T>> {
        if fetch {
            Some(idx_vals.map(|(i, val)| self.at(i).fetch_mul(val)).collect())
        } else {
            idx_vals.for_each(|(i, val)| {
                self.at(i).fetch_mul(val);
            });
            None
        }
    }

    fn local_fetch_div(
        &mut self,
        idx_vals: impl Iterator<Item = (usize, T)>,
        fetch: bool,
    ) -> Option<Vec<T>> {
        if fetch {
            Some(idx_vals.map(|(i, val)| self.at(i).fetch_div(val)).collect())
        } else {
            idx_vals.for_each(|(i, val)| {
                self.at(i).fetch_div(val);
            });
            None
        }
    }

    fn local_fetch_rem(
        &mut self,
        idx_vals: impl Iterator<Item = (usize, T)>,
        fetch: bool,
    ) -> Option<Vec<T>> {
        if fetch {
            Some(idx_vals.map(|(i, val)| self.at(i).fetch_rem(val)).collect())
        } else {
            idx_vals.for_each(|(i, val)| {
                self.at(i).fetch_rem(val);
            });
            None
        }
    }
}

impl<T: Dist + ElementBitWiseOps> LocalBitWiseOps<T> for GenericAtomicLocalData<T> {
    fn local_fetch_bit_and(
        &mut self,
        idx_vals: impl Iterator<Item = (usize, T)>,
        fetch: bool,
    ) -> Option<Vec<T>> {
        if fetch {
            Some(idx_vals.map(|(i, val)| self.at(i).fetch_and(val)).collect())
        } else {
            idx_vals.for_each(|(i, val)| {
                self.at(i).fetch_and(val);
            });
            None
        }
    }

    fn local_fetch_bit_or(
        &mut self,
        idx_vals: impl Iterator<Item = (usize, T)>,
        fetch: bool,
    ) -> Option<Vec<T>> {
        if fetch {
            Some(idx_vals.map(|(i, val)| self.at(i).fetch_or(val)).collect())
        } else {
            idx_vals.for_each(|(i, val)| {
                self.at(i).fetch_or(val);
            });
            None
        }
    }

    fn local_fetch_bit_xor(
        &mut self,
        idx_vals: impl Iterator<Item = (usize, T)>,
        fetch: bool,
    ) -> Option<Vec<T>> {
        if fetch {
            Some(idx_vals.map(|(i, val)| self.at(i).fetch_xor(val)).collect())
        } else {
            idx_vals.for_each(|(i, val)| {
                self.at(i).fetch_xor(val);
            });
            None
        }
    }
}

impl<T: ElementOps> LocalReadOnlyOps<T> for GenericAtomicLocalData<T> {
    fn local_load(&self, idx_vals: impl Iterator<Item = (usize, T)>) -> Vec<T> {
        idx_vals.map(|(i, _val)| self.at(i).load()).collect()
    }
}

impl<T: Dist + ElementShiftOps> LocalShiftOps<T> for GenericAtomicLocalData<T> {
    fn local_fetch_shl(
        &mut self,
        idx_vals: impl Iterator<Item = (usize, T)>,
        fetch: bool,
    ) -> Option<Vec<T>> {
        if fetch {
            Some(idx_vals.map(|(i, val)| self.at(i).fetch_shl(val)).collect())
        } else {
            idx_vals.for_each(|(i, val)| {
                self.at(i).fetch_shl(val);
            });
            None
        }
    }

    fn local_fetch_shr(
        &mut self,
        idx_vals: impl Iterator<Item = (usize, T)>,
        fetch: bool,
    ) -> Option<Vec<T>> {
        if fetch {
            Some(idx_vals.map(|(i, val)| self.at(i).fetch_shr(val)).collect())
        } else {
            idx_vals.for_each(|(i, val)| {
                self.at(i).fetch_shr(val);
            });
            None
        }
    }
}

impl<T: ElementCompareEqOps> LocalCompareExchangeOps<T> for GenericAtomicLocalData<T> {
    fn local_compare_exchange(
        &mut self,
        idx_vals: impl Iterator<Item = (usize, T)>,
        current: T,
    ) -> Vec<Result<T, T>> {
        idx_vals
            .map(|(i, val)| self.at(i).compare_exchange(current, val))
            .collect()
    }
}

impl<T: ElementComparePartialEqOps> LocalCompareExchangeOpsEpsilon<T>
    for GenericAtomicLocalData<T>
{
    fn local_compare_exchange_epsilon(
        &mut self,
        idx_vals: impl Iterator<Item = (usize, T)>,
        current: T,
        epsilon: T,
    ) -> Vec<Result<T, T>> {
        idx_vals
            .map(|(i, val)| self.at(i).compare_exchange_epsilon(current, val, epsilon))
            .collect()
    }
}
