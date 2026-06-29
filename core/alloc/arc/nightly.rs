use crate::alloc::{ConcurrentAllocator, DynAllocator, TryReserveError};

pub type Arc<T, A = DynAllocator> = std::sync::Arc<T, A>;
pub type ArcSlice<T, A = DynAllocator> = std::sync::Arc<[T], A>;
pub type Weak<T, A = DynAllocator> = std::sync::Weak<T, A>;

pub fn try_arc_slice_from_slice<T: Clone>(slice: &[T]) -> Result<ArcSlice<T>, TryReserveError> {
    try_arc_slice_from_slice_in(slice, DynAllocator::default())
}

pub fn try_arc_slice_from_slice_in<T: Clone, A: ConcurrentAllocator>(
    slice: &[T],
    alloc: A,
) -> Result<ArcSlice<T>, TryReserveError> {
    std::sync::Arc::<[T], DynAllocator>::try_clone_from_ref_in(slice, DynAllocator::new(alloc))
        .map_err(|_| TryReserveError)
}
