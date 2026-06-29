use super::TursoNewExt;
use crate::alloc::Arc;
#[cfg(nightly)]
use crate::alloc::DynAllocator;

#[cfg(not(nightly))]
fn arc<T>(value: T) -> Arc<T> {
    crate::sync::Arc::new(value)
}

#[cfg(nightly)]
fn arc<T>(value: T) -> Arc<T> {
    std::sync::Arc::new_in(value, DynAllocator::default())
}

impl<T> TursoNewExt<T> for Arc<T> {
    fn new(value: T) -> Self {
        arc(value)
    }
}
