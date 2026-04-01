use core::fmt;
use std::sync::Arc;

pub(crate) use self::inner::*;

// Nightly case: use the unstable `allocator_api` feature from std directly.
#[cfg(nightly)]
mod inner {
    use std::alloc::Layout;
    use std::ptr::NonNull;

    use super::TursoAllocator;

    pub use std::alloc::{AllocError, Allocator, Global};

    unsafe impl TursoAllocator for Global {
        #[inline]
        fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
            Allocator::allocate(&Global, layout)
        }

        #[inline]
        unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
            unsafe {
                Allocator::deallocate(&Global, ptr, layout);
            }
        }
    }

    unsafe impl Allocator for super::SharedAllocator {
        #[inline]
        fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
            TursoAllocator::allocate(self, layout)
        }

        #[inline]
        unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
            unsafe { TursoAllocator::deallocate(self, ptr, layout) }
        }
    }

    #[expect(dead_code)]
    pub(crate) fn do_alloc<A: Allocator>(alloc: &A, layout: Layout) -> Result<NonNull<[u8]>, ()> {
        match alloc.allocate(layout) {
            Ok(ptr) => Ok(ptr),
            Err(_) => Err(()),
        }
    }
}

// Non-nightly with allocator-api2: use the polyfill crate.
#[cfg(all(not(nightly), feature = "allocator-api2"))]
mod inner {
    use std::alloc::Layout;
    use std::ptr::NonNull;

    use super::TursoAllocator;

    pub use allocator_api2::alloc::{AllocError, Allocator, Global};

    unsafe impl TursoAllocator for Global {
        #[inline]
        fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
            Allocator::allocate(&Global, layout)
        }

        #[inline]
        unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
            unsafe {
                Allocator::deallocate(&Global, ptr, layout);
            }
        }
    }

    unsafe impl Allocator for super::SharedAllocator {
        #[inline]
        fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
            TursoAllocator::allocate(self, layout)
        }

        #[inline]
        unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
            unsafe { TursoAllocator::deallocate(self, ptr, layout) }
        }
    }

    #[expect(dead_code)]
    pub(crate) fn do_alloc<A: Allocator>(alloc: &A, layout: Layout) -> Result<NonNull<[u8]>, ()> {
        match alloc.allocate(layout) {
            Ok(ptr) => Ok(ptr),
            Err(_) => Err(()),
        }
    }
}

// No-defaults case: define a minimal Allocator trait and AllocError ourselves.
#[cfg(not(any(nightly, feature = "allocator-api2")))]
mod inner {
    use core::fmt;
    use std::alloc::Layout;
    use std::ptr::NonNull;

    use super::TursoAllocator;

    pub unsafe trait Allocator {
        fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError>;
        unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout);
    }

    /// The `AllocError` error indicates an allocation failure
    /// that may be due to resource exhaustion or to
    /// something wrong when combining the given input arguments with this
    /// allocator.
    #[derive(Clone, PartialEq, Eq, Debug)]
    pub struct AllocError;

    impl fmt::Display for AllocError {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("memory allocation failed")
        }
    }

    /// A zero-sized type that implements [`Allocator`] by forwarding to the
    /// global allocator (`std::alloc::{alloc, dealloc}`).
    #[derive(Copy, Clone, Default, Debug)]
    pub struct Global;

    unsafe impl TursoAllocator for Global {
        #[inline]
        fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
            match unsafe { NonNull::new(std::alloc::alloc(layout)) } {
                Some(data) => {
                    // SAFETY: this is NonNull::slice_from_raw_parts.
                    Ok(unsafe {
                        NonNull::new_unchecked(core::ptr::slice_from_raw_parts_mut(
                            data.as_ptr(),
                            layout.size(),
                        ))
                    })
                }
                None => Err(AllocError),
            }
        }

        #[inline]
        unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
            if layout.size() != 0 {
                // SAFETY: caller guarantees `ptr` was allocated with this allocator
                // and `layout` matches the original allocation.
                unsafe { std::alloc::dealloc(ptr.as_ptr(), layout) }
            }
        }
    }

    unsafe impl Allocator for super::SharedAllocator {
        #[inline]
        fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
            TursoAllocator::allocate(self, layout)
        }

        #[inline]
        unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
            unsafe { TursoAllocator::deallocate(self, ptr, layout) }
        }
    }

    #[expect(dead_code)]
    pub(crate) fn do_alloc<A: Allocator>(alloc: &A, layout: Layout) -> Result<NonNull<[u8]>, ()> {
        match alloc.allocate(layout) {
            Ok(ptr) => Ok(ptr),
            Err(_) => Err(()),
        }
    }
}

/// Custom Allocator Trait to not depend directly on `allocator_api`. Without `nightly` or `allocator_api2` feature
pub unsafe trait TursoAllocator: Send + Sync + 'static {
    fn allocate(&self, layout: std::alloc::Layout) -> Result<std::ptr::NonNull<[u8]>, AllocError>;
    unsafe fn deallocate(&self, ptr: std::ptr::NonNull<u8>, layout: std::alloc::Layout);
}

/// A cloneable, type-erased allocator backed by `Arc<dyn TursoAllocator>`.
#[derive(Clone)]
pub struct SharedAllocator(Arc<dyn TursoAllocator>);

impl SharedAllocator {
    pub fn new<A: TursoAllocator>(alloc: A) -> Self {
        Self(Arc::new(alloc))
    }

    /// Create a `SharedAllocator` backed by the global allocator.
    pub fn global() -> Self {
        Self::new(Global)
    }
}

impl fmt::Debug for SharedAllocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedAllocator").finish()
    }
}

unsafe impl TursoAllocator for SharedAllocator {
    #[inline]
    fn allocate(&self, layout: std::alloc::Layout) -> Result<std::ptr::NonNull<[u8]>, AllocError> {
        self.0.allocate(layout)
    }

    #[inline]
    unsafe fn deallocate(&self, ptr: std::ptr::NonNull<u8>, layout: std::alloc::Layout) {
        self.0.deallocate(ptr, layout)
    }
}
