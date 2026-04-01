#![expect(dead_code)]

use crate::alloc::{AllocError, SharedAllocator};

#[cfg(all(not(feature = "nightly"), feature = "allocator-api2"))]
use allocator_api2::collections::TryReserveError;
#[cfg(not(all(not(feature = "nightly"), feature = "allocator-api2")))]
use std::collections::TryReserveError;

#[expect(unused_imports)]
pub(crate) use self::inner::*;

#[cfg(feature = "nightly")]
mod inner {
    use super::TryReserveError;
    use crate::alloc::{AllocError, SharedAllocator};

    pub(crate) type Vec<T> = std::vec::Vec<T, SharedAllocator>;
    pub(crate) type VecDeque<T> = std::collections::VecDeque<T, SharedAllocator>;
    pub(crate) type BTreeMap<K, V> = std::collections::BTreeMap<K, V, SharedAllocator>;
    pub(crate) type BTreeSet<T> = std::collections::BTreeSet<T, SharedAllocator>;
    pub(crate) type Box<T> = std::boxed::Box<T, SharedAllocator>;
    pub(crate) type Arc<T> = std::sync::Arc<T, SharedAllocator>;

    impl<T> super::VecAllocatorExt<T> for Vec<T> {
        #[inline]
        fn new_in(alloc: SharedAllocator) -> Self {
            std::vec::Vec::new_in(alloc)
        }

        #[inline]
        fn with_capacity_in(capacity: usize, alloc: SharedAllocator) -> Self {
            std::vec::Vec::with_capacity_in(capacity, alloc)
        }

        #[inline]
        fn try_with_capacity(capacity: usize) -> Result<Self, TryReserveError> {
            std::vec::Vec::try_with_capacity_in(capacity, SharedAllocator::global())
        }

        #[inline]
        fn try_with_capacity_in(
            capacity: usize,
            alloc: SharedAllocator,
        ) -> Result<Self, TryReserveError> {
            std::vec::Vec::try_with_capacity_in(capacity, alloc)
        }
    }

    impl<T> super::VecExt<T> for Vec<T> {
        #[inline]
        fn try_push(&mut self, value: T) -> Result<(), TryReserveError> {
            self.try_reserve(1)?;
            self.push(value);
            Ok(())
        }

        #[inline]
        fn try_resize(&mut self, new_len: usize, value: T) -> Result<(), TryReserveError>
        where
            T: Clone,
        {
            if new_len > self.len() {
                self.try_reserve(new_len - self.len())?;
            }
            self.resize(new_len, value);
            Ok(())
        }

        #[inline]
        fn try_extend_from_slice(&mut self, other: &[T]) -> Result<(), TryReserveError>
        where
            T: Clone,
        {
            self.try_reserve(other.len())?;
            self.extend_from_slice(other);
            Ok(())
        }
    }

    impl<T> super::VecDequeAllocatorExt<T> for VecDeque<T> {
        #[inline]
        fn new_in(alloc: SharedAllocator) -> Self {
            std::collections::VecDeque::new_in(alloc)
        }

        #[inline]
        fn with_capacity_in(capacity: usize, alloc: SharedAllocator) -> Self {
            std::collections::VecDeque::with_capacity_in(capacity, alloc)
        }
    }

    impl<K, V> super::BTreeMapAllocatorExt<K, V> for BTreeMap<K, V> {
        #[inline]
        fn new_in(alloc: SharedAllocator) -> Self {
            std::collections::BTreeMap::new_in(alloc)
        }
    }

    impl<T> super::BTreeSetAllocatorExt<T> for BTreeSet<T> {
        #[inline]
        fn new_in(alloc: SharedAllocator) -> Self {
            std::collections::BTreeSet::new_in(alloc)
        }
    }

    impl<T> super::BoxExt for Box<T> {
        type Value = T;

        #[inline]
        fn new_in(x: T, alloc: SharedAllocator) -> Self {
            std::boxed::Box::new_in(x, alloc)
        }

        #[inline]
        fn try_new(x: T) -> Result<Self, AllocError> {
            std::boxed::Box::try_new_in(x, SharedAllocator::global())
        }

        #[inline]
        fn try_new_in(x: T, alloc: SharedAllocator) -> Result<Self, AllocError> {
            std::boxed::Box::try_new_in(x, alloc)
        }
    }

    impl<T> super::ArcExt for Arc<T> {
        type Value = T;

        #[inline]
        fn new_in(x: T, alloc: SharedAllocator) -> Self {
            std::sync::Arc::new_in(x, alloc)
        }

        #[inline]
        fn try_new(x: T) -> Result<Self, AllocError> {
            std::sync::Arc::try_new_in(x, SharedAllocator::global())
        }

        #[inline]
        fn try_new_in(x: T, alloc: SharedAllocator) -> Result<Self, AllocError> {
            std::sync::Arc::try_new_in(x, alloc)
        }
    }
}

#[cfg(all(not(feature = "nightly"), feature = "allocator-api2"))]
mod inner {
    use super::TryReserveError;
    use crate::alloc::{AllocError, SharedAllocator};

    pub(crate) type Vec<T> = allocator_api2::vec::Vec<T, SharedAllocator>;
    pub(crate) type VecDeque<T> = std::collections::VecDeque<T>;
    pub(crate) type BTreeMap<K, V> = std::collections::BTreeMap<K, V>;
    pub(crate) type BTreeSet<T> = std::collections::BTreeSet<T>;
    pub(crate) type Box<T> = std::boxed::Box<T>;
    pub(crate) type Arc<T> = std::sync::Arc<T>;

    impl<T> super::VecAllocatorExt<T> for Vec<T> {
        #[inline]
        fn new_in(alloc: SharedAllocator) -> Self {
            allocator_api2::vec::Vec::new_in(alloc)
        }

        #[inline]
        fn with_capacity_in(capacity: usize, alloc: SharedAllocator) -> Self {
            allocator_api2::vec::Vec::with_capacity_in(capacity, alloc)
        }

        #[inline]
        fn try_with_capacity(capacity: usize) -> Result<Self, TryReserveError> {
            let mut v = allocator_api2::vec::Vec::new_in(SharedAllocator::global());
            v.try_reserve(capacity)?;
            Ok(v)
        }

        #[inline]
        fn try_with_capacity_in(
            capacity: usize,
            alloc: SharedAllocator,
        ) -> Result<Self, TryReserveError> {
            let mut v = allocator_api2::vec::Vec::new_in(alloc);
            v.try_reserve(capacity)?;
            Ok(v)
        }
    }

    impl<T> super::VecExt<T> for Vec<T> {
        #[inline]
        fn try_push(&mut self, value: T) -> Result<(), TryReserveError> {
            self.try_reserve(1)?;
            self.push(value);
            Ok(())
        }

        #[inline]
        fn try_resize(&mut self, new_len: usize, value: T) -> Result<(), TryReserveError>
        where
            T: Clone,
        {
            if new_len > self.len() {
                self.try_reserve(new_len - self.len())?;
            }
            self.resize(new_len, value);
            Ok(())
        }

        #[inline]
        fn try_extend_from_slice(&mut self, other: &[T]) -> Result<(), TryReserveError>
        where
            T: Clone,
        {
            self.try_reserve(other.len())?;
            self.extend_from_slice(other);
            Ok(())
        }
    }

    impl<T> super::VecDequeAllocatorExt<T> for VecDeque<T> {
        #[inline]
        fn new_in(_alloc: SharedAllocator) -> Self {
            std::collections::VecDeque::new()
        }

        #[inline]
        fn with_capacity_in(capacity: usize, _alloc: SharedAllocator) -> Self {
            std::collections::VecDeque::with_capacity(capacity)
        }
    }

    impl<K, V> super::BTreeMapAllocatorExt<K, V> for BTreeMap<K, V> {
        #[inline]
        fn new_in(_alloc: SharedAllocator) -> Self {
            std::collections::BTreeMap::new()
        }
    }

    impl<T> super::BTreeSetAllocatorExt<T> for BTreeSet<T> {
        #[inline]
        fn new_in(_alloc: SharedAllocator) -> Self {
            std::collections::BTreeSet::new()
        }
    }

    impl<T> super::BoxExt for Box<T> {
        type Value = T;

        #[inline]
        fn new_in(x: T, _alloc: SharedAllocator) -> Self {
            std::boxed::Box::new(x)
        }

        #[inline]
        fn try_new(x: T) -> Result<Self, AllocError> {
            Ok(std::boxed::Box::new(x))
        }

        #[inline]
        fn try_new_in(x: T, _alloc: SharedAllocator) -> Result<Self, AllocError> {
            Ok(std::boxed::Box::new(x))
        }
    }

    impl<T> super::ArcExt for Arc<T> {
        type Value = T;

        #[inline]
        fn new_in(x: T, _alloc: SharedAllocator) -> Self {
            std::sync::Arc::new(x)
        }

        #[inline]
        fn try_new(x: T) -> Result<Self, AllocError> {
            Ok(std::sync::Arc::new(x))
        }

        #[inline]
        fn try_new_in(x: T, _alloc: SharedAllocator) -> Result<Self, AllocError> {
            Ok(std::sync::Arc::new(x))
        }
    }
}

#[cfg(not(any(feature = "nightly", feature = "allocator-api2")))]
mod inner {
    use super::TryReserveError;
    use crate::alloc::{AllocError, SharedAllocator};

    pub(crate) type Vec<T> = std::vec::Vec<T>;
    pub(crate) type VecDeque<T> = std::collections::VecDeque<T>;
    pub(crate) type BTreeMap<K, V> = std::collections::BTreeMap<K, V>;
    pub(crate) type BTreeSet<T> = std::collections::BTreeSet<T>;
    pub(crate) type Box<T> = std::boxed::Box<T>;
    pub(crate) type Arc<T> = std::sync::Arc<T>;

    impl<T> super::VecAllocatorExt<T> for Vec<T> {
        #[inline]
        fn new_in(_alloc: SharedAllocator) -> Self {
            std::vec::Vec::new()
        }

        #[inline]
        fn with_capacity_in(capacity: usize, _alloc: SharedAllocator) -> Self {
            std::vec::Vec::with_capacity(capacity)
        }

        #[inline]
        fn try_with_capacity(capacity: usize) -> Result<Self, TryReserveError> {
            let mut v = std::vec::Vec::new();
            v.try_reserve(capacity)?;
            Ok(v)
        }

        #[inline]
        fn try_with_capacity_in(
            capacity: usize,
            _alloc: SharedAllocator,
        ) -> Result<Self, TryReserveError> {
            let mut v = std::vec::Vec::new();
            v.try_reserve(capacity)?;
            Ok(v)
        }
    }

    impl<T> super::VecExt<T> for Vec<T> {
        #[inline]
        fn try_push(&mut self, value: T) -> Result<(), TryReserveError> {
            self.try_reserve(1)?;
            self.push(value);
            Ok(())
        }

        #[inline]
        fn try_resize(&mut self, new_len: usize, value: T) -> Result<(), TryReserveError>
        where
            T: Clone,
        {
            if new_len > self.len() {
                self.try_reserve(new_len - self.len())?;
            }
            self.resize(new_len, value);
            Ok(())
        }

        #[inline]
        fn try_extend_from_slice(&mut self, other: &[T]) -> Result<(), TryReserveError>
        where
            T: Clone,
        {
            self.try_reserve(other.len())?;
            self.extend_from_slice(other);
            Ok(())
        }
    }

    impl<T> super::VecDequeAllocatorExt<T> for VecDeque<T> {
        #[inline]
        fn new_in(_alloc: SharedAllocator) -> Self {
            std::collections::VecDeque::new()
        }

        #[inline]
        fn with_capacity_in(capacity: usize, _alloc: SharedAllocator) -> Self {
            std::collections::VecDeque::with_capacity(capacity)
        }
    }

    impl<K, V> super::BTreeMapAllocatorExt<K, V> for BTreeMap<K, V> {
        #[inline]
        fn new_in(_alloc: SharedAllocator) -> Self {
            std::collections::BTreeMap::new()
        }
    }

    impl<T> super::BTreeSetAllocatorExt<T> for BTreeSet<T> {
        #[inline]
        fn new_in(_alloc: SharedAllocator) -> Self {
            std::collections::BTreeSet::new()
        }
    }

    impl<T> super::BoxExt for Box<T> {
        type Value = T;

        #[inline]
        fn new_in(x: T, _alloc: SharedAllocator) -> Self {
            std::boxed::Box::new(x)
        }

        #[inline]
        fn try_new(x: T) -> Result<Self, AllocError> {
            Ok(std::boxed::Box::new(x))
        }

        #[inline]
        fn try_new_in(x: T, _alloc: SharedAllocator) -> Result<Self, AllocError> {
            Ok(std::boxed::Box::new(x))
        }
    }

    impl<T> super::ArcExt for Arc<T> {
        type Value = T;

        #[inline]
        fn new_in(x: T, _alloc: SharedAllocator) -> Self {
            std::sync::Arc::new(x)
        }

        #[inline]
        fn try_new(x: T) -> Result<Self, AllocError> {
            Ok(std::sync::Arc::new(x))
        }

        #[inline]
        fn try_new_in(x: T, _alloc: SharedAllocator) -> Result<Self, AllocError> {
            Ok(std::sync::Arc::new(x))
        }
    }
}

/// Stable shim for the nightly `Vec<T, A>` allocator-aware constructors.
pub(crate) trait VecAllocatorExt<T> {
    /// See [`Vec::new_in`](https://doc.rust-lang.org/nightly/std/vec/struct.Vec.html#method.new_in)
    fn new_in(alloc: SharedAllocator) -> Self;

    /// See [`Vec::with_capacity_in`](https://doc.rust-lang.org/nightly/std/vec/struct.Vec.html#method.with_capacity_in)
    fn with_capacity_in(capacity: usize, alloc: SharedAllocator) -> Self;

    /// See [`Vec::try_with_capacity`](https://doc.rust-lang.org/nightly/std/vec/struct.Vec.html#method.try_with_capacity)
    fn try_with_capacity(capacity: usize) -> Result<Self, TryReserveError>
    where
        Self: Sized;

    /// See [`Vec::try_with_capacity_in`](https://doc.rust-lang.org/nightly/std/vec/struct.Vec.html#method.try_with_capacity_in)
    fn try_with_capacity_in(
        capacity: usize,
        alloc: SharedAllocator,
    ) -> Result<Self, TryReserveError>
    where
        Self: Sized;
}

/// Local fallible helpers built on top of `try_reserve`.
pub(crate) trait VecExt<T> {
    fn try_push(&mut self, value: T) -> Result<(), TryReserveError>;

    fn try_resize(&mut self, new_len: usize, value: T) -> Result<(), TryReserveError>
    where
        T: Clone;

    fn try_extend_from_slice(&mut self, other: &[T]) -> Result<(), TryReserveError>
    where
        T: Clone;
}

/// Stable shim for the nightly `VecDeque<T, A>` allocator-aware constructors.
pub(crate) trait VecDequeAllocatorExt<T> {
    /// See [`VecDeque::new_in`](https://doc.rust-lang.org/nightly/std/collections/vec_deque/struct.VecDeque.html#method.new_in)
    fn new_in(alloc: SharedAllocator) -> Self;

    /// See [`VecDeque::with_capacity_in`](https://doc.rust-lang.org/nightly/std/collections/vec_deque/struct.VecDeque.html#method.with_capacity_in)
    fn with_capacity_in(capacity: usize, alloc: SharedAllocator) -> Self;
}

/// Stable shim for the nightly `BTreeMap<K, V, A>::new_in`.
pub(crate) trait BTreeMapAllocatorExt<K, V> {
    /// See [`BTreeMap::new_in`](https://doc.rust-lang.org/nightly/std/collections/struct.BTreeMap.html#method.new_in)
    fn new_in(alloc: SharedAllocator) -> Self;
}

/// Stable shim for the nightly `BTreeSet<T, A>::new_in`.
pub(crate) trait BTreeSetAllocatorExt<T> {
    /// See [`BTreeSet::new_in`](https://doc.rust-lang.org/nightly/std/collections/struct.BTreeSet.html#method.new_in)
    fn new_in(alloc: SharedAllocator) -> Self;
}

/// Stable shim for the nightly allocator-aware `Box` constructors.
pub(crate) trait BoxExt {
    type Value;

    /// See [`Box::new_in`](https://doc.rust-lang.org/nightly/std/boxed/struct.Box.html#method.new_in)
    fn new_in(x: Self::Value, alloc: SharedAllocator) -> Self;

    /// See [`Box::try_new`](https://doc.rust-lang.org/nightly/std/boxed/struct.Box.html#method.try_new)
    fn try_new(x: Self::Value) -> Result<Self, AllocError>
    where
        Self: Sized;

    /// See [`Box::try_new_in`](https://doc.rust-lang.org/nightly/std/boxed/struct.Box.html#method.try_new_in)
    fn try_new_in(x: Self::Value, alloc: SharedAllocator) -> Result<Self, AllocError>
    where
        Self: Sized;
}

/// Stable shim for the nightly allocator-aware `Arc` constructors.
pub(crate) trait ArcExt {
    type Value;

    /// See [`Arc::new_in`](https://doc.rust-lang.org/nightly/std/sync/struct.Arc.html#method.new_in)
    fn new_in(x: Self::Value, alloc: SharedAllocator) -> Self;

    /// See [`Arc::try_new`](https://doc.rust-lang.org/nightly/std/sync/struct.Arc.html#method.try_new)
    fn try_new(x: Self::Value) -> Result<Self, AllocError>
    where
        Self: Sized;

    /// See [`Arc::try_new_in`](https://doc.rust-lang.org/nightly/std/sync/struct.Arc.html#method.try_new_in)
    fn try_new_in(x: Self::Value, alloc: SharedAllocator) -> Result<Self, AllocError>
    where
        Self: Sized;
}
