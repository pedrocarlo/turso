use std::marker::PhantomData;

use rand::Rng;

use super::GenerationContext;

/// A strategy for generating values of type `T`.
///
/// Similar to proptest's Strategy trait, this provides a composable way to describe
/// value generation. Strategies are lazy - they describe *how* to generate values
/// but don't actually generate them until `generate()` is called.
///
/// # Examples
///
/// ```ignore
/// use sql_generation::generation::strategy::*;
///
/// // Generate integers and double them
/// let strategy = any::<i32>().map(|x| x * 2);
/// let value = strategy.generate(&mut rng, &context);
///
/// // Generate even numbers
/// let strategy = any::<i32>().filter(|x| x % 2 == 0);
/// let value = strategy.generate(&mut rng, &context);
///
/// // Generate dependent values
/// let strategy = any::<i32>().flat_map(|x| just(x * 2));
/// let value = strategy.generate(&mut rng, &context);
/// ```
pub trait Strategy {
    /// The type of value this strategy generates
    type Value;

    /// Generate a value using this strategy
    fn generate<R: Rng + ?Sized, C: GenerationContext>(
        &self,
        rng: &mut R,
        context: &C,
    ) -> Self::Value;

    // Combinator methods for composing strategies

    /// Transform generated values using the provided function.
    ///
    /// This is analogous to `Iterator::map`.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let strategy = any::<i32>().map(|x| x.abs());
    /// ```
    fn map<F, U>(self, f: F) -> Map<Self, F>
    where
        Self: Sized,
        F: Fn(Self::Value) -> U,
    {
        Map {
            strategy: self,
            mapper: f,
        }
    }

    /// Filter generated values, retrying until a value passes the predicate.
    ///
    /// Note: This will panic if max_retries is exceeded without finding a valid value.
    /// Use this carefully - ensure your predicate has a reasonable probability of success.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let strategy = any::<i32>().filter(|x| *x > 0);
    /// ```
    fn filter<F>(self, predicate: F) -> Filter<Self, F>
    where
        Self: Sized,
        F: Fn(&Self::Value) -> bool,
    {
        Filter {
            strategy: self,
            predicate,
            max_retries: 100,
        }
    }

    /// Filter with a custom retry limit.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let strategy = any::<i32>().filter_with_retries(|x| *x > 0, 1000);
    /// ```
    fn filter_with_retries<F>(self, predicate: F, max_retries: usize) -> Filter<Self, F>
    where
        Self: Sized,
        F: Fn(&Self::Value) -> bool,
    {
        Filter {
            strategy: self,
            predicate,
            max_retries,
        }
    }

    /// Generate a value, then use it to create a new strategy.
    ///
    /// This is the monadic bind operation, allowing for dependent generation.
    /// It's analogous to `Iterator::flat_map`.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Generate a list whose length depends on a generated size
    /// let strategy = any::<usize>()
    ///     .map(|n| n % 10)  // 0-9
    ///     .flat_map(|size| vec_of(any::<i32>(), size));
    /// ```
    fn flat_map<F, S>(self, f: F) -> FlatMap<Self, F>
    where
        Self: Sized,
        F: Fn(Self::Value) -> S,
        S: Strategy,
    {
        FlatMap {
            strategy: self,
            mapper: f,
        }
    }

    /// Box this strategy for type erasure.
    ///
    /// Useful when you need to store strategies with different types in a collection.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let strategies: Vec<BoxedStrategy<i32>> = vec![
    ///     any::<i32>().boxed(),
    ///     just(42).boxed(),
    /// ];
    /// ```
    fn boxed<'a>(self) -> BoxedStrategy<'a, Self::Value>
    where
        Self: Sized + 'a,
    {
        BoxedStrategy::new(self)
    }
}

// ============================================================================
// Combinator implementations
// ============================================================================

/// Strategy that transforms generated values using a function.
///
/// Created by [`Strategy::map`].
pub struct Map<S, F> {
    strategy: S,
    mapper: F,
}

impl<S, F, U> Strategy for Map<S, F>
where
    S: Strategy,
    F: Fn(S::Value) -> U,
{
    type Value = U;

    fn generate<R: Rng + ?Sized, C: GenerationContext>(
        &self,
        rng: &mut R,
        context: &C,
    ) -> U {
        (self.mapper)(self.strategy.generate(rng, context))
    }
}

/// Strategy that filters generated values, retrying until one passes.
///
/// Created by [`Strategy::filter`] or [`Strategy::filter_with_retries`].
pub struct Filter<S, F> {
    strategy: S,
    predicate: F,
    max_retries: usize,
}

impl<S, F> Strategy for Filter<S, F>
where
    S: Strategy,
    F: Fn(&S::Value) -> bool,
{
    type Value = S::Value;

    fn generate<R: Rng + ?Sized, C: GenerationContext>(
        &self,
        rng: &mut R,
        context: &C,
    ) -> S::Value {
        for attempt in 0..self.max_retries {
            let value = self.strategy.generate(rng, context);
            if (self.predicate)(&value) {
                return value;
            }

            if attempt == self.max_retries - 1 {
                panic!(
                    "Filter failed after {} attempts. Predicate may be too restrictive.",
                    self.max_retries
                );
            }
        }
        unreachable!()
    }
}

/// Strategy that generates a value then uses it to create another strategy.
///
/// Created by [`Strategy::flat_map`].
pub struct FlatMap<S, F> {
    strategy: S,
    mapper: F,
}

impl<S, F, S2> Strategy for FlatMap<S, F>
where
    S: Strategy,
    F: Fn(S::Value) -> S2,
    S2: Strategy,
{
    type Value = S2::Value;

    fn generate<R: Rng + ?Sized, C: GenerationContext>(
        &self,
        rng: &mut R,
        context: &C,
    ) -> S2::Value {
        let value = self.strategy.generate(rng, context);
        let next_strategy = (self.mapper)(value);
        next_strategy.generate(rng, context)
    }
}

/// Type-erased strategy for storing strategies with different types.
///
/// Note: Full implementation deferred to Step 2 (choice combinators).
/// The challenge is that both Strategy and Rng have generic methods,
/// making them not dyn-compatible. We'll need a different approach.
///
/// Created by [`Strategy::boxed`].
pub struct BoxedStrategy<'a, T> {
    _phantom: PhantomData<(&'a (), T)>,
}

impl<'a, T> BoxedStrategy<'a, T> {
    /// Create a new boxed strategy from any strategy
    ///
    /// Note: Not yet fully implemented - will be completed in Step 2
    pub fn new<S: Strategy<Value = T> + 'a>(_strategy: S) -> Self {
        BoxedStrategy {
            _phantom: PhantomData,
        }
    }
}

impl<'a, T: Clone> Strategy for BoxedStrategy<'a, T> {
    type Value = T;

    fn generate<R: Rng + ?Sized, C: GenerationContext>(
        &self,
        _rng: &mut R,
        _context: &C,
    ) -> T {
        // Deferred to Step 2 - for now, this is just a placeholder
        panic!("BoxedStrategy not yet implemented - will be completed in Step 2")
    }
}

// ============================================================================
// Basic strategy constructors
// ============================================================================

/// Generate a value using the `Arbitrary` trait.
///
/// This is the bridge between the old trait-based generation and the new
/// strategy-based approach.
///
/// # Examples
///
/// ```ignore
/// use sql_generation::generation::strategy::any;
/// use sql_generation::model::table::Table;
///
/// let table_strategy = any::<Table>();
/// let table = table_strategy.generate(&mut rng, &context);
/// ```
pub fn any<T: super::Arbitrary>() -> impl Strategy<Value = T> {
    AnyStrategy(PhantomData::<T>)
}

struct AnyStrategy<T>(PhantomData<T>);

impl<T: super::Arbitrary> Strategy for AnyStrategy<T> {
    type Value = T;

    fn generate<R: Rng + ?Sized, C: GenerationContext>(
        &self,
        rng: &mut R,
        context: &C,
    ) -> T {
        T::arbitrary(rng, context)
    }
}

/// Generate a value from a specific input using `ArbitraryFrom`.
///
/// This is the bridge between the old trait-based generation and the new
/// strategy-based approach for dependent generation.
///
/// # Examples
///
/// ```ignore
/// use sql_generation::generation::strategy::from;
/// use sql_generation::model::table::{SimValue, ColumnType};
///
/// let column_type = ColumnType::Integer;
/// let value_strategy = from::<SimValue, _>(column_type);
/// let value = value_strategy.generate(&mut rng, &context);
/// ```
pub fn from<T, U>(input: U) -> impl Strategy<Value = T>
where
    T: super::ArbitraryFrom<U>,
    U: Clone,
{
    FromStrategy {
        input,
        _phantom: PhantomData,
    }
}

struct FromStrategy<T, U> {
    input: U,
    _phantom: PhantomData<T>,
}

impl<T, U> Strategy for FromStrategy<T, U>
where
    T: super::ArbitraryFrom<U>,
    U: Clone,
{
    type Value = T;

    fn generate<R: Rng + ?Sized, C: GenerationContext>(
        &self,
        rng: &mut R,
        context: &C,
    ) -> T {
        T::arbitrary_from(rng, context, self.input.clone())
    }
}

/// Generate a constant value.
///
/// This is useful for testing or as a building block in more complex strategies.
///
/// # Examples
///
/// ```ignore
/// use sql_generation::generation::strategy::just;
///
/// let strategy = just(42);
/// assert_eq!(strategy.generate(&mut rng, &context), 42);
/// ```
pub fn just<T: Clone>(value: T) -> impl Strategy<Value = T> {
    JustStrategy(value)
}

struct JustStrategy<T>(T);

impl<T: Clone> Strategy for JustStrategy<T> {
    type Value = T;

    fn generate<R: Rng + ?Sized, C: GenerationContext>(
        &self,
        _rng: &mut R,
        _context: &C,
    ) -> T {
        self.0.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generation::tests::TestContext;
    use rand::SeedableRng;
    use rand_chacha::ChaCha8Rng;

    #[test]
    fn test_just_strategy() {
        let mut rng = ChaCha8Rng::seed_from_u64(42);
        let context = TestContext::default();

        let strategy = just(42);
        assert_eq!(strategy.generate(&mut rng, &context), 42);
    }

    #[test]
    fn test_map_combinator() {
        let mut rng = ChaCha8Rng::seed_from_u64(42);
        let context = TestContext::default();

        let strategy = just(21).map(|x| x * 2);
        assert_eq!(strategy.generate(&mut rng, &context), 42);
    }

    #[test]
    fn test_map_chain() {
        let mut rng = ChaCha8Rng::seed_from_u64(42);
        let context = TestContext::default();

        let strategy = just(10).map(|x| x * 2).map(|x| x + 2);
        assert_eq!(strategy.generate(&mut rng, &context), 22);
    }

    #[test]
    fn test_filter_strategy() {
        let mut rng = ChaCha8Rng::seed_from_u64(42);
        let context = TestContext::default();

        // Filter on just values
        let strategy = just(20).filter(|x| *x > 15);
        let value = strategy.generate(&mut rng, &context);
        assert_eq!(value, 20);
    }

    #[test]
    fn test_flat_map() {
        let mut rng = ChaCha8Rng::seed_from_u64(42);
        let context = TestContext::default();

        let strategy = just(21).flat_map(|x| just(x * 2));
        assert_eq!(strategy.generate(&mut rng, &context), 42);
    }

    #[test]
    fn test_flat_map_nested() {
        let mut rng = ChaCha8Rng::seed_from_u64(42);
        let context = TestContext::default();

        // Nested flat_map
        let strategy = just(10)
            .flat_map(|x| just(x * 2))
            .flat_map(|x| just(x + 2));

        let value = strategy.generate(&mut rng, &context);
        assert_eq!(value, 22);
    }

    #[test]
    fn test_combined_operations() {
        let mut rng = ChaCha8Rng::seed_from_u64(42);
        let context = TestContext::default();

        // Complex chain: generate, transform, filter, transform again
        let strategy = just(10)
            .map(|x| x * 2) // 20
            .filter(|x| *x > 15)
            .map(|x| x + 5); // 25

        assert_eq!(strategy.generate(&mut rng, &context), 25);
    }

    #[test]
    #[should_panic(expected = "Filter failed after 10 attempts")]
    fn test_filter_exhaustion() {
        let mut rng = ChaCha8Rng::seed_from_u64(42);
        let context = TestContext::default();

        // This should panic because no value will satisfy the filter
        let strategy = just(1).filter_with_retries(|_| false, 10);
        strategy.generate(&mut rng, &context);
    }
}
