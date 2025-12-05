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

// ============================================================================
// Choice combinators
// ============================================================================

/// Choose uniformly from a tuple of 2 strategies.
///
/// This is the simplest form of choice - pick one of two strategies with equal probability.
///
/// # Examples
///
/// ```ignore
/// use sql_generation::generation::strategy::*;
///
/// let strategy = one_of_2(just(1), just(2));
/// let value = strategy.generate(&mut rng, &context);
/// assert!(value == 1 || value == 2);
/// ```
pub fn one_of_2<S1, S2>(s1: S1, s2: S2) -> impl Strategy<Value = S1::Value>
where
    S1: Strategy,
    S2: Strategy<Value = S1::Value>,
{
    OneOf2 { s1, s2 }
}

struct OneOf2<S1, S2> {
    s1: S1,
    s2: S2,
}

impl<S1, S2> Strategy for OneOf2<S1, S2>
where
    S1: Strategy,
    S2: Strategy<Value = S1::Value>,
{
    type Value = S1::Value;

    fn generate<R: Rng + ?Sized, C: GenerationContext>(
        &self,
        rng: &mut R,
        context: &C,
    ) -> S1::Value {
        if rng.random_bool(0.5) {
            self.s1.generate(rng, context)
        } else {
            self.s2.generate(rng, context)
        }
    }
}

/// Choose uniformly from a tuple of 3 strategies.
///
/// # Examples
///
/// ```ignore
/// let strategy = one_of_3(just(1), just(2), just(3));
/// ```
pub fn one_of_3<S1, S2, S3>(s1: S1, s2: S2, s3: S3) -> impl Strategy<Value = S1::Value>
where
    S1: Strategy,
    S2: Strategy<Value = S1::Value>,
    S3: Strategy<Value = S1::Value>,
{
    OneOf3 { s1, s2, s3 }
}

struct OneOf3<S1, S2, S3> {
    s1: S1,
    s2: S2,
    s3: S3,
}

impl<S1, S2, S3> Strategy for OneOf3<S1, S2, S3>
where
    S1: Strategy,
    S2: Strategy<Value = S1::Value>,
    S3: Strategy<Value = S1::Value>,
{
    type Value = S1::Value;

    fn generate<R: Rng + ?Sized, C: GenerationContext>(
        &self,
        rng: &mut R,
        context: &C,
    ) -> S1::Value {
        match rng.random_range(0..3) {
            0 => self.s1.generate(rng, context),
            1 => self.s2.generate(rng, context),
            _ => self.s3.generate(rng, context),
        }
    }
}

/// Choose uniformly from a tuple of 4 strategies.
pub fn one_of_4<S1, S2, S3, S4>(
    s1: S1,
    s2: S2,
    s3: S3,
    s4: S4,
) -> impl Strategy<Value = S1::Value>
where
    S1: Strategy,
    S2: Strategy<Value = S1::Value>,
    S3: Strategy<Value = S1::Value>,
    S4: Strategy<Value = S1::Value>,
{
    OneOf4 { s1, s2, s3, s4 }
}

struct OneOf4<S1, S2, S3, S4> {
    s1: S1,
    s2: S2,
    s3: S3,
    s4: S4,
}

impl<S1, S2, S3, S4> Strategy for OneOf4<S1, S2, S3, S4>
where
    S1: Strategy,
    S2: Strategy<Value = S1::Value>,
    S3: Strategy<Value = S1::Value>,
    S4: Strategy<Value = S1::Value>,
{
    type Value = S1::Value;

    fn generate<R: Rng + ?Sized, C: GenerationContext>(
        &self,
        rng: &mut R,
        context: &C,
    ) -> S1::Value {
        match rng.random_range(0..4) {
            0 => self.s1.generate(rng, context),
            1 => self.s2.generate(rng, context),
            2 => self.s3.generate(rng, context),
            _ => self.s4.generate(rng, context),
        }
    }
}

/// Choose from strategies with weighted probabilities (2 strategies).
///
/// The weights don't need to sum to any particular value - they represent relative probabilities.
///
/// # Examples
///
/// ```ignore
/// // 75% chance of generating 1, 25% chance of generating 2
/// let strategy = weighted_2((3, just(1)), (1, just(2)));
/// ```
pub fn weighted_2<S1, S2>(w1: (usize, S1), w2: (usize, S2)) -> impl Strategy<Value = S1::Value>
where
    S1: Strategy,
    S2: Strategy<Value = S1::Value>,
{
    Weighted2 {
        w1: w1.0,
        s1: w1.1,
        w2: w2.0,
        s2: w2.1,
    }
}

struct Weighted2<S1, S2> {
    w1: usize,
    s1: S1,
    w2: usize,
    s2: S2,
}

impl<S1, S2> Strategy for Weighted2<S1, S2>
where
    S1: Strategy,
    S2: Strategy<Value = S1::Value>,
{
    type Value = S1::Value;

    fn generate<R: Rng + ?Sized, C: GenerationContext>(
        &self,
        rng: &mut R,
        context: &C,
    ) -> S1::Value {
        let total = self.w1 + self.w2;
        let choice = rng.random_range(0..total);

        if choice < self.w1 {
            self.s1.generate(rng, context)
        } else {
            self.s2.generate(rng, context)
        }
    }
}

/// Choose from strategies with weighted probabilities (3 strategies).
pub fn weighted_3<S1, S2, S3>(
    w1: (usize, S1),
    w2: (usize, S2),
    w3: (usize, S3),
) -> impl Strategy<Value = S1::Value>
where
    S1: Strategy,
    S2: Strategy<Value = S1::Value>,
    S3: Strategy<Value = S1::Value>,
{
    Weighted3 {
        w1: w1.0,
        s1: w1.1,
        w2: w2.0,
        s2: w2.1,
        w3: w3.0,
        s3: w3.1,
    }
}

struct Weighted3<S1, S2, S3> {
    w1: usize,
    s1: S1,
    w2: usize,
    s2: S2,
    w3: usize,
    s3: S3,
}

impl<S1, S2, S3> Strategy for Weighted3<S1, S2, S3>
where
    S1: Strategy,
    S2: Strategy<Value = S1::Value>,
    S3: Strategy<Value = S1::Value>,
{
    type Value = S1::Value;

    fn generate<R: Rng + ?Sized, C: GenerationContext>(
        &self,
        rng: &mut R,
        context: &C,
    ) -> S1::Value {
        let total = self.w1 + self.w2 + self.w3;
        let mut choice = rng.random_range(0..total);

        if choice < self.w1 {
            return self.s1.generate(rng, context);
        }
        choice -= self.w1;

        if choice < self.w2 {
            self.s2.generate(rng, context)
        } else {
            self.s3.generate(rng, context)
        }
    }
}

/// Choose from strategies with weighted probabilities (4 strategies).
pub fn weighted_4<S1, S2, S3, S4>(
    w1: (usize, S1),
    w2: (usize, S2),
    w3: (usize, S3),
    w4: (usize, S4),
) -> impl Strategy<Value = S1::Value>
where
    S1: Strategy,
    S2: Strategy<Value = S1::Value>,
    S3: Strategy<Value = S1::Value>,
    S4: Strategy<Value = S1::Value>,
{
    Weighted4 {
        w1: w1.0,
        s1: w1.1,
        w2: w2.0,
        s2: w2.1,
        w3: w3.0,
        s3: w3.1,
        w4: w4.0,
        s4: w4.1,
    }
}

struct Weighted4<S1, S2, S3, S4> {
    w1: usize,
    s1: S1,
    w2: usize,
    s2: S2,
    w3: usize,
    s3: S3,
    w4: usize,
    s4: S4,
}

impl<S1, S2, S3, S4> Strategy for Weighted4<S1, S2, S3, S4>
where
    S1: Strategy,
    S2: Strategy<Value = S1::Value>,
    S3: Strategy<Value = S1::Value>,
    S4: Strategy<Value = S1::Value>,
{
    type Value = S1::Value;

    fn generate<R: Rng + ?Sized, C: GenerationContext>(
        &self,
        rng: &mut R,
        context: &C,
    ) -> S1::Value {
        let total = self.w1 + self.w2 + self.w3 + self.w4;
        let mut choice = rng.random_range(0..total);

        if choice < self.w1 {
            return self.s1.generate(rng, context);
        }
        choice -= self.w1;

        if choice < self.w2 {
            return self.s2.generate(rng, context);
        }
        choice -= self.w2;

        if choice < self.w3 {
            self.s3.generate(rng, context)
        } else {
            self.s4.generate(rng, context)
        }
    }
}

/// Try strategies in sequence with backtracking and retry limits.
///
/// Each strategy is attempted up to its retry limit. If a strategy returns `None`,
/// it's retried or the next strategy is attempted. Returns `None` if all strategies
/// are exhausted.
///
/// # Examples
///
/// ```ignore
/// // Try first strategy 5 times, then second strategy 3 times
/// let strategy = backtrack_2(
///     (5, filter_strategy.map(Some)),
///     (3, fallback_strategy.map(Some)),
/// );
/// ```
pub fn backtrack_2<T, S1, S2>(r1: (usize, S1), r2: (usize, S2)) -> impl Strategy<Value = Option<T>>
where
    S1: Strategy<Value = Option<T>>,
    S2: Strategy<Value = Option<T>>,
{
    Backtrack2 {
        max_retries1: r1.0,
        s1: r1.1,
        max_retries2: r2.0,
        s2: r2.1,
    }
}

struct Backtrack2<S1, S2> {
    max_retries1: usize,
    s1: S1,
    max_retries2: usize,
    s2: S2,
}

impl<S1, S2, T> Strategy for Backtrack2<S1, S2>
where
    S1: Strategy<Value = Option<T>>,
    S2: Strategy<Value = Option<T>>,
{
    type Value = Option<T>;

    fn generate<R: Rng + ?Sized, C: GenerationContext>(
        &self,
        rng: &mut R,
        context: &C,
    ) -> Option<T> {
        let mut retries1 = self.max_retries1;
        let mut retries2 = self.max_retries2;

        loop {
            let active_count = (retries1 > 0) as usize + (retries2 > 0) as usize;

            if active_count == 0 {
                return None;
            }

            // Pick a random active strategy
            let choice = rng.random_range(0..active_count);
            let mut idx = 0;

            if retries1 > 0 {
                if idx == choice {
                    if let Some(value) = self.s1.generate(rng, context) {
                        return Some(value);
                    }
                    retries1 -= 1;
                    continue;
                }
                idx += 1;
            }

            if retries2 > 0 {
                if idx == choice {
                    if let Some(value) = self.s2.generate(rng, context) {
                        return Some(value);
                    }
                    retries2 -= 1;
                    continue;
                }
            }
        }
    }
}

/// Try 3 strategies with backtracking.
pub fn backtrack_3<T, S1, S2, S3>(
    r1: (usize, S1),
    r2: (usize, S2),
    r3: (usize, S3),
) -> impl Strategy<Value = Option<T>>
where
    S1: Strategy<Value = Option<T>>,
    S2: Strategy<Value = Option<T>>,
    S3: Strategy<Value = Option<T>>,
{
    Backtrack3 {
        max_retries1: r1.0,
        s1: r1.1,
        max_retries2: r2.0,
        s2: r2.1,
        max_retries3: r3.0,
        s3: r3.1,
    }
}

struct Backtrack3<S1, S2, S3> {
    max_retries1: usize,
    s1: S1,
    max_retries2: usize,
    s2: S2,
    max_retries3: usize,
    s3: S3,
}

impl<S1, S2, S3, T> Strategy for Backtrack3<S1, S2, S3>
where
    S1: Strategy<Value = Option<T>>,
    S2: Strategy<Value = Option<T>>,
    S3: Strategy<Value = Option<T>>,
{
    type Value = Option<T>;

    fn generate<R: Rng + ?Sized, C: GenerationContext>(
        &self,
        rng: &mut R,
        context: &C,
    ) -> Option<T> {
        let mut retries1 = self.max_retries1;
        let mut retries2 = self.max_retries2;
        let mut retries3 = self.max_retries3;

        loop {
            let active_count = (retries1 > 0) as usize
                + (retries2 > 0) as usize
                + (retries3 > 0) as usize;

            if active_count == 0 {
                return None;
            }

            let choice = rng.random_range(0..active_count);
            let mut idx = 0;

            if retries1 > 0 {
                if idx == choice {
                    if let Some(value) = self.s1.generate(rng, context) {
                        return Some(value);
                    }
                    retries1 -= 1;
                    continue;
                }
                idx += 1;
            }

            if retries2 > 0 {
                if idx == choice {
                    if let Some(value) = self.s2.generate(rng, context) {
                        return Some(value);
                    }
                    retries2 -= 1;
                    continue;
                }
                idx += 1;
            }

            if retries3 > 0 {
                if idx == choice {
                    if let Some(value) = self.s3.generate(rng, context) {
                        return Some(value);
                    }
                    retries3 -= 1;
                    continue;
                }
            }
        }
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

    // ========================================================================
    // Choice combinator tests
    // ========================================================================

    #[test]
    fn test_one_of_2() {
        let mut rng = ChaCha8Rng::seed_from_u64(42);
        let context = TestContext::default();

        let strategy = one_of_2(just(1), just(2));

        // Generate multiple values to ensure both are possible
        let mut seen = std::collections::HashSet::new();
        for _ in 0..100 {
            let value = strategy.generate(&mut rng, &context);
            assert!(value == 1 || value == 2);
            seen.insert(value);
        }

        // Both values should appear at least once
        assert_eq!(seen.len(), 2);
    }

    #[test]
    fn test_one_of_3() {
        let mut rng = ChaCha8Rng::seed_from_u64(42);
        let context = TestContext::default();

        let strategy = one_of_3(just(1), just(2), just(3));

        let mut seen = std::collections::HashSet::new();
        for _ in 0..100 {
            let value = strategy.generate(&mut rng, &context);
            assert!(value == 1 || value == 2 || value == 3);
            seen.insert(value);
        }

        assert_eq!(seen.len(), 3);
    }

    #[test]
    fn test_one_of_4() {
        let mut rng = ChaCha8Rng::seed_from_u64(42);
        let context = TestContext::default();

        let strategy = one_of_4(just(1), just(2), just(3), just(4));

        let mut seen = std::collections::HashSet::new();
        for _ in 0..100 {
            let value = strategy.generate(&mut rng, &context);
            assert!(value >= 1 && value <= 4);
            seen.insert(value);
        }

        assert_eq!(seen.len(), 4);
    }

    #[test]
    fn test_weighted_2() {
        let mut rng = ChaCha8Rng::seed_from_u64(42);
        let context = TestContext::default();

        // 3:1 ratio
        let strategy = weighted_2((3, just(1)), (1, just(2)));

        let mut count1 = 0;
        let mut count2 = 0;

        for _ in 0..1000 {
            match strategy.generate(&mut rng, &context) {
                1 => count1 += 1,
                2 => count2 += 1,
                _ => panic!("unexpected value"),
            }
        }

        // With 3:1 ratio, we expect roughly 750:250
        // Allow for some variance (600-900 for value 1)
        assert!(count1 > 600 && count1 < 900);
        assert!(count2 > 100 && count2 < 400);
    }

    #[test]
    fn test_weighted_3() {
        let mut rng = ChaCha8Rng::seed_from_u64(42);
        let context = TestContext::default();

        // 5:3:2 ratio
        let strategy = weighted_3((5, just(1)), (3, just(2)), (2, just(3)));

        let mut counts = vec![0, 0, 0];

        for _ in 0..1000 {
            let value = strategy.generate(&mut rng, &context);
            counts[(value - 1) as usize] += 1;
        }

        // Value 1 should be most common, value 3 least common
        assert!(counts[0] > counts[1]);
        assert!(counts[1] > counts[2]);
    }

    #[test]
    fn test_weighted_4() {
        let mut rng = ChaCha8Rng::seed_from_u64(42);
        let context = TestContext::default();

        let strategy = weighted_4((1, just(1)), (1, just(2)), (1, just(3)), (1, just(4)));

        let mut seen = std::collections::HashSet::new();
        for _ in 0..100 {
            let value = strategy.generate(&mut rng, &context);
            seen.insert(value);
        }

        // All 4 values should appear
        assert_eq!(seen.len(), 4);
    }

    #[test]
    fn test_backtrack_2_success() {
        let mut rng = ChaCha8Rng::seed_from_u64(42);
        let context = TestContext::default();

        // Both strategies always succeed
        let strategy = backtrack_2((5, just(Some(1))), (5, just(Some(2))));

        let result = strategy.generate(&mut rng, &context);
        assert!(result == Some(1) || result == Some(2));
    }

    #[test]
    fn test_backtrack_2_exhaustion() {
        let mut rng = ChaCha8Rng::seed_from_u64(42);
        let context = TestContext::default();

        // Both strategies always fail
        let strategy = backtrack_2::<i32, _, _>((3, just(None)), (3, just(None)));

        let result = strategy.generate(&mut rng, &context);
        assert_eq!(result, None);
    }

    #[test]
    fn test_backtrack_2_fallback() {
        let mut rng = ChaCha8Rng::seed_from_u64(42);
        let context = TestContext::default();

        // First strategy always fails, second always succeeds
        let strategy = backtrack_2::<i32, _, _>((5, just(None)), (5, just(Some(42))));

        let result = strategy.generate(&mut rng, &context);
        assert_eq!(result, Some(42));
    }

    #[test]
    fn test_backtrack_3() {
        let mut rng = ChaCha8Rng::seed_from_u64(42);
        let context = TestContext::default();

        // First two fail, third succeeds
        let strategy = backtrack_3::<i32, _, _, _>((2, just(None)), (2, just(None)), (2, just(Some(99))));

        let result = strategy.generate(&mut rng, &context);
        assert_eq!(result, Some(99));
    }

    #[test]
    fn test_choice_with_map() {
        let mut rng = ChaCha8Rng::seed_from_u64(42);
        let context = TestContext::default();

        // Combine choice with map
        let strategy = one_of_2(just(1), just(2)).map(|x| x * 10);

        let value = strategy.generate(&mut rng, &context);
        assert!(value == 10 || value == 20);
    }

    #[test]
    fn test_choice_with_flat_map() {
        let mut rng = ChaCha8Rng::seed_from_u64(42);
        let context = TestContext::default();

        // Generate a choice, then use it to pick another strategy
        let strategy = one_of_2(just(1), just(2)).flat_map(|x| {
            if x == 1 {
                just(10)
            } else {
                just(20)
            }
        });

        let value = strategy.generate(&mut rng, &context);
        assert!(value == 10 || value == 20);
    }

    #[test]
    fn test_nested_choice() {
        let mut rng = ChaCha8Rng::seed_from_u64(42);
        let context = TestContext::default();

        // Nested one_of
        let inner1 = one_of_2(just(1), just(2));
        let inner2 = one_of_2(just(3), just(4));
        let strategy = one_of_2(inner1, inner2);

        let mut seen = std::collections::HashSet::new();
        for _ in 0..200 {
            let value = strategy.generate(&mut rng, &context);
            assert!(value >= 1 && value <= 4);
            seen.insert(value);
        }

        // All 4 values should eventually appear
        assert_eq!(seen.len(), 4);
    }
}
