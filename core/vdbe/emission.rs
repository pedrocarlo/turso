//! Composable, lazily-emitted bytecode with exact instruction counts.
//!
//! An [`Emission`] is a value describing instructions to append to a
//! [`ProgramBuilder`], built up from plain [`Insn`]s with combinators the way
//! iterators are built from adapters: tuples sequence emissions, [`Either`]
//! selects between shapes, [`Repeat`] emits once per item of an iterator.
//!
//! The point of the indirection is the **exactness contract**: [`Emission::size`]
//! returns exactly the number of instructions [`Emission::emit`] appends, so
//! [`ProgramBuilder::emit_all`] can reserve the precise capacity up front.
//! Exactness holds *by construction* rather than by analysis:
//!
//! - branches are resolved to one concrete arm when the [`Either`] is built;
//! - loops are built over concrete [`ExactSizeIterator`]s;
//! - all fallible work (schema lookups, `?`) happens while *constructing* the
//!   emission value — [`Emission::emit`] is infallible, like
//!   [`ProgramBuilder::emit_insn`].
//!
//! Registers, labels and cursors are still allocated eagerly on the builder
//! while the emission value is constructed (they are cheap counter bumps);
//! only instruction emission is deferred. Jump targets therefore work
//! unchanged: allocate a label up front, embed it in [`Insn`]s, and place a
//! [`ResolveLabel`] node in the composition where the label should land.
//!
//! [`ProgramBuilder::emit_all`] debug-asserts the contract, so any impl whose
//! `size` disagrees with its `emit` fails every test that translates through it.
//!
//! Future extension point (not needed by current users, add when the first
//! conversion requires it): an `Effect` node — a size-0 wrapper around
//! `FnOnce(&mut ProgramBuilder)` for builder side-channel mutations between
//! instructions (`reg_result_cols_start`, constant spans), debug-asserted to
//! emit nothing.

use super::builder::ProgramBuilder;
use super::insn::Insn;
use super::BranchOffset;

/// A composed piece of bytecode: reports the exact number of instructions it
/// will append to a [`ProgramBuilder`], then appends them.
pub trait Emission: Sized {
    /// Exactly how many instructions [`Self::emit`] will append.
    fn size(&self) -> usize;

    /// Append exactly [`Self::size`] instructions to `program`.
    ///
    /// Prefer [`ProgramBuilder::emit_all`], which reserves capacity first and
    /// debug-asserts the exactness contract.
    fn emit(self, program: &mut ProgramBuilder);
}

/// An [`Emission`] whose instruction count is a compile-time constant.
///
/// This is what lets [`Repeat`] compute its size as `iter.len() * E::SIZE`
/// without constructing each item's emission twice.
pub trait StaticEmission: Emission {
    const SIZE: usize;
}

impl Emission for Insn {
    fn size(&self) -> usize {
        1
    }

    fn emit(self, program: &mut ProgramBuilder) {
        // Must go through emit_insn: it maintains the program's readonly flag
        // and tracing, not just the insns vec.
        program.emit_insn(self);
    }
}

impl StaticEmission for Insn {
    const SIZE: usize = 1;
}

impl Emission for () {
    fn size(&self) -> usize {
        0
    }

    fn emit(self, _program: &mut ProgramBuilder) {}
}

impl StaticEmission for () {
    const SIZE: usize = 0;
}

impl<E: Emission> Emission for Option<E> {
    fn size(&self) -> usize {
        self.as_ref().map_or(0, Emission::size)
    }

    fn emit(self, program: &mut ProgramBuilder) {
        if let Some(emission) = self {
            emission.emit(program);
        }
    }
}

/// Exactly one of two emission shapes, chosen when the value is constructed —
/// which is what keeps branchy emissions exact rather than upper-bounded.
/// Nest (`Either<A, Either<B, C>>`) for three or more shapes.
pub enum Either<A, B> {
    Left(A),
    Right(B),
}

impl<A: Emission, B: Emission> Emission for Either<A, B> {
    fn size(&self) -> usize {
        match self {
            Self::Left(a) => a.size(),
            Self::Right(b) => b.size(),
        }
    }

    fn emit(self, program: &mut ProgramBuilder) {
        match self {
            Self::Left(a) => a.emit(program),
            Self::Right(b) => b.emit(program),
        }
    }
}

/// Tuples are the sequencing combinator: emit left-to-right, size is the sum.
/// Nest tuples for more than eight elements.
macro_rules! impl_emission_tuple {
    ($($name:ident),+) => {
        impl<$($name: Emission),+> Emission for ($($name,)+) {
            fn size(&self) -> usize {
                #[allow(non_snake_case)]
                let ($($name,)+) = self;
                0 $(+ $name.size())+
            }

            fn emit(self, program: &mut ProgramBuilder) {
                #[allow(non_snake_case)]
                let ($($name,)+) = self;
                $($name.emit(program);)+
            }
        }

        impl<$($name: StaticEmission),+> StaticEmission for ($($name,)+) {
            const SIZE: usize = 0 $(+ $name::SIZE)+;
        }
    };
}

impl_emission_tuple!(A);
impl_emission_tuple!(A, B);
impl_emission_tuple!(A, B, C);
impl_emission_tuple!(A, B, C, D);
impl_emission_tuple!(A, B, C, D, E);
impl_emission_tuple!(A, B, C, D, E, F);
impl_emission_tuple!(A, B, C, D, E, F, G);
impl_emission_tuple!(A, B, C, D, E, F, G, H);

/// One statically-sized emission per item of a concrete iterator:
/// `size = iter.len() * E::SIZE`, so items are built exactly once, at emit time.
pub struct Repeat<I, F> {
    iter: I,
    f: F,
}

impl<I, F> Repeat<I, F> {
    pub fn new<E>(iter: impl IntoIterator<IntoIter = I>, f: F) -> Self
    where
        I: ExactSizeIterator,
        F: FnMut(I::Item) -> E,
        E: StaticEmission,
    {
        Self {
            iter: iter.into_iter(),
            f,
        }
    }
}

impl<I, F, E> Emission for Repeat<I, F>
where
    I: ExactSizeIterator,
    F: FnMut(I::Item) -> E,
    E: StaticEmission,
{
    fn size(&self) -> usize {
        self.iter.len() * E::SIZE
    }

    fn emit(self, program: &mut ProgramBuilder) {
        let Self { iter, mut f } = self;
        for item in iter {
            f(item).emit(program);
        }
    }
}

/// Size-0 node that resolves `label` to whatever instruction the next node in
/// the composition emits, via [`ProgramBuilder::preassign_label_to_next_insn`].
/// The label itself is allocated eagerly with [`ProgramBuilder::allocate_label`]
/// while the emission value is being constructed.
#[allow(dead_code)] // remove when the first control-flow conversion lands
pub struct ResolveLabel(pub BranchOffset);

impl Emission for ResolveLabel {
    fn size(&self) -> usize {
        0
    }

    fn emit(self, program: &mut ProgramBuilder) {
        program.preassign_label_to_next_insn(self.0);
    }
}

impl StaticEmission for ResolveLabel {
    const SIZE: usize = 0;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vdbe::builder::{ProgramBuilder, ProgramBuilderOpts, QueryMode};

    fn test_builder() -> ProgramBuilder {
        ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 0, 0))
    }

    fn null(dest: usize) -> Insn {
        Insn::Null {
            dest,
            dest_end: None,
        }
    }

    fn integer(value: i64) -> Insn {
        Insn::Integer { value, dest: 1 }
    }

    /// Emitted integer values, for asserting emission order.
    fn emitted_integers(program: &ProgramBuilder) -> Vec<i64> {
        program
            .insns
            .iter()
            .map(|(insn, _)| match insn {
                Insn::Integer { value, .. } => *value,
                other => panic!("expected only Integer insns, got {other:?}"),
            })
            .collect()
    }

    #[test]
    fn insn_size_is_one_and_emits_itself() {
        let mut program = test_builder();
        let emission = null(1);
        assert_eq!(emission.size(), 1);
        program.emit_all(emission);
        assert_eq!(program.insns.len(), 1);
        assert!(matches!(program.insns[0].0, Insn::Null { dest: 1, .. }));
    }

    #[test]
    fn unit_and_option_sizes() {
        assert_eq!(().size(), 0);
        assert_eq!(None::<Insn>.size(), 0);
        assert_eq!(Some(null(1)).size(), 1);

        let mut program = test_builder();
        program.emit_all(((), None::<Insn>, Some(null(1))));
        assert_eq!(program.insns.len(), 1);
    }

    #[test]
    fn tuple_sums_size_and_emits_in_order() {
        let emission = (integer(1), (), Some(integer(2)), (integer(3), integer(4)));
        assert_eq!(emission.size(), 4);
        let mut program = test_builder();
        program.emit_all(emission);
        assert_eq!(emitted_integers(&program), vec![1, 2, 3, 4]);
    }

    #[test]
    fn either_size_matches_chosen_arm() {
        let left: Either<Insn, (Insn, Insn)> = Either::Left(integer(1));
        assert_eq!(left.size(), 1);
        let right: Either<Insn, (Insn, Insn)> = Either::Right((integer(2), integer(3)));
        assert_eq!(right.size(), 2);

        let mut program = test_builder();
        program.emit_all(left);
        program.emit_all(right);
        assert_eq!(emitted_integers(&program), vec![1, 2, 3]);
    }

    #[test]
    fn repeat_size_is_len_times_static_size() {
        let emission = Repeat::new(0..5usize, |i| (integer(i as i64), integer(i as i64 + 100)));
        assert_eq!(emission.size(), 10);
        let mut program = test_builder();
        program.emit_all(emission);
        assert_eq!(
            emitted_integers(&program),
            vec![0, 100, 1, 101, 2, 102, 3, 103, 4, 104]
        );

        let empty = Repeat::new(std::iter::empty::<i64>(), integer);
        assert_eq!(empty.size(), 0);
        program.emit_all(empty);
        assert_eq!(program.insns.len(), 10);
    }

    #[test]
    fn static_size_of_tuples_folds() {
        assert_eq!(<(Insn, (Insn, ())) as StaticEmission>::SIZE, 2);
        assert_eq!(<((), ResolveLabel) as StaticEmission>::SIZE, 0);
    }

    #[test]
    fn emit_all_reserves_exactly() {
        let mut program = test_builder();
        assert_eq!(program.insns.capacity(), 0);
        let emission = Repeat::new(0..7usize, |i| integer(i as i64));
        let size = emission.size();
        program.emit_all(emission);
        assert_eq!(program.insns.len(), size);
        assert!(program.insns.capacity() >= size);
    }

    #[test]
    fn resolve_label_anchors_next_insn() {
        let mut program = test_builder();
        let label = program.allocate_label();
        let emission = (
            Insn::Goto { target_pc: label },
            null(1),
            ResolveLabel(label),
            null(2),
        );
        assert_eq!(emission.size(), 3);
        program.emit_all(emission);
        program.resolve_labels().unwrap();
        // The label must resolve to the insn following ResolveLabel: offset 2.
        let Insn::Goto { target_pc } = program.insns[0].0 else {
            panic!("expected Goto at offset 0");
        };
        assert_eq!(target_pc.as_offset_int(), 2);
    }
}
