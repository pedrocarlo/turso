//! Compile-time-derived bounds on `ProgramBuilder` emissions.
//!
//! The [`turso_macros::emission_count`] attribute macro statically counts how many
//! instructions, labels and cursors a translate function emits and generates
//! `<fn>_EMISSIONS` constants of type [`EmissionBound`]. Those constants feed
//! [`ProgramBuilderOpts`] so the builder can preallocate, and they can never drift
//! from the emission code they describe: adding an `emit_insn` call to an annotated
//! function updates the constant at compile time.
//!
//! Counts are *upper bounds*, not exact values: branches contribute the maximum of
//! their arms and early returns simply emit less than the bound. Loops cannot be
//! bounded statically, so the macro splits them out into `<fn>_EMISSIONS_LOOP<i>`
//! per-iteration constants and requires a `#[emissions(per_iter = <expr>)]`
//! annotation supplying the iteration count.
//!
//! In debug builds the macro additionally wraps annotated functions to compare the
//! estimate against the actual emission delta (via [`ProgramBuilder::emission_snapshot`])
//! and reports through [`check_emission_estimate`], so every test run validates the
//! bounds instead of them being eyeballed.
//!
//! [`ProgramBuilder::emission_snapshot`]: super::builder::ProgramBuilder::emission_snapshot

use super::builder::ProgramBuilderOpts;

/// An upper bound on (or snapshot of) the instructions, labels and cursors a piece
/// of translation code emits into a `ProgramBuilder`.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct EmissionBound {
    pub insns: usize,
    pub labels: usize,
    pub cursors: usize,
}

impl EmissionBound {
    pub const ZERO: Self = Self::new(0, 0, 0);

    pub const fn new(insns: usize, labels: usize, cursors: usize) -> Self {
        Self {
            insns,
            labels,
            cursors,
        }
    }

    pub const fn plus(self, other: Self) -> Self {
        Self {
            insns: self.insns + other.insns,
            labels: self.labels + other.labels,
            cursors: self.cursors + other.cursors,
        }
    }

    /// Scale a per-iteration bound by an iteration count.
    pub const fn times(self, n: usize) -> Self {
        Self {
            insns: self.insns * n,
            labels: self.labels * n,
            cursors: self.cursors * n,
        }
    }

    /// Component-wise maximum: a sound joint upper bound for two mutually
    /// exclusive branches.
    pub const fn max(self, other: Self) -> Self {
        const fn max(a: usize, b: usize) -> usize {
            if a > b {
                a
            } else {
                b
            }
        }
        Self {
            insns: max(self.insns, other.insns),
            labels: max(self.labels, other.labels),
            cursors: max(self.cursors, other.cursors),
        }
    }

    pub const fn saturating_sub(self, other: Self) -> Self {
        Self {
            insns: self.insns.saturating_sub(other.insns),
            labels: self.labels.saturating_sub(other.labels),
            cursors: self.cursors.saturating_sub(other.cursors),
        }
    }

    /// Whether this bound is large enough to cover `other` in every component.
    pub const fn covers(self, other: Self) -> bool {
        self.insns >= other.insns && self.labels >= other.labels && self.cursors >= other.cursors
    }

    pub const fn to_opts(self) -> ProgramBuilderOpts {
        ProgramBuilderOpts {
            num_cursors: self.cursors,
            approx_num_insns: self.insns,
            approx_num_labels: self.labels,
        }
    }
}

/// Debug-build hook called by `#[emission_count]`-generated wrappers: compares the
/// macro-derived estimate against the emission delta actually observed while the
/// annotated function ran.
///
/// An exceeded estimate is not a correctness bug (the builder just reallocates), so
/// this warns instead of asserting. Filter on the `emission_estimate` tracing target
/// to audit estimate tightness across a test run.
pub fn check_emission_estimate(
    fn_name: &'static str,
    estimate: EmissionBound,
    base: EmissionBound,
    end: EmissionBound,
) {
    let actual = end.saturating_sub(base);
    if !estimate.covers(actual) {
        tracing::warn!(
            target: "emission_estimate",
            fn_name,
            ?estimate,
            ?actual,
            "emission estimate exceeded; the #[emission_count] bound misses emissions \
             (unannotated callee, un-composed helper, or a loop per_iter that is too small)"
        );
    } else {
        tracing::trace!(
            target: "emission_estimate",
            fn_name,
            ?estimate,
            ?actual,
            "emission estimate held"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::EmissionBound;
    use crate::vdbe::builder::{CursorType, ProgramBuilder, ProgramBuilderOpts, QueryMode};

    fn test_builder() -> ProgramBuilder {
        ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 0, 0))
    }

    fn delta(program: &ProgramBuilder, base: EmissionBound) -> EmissionBound {
        program.emission_snapshot().saturating_sub(base)
    }

    #[turso_macros::emission_count]
    fn emit_straight_line(program: &mut ProgramBuilder) {
        program.emit_int(1, 1);
        program.emit_int(2, 2);
        program.allocate_label();
        program.alloc_cursor_id(CursorType::Sorter);
    }

    #[test]
    fn straight_line_count_is_exact() {
        let mut program = test_builder();
        let base = program.emission_snapshot();
        emit_straight_line(&mut program);
        assert_eq!(emit_straight_line_EMISSIONS, EmissionBound::new(2, 1, 1));
        assert_eq!(delta(&program, base), emit_straight_line_EMISSIONS);
    }

    #[turso_macros::emission_count]
    fn emit_branchy(program: &mut ProgramBuilder, take_big: bool) {
        if take_big {
            program.emit_int(1, 1);
            program.emit_int(2, 2);
            program.emit_int(3, 3);
        } else {
            program.emit_null(1, None);
        }
    }

    #[test]
    fn branches_contribute_max_of_arms() {
        assert_eq!(emit_branchy_EMISSIONS, EmissionBound::new(3, 0, 0));
        for take_big in [false, true] {
            let mut program = test_builder();
            let base = program.emission_snapshot();
            emit_branchy(&mut program, take_big);
            assert!(emit_branchy_EMISSIONS.covers(delta(&program, base)));
        }
    }

    #[turso_macros::emission_count]
    fn emit_leaf(program: &mut ProgramBuilder) {
        program.emit_int(1, 1);
        program.emit_int(2, 2);
    }

    #[turso_macros::emission_count(compose(emit_leaf))]
    fn emit_composed(program: &mut ProgramBuilder) {
        emit_leaf(program);
        program.allocate_label();
    }

    #[test]
    fn composed_callee_counts_are_included() {
        assert_eq!(emit_leaf_EMISSIONS, EmissionBound::new(2, 0, 0));
        assert_eq!(emit_composed_EMISSIONS, EmissionBound::new(2, 1, 0));
        let mut program = test_builder();
        let base = program.emission_snapshot();
        emit_composed(&mut program);
        assert_eq!(delta(&program, base), emit_composed_EMISSIONS);
    }

    #[turso_macros::emission_count(compose(emit_leaf))]
    fn emit_matchy(program: &mut ProgramBuilder, choice: u8) {
        match choice {
            0 => emit_leaf(program),
            1 => program.emit_int(1, 1),
            _ => {}
        }
    }

    #[test]
    fn symbolic_branch_max_resolves_at_const_eval() {
        // max(emit_leaf_EMISSIONS = 2 insns, 1 insn, 0) folds to 2 via const eval.
        assert_eq!(emit_matchy_EMISSIONS, EmissionBound::new(2, 0, 0));
        for choice in 0u8..3 {
            let mut program = test_builder();
            let base = program.emission_snapshot();
            emit_matchy(&mut program, choice);
            assert!(emit_matchy_EMISSIONS.covers(delta(&program, base)));
        }
    }

    #[turso_macros::emission_count]
    fn emit_loopy(program: &mut ProgramBuilder, n: usize) {
        program.allocate_label();
        #[emissions(per_iter = n)]
        for i in 0..n {
            program.emit_int(i as i64, i + 1);
        }
    }

    #[test]
    fn loops_are_bounded_by_per_iter_annotation() {
        assert_eq!(emit_loopy_EMISSIONS, EmissionBound::new(0, 1, 0));
        assert_eq!(emit_loopy_EMISSIONS_LOOP0, EmissionBound::new(1, 0, 0));
        let n = 5;
        let mut program = test_builder();
        let base = program.emission_snapshot();
        emit_loopy(&mut program, n);
        let estimate = emit_loopy_EMISSIONS.plus(emit_loopy_EMISSIONS_LOOP0.times(n));
        assert_eq!(delta(&program, base), estimate);
    }

    #[turso_macros::emission_count]
    fn emit_early_return(program: &mut ProgramBuilder, bail: bool) -> crate::Result<()> {
        program.emit_int(1, 1);
        if bail {
            return Ok(());
        }
        program.emit_int(2, 2);
        program.emit_int(3, 3);
        Ok(())
    }

    #[test]
    fn early_returns_stay_within_bound() {
        assert_eq!(emit_early_return_EMISSIONS, EmissionBound::new(3, 0, 0));
        for bail in [false, true] {
            let mut program = test_builder();
            let base = program.emission_snapshot();
            emit_early_return(&mut program, bail).unwrap();
            assert!(emit_early_return_EMISSIONS.covers(delta(&program, base)));
        }
    }
}
