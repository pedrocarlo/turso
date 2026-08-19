use std::ops::ControlFlow;

use crate::error::{SQLITE_CONSTRAINT_TRIGGER, SQLITE_ERROR};
#[cfg(feature = "json")]
use crate::function::JsonFunc;
use crate::function::{Func, FuncCtx, MathFuncArity, ScalarFunc};
use crate::translate::{expr, semantic::hir};
use crate::util::parse_numeric_literal;
use crate::vdbe::{
    builder::ProgramBuilder,
    insn::{CmpInsFlags, Insn},
    BranchOffset,
};
use crate::{LimboError, Numeric, Result, Value};
use turso_parser::ast::{Literal, Operator, ResolveType, UnaryOperator};

#[derive(Clone, Copy)]
enum BinaryOperands {
    Shared(usize),
    Pair { lhs: usize, rhs: usize },
}

#[derive(Clone, Copy)]
struct BetweenRegisters {
    value: usize,
    start: usize,
    end: usize,
    width: usize,
    lower: usize,
    upper: usize,
}

#[derive(Clone, Copy)]
struct CaseRegisters {
    base: Option<usize>,
    when: usize,
    return_label: BranchOffset,
    next_label: BranchOffset,
}

#[derive(Clone, Copy)]
struct InListRegisters {
    lhs: usize,
    rhs: usize,
    width: usize,
    null_flag: usize,
    result: usize,
    match_label: BranchOffset,
    false_label: BranchOffset,
    null_label: BranchOffset,
}

#[derive(Clone, Copy)]
struct LikeRegisters {
    start: usize,
    result: usize,
}

#[derive(Clone, Copy)]
struct ConcatWsRegisters {
    result: usize,
    arguments: usize,
}

#[derive(Clone, Copy)]
struct IfNullRegisters {
    value: usize,
    copy_label: BranchOffset,
}

#[derive(Clone, Copy)]
struct IifRegisters {
    condition: usize,
    end_label: BranchOffset,
    false_label: BranchOffset,
}

#[derive(Clone, Copy)]
struct CoalesceRegisters {
    end_label: BranchOffset,
}

#[derive(Clone, Copy)]
struct SubscriptRegisters {
    base: usize,
    index: usize,
}

#[derive(Clone, Copy)]
enum ExprRegisters {
    None,
    Binary(BinaryOperands),
    Between(BetweenRegisters),
    Case(CaseRegisters),
    InList(InListRegisters),
    Like(LikeRegisters),
    ConcatWs(ConcatWsRegisters),
    IfNull(IfNullRegisters),
    Iif(IifRegisters),
    Coalesce(CoalesceRegisters),
    Array(usize),
    Subscript(SubscriptRegisters),
    FieldAccess(usize),
    RaiseMessage(usize),
    Function(usize),
}

enum NullTest {
    IsNull,
    NotNull,
}

struct LoweringContext {
    target: usize,
    registers: ExprRegisters,
}

impl LoweringContext {
    const fn new(target: usize) -> Self {
        Self {
            target,
            registers: ExprRegisters::None,
        }
    }
}

fn binary_registers(registers: ExprRegisters) -> BinaryOperands {
    let ExprRegisters::Binary(operands) = registers else {
        unreachable!("binary operand registers were allocated")
    };
    operands
}

fn comparison_flags(component: &hir::ComparisonComponent) -> CmpInsFlags {
    let flags = CmpInsFlags::default().with_affinity(component.affinity);
    if component.array {
        flags.array_cmp()
    } else {
        flags
    }
}

fn comparison_collation(
    component: &hir::ComparisonComponent,
) -> Option<crate::translate::collate::CollationSeq> {
    component
        .collation
        .as_ref()
        .map(|collation| *collation.value())
}

#[derive(Clone, Copy)]
enum EmptyArgumentStart {
    UseTarget,
    UseZero,
    UseNextRegister,
    ReserveOneRegister,
}

fn plain_function_lowering(function: &Func) -> Option<EmptyArgumentStart> {
    match function {
        Func::External(_) | Func::Dialect(_) | Func::Vector(_) => {
            Some(EmptyArgumentStart::UseTarget)
        }
        #[cfg(all(feature = "fts", not(target_family = "wasm")))]
        Func::Fts(_) => Some(EmptyArgumentStart::UseNextRegister),
        Func::Math(function) if matches!(function.arity(), MathFuncArity::Nullary) => {
            Some(EmptyArgumentStart::UseZero)
        }
        Func::Math(_) => Some(EmptyArgumentStart::UseTarget),
        Func::Scalar(
            ScalarFunc::Changes
            | ScalarFunc::TotalChanges
            | ScalarFunc::Random
            | ScalarFunc::Date
            | ScalarFunc::DateTime
            | ScalarFunc::JulianDay
            | ScalarFunc::UnixEpoch
            | ScalarFunc::Time
            | ScalarFunc::StrfTime,
        ) => Some(EmptyArgumentStart::ReserveOneRegister),
        #[cfg(feature = "test_helper")]
        Func::Scalar(ScalarFunc::TestNondetCounter) => Some(EmptyArgumentStart::ReserveOneRegister),
        Func::Scalar(
            ScalarFunc::Abs
            | ScalarFunc::Like
            | ScalarFunc::Glob
            | ScalarFunc::Lower
            | ScalarFunc::Upper
            | ScalarFunc::Length
            | ScalarFunc::OctetLength
            | ScalarFunc::Typeof
            | ScalarFunc::Unicode
            | ScalarFunc::Unistr
            | ScalarFunc::UnistrQuote
            | ScalarFunc::Quote
            | ScalarFunc::RandomBlob
            | ScalarFunc::Sign
            | ScalarFunc::Soundex
            | ScalarFunc::ZeroBlob
            | ScalarFunc::SequenceWatermark
            | ScalarFunc::TimeDiff
            | ScalarFunc::Hex
            | ScalarFunc::Nullif
            | ScalarFunc::Instr
            | ScalarFunc::Replace
            | ScalarFunc::Trim
            | ScalarFunc::LTrim
            | ScalarFunc::RTrim
            | ScalarFunc::Round
            | ScalarFunc::Unhex
            | ScalarFunc::Min
            | ScalarFunc::Max
            | ScalarFunc::Concat
            | ScalarFunc::Char
            | ScalarFunc::Printf
            | ScalarFunc::GetByte
            | ScalarFunc::SetByte
            | ScalarFunc::ArrayLength
            | ScalarFunc::ArrayAppend
            | ScalarFunc::ArrayPrepend
            | ScalarFunc::ArrayCat
            | ScalarFunc::ArrayRemove
            | ScalarFunc::ArrayContains
            | ScalarFunc::ArrayPosition
            | ScalarFunc::ArraySlice
            | ScalarFunc::StringToArray
            | ScalarFunc::ArrayToString
            | ScalarFunc::ArrayOverlap
            | ScalarFunc::ArrayContainsAll
            | ScalarFunc::TestUintEncode
            | ScalarFunc::TestUintDecode
            | ScalarFunc::TestUintAdd
            | ScalarFunc::TestUintSub
            | ScalarFunc::TestUintMul
            | ScalarFunc::TestUintDiv
            | ScalarFunc::TestUintLt
            | ScalarFunc::TestUintEq
            | ScalarFunc::StringReverse
            | ScalarFunc::Gcd
            | ScalarFunc::Lcm
            | ScalarFunc::Repeat
            | ScalarFunc::Lpad
            | ScalarFunc::Rpad
            | ScalarFunc::BooleanToInt
            | ScalarFunc::IntToBoolean
            | ScalarFunc::ValidateIpAddr
            | ScalarFunc::NumericEncode
            | ScalarFunc::NumericDecode
            | ScalarFunc::NumericAdd
            | ScalarFunc::NumericSub
            | ScalarFunc::NumericMul
            | ScalarFunc::NumericDiv
            | ScalarFunc::NumericLt
            | ScalarFunc::NumericEq
            | ScalarFunc::TableColumnsJsonArray
            | ScalarFunc::BinRecordJsonObject,
        ) => Some(EmptyArgumentStart::UseNextRegister),
        #[cfg(all(feature = "fs", not(target_family = "wasm")))]
        Func::Scalar(ScalarFunc::LoadExtension) => Some(EmptyArgumentStart::UseNextRegister),
        #[cfg(feature = "json")]
        Func::Json(JsonFunc::JsonRemove) => Some(EmptyArgumentStart::ReserveOneRegister),
        #[cfg(feature = "json")]
        Func::Json(function) if !function.is_internal() => {
            Some(EmptyArgumentStart::UseNextRegister)
        }
        _ => None,
    }
}

struct ExprLowerer<'a> {
    program: &'a mut ProgramBuilder,
}

impl hir::ExprVisitor for ExprLowerer<'_> {
    type Context = LoweringContext;
    type Output = usize;
    type Error = LimboError;

    fn pre_order(
        &mut self,
        parent: &hir::Expr,
        context: &mut LoweringContext,
        child_index: usize,
        child: &hir::Expr,
    ) -> Result<ControlFlow<(), LoweringContext>> {
        let target = match parent {
            hir::Expr::Unary { operator, .. } => match operator {
                UnaryOperator::Positive => context.target,
                UnaryOperator::Negative
                    if matches!(child, hir::Expr::Literal(Literal::Numeric(_))) =>
                {
                    return Ok(ControlFlow::Break(()));
                }
                UnaryOperator::BitwiseNot
                    if matches!(
                        child,
                        hir::Expr::Literal(Literal::Numeric(_) | Literal::Null)
                    ) =>
                {
                    return Ok(ControlFlow::Break(()));
                }
                UnaryOperator::Negative | UnaryOperator::BitwiseNot | UnaryOperator::Not => {
                    self.program.alloc_register()
                }
            },
            hir::Expr::IsNull(_) | hir::Expr::NotNull(_) | hir::Expr::TruthTest { .. } => {
                self.program.alloc_register()
            }
            hir::Expr::Collate { .. } => context.target,
            hir::Expr::Cast { .. } => {
                if child_index == 0 {
                    context.target
                } else {
                    return Ok(ControlFlow::Break(()));
                }
            }
            hir::Expr::Binary {
                lhs,
                rhs,
                comparison,
                ..
            } => {
                if matches!(context.registers, ExprRegisters::None) {
                    let width = comparison
                        .as_ref()
                        .map_or(1, |comparison| comparison.components.len());
                    let operands = if lhs.equivalent(rhs) {
                        BinaryOperands::Shared(self.program.alloc_registers(width))
                    } else {
                        let lhs = self.program.alloc_registers(width * 2);
                        BinaryOperands::Pair {
                            lhs,
                            rhs: lhs + width,
                        }
                    };
                    context.registers = ExprRegisters::Binary(operands);
                }
                match (context.registers, child_index) {
                    (ExprRegisters::Binary(BinaryOperands::Shared(register)), 0) => register,
                    (ExprRegisters::Binary(BinaryOperands::Shared(_)), 1) => {
                        return Ok(ControlFlow::Break(()));
                    }
                    (ExprRegisters::Binary(BinaryOperands::Pair { lhs, .. }), 0) => lhs,
                    (ExprRegisters::Binary(BinaryOperands::Pair { rhs, .. }), 1) => rhs,
                    (_, _) => unreachable!("binary expression has two children"),
                }
            }
            hir::Expr::Between {
                expr,
                negated,
                start,
                end,
                start_comparison,
                ..
            } => {
                if matches!(context.registers, ExprRegisters::None) {
                    let width = start_comparison.components.len();
                    if width == 0 {
                        return Err(LimboError::InternalError(
                            "HIR BETWEEN comparison has no components".to_string(),
                        ));
                    }
                    let value = self.program.alloc_registers(width);
                    let lower = self.program.alloc_register();
                    let start_register = if expr.equivalent(start) {
                        value
                    } else {
                        self.program.alloc_registers(width)
                    };
                    let upper = self.program.alloc_register();
                    let end_register = if expr.equivalent(end) {
                        value
                    } else {
                        self.program.alloc_registers(width)
                    };
                    context.registers = ExprRegisters::Between(BetweenRegisters {
                        value,
                        start: start_register,
                        end: end_register,
                        width,
                        lower,
                        upper,
                    });
                }
                let ExprRegisters::Between(registers) = context.registers else {
                    unreachable!("BETWEEN registers were allocated")
                };
                debug_assert_eq!(registers.width, start_comparison.components.len());
                match child_index {
                    0 => registers.value,
                    1 if registers.start == registers.value => {
                        return Ok(ControlFlow::Break(()));
                    }
                    1 => registers.start,
                    2 => {
                        self.emit_comparison(
                            if *negated {
                                Operator::Less
                            } else {
                                Operator::GreaterEquals
                            },
                            start_comparison,
                            registers.value,
                            registers.start,
                            registers.lower,
                        )?;
                        if registers.end == registers.value {
                            return Ok(ControlFlow::Break(()));
                        }
                        registers.end
                    }
                    _ => unreachable!("BETWEEN has three children"),
                }
            }
            hir::Expr::Case {
                base,
                when_then,
                base_comparisons,
                ..
            } => {
                if matches!(context.registers, ExprRegisters::None) {
                    context.registers = ExprRegisters::Case(CaseRegisters {
                        base: base.as_ref().map(|_| self.program.alloc_register()),
                        when: self.program.alloc_register(),
                        return_label: self.program.allocate_label(),
                        next_label: self.program.allocate_label(),
                    });
                }
                let ExprRegisters::Case(registers) = &mut context.registers else {
                    unreachable!("CASE registers were allocated")
                };
                let pair_start = usize::from(base.is_some());
                if child_index < pair_start {
                    registers.base.expect("simple CASE has a base register")
                } else if child_index < pair_start + when_then.len() * 2 {
                    let pair_child = child_index - pair_start;
                    let pair_index = pair_child / 2;
                    if pair_child % 2 == 0 {
                        if pair_index > 0 {
                            self.finish_case_arm(registers);
                        }
                        registers.when
                    } else {
                        self.emit_case_test(registers, base_comparisons.get(pair_index))?;
                        context.target
                    }
                } else {
                    debug_assert!(when_then.len() * 2 + pair_start == child_index);
                    if !when_then.is_empty() {
                        self.finish_case_arm(registers);
                    }
                    context.target
                }
            }
            hir::Expr::InList {
                values,
                comparisons,
                ..
            } => {
                if values.is_empty() {
                    if child_index == 0 {
                        return Ok(ControlFlow::Break(()));
                    }
                    unreachable!("empty IN list only has its skipped left operand")
                }
                if comparisons.len() != values.len() {
                    return Err(LimboError::InternalError(
                        "HIR IN list has mismatched values and comparisons".to_string(),
                    ));
                }
                if matches!(context.registers, ExprRegisters::None) {
                    let width = comparisons[0].components.len();
                    if width == 0
                        || comparisons
                            .iter()
                            .any(|comparison| comparison.components.len() != width)
                    {
                        return Err(LimboError::InternalError(
                            "HIR IN list has invalid comparison widths".to_string(),
                        ));
                    }
                    let false_label = self.program.allocate_label();
                    let null_label = self.program.allocate_label();
                    let result = self.program.alloc_register();
                    self.program.emit_no_constant_insn(Insn::Null {
                        dest: result,
                        dest_end: None,
                    });
                    let lhs = self.program.alloc_registers(width);
                    let match_label = self.program.allocate_label();
                    let null_flag = self.program.alloc_register();
                    context.registers = ExprRegisters::InList(InListRegisters {
                        lhs,
                        rhs: 0,
                        width,
                        null_flag,
                        result,
                        match_label,
                        false_label,
                        null_label,
                    });
                }
                let ExprRegisters::InList(registers) = &mut context.registers else {
                    unreachable!("IN list registers were allocated")
                };
                match child_index {
                    0 => registers.lhs,
                    value_child => {
                        if value_child == 1 {
                            self.program.emit_insn(Insn::BitAnd {
                                lhs: registers.lhs,
                                rhs: registers.lhs,
                                dest: registers.null_flag,
                            });
                        } else {
                            self.finish_in_list_value(registers, &comparisons[value_child - 2])?;
                        }
                        registers.rhs = self.program.alloc_registers(registers.width);
                        registers.rhs
                    }
                }
            }
            hir::Expr::Like {
                negated,
                operator,
                argument_count,
                ..
            } => {
                if *argument_count < 2 {
                    return Err(LimboError::InternalError(
                        "HIR LIKE-family expression has fewer than two arguments".to_string(),
                    ));
                }
                if matches!(context.registers, ExprRegisters::None) {
                    context.registers = ExprRegisters::Like(LikeRegisters {
                        start: self.program.alloc_registers(*argument_count),
                        result: if *negated {
                            self.program.alloc_register()
                        } else {
                            context.target
                        },
                    });
                }
                let ExprRegisters::Like(registers) = context.registers else {
                    unreachable!("LIKE-family registers were allocated")
                };
                match (operator, child_index) {
                    (turso_parser::ast::LikeOperator::Match, 0) => registers.start,
                    (turso_parser::ast::LikeOperator::Match, 1) => {
                        registers.start + argument_count - 1
                    }
                    (turso_parser::ast::LikeOperator::Match, _) => {
                        unreachable!("MATCH has two expression children")
                    }
                    (_, 0) => registers.start + 1,
                    (_, 1) => registers.start,
                    (_, 2) => registers.start + 2,
                    (_, _) => unreachable!("LIKE-family expression has at most three children"),
                }
            }
            hir::Expr::Array(elements) => {
                if matches!(context.registers, ExprRegisters::None) {
                    context.registers =
                        ExprRegisters::Array(self.program.alloc_registers(elements.len()));
                }
                let ExprRegisters::Array(start) = context.registers else {
                    unreachable!("array element registers were allocated")
                };
                start + child_index
            }
            hir::Expr::Subscript { .. } => {
                if matches!(context.registers, ExprRegisters::None) {
                    context.registers = ExprRegisters::Subscript(SubscriptRegisters {
                        base: self.program.alloc_register(),
                        index: self.program.alloc_register(),
                    });
                }
                let ExprRegisters::Subscript(registers) = context.registers else {
                    unreachable!("subscript registers were allocated")
                };
                match child_index {
                    0 => registers.base,
                    1 => registers.index,
                    _ => unreachable!("subscript has two children"),
                }
            }
            hir::Expr::FieldAccess(_) => {
                if matches!(context.registers, ExprRegisters::None) {
                    context.registers = ExprRegisters::FieldAccess(self.program.alloc_register());
                }
                let ExprRegisters::FieldAccess(base) = context.registers else {
                    unreachable!("field-access base register was allocated")
                };
                debug_assert_eq!(child_index, 0);
                base
            }
            hir::Expr::Raise {
                action, message, ..
            } => {
                debug_assert_eq!(child_index, 0);
                if *action == ResolveType::Ignore
                    || matches!(
                        message.as_deref(),
                        Some(hir::Expr::Literal(Literal::String(_)))
                    )
                {
                    return Ok(ControlFlow::Break(()));
                }
                if matches!(context.registers, ExprRegisters::None) {
                    context.registers = ExprRegisters::RaiseMessage(self.program.alloc_register());
                }
                let ExprRegisters::RaiseMessage(message) = context.registers else {
                    unreachable!("RAISE message register was allocated")
                };
                message
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::CustomType(_)) =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "custom-type function has invalid HIR arguments".to_string(),
                    ));
                };
                let runtime_argument = match &call.operation {
                    hir::FunctionOperation::CustomType(hir::CustomTypeOperation::UnionValue {
                        ..
                    }) if values.len() == 2 => 1,
                    hir::FunctionOperation::CustomType(hir::CustomTypeOperation::UnionTag {
                        ..
                    }) if values.len() == 1 => 0,
                    hir::FunctionOperation::CustomType(
                        hir::CustomTypeOperation::UnionExtract { .. }
                        | hir::CustomTypeOperation::StructExtract { .. },
                    ) if values.len() == 2 => 0,
                    _ => {
                        return Err(LimboError::InternalError(
                            "custom-type function has invalid HIR arguments".to_string(),
                        ));
                    }
                };
                if !order_by.is_empty() {
                    return Err(LimboError::InternalError(
                        "custom-type function has argument ordering".to_string(),
                    ));
                }
                if child_index != runtime_argument {
                    return Ok(ControlFlow::Break(()));
                }
                if matches!(context.registers, ExprRegisters::None) {
                    context.registers = ExprRegisters::Function(self.program.alloc_register());
                }
                let ExprRegisters::Function(argument) = context.registers else {
                    unreachable!("custom-type function register was allocated")
                };
                argument
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Ordinary)
                    && matches!(
                        call.function.value(),
                        Func::Scalar(
                            ScalarFunc::Likely | ScalarFunc::Likelihood | ScalarFunc::Unlikely
                        )
                    ) =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has invalid HIR arguments".to_string(),
                    ));
                };
                let expected = match call.function.value() {
                    Func::Scalar(ScalarFunc::Likely | ScalarFunc::Unlikely) => 1,
                    Func::Scalar(ScalarFunc::Likelihood) => 2,
                    _ => unreachable!("planner hint was checked by match guard"),
                };
                if !order_by.is_empty() || values.len() != expected {
                    return Err(LimboError::InternalError(
                        "planner hint has invalid HIR arguments".to_string(),
                    ));
                }
                if child_index == 0 {
                    context.target
                } else {
                    debug_assert_eq!(child_index, 1);
                    return Ok(ControlFlow::Break(()));
                }
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Ordinary)
                    && matches!(
                        call.function.value(),
                        Func::Scalar(ScalarFunc::Substr | ScalarFunc::Substring)
                    ) =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has invalid HIR arguments".to_string(),
                    ));
                };
                if !order_by.is_empty() || !matches!(values.len(), 2 | 3) {
                    return Err(LimboError::InternalError(
                        "substring has invalid HIR arguments".to_string(),
                    ));
                }
                if matches!(context.registers, ExprRegisters::None) {
                    context.registers = ExprRegisters::Function(self.program.alloc_registers(3));
                }
                let ExprRegisters::Function(start) = context.registers else {
                    unreachable!("substring registers were allocated")
                };
                start + child_index
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Ordinary)
                    && matches!(
                        call.function.value(),
                        Func::Scalar(ScalarFunc::LastInsertRowid)
                    ) =>
            {
                return Ok(ControlFlow::Break(()));
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Ordinary)
                    && matches!(call.function.value(), Func::Scalar(ScalarFunc::Coalesce)) =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has invalid HIR arguments".to_string(),
                    ));
                };
                if !order_by.is_empty() || values.len() < 2 {
                    return Err(LimboError::InternalError(
                        "coalesce has invalid HIR arguments".to_string(),
                    ));
                }
                if matches!(context.registers, ExprRegisters::None) {
                    context.registers = ExprRegisters::Coalesce(CoalesceRegisters {
                        end_label: self.program.allocate_label(),
                    });
                }
                let ExprRegisters::Coalesce(registers) = context.registers else {
                    unreachable!("coalesce registers were allocated")
                };
                if child_index > 0 {
                    self.program.emit_insn(Insn::NotNull {
                        reg: context.target,
                        target_pc: registers.end_label,
                    });
                }
                context.target
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Ordinary)
                    && matches!(call.function.value(), Func::Scalar(ScalarFunc::Iif)) =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has invalid HIR arguments".to_string(),
                    ));
                };
                if !order_by.is_empty() || values.len() < 2 {
                    return Err(LimboError::InternalError(
                        "iif has invalid HIR arguments".to_string(),
                    ));
                }
                if matches!(context.registers, ExprRegisters::None) {
                    let end_label = self.program.allocate_label();
                    let condition = self.program.alloc_register();
                    let false_label = self.program.allocate_label();
                    context.registers = ExprRegisters::Iif(IifRegisters {
                        condition,
                        end_label,
                        false_label,
                    });
                }
                let target = context.target;
                let ExprRegisters::Iif(registers) = &mut context.registers else {
                    unreachable!("iif registers were allocated")
                };
                if child_index % 2 == 1 {
                    self.program.emit_insn(Insn::IfNot {
                        reg: registers.condition,
                        target_pc: registers.false_label,
                        jump_if_null: true,
                    });
                    target
                } else if child_index == 0 {
                    registers.condition
                } else {
                    self.program.emit_insn(Insn::Goto {
                        target_pc: registers.end_label,
                    });
                    self.program
                        .preassign_label_to_next_insn(registers.false_label);
                    if child_index == values.len() - 1 {
                        target
                    } else {
                        registers.false_label = self.program.allocate_label();
                        registers.condition
                    }
                }
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Ordinary)
                    && matches!(call.function.value(), Func::Scalar(ScalarFunc::IfNull)) =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has invalid HIR arguments".to_string(),
                    ));
                };
                if !order_by.is_empty() || values.len() != 2 {
                    return Err(LimboError::InternalError(
                        "ifnull has invalid HIR arguments".to_string(),
                    ));
                }
                if matches!(context.registers, ExprRegisters::None) {
                    context.registers = ExprRegisters::IfNull(IfNullRegisters {
                        value: self.program.alloc_register(),
                        copy_label: self.program.allocate_label(),
                    });
                }
                let ExprRegisters::IfNull(registers) = context.registers else {
                    unreachable!("ifnull registers were allocated")
                };
                match child_index {
                    0 => registers.value,
                    1 => {
                        self.program.emit_insn(Insn::NotNull {
                            reg: registers.value,
                            target_pc: registers.copy_label,
                        });
                        registers.value
                    }
                    _ => unreachable!("ifnull has two arguments"),
                }
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Ordinary)
                    && matches!(call.function.value(), Func::Scalar(ScalarFunc::ConcatWs)) =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has invalid HIR arguments".to_string(),
                    ));
                };
                if !order_by.is_empty() {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has argument ordering".to_string(),
                    ));
                }
                if matches!(context.registers, ExprRegisters::None) {
                    let result = self.program.alloc_registers(values.len() + 1);
                    context.registers = ExprRegisters::ConcatWs(ConcatWsRegisters {
                        result,
                        arguments: result + 1,
                    });
                }
                let ExprRegisters::ConcatWs(registers) = context.registers else {
                    unreachable!("concat_ws registers were allocated")
                };
                registers.arguments + child_index
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Ordinary)
                    && plain_function_lowering(call.function.value()).is_some() =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has invalid HIR arguments".to_string(),
                    ));
                };
                if !order_by.is_empty() {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has argument ordering".to_string(),
                    ));
                }
                if matches!(context.registers, ExprRegisters::None) {
                    context.registers =
                        ExprRegisters::Function(self.program.alloc_registers(values.len()));
                }
                let ExprRegisters::Function(start) = context.registers else {
                    unreachable!("function argument registers were allocated")
                };
                start + child_index
            }
            hir::Expr::Row(_) => context.target + child_index,
            _ => return Ok(ControlFlow::Break(())),
        };
        Ok(ControlFlow::Continue(LoweringContext::new(target)))
    }

    fn post_order(
        &mut self,
        expression: &hir::Expr,
        context: LoweringContext,
        children: &[usize],
    ) -> Result<usize> {
        let target = context.target;
        match expression {
            hir::Expr::Literal(literal) => emit_literal(self.program, literal, target),
            hir::Expr::Parameter(parameter) => Ok(emit_parameter(self.program, parameter, target)),
            hir::Expr::Unary { operator, expr } => {
                self.emit_unary(*operator, expr, target, children)
            }
            hir::Expr::IsNull(_) => self.emit_null_test(NullTest::IsNull, target, children),
            hir::Expr::NotNull(_) => self.emit_null_test(NullTest::NotNull, target, children),
            hir::Expr::TruthTest {
                is_true, negated, ..
            } => {
                let [value] = children else {
                    unreachable!("truth test has one lowered child")
                };
                self.program.emit_insn(Insn::IsTrue {
                    reg: *value,
                    dest: target,
                    null_value: *negated,
                    invert: *negated == *is_true,
                });
                Ok(target)
            }
            hir::Expr::Collate { .. } => {
                let [value] = children else {
                    unreachable!("COLLATE has one lowered child")
                };
                debug_assert_eq!(*value, target);
                Ok(target)
            }
            hir::Expr::Cast {
                target: cast_target,
                ..
            } if cast_target.programs.apply_builtin_affinity
                && cast_target.programs.encode.is_empty()
                && cast_target.programs.domain.is_none() =>
            {
                let [value] = children else {
                    unreachable!("built-in CAST has one lowered child")
                };
                debug_assert_eq!(*value, target);
                self.program.emit_insn(Insn::Cast {
                    reg: target,
                    affinity: cast_target.affinity,
                });
                Ok(target)
            }
            hir::Expr::Binary {
                operator: Operator::Concat,
                array_concat,
                custom: None,
                comparison: None,
                ..
            } => self.emit_concat(
                binary_registers(context.registers),
                target,
                children,
                *array_concat,
            ),
            hir::Expr::Binary {
                operator,
                array_concat: false,
                custom: None,
                comparison: Some(comparison),
                ..
            } => self.emit_binary_comparison(
                *operator,
                comparison,
                binary_registers(context.registers),
                target,
                children,
            ),
            hir::Expr::Binary {
                operator: operator @ (Operator::ArrayContains | Operator::ArrayOverlap),
                array_concat: false,
                custom: None,
                comparison: None,
                ..
            } => self.emit_array_binary(
                *operator,
                binary_registers(context.registers),
                target,
                children,
            ),
            hir::Expr::Binary {
                operator,
                array_concat: false,
                custom: None,
                comparison: None,
                ..
            } => self.emit_binary(
                *operator,
                binary_registers(context.registers),
                target,
                children,
            ),
            hir::Expr::Between {
                negated,
                end_comparison,
                ..
            } => {
                let ExprRegisters::Between(registers) = context.registers else {
                    unreachable!("BETWEEN registers were allocated")
                };
                debug_assert_eq!(registers.width, end_comparison.components.len());
                self.emit_comparison(
                    if *negated {
                        Operator::Greater
                    } else {
                        Operator::LessEquals
                    },
                    end_comparison,
                    registers.value,
                    registers.end,
                    registers.upper,
                )?;
                self.program.emit_insn(if *negated {
                    Insn::Or {
                        lhs: registers.lower,
                        rhs: registers.upper,
                        dest: target,
                    }
                } else {
                    Insn::And {
                        lhs: registers.lower,
                        rhs: registers.upper,
                        dest: target,
                    }
                });
                Ok(target)
            }
            hir::Expr::Case {
                when_then,
                else_expr,
                ..
            } => {
                let ExprRegisters::Case(mut registers) = context.registers else {
                    unreachable!("CASE registers were allocated")
                };
                if else_expr.is_none() {
                    if !when_then.is_empty() {
                        self.finish_case_arm(&mut registers);
                    }
                    self.program.emit_insn(Insn::Null {
                        dest: target,
                        dest_end: None,
                    });
                }
                self.program
                    .preassign_label_to_next_insn(registers.return_label);
                Ok(target)
            }
            hir::Expr::InList {
                negated,
                values,
                comparisons,
                ..
            } => {
                if values.is_empty() {
                    debug_assert!(children.is_empty());
                    self.program.emit_insn(Insn::Integer {
                        value: i64::from(*negated),
                        dest: target,
                    });
                    return Ok(target);
                }
                let ExprRegisters::InList(registers) = context.registers else {
                    unreachable!("IN list registers were allocated")
                };
                debug_assert_eq!(children.len(), values.len() + 1);
                debug_assert_eq!(children[0], registers.lhs);
                self.finish_in_list_value(
                    &registers,
                    comparisons.last().expect("non-empty IN has a comparison"),
                )?;
                self.program.emit_insn(Insn::IsNull {
                    reg: registers.null_flag,
                    target_pc: registers.null_label,
                });
                self.program.emit_insn(Insn::Goto {
                    target_pc: registers.false_label,
                });
                self.program
                    .preassign_label_to_next_insn(registers.match_label);
                self.program.emit_insn(Insn::Integer {
                    value: 1,
                    dest: registers.result,
                });
                self.program
                    .preassign_label_to_next_insn(registers.false_label);
                self.program.emit_insn(Insn::AddImm {
                    register: registers.result,
                    value: 0,
                });
                if *negated {
                    self.program.emit_insn(Insn::Not {
                        reg: registers.result,
                        dest: registers.result,
                    });
                }
                self.program
                    .preassign_label_to_next_insn(registers.null_label);
                self.program.emit_insn(Insn::Copy {
                    src_reg: registers.result,
                    dst_reg: target,
                    extra_amount: 0,
                });
                Ok(target)
            }
            hir::Expr::Like {
                negated,
                operator,
                function,
                argument_count,
                rhs,
                escape,
                ..
            } => {
                let ExprRegisters::Like(registers) = context.registers else {
                    unreachable!("LIKE-family registers were allocated")
                };
                let expected_children = 2 + usize::from(escape.is_some());
                debug_assert_eq!(children.len(), expected_children);
                let constant_mask = if matches!(
                    operator,
                    turso_parser::ast::LikeOperator::Like | turso_parser::ast::LikeOperator::Glob
                ) && matches!(rhs.as_ref(), hir::Expr::Literal(_))
                {
                    self.program.mark_last_insn_constant();
                    1
                } else {
                    0
                };
                self.program.emit_insn(Insn::Function {
                    constant_mask,
                    start_reg: registers.start,
                    dest: registers.result,
                    func: FuncCtx {
                        func: function.value().clone(),
                        arg_count: *argument_count,
                    },
                });
                if *negated {
                    self.program.emit_insn(Insn::Not {
                        reg: registers.result,
                        dest: target,
                    });
                }
                Ok(target)
            }
            hir::Expr::Array(elements) => {
                let start = match context.registers {
                    ExprRegisters::Array(start) => start,
                    ExprRegisters::None if elements.is_empty() => self.program.alloc_registers(0),
                    _ => unreachable!("array element registers were allocated"),
                };
                debug_assert!(children
                    .iter()
                    .enumerate()
                    .all(|(index, register)| *register == start + index));
                self.program.emit_insn(Insn::MakeArray {
                    start_reg: start,
                    count: elements.len(),
                    dest: target,
                });
                Ok(target)
            }
            hir::Expr::Subscript { .. } => {
                let ExprRegisters::Subscript(registers) = context.registers else {
                    unreachable!("subscript registers were allocated")
                };
                debug_assert_eq!(children, [registers.base, registers.index]);
                self.program.emit_insn(Insn::ArrayElement {
                    array_reg: registers.base,
                    index_reg: registers.index,
                    dest: target,
                });
                Ok(target)
            }
            hir::Expr::FieldAccess(access) => {
                let ExprRegisters::FieldAccess(base) = context.registers else {
                    unreachable!("field-access base register was allocated")
                };
                debug_assert_eq!(children, [base]);
                self.program.emit_insn(match access.kind {
                    hir::FieldAccessKind::Struct { field_index } => Insn::StructField {
                        src_reg: base,
                        field_index,
                        dest: target,
                    },
                    hir::FieldAccessKind::Union { tag_index } => Insn::UnionExtract {
                        src_reg: base,
                        expected_tag: tag_index,
                        dest: target,
                    },
                });
                Ok(target)
            }
            hir::Expr::Raise { action, message } => {
                let in_trigger = self.program.trigger.is_some();
                match action {
                    ResolveType::Ignore => {
                        if !in_trigger {
                            crate::bail_parse_error!(
                                "RAISE() may only be used within a trigger-program"
                            );
                        }
                        if message.is_some() {
                            return Err(LimboError::InternalError(
                                "RAISE(IGNORE) has a message".to_string(),
                            ));
                        }
                        debug_assert!(children.is_empty());
                        self.program.emit_insn(Insn::Halt {
                            err_code: 0,
                            description: String::new(),
                            on_error: Some(ResolveType::Ignore),
                            description_reg: None,
                        });
                    }
                    action @ (ResolveType::Fail | ResolveType::Abort | ResolveType::Rollback) => {
                        if !in_trigger && *action != ResolveType::Abort {
                            crate::bail_parse_error!(
                                "RAISE() may only be used within a trigger-program"
                            );
                        }
                        let Some(message) = message else {
                            crate::bail_parse_error!("RAISE requires an error message");
                        };
                        let (description, description_reg) = match message.as_ref() {
                            hir::Expr::Literal(Literal::String(value)) => {
                                debug_assert!(children.is_empty());
                                (expr::sanitize_string(value), None)
                            }
                            _ => {
                                let [message] = children else {
                                    unreachable!("dynamic RAISE has one lowered message")
                                };
                                (String::new(), Some(*message))
                            }
                        };
                        self.program.emit_insn(Insn::Halt {
                            err_code: if in_trigger {
                                SQLITE_CONSTRAINT_TRIGGER
                            } else {
                                SQLITE_ERROR
                            },
                            description,
                            on_error: Some(*action),
                            description_reg,
                        });
                    }
                    ResolveType::Replace => {
                        crate::bail_parse_error!("REPLACE is not valid for RAISE");
                    }
                }
                Ok(target)
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Ordinary)
                    && matches!(
                        call.function.value(),
                        Func::Scalar(
                            ScalarFunc::Likely | ScalarFunc::Likelihood | ScalarFunc::Unlikely
                        )
                    ) =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has invalid HIR arguments".to_string(),
                    ));
                };
                let expected = match call.function.value() {
                    Func::Scalar(ScalarFunc::Likely | ScalarFunc::Unlikely) => 1,
                    Func::Scalar(ScalarFunc::Likelihood) => 2,
                    _ => unreachable!("planner hint was checked by match guard"),
                };
                if !order_by.is_empty() || values.len() != expected || children != [target] {
                    return Err(LimboError::InternalError(
                        "planner hint has invalid lowered arguments".to_string(),
                    ));
                }
                Ok(target)
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Ordinary)
                    && matches!(
                        call.function.value(),
                        Func::Scalar(ScalarFunc::Substr | ScalarFunc::Substring)
                    ) =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has invalid HIR arguments".to_string(),
                    ));
                };
                if !order_by.is_empty()
                    || !matches!(values.len(), 2 | 3)
                    || children.len() != values.len()
                {
                    return Err(LimboError::InternalError(
                        "substring has invalid lowered arguments".to_string(),
                    ));
                }
                let ExprRegisters::Function(start) = context.registers else {
                    unreachable!("substring registers were allocated")
                };
                debug_assert!(children
                    .iter()
                    .enumerate()
                    .all(|(index, register)| *register == start + index));
                self.program.emit_insn(Insn::Function {
                    constant_mask: 0,
                    start_reg: start,
                    dest: target,
                    func: FuncCtx {
                        func: call.function.value().clone(),
                        arg_count: values.len(),
                    },
                });
                Ok(target)
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::CustomType(_)) =>
            {
                let [argument] = children else {
                    return Err(LimboError::InternalError(
                        "custom-type function has invalid lowered arguments".to_string(),
                    ));
                };
                let ExprRegisters::Function(expected_argument) = context.registers else {
                    unreachable!("custom-type function register was allocated")
                };
                debug_assert_eq!(*argument, expected_argument);
                self.program.emit_insn(match &call.operation {
                    hir::FunctionOperation::CustomType(hir::CustomTypeOperation::UnionValue {
                        tag_index,
                        ..
                    }) => Insn::UnionPack {
                        tag_index: *tag_index,
                        value_reg: *argument,
                        dest: target,
                    },
                    hir::FunctionOperation::CustomType(hir::CustomTypeOperation::UnionTag {
                        tag_names,
                        ..
                    }) => Insn::UnionTag {
                        src_reg: *argument,
                        dest: target,
                        tag_names: tag_names.clone(),
                    },
                    hir::FunctionOperation::CustomType(
                        hir::CustomTypeOperation::UnionExtract { tag_index, .. },
                    ) => Insn::UnionExtract {
                        src_reg: *argument,
                        expected_tag: *tag_index,
                        dest: target,
                    },
                    hir::FunctionOperation::CustomType(
                        hir::CustomTypeOperation::StructExtract { field_index, .. },
                    ) => Insn::StructField {
                        src_reg: *argument,
                        field_index: *field_index,
                        dest: target,
                    },
                    _ => unreachable!("custom-type operation was checked by match guard"),
                });
                Ok(target)
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Ordinary)
                    && matches!(
                        call.function.value(),
                        Func::Scalar(
                            ScalarFunc::SqliteVersion
                                | ScalarFunc::TursoVersion
                                | ScalarFunc::SqliteSourceId
                        )
                    ) =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has invalid HIR arguments".to_string(),
                    ));
                };
                if !values.is_empty() || !order_by.is_empty() || !children.is_empty() {
                    return Err(LimboError::InternalError(
                        "version function has invalid lowered arguments".to_string(),
                    ));
                }
                let output = self.program.alloc_register();
                self.program.emit_insn(Insn::Function {
                    constant_mask: 0,
                    start_reg: output,
                    dest: output,
                    func: FuncCtx {
                        func: call.function.value().clone(),
                        arg_count: 0,
                    },
                });
                self.program.emit_insn(Insn::Copy {
                    src_reg: output,
                    dst_reg: target,
                    extra_amount: 0,
                });
                Ok(target)
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Ordinary)
                    && matches!(
                        call.function.value(),
                        Func::Scalar(ScalarFunc::LastInsertRowid)
                    ) =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has invalid HIR arguments".to_string(),
                    ));
                };
                if !order_by.is_empty() || !children.is_empty() {
                    return Err(LimboError::InternalError(
                        "last_insert_rowid has invalid lowered arguments".to_string(),
                    ));
                }
                let start_reg = self.program.alloc_register();
                self.program.emit_insn(Insn::Function {
                    constant_mask: 0,
                    start_reg,
                    dest: target,
                    func: FuncCtx {
                        func: call.function.value().clone(),
                        arg_count: values.len(),
                    },
                });
                Ok(target)
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Ordinary)
                    && matches!(call.function.value(), Func::Scalar(ScalarFunc::Coalesce)) =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has invalid HIR arguments".to_string(),
                    ));
                };
                if !order_by.is_empty() || values.len() < 2 || children.len() != values.len() {
                    return Err(LimboError::InternalError(
                        "coalesce has invalid lowered arguments".to_string(),
                    ));
                }
                let ExprRegisters::Coalesce(registers) = context.registers else {
                    unreachable!("coalesce registers were allocated")
                };
                debug_assert!(children.iter().all(|child| *child == target));
                self.program
                    .preassign_label_to_next_insn(registers.end_label);
                Ok(target)
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Ordinary)
                    && matches!(call.function.value(), Func::Scalar(ScalarFunc::Iif)) =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has invalid HIR arguments".to_string(),
                    ));
                };
                if !order_by.is_empty() || values.len() < 2 || children.len() != values.len() {
                    return Err(LimboError::InternalError(
                        "iif has invalid lowered arguments".to_string(),
                    ));
                }
                let ExprRegisters::Iif(registers) = context.registers else {
                    unreachable!("iif registers were allocated")
                };
                if values.len() % 2 == 0 {
                    self.program.emit_insn(Insn::Goto {
                        target_pc: registers.end_label,
                    });
                    self.program
                        .preassign_label_to_next_insn(registers.false_label);
                    self.program.emit_insn(Insn::Null {
                        dest: target,
                        dest_end: None,
                    });
                }
                self.program
                    .preassign_label_to_next_insn(registers.end_label);
                Ok(target)
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Ordinary)
                    && matches!(call.function.value(), Func::Scalar(ScalarFunc::IfNull)) =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has invalid HIR arguments".to_string(),
                    ));
                };
                if !order_by.is_empty() || values.len() != 2 || children.len() != 2 {
                    return Err(LimboError::InternalError(
                        "ifnull has invalid lowered arguments".to_string(),
                    ));
                }
                let ExprRegisters::IfNull(registers) = context.registers else {
                    unreachable!("ifnull registers were allocated")
                };
                debug_assert_eq!(children, [registers.value, registers.value]);
                self.program
                    .preassign_label_to_next_insn(registers.copy_label);
                self.program.emit_insn(Insn::Copy {
                    src_reg: registers.value,
                    dst_reg: target,
                    extra_amount: 0,
                });
                Ok(target)
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Ordinary)
                    && matches!(call.function.value(), Func::Scalar(ScalarFunc::ConcatWs)) =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has invalid HIR arguments".to_string(),
                    ));
                };
                if !order_by.is_empty() || children.len() != values.len() {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has invalid lowered arguments".to_string(),
                    ));
                }
                let ExprRegisters::ConcatWs(registers) = context.registers else {
                    unreachable!("concat_ws registers were allocated")
                };
                debug_assert!(children
                    .iter()
                    .enumerate()
                    .all(|(index, register)| *register == registers.arguments + index));
                self.program.emit_insn(Insn::Function {
                    constant_mask: 0,
                    start_reg: registers.arguments,
                    dest: registers.result,
                    func: FuncCtx {
                        func: call.function.value().clone(),
                        arg_count: values.len(),
                    },
                });
                self.program.emit_insn(Insn::Copy {
                    src_reg: registers.result,
                    dst_reg: target,
                    extra_amount: 0,
                });
                Ok(target)
            }
            hir::Expr::Function(call)
                if matches!(call.evaluation, hir::FunctionEvaluation::Scalar)
                    && matches!(call.operation, hir::FunctionOperation::Ordinary)
                    && plain_function_lowering(call.function.value()).is_some() =>
            {
                let hir::FunctionArguments::Expressions {
                    values,
                    distinctness: None,
                    order_by,
                } = &call.arguments
                else {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has invalid HIR arguments".to_string(),
                    ));
                };
                if !order_by.is_empty() || children.len() != values.len() {
                    return Err(LimboError::InternalError(
                        "ordinary scalar function has invalid lowered arguments".to_string(),
                    ));
                }
                debug_assert!(children
                    .windows(2)
                    .all(|registers| registers[1] == registers[0] + 1));
                let lowering = plain_function_lowering(call.function.value())
                    .expect("plain function lowering was checked by the match guard");
                let start_reg = if let Some(start) = children.first() {
                    *start
                } else {
                    match lowering {
                        EmptyArgumentStart::UseTarget => target,
                        EmptyArgumentStart::UseZero => 0,
                        EmptyArgumentStart::UseNextRegister => self.program.alloc_registers(0),
                        EmptyArgumentStart::ReserveOneRegister => self.program.alloc_register(),
                    }
                };
                self.program.emit_insn(Insn::Function {
                    constant_mask: 0,
                    start_reg,
                    dest: target,
                    func: FuncCtx {
                        func: call.function.value().clone(),
                        arg_count: values.len(),
                    },
                });
                Ok(target)
            }
            hir::Expr::Row(values) => {
                debug_assert_eq!(children.len(), values.len());
                debug_assert!(children
                    .iter()
                    .enumerate()
                    .all(|(index, register)| *register == target + index));
                Ok(target)
            }
            _ => Err(LimboError::InternalError(
                "HIR expression lowering is not implemented for this expression".to_string(),
            )),
        }
    }
}

impl ExprLowerer<'_> {
    fn finish_in_list_value(
        &mut self,
        registers: &InListRegisters,
        comparison: &hir::ComparisonSemantics,
    ) -> Result<()> {
        if comparison.components.len() != registers.width {
            return Err(LimboError::InternalError(
                "HIR IN comparison width changed between values".to_string(),
            ));
        }
        self.program.emit_insn(Insn::BitAnd {
            lhs: registers.null_flag,
            rhs: registers.rhs,
            dest: registers.null_flag,
        });
        match comparison.components.as_slice() {
            [] => unreachable!("IN comparison width was checked"),
            [component] => {
                self.program.emit_insn(Insn::Eq {
                    lhs: registers.lhs,
                    rhs: registers.rhs,
                    target_pc: registers.match_label,
                    flags: comparison_flags(component),
                    collation: comparison_collation(component),
                });
            }
            components => {
                let skip_label = self.program.allocate_label();
                for (index, component) in components.iter().enumerate() {
                    if index + 1 == components.len() {
                        self.program.emit_insn(Insn::Eq {
                            lhs: registers.lhs + index,
                            rhs: registers.rhs + index,
                            target_pc: registers.match_label,
                            flags: comparison_flags(component),
                            collation: comparison_collation(component),
                        });
                    } else {
                        self.program.emit_insn(Insn::Ne {
                            lhs: registers.lhs + index,
                            rhs: registers.rhs + index,
                            target_pc: skip_label,
                            flags: comparison_flags(component),
                            collation: comparison_collation(component),
                        });
                    }
                }
                self.program.preassign_label_to_next_insn(skip_label);
            }
        }
        Ok(())
    }

    fn finish_case_arm(&mut self, registers: &mut CaseRegisters) {
        self.program.emit_insn(Insn::Goto {
            target_pc: registers.return_label,
        });
        self.program
            .preassign_label_to_next_insn(registers.next_label);
        registers.next_label = self.program.allocate_label();
    }

    fn emit_case_test(
        &mut self,
        registers: &CaseRegisters,
        comparison: Option<&hir::ComparisonSemantics>,
    ) -> Result<()> {
        match (registers.base, comparison) {
            (None, None) => {
                self.program.emit_insn(Insn::IfNot {
                    reg: registers.when,
                    target_pc: registers.next_label,
                    jump_if_null: true,
                });
            }
            (Some(base), Some(comparison)) => {
                let [component] = comparison.components.as_slice() else {
                    return Err(LimboError::InternalError(
                        "simple CASE comparison must have one component".to_string(),
                    ));
                };
                let flags = CmpInsFlags::default()
                    .with_affinity(component.affinity)
                    .jump_if_null();
                self.program.emit_insn(Insn::Ne {
                    lhs: base,
                    rhs: registers.when,
                    target_pc: registers.next_label,
                    flags: if component.array {
                        flags.array_cmp()
                    } else {
                        flags
                    },
                    collation: component
                        .collation
                        .as_ref()
                        .map(|collation| *collation.value()),
                });
            }
            (None, Some(_)) => {
                return Err(LimboError::InternalError(
                    "searched CASE contains comparison metadata".to_string(),
                ));
            }
            (Some(_), None) => {
                return Err(LimboError::InternalError(
                    "simple CASE is missing comparison metadata".to_string(),
                ));
            }
        }
        Ok(())
    }

    fn emit_null_test(
        &mut self,
        test: NullTest,
        target: usize,
        children: &[usize],
    ) -> Result<usize> {
        let [value] = children else {
            unreachable!("NULL test has one lowered child")
        };
        let if_true_label = self.program.allocate_label();
        let instruction = match test {
            NullTest::IsNull => Insn::IsNull {
                reg: *value,
                target_pc: if_true_label,
            },
            NullTest::NotNull => Insn::NotNull {
                reg: *value,
                target_pc: if_true_label,
            },
        };
        expr::functions::wrap_eval_jump_expr(self.program, instruction, target, if_true_label);
        Ok(target)
    }

    fn emit_array_binary(
        &mut self,
        operator: Operator,
        operands: BinaryOperands,
        target: usize,
        children: &[usize],
    ) -> Result<usize> {
        let start_reg = match operands {
            BinaryOperands::Shared(source) => {
                debug_assert_eq!(children, [source]);
                let start = self.program.alloc_registers(2);
                self.program.emit_insn(Insn::Copy {
                    src_reg: source,
                    dst_reg: start,
                    extra_amount: 0,
                });
                self.program.emit_insn(Insn::Copy {
                    src_reg: source,
                    dst_reg: start + 1,
                    extra_amount: 0,
                });
                start
            }
            BinaryOperands::Pair { lhs, rhs } => {
                debug_assert_eq!(children, [lhs, rhs]);
                debug_assert_eq!(rhs, lhs + 1);
                lhs
            }
        };
        let function = match operator {
            Operator::ArrayContains => ScalarFunc::ArrayContainsAll,
            Operator::ArrayOverlap => ScalarFunc::ArrayOverlap,
            _ => unreachable!("array binary operator was matched by the caller"),
        };
        self.program.emit_insn(Insn::Function {
            constant_mask: 0,
            start_reg,
            dest: target,
            func: FuncCtx {
                func: Func::Scalar(function),
                arg_count: 2,
            },
        });
        Ok(target)
    }

    fn emit_binary_comparison(
        &mut self,
        operator: Operator,
        comparison: &hir::ComparisonSemantics,
        operands: BinaryOperands,
        target: usize,
        children: &[usize],
    ) -> Result<usize> {
        let (lhs, rhs) = match operands {
            BinaryOperands::Shared(register) => {
                debug_assert_eq!(children, [register]);
                (register, register)
            }
            BinaryOperands::Pair { lhs, rhs } => {
                debug_assert_eq!(children, [lhs, rhs]);
                (lhs, rhs)
            }
        };
        self.emit_comparison(operator, comparison, lhs, rhs, target)
    }

    fn emit_comparison(
        &mut self,
        operator: Operator,
        comparison: &hir::ComparisonSemantics,
        lhs: usize,
        rhs: usize,
        target: usize,
    ) -> Result<usize> {
        match comparison.components.as_slice() {
            [] => Err(LimboError::InternalError(
                "HIR comparison has no components".to_string(),
            )),
            [_] => self.emit_scalar_comparison(operator, comparison, lhs, rhs, target),
            components => {
                self.emit_row_comparison(operator, components, lhs, rhs, target)?;
                Ok(target)
            }
        }
    }

    fn emit_scalar_comparison(
        &mut self,
        operator: Operator,
        comparison: &hir::ComparisonSemantics,
        lhs: usize,
        rhs: usize,
        target: usize,
    ) -> Result<usize> {
        let [component] = comparison.components.as_slice() else {
            unreachable!("scalar comparison has one component")
        };
        let base_flags = CmpInsFlags::default().with_affinity(component.affinity);
        let comparison_flags = if component.array {
            base_flags.array_cmp()
        } else {
            base_flags
        };
        let collation = component
            .collation
            .as_ref()
            .map(|resolved| *resolved.value());
        let if_true_label = self.program.allocate_label();
        let (instruction, null_equal) = match operator {
            Operator::Equals => (
                Insn::Eq {
                    lhs,
                    rhs,
                    target_pc: if_true_label,
                    flags: comparison_flags,
                    collation,
                },
                false,
            ),
            Operator::NotEquals => (
                Insn::Ne {
                    lhs,
                    rhs,
                    target_pc: if_true_label,
                    flags: comparison_flags,
                    collation,
                },
                false,
            ),
            Operator::Less => (
                Insn::Lt {
                    lhs,
                    rhs,
                    target_pc: if_true_label,
                    flags: comparison_flags,
                    collation,
                },
                false,
            ),
            Operator::LessEquals => (
                Insn::Le {
                    lhs,
                    rhs,
                    target_pc: if_true_label,
                    flags: comparison_flags,
                    collation,
                },
                false,
            ),
            Operator::Greater => (
                Insn::Gt {
                    lhs,
                    rhs,
                    target_pc: if_true_label,
                    flags: comparison_flags,
                    collation,
                },
                false,
            ),
            Operator::GreaterEquals => (
                Insn::Ge {
                    lhs,
                    rhs,
                    target_pc: if_true_label,
                    flags: comparison_flags,
                    collation,
                },
                false,
            ),
            Operator::Is => (
                Insn::Eq {
                    lhs,
                    rhs,
                    target_pc: if_true_label,
                    flags: base_flags.null_eq(),
                    collation,
                },
                true,
            ),
            Operator::IsNot => (
                Insn::Ne {
                    lhs,
                    rhs,
                    target_pc: if_true_label,
                    flags: base_flags.null_eq(),
                    collation,
                },
                true,
            ),
            _ => {
                return Err(LimboError::InternalError(format!(
                    "HIR comparison lowering is not implemented for {operator:?}"
                )));
            }
        };
        if null_equal {
            expr::functions::wrap_eval_jump_expr(self.program, instruction, target, if_true_label);
        } else {
            expr::functions::wrap_eval_jump_expr_zero_or_null(
                self.program,
                instruction,
                target,
                if_true_label,
                lhs,
                rhs,
            );
        }
        Ok(target)
    }

    fn emit_row_comparison(
        &mut self,
        operator: Operator,
        components: &[hir::ComparisonComponent],
        lhs_start: usize,
        rhs_start: usize,
        target: usize,
    ) -> Result<()> {
        enum Ordering {
            Less,
            Greater,
        }

        let mut emit_equality = |target: usize, null_equal: bool| {
            let null_seen = if null_equal {
                None
            } else {
                let register = self.program.alloc_register();
                self.program.emit_insn(Insn::Integer {
                    value: 0,
                    dest: register,
                });
                Some(register)
            };

            let done = self.program.allocate_label();
            for (index, component) in components.iter().enumerate() {
                let next = self.program.allocate_label();
                let lhs = lhs_start + index;
                let rhs = rhs_start + index;
                self.program.emit_insn(Insn::Eq {
                    lhs,
                    rhs,
                    target_pc: next,
                    flags: if null_equal {
                        CmpInsFlags::default()
                            .null_eq()
                            .with_affinity(component.affinity)
                    } else {
                        CmpInsFlags::default().with_affinity(component.affinity)
                    },
                    collation: component
                        .collation
                        .as_ref()
                        .map(|collation| *collation.value()),
                });
                if null_equal {
                    self.program.emit_insn(Insn::Integer {
                        value: 0,
                        dest: target,
                    });
                    self.program.emit_insn(Insn::Goto { target_pc: done });
                } else {
                    let mark_null = self.program.allocate_label();
                    self.program.emit_insn(Insn::IsNull {
                        reg: lhs,
                        target_pc: mark_null,
                    });
                    self.program.emit_insn(Insn::IsNull {
                        reg: rhs,
                        target_pc: mark_null,
                    });
                    self.program.emit_insn(Insn::Integer {
                        value: 0,
                        dest: target,
                    });
                    self.program.emit_insn(Insn::Goto { target_pc: done });
                    self.program.preassign_label_to_next_insn(mark_null);
                    self.program.emit_insn(Insn::Integer {
                        value: 1,
                        dest: null_seen.expect("null tracking register must exist"),
                    });
                }
                self.program.preassign_label_to_next_insn(next);
            }
            self.program.emit_insn(Insn::Integer {
                value: 1,
                dest: target,
            });
            if !null_equal {
                let finish = self.program.allocate_label();
                self.program.emit_insn(Insn::IfNot {
                    reg: null_seen.expect("null tracking register must exist"),
                    target_pc: finish,
                    jump_if_null: true,
                });
                self.program.emit_insn(Insn::Null {
                    dest: target,
                    dest_end: None,
                });
                self.program.preassign_label_to_next_insn(finish);
            }
            self.program.preassign_label_to_next_insn(done);
        };

        let emit_ordering =
            |program: &mut ProgramBuilder, ordering: Ordering, include_equal: bool| {
                let done = program.allocate_label();
                let null_result = program.allocate_label();
                for (index, component) in components.iter().enumerate() {
                    let next = program.allocate_label();
                    let lhs = lhs_start + index;
                    let rhs = rhs_start + index;
                    let flags = CmpInsFlags::default().with_affinity(component.affinity);
                    let collation = component
                        .collation
                        .as_ref()
                        .map(|collation| *collation.value());
                    program.emit_insn(Insn::IsNull {
                        reg: lhs,
                        target_pc: null_result,
                    });
                    program.emit_insn(Insn::IsNull {
                        reg: rhs,
                        target_pc: null_result,
                    });
                    program.emit_insn(Insn::Eq {
                        lhs,
                        rhs,
                        target_pc: next,
                        flags,
                        collation,
                    });
                    let if_true = program.allocate_label();
                    program.emit_insn(match ordering {
                        Ordering::Less => Insn::Lt {
                            lhs,
                            rhs,
                            target_pc: if_true,
                            flags,
                            collation,
                        },
                        Ordering::Greater => Insn::Gt {
                            lhs,
                            rhs,
                            target_pc: if_true,
                            flags,
                            collation,
                        },
                    });
                    program.emit_insn(Insn::Integer {
                        value: 0,
                        dest: target,
                    });
                    program.emit_insn(Insn::Goto { target_pc: done });
                    program.preassign_label_to_next_insn(if_true);
                    program.emit_insn(Insn::Integer {
                        value: 1,
                        dest: target,
                    });
                    program.emit_insn(Insn::Goto { target_pc: done });
                    program.preassign_label_to_next_insn(next);
                }
                program.emit_insn(Insn::Integer {
                    value: if include_equal { 1 } else { 0 },
                    dest: target,
                });
                program.emit_insn(Insn::Goto { target_pc: done });
                program.preassign_label_to_next_insn(null_result);
                program.emit_insn(Insn::Null {
                    dest: target,
                    dest_end: None,
                });
                program.preassign_label_to_next_insn(done);
            };

        match operator {
            Operator::Equals => emit_equality(target, false),
            Operator::NotEquals => {
                emit_equality(target, false);
                self.program.emit_insn(Insn::Not {
                    reg: target,
                    dest: target,
                });
            }
            Operator::Is => emit_equality(target, true),
            Operator::IsNot => {
                emit_equality(target, true);
                self.program.emit_insn(Insn::Not {
                    reg: target,
                    dest: target,
                });
            }
            Operator::Less => emit_ordering(self.program, Ordering::Less, false),
            Operator::LessEquals => emit_ordering(self.program, Ordering::Less, true),
            Operator::Greater => emit_ordering(self.program, Ordering::Greater, false),
            Operator::GreaterEquals => emit_ordering(self.program, Ordering::Greater, true),
            _ => {
                return Err(LimboError::InternalError(format!(
                    "HIR row comparison lowering is not implemented for {operator:?}"
                )));
            }
        }
        Ok(())
    }

    fn emit_concat(
        &mut self,
        operands: BinaryOperands,
        target: usize,
        children: &[usize],
        array_concat: bool,
    ) -> Result<usize> {
        let (lhs, rhs) = match operands {
            BinaryOperands::Shared(register) => {
                debug_assert_eq!(children, [register]);
                (register, register)
            }
            BinaryOperands::Pair { lhs, rhs } => {
                debug_assert_eq!(children, [lhs, rhs]);
                (lhs, rhs)
            }
        };
        let instruction = if array_concat {
            Insn::ArrayConcat {
                lhs,
                rhs,
                dest: target,
            }
        } else {
            Insn::Concat {
                lhs,
                rhs,
                dest: target,
            }
        };
        self.program.emit_insn(instruction);
        Ok(target)
    }

    fn emit_binary(
        &mut self,
        operator: Operator,
        operands: BinaryOperands,
        target: usize,
        children: &[usize],
    ) -> Result<usize> {
        let (lhs, rhs) = match operands {
            BinaryOperands::Shared(register) => {
                debug_assert_eq!(children, [register]);
                (register, register)
            }
            BinaryOperands::Pair { lhs, rhs } => {
                debug_assert_eq!(children, [lhs, rhs]);
                (lhs, rhs)
            }
        };
        let instruction = match operator {
            Operator::Add => Insn::Add {
                lhs,
                rhs,
                dest: target,
            },
            Operator::Subtract => Insn::Subtract {
                lhs,
                rhs,
                dest: target,
            },
            Operator::Multiply => Insn::Multiply {
                lhs,
                rhs,
                dest: target,
            },
            Operator::Divide => Insn::Divide {
                lhs,
                rhs,
                dest: target,
            },
            Operator::Modulus => Insn::Remainder {
                lhs,
                rhs,
                dest: target,
            },
            Operator::And => Insn::And {
                lhs,
                rhs,
                dest: target,
            },
            Operator::Or => Insn::Or {
                lhs,
                rhs,
                dest: target,
            },
            Operator::BitwiseAnd => Insn::BitAnd {
                lhs,
                rhs,
                dest: target,
            },
            Operator::BitwiseOr => Insn::BitOr {
                lhs,
                rhs,
                dest: target,
            },
            Operator::LeftShift => Insn::ShiftLeft {
                lhs,
                rhs,
                dest: target,
            },
            Operator::RightShift => Insn::ShiftRight {
                lhs,
                rhs,
                dest: target,
            },
            #[cfg(feature = "json")]
            operator @ (Operator::ArrowRight | Operator::ArrowRightShift) => {
                let function = match operator {
                    Operator::ArrowRight => JsonFunc::JsonArrowExtract,
                    Operator::ArrowRightShift => JsonFunc::JsonArrowShiftExtract,
                    _ => unreachable!("JSON arrow operator was matched"),
                };
                self.program.emit_insn(Insn::Function {
                    constant_mask: 0,
                    start_reg: lhs,
                    dest: target,
                    func: FuncCtx {
                        func: Func::Json(function),
                        arg_count: 2,
                    },
                });
                return Ok(target);
            }
            _ => {
                return Err(LimboError::InternalError(format!(
                    "HIR binary lowering is not implemented for {operator:?}"
                )));
            }
        };
        self.program.emit_insn(instruction);
        Ok(target)
    }

    fn emit_unary(
        &mut self,
        operator: UnaryOperator,
        expression: &hir::Expr,
        target: usize,
        children: &[usize],
    ) -> Result<usize> {
        match (operator, expression) {
            (UnaryOperator::Positive, _) => {
                debug_assert_eq!(children, [target]);
            }
            (UnaryOperator::Negative, hir::Expr::Literal(Literal::Numeric(value))) => {
                let value = format!("-{value}");
                match parse_numeric_literal(&value)? {
                    Value::Numeric(Numeric::Integer(value)) => {
                        self.program.emit_insn(Insn::Integer {
                            value,
                            dest: target,
                        });
                    }
                    Value::Numeric(Numeric::Float(value)) => {
                        self.program.emit_insn(Insn::Real {
                            value: value.into(),
                            dest: target,
                        });
                    }
                    _ => unreachable!("numeric parser returns a numeric value"),
                }
            }
            (UnaryOperator::Negative, _) => {
                let [value] = children else {
                    unreachable!("non-literal negation has one lowered child")
                };
                let zero = self.program.alloc_register();
                self.program.emit_insn(Insn::Integer {
                    value: 0,
                    dest: zero,
                });
                self.program.mark_last_insn_constant();
                self.program.emit_insn(Insn::Subtract {
                    lhs: zero,
                    rhs: *value,
                    dest: target,
                });
            }
            (UnaryOperator::BitwiseNot, hir::Expr::Literal(Literal::Numeric(value))) => {
                let value = match parse_numeric_literal(value)? {
                    Value::Numeric(Numeric::Integer(value)) => !value,
                    Value::Numeric(Numeric::Float(value)) => !(f64::from(value) as i64),
                    _ => unreachable!("numeric parser returns a numeric value"),
                };
                self.program.emit_insn(Insn::Integer {
                    value,
                    dest: target,
                });
            }
            (UnaryOperator::BitwiseNot, hir::Expr::Literal(Literal::Null)) => {
                self.program.emit_insn(Insn::Null {
                    dest: target,
                    dest_end: None,
                });
            }
            (UnaryOperator::BitwiseNot, _) => {
                let [value] = children else {
                    unreachable!("non-literal bitwise negation has one lowered child")
                };
                self.program.emit_insn(Insn::BitNot {
                    reg: *value,
                    dest: target,
                });
            }
            (UnaryOperator::Not, _) => {
                let [value] = children else {
                    unreachable!("logical negation has one lowered child")
                };
                self.program.emit_insn(Insn::Not {
                    reg: *value,
                    dest: target,
                });
            }
        }
        Ok(target)
    }
}

/// Lower a resolved expression into an existing target register.
pub(crate) fn translate_expr(
    program: &mut ProgramBuilder,
    expression: &hir::Expr,
    target: usize,
) -> Result<usize> {
    expression.walk(LoweringContext::new(target), &mut ExprLowerer { program })
}

/// Emit a literal already selected by semantic analysis.
pub(crate) fn emit_literal(
    program: &mut ProgramBuilder,
    literal: &turso_parser::ast::Literal,
    target: usize,
) -> Result<usize> {
    expr::emit_literal(program, literal, target)
}

/// Emit a parameter using its resolved slot and original SQL spelling.
pub(crate) fn emit_parameter(
    program: &mut ProgramBuilder,
    parameter: &hir::Parameter,
    target: usize,
) -> usize {
    let index = program.register_parameter(parameter.index, &parameter.spelling);
    program.emit_insn(Insn::Variable {
        index,
        dest: target,
    });
    target
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use super::*;
    use crate::parameters::ParameterSpelling;
    use crate::translate::semantic::hir::TypeFact;
    use crate::vdbe::builder::{ProgramBuilderOpts, QueryMode};

    fn program() -> ProgramBuilder {
        ProgramBuilder::new(QueryMode::Normal, None, ProgramBuilderOpts::new(0, 4, 0))
    }

    fn resolved_function(function: Func) -> hir::ResolvedFunction {
        hir::CatalogObject::new(
            hir::CatalogObjectId::new(1),
            hir::CatalogSnapshot::from_id(1),
            None,
            crate::sync::Arc::new(function),
        )
    }

    fn ordinary_scalar_call(function: Func, values: Vec<hir::Expr>) -> hir::Expr {
        hir::Expr::Function(hir::FunctionCall {
            function: resolved_function(function),
            evaluation: hir::FunctionEvaluation::Scalar,
            arguments: hir::FunctionArguments::Expressions {
                values,
                distinctness: None,
                order_by: Vec::new(),
            },
            result_type: TypeFact::dynamic(),
            operation: hir::FunctionOperation::Ordinary,
        })
    }

    fn custom_type_call(
        function: ScalarFunc,
        values: Vec<hir::Expr>,
        operation: hir::CustomTypeOperation,
    ) -> hir::Expr {
        hir::Expr::Function(hir::FunctionCall {
            function: resolved_function(Func::Scalar(function)),
            evaluation: hir::FunctionEvaluation::Scalar,
            arguments: hir::FunctionArguments::Expressions {
                values,
                distinctness: None,
                order_by: Vec::new(),
            },
            result_type: TypeFact::dynamic(),
            operation: hir::FunctionOperation::CustomType(operation),
        })
    }

    fn resolved_type(kind: crate::schema::TypeDefKind) -> hir::ResolvedType {
        hir::CatalogObject::new(
            hir::CatalogObjectId::new(2),
            hir::CatalogSnapshot::from_id(1),
            None,
            crate::sync::Arc::new(crate::schema::TypeDef {
                name: "container".to_string(),
                is_builtin: false,
                not_null: false,
                is_domain: false,
                sql: String::new(),
                domain_checks: Vec::new(),
                kind,
            }),
        )
    }

    #[test]
    fn custom_type_calls_use_resolved_metadata_and_skip_name_arguments() {
        #[derive(Debug)]
        enum ExpectedInsn {
            UnionPack,
            UnionTag,
            UnionExtract,
            StructField,
        }

        let union_type =
            resolved_type(crate::schema::TypeDefKind::Union(crate::schema::UnionDef {
                variants: Vec::new(),
                tag_names: crate::sync::Arc::from(Vec::<String>::new()),
            }));
        let struct_type = resolved_type(crate::schema::TypeDefKind::Struct(
            crate::schema::StructDef { fields: Vec::new() },
        ));
        let runtime_value = || hir::Expr::Literal(Literal::Numeric("7".to_string()));
        let semantic_name = || hir::Expr::Literal(Literal::String("'ignored'".to_string()));
        let cases = [
            (
                custom_type_call(
                    ScalarFunc::UnionValueFunc,
                    vec![semantic_name(), runtime_value()],
                    hir::CustomTypeOperation::UnionValue {
                        union_type: union_type.clone(),
                        tag_index: 3,
                    },
                ),
                ExpectedInsn::UnionPack,
            ),
            (
                custom_type_call(
                    ScalarFunc::UnionTagFunc,
                    vec![runtime_value()],
                    hir::CustomTypeOperation::UnionTag {
                        union_type: union_type.clone(),
                        tag_names: crate::sync::Arc::from([
                            "email".to_string(),
                            "chat".to_string(),
                        ]),
                    },
                ),
                ExpectedInsn::UnionTag,
            ),
            (
                custom_type_call(
                    ScalarFunc::UnionExtractFunc,
                    vec![runtime_value(), semantic_name()],
                    hir::CustomTypeOperation::UnionExtract {
                        union_type,
                        tag_index: 4,
                    },
                ),
                ExpectedInsn::UnionExtract,
            ),
            (
                custom_type_call(
                    ScalarFunc::StructExtractFunc,
                    vec![runtime_value(), semantic_name()],
                    hir::CustomTypeOperation::StructExtract {
                        struct_type,
                        field_index: 5,
                    },
                ),
                ExpectedInsn::StructField,
            ),
        ];

        for (expression, expected_operation) in cases {
            let mut program = program();
            translate_expr(&mut program, &expression, 8).unwrap();

            assert_eq!(program.insns.len(), 2);
            assert!(matches!(
                program.insns[0].0,
                Insn::Integer { value: 7, dest: 1 }
            ));
            match (&program.insns[1].0, expected_operation) {
                (
                    Insn::UnionPack {
                        tag_index: 3,
                        value_reg: 1,
                        dest: 8,
                    },
                    ExpectedInsn::UnionPack,
                ) => {}
                (
                    Insn::UnionTag {
                        src_reg: 1,
                        dest: 8,
                        tag_names,
                    },
                    ExpectedInsn::UnionTag,
                ) if tag_names.as_ref() == ["email", "chat"] => {}
                (
                    Insn::UnionExtract {
                        src_reg: 1,
                        expected_tag: 4,
                        dest: 8,
                    },
                    ExpectedInsn::UnionExtract,
                )
                | (
                    Insn::StructField {
                        src_reg: 1,
                        field_index: 5,
                        dest: 8,
                    },
                    ExpectedInsn::StructField,
                ) => {}
                unexpected => panic!("unexpected custom-type lowering: {unexpected:?}"),
            }
        }
    }

    fn trigger_program() -> ProgramBuilder {
        let mut program = program();
        program.trigger = Some(crate::sync::Arc::new(crate::schema::Trigger::new(
            "trigger".to_string(),
            String::new(),
            "items".to_string(),
            None,
            turso_parser::ast::TriggerEvent::Insert,
            true,
            None,
            Vec::new(),
            false,
            None,
        )));
        program
    }

    #[test]
    fn resolved_parameter_spelling_reaches_program_metadata() {
        let cases = [
            (1, ParameterSpelling::Anonymous, None),
            (5, ParameterSpelling::Numbered, Some("?5")),
            (
                6,
                ParameterSpelling::Named(":name".to_string()),
                Some(":name"),
            ),
        ];

        for (index, spelling, expected_name) in cases {
            let mut program = program();
            let parameter = hir::Parameter {
                index: NonZeroU32::new(index).unwrap(),
                spelling,
                type_fact: TypeFact::dynamic(),
            };

            assert_eq!(emit_parameter(&mut program, &parameter, 2), 2);

            let index = usize::try_from(index).unwrap().try_into().unwrap();
            assert_eq!(program.parameters.name(index).as_deref(), expected_name);
            assert!(matches!(
                program.insns.as_slice(),
                [(Insn::Variable { index: emitted, dest: 2 }, _)] if *emitted == index
            ));
        }
    }

    #[test]
    fn resolved_literal_uses_existing_opcode_emission() {
        let mut program = program();

        assert_eq!(
            emit_literal(
                &mut program,
                &turso_parser::ast::Literal::String("'value'".to_string()),
                3,
            )
            .expect("literal emits"),
            3
        );
        assert!(matches!(
            program.insns.as_slice(),
            [(Insn::String8 { value, dest: 3 }, _)] if value == "value"
        ));
    }

    #[test]
    fn unary_lowering_keeps_existing_register_and_opcode_order() {
        let mut program = program();
        let expression = hir::Expr::Unary {
            operator: UnaryOperator::Negative,
            expr: Box::new(hir::Expr::Parameter(hir::Parameter {
                index: NonZeroU32::new(1).unwrap(),
                spelling: ParameterSpelling::Anonymous,
                type_fact: TypeFact::dynamic(),
            })),
        };

        assert_eq!(translate_expr(&mut program, &expression, 8).unwrap(), 8);
        assert!(matches!(
            program.insns.as_slice(),
            [
                (Insn::Variable { index, dest: 1 }, _),
                (Insn::Integer { value: 0, dest: 2 }, _),
                (Insn::Subtract { lhs: 2, rhs: 1, dest: 8 }, _),
            ] if index.get() == 1
        ));
    }

    #[test]
    fn unary_literal_fast_paths_skip_child_emission() {
        let cases = [
            (
                UnaryOperator::Negative,
                Literal::Numeric("3".to_string()),
                -3,
            ),
            (
                UnaryOperator::BitwiseNot,
                Literal::Numeric("3".to_string()),
                !3,
            ),
        ];

        for (operator, literal, expected) in cases {
            let mut program = program();
            let expression = hir::Expr::Unary {
                operator,
                expr: Box::new(hir::Expr::Literal(literal)),
            };
            translate_expr(&mut program, &expression, 4).unwrap();
            assert!(matches!(
                program.insns.as_slice(),
                [(Insn::Integer { value, dest: 4 }, _)] if *value == expected
            ));
        }

        let mut program = program();
        let expression = hir::Expr::Unary {
            operator: UnaryOperator::BitwiseNot,
            expr: Box::new(hir::Expr::Literal(Literal::Null)),
        };
        translate_expr(&mut program, &expression, 4).unwrap();
        assert!(matches!(
            program.insns.as_slice(),
            [(
                Insn::Null {
                    dest: 4,
                    dest_end: None
                },
                _
            )]
        ));
    }

    #[test]
    fn null_test_lowering_keeps_existing_sequence_for_nested_expressions() {
        for is_null in [true, false] {
            let value = hir::Expr::Binary {
                lhs: Box::new(hir::Expr::Literal(Literal::Numeric("2".to_string()))),
                operator: Operator::Add,
                rhs: Box::new(hir::Expr::Literal(Literal::Numeric("3".to_string()))),
                array_concat: false,
                custom: None,
                comparison: None,
            };
            let expression = if is_null {
                hir::Expr::IsNull(Box::new(value))
            } else {
                hir::Expr::NotNull(Box::new(value))
            };
            let mut program = program();

            assert_eq!(translate_expr(&mut program, &expression, 8).unwrap(), 8);
            assert!(matches!(
                &program.insns[..4],
                [
                    (Insn::Integer { value: 2, dest: 2 }, _),
                    (Insn::Integer { value: 3, dest: 3 }, _),
                    (
                        Insn::Add {
                            lhs: 2,
                            rhs: 3,
                            dest: 1
                        },
                        _
                    ),
                    (Insn::Integer { value: 1, dest: 8 }, _),
                ]
            ));
            if is_null {
                assert!(matches!(program.insns[4].0, Insn::IsNull { reg: 1, .. }));
            } else {
                assert!(matches!(program.insns[4].0, Insn::NotNull { reg: 1, .. }));
            }
            assert!(matches!(
                program.insns[5].0,
                Insn::Integer { value: 0, dest: 8 }
            ));
        }
    }

    #[test]
    fn truth_test_lowering_keeps_existing_flags_for_nested_expressions() {
        for (is_true, negated, expected_invert) in [
            (true, false, false),
            (false, false, true),
            (true, true, true),
            (false, true, false),
        ] {
            let expression = hir::Expr::TruthTest {
                expr: Box::new(hir::Expr::Binary {
                    lhs: Box::new(hir::Expr::Literal(Literal::Numeric("2".to_string()))),
                    operator: Operator::Add,
                    rhs: Box::new(hir::Expr::Literal(Literal::Numeric("3".to_string()))),
                    array_concat: false,
                    custom: None,
                    comparison: None,
                }),
                is_true,
                negated,
            };
            let mut program = program();

            assert_eq!(translate_expr(&mut program, &expression, 8).unwrap(), 8);
            assert!(matches!(
                program.insns.as_slice(),
                [
                    (Insn::Integer { value: 2, dest: 2 }, _),
                    (Insn::Integer { value: 3, dest: 3 }, _),
                    (
                        Insn::Add {
                            lhs: 2,
                            rhs: 3,
                            dest: 1
                        },
                        _
                    ),
                    (
                        Insn::IsTrue {
                            reg: 1,
                            dest: 8,
                            null_value,
                            invert,
                        },
                        _
                    ),
                ] if *null_value == negated && *invert == expected_invert
            ));
        }
    }

    #[test]
    fn collate_lowers_only_its_child_and_comparison_uses_resolved_metadata() {
        let collation = hir::CatalogObject::new(
            hir::CatalogObjectId::new(1),
            hir::CatalogSnapshot::from_id(1),
            None,
            crate::sync::Arc::new(crate::translate::collate::CollationSeq::NoCase),
        );
        let expression = hir::Expr::Binary {
            lhs: Box::new(hir::Expr::Collate {
                expr: Box::new(hir::Expr::Literal(Literal::String("'left'".to_string()))),
                collation: collation.clone(),
            }),
            operator: Operator::Equals,
            rhs: Box::new(hir::Expr::Literal(Literal::String("'right'".to_string()))),
            array_concat: false,
            custom: None,
            comparison: Some(hir::ComparisonSemantics {
                components: vec![hir::ComparisonComponent {
                    affinity: crate::vdbe::affinity::Affinity::Text,
                    collation: Some(collation),
                    array: false,
                }],
            }),
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert!(matches!(
            &program.insns[..2],
            [
                (Insn::String8 { value, dest: 1 }, _),
                (Insn::String8 { dest: 2, .. }, _),
            ] if value == "left"
        ));
        assert!(matches!(
            program.insns[3].0,
            Insn::Eq {
                collation: Some(crate::translate::collate::CollationSeq::NoCase),
                ..
            }
        ));
        assert_eq!(program.curr_collation_ctx(), None);
    }

    #[test]
    fn builtin_cast_uses_resolved_affinity_and_skips_type_parameters() {
        for affinity in [
            crate::vdbe::affinity::Affinity::Numeric,
            crate::vdbe::affinity::Affinity::Text,
        ] {
            let expression = hir::Expr::Cast {
                expr: Box::new(hir::Expr::Binary {
                    lhs: Box::new(hir::Expr::Literal(Literal::Numeric("2".to_string()))),
                    operator: Operator::Add,
                    rhs: Box::new(hir::Expr::Literal(Literal::Numeric("3".to_string()))),
                    array_concat: false,
                    custom: None,
                    comparison: None,
                }),
                target: hir::TypeName {
                    name: "resolved".to_string(),
                    parameters: vec![hir::Expr::Literal(Literal::Numeric("99".to_string()))],
                    array_dimensions: 0,
                    type_fact: TypeFact::dynamic(),
                    affinity,
                    programs: hir::BoundCastPrograms {
                        encode: Vec::new(),
                        domain: None,
                        apply_builtin_affinity: true,
                    },
                },
            };
            let mut program = program();

            assert_eq!(translate_expr(&mut program, &expression, 8).unwrap(), 8);
            assert!(matches!(
                program.insns.as_slice(),
                [
                    (Insn::Integer { value: 2, dest: 1 }, _),
                    (Insn::Integer { value: 3, dest: 2 }, _),
                    (
                        Insn::Add {
                            lhs: 1,
                            rhs: 2,
                            dest: 8
                        },
                        _
                    ),
                    (
                        Insn::Cast {
                            reg: 8,
                            affinity: emitted_affinity
                        },
                        _
                    ),
                ] if *emitted_affinity == affinity
            ));
        }
    }

    #[test]
    fn simple_case_evaluates_base_once_and_uses_resolved_comparisons() {
        let collation = hir::CatalogObject::new(
            hir::CatalogObjectId::new(1),
            hir::CatalogSnapshot::from_id(1),
            None,
            crate::sync::Arc::new(crate::translate::collate::CollationSeq::NoCase),
        );
        let expression = hir::Expr::Case {
            base: Some(Box::new(hir::Expr::Literal(Literal::Numeric(
                "99".to_string(),
            )))),
            when_then: vec![
                (
                    hir::Expr::Literal(Literal::Numeric("1".to_string())),
                    hir::Expr::Literal(Literal::String("'one'".to_string())),
                ),
                (
                    hir::Expr::Literal(Literal::Numeric("2".to_string())),
                    hir::Expr::Literal(Literal::String("'two'".to_string())),
                ),
            ],
            else_expr: Some(Box::new(hir::Expr::Literal(Literal::String(
                "'other'".to_string(),
            )))),
            base_comparisons: vec![
                hir::ComparisonSemantics {
                    components: vec![hir::ComparisonComponent {
                        affinity: crate::vdbe::affinity::Affinity::Text,
                        collation: Some(collation),
                        array: false,
                    }],
                },
                numeric_comparison(),
            ],
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(instruction, _)| {
                    matches!(instruction, Insn::Integer { value: 99, .. })
                })
                .count(),
            1
        );
        let comparisons = program
            .insns
            .iter()
            .filter_map(|(instruction, _)| match instruction {
                Insn::Ne {
                    lhs,
                    rhs,
                    flags,
                    collation,
                    ..
                } => Some((
                    *lhs,
                    *rhs,
                    flags.get_affinity(),
                    flags.has_jump_if_null(),
                    *collation,
                )),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            comparisons,
            vec![
                (
                    1,
                    2,
                    crate::vdbe::affinity::Affinity::Text,
                    true,
                    Some(crate::translate::collate::CollationSeq::NoCase),
                ),
                (1, 2, crate::vdbe::affinity::Affinity::Numeric, true, None,),
            ]
        );
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::Goto { .. }))
                .count(),
            2
        );
    }

    #[test]
    fn searched_case_uses_if_not_and_missing_else_writes_null() {
        let expression = hir::Expr::Case {
            base: None,
            when_then: vec![
                (
                    hir::Expr::Literal(Literal::Null),
                    hir::Expr::Literal(Literal::String("'null'".to_string())),
                ),
                (
                    hir::Expr::Literal(Literal::Numeric("1".to_string())),
                    hir::Expr::Literal(Literal::String("'true'".to_string())),
                ),
            ],
            else_expr: None,
            base_comparisons: Vec::new(),
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        let tests = program
            .insns
            .iter()
            .filter_map(|(instruction, _)| match instruction {
                Insn::IfNot {
                    reg, jump_if_null, ..
                } => Some((*reg, *jump_if_null)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(tests, vec![(1, true), (1, true)]);
        assert!(matches!(
            program.insns.last().map(|(instruction, _)| instruction),
            Some(Insn::Null {
                dest: 8,
                dest_end: None,
            })
        ));
    }

    #[test]
    fn case_lowering_does_not_use_the_call_stack() {
        let mut expression = hir::Expr::Literal(Literal::Numeric("1".to_string()));
        for _ in 0..10_000 {
            expression = hir::Expr::Case {
                base: None,
                when_then: vec![(
                    hir::Expr::Literal(Literal::Numeric("1".to_string())),
                    expression,
                )],
                else_expr: Some(Box::new(hir::Expr::Literal(Literal::Null))),
                base_comparisons: Vec::new(),
            };
        }

        let mut program = program();
        translate_expr(&mut program, &expression, 8).unwrap();
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::IfNot { .. }))
                .count(),
            10_000
        );

        std::mem::forget(expression);
    }

    #[test]
    fn empty_in_list_skips_its_left_operand() {
        for (negated, expected) in [(false, 0), (true, 1)] {
            let expression = hir::Expr::InList {
                lhs: Box::new(hir::Expr::Parameter(hir::Parameter {
                    index: NonZeroU32::new(1).unwrap(),
                    spelling: ParameterSpelling::Anonymous,
                    type_fact: TypeFact::dynamic(),
                })),
                negated,
                values: Vec::new(),
                comparisons: Vec::new(),
            };
            let mut program = program();

            translate_expr(&mut program, &expression, 8).unwrap();
            assert!(matches!(
                program.insns.as_slice(),
                [(Insn::Integer { value, dest: 8 }, _)] if *value == expected
            ));
        }
    }

    #[test]
    fn scalar_in_list_uses_resolved_comparisons_and_tracks_nulls() {
        let collation = hir::CatalogObject::new(
            hir::CatalogObjectId::new(1),
            hir::CatalogSnapshot::from_id(1),
            None,
            crate::sync::Arc::new(crate::translate::collate::CollationSeq::NoCase),
        );
        let comparison = hir::ComparisonSemantics {
            components: vec![hir::ComparisonComponent {
                affinity: crate::vdbe::affinity::Affinity::Text,
                collation: Some(collation),
                array: true,
            }],
        };
        let expression = hir::Expr::InList {
            lhs: Box::new(hir::Expr::Literal(Literal::String("'a'".to_string()))),
            negated: true,
            values: vec![
                hir::Expr::Literal(Literal::String("'b'".to_string())),
                hir::Expr::Literal(Literal::Null),
            ],
            comparisons: vec![comparison.clone(), comparison],
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::BitAnd { .. }))
                .count(),
            3
        );
        let comparisons = program
            .insns
            .iter()
            .filter_map(|(instruction, _)| match instruction {
                Insn::Eq {
                    flags, collation, ..
                } => Some((flags.get_affinity(), flags.has_array_cmp(), *collation)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            comparisons,
            vec![
                (
                    crate::vdbe::affinity::Affinity::Text,
                    true,
                    Some(crate::translate::collate::CollationSeq::NoCase),
                );
                2
            ]
        );
        assert!(program
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::IsNull { .. })));
        assert!(program
            .insns
            .iter()
            .any(|(instruction, _)| matches!(instruction, Insn::Not { .. })));
        assert!(matches!(
            program.insns.last().map(|(instruction, _)| instruction),
            Some(Insn::Copy {
                dst_reg: 8,
                extra_amount: 0,
                ..
            })
        ));
    }

    #[test]
    fn row_in_list_compares_each_component() {
        let expression = hir::Expr::InList {
            lhs: Box::new(hir::Expr::Row(vec![
                hir::Expr::Literal(Literal::Numeric("1".to_string())),
                hir::Expr::Literal(Literal::Numeric("2".to_string())),
            ])),
            negated: false,
            values: vec![hir::Expr::Row(vec![
                hir::Expr::Literal(Literal::Numeric("3".to_string())),
                hir::Expr::Literal(Literal::Numeric("4".to_string())),
            ])],
            comparisons: vec![numeric_row_comparison(2)],
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        let comparisons = program
            .insns
            .iter()
            .filter_map(|(instruction, _)| match instruction {
                Insn::Ne { lhs, rhs, .. } => Some(("ne", *lhs, *rhs)),
                Insn::Eq { lhs, rhs, .. } => Some(("eq", *lhs, *rhs)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(comparisons, vec![("ne", 2, 5), ("eq", 3, 6)]);
    }

    #[test]
    fn nested_in_list_lowering_does_not_use_the_call_stack() {
        let mut expression = hir::Expr::Literal(Literal::Numeric("1".to_string()));
        for _ in 0..10_000 {
            expression = hir::Expr::InList {
                lhs: Box::new(expression),
                negated: false,
                values: vec![hir::Expr::Literal(Literal::Numeric("1".to_string()))],
                comparisons: vec![numeric_comparison()],
            };
        }

        let mut program = program();
        translate_expr(&mut program, &expression, 8).unwrap();
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::Eq { .. }))
                .count(),
            10_000
        );

        std::mem::forget(expression);
    }

    #[test]
    fn like_uses_legacy_argument_order_and_resolved_function() {
        let expression = hir::Expr::Like {
            lhs: Box::new(hir::Expr::Literal(Literal::String("'value'".to_string()))),
            negated: false,
            operator: turso_parser::ast::LikeOperator::Like,
            function: resolved_function(Func::Scalar(ScalarFunc::Like)),
            argument_count: 2,
            rhs: Box::new(hir::Expr::Literal(Literal::String("'pattern'".to_string()))),
            escape: None,
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert!(matches!(
            program.insns.as_slice(),
            [
                (Insn::String8 { value: lhs, dest: 2 }, _),
                (Insn::String8 { value: rhs, dest: 1 }, _),
                (
                    Insn::Function {
                        constant_mask: 1,
                        start_reg: 1,
                        dest: 8,
                        func:
                            FuncCtx {
                                func: Func::Scalar(ScalarFunc::Like),
                                arg_count: 2,
                            },
                    },
                    _,
                ),
            ] if lhs == "value" && rhs == "pattern"
        ));
    }

    #[test]
    fn like_escape_and_negation_keep_existing_register_shape() {
        let expression = hir::Expr::Like {
            lhs: Box::new(hir::Expr::Literal(Literal::String("'value'".to_string()))),
            negated: true,
            operator: turso_parser::ast::LikeOperator::Like,
            function: resolved_function(Func::Scalar(ScalarFunc::Like)),
            argument_count: 3,
            rhs: Box::new(hir::Expr::Literal(Literal::String("'pattern'".to_string()))),
            escape: Some(Box::new(hir::Expr::Literal(Literal::String(
                "'!'".to_string(),
            )))),
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert!(matches!(
            program.insns.as_slice(),
            [
                (Insn::String8 { dest: 2, .. }, _),
                (Insn::String8 { dest: 1, .. }, _),
                (Insn::String8 { dest: 3, .. }, _),
                (
                    Insn::Function {
                        constant_mask: 1,
                        start_reg: 1,
                        dest: 4,
                        func: FuncCtx { arg_count: 3, .. },
                    },
                    _,
                ),
                (Insn::Not { reg: 4, dest: 8 }, _),
            ]
        ));
    }

    #[test]
    fn regexp_uses_the_function_resolved_by_semantic_analysis() {
        let expression = hir::Expr::Like {
            lhs: Box::new(hir::Expr::Literal(Literal::String("'value'".to_string()))),
            negated: false,
            operator: turso_parser::ast::LikeOperator::Regexp,
            function: resolved_function(Func::Scalar(ScalarFunc::Abs)),
            argument_count: 2,
            rhs: Box::new(hir::Expr::Literal(Literal::String("'pattern'".to_string()))),
            escape: None,
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert!(matches!(
            program.insns.last().map(|(instruction, _)| instruction),
            Some(Insn::Function {
                constant_mask: 0,
                start_reg: 1,
                dest: 8,
                func: FuncCtx {
                    func: Func::Scalar(ScalarFunc::Abs),
                    arg_count: 2,
                },
            })
        ));
    }

    #[test]
    fn match_places_row_columns_before_the_query() {
        let expression = hir::Expr::Like {
            lhs: Box::new(hir::Expr::Row(vec![
                hir::Expr::Literal(Literal::String("'one'".to_string())),
                hir::Expr::Literal(Literal::String("'two'".to_string())),
            ])),
            negated: false,
            operator: turso_parser::ast::LikeOperator::Match,
            function: resolved_function(Func::Scalar(ScalarFunc::Abs)),
            argument_count: 3,
            rhs: Box::new(hir::Expr::Literal(Literal::String("'query'".to_string()))),
            escape: None,
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert!(matches!(
            program.insns.as_slice(),
            [
                (Insn::String8 { dest: 1, .. }, _),
                (Insn::String8 { dest: 2, .. }, _),
                (Insn::String8 { dest: 3, .. }, _),
                (
                    Insn::Function {
                        constant_mask: 0,
                        start_reg: 1,
                        dest: 8,
                        func: FuncCtx { arg_count: 3, .. },
                    },
                    _,
                ),
            ]
        ));
    }

    #[test]
    fn nested_like_lowering_does_not_use_the_call_stack() {
        let function = resolved_function(Func::Scalar(ScalarFunc::Like));
        let mut expression = hir::Expr::Literal(Literal::String("'value'".to_string()));
        for _ in 0..10_000 {
            expression = hir::Expr::Like {
                lhs: Box::new(expression),
                negated: false,
                operator: turso_parser::ast::LikeOperator::Like,
                function: function.clone(),
                argument_count: 2,
                rhs: Box::new(hir::Expr::Literal(Literal::String("'pattern'".to_string()))),
                escape: None,
            };
        }

        let mut program = program();
        translate_expr(&mut program, &expression, 8).unwrap();
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::Function { .. }))
                .count(),
            10_000
        );

        std::mem::forget(expression);
    }

    #[test]
    fn empty_array_uses_the_existing_make_array_opcode() {
        let mut program = program();

        translate_expr(&mut program, &hir::Expr::Array(Vec::new()), 8).unwrap();
        assert!(matches!(
            program.insns.as_slice(),
            [(
                Insn::MakeArray {
                    start_reg: 1,
                    count: 0,
                    dest: 8,
                },
                _,
            )]
        ));
    }

    #[test]
    fn array_elements_use_consecutive_registers() {
        let expression = hir::Expr::Array(vec![
            hir::Expr::Literal(Literal::Numeric("1".to_string())),
            hir::Expr::Binary {
                lhs: Box::new(hir::Expr::Literal(Literal::Numeric("2".to_string()))),
                operator: Operator::Add,
                rhs: Box::new(hir::Expr::Literal(Literal::Numeric("3".to_string()))),
                array_concat: false,
                custom: None,
                comparison: None,
            },
        ]);
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert!(matches!(
            program.insns.as_slice(),
            [
                (Insn::Integer { value: 1, dest: 1 }, _),
                (Insn::Integer { value: 2, dest: 3 }, _),
                (Insn::Integer { value: 3, dest: 4 }, _),
                (
                    Insn::Add {
                        lhs: 3,
                        rhs: 4,
                        dest: 2,
                    },
                    _,
                ),
                (
                    Insn::MakeArray {
                        start_reg: 1,
                        count: 2,
                        dest: 8,
                    },
                    _,
                ),
            ]
        ));
    }

    #[test]
    fn subscript_uses_separate_base_and_index_registers() {
        let expression = hir::Expr::Subscript {
            base: Box::new(hir::Expr::Array(vec![
                hir::Expr::Literal(Literal::Numeric("10".to_string())),
                hir::Expr::Literal(Literal::Numeric("20".to_string())),
            ])),
            index: Box::new(hir::Expr::Literal(Literal::Numeric("1".to_string()))),
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert!(matches!(
            program.insns.as_slice(),
            [
                (Insn::Integer { value: 10, dest: 3 }, _),
                (Insn::Integer { value: 20, dest: 4 }, _),
                (
                    Insn::MakeArray {
                        start_reg: 3,
                        count: 2,
                        dest: 1,
                    },
                    _,
                ),
                (Insn::Integer { value: 1, dest: 2 }, _),
                (
                    Insn::ArrayElement {
                        array_reg: 1,
                        index_reg: 2,
                        dest: 8,
                    },
                    _,
                ),
            ]
        ));
    }

    #[test]
    fn nested_array_lowering_does_not_use_the_call_stack() {
        let mut expression = hir::Expr::Literal(Literal::Numeric("1".to_string()));
        for _ in 0..10_000 {
            expression = hir::Expr::Array(vec![expression]);
        }

        let mut program = program();
        translate_expr(&mut program, &expression, 8).unwrap();
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::MakeArray { .. }))
                .count(),
            10_000
        );

        std::mem::forget(expression);
    }

    #[test]
    fn field_access_uses_the_resolved_member_index() {
        let cases = [
            (
                hir::FieldAccessKind::Struct { field_index: 7 },
                resolved_type(crate::schema::TypeDefKind::Struct(
                    crate::schema::StructDef { fields: Vec::new() },
                )),
            ),
            (
                hir::FieldAccessKind::Union { tag_index: 3 },
                resolved_type(crate::schema::TypeDefKind::Union(crate::schema::UnionDef {
                    variants: Vec::new(),
                    tag_names: crate::sync::Arc::from(Vec::<String>::new()),
                })),
            ),
        ];

        for (kind, container_type) in cases {
            let expression = hir::Expr::FieldAccess(hir::FieldAccess {
                base: Box::new(hir::Expr::Literal(Literal::Numeric("1".to_string()))),
                field_name: "already_resolved".to_string(),
                kind,
                container_type,
                result_type: TypeFact::dynamic(),
            });
            let mut program = program();

            translate_expr(&mut program, &expression, 8).unwrap();
            assert!(matches!(
                (&program.insns[0].0, &program.insns[1].0, kind),
                (
                    Insn::Integer { dest: 1, .. },
                    Insn::StructField {
                        src_reg: 1,
                        field_index: 7,
                        dest: 8,
                    },
                    hir::FieldAccessKind::Struct { .. },
                ) | (
                    Insn::Integer { dest: 1, .. },
                    Insn::UnionExtract {
                        src_reg: 1,
                        expected_tag: 3,
                        dest: 8,
                    },
                    hir::FieldAccessKind::Union { .. },
                )
            ));
        }
    }

    #[test]
    fn nested_field_access_writes_each_result_into_its_parent_register() {
        let struct_type = resolved_type(crate::schema::TypeDefKind::Struct(
            crate::schema::StructDef { fields: Vec::new() },
        ));
        let union_type =
            resolved_type(crate::schema::TypeDefKind::Union(crate::schema::UnionDef {
                variants: Vec::new(),
                tag_names: crate::sync::Arc::from(Vec::<String>::new()),
            }));
        let expression = hir::Expr::FieldAccess(hir::FieldAccess {
            base: Box::new(hir::Expr::FieldAccess(hir::FieldAccess {
                base: Box::new(hir::Expr::Literal(Literal::Numeric("1".to_string()))),
                field_name: "variant".to_string(),
                kind: hir::FieldAccessKind::Union { tag_index: 2 },
                container_type: union_type,
                result_type: TypeFact::dynamic(),
            })),
            field_name: "field".to_string(),
            kind: hir::FieldAccessKind::Struct { field_index: 4 },
            container_type: struct_type,
            result_type: TypeFact::dynamic(),
        });
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert!(matches!(
            program.insns.as_slice(),
            [
                (Insn::Integer { dest: 2, .. }, _),
                (
                    Insn::UnionExtract {
                        src_reg: 2,
                        expected_tag: 2,
                        dest: 1,
                    },
                    _,
                ),
                (
                    Insn::StructField {
                        src_reg: 1,
                        field_index: 4,
                        dest: 8,
                    },
                    _,
                ),
            ]
        ));
    }

    #[test]
    fn nested_field_access_lowering_does_not_use_the_call_stack() {
        let container_type = resolved_type(crate::schema::TypeDefKind::Struct(
            crate::schema::StructDef { fields: Vec::new() },
        ));
        let mut expression = hir::Expr::Literal(Literal::Numeric("1".to_string()));
        for _ in 0..10_000 {
            expression = hir::Expr::FieldAccess(hir::FieldAccess {
                base: Box::new(expression),
                field_name: "field".to_string(),
                kind: hir::FieldAccessKind::Struct { field_index: 0 },
                container_type: container_type.clone(),
                result_type: TypeFact::dynamic(),
            });
        }

        let mut program = program();
        translate_expr(&mut program, &expression, 8).unwrap();
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::StructField { .. }))
                .count(),
            10_000
        );

        std::mem::forget(expression);
    }

    #[test]
    fn standalone_abort_embeds_a_literal_message() {
        let expression = hir::Expr::Raise {
            action: ResolveType::Abort,
            message: Some(Box::new(hir::Expr::Literal(Literal::String(
                "'stop'' now'".to_string(),
            )))),
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert!(matches!(
            program.insns.as_slice(),
            [(
                Insn::Halt {
                    err_code: SQLITE_ERROR,
                    description,
                    on_error: Some(ResolveType::Abort),
                    description_reg: None,
                },
                _,
            )] if description == "stop' now"
        ));
    }

    #[test]
    fn dynamic_raise_message_uses_one_result_register() {
        let expression = hir::Expr::Raise {
            action: ResolveType::Abort,
            message: Some(Box::new(hir::Expr::Binary {
                lhs: Box::new(hir::Expr::Literal(Literal::Numeric("2".to_string()))),
                operator: Operator::Add,
                rhs: Box::new(hir::Expr::Literal(Literal::Numeric("3".to_string()))),
                array_concat: false,
                custom: None,
                comparison: None,
            })),
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert!(matches!(
            program.insns.as_slice(),
            [
                (Insn::Integer { value: 2, dest: 2 }, _),
                (Insn::Integer { value: 3, dest: 3 }, _),
                (
                    Insn::Add {
                        lhs: 2,
                        rhs: 3,
                        dest: 1,
                    },
                    _,
                ),
                (
                    Insn::Halt {
                        err_code: SQLITE_ERROR,
                        description_reg: Some(1),
                        on_error: Some(ResolveType::Abort),
                        ..
                    },
                    _,
                ),
            ]
        ));
    }

    #[test]
    fn trigger_raise_uses_trigger_error_codes_and_ignore_shape() {
        let cases = [
            (
                hir::Expr::Raise {
                    action: ResolveType::Ignore,
                    message: None,
                },
                ResolveType::Ignore,
                0,
            ),
            (
                hir::Expr::Raise {
                    action: ResolveType::Fail,
                    message: Some(Box::new(hir::Expr::Literal(Literal::String(
                        "'stop'".to_string(),
                    )))),
                },
                ResolveType::Fail,
                SQLITE_CONSTRAINT_TRIGGER,
            ),
        ];

        for (expression, expected_action, expected_code) in cases {
            let mut program = trigger_program();
            translate_expr(&mut program, &expression, 8).unwrap();
            assert!(matches!(
                program.insns.as_slice(),
                [(Insn::Halt { err_code, on_error: Some(action), .. }, _)]
                    if *err_code == expected_code && *action == expected_action
            ));
        }
    }

    #[test]
    fn nested_raise_lowering_does_not_use_the_call_stack() {
        let mut expression = hir::Expr::Literal(Literal::Numeric("1".to_string()));
        for _ in 0..10_000 {
            expression = hir::Expr::Raise {
                action: ResolveType::Abort,
                message: Some(Box::new(expression)),
            };
        }

        let mut program = program();
        translate_expr(&mut program, &expression, 8).unwrap();
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::Halt { .. }))
                .count(),
            10_000
        );

        std::mem::forget(expression);
    }

    #[derive(Clone, Copy)]
    enum ExpectedBinaryInsn {
        Add,
        Subtract,
        Multiply,
        Divide,
        Remainder,
        And,
        Or,
        BitAnd,
        BitOr,
        ShiftLeft,
        ShiftRight,
    }

    #[derive(Clone, Copy)]
    enum ExpectedComparisonInsn {
        Eq,
        Ne,
        Lt,
        Le,
        Gt,
        Ge,
    }

    fn comparison_expression(operator: Operator, component: hir::ComparisonComponent) -> hir::Expr {
        hir::Expr::Binary {
            lhs: Box::new(hir::Expr::Literal(Literal::Numeric("2".to_string()))),
            operator,
            rhs: Box::new(hir::Expr::Literal(Literal::Numeric("3".to_string()))),
            array_concat: false,
            custom: None,
            comparison: Some(hir::ComparisonSemantics {
                components: vec![component],
            }),
        }
    }

    fn row_comparison_expression(
        operator: Operator,
        components: Vec<hir::ComparisonComponent>,
    ) -> hir::Expr {
        hir::Expr::Binary {
            lhs: Box::new(hir::Expr::Row(vec![
                hir::Expr::Literal(Literal::String("'a'".to_string())),
                hir::Expr::Literal(Literal::Numeric("2".to_string())),
            ])),
            operator,
            rhs: Box::new(hir::Expr::Row(vec![
                hir::Expr::Literal(Literal::String("'b'".to_string())),
                hir::Expr::Literal(Literal::Numeric("3".to_string())),
            ])),
            array_concat: false,
            custom: None,
            comparison: Some(hir::ComparisonSemantics { components }),
        }
    }

    #[test]
    fn ordinary_binary_lowering_keeps_existing_register_and_opcode_shape() {
        let cases = [
            (Operator::Add, ExpectedBinaryInsn::Add),
            (Operator::Subtract, ExpectedBinaryInsn::Subtract),
            (Operator::Multiply, ExpectedBinaryInsn::Multiply),
            (Operator::Divide, ExpectedBinaryInsn::Divide),
            (Operator::Modulus, ExpectedBinaryInsn::Remainder),
            (Operator::And, ExpectedBinaryInsn::And),
            (Operator::Or, ExpectedBinaryInsn::Or),
            (Operator::BitwiseAnd, ExpectedBinaryInsn::BitAnd),
            (Operator::BitwiseOr, ExpectedBinaryInsn::BitOr),
            (Operator::LeftShift, ExpectedBinaryInsn::ShiftLeft),
            (Operator::RightShift, ExpectedBinaryInsn::ShiftRight),
        ];

        for (operator, expected) in cases {
            let expression = hir::Expr::Binary {
                lhs: Box::new(hir::Expr::Literal(Literal::Numeric("2".to_string()))),
                operator,
                rhs: Box::new(hir::Expr::Literal(Literal::Numeric("3".to_string()))),
                array_concat: false,
                custom: None,
                comparison: None,
            };
            let mut program = program();

            assert_eq!(translate_expr(&mut program, &expression, 8).unwrap(), 8);
            assert!(matches!(
                &program.insns[..2],
                [
                    (Insn::Integer { value: 2, dest: 1 }, _),
                    (Insn::Integer { value: 3, dest: 2 }, _),
                ]
            ));
            let instruction = &program.insns[2].0;
            match expected {
                ExpectedBinaryInsn::Add => assert!(matches!(
                    instruction,
                    Insn::Add {
                        lhs: 1,
                        rhs: 2,
                        dest: 8
                    }
                )),
                ExpectedBinaryInsn::Subtract => assert!(matches!(
                    instruction,
                    Insn::Subtract {
                        lhs: 1,
                        rhs: 2,
                        dest: 8
                    }
                )),
                ExpectedBinaryInsn::Multiply => assert!(matches!(
                    instruction,
                    Insn::Multiply {
                        lhs: 1,
                        rhs: 2,
                        dest: 8
                    }
                )),
                ExpectedBinaryInsn::Divide => assert!(matches!(
                    instruction,
                    Insn::Divide {
                        lhs: 1,
                        rhs: 2,
                        dest: 8
                    }
                )),
                ExpectedBinaryInsn::Remainder => assert!(matches!(
                    instruction,
                    Insn::Remainder {
                        lhs: 1,
                        rhs: 2,
                        dest: 8
                    }
                )),
                ExpectedBinaryInsn::And => assert!(matches!(
                    instruction,
                    Insn::And {
                        lhs: 1,
                        rhs: 2,
                        dest: 8
                    }
                )),
                ExpectedBinaryInsn::Or => assert!(matches!(
                    instruction,
                    Insn::Or {
                        lhs: 1,
                        rhs: 2,
                        dest: 8
                    }
                )),
                ExpectedBinaryInsn::BitAnd => assert!(matches!(
                    instruction,
                    Insn::BitAnd {
                        lhs: 1,
                        rhs: 2,
                        dest: 8
                    }
                )),
                ExpectedBinaryInsn::BitOr => assert!(matches!(
                    instruction,
                    Insn::BitOr {
                        lhs: 1,
                        rhs: 2,
                        dest: 8
                    }
                )),
                ExpectedBinaryInsn::ShiftLeft => assert!(matches!(
                    instruction,
                    Insn::ShiftLeft {
                        lhs: 1,
                        rhs: 2,
                        dest: 8
                    }
                )),
                ExpectedBinaryInsn::ShiftRight => assert!(matches!(
                    instruction,
                    Insn::ShiftRight {
                        lhs: 1,
                        rhs: 2,
                        dest: 8
                    }
                )),
            }
        }
    }

    #[cfg(feature = "json")]
    #[test]
    fn json_arrow_lowering_keeps_existing_function_and_register_shape() {
        for (operator, expected) in [
            (Operator::ArrowRight, JsonFunc::JsonArrowExtract),
            (Operator::ArrowRightShift, JsonFunc::JsonArrowShiftExtract),
        ] {
            let expression = hir::Expr::Binary {
                lhs: Box::new(hir::Expr::Literal(Literal::String(
                    "'{\"value\": 3}'".to_string(),
                ))),
                operator,
                rhs: Box::new(hir::Expr::Literal(Literal::String("'$.value'".to_string()))),
                array_concat: false,
                custom: None,
                comparison: None,
            };
            let mut program = program();

            assert_eq!(translate_expr(&mut program, &expression, 8).unwrap(), 8);
            assert!(matches!(
                &program.insns[..2],
                [
                    (Insn::String8 { dest: 1, .. }, _),
                    (Insn::String8 { dest: 2, .. }, _),
                ]
            ));
            let Insn::Function {
                constant_mask: 0,
                start_reg: 1,
                dest: 8,
                func:
                    FuncCtx {
                        func: Func::Json(actual),
                        arg_count: 2,
                    },
            } = &program.insns[2].0
            else {
                panic!("JSON arrow emits its resolved built-in function")
            };
            assert_eq!(actual, &expected);
        }
    }

    fn numeric_comparison() -> hir::ComparisonSemantics {
        hir::ComparisonSemantics {
            components: vec![hir::ComparisonComponent {
                affinity: crate::vdbe::affinity::Affinity::Numeric,
                collation: None,
                array: false,
            }],
        }
    }

    fn numeric_row_comparison(width: usize) -> hir::ComparisonSemantics {
        hir::ComparisonSemantics {
            components: (0..width)
                .map(|_| hir::ComparisonComponent {
                    affinity: crate::vdbe::affinity::Affinity::Numeric,
                    collation: None,
                    array: false,
                })
                .collect(),
        }
    }

    #[test]
    fn scalar_between_lowering_keeps_existing_comparison_order() {
        for negated in [false, true] {
            let expression = hir::Expr::Between {
                expr: Box::new(hir::Expr::Literal(Literal::Numeric("2".to_string()))),
                negated,
                start: Box::new(hir::Expr::Literal(Literal::Numeric("1".to_string()))),
                end: Box::new(hir::Expr::Literal(Literal::Numeric("3".to_string()))),
                start_comparison: numeric_comparison(),
                end_comparison: numeric_comparison(),
            };
            let mut program = program();

            assert_eq!(translate_expr(&mut program, &expression, 8).unwrap(), 8);
            assert!(matches!(
                &program.insns[..3],
                [
                    (Insn::Integer { value: 2, dest: 1 }, _),
                    (Insn::Integer { value: 1, dest: 3 }, _),
                    (Insn::Integer { value: 1, dest: 2 }, _),
                ]
            ));
            if negated {
                assert!(matches!(
                    program.insns[3].0,
                    Insn::Lt { lhs: 1, rhs: 3, .. }
                ));
            } else {
                assert!(matches!(
                    program.insns[3].0,
                    Insn::Ge { lhs: 1, rhs: 3, .. }
                ));
            }
            assert!(matches!(
                program.insns[4].0,
                Insn::ZeroOrNull {
                    rg1: 1,
                    rg2: 3,
                    dest: 2,
                }
            ));
            assert!(matches!(
                &program.insns[5..7],
                [
                    (Insn::Integer { value: 3, dest: 5 }, _),
                    (Insn::Integer { value: 1, dest: 4 }, _),
                ]
            ));
            if negated {
                assert!(matches!(
                    program.insns[7].0,
                    Insn::Gt { lhs: 1, rhs: 5, .. }
                ));
                assert!(matches!(
                    program.insns[9].0,
                    Insn::Or {
                        lhs: 2,
                        rhs: 4,
                        dest: 8,
                    }
                ));
            } else {
                assert!(matches!(
                    program.insns[7].0,
                    Insn::Le { lhs: 1, rhs: 5, .. }
                ));
                assert!(matches!(
                    program.insns[9].0,
                    Insn::And {
                        lhs: 2,
                        rhs: 4,
                        dest: 8,
                    }
                ));
            }
        }
    }

    #[test]
    fn scalar_between_reuses_equivalent_value_bounds() {
        let value = hir::Expr::Literal(Literal::Numeric("7".to_string()));
        let expression = hir::Expr::Between {
            expr: Box::new(value.clone()),
            negated: false,
            start: Box::new(value.clone()),
            end: Box::new(value),
            start_comparison: numeric_comparison(),
            end_comparison: numeric_comparison(),
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(instruction, _)| {
                    matches!(instruction, Insn::Integer { value: 7, .. })
                })
                .count(),
            1
        );
        assert!(matches!(
            program.insns[2].0,
            Insn::Ge { lhs: 1, rhs: 1, .. }
        ));
        assert!(matches!(
            program.insns[5].0,
            Insn::Le { lhs: 1, rhs: 1, .. }
        ));
    }

    #[test]
    fn row_between_uses_register_ranges_resolved_facts_and_existing_order() {
        let collation = hir::CatalogObject::new(
            hir::CatalogObjectId::new(1),
            hir::CatalogSnapshot::from_id(1),
            None,
            crate::sync::Arc::new(crate::translate::collate::CollationSeq::NoCase),
        );
        let comparison = hir::ComparisonSemantics {
            components: vec![
                hir::ComparisonComponent {
                    affinity: crate::vdbe::affinity::Affinity::Text,
                    collation: Some(collation),
                    array: false,
                },
                hir::ComparisonComponent {
                    affinity: crate::vdbe::affinity::Affinity::Numeric,
                    collation: None,
                    array: false,
                },
            ],
        };
        let expression = hir::Expr::Between {
            expr: Box::new(hir::Expr::Row(vec![
                hir::Expr::Literal(Literal::Numeric("20".to_string())),
                hir::Expr::Literal(Literal::Numeric("21".to_string())),
            ])),
            negated: false,
            start: Box::new(hir::Expr::Row(vec![
                hir::Expr::Literal(Literal::Numeric("10".to_string())),
                hir::Expr::Literal(Literal::Numeric("11".to_string())),
            ])),
            end: Box::new(hir::Expr::Row(vec![
                hir::Expr::Literal(Literal::Numeric("30".to_string())),
                hir::Expr::Literal(Literal::Numeric("31".to_string())),
            ])),
            start_comparison: comparison.clone(),
            end_comparison: comparison,
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 12).unwrap();
        let value_destinations = program
            .insns
            .iter()
            .filter_map(|(instruction, _)| match instruction {
                Insn::Integer { value, dest } if matches!(*value, 10 | 11 | 20 | 21 | 30 | 31) => {
                    Some(*dest)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(value_destinations, vec![1, 2, 4, 5, 7, 8]);

        let lower_position = program
            .insns
            .iter()
            .position(|(instruction, _)| matches!(instruction, Insn::Gt { .. }))
            .expect("lower comparison is emitted");
        let end_position = program
            .insns
            .iter()
            .position(|(instruction, _)| {
                matches!(instruction, Insn::Integer { value: 30, dest: 7 })
            })
            .expect("end row is evaluated");
        assert!(lower_position < end_position);

        let comparisons = program
            .insns
            .iter()
            .filter_map(|(instruction, _)| match instruction {
                Insn::Gt {
                    lhs,
                    rhs,
                    flags,
                    collation,
                    ..
                }
                | Insn::Lt {
                    lhs,
                    rhs,
                    flags,
                    collation,
                    ..
                } => Some((*lhs, *rhs, flags.get_affinity(), *collation)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            comparisons,
            vec![
                (
                    1,
                    4,
                    crate::vdbe::affinity::Affinity::Text,
                    Some(crate::translate::collate::CollationSeq::NoCase)
                ),
                (2, 5, crate::vdbe::affinity::Affinity::Numeric, None),
                (
                    1,
                    7,
                    crate::vdbe::affinity::Affinity::Text,
                    Some(crate::translate::collate::CollationSeq::NoCase)
                ),
                (2, 8, crate::vdbe::affinity::Affinity::Numeric, None),
            ]
        );
        assert!(matches!(
            program.insns.last().map(|(instruction, _)| instruction),
            Some(Insn::And {
                lhs: 3,
                rhs: 6,
                dest: 12,
            })
        ));
    }

    #[test]
    fn row_not_between_uses_less_greater_and_or() {
        let expression = hir::Expr::Between {
            expr: Box::new(hir::Expr::Row(vec![
                hir::Expr::Literal(Literal::Numeric("2".to_string())),
                hir::Expr::Literal(Literal::Numeric("3".to_string())),
            ])),
            negated: true,
            start: Box::new(hir::Expr::Row(vec![
                hir::Expr::Literal(Literal::Numeric("1".to_string())),
                hir::Expr::Literal(Literal::Numeric("1".to_string())),
            ])),
            end: Box::new(hir::Expr::Row(vec![
                hir::Expr::Literal(Literal::Numeric("4".to_string())),
                hir::Expr::Literal(Literal::Numeric("4".to_string())),
            ])),
            start_comparison: numeric_row_comparison(2),
            end_comparison: numeric_row_comparison(2),
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 12).unwrap();
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::Lt { .. }))
                .count(),
            2
        );
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::Gt { .. }))
                .count(),
            2
        );
        assert!(matches!(
            program.insns.last().map(|(instruction, _)| instruction),
            Some(Insn::Or {
                lhs: 3,
                rhs: 6,
                dest: 12,
            })
        ));
    }

    #[test]
    fn row_between_reuses_equivalent_value_ranges() {
        let value = hir::Expr::Row(vec![
            hir::Expr::Literal(Literal::Numeric("7".to_string())),
            hir::Expr::Literal(Literal::Numeric("8".to_string())),
        ]);
        let expression = hir::Expr::Between {
            expr: Box::new(value.clone()),
            negated: false,
            start: Box::new(value.clone()),
            end: Box::new(value),
            start_comparison: numeric_row_comparison(2),
            end_comparison: numeric_row_comparison(2),
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 12).unwrap();
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(instruction, _)| matches!(
                    instruction,
                    Insn::Integer { value: 7 | 8, .. }
                ))
                .count(),
            2
        );
        let comparisons = program
            .insns
            .iter()
            .filter_map(|(instruction, _)| match instruction {
                Insn::Gt { lhs, rhs, .. } | Insn::Lt { lhs, rhs, .. } => Some((lhs, rhs)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(comparisons.len(), 4);
        assert!(comparisons.into_iter().all(|(lhs, rhs)| lhs == rhs));
    }

    #[test]
    fn row_comparison_uses_consecutive_registers_and_resolved_component_facts() {
        let collation = hir::CatalogObject::new(
            hir::CatalogObjectId::new(1),
            hir::CatalogSnapshot::from_id(1),
            None,
            crate::sync::Arc::new(crate::translate::collate::CollationSeq::NoCase),
        );
        let expression = row_comparison_expression(
            Operator::Less,
            vec![
                hir::ComparisonComponent {
                    affinity: crate::vdbe::affinity::Affinity::Text,
                    collation: Some(collation),
                    array: false,
                },
                hir::ComparisonComponent {
                    affinity: crate::vdbe::affinity::Affinity::Numeric,
                    collation: None,
                    array: false,
                },
            ],
        );
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert!(matches!(
            &program.insns[..4],
            [
                (Insn::String8 { dest: 1, .. }, _),
                (Insn::Integer { value: 2, dest: 2 }, _),
                (Insn::String8 { dest: 3, .. }, _),
                (Insn::Integer { value: 3, dest: 4 }, _),
            ]
        ));

        let comparisons = program
            .insns
            .iter()
            .filter_map(|(instruction, _)| match instruction {
                Insn::Lt {
                    lhs,
                    rhs,
                    flags,
                    collation,
                    ..
                } => Some((*lhs, *rhs, flags.get_affinity(), *collation)),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            comparisons,
            vec![
                (
                    1,
                    3,
                    crate::vdbe::affinity::Affinity::Text,
                    Some(crate::translate::collate::CollationSeq::NoCase),
                ),
                (2, 4, crate::vdbe::affinity::Affinity::Numeric, None,),
            ]
        );
    }

    #[test]
    fn row_equality_preserves_null_and_is_uses_null_equal() {
        for (operator, null_equal) in [(Operator::Equals, false), (Operator::Is, true)] {
            let expression = row_comparison_expression(
                operator,
                vec![
                    hir::ComparisonComponent {
                        affinity: crate::vdbe::affinity::Affinity::Text,
                        collation: None,
                        array: false,
                    },
                    hir::ComparisonComponent {
                        affinity: crate::vdbe::affinity::Affinity::Numeric,
                        collation: None,
                        array: false,
                    },
                ],
            );
            let mut program = program();

            translate_expr(&mut program, &expression, 8).unwrap();
            let equality_flags = program
                .insns
                .iter()
                .filter_map(|(instruction, _)| match instruction {
                    Insn::Eq { flags, .. } => Some(flags),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(equality_flags.len(), 2);
            assert!(equality_flags
                .iter()
                .all(|flags| flags.has_nulleq() == null_equal));
            assert_eq!(
                program.insns.iter().any(|(instruction, _)| matches!(
                    instruction,
                    Insn::Null {
                        dest: 8,
                        dest_end: None,
                    }
                )),
                !null_equal
            );
        }
    }

    #[test]
    fn row_ordering_emits_each_component_in_lexicographic_order() {
        for (operator, inclusive) in [
            (Operator::Less, false),
            (Operator::LessEquals, true),
            (Operator::Greater, false),
            (Operator::GreaterEquals, true),
        ] {
            let expression = row_comparison_expression(
                operator,
                vec![
                    hir::ComparisonComponent {
                        affinity: crate::vdbe::affinity::Affinity::Text,
                        collation: None,
                        array: false,
                    },
                    hir::ComparisonComponent {
                        affinity: crate::vdbe::affinity::Affinity::Numeric,
                        collation: None,
                        array: false,
                    },
                ],
            );
            let mut program = program();

            translate_expr(&mut program, &expression, 8).unwrap();
            let equality_pairs = program
                .insns
                .iter()
                .filter_map(|(instruction, _)| match instruction {
                    Insn::Eq { lhs, rhs, .. } => Some((*lhs, *rhs)),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(equality_pairs, vec![(1, 3), (2, 4)]);
            let ordering_pairs = program
                .insns
                .iter()
                .filter_map(|(instruction, _)| match instruction {
                    Insn::Lt { lhs, rhs, .. } | Insn::Gt { lhs, rhs, .. } => Some((*lhs, *rhs)),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(ordering_pairs, vec![(1, 3), (2, 4)]);
            assert!(program.insns.iter().any(|(instruction, _)| matches!(
                instruction,
                Insn::Integer { value, dest: 8 } if *value == i64::from(inclusive)
            )));
        }
    }

    #[test]
    fn scalar_comparison_lowering_keeps_existing_opcode_and_null_shape() {
        let cases = [
            (Operator::Equals, ExpectedComparisonInsn::Eq, false),
            (Operator::NotEquals, ExpectedComparisonInsn::Ne, false),
            (Operator::Less, ExpectedComparisonInsn::Lt, false),
            (Operator::LessEquals, ExpectedComparisonInsn::Le, false),
            (Operator::Greater, ExpectedComparisonInsn::Gt, false),
            (Operator::GreaterEquals, ExpectedComparisonInsn::Ge, false),
            (Operator::Is, ExpectedComparisonInsn::Eq, true),
            (Operator::IsNot, ExpectedComparisonInsn::Ne, true),
        ];

        for (operator, expected, null_equal) in cases {
            let expression = comparison_expression(
                operator,
                hir::ComparisonComponent {
                    affinity: crate::vdbe::affinity::Affinity::Numeric,
                    collation: None,
                    array: false,
                },
            );
            let mut program = program();

            assert_eq!(translate_expr(&mut program, &expression, 8).unwrap(), 8);
            assert!(matches!(
                &program.insns[..3],
                [
                    (Insn::Integer { value: 2, dest: 1 }, _),
                    (Insn::Integer { value: 3, dest: 2 }, _),
                    (Insn::Integer { value: 1, dest: 8 }, _),
                ]
            ));
            let flags = match (expected, &program.insns[3].0) {
                (
                    ExpectedComparisonInsn::Eq,
                    Insn::Eq {
                        lhs: 1,
                        rhs: 2,
                        flags,
                        collation: None,
                        ..
                    },
                )
                | (
                    ExpectedComparisonInsn::Ne,
                    Insn::Ne {
                        lhs: 1,
                        rhs: 2,
                        flags,
                        collation: None,
                        ..
                    },
                )
                | (
                    ExpectedComparisonInsn::Lt,
                    Insn::Lt {
                        lhs: 1,
                        rhs: 2,
                        flags,
                        collation: None,
                        ..
                    },
                )
                | (
                    ExpectedComparisonInsn::Le,
                    Insn::Le {
                        lhs: 1,
                        rhs: 2,
                        flags,
                        collation: None,
                        ..
                    },
                )
                | (
                    ExpectedComparisonInsn::Gt,
                    Insn::Gt {
                        lhs: 1,
                        rhs: 2,
                        flags,
                        collation: None,
                        ..
                    },
                )
                | (
                    ExpectedComparisonInsn::Ge,
                    Insn::Ge {
                        lhs: 1,
                        rhs: 2,
                        flags,
                        collation: None,
                        ..
                    },
                ) => flags,
                _ => panic!("comparison uses expected instruction"),
            };
            assert_eq!(
                flags.get_affinity(),
                crate::vdbe::affinity::Affinity::Numeric
            );
            assert_eq!(flags.has_nulleq(), null_equal);
            assert!(!flags.has_array_cmp());
            if null_equal {
                assert!(matches!(
                    program.insns[4].0,
                    Insn::Integer { value: 0, dest: 8 }
                ));
            } else {
                assert!(matches!(
                    program.insns[4].0,
                    Insn::ZeroOrNull {
                        rg1: 1,
                        rg2: 2,
                        dest: 8
                    }
                ));
            }
        }
    }

    #[test]
    fn scalar_comparison_uses_resolved_array_and_collation_metadata() {
        let collation = hir::CatalogObject::new(
            hir::CatalogObjectId::new(1),
            hir::CatalogSnapshot::from_id(1),
            None,
            crate::sync::Arc::new(crate::translate::collate::CollationSeq::NoCase),
        );
        let expression = comparison_expression(
            Operator::Equals,
            hir::ComparisonComponent {
                affinity: crate::vdbe::affinity::Affinity::Text,
                collation: Some(collation),
                array: true,
            },
        );
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        let Insn::Eq {
            flags, collation, ..
        } = &program.insns[3].0
        else {
            panic!("equals comparison emits Eq")
        };
        assert_eq!(flags.get_affinity(), crate::vdbe::affinity::Affinity::Text);
        assert!(flags.has_array_cmp());
        assert_eq!(
            *collation,
            Some(crate::translate::collate::CollationSeq::NoCase)
        );
    }

    #[test]
    fn array_binary_lowering_keeps_existing_function_and_register_shape() {
        for operator in [Operator::ArrayContains, Operator::ArrayOverlap] {
            for shared in [false, true] {
                let rhs = if shared { "2" } else { "3" };
                let expression = hir::Expr::Binary {
                    lhs: Box::new(hir::Expr::Literal(Literal::Numeric("2".to_string()))),
                    operator,
                    rhs: Box::new(hir::Expr::Literal(Literal::Numeric(rhs.to_string()))),
                    array_concat: false,
                    custom: None,
                    comparison: None,
                };
                let mut program = program();

                assert_eq!(translate_expr(&mut program, &expression, 8).unwrap(), 8);
                let function_index = if shared {
                    assert!(matches!(
                        &program.insns[..3],
                        [
                            (Insn::Integer { value: 2, dest: 1 }, _),
                            (
                                Insn::Copy {
                                    src_reg: 1,
                                    dst_reg: 2,
                                    extra_amount: 0
                                },
                                _
                            ),
                            (
                                Insn::Copy {
                                    src_reg: 1,
                                    dst_reg: 3,
                                    extra_amount: 0
                                },
                                _
                            ),
                        ]
                    ));
                    3
                } else {
                    assert!(matches!(
                        &program.insns[..2],
                        [
                            (Insn::Integer { value: 2, dest: 1 }, _),
                            (Insn::Integer { value: 3, dest: 2 }, _),
                        ]
                    ));
                    2
                };
                let expected_start = if shared { 2 } else { 1 };
                let Insn::Function {
                    constant_mask: 0,
                    start_reg,
                    dest: 8,
                    func:
                        FuncCtx {
                            func: Func::Scalar(function),
                            arg_count: 2,
                        },
                } = &program.insns[function_index].0
                else {
                    panic!("array operator emits scalar function")
                };
                assert_eq!(*start_reg, expected_start);
                assert!(matches!(
                    (operator, function),
                    (Operator::ArrayContains, ScalarFunc::ArrayContainsAll)
                        | (Operator::ArrayOverlap, ScalarFunc::ArrayOverlap)
                ));
            }
        }
    }

    #[test]
    fn concat_lowering_uses_resolved_array_semantics() {
        for array_concat in [false, true] {
            let expression = hir::Expr::Binary {
                lhs: Box::new(hir::Expr::Literal(Literal::String("'left'".to_string()))),
                operator: Operator::Concat,
                rhs: Box::new(hir::Expr::Literal(Literal::String("'right'".to_string()))),
                array_concat,
                custom: None,
                comparison: None,
            };
            let mut program = program();

            assert_eq!(translate_expr(&mut program, &expression, 8).unwrap(), 8);
            assert!(matches!(
                &program.insns[..2],
                [
                    (Insn::String8 { value, dest: 1 }, _),
                    (Insn::String8 { dest: 2, .. }, _),
                ] if value == "left"
            ));
            if array_concat {
                assert!(matches!(
                    program.insns[2].0,
                    Insn::ArrayConcat {
                        lhs: 1,
                        rhs: 2,
                        dest: 8
                    }
                ));
            } else {
                assert!(matches!(
                    program.insns[2].0,
                    Insn::Concat {
                        lhs: 1,
                        rhs: 2,
                        dest: 8
                    }
                ));
            }
        }
    }

    #[test]
    fn concat_can_share_equivalent_operand_registers() {
        let expression = hir::Expr::Binary {
            lhs: Box::new(hir::Expr::Literal(Literal::String("'value'".to_string()))),
            operator: Operator::Concat,
            rhs: Box::new(hir::Expr::Literal(Literal::String("'value'".to_string()))),
            array_concat: true,
            custom: None,
            comparison: None,
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert!(matches!(
            program.insns.as_slice(),
            [
                (Insn::String8 { value, dest: 1 }, _),
                (
                    Insn::ArrayConcat {
                        lhs: 1,
                        rhs: 1,
                        dest: 8
                    },
                    _
                ),
            ] if value == "value"
        ));
    }

    #[test]
    fn equivalent_binary_operands_share_one_register() {
        let expression = hir::Expr::Binary {
            lhs: Box::new(hir::Expr::Literal(Literal::Numeric("2".to_string()))),
            operator: Operator::Add,
            rhs: Box::new(hir::Expr::Literal(Literal::Numeric("2".to_string()))),
            array_concat: false,
            custom: None,
            comparison: None,
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert!(matches!(
            program.insns.as_slice(),
            [
                (Insn::Integer { value: 2, dest: 1 }, _),
                (
                    Insn::Add {
                        lhs: 1,
                        rhs: 1,
                        dest: 8
                    },
                    _
                ),
            ]
        ));
    }

    #[test]
    fn nested_binary_operands_are_allocated_before_lowering_children() {
        let expression = hir::Expr::Binary {
            lhs: Box::new(hir::Expr::Binary {
                lhs: Box::new(hir::Expr::Literal(Literal::Numeric("2".to_string()))),
                operator: Operator::Add,
                rhs: Box::new(hir::Expr::Literal(Literal::Numeric("3".to_string()))),
                array_concat: false,
                custom: None,
                comparison: None,
            }),
            operator: Operator::Multiply,
            rhs: Box::new(hir::Expr::Literal(Literal::Numeric("4".to_string()))),
            array_concat: false,
            custom: None,
            comparison: None,
        };
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert!(matches!(
            program.insns.as_slice(),
            [
                (Insn::Integer { value: 2, dest: 3 }, _),
                (Insn::Integer { value: 3, dest: 4 }, _),
                (
                    Insn::Add {
                        lhs: 3,
                        rhs: 4,
                        dest: 1
                    },
                    _
                ),
                (Insn::Integer { value: 4, dest: 2 }, _),
                (
                    Insn::Multiply {
                        lhs: 1,
                        rhs: 2,
                        dest: 8
                    },
                    _
                ),
            ]
        ));
    }

    #[test]
    fn binary_lowering_does_not_use_the_call_stack() {
        let mut expression = hir::Expr::Literal(Literal::Numeric("1".to_string()));
        for value in 2..=10_000 {
            expression = hir::Expr::Binary {
                lhs: Box::new(expression),
                operator: Operator::Add,
                rhs: Box::new(hir::Expr::Literal(Literal::Numeric(value.to_string()))),
                array_concat: false,
                custom: None,
                comparison: None,
            };
        }

        let mut program = program();
        translate_expr(&mut program, &expression, 4).unwrap();
        assert_eq!(program.insns.len(), 19_999);

        std::mem::forget(expression);
    }

    #[test]
    fn dialect_scalar_function_uses_consecutive_argument_registers() {
        let expression = ordinary_scalar_call(
            Func::Dialect("catalog_value".to_string()),
            vec![
                hir::Expr::Literal(Literal::Numeric("1".to_string())),
                hir::Expr::Binary {
                    lhs: Box::new(hir::Expr::Literal(Literal::Numeric("2".to_string()))),
                    operator: Operator::Add,
                    rhs: Box::new(hir::Expr::Literal(Literal::Numeric("3".to_string()))),
                    array_concat: false,
                    custom: None,
                    comparison: None,
                },
            ],
        );
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert!(matches!(
            program.insns.as_slice(),
            [
                (Insn::Integer { value: 1, dest: 1 }, _),
                (Insn::Integer { value: 2, dest: 3 }, _),
                (Insn::Integer { value: 3, dest: 4 }, _),
                (
                    Insn::Add {
                        lhs: 3,
                        rhs: 4,
                        dest: 2,
                    },
                    _,
                ),
                (
                    Insn::Function {
                        constant_mask: 0,
                        start_reg: 1,
                        dest: 8,
                        func: FuncCtx {
                            func: Func::Dialect(name),
                            arg_count: 2,
                        },
                    },
                    _,
                ),
            ] if name == "catalog_value"
        ));
    }

    #[test]
    fn zero_argument_dialect_function_keeps_legacy_start_register() {
        let expression =
            ordinary_scalar_call(Func::Dialect("catalog_value".to_string()), Vec::new());
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert!(matches!(
            program.insns.as_slice(),
            [(
                Insn::Function {
                    start_reg: 8,
                    dest: 8,
                    func: FuncCtx { arg_count: 0, .. },
                    ..
                },
                _,
            )]
        ));
    }

    #[test]
    fn math_functions_keep_legacy_argument_registers() {
        use crate::function::MathFunc;

        let cases = [
            (MathFunc::Pi, Vec::new(), 0, 0),
            (
                MathFunc::Sqrt,
                vec![hir::Expr::Literal(Literal::Numeric("4".to_string()))],
                1,
                1,
            ),
            (
                MathFunc::Pow,
                vec![
                    hir::Expr::Literal(Literal::Numeric("2".to_string())),
                    hir::Expr::Literal(Literal::Numeric("3".to_string())),
                ],
                1,
                2,
            ),
            (
                MathFunc::Log,
                vec![hir::Expr::Literal(Literal::Numeric("8".to_string()))],
                1,
                1,
            ),
            (
                MathFunc::Log,
                vec![
                    hir::Expr::Literal(Literal::Numeric("2".to_string())),
                    hir::Expr::Literal(Literal::Numeric("8".to_string())),
                ],
                1,
                2,
            ),
        ];

        for (function, arguments, expected_start, expected_count) in cases {
            let mut program = program();
            let expression = ordinary_scalar_call(Func::Math(function), arguments);

            translate_expr(&mut program, &expression, 8).unwrap();
            assert!(matches!(
                program.insns.last().map(|(instruction, _)| instruction),
                Some(Insn::Function {
                    constant_mask: 0,
                    start_reg,
                    dest: 8,
                    func: FuncCtx {
                        func: Func::Math(_),
                        arg_count,
                    },
                }) if *start_reg == expected_start && *arg_count == expected_count
            ));
        }
    }

    #[cfg(all(feature = "fts", not(target_family = "wasm")))]
    #[test]
    fn direct_fts_functions_use_consecutive_argument_registers() {
        use crate::function::FtsFunc;

        for (function, argument_count) in [
            (FtsFunc::Score, 2),
            (FtsFunc::Match, 3),
            (FtsFunc::Highlight, 4),
        ] {
            let arguments = (0..argument_count)
                .map(|value| hir::Expr::Literal(Literal::Numeric(value.to_string())))
                .collect();
            let expression = ordinary_scalar_call(Func::Fts(function.clone()), arguments);
            let mut program = program();

            translate_expr(&mut program, &expression, 8).unwrap();
            assert!(program.insns[..argument_count].iter().enumerate().all(
                |(index, (instruction, _))| {
                    matches!(instruction, Insn::Integer { dest, .. } if *dest == index + 1)
                }
            ));
            assert!(matches!(
                program.insns.last().map(|(instruction, _)| instruction),
                Some(Insn::Function {
                    constant_mask: 0,
                    start_reg: 1,
                    dest: 8,
                    func: FuncCtx {
                        func: Func::Fts(emitted),
                        arg_count,
                    },
                }) if *emitted == function && *arg_count == argument_count
            ));
        }
    }

    #[test]
    fn vector_functions_use_consecutive_argument_registers() {
        use crate::function::VectorFunc;

        let cases = [
            (VectorFunc::Vector, 1),
            (VectorFunc::VectorDistanceL2, 2),
            (VectorFunc::VectorSlice, 3),
        ];

        for (function, argument_count) in cases {
            let arguments = (0..argument_count)
                .map(|value| hir::Expr::Literal(Literal::Numeric(value.to_string())))
                .collect();
            let expression = ordinary_scalar_call(Func::Vector(function), arguments);
            let mut program = program();

            translate_expr(&mut program, &expression, 8).unwrap();
            assert!(program.insns[..argument_count].iter().enumerate().all(
                |(index, (instruction, _))| {
                    matches!(instruction, Insn::Integer { dest, .. } if *dest == index + 1)
                }
            ));
            assert!(matches!(
                program.insns.last().map(|(instruction, _)| instruction),
                Some(Insn::Function {
                    constant_mask: 0,
                    start_reg: 1,
                    dest: 8,
                    func: FuncCtx {
                        func: Func::Vector(_),
                        arg_count,
                    },
                }) if *arg_count == argument_count
            ));
        }
    }

    #[test]
    fn plain_builtin_functions_use_legacy_argument_registers() {
        let mut empty_program = program();
        let expression = ordinary_scalar_call(Func::Scalar(ScalarFunc::Char), Vec::new());

        translate_expr(&mut empty_program, &expression, 8).unwrap();
        assert!(matches!(
            empty_program.insns.as_slice(),
            [(
                Insn::Function {
                    start_reg: 1,
                    dest: 8,
                    func: FuncCtx { arg_count: 0, .. },
                    ..
                },
                _,
            )]
        ));
        assert_eq!(empty_program.alloc_register(), 1);

        for (function, argument_count) in [
            (ScalarFunc::Char, 2),
            (ScalarFunc::GetByte, 2),
            (ScalarFunc::ArraySlice, 3),
            (ScalarFunc::Abs, 1),
            (ScalarFunc::SequenceWatermark, 1),
            (ScalarFunc::TimeDiff, 2),
            (ScalarFunc::Hex, 1),
            (ScalarFunc::Nullif, 2),
            (ScalarFunc::Instr, 2),
            (ScalarFunc::Replace, 3),
            (ScalarFunc::Trim, 1),
            (ScalarFunc::LTrim, 2),
            (ScalarFunc::RTrim, 1),
            (ScalarFunc::Round, 2),
            (ScalarFunc::Unhex, 2),
            (ScalarFunc::Min, 2),
            (ScalarFunc::Max, 3),
            (ScalarFunc::Concat, 1),
            (ScalarFunc::Concat, 3),
            (ScalarFunc::DateTime, 2),
            (ScalarFunc::StrfTime, 2),
            (ScalarFunc::StringReverse, 1),
            (ScalarFunc::Gcd, 2),
            (ScalarFunc::NumericEncode, 3),
            (ScalarFunc::TableColumnsJsonArray, 1),
            (ScalarFunc::BinRecordJsonObject, 2),
        ] {
            let arguments = (0..argument_count)
                .map(|value| hir::Expr::Literal(Literal::Numeric(value.to_string())))
                .collect();
            let expression = ordinary_scalar_call(Func::Scalar(function.clone()), arguments);
            let mut program = program();

            translate_expr(&mut program, &expression, 8).unwrap();
            assert!(program.insns[..argument_count].iter().enumerate().all(
                |(index, (instruction, _))| {
                    matches!(instruction, Insn::Integer { dest, .. } if *dest == index + 1)
                }
            ));
            assert!(matches!(
                program.insns.last().map(|(instruction, _)| instruction),
                Some(Insn::Function {
                    constant_mask: 0,
                    start_reg: 1,
                    dest: 8,
                    func: FuncCtx {
                        func: Func::Scalar(emitted),
                        arg_count,
                    },
                }) if *emitted == function && *arg_count == argument_count
            ));
        }
    }

    #[test]
    fn direct_like_functions_use_plain_legacy_argument_registers() {
        for function in [ScalarFunc::Like, ScalarFunc::Glob] {
            for argument_count in [2, 3] {
                let arguments = (0..argument_count)
                    .map(|value| hir::Expr::Literal(Literal::Numeric(value.to_string())))
                    .collect();
                let expression = ordinary_scalar_call(Func::Scalar(function.clone()), arguments);
                let mut program = program();

                translate_expr(&mut program, &expression, 8).unwrap();
                assert!(program.insns[..argument_count].iter().enumerate().all(
                    |(index, (instruction, _))| {
                        matches!(instruction, Insn::Integer { dest, .. } if *dest == index + 1)
                    }
                ));
                assert!(matches!(
                    program.insns.last().map(|(instruction, _)| instruction),
                    Some(Insn::Function {
                        constant_mask: 0,
                        start_reg: 1,
                        dest: 8,
                        func: FuncCtx {
                            func: Func::Scalar(emitted),
                            arg_count,
                        },
                    }) if *emitted == function && *arg_count == argument_count
                ));
            }
        }
    }

    #[test]
    fn planner_hints_only_lower_the_value_expression() {
        for (function, arguments) in [
            (
                ScalarFunc::Likely,
                vec![hir::Expr::Literal(Literal::Numeric("7".to_string()))],
            ),
            (
                ScalarFunc::Unlikely,
                vec![hir::Expr::Literal(Literal::Numeric("7".to_string()))],
            ),
            (
                ScalarFunc::Likelihood,
                vec![
                    hir::Expr::Literal(Literal::Numeric("7".to_string())),
                    hir::Expr::Literal(Literal::Numeric("0.5".to_string())),
                ],
            ),
        ] {
            let expression = ordinary_scalar_call(Func::Scalar(function), arguments);
            let mut program = program();

            translate_expr(&mut program, &expression, 8).unwrap();
            assert!(matches!(
                program.insns.as_slice(),
                [(Insn::Integer { value: 7, dest: 8 }, _)]
            ));
            assert_eq!(program.alloc_register(), 1);
        }
    }

    #[test]
    fn concat_ws_keeps_legacy_result_and_argument_registers() {
        for argument_count in [2, 3] {
            let arguments = (0..argument_count)
                .map(|value| hir::Expr::Literal(Literal::Numeric(value.to_string())))
                .collect();
            let expression = ordinary_scalar_call(Func::Scalar(ScalarFunc::ConcatWs), arguments);
            let mut program = program();

            translate_expr(&mut program, &expression, 8).unwrap();
            assert!(program.insns[..argument_count].iter().enumerate().all(
                |(index, (instruction, _))| {
                    matches!(instruction, Insn::Integer { dest, .. } if *dest == index + 2)
                }
            ));
            assert!(matches!(
                program
                    .insns
                    .get(argument_count)
                    .map(|(instruction, _)| instruction),
                Some(Insn::Function {
                    constant_mask: 0,
                    start_reg: 2,
                    dest: 1,
                    func: FuncCtx {
                        func: Func::Scalar(ScalarFunc::ConcatWs),
                        arg_count,
                    },
                }) if *arg_count == argument_count
            ));
            assert!(matches!(
                program.insns.last().map(|(instruction, _)| instruction),
                Some(Insn::Copy {
                    src_reg: 1,
                    dst_reg: 8,
                    extra_amount: 0,
                })
            ));
        }
    }

    #[test]
    fn ifnull_keeps_legacy_short_circuit_register_and_opcode_shape() {
        let expression = ordinary_scalar_call(
            Func::Scalar(ScalarFunc::IfNull),
            vec![
                hir::Expr::Literal(Literal::Numeric("1".to_string())),
                hir::Expr::Literal(Literal::Numeric("2".to_string())),
            ],
        );
        let mut program = program();

        translate_expr(&mut program, &expression, 8).unwrap();
        assert!(matches!(
            program.insns.as_slice(),
            [
                (Insn::Integer { dest: 1, .. }, _),
                (Insn::NotNull { reg: 1, .. }, _),
                (Insn::Integer { dest: 1, .. }, _),
                (
                    Insn::Copy {
                        src_reg: 1,
                        dst_reg: 8,
                        extra_amount: 0,
                    },
                    _
                ),
            ]
        ));
        assert_eq!(program.alloc_register(), 2);
    }

    #[test]
    fn iif_keeps_legacy_branch_register_and_opcode_shapes() {
        for (argument_count, expected_shape) in [
            (2, "condition if-not value goto null"),
            (
                4,
                "condition if-not value goto condition if-not value goto null",
            ),
            (
                5,
                "condition if-not value goto condition if-not value goto value",
            ),
        ] {
            let arguments = (0..argument_count)
                .map(|value| hir::Expr::Literal(Literal::Numeric(value.to_string())))
                .collect();
            let expression = ordinary_scalar_call(Func::Scalar(ScalarFunc::Iif), arguments);
            let mut program = program();

            translate_expr(&mut program, &expression, 8).unwrap();
            let shape = program
                .insns
                .iter()
                .map(|(instruction, _)| match instruction {
                    Insn::Integer { dest: 1, .. } => "condition",
                    Insn::Integer { dest: 8, .. } => "value",
                    Insn::IfNot {
                        reg: 1,
                        jump_if_null: true,
                        ..
                    } => "if-not",
                    Insn::Goto { .. } => "goto",
                    Insn::Null {
                        dest: 8,
                        dest_end: None,
                    } => "null",
                    instruction => panic!("unexpected IIF instruction: {instruction:?}"),
                })
                .collect::<Vec<_>>()
                .join(" ");
            assert_eq!(shape, expected_shape);
            assert_eq!(program.alloc_register(), 2);
        }
    }

    #[test]
    fn coalesce_keeps_legacy_target_register_and_opcode_shape() {
        for argument_count in [2, 3] {
            let arguments = (0..argument_count)
                .map(|value| hir::Expr::Literal(Literal::Numeric(value.to_string())))
                .collect();
            let expression = ordinary_scalar_call(Func::Scalar(ScalarFunc::Coalesce), arguments);
            let mut program = program();

            translate_expr(&mut program, &expression, 8).unwrap();
            assert_eq!(program.insns.len(), argument_count * 2 - 1);
            for (index, (instruction, _)) in program.insns.iter().enumerate() {
                if index % 2 == 0 {
                    assert!(matches!(instruction, Insn::Integer { dest: 8, .. }));
                } else {
                    assert!(matches!(instruction, Insn::NotNull { reg: 8, .. }));
                }
            }
            assert_eq!(program.alloc_register(), 1);
        }
    }

    #[test]
    fn last_insert_rowid_keeps_legacy_ignored_argument_behavior() {
        for argument_count in [0, 2] {
            let arguments = (0..argument_count)
                .map(|value| hir::Expr::Literal(Literal::Numeric(value.to_string())))
                .collect();
            let expression =
                ordinary_scalar_call(Func::Scalar(ScalarFunc::LastInsertRowid), arguments);
            let mut program = program();

            translate_expr(&mut program, &expression, 8).unwrap();
            assert!(matches!(
                program.insns.as_slice(),
                [(
                    Insn::Function {
                        constant_mask: 0,
                        start_reg: 1,
                        dest: 8,
                        func: FuncCtx {
                            func: Func::Scalar(ScalarFunc::LastInsertRowid),
                            arg_count,
                        },
                    },
                    _,
                )] if *arg_count == argument_count
            ));
            assert_eq!(program.alloc_register(), 2);
        }
    }

    #[test]
    fn version_functions_keep_legacy_output_register_and_copy_shape() {
        for function in [
            ScalarFunc::SqliteVersion,
            ScalarFunc::TursoVersion,
            ScalarFunc::SqliteSourceId,
        ] {
            let expression = ordinary_scalar_call(Func::Scalar(function.clone()), Vec::new());
            let mut program = program();

            translate_expr(&mut program, &expression, 8).unwrap();
            assert!(matches!(
                program.insns.as_slice(),
                [
                    (
                        Insn::Function {
                            constant_mask: 0,
                            start_reg: 1,
                            dest: 1,
                            func: FuncCtx {
                                func: Func::Scalar(emitted),
                                arg_count: 0,
                            },
                        },
                        _,
                    ),
                    (
                        Insn::Copy {
                            src_reg: 1,
                            dst_reg: 8,
                            extra_amount: 0,
                        },
                        _,
                    ),
                ] if *emitted == function
            ));
            assert_eq!(program.alloc_register(), 2);
        }
    }

    #[test]
    fn substring_functions_always_reserve_three_legacy_argument_registers() {
        for function in [ScalarFunc::Substr, ScalarFunc::Substring] {
            for argument_count in [2, 3] {
                let arguments = (0..argument_count)
                    .map(|value| hir::Expr::Literal(Literal::Numeric(value.to_string())))
                    .collect();
                let expression = ordinary_scalar_call(Func::Scalar(function.clone()), arguments);
                let mut program = program();

                translate_expr(&mut program, &expression, 8).unwrap();
                assert!(program.insns[..argument_count].iter().enumerate().all(
                    |(index, (instruction, _))| {
                        matches!(instruction, Insn::Integer { dest, .. } if *dest == index + 1)
                    }
                ));
                assert!(matches!(
                    program.insns.last().map(|(instruction, _)| instruction),
                    Some(Insn::Function {
                        constant_mask: 0,
                        start_reg: 1,
                        dest: 8,
                        func: FuncCtx {
                            func: Func::Scalar(emitted),
                            arg_count,
                        },
                    }) if *emitted == function && *arg_count == argument_count
                ));
                assert_eq!(program.alloc_register(), 4);
            }
        }
    }

    #[test]
    fn empty_plain_functions_reserve_the_legacy_start_register() {
        for function in [
            ScalarFunc::Changes,
            ScalarFunc::TotalChanges,
            ScalarFunc::Random,
            ScalarFunc::Date,
            ScalarFunc::DateTime,
            ScalarFunc::JulianDay,
            ScalarFunc::UnixEpoch,
            ScalarFunc::Time,
            ScalarFunc::StrfTime,
            #[cfg(feature = "test_helper")]
            ScalarFunc::TestNondetCounter,
        ] {
            let expression = ordinary_scalar_call(Func::Scalar(function), Vec::new());
            let mut program = program();

            translate_expr(&mut program, &expression, 8).unwrap();
            assert!(matches!(
                program.insns.as_slice(),
                [(
                    Insn::Function {
                        start_reg: 1,
                        dest: 8,
                        func: FuncCtx { arg_count: 0, .. },
                        ..
                    },
                    _,
                )]
            ));
            assert_eq!(program.alloc_register(), 2);
        }
    }

    #[cfg(feature = "json")]
    #[test]
    fn json_functions_preserve_empty_and_nonempty_register_shapes() {
        let mut empty_array_program = program();
        let expression = ordinary_scalar_call(Func::Json(JsonFunc::JsonArray), Vec::new());

        translate_expr(&mut empty_array_program, &expression, 8).unwrap();
        assert!(matches!(
            empty_array_program.insns.as_slice(),
            [(
                Insn::Function {
                    start_reg: 1,
                    dest: 8,
                    func: FuncCtx { arg_count: 0, .. },
                    ..
                },
                _,
            )]
        ));
        assert_eq!(empty_array_program.alloc_register(), 1);

        let mut empty_remove_program = program();
        let expression = ordinary_scalar_call(Func::Json(JsonFunc::JsonRemove), Vec::new());

        translate_expr(&mut empty_remove_program, &expression, 8).unwrap();
        assert!(matches!(
            empty_remove_program.insns.as_slice(),
            [(
                Insn::Function {
                    start_reg: 1,
                    dest: 8,
                    func: FuncCtx { arg_count: 0, .. },
                    ..
                },
                _,
            )]
        ));
        assert_eq!(empty_remove_program.alloc_register(), 2);

        let mut object_program = program();
        let expression = ordinary_scalar_call(
            Func::Json(JsonFunc::JsonObject),
            vec![
                hir::Expr::Literal(Literal::String("'key'".to_string())),
                hir::Expr::Literal(Literal::Numeric("1".to_string())),
            ],
        );

        translate_expr(&mut object_program, &expression, 8).unwrap();
        assert!(matches!(
            object_program.insns.as_slice(),
            [
                (Insn::String8 { dest: 1, .. }, _),
                (Insn::Integer { dest: 2, .. }, _),
                (
                    Insn::Function {
                        start_reg: 1,
                        dest: 8,
                        func: FuncCtx { arg_count: 2, .. },
                        ..
                    },
                    _,
                ),
            ]
        ));
    }

    #[cfg(feature = "json")]
    #[test]
    fn nested_json_functions_do_not_use_the_call_stack() {
        let mut expression = hir::Expr::Literal(Literal::String("'value'".to_string()));
        for _ in 0..10_000 {
            expression = ordinary_scalar_call(Func::Json(JsonFunc::JsonQuote), vec![expression]);
        }

        let mut program = program();
        translate_expr(&mut program, &expression, 8).unwrap();
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::Function { .. }))
                .count(),
            10_000
        );

        std::mem::forget(expression);
    }

    #[test]
    fn nested_dialect_functions_do_not_use_the_call_stack() {
        let mut expression = hir::Expr::Literal(Literal::Numeric("1".to_string()));
        for _ in 0..10_000 {
            expression =
                ordinary_scalar_call(Func::Dialect("catalog_value".to_string()), vec![expression]);
        }

        let mut program = program();
        translate_expr(&mut program, &expression, 8).unwrap();
        assert_eq!(
            program
                .insns
                .iter()
                .filter(|(instruction, _)| matches!(instruction, Insn::Function { .. }))
                .count(),
            10_000
        );

        std::mem::forget(expression);
    }

    #[test]
    fn expression_lowering_does_not_use_call_stack() {
        let mut expression = hir::Expr::Literal(Literal::Numeric("1".to_string()));
        for _ in 0..10_000 {
            expression = hir::Expr::Unary {
                operator: UnaryOperator::Positive,
                expr: Box::new(expression),
            };
        }

        let mut program = program();
        translate_expr(&mut program, &expression, 4).unwrap();
        assert!(matches!(
            program.insns.as_slice(),
            [(Insn::Integer { value: 1, dest: 4 }, _)]
        ));

        std::mem::forget(expression);
    }
}
