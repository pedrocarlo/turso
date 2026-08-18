use std::ops::ControlFlow;

use crate::translate::{expr, semantic::hir};
use crate::util::parse_numeric_literal;
use crate::vdbe::{builder::ProgramBuilder, insn::Insn};
use crate::{LimboError, Numeric, Result, Value};
use turso_parser::ast::{Literal, Operator, UnaryOperator};

enum BinaryOperands {
    Unallocated,
    Shared(usize),
    Pair { lhs: usize, rhs: usize },
}

struct LoweringContext {
    target: usize,
    binary_operands: BinaryOperands,
}

impl LoweringContext {
    const fn new(target: usize) -> Self {
        Self {
            target,
            binary_operands: BinaryOperands::Unallocated,
        }
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
            hir::Expr::Binary { lhs, rhs, .. } => {
                if matches!(context.binary_operands, BinaryOperands::Unallocated) {
                    context.binary_operands = if lhs.equivalent(rhs) {
                        BinaryOperands::Shared(self.program.alloc_register())
                    } else {
                        let lhs = self.program.alloc_registers(2);
                        BinaryOperands::Pair { lhs, rhs: lhs + 1 }
                    };
                }
                match (&context.binary_operands, child_index) {
                    (BinaryOperands::Shared(register), 0) => *register,
                    (BinaryOperands::Shared(_), 1) => return Ok(ControlFlow::Break(())),
                    (BinaryOperands::Pair { lhs, .. }, 0) => *lhs,
                    (BinaryOperands::Pair { rhs, .. }, 1) => *rhs,
                    (BinaryOperands::Unallocated, _) => {
                        unreachable!("binary operand registers were allocated")
                    }
                    (_, _) => unreachable!("binary expression has two children"),
                }
            }
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
            hir::Expr::Binary {
                operator: Operator::Concat,
                array_concat,
                custom: None,
                comparison: None,
                ..
            } => self.emit_concat(context.binary_operands, target, children, *array_concat),
            hir::Expr::Binary {
                operator,
                array_concat: false,
                custom: None,
                comparison: None,
                ..
            } => self.emit_binary(*operator, context.binary_operands, target, children),
            _ => Err(LimboError::InternalError(
                "HIR expression lowering is not implemented for this expression".to_string(),
            )),
        }
    }
}

impl ExprLowerer<'_> {
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
            BinaryOperands::Unallocated => {
                unreachable!("binary expression allocated operand registers")
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
            BinaryOperands::Unallocated => {
                unreachable!("binary expression allocated operand registers")
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
