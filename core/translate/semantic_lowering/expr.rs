use std::ops::ControlFlow;

use crate::translate::{expr, semantic::hir};
use crate::util::parse_numeric_literal;
use crate::vdbe::{builder::ProgramBuilder, insn::Insn};
use crate::{LimboError, Numeric, Result, Value};
use turso_parser::ast::{Literal, UnaryOperator};

struct ExprLowerer<'a> {
    program: &'a mut ProgramBuilder,
}

impl hir::ExprVisitor for ExprLowerer<'_> {
    type Context = usize;
    type Output = usize;
    type Error = LimboError;

    fn pre_order(
        &mut self,
        parent: &hir::Expr,
        context: &usize,
        _child_index: usize,
        child: &hir::Expr,
    ) -> Result<ControlFlow<(), usize>> {
        let hir::Expr::Unary { operator, .. } = parent else {
            return Ok(ControlFlow::Break(()));
        };

        let context = match operator {
            UnaryOperator::Positive => *context,
            UnaryOperator::Negative if matches!(child, hir::Expr::Literal(Literal::Numeric(_))) => {
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
        };
        Ok(ControlFlow::Continue(context))
    }

    fn post_order(
        &mut self,
        expression: &hir::Expr,
        target: usize,
        children: &[usize],
    ) -> Result<usize> {
        match expression {
            hir::Expr::Literal(literal) => emit_literal(self.program, literal, target),
            hir::Expr::Parameter(parameter) => Ok(emit_parameter(self.program, parameter, target)),
            hir::Expr::Unary { operator, expr } => {
                self.emit_unary(*operator, expr, target, children)
            }
            _ => Err(LimboError::InternalError(
                "HIR expression lowering is not implemented for this expression".to_string(),
            )),
        }
    }
}

impl ExprLowerer<'_> {
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
    expression.walk(target, &mut ExprLowerer { program })
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
