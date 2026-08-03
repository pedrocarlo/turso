pub(crate) fn bind_check_constraint(
    expr: &ast::Expr,
    table_name: &str,
    column_names: &[&str],
    resolver: &Resolver,
) -> Result<()> {
    let normalized_table = normalize_ident(table_name);
    walk_expr(expr, &mut |expr: &ast::Expr| -> Result<WalkControl> {
        match expr {
            ast::Expr::Id(name) | ast::Expr::Name(name) => {
                let normalized_name = normalize_ident(name.as_str());
                if !column_names
                    .iter()
                    .any(|column| normalize_ident(column) == normalized_name)
                    && !super::planner::ROWID_STRS
                        .iter()
                        .any(|rowid| rowid.eq_ignore_ascii_case(&normalized_name))
                {
                    crate::bail_parse_error!("no such column: {}", name.as_str());
                }
            }
            ast::Expr::Qualified(table, column) => {
                if normalize_ident(table.as_str()) != normalized_table {
                    crate::bail_parse_error!(
                        "no such column: {}.{}",
                        table.as_str(),
                        column.as_str()
                    );
                }
                let column_name = normalize_ident(column.as_str());
                if !column_names
                    .iter()
                    .any(|column| normalize_ident(column) == column_name)
                    && !super::planner::ROWID_STRS
                        .iter()
                        .any(|rowid| rowid.eq_ignore_ascii_case(&column_name))
                {
                    crate::bail_parse_error!("no such column: {}", column.as_str());
                }
            }
            ast::Expr::DoublyQualified(database, table, column) => {
                crate::bail_parse_error!(
                    "no such column: {}.{}.{}",
                    database.as_str(),
                    table.as_str(),
                    column.as_str()
                );
            }
            ast::Expr::FunctionCall {
                name,
                args,
                filter_over,
                ..
            } => {
                if filter_over.over_clause.is_some() {
                    crate::bail_parse_error!("misuse of window function {}()", name.as_str());
                }
                let Some(function) = resolver.resolve_function(name.as_str(), args.len())? else {
                    crate::bail_parse_error!("no such function: {}", name.as_str());
                };
                if matches!(function, Func::Agg(..)) {
                    crate::bail_parse_error!("misuse of aggregate function {}()", name.as_str());
                }
                if matches!(function, Func::Window(..)) {
                    crate::bail_parse_error!("misuse of window function {}()", name.as_str());
                }
            }
            ast::Expr::FunctionCallStar { name, filter_over } => {
                if filter_over.over_clause.is_some() {
                    crate::bail_parse_error!("misuse of window function {}()", name.as_str());
                }
                let Some(function) = resolver.resolve_function(name.as_str(), 0)? else {
                    crate::bail_parse_error!("no such function: {}", name.as_str());
                };
                if matches!(function, Func::Agg(..)) {
                    crate::bail_parse_error!("misuse of aggregate function {}()", name.as_str());
                }
                if matches!(function, Func::Window(..)) {
                    crate::bail_parse_error!("misuse of window function {}()", name.as_str());
                }
            }
            ast::Expr::Variable(_) => {
                crate::bail_parse_error!("parameters prohibited in CHECK constraints");
            }
            ast::Expr::Subquery(_) | ast::Expr::Exists(_) | ast::Expr::InSelect { .. } => {
                crate::bail_parse_error!("subqueries prohibited in CHECK constraints");
            }
            _ => {}
        }
        Ok(WalkControl::Continue)
    })?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
enum CheckExprType {
    Integer,
    Real,
    Text,
    Blob,
    Any,
    Null,
    CustomType(String),
}

impl CheckExprType {
    fn is_numeric(&self) -> bool {
        matches!(self, Self::Integer | Self::Real)
    }

    fn is_compatible_with(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, _) | (_, Self::Null) => true,
            (Self::Any, _) | (_, Self::Any) => true,
            (left, right) if left == right => true,
            (left, right) if left.is_numeric() && right.is_numeric() => true,
            _ => false,
        }
    }

    fn display_name(&self) -> &str {
        match self {
            Self::Integer => "INTEGER",
            Self::Real => "REAL",
            Self::Text => "TEXT",
            Self::Blob => "BLOB",
            Self::Any => "ANY",
            Self::Null => "NULL",
            Self::CustomType(name) => name.as_str(),
        }
    }
}

/// Bind raw CHECK references while validating STRICT-table comparison types.
pub(crate) fn bind_strict_check_constraint(
    expr: &ast::Expr,
    columns: &[&ast::ColumnDefinition],
    resolver: &Resolver,
) -> Result<()> {
    use ast::Operator;
    match expr {
        ast::Expr::Binary(lhs, op, rhs) => match op {
            Operator::Equals
            | Operator::NotEquals
            | Operator::Less
            | Operator::LessEquals
            | Operator::Greater
            | Operator::GreaterEquals => {
                let left_type = resolve_check_expr_type(lhs, columns, resolver)?;
                let right_type = resolve_check_expr_type(rhs, columns, resolver)?;
                if !left_type.is_compatible_with(&right_type) {
                    crate::bail_parse_error!(
                        "type mismatch in CHECK constraint: cannot compare {} with {}",
                        left_type.display_name(),
                        right_type.display_name()
                    );
                }
            }
            _ => {
                bind_strict_check_constraint(lhs, columns, resolver)?;
                bind_strict_check_constraint(rhs, columns, resolver)?;
            }
        },
        ast::Expr::Between {
            lhs, start, end, ..
        } => {
            let lhs_type = resolve_check_expr_type(lhs, columns, resolver)?;
            let start_type = resolve_check_expr_type(start, columns, resolver)?;
            let end_type = resolve_check_expr_type(end, columns, resolver)?;
            if !lhs_type.is_compatible_with(&start_type) {
                crate::bail_parse_error!(
                    "type mismatch in CHECK BETWEEN: cannot compare {} with {}",
                    lhs_type.display_name(),
                    start_type.display_name()
                );
            }
            if !lhs_type.is_compatible_with(&end_type) {
                crate::bail_parse_error!(
                    "type mismatch in CHECK BETWEEN: cannot compare {} with {}",
                    lhs_type.display_name(),
                    end_type.display_name()
                );
            }
        }
        ast::Expr::InList { lhs, rhs, .. } => {
            let lhs_type = resolve_check_expr_type(lhs, columns, resolver)?;
            for item in rhs {
                let item_type = resolve_check_expr_type(item, columns, resolver)?;
                if !lhs_type.is_compatible_with(&item_type) {
                    crate::bail_parse_error!(
                        "type mismatch in CHECK IN list: cannot compare {} with {}",
                        lhs_type.display_name(),
                        item_type.display_name()
                    );
                }
            }
        }
        ast::Expr::Parenthesized(exprs) => {
            for expr in exprs {
                bind_strict_check_constraint(expr, columns, resolver)?;
            }
        }
        ast::Expr::Unary(_, inner) => {
            bind_strict_check_constraint(inner, columns, resolver)?;
        }
        ast::Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            if let Some(base) = base {
                bind_strict_check_constraint(base, columns, resolver)?;
            }
            for (when_expr, then_expr) in when_then_pairs {
                bind_strict_check_constraint(when_expr, columns, resolver)?;
                bind_strict_check_constraint(then_expr, columns, resolver)?;
            }
            if let Some(else_expr) = else_expr {
                bind_strict_check_constraint(else_expr, columns, resolver)?;
            }
        }
        ast::Expr::FunctionCall { args, .. } => {
            for arg in args {
                bind_strict_check_constraint(arg, columns, resolver)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn resolve_check_expr_type(
    expr: &ast::Expr,
    columns: &[&ast::ColumnDefinition],
    resolver: &Resolver,
) -> Result<CheckExprType> {
    use ast::{Literal, Operator, UnaryOperator};
    match expr {
        ast::Expr::Id(name) | ast::Expr::Name(name) => {
            let name = normalize_ident(name.as_str());
            if super::planner::ROWID_STRS
                .iter()
                .any(|rowid| rowid.eq_ignore_ascii_case(&name))
            {
                return Ok(CheckExprType::Integer);
            }
            let Some(column) = columns
                .iter()
                .find(|column| normalize_ident(column.col_name.as_str()) == name)
            else {
                crate::bail_parse_error!("no such column: {}", name);
            };
            resolve_check_column_type(column, resolver)
        }
        ast::Expr::Qualified(_, column) => {
            let column_name = normalize_ident(column.as_str());
            if super::planner::ROWID_STRS
                .iter()
                .any(|rowid| rowid.eq_ignore_ascii_case(&column_name))
            {
                return Ok(CheckExprType::Integer);
            }
            let Some(column) = columns
                .iter()
                .find(|column| normalize_ident(column.col_name.as_str()) == column_name)
            else {
                crate::bail_parse_error!("no such column: {}", column_name);
            };
            resolve_check_column_type(column, resolver)
        }
        ast::Expr::Literal(literal) => match literal {
            Literal::Numeric(value) => {
                if value.contains('.') || value.contains('e') || value.contains('E') {
                    Ok(CheckExprType::Real)
                } else {
                    Ok(CheckExprType::Integer)
                }
            }
            Literal::String(_) => Ok(CheckExprType::Text),
            Literal::Blob(_) => Ok(CheckExprType::Blob),
            Literal::Null => Ok(CheckExprType::Null),
            Literal::True | Literal::False => Ok(CheckExprType::Integer),
            Literal::CurrentDate | Literal::CurrentTime | Literal::CurrentTimestamp => {
                Ok(CheckExprType::Text)
            }
            Literal::Keyword(value) => crate::bail_parse_error!(
                "cannot determine type of '{}' in CHECK constraint; use CAST",
                value
            ),
        },
        ast::Expr::Parenthesized(exprs) if exprs.len() == 1 => {
            resolve_check_expr_type(&exprs[0], columns, resolver)
        }
        ast::Expr::Parenthesized(_) => crate::bail_parse_error!(
            "cannot determine type of expression in CHECK constraint; use CAST"
        ),
        ast::Expr::Cast { type_name, .. } => {
            let Some(type_name) = type_name else {
                crate::bail_parse_error!(
                    "cannot determine type of CAST in CHECK constraint; use CAST with explicit type"
                );
            };
            resolve_check_type_name(&type_name.name, resolver)
        }
        ast::Expr::Unary(op, inner) => match op {
            UnaryOperator::Negative | UnaryOperator::Positive => {
                let inner_type = resolve_check_expr_type(inner, columns, resolver)?;
                if !inner_type.is_numeric() && inner_type != CheckExprType::Null {
                    crate::bail_parse_error!(
                        "unary minus/plus requires a numeric type, got {}",
                        inner_type.display_name()
                    );
                }
                Ok(inner_type)
            }
            UnaryOperator::BitwiseNot | UnaryOperator::Not => Ok(CheckExprType::Integer),
        },
        ast::Expr::Binary(lhs, op, rhs) => match op {
            Operator::Add | Operator::Subtract | Operator::Multiply | Operator::Divide => {
                let left_type = resolve_check_expr_type(lhs, columns, resolver)?;
                let right_type = resolve_check_expr_type(rhs, columns, resolver)?;
                if left_type == CheckExprType::Null || right_type == CheckExprType::Null {
                    return Ok(CheckExprType::Null);
                }
                if !left_type.is_numeric() || !right_type.is_numeric() {
                    crate::bail_parse_error!(
                        "arithmetic requires numeric types, got {} and {}",
                        left_type.display_name(),
                        right_type.display_name()
                    );
                }
                if left_type == CheckExprType::Real || right_type == CheckExprType::Real {
                    Ok(CheckExprType::Real)
                } else {
                    Ok(CheckExprType::Integer)
                }
            }
            Operator::Modulus
            | Operator::BitwiseAnd
            | Operator::BitwiseOr
            | Operator::LeftShift
            | Operator::RightShift => Ok(CheckExprType::Integer),
            Operator::Concat => Ok(CheckExprType::Text),
            Operator::And | Operator::Or => {
                bind_strict_check_constraint(lhs, columns, resolver)?;
                bind_strict_check_constraint(rhs, columns, resolver)?;
                Ok(CheckExprType::Integer)
            }
            Operator::Equals
            | Operator::NotEquals
            | Operator::Less
            | Operator::LessEquals
            | Operator::Greater
            | Operator::GreaterEquals => {
                let left_type = resolve_check_expr_type(lhs, columns, resolver)?;
                let right_type = resolve_check_expr_type(rhs, columns, resolver)?;
                if !left_type.is_compatible_with(&right_type) {
                    crate::bail_parse_error!(
                        "type mismatch in CHECK constraint: cannot compare {} with {}",
                        left_type.display_name(),
                        right_type.display_name()
                    );
                }
                Ok(CheckExprType::Integer)
            }
            Operator::Is | Operator::IsNot => Ok(CheckExprType::Integer),
            _ => crate::bail_parse_error!(
                "cannot determine type of expression in CHECK constraint; use CAST"
            ),
        },
        ast::Expr::NotNull(_) | ast::Expr::IsNull(_) => Ok(CheckExprType::Integer),
        ast::Expr::FunctionCall { name, args, .. } => {
            let Some(function) = resolver.resolve_function(name.as_str(), args.len())? else {
                crate::bail_parse_error!(
                    "cannot determine return type of function {}() in CHECK constraint; \
                     wrap with CAST to specify the type, e.g. CAST({}(...) AS INTEGER)",
                    name.as_str(),
                    name.as_str()
                );
            };
            resolve_check_function_type(&function, name.as_str(), args, columns, resolver)
        }
        ast::Expr::FunctionCallStar { name, .. } => {
            let Some(function) = resolver.resolve_function(name.as_str(), 0)? else {
                crate::bail_parse_error!(
                    "cannot determine return type of function {}() in CHECK constraint; \
                     wrap with CAST to specify the type, e.g. CAST({}(...) AS INTEGER)",
                    name.as_str(),
                    name.as_str()
                );
            };
            resolve_check_function_type(&function, name.as_str(), &[], columns, resolver)
        }
        _ => crate::bail_parse_error!(
            "cannot determine type of expression in CHECK constraint; use CAST"
        ),
    }
}

fn resolve_check_function_type(
    function: &Func,
    name: &str,
    args: &[Box<ast::Expr>],
    columns: &[&ast::ColumnDefinition],
    resolver: &Resolver,
) -> Result<CheckExprType> {
    match function {
        Func::Scalar(function) => {
            resolve_check_scalar_function_type(function, args, columns, resolver)
        }
        Func::Math(function) => Ok(resolve_check_math_function_type(function)),
        #[cfg(feature = "json")]
        Func::Json(function) => Ok(resolve_check_json_function_type(function)),
        Func::Agg(_) => crate::bail_parse_error!("misuse of aggregate function {}()", name),
        Func::External(_) => crate::bail_parse_error!(
            "cannot determine return type of function {}() in CHECK constraint; \
             wrap with CAST to specify the type, e.g. CAST({}(...) AS INTEGER)",
            name,
            name
        ),
        _ => Ok(CheckExprType::Any),
    }
}

fn resolve_check_scalar_function_type(
    function: &ScalarFunc,
    args: &[Box<ast::Expr>],
    columns: &[&ast::ColumnDefinition],
    resolver: &Resolver,
) -> Result<CheckExprType> {
    match function {
        ScalarFunc::Length
        | ScalarFunc::OctetLength
        | ScalarFunc::Instr
        | ScalarFunc::Unicode
        | ScalarFunc::Sign
        | ScalarFunc::Random
        | ScalarFunc::Changes
        | ScalarFunc::TotalChanges
        | ScalarFunc::LastInsertRowid
        | ScalarFunc::Glob
        | ScalarFunc::Like
        | ScalarFunc::Likely
        | ScalarFunc::Unlikely
        | ScalarFunc::Likelihood
        | ScalarFunc::BooleanToInt
        | ScalarFunc::IntToBoolean
        | ScalarFunc::IsAutocommit
        | ScalarFunc::ConnTxnId
        | ScalarFunc::TestUintLt
        | ScalarFunc::TestUintEq
        | ScalarFunc::NumericLt
        | ScalarFunc::NumericEq
        | ScalarFunc::ValidateIpAddr
        | ScalarFunc::GetByte
        | ScalarFunc::UnixEpoch => Ok(CheckExprType::Integer),
        ScalarFunc::Upper
        | ScalarFunc::Lower
        | ScalarFunc::Trim
        | ScalarFunc::LTrim
        | ScalarFunc::RTrim
        | ScalarFunc::Hex
        | ScalarFunc::Soundex
        | ScalarFunc::Quote
        | ScalarFunc::Replace
        | ScalarFunc::Substr
        | ScalarFunc::Substring
        | ScalarFunc::Char
        | ScalarFunc::Concat
        | ScalarFunc::ConcatWs
        | ScalarFunc::Typeof
        | ScalarFunc::SqliteVersion
        | ScalarFunc::TursoVersion
        | ScalarFunc::SqliteSourceId
        | ScalarFunc::Date
        | ScalarFunc::Time
        | ScalarFunc::DateTime
        | ScalarFunc::StrfTime
        | ScalarFunc::TimeDiff
        | ScalarFunc::Printf
        | ScalarFunc::StringReverse => Ok(CheckExprType::Text),
        ScalarFunc::Round | ScalarFunc::JulianDay => Ok(CheckExprType::Real),
        ScalarFunc::RandomBlob | ScalarFunc::ZeroBlob | ScalarFunc::Unhex | ScalarFunc::SetByte => {
            Ok(CheckExprType::Blob)
        }
        ScalarFunc::Abs | ScalarFunc::Nullif => {
            args.first().map_or(Ok(CheckExprType::Any), |arg| {
                resolve_check_expr_type(arg, columns, resolver)
            })
        }
        ScalarFunc::Coalesce | ScalarFunc::IfNull => {
            for arg in args {
                let arg_type = resolve_check_expr_type(arg, columns, resolver)?;
                if arg_type != CheckExprType::Null {
                    return Ok(arg_type);
                }
            }
            Ok(CheckExprType::Null)
        }
        ScalarFunc::Min | ScalarFunc::Max => args.first().map_or(Ok(CheckExprType::Any), |arg| {
            resolve_check_expr_type(arg, columns, resolver)
        }),
        ScalarFunc::Iif if args.len() >= 2 => resolve_check_expr_type(&args[1], columns, resolver),
        ScalarFunc::Iif => Ok(CheckExprType::Any),
        ScalarFunc::TestUintEncode
        | ScalarFunc::TestUintDecode
        | ScalarFunc::TestUintAdd
        | ScalarFunc::TestUintSub
        | ScalarFunc::TestUintMul
        | ScalarFunc::TestUintDiv
        | ScalarFunc::NumericEncode
        | ScalarFunc::NumericDecode
        | ScalarFunc::NumericAdd
        | ScalarFunc::NumericSub
        | ScalarFunc::NumericMul
        | ScalarFunc::NumericDiv => Ok(CheckExprType::Blob),
        _ => Ok(CheckExprType::Any),
    }
}

fn resolve_check_math_function_type(function: &MathFunc) -> CheckExprType {
    match function {
        MathFunc::Ceil | MathFunc::Ceiling | MathFunc::Floor | MathFunc::Trunc => {
            CheckExprType::Integer
        }
        _ => CheckExprType::Real,
    }
}

#[cfg(feature = "json")]
fn resolve_check_json_function_type(function: &crate::function::JsonFunc) -> CheckExprType {
    use crate::function::JsonFunc;
    match function {
        JsonFunc::Json
        | JsonFunc::JsonArray
        | JsonFunc::JsonObject
        | JsonFunc::JsonPatch
        | JsonFunc::JsonRemove
        | JsonFunc::JsonReplace
        | JsonFunc::JsonInsert
        | JsonFunc::JsonSet
        | JsonFunc::JsonPretty
        | JsonFunc::JsonQuote
        | JsonFunc::JsonType => CheckExprType::Text,
        JsonFunc::Jsonb
        | JsonFunc::JsonbArray
        | JsonFunc::JsonbObject
        | JsonFunc::JsonbPatch
        | JsonFunc::JsonbRemove
        | JsonFunc::JsonbReplace
        | JsonFunc::JsonbInsert
        | JsonFunc::JsonbSet => CheckExprType::Blob,
        JsonFunc::JsonArrayLength | JsonFunc::JsonErrorPosition | JsonFunc::JsonValid => {
            CheckExprType::Integer
        }
        JsonFunc::JsonExtract
        | JsonFunc::JsonbExtract
        | JsonFunc::JsonArrowExtract
        | JsonFunc::JsonArrowShiftExtract => CheckExprType::Any,
    }
}

fn resolve_check_column_type(
    column: &ast::ColumnDefinition,
    resolver: &Resolver,
) -> Result<CheckExprType> {
    match &column.col_type {
        Some(column_type) => resolve_check_type_name(&column_type.name, resolver),
        None => Ok(CheckExprType::Any),
    }
}

fn resolve_check_type_name(type_name: &str, resolver: &Resolver) -> Result<CheckExprType> {
    let resolved = turso_macros::match_ignore_ascii_case!(match type_name.as_bytes() {
        b"INT" | b"INTEGER" => Some(CheckExprType::Integer),
        b"REAL" | b"FLOAT" | b"DOUBLE" => Some(CheckExprType::Real),
        b"TEXT" => Some(CheckExprType::Text),
        b"BLOB" => Some(CheckExprType::Blob),
        b"ANY" => Some(CheckExprType::Any),
        _ => None,
    });
    if let Some(resolved) = resolved {
        return Ok(resolved);
    }
    if let Ok(Some(resolved)) = resolver.schema().resolve_type_unchecked(type_name) {
        if resolved.is_domain() {
            return resolve_check_type_name(&resolved.primitive, resolver);
        }
        return Ok(CheckExprType::CustomType(type_name.to_lowercase()));
    }
    crate::bail_parse_error!("unknown type '{}' in CHECK constraint", type_name);
}

