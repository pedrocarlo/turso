    fn resolve_alias(&self, name: &str) -> Option<ast::Expr> {
        let normalized = normalize_ident(name);
        let aliases = self.aliases();
        // Prefer explicit AS aliases over inferred column names.
        // Among explicit aliases, first match wins (SQLite behavior).
        // e.g. SELECT -a AS b, a, t.b ORDER BY b → resolves to -a (explicit AS b)
        // e.g. SELECT a, -b AS a ORDER BY a → resolves to -b (explicit AS a, wins over inferred a)
        if let Some(alias) = aliases
            .iter()
            .find(|a| a.is_explicit_alias && a.name.eq_ignore_ascii_case(&normalized))
        {
            return Some(alias.expr.clone());
        }
        // Fallback: inferred names (first match wins)
        aliases
            .iter()
            .find(|a| a.name.eq_ignore_ascii_case(&normalized))
            .map(|a| a.expr.clone())
    }

    fn resolve_outer_alias(&mut self, name: &str) -> Option<ast::Expr> {
        let normalized = normalize_ident(name);
        let resolved = self.outer_query_frames_iter().find_map(|frame| {
            frame
                .aliases
                .iter()
                .find(|alias| alias.name.eq_ignore_ascii_case(&normalized))
                .map(|alias| alias.expr.clone())
        })?;
        self.record_outer_refs_in_expr(&resolved);
        Some(resolved)
    }

    fn record_outer_refs_in_expr(&mut self, expr: &ast::Expr) {
        let _ = walk_expr(expr, &mut |expr| {
            if let ast::Expr::Column { table, column, .. } = expr {
                self.tracking.record_outer_ref(*table, *column);
            }
            Ok(WalkControl::Continue)
        });
    }

    fn resolve_unqualified_column(
        &mut self,
        name: &str,
        scope: &BindScope,
    ) -> Result<Option<ast::Expr>> {
        if let Some((table_id, col_idx, is_rowid_alias)) = scope.find_column_unqualified(name)? {
            self.tracking.record_column(table_id, col_idx);
            return Ok(Some(ast::Expr::Column {
                database: None,
                table: table_id,
                column: col_idx,
                is_rowid_alias,
            }));
        }

        for st in &scope.tables {
            if let Some(row_id_expr) =
                parse_row_id(name, st.internal_id, || scope.tables.len() != 1)?
            {
                self.tracking.record_rowid(st.internal_id);
                return Ok(Some(row_id_expr));
            }
        }

        let outer_match = {
            let mut result = None;
            for outer_scope in self.outer_scopes_iter() {
                if let Some(found) = outer_scope.find_column_unqualified(name)? {
                    result = Some(found);
                    break;
                }
            }
            result
        };
        if let Some((table_id, col_idx, is_rowid_alias)) = outer_match {
            self.tracking.record_outer_ref(table_id, col_idx);
            return Ok(Some(ast::Expr::Column {
                database: None,
                table: table_id,
                column: col_idx,
                is_rowid_alias,
            }));
        }

        Ok(None)
    }

    fn resolve_qualified_column(
        &mut self,
        table_name: &str,
        col_name: &str,
        scope: &BindScope,
    ) -> Result<Option<ast::Expr>> {
        // Try real columns first. A user-defined column named "oid", "rowid",
        // or "_rowid_" takes priority over the rowid pseudo-column.
        // find_column_qualified returns Err only when the table IS found but
        // the column is NOT — we intercept that case to try rowid fallback.
        match scope.find_column_qualified(table_name, col_name) {
            Ok(Some((table_id, col_idx, is_rowid_alias))) => {
                self.tracking.record_column(table_id, col_idx);
                return Ok(Some(ast::Expr::Column {
                    database: None,
                    table: table_id,
                    column: col_idx,
                    is_rowid_alias,
                }));
            }
            Ok(None) => {
                // Table not found — continue to outer scope / rowid checks
            }
            Err(err) => {
                // Ambiguity is definitive — never fall back to rowid.
                if err.to_string().contains("ambiguous column name") {
                    return Err(err);
                }
                // Table found but column not found — try rowid pseudo-column
                if let Some(st) = scope.find_table_by_identifier(table_name) {
                    if let Some(row_id_expr) = parse_row_id(col_name, st.internal_id, || false)? {
                        self.tracking.record_rowid(st.internal_id);
                        return Ok(Some(row_id_expr));
                    }
                }
                // Not a rowid either — re-raise the original error
                return Err(crate::LimboError::ParseError(format!(
                    "no such column: {table_name}.{col_name}"
                )));
            }
        }

        // Check outer scopes for columns and rowid
        let outer_match: Option<Result<ast::Expr>> = {
            let mut result = None;
            for outer_scope in self.outer_scopes_iter() {
                // Check real columns first, rowid as fallback
                match outer_scope.find_column_qualified(table_name, col_name) {
                    Ok(Some((table_id, col_idx, is_rowid_alias))) => {
                        result = Some(Ok(ast::Expr::Column {
                            database: None,
                            table: table_id,
                            column: col_idx,
                            is_rowid_alias,
                        }));
                        break;
                    }
                    Ok(None) => {
                        // Table not found in this scope, continue to next
                        continue;
                    }
                    Err(_) => {
                        // Table found but column not — try rowid pseudo-column
                    }
                }
                if let Some(st) = outer_scope.find_table_by_identifier(table_name) {
                    if let Some(row_id_expr) = parse_row_id(col_name, st.internal_id, || false)? {
                        result = Some(Ok(row_id_expr));
                        break;
                    }
                }
                // Column name not found in this scope as real column or rowid
                result = Some(Err(crate::LimboError::ParseError(format!(
                    "no such column: {table_name}.{col_name}"
                ))));
                break;
            }
            result
        };
        if let Some(outer_result) = outer_match {
            let resolved = outer_result?;
            match &resolved {
                ast::Expr::Column {
                    table: table_id,
                    column: col_idx,
                    ..
                } => {
                    self.tracking.record_outer_ref(*table_id, *col_idx);
                }
                ast::Expr::RowId {
                    table: table_id, ..
                } => {
                    self.tracking.record_rowid(*table_id);
                }
                _ => {}
            }
            return Ok(Some(resolved));
        }

        Ok(None)
    }

    /// Search scope tables for a column named `col_name` with a struct/union
    /// type. Errors on ambiguity (>1 match). Returns
    /// `(internal_id, col_idx, is_rowid_alias, type_def)` or `None`.
    /// Scope-based counterpart of `find_custom_type_column`.
    fn find_custom_type_column_in_scope<'t>(
        &'t self,
        scope: &BindScope,
        col_name: &str,
    ) -> Result<Option<(TableInternalId, usize, bool, &'t crate::schema::TypeDef)>> {
        let mut result = None;
        let mut match_count = 0usize;
        for st in &scope.tables {
            let ScopeTableSource::Table(table) = &st.source else {
                continue;
            };
            let cols = table.columns();
            if let Some(col_idx) = cols.iter().position(|c| {
                c.name
                    .as_ref()
                    .is_some_and(|n| n.eq_ignore_ascii_case(col_name))
            }) {
                let col = &cols[col_idx];
                let type_def = self.resolver.schema().get_type_def_unchecked(&col.ty_str);
                let is_struct_or_union = type_def
                    .map(|td| td.is_struct() || td.is_union())
                    .unwrap_or(false);
                if is_struct_or_union {
                    match_count += 1;
                    result = Some((
                        st.internal_id,
                        col_idx,
                        col.is_rowid_alias(),
                        &**type_def.unwrap(),
                    ));
                }
            }
        }
        if match_count > 1 {
            crate::bail_parse_error!(
                "ambiguous column reference: '{}' — multiple tables have a struct/union column with this name",
                col_name
            );
        }
        Ok(result)
    }

    fn field_access_resolution(
        type_def: &crate::schema::TypeDef,
        field_name: &str,
    ) -> Option<ast::FieldAccessResolution> {
        if let Some((field_index, _)) = type_def.find_struct_field(field_name) {
            Some(ast::FieldAccessResolution::StructField { field_index })
        } else if let Some((tag_index, _)) = type_def.find_union_variant(field_name) {
            Some(ast::FieldAccessResolution::UnionVariant { tag_index })
        } else {
            None
        }
    }

    fn make_field_access_expr(
        table_id: TableInternalId,
        col_idx: usize,
        is_rowid_alias: bool,
        field_name: &str,
        type_def: &crate::schema::TypeDef,
    ) -> Result<ast::Expr> {
        let Some(resolved) = Self::field_access_resolution(type_def, field_name) else {
            if type_def.is_struct() {
                crate::bail_parse_error!(
                    "no such field '{}' in struct type '{}'",
                    field_name,
                    type_def.name
                );
            }
            if type_def.is_union() {
                crate::bail_parse_error!(
                    "no such variant '{}' in union type '{}'",
                    field_name,
                    type_def.name
                );
            }
            crate::bail_parse_error!("type '{}' is not a struct or union type", type_def.name);
        };

        Ok(ast::Expr::FieldAccess {
            base: Box::new(ast::Expr::Column {
                database: None,
                table: table_id,
                column: col_idx,
                is_rowid_alias,
            }),
            field: ast::Name::from_bytes(field_name.as_bytes()),
            resolved: Some(resolved),
        })
    }

    /// Try to resolve `col.mid.leaf` as 2-level deep field access
    /// (e.g. `data.telegram.chat_id`). Scope-based counterpart of
    /// `try_resolve_nested_field_access`.
    fn try_resolve_nested_field_access_in_scope(
        &mut self,
        scope: &BindScope,
        col_name: &str,
        mid_name: &str,
        leaf_name: &str,
    ) -> Result<Option<ast::Expr>> {
        let Some((table_id, col_idx, is_rowid_alias, td)) =
            self.find_custom_type_column_in_scope(scope, col_name)?
        else {
            return Ok(None);
        };

        // Case A: UNION column — mid_name is a variant tag.
        // Case B: STRUCT column — mid_name is a struct field.
        let Some(mid_resolution) = Self::field_access_resolution(td, mid_name) else {
            return Ok(None);
        };
        let inner_type_name = td
            .find_union_variant(mid_name)
            .map(|(_, variant)| variant.type_name.as_str())
            .or_else(|| {
                td.find_struct_field(mid_name)
                    .map(|(_, field)| field.type_name.as_str())
            })
            .expect("resolved field access has a matching type definition entry");
        let Some(inner_type) = self
            .resolver
            .schema()
            .get_type_def_unchecked(inner_type_name)
        else {
            return Ok(None);
        };
        let Some(leaf_resolution) = Self::field_access_resolution(inner_type, leaf_name) else {
            return Ok(None);
        };

        let nested_expr = ast::Expr::FieldAccess {
            base: Box::new(ast::Expr::FieldAccess {
                base: Box::new(ast::Expr::Column {
                    database: None,
                    table: table_id,
                    column: col_idx,
                    is_rowid_alias,
                }),
                field: ast::Name::from_bytes(mid_name.as_bytes()),
                resolved: Some(mid_resolution),
            }),
            field: ast::Name::from_bytes(leaf_name.as_bytes()),
            resolved: Some(leaf_resolution),
        };
        self.tracking.record_column(table_id, col_idx);
        Ok(Some(nested_expr))
    }

    fn custom_type_expr_definition(
        &self,
        expr: &ast::Expr,
        scope: &BindScope,
    ) -> Option<Arc<crate::schema::TypeDef>> {
        let type_name = match expr {
            ast::Expr::Column { table, column, .. } => {
                let scope_table = scope
                    .tables
                    .iter()
                    .find(|scope_table| scope_table.internal_id == *table)
                    .or_else(|| {
                        self.all_outer_scopes_iter().find_map(|outer_scope| {
                            outer_scope
                                .tables
                                .iter()
                                .find(|scope_table| scope_table.internal_id == *table)
                        })
                    })?;
                let ScopeTableSource::Table(table) = &scope_table.source else {
                    return None;
                };
                table.columns().get(*column)?.ty_str.clone()
            }
            ast::Expr::Variable(variable) => variable.col_type.as_ref()?.to_string(),
            ast::Expr::FieldAccess { base, field, .. } => {
                let parent = self.custom_type_expr_definition(base, scope)?;
                parent
                    .find_struct_field(field.as_str())
                    .map(|(_, field)| field.type_name.clone())
                    .or_else(|| {
                        parent
                            .find_union_variant(field.as_str())
                            .map(|(_, variant)| variant.type_name.clone())
                    })?
            }
            ast::Expr::BoundCustomTypeFunction { resolution, .. } => match resolution {
                ast::CustomTypeFunctionResolution::UnionValue { result_type, .. }
                | ast::CustomTypeFunctionResolution::UnionExtract { result_type, .. }
                | ast::CustomTypeFunctionResolution::StructExtract { result_type, .. } => {
                    result_type.clone()
                }
                ast::CustomTypeFunctionResolution::UnionTag { .. } => return None,
            },
            ast::Expr::FunctionCall { name, args, .. } => {
                let function_name = normalize_ident(name.as_str());
                match function_name.as_str() {
                    "union_extract" if args.len() == 2 => {
                        let ast::Expr::Literal(ast::Literal::String(tag_name)) = args[1].as_ref()
                        else {
                            return None;
                        };
                        let union = self.custom_type_expr_definition(&args[0], scope)?;
                        union
                            .find_union_variant(tag_name.trim_matches('\''))?
                            .1
                            .type_name
                            .clone()
                    }
                    "struct_extract" if args.len() == 2 => {
                        let ast::Expr::Literal(ast::Literal::String(field_name)) = args[1].as_ref()
                        else {
                            return None;
                        };
                        let structure = self.custom_type_expr_definition(&args[0], scope)?;
                        structure
                            .find_struct_field(field_name.trim_matches('\''))?
                            .1
                            .type_name
                            .clone()
                    }
                    _ => return None,
                }
            }
            _ => return None,
        };
        self.resolver
            .schema()
            .get_type_def_unchecked(&type_name)
            .cloned()
    }

    fn bind_custom_type_function_calls(
        &self,
        expr: &mut ast::Expr,
        scope: &BindScope,
        expected_type: Option<&crate::schema::TypeDef>,
    ) -> Result<()> {
        walk_expr_mut(expr, &mut |expr| {
            let ast::Expr::FunctionCall {
                name,
                args,
                order_by,
                within_group,
                filter_over,
                ..
            } = expr
            else {
                return Ok(WalkControl::Continue);
            };
            let function_name = normalize_ident(name.as_str());
            if matches!(
                function_name.as_str(),
                "union_value" | "union_tag" | "union_extract" | "struct_extract"
            ) {
                if !order_by.is_empty() || !within_group.is_empty() {
                    crate::bail_parse_error!(
                        "ORDER BY is not allowed for scalar function {}()",
                        function_name
                    );
                }
                if filter_over.filter_clause.is_some() || filter_over.over_clause.is_some() {
                    crate::bail_parse_error!(
                        "{}() may not be used as an aggregate or window function",
                        function_name
                    );
                }
            }
            let (resolution, children_already_bound) = match function_name.as_str() {
                "union_value" => {
                    let ast::Expr::Literal(ast::Literal::String(tag_name)) = args[0].as_ref()
                    else {
                        unreachable!("union_value literal argument was validated earlier")
                    };
                    let tag_name = tag_name.trim_matches('\'');
                    let union = expected_type
                        .filter(|type_def| type_def.is_union())
                        .ok_or_else(|| {
                            crate::LimboError::ParseError(
                                "union_value() can only be used in INSERT/UPDATE targeting a union-typed column"
                                    .to_string(),
                            )
                        })?;
                    let (tag_index, variant) =
                        union.find_union_variant(tag_name).ok_or_else(|| {
                            crate::LimboError::ParseError(format!(
                                "unknown variant '{}' in union type '{}'",
                                tag_name, union.name
                            ))
                        })?;
                    let value_type = self
                        .resolver
                        .schema()
                        .get_type_def_unchecked(&variant.type_name);
                    self.bind_custom_type_function_calls(
                        &mut args[1],
                        scope,
                        value_type.map(AsRef::as_ref),
                    )?;
                    (
                        ast::CustomTypeFunctionResolution::UnionValue {
                            tag_index,
                            result_type: union.name.clone(),
                        },
                        true,
                    )
                }
                "union_tag" => {
                    let union = self
                        .custom_type_expr_definition(&args[0], scope)
                        .filter(|type_def| type_def.is_union())
                        .ok_or_else(|| {
                            crate::LimboError::ParseError(
                                "union_tag() argument must have a known union type".to_string(),
                            )
                        })?;
                    let tag_names = Arc::clone(
                        &union
                            .union_def()
                            .expect("union type must have a union definition")
                            .tag_names,
                    );
                    (
                        ast::CustomTypeFunctionResolution::UnionTag { tag_names },
                        false,
                    )
                }
                "union_extract" => {
                    let ast::Expr::Literal(ast::Literal::String(tag_name)) = args[1].as_ref()
                    else {
                        unreachable!("union_extract literal argument was validated earlier")
                    };
                    let tag_name = tag_name.trim_matches('\'');
                    let union = self
                        .custom_type_expr_definition(&args[0], scope)
                        .filter(|type_def| type_def.is_union())
                        .ok_or_else(|| {
                            crate::LimboError::ParseError(
                                "union_extract() first argument must have a known union type"
                                    .to_string(),
                            )
                        })?;
                    let (tag_index, variant) =
                        union.find_union_variant(tag_name).ok_or_else(|| {
                            crate::LimboError::ParseError(format!(
                                "unknown variant '{}' in union type '{}'",
                                tag_name, union.name
                            ))
                        })?;
                    (
                        ast::CustomTypeFunctionResolution::UnionExtract {
                            tag_index,
                            result_type: variant.type_name.clone(),
                        },
                        false,
                    )
                }
                "struct_extract" => {
                    let ast::Expr::Literal(ast::Literal::String(field_name)) = args[1].as_ref()
                    else {
                        unreachable!("struct_extract literal argument was validated earlier")
                    };
                    let field_name = field_name.trim_matches('\'');
                    let structure = self
                        .custom_type_expr_definition(&args[0], scope)
                        .filter(|type_def| type_def.is_struct())
                        .ok_or_else(|| {
                            crate::LimboError::ParseError(
                                "struct_extract() first argument must have a known struct type"
                                    .to_string(),
                            )
                        })?;
                    let (field_index, field) =
                        structure.find_struct_field(field_name).ok_or_else(|| {
                            crate::LimboError::ParseError(format!(
                                "unknown field '{}' in struct type '{}'",
                                field_name, structure.name
                            ))
                        })?;
                    (
                        ast::CustomTypeFunctionResolution::StructExtract {
                            field_index,
                            result_type: field.type_name.clone(),
                        },
                        false,
                    )
                }
                _ => return Ok(WalkControl::Continue),
            };

            let call = Box::new(take_expr(expr));
            *expr = ast::Expr::BoundCustomTypeFunction { call, resolution };
            Ok(if children_already_bound {
                WalkControl::SkipChildren
            } else {
                WalkControl::Continue
            })
        })?;
        Ok(())
    }

