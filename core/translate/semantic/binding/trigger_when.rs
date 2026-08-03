    // ── Trigger WHEN binding ────────────────────────────────────────────

    /// Bind NEW/OLD references and subqueries in a trigger WHEN clause.
    /// Top-level identifiers are either resolved here or rejected. DQS string
    /// fallback is also completed here so emission never binds raw SQL names.
    fn bind_trigger_when(
        &mut self,
        expr: &mut ast::Expr,
        table: Arc<BTreeTable>,
        new_registers: Option<&[usize]>,
        old_registers: Option<&[usize]>,
    ) -> Result<HashMap<ast::TableInternalId, BoundSubquery>> {
        let saved_trigger_columns = self.trigger_columns.take();
        self.trigger_columns = Some(TriggerColumnBindings {
            table,
            new_registers: new_registers.map(<[usize]>::to_vec),
            old_registers: old_registers.map(<[usize]>::to_vec),
        });

        let result = self.with_query(|ctx| {
            let scope = BindScope::empty();
            ctx.with_phase(BindPhase::NoAliases, |ctx| {
                walk_expr_mut(expr, &mut |e: &mut ast::Expr| -> Result<WalkControl> {
                    match e {
                        ast::Expr::Exists(_)
                        | ast::Expr::Subquery(_)
                        | ast::Expr::InSelect { .. } => {
                            ctx.bind_expr(e, &scope)?;
                            Ok(WalkControl::SkipChildren)
                        }
                        ast::Expr::Qualified(_, _) | ast::Expr::DoublyQualified(_, _, _) => {
                            if !ctx.bind_trigger_column(e)? {
                                let (namespace, column) = match e {
                                    ast::Expr::Qualified(namespace, column)
                                    | ast::Expr::DoublyQualified(_, namespace, column) => {
                                        (namespace.as_str(), column.as_str())
                                    }
                                    _ => unreachable!(),
                                };
                                crate::bail_parse_error!(
                                    "no such column: {}.{}",
                                    namespace,
                                    column
                                );
                            }
                            Ok(WalkControl::Continue)
                        }
                        ast::Expr::Id(name) => {
                            let identifier = normalize_ident(name.as_str());
                            let trigger_table = &ctx
                                .trigger_columns
                                .as_ref()
                                .expect("trigger columns must be set")
                                .table;
                            if trigger_table.get_column(&identifier).is_some()
                                || super::planner::ROWID_STRS
                                    .iter()
                                    .any(|name| name.eq_ignore_ascii_case(&identifier))
                            {
                                crate::bail_parse_error!("no such column: {}", identifier);
                            }
                            if name.quoted_with('"') && ctx.resolver.dqs_dml.is_enabled() {
                                *e = ast::Expr::Literal(ast::Literal::String(name.as_literal()));
                                Ok(WalkControl::Continue)
                            } else {
                                crate::bail_parse_error!("no such column: {}", identifier);
                            }
                        }
                        _ => Ok(WalkControl::Continue),
                    }
                })
            })?;
            Ok(std::mem::take(&mut ctx.subquery_bindings))
        });

        self.trigger_columns = saved_trigger_columns;
        result
    }

