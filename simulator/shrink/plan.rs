use either::Either;
use indexmap::IndexSet;
use itertools::Itertools;

use crate::{
    SandboxedResult, SimulatorEnv,
    model::{
        Query,
        interactions::{InteractionPlan, InteractionType, Interactions, InteractionsType, Span},
        property::{Property, PropertyDiscriminants},
    },
    run_simulation,
    runner::execution::Execution,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

fn retain_relevant_queries(
    extensional_queries: &mut Vec<Query>,
    depending_tables: &IndexSet<String>,
) {
    extensional_queries.retain(|query| {
        query.is_transaction()
            || (!matches!(query, Query::Select(..))
                && query.uses().iter().any(|t| depending_tables.contains(t)))
    });
}

impl InteractionPlan {
    /// Create a smaller interaction plan by deleting a property
    pub(crate) fn shrink_interaction_plan(&self, failing_execution: &Execution) -> InteractionPlan {
        // todo: this is a very naive implementation, next steps are;
        // - Shrink to multiple values by removing random interactions
        // - Shrink properties by removing their extensions, or shrinking their values
        let mut plan = self.clone();

        let all_interactions = self.interactions_list();
        let failing_interaction = &all_interactions[failing_execution.interaction_index];

        let range = self.find_interactions_range(failing_interaction.id());

        // Interactions that are part of the failing overall property
        let mut failing_property = all_interactions
            [range.start..=failing_execution.interaction_index]
            .iter()
            .rev();

        let depending_tables = failing_property
            .find_map(|interaction| {
                match &interaction.interaction {
                    InteractionType::Query(query) | InteractionType::FaultyQuery(query) => {
                        Some(query.dependencies())
                    }
                    // Fault does not depend on
                    InteractionType::Fault(..) => Some(IndexSet::new()),
                    _ => None,
                }
            })
            .unwrap_or_else(|| IndexSet::new());

        let before = self.len_properties();

        // Remove all properties after the failing one
        plan.truncate(failing_execution.interaction_index + 1);

        // means we errored in some fault on transaction statement so just maintain the statements from before the failing one
        if !depending_tables.is_empty() {
            plan.remove_properties(&depending_tables, failing_execution.interaction_index);
        }

        let after = plan.len_properties();

        tracing::info!(
            "Shrinking interaction plan from {} to {} properties",
            before,
            after
        );

        plan
    }

    /// Create a smaller interaction plan by deleting a property
    pub(crate) fn brute_shrink_interaction_plan(
        &self,
        result: &SandboxedResult,
        env: Arc<Mutex<SimulatorEnv>>,
    ) -> InteractionPlan {
        let failing_execution = match result {
            SandboxedResult::Panicked {
                error: _,
                last_execution: e,
            } => e,
            SandboxedResult::FoundBug {
                error: _,
                history: _,
                last_execution: e,
            } => e,
            SandboxedResult::Correct => {
                unreachable!("shrink is never called on correct result")
            }
        };

        let mut plan = self.clone();
        let all_interactions = self.interactions_list();
        let property_id = all_interactions[failing_execution.interaction_index].id();

        let before = self.len_properties();

        plan.truncate(failing_execution.interaction_index + 1);

        // phase 1: shrink extensions
        for interaction in &mut plan {
            if let InteractionsType::Property(property) = &mut interaction.interactions {
                match property {
                    Property::InsertValuesSelect { queries, .. }
                    | Property::DoubleCreateFailure { queries, .. }
                    | Property::DeleteSelect { queries, .. }
                    | Property::DropSelect { queries, .. }
                    | Property::Queries { queries } => {
                        let mut temp_plan = InteractionPlan::new_with(
                            queries
                                .iter()
                                .map(|q| {
                                    Interactions::new(
                                        interaction.connection_index,
                                        InteractionsType::Query(q.clone()),
                                    )
                                })
                                .collect(),
                            self.mvcc,
                        );

                        temp_plan = InteractionPlan::iterative_shrink(
                            temp_plan,
                            failing_execution,
                            result,
                            env.clone(),
                            secondary_interactions_index,
                        );
                        //temp_plan = Self::shrink_queries(temp_plan, failing_execution, result, env);

                        *queries = temp_plan
                            .into_iter()
                            .filter_map(|i| match i.interactions {
                                InteractionsType::Query(q) => Some(q),
                                _ => None,
                            })
                            .collect();
                    }
                    Property::WhereTrueFalseNull { .. }
                    | Property::UnionAllPreservesCardinality { .. }
                    | Property::SelectLimit { .. }
                    | Property::SelectSelectOptimizer { .. }
                    | Property::FaultyQuery { .. }
                    | Property::FsyncNoWait { .. }
                    | Property::ReadYourUpdatesBack { .. }
                    | Property::TableHasExpectedContent { .. }
                    | Property::AllTableHaveExpectedContent { .. } => {}
                }
            }
        }

        // phase 2: shrink the entire plan
        plan = Self::iterative_shrink(
            plan,
            failing_execution,
            result,
            env,
            secondary_interactions_index,
        );

        let after = plan.len_properties();

        tracing::info!(
            "Shrinking interaction plan from {} to {} properties",
            before,
            after
        );

        plan
    }

    /// shrink a plan by removing one interaction at a time (and its deps) while preserving the error
    fn iterative_shrink(
        mut plan: InteractionPlan,
        failing_execution: &Execution,
        old_result: &SandboxedResult,
        env: Arc<Mutex<SimulatorEnv>>,
        secondary_interaction_index: usize,
    ) -> InteractionPlan {
        for i in (0..plan.len_properties()).rev() {
            if i == secondary_interaction_index {
                continue;
            }
            let mut test_plan = plan.clone();

            // TODO: change
            test_plan.remove_property(i);

            if Self::test_shrunk_plan(&test_plan, failing_execution, old_result, env.clone()) {
                plan = test_plan;
            }
        }
        plan
    }

    fn test_shrunk_plan(
        test_plan: &InteractionPlan,
        failing_execution: &Execution,
        old_result: &SandboxedResult,
        env: Arc<Mutex<SimulatorEnv>>,
    ) -> bool {
        let last_execution = Arc::new(Mutex::new(*failing_execution));
        let result = SandboxedResult::from(
            std::panic::catch_unwind(|| {
                let plan = test_plan.static_iterator();

                run_simulation(env.clone(), plan, last_execution.clone())
            }),
            last_execution,
        );
        match (old_result, &result) {
            (
                SandboxedResult::Panicked { error: e1, .. },
                SandboxedResult::Panicked { error: e2, .. },
            )
            | (
                SandboxedResult::FoundBug { error: e1, .. },
                SandboxedResult::FoundBug { error: e2, .. },
            ) => e1 == e2,
            _ => false,
        }
    }

    /// Remove all properties that do not use the failing tables
    fn remove_properties(
        &mut self,
        depending_tables: &IndexSet<String>,
        failing_interaction_index: usize,
    ) {
        // First pass - mark indexes that should be retained
        let mut retain_map = Vec::with_capacity(self.len());
        let mut iter = self.interactions_list().iter().enumerate().peekable();
        while let Some((idx, interaction)) = iter.next() {
            let id = interaction.id();
            // get interactions from a particular property
            let span = interaction
                .span
                .as_ref()
                .expect("we should loop on interactions that a span");

            let first = std::iter::once((idx, interaction));

            let property_interactions = match span.span {
                Span::Start => {
                    Either::Left(first.chain(iter.peeking_take_while(|(_, interaction)| {
                        interaction.id() == id
                            && interaction
                                .span
                                .as_ref()
                                .is_some_and(|span| matches!(span.span, Span::End))
                    })))
                }
                Span::End => panic!("we should always be at the start of an interaction"),
                Span::StartEnd => Either::Right(first),
            };

            for (idx, interaction) in property_interactions.into_iter() {
                let retain = if idx == failing_interaction_index {
                    true
                } else {
                    let has_table = interaction
                        .uses()
                        .iter()
                        .any(|t| depending_tables.contains(t));

                    let is_fault = matches!(&interaction.interaction, InteractionType::Fault(..));
                    let is_transaction = matches!(
                        &interaction.interaction,
                        InteractionType::Query(Query::Begin(..))
                            | InteractionType::Query(Query::Commit(..))
                            | InteractionType::Query(Query::Rollback(..))
                    );

                    let mut skip_interaction = matches!(
                        &interaction.interaction,
                        InteractionType::Query(Query::Select(_))
                    );

                    if let Some(property) = span.property {
                        skip_interaction = skip_interaction
                            || matches!(
                                property,
                                PropertyDiscriminants::AllTableHaveExpectedContent
                                    | PropertyDiscriminants::SelectLimit
                                    | PropertyDiscriminants::SelectSelectOptimizer
                                    | PropertyDiscriminants::TableHasExpectedContent
                                    | PropertyDiscriminants::UnionAllPreservesCardinality
                                    | PropertyDiscriminants::WhereTrueFalseNull
                            );
                    }

                    is_fault || is_transaction || (has_table && !skip_interaction)
                };
                retain_map.push(retain);
            }
        }

        debug_assert!(self.len() == retain_map.len());

        let mut idx = 0;
        // Remove all properties that do not use the failing tables
        self.retain_mut(|_| {
            let retain = retain_map[idx];
            idx += 1;
            retain
        });

        // Comprises of idxs of Begin interactions
        let mut begin_idx: HashMap<usize, Vec<usize>> = HashMap::new();
        // Comprises of idxs of Commit and Rollback intereactions
        let mut end_tx_idx: HashMap<usize, Vec<usize>> = HashMap::new();

        for (idx, interaction) in self.interactions_list().into_iter().enumerate() {
            match &interaction.interaction {
                InteractionType::Query(Query::Begin(..)) => {
                    begin_idx
                        .entry(interaction.connection_index)
                        .or_insert_with(|| vec![idx]);
                }
                InteractionType::Query(Query::Commit(..))
                | InteractionType::Query(Query::Rollback(..)) => {
                    let last_begin = begin_idx
                        .get(&interaction.connection_index)
                        .and_then(|list| list.last())
                        .unwrap()
                        + 1;
                    if last_begin == idx {
                        end_tx_idx
                            .entry(interaction.connection_index)
                            .or_insert_with(|| vec![idx]);
                    }
                }
                _ => {}
            }
        }

        // remove interactions if its just a Begin Commit/Rollback with no queries in the middle
        let mut range_transactions = end_tx_idx
            .into_iter()
            .map(|(conn_index, list)| (conn_index, list.into_iter().peekable()))
            .collect::<HashMap<_, _>>();
        let mut idx = 0;
        self.retain_mut(|interactions| {
            let mut retain = true;

            let iter = range_transactions.get_mut(&interactions.connection_index);

            if let Some(iter) = iter {
                if let Some(txn_interaction_idx) = iter.peek().copied() {
                    if txn_interaction_idx == idx {
                        iter.next();
                    }
                    if txn_interaction_idx == idx || txn_interaction_idx.saturating_sub(1) == idx {
                        retain = false;
                    }
                }
            }

            idx += 1;
            retain
        });
    }
}
