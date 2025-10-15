use std::vec;

use sql_generation::{
    generation::{Arbitrary, ArbitraryFrom, GenerationContext, frequency},
    model::query::{
        Create,
        transaction::{Begin, Commit},
    },
};

use crate::{
    SimulatorEnv,
    generation::{
        WeightedDistribution,
        property::PropertyDistribution,
        query::{QueryDistribution, possible_queries},
    },
    model::{
        Query,
        interactions::{
            Fault, Interaction, InteractionPlan, InteractionPlanIterator, InteractionStats,
            InteractionType, Interactions, InteractionsType,
        },
        metrics::Remaining,
    },
};

use super::property::Property;

impl InteractionPlan {
    pub fn init_plan(env: &mut SimulatorEnv) -> Self {
        let mut plan = InteractionPlan::new(env.profile.experimental_mvcc);

        // First create at least one table
        let create_query = Create::arbitrary(&mut env.rng.clone(), &env.connection_context(0));

        // initial query starts at 0th connection
        let interactions =
            Interactions::new(0, InteractionsType::Query(Query::Create(create_query)));

        plan.append_interactions(interactions.interactions());
        plan.set_last_interactions(interactions);

        plan
    }

    pub fn generator<'a>(
        &'a mut self,
        rng: &'a mut impl rand::Rng,
    ) -> impl InteractionPlanIterator {
        let interactions = self.interactions_list();
        let iter = interactions.into_iter();
        PlanGenerator {
            plan: self,
            peek: None,
            iter,
            rng,
        }
    }

    /// Appends a new [Interactions] and outputs the next set of [Interaction] to take
    pub fn generate_next_interaction(
        &mut self,
        rng: &mut impl rand::Rng,
        env: &mut SimulatorEnv,
    ) -> Option<Interactions> {
        let num_interactions = env.opts.max_interactions as usize;
        // If last interaction needs to check all db tables, generate the Property to do so
        if let Some(i) = self.last_interactions()
            && i.check_tables()
        {
            let check_all_tables = Interactions::new(
                i.connection_index,
                InteractionsType::Property(Property::AllTableHaveExpectedContent {
                    tables: env
                        .connection_context(i.connection_index)
                        .tables()
                        .iter()
                        .map(|t| t.name.clone())
                        .collect(),
                }),
            );

            return Some(check_all_tables);
        }

        if self.len() < num_interactions {
            let conn_index = env.choose_conn(rng);
            let interactions = if self.mvcc && !env.conn_in_transaction(conn_index) {
                let query = Query::Begin(Begin::Concurrent);
                Interactions::new(conn_index, InteractionsType::Query(query))
            } else if self.mvcc
                && env.conn_in_transaction(conn_index)
                && env.has_conn_executed_query_after_transaction(conn_index)
                && rng.random_bool(0.4)
            {
                let query = Query::Commit(Commit);
                Interactions::new(conn_index, InteractionsType::Query(query))
            } else {
                let conn_ctx = &env.connection_context(conn_index);
                Interactions::arbitrary_from(rng, conn_ctx, (env, self.stats(), conn_index))
            };

            tracing::debug!("Generating interaction {}/{}", self.len(), num_interactions);

            Some(interactions)
        } else {
            // after we generated all interactions if some connection is still in a transaction, commit
            (0..env.connections.len())
                .find(|idx| env.conn_in_transaction(*idx))
                .map(|conn_index| {
                    Interactions::new(conn_index, InteractionsType::Query(Query::Commit(Commit)))
                })
        }
    }
}

pub struct PlanGenerator<'a, R: rand::Rng> {
    plan: &'a mut InteractionPlan,
    peek: Option<Interaction>,
    iter: <Vec<Interaction> as IntoIterator>::IntoIter,
    rng: &'a mut R,
}

impl<'a, R: rand::Rng> PlanGenerator<'a, R> {
    fn next_interaction(&mut self, env: &mut SimulatorEnv) -> Option<Interaction> {
        self.iter
            .next()
            .or_else(|| {
                // Iterator ended, try to create a new iterator
                // This will not be an infinte sequence because generate_next_interaction will eventually
                // stop generating
                let interactions = self.plan.generate_next_interaction(self.rng, env)?;

                self.plan.push(interactions.clone());

                let mut iter = interactions.interactions().into_iter();
                let next = iter.next();
                self.iter = iter;

                next
            })
            .map(|interaction| {
                // Certain properties can generate intermediate queries
                // we need to generate them here and substitute
                if let InteractionType::Query(Query::Placeholder) = &interaction.interaction {
                    let stats = self.plan.stats();

                    let conn_ctx = env.connection_context(interaction.connection_index);

                    let remaining_ = Remaining::new(
                        env.opts.max_interactions,
                        &env.profile.query,
                        &stats,
                        env.profile.experimental_mvcc,
                        &conn_ctx,
                    );

                    let Some(InteractionsType::Property(property)) = &mut self
                        .plan
                        .last_interactions_mut()
                        .map(|interactions| &mut interactions.interactions)
                    else {
                        unreachable!("only properties have extensional queries");
                    };

                    let queries = possible_queries(conn_ctx.tables());
                    let query_distr = QueryDistribution::new(queries, &remaining_);

                    let query_gen = property.get_extensional_query_gen_function();

                    let mut count = 0;
                    let new_query = loop {
                        if count > 1_000_000 {
                            panic!("possible infinite loop in query generation");
                        }
                        if let Some(new_query) =
                            (query_gen)(self.rng, &conn_ctx, &query_distr, property)
                        {
                            let queries = property.get_extensional_queries().unwrap();
                            let query = queries
                                .iter_mut()
                                .find(|query| matches!(query, Query::Placeholder))
                                .expect("Placeholder should be present in extensional queries");
                            *query = new_query.clone();
                            break new_query;
                        }
                        count += 1;
                    };
                    Interaction::new(
                        interaction.connection_index,
                        InteractionType::Query(new_query),
                    )
                } else {
                    interaction
                }
            })
    }

    fn peek(&mut self, env: &mut SimulatorEnv) -> Option<&Interaction> {
        if self.peek.is_none() {
            self.peek = self.next_interaction(env);
        }
        self.peek.as_ref()
    }
}

impl<'a, R: rand::Rng> InteractionPlanIterator for PlanGenerator<'a, R> {
    /// try to generate the next [Interactions] and store it
    fn next(&mut self, env: &mut SimulatorEnv) -> Option<Interaction> {
        let mvcc = self.plan.mvcc;
        match self.peek(env) {
            Some(peek_interaction) => {
                if mvcc && peek_interaction.is_ddl() {
                    // if any connection is in a transaction,
                    // try to commit the transaction as we cannot execute DDL statements in concurrent mode

                    if let Some(conn_index) =
                        (0..env.connections.len()).find(|idx| env.conn_in_transaction(*idx))
                    {
                        return Some(Interaction::new(
                            conn_index,
                            InteractionType::Query(Query::Commit(Commit)),
                        ));
                    }
                }

                self.peek.take()
            }
            None => {
                // after we generated all interactions if some connection is still in a transaction, commit
                (0..env.connections.len())
                    .find(|idx| env.conn_in_transaction(*idx))
                    .map(|conn_index| {
                        let query = Query::Commit(Commit);
                        let interaction =
                            Interactions::new(conn_index, InteractionsType::Query(query));
                        self.plan.push(interaction);

                        Interaction::new(conn_index, InteractionType::Query(Query::Commit(Commit)))
                    })
            }
        }
    }
}

fn random_fault<R: rand::Rng + ?Sized>(
    rng: &mut R,
    env: &SimulatorEnv,
    conn_index: usize,
) -> Interactions {
    let faults = if env.opts.disable_reopen_database {
        vec![Fault::Disconnect]
    } else {
        vec![Fault::Disconnect, Fault::ReopenDatabase]
    };
    let fault = faults[rng.random_range(0..faults.len())];
    Interactions::new(conn_index, InteractionsType::Fault(fault))
}

impl ArbitraryFrom<(&SimulatorEnv, InteractionStats, usize)> for Interactions {
    fn arbitrary_from<R: rand::Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        conn_ctx: &C,
        (env, stats, conn_index): (&SimulatorEnv, InteractionStats, usize),
    ) -> Self {
        let remaining_ = Remaining::new(
            env.opts.max_interactions,
            &env.profile.query,
            &stats,
            env.profile.experimental_mvcc,
            conn_ctx,
        );

        let queries = possible_queries(conn_ctx.tables());
        let query_distr = QueryDistribution::new(queries, &remaining_);

        #[expect(clippy::type_complexity)]
        let mut choices: Vec<(u32, Box<dyn Fn(&mut R) -> Interactions>)> = vec![
            (
                query_distr.weights().total_weight(),
                Box::new(|rng: &mut R| {
                    Interactions::new(
                        conn_index,
                        InteractionsType::Query(Query::arbitrary_from(rng, conn_ctx, &query_distr)),
                    )
                }),
            ),
            (
                remaining_
                    .select
                    .min(remaining_.insert)
                    .min(remaining_.create)
                    .max(1),
                Box::new(|rng: &mut R| random_fault(rng, env, conn_index)),
            ),
        ];

        if let Ok(property_distr) =
            PropertyDistribution::new(env, &remaining_, &query_distr, conn_ctx)
        {
            choices.push((
                property_distr.weights().total_weight(),
                Box::new(move |rng: &mut R| {
                    Interactions::new(
                        conn_index,
                        InteractionsType::Property(Property::arbitrary_from(
                            rng,
                            conn_ctx,
                            &property_distr,
                        )),
                    )
                }),
            ));
        };

        frequency(choices, rng)
    }
}
