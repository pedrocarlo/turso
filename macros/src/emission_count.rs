//! Implementation of the `#[emission_count]` attribute macro.
//!
//! The macro statically counts how many instructions, labels and cursors a translate
//! function emits into a `ProgramBuilder` and generates companion constants of type
//! `crate::vdbe::emission::EmissionBound` (so it is only usable inside `turso_core`):
//!
//! - `<fn>_EMISSIONS`: bound on emissions outside of loops. Straight-line emission
//!   call sites count 1 each; `if`/`match` contribute the component-wise max of
//!   their arms; calls to functions listed in `compose(...)` contribute that
//!   function's own `_EMISSIONS` constant (resolved by naming convention, so the
//!   callee must be annotated and in scope).
//! - `<fn>_EMISSIONS_LOOP<i>`: per-iteration bound of the i-th loop (in source
//!   order). Loops that emit must carry `#[emissions(per_iter = <expr>)]` where
//!   `<expr>` evaluates — using only the function's parameters — to an upper bound
//!   on the loop's total iterations per call.
//!
//! In debug builds the function is additionally wrapped so that its actual emission
//! delta is compared against `<fn>_EMISSIONS + Σ <fn>_EMISSIONS_LOOP<i> * per_iter`
//! via `crate::vdbe::emission::check_emission_estimate`, which warns on the
//! `emission_estimate` tracing target when the bound is exceeded. Every test run
//! therefore validates the derived bounds.
//!
//! Known blind spots (caught at runtime by the debug check, not at compile time):
//! emissions performed by un-composed callees, emissions hidden inside macro
//! invocations other than `emit_explain!`, and closures that run more than once
//! (closure bodies are counted as executing exactly once).

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote, ToTokens};
use std::collections::HashSet;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::visit_mut::{self, VisitMut};
use syn::{Attribute, Block, Expr, FnArg, Ident, ItemFn, Pat, Token, Type};

/// `ProgramBuilder` methods that emit exactly one instruction per call
/// (or at most one, which is fine for an upper bound).
const SINGLE_INSN_METHODS: &[&str] = &[
    "emit_insn",
    "emit_no_constant_insn",
    "emit_string8",
    "emit_string8_new_reg",
    "emit_int",
    "emit_bool",
    "emit_null",
    "emit_result_row",
    "emit_halt_err",
    "emit_explain",
    "emit_column_or_rowid",
    "emit_column_has_field",
    "emit_column_affinity",
    "alloc_registers_and_init_w_null",
];

/// `ProgramBuilder` methods that allocate at most one cursor per call.
const CURSOR_ALLOC_METHODS: &[&str] = &[
    "alloc_cursor_id",
    "alloc_cursor_id_keyed",
    "alloc_cursor_id_keyed_if_not_exists",
    "alloc_cursor_index",
    "alloc_cursor_index_if_not_exists",
];

/// Methods that emit a data-dependent number of instructions and therefore cannot
/// be counted at a call site.
const VARIABLE_EMIT_METHODS: &[&str] = &["close_cursors", "prologue", "epilogue"];

pub(crate) struct EmissionCountArgs {
    compose: HashSet<String>,
}

impl Parse for EmissionCountArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut compose = HashSet::new();
        if input.is_empty() {
            return Ok(Self { compose });
        }
        let keyword: Ident = input.parse()?;
        if keyword != "compose" {
            return Err(syn::Error::new(
                keyword.span(),
                "expected `compose(<fn>, ...)`",
            ));
        }
        let content;
        syn::parenthesized!(content in input);
        for ident in Punctuated::<Ident, Token![,]>::parse_terminated(&content)? {
            compose.insert(ident.to_string());
        }
        if !input.is_empty() {
            return Err(input.error("unexpected tokens after `compose(...)`"));
        }
        Ok(Self { compose })
    }
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
struct StaticCost {
    insns: usize,
    labels: usize,
    cursors: usize,
}

impl StaticCost {
    fn is_zero(self) -> bool {
        self == Self::default()
    }

    fn add(&mut self, other: Self) {
        self.insns += other.insns;
        self.labels += other.labels;
        self.cursors += other.cursors;
    }

    fn max(self, other: Self) -> Self {
        Self {
            insns: self.insns.max(other.insns),
            labels: self.labels.max(other.labels),
            cursors: self.cursors.max(other.cursors),
        }
    }
}

/// A symbolic (non-literal) part of a cost expression, resolved by const evaluation.
#[derive(Clone)]
enum Term {
    /// The `_EMISSIONS` constant of a composed callee.
    Callee(syn::Path),
    /// Component-wise max over mutually exclusive branch arms.
    Max(Vec<Cost>),
}

#[derive(Clone, Default)]
struct Cost {
    fixed: StaticCost,
    terms: Vec<Term>,
}

impl Cost {
    fn is_zero(&self) -> bool {
        self.fixed.is_zero() && self.terms.is_empty()
    }

    fn add(&mut self, other: Cost) {
        self.fixed.add(other.fixed);
        self.terms.extend(other.terms);
    }

    /// Render as a const expression of type `EmissionBound`.
    fn render(&self) -> TokenStream2 {
        let mut pieces: Vec<TokenStream2> = Vec::new();
        if !self.fixed.is_zero() || self.terms.is_empty() {
            let (insns, labels, cursors) =
                (self.fixed.insns, self.fixed.labels, self.fixed.cursors);
            pieces.push(quote!(crate::vdbe::emission::EmissionBound::new(
                #insns, #labels, #cursors
            )));
        }
        for term in &self.terms {
            pieces.push(match term {
                Term::Callee(path) => emissions_const_path(path).to_token_stream(),
                Term::Max(arms) => {
                    let mut arms = arms.iter();
                    let first = arms
                        .next()
                        .expect("Max terms always have at least two arms")
                        .render();
                    arms.fold(first, |acc, arm| {
                        let arm = arm.render();
                        quote!(#acc.max(#arm))
                    })
                }
            });
        }
        let mut pieces = pieces.into_iter();
        let first = pieces
            .next()
            .expect("at least one piece is always rendered");
        pieces.fold(first, |acc, piece| quote!(#acc.plus(#piece)))
    }
}

/// `path::to::callee` -> `path::to::callee_EMISSIONS`
fn emissions_const_path(path: &syn::Path) -> syn::Path {
    let mut path = path.clone();
    let segment = path.segments.last_mut().expect("call paths are non-empty");
    segment.ident = format_ident!("{}_EMISSIONS", segment.ident, span = segment.ident.span());
    path
}

struct LoopInfo {
    per_iter: Expr,
    cost: Cost,
}

struct BodyCollector<'a> {
    compose: &'a HashSet<String>,
    /// Cost accumulator for the region currently being collected; child regions
    /// (branch arms, loop bodies) are collected by temporarily swapping it out.
    cost: Cost,
    loops: Vec<LoopInfo>,
    errors: Vec<syn::Error>,
}

impl BodyCollector<'_> {
    fn collect_block(&mut self, block: &mut Block) -> Cost {
        let saved = std::mem::take(&mut self.cost);
        self.visit_block_mut(block);
        std::mem::replace(&mut self.cost, saved)
    }

    fn collect_expr(&mut self, expr: &mut Expr) -> Cost {
        let saved = std::mem::take(&mut self.cost);
        self.visit_expr_mut(expr);
        std::mem::replace(&mut self.cost, saved)
    }

    /// Fold the costs of mutually exclusive branch arms into the accumulator as an
    /// upper bound (component-wise max).
    fn push_branches(&mut self, arms: Vec<Cost>) {
        let mut arms: Vec<Cost> = arms.into_iter().filter(|arm| !arm.is_zero()).collect();
        match arms.len() {
            0 => {}
            1 => {
                let arm = arms.pop().expect("len is 1");
                self.cost.add(arm);
            }
            _ if arms.iter().all(|arm| arm.terms.is_empty()) => {
                // All arms are literal counts: fold the max in the macro itself.
                let folded = arms
                    .iter()
                    .fold(StaticCost::default(), |acc, arm| acc.max(arm.fixed));
                self.cost.fixed.add(folded);
            }
            _ => self.cost.terms.push(Term::Max(arms)),
        }
    }

    fn handle_loop(
        &mut self,
        attrs: &mut Vec<Attribute>,
        per_iter_cost: Cost,
        loop_span: proc_macro2::Span,
    ) {
        let annotation = self.extract_emissions_attr(attrs);
        if per_iter_cost.is_zero() {
            return;
        }
        match annotation {
            Some(per_iter) => self.loops.push(LoopInfo {
                per_iter,
                cost: per_iter_cost,
            }),
            None => self.errors.push(syn::Error::new(
                loop_span,
                "this loop emits instructions/labels/cursors; annotate it with \
                 #[emissions(per_iter = <expr>)] where <expr> is an upper bound on the \
                 loop's total iterations per call, computed from the function's parameters",
            )),
        }
    }

    /// Find, remove and parse a `#[emissions(per_iter = <expr>)]` attribute.
    fn extract_emissions_attr(&mut self, attrs: &mut Vec<Attribute>) -> Option<Expr> {
        let index = attrs.iter().position(|a| a.path().is_ident("emissions"))?;
        let attr = attrs.remove(index);
        match attr.parse_args::<PerIterArg>() {
            Ok(arg) => Some(arg.0),
            Err(err) => {
                self.errors.push(err);
                None
            }
        }
    }
}

struct PerIterArg(Expr);

impl Parse for PerIterArg {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let key: Ident = input.parse()?;
        if key != "per_iter" {
            return Err(syn::Error::new(key.span(), "expected `per_iter = <expr>`"));
        }
        input.parse::<Token![=]>()?;
        Ok(Self(input.parse()?))
    }
}

impl VisitMut for BodyCollector<'_> {
    fn visit_item_mut(&mut self, _item: &mut syn::Item) {
        // Nested items (fns, consts, ...) don't execute inline; don't count them.
    }

    fn visit_expr_method_call_mut(&mut self, node: &mut syn::ExprMethodCall) {
        visit_mut::visit_expr_method_call_mut(self, node);
        let name = node.method.to_string();
        if SINGLE_INSN_METHODS.contains(&name.as_str()) {
            self.cost.fixed.insns += 1;
        } else if name == "allocate_label" {
            self.cost.fixed.labels += 1;
        } else if CURSOR_ALLOC_METHODS.contains(&name.as_str()) {
            self.cost.fixed.cursors += 1;
        } else if VARIABLE_EMIT_METHODS.contains(&name.as_str()) {
            self.errors.push(syn::Error::new(
                node.method.span(),
                format!(
                    "`{name}` emits a data-dependent number of instructions and cannot be \
                     counted by #[emission_count]; inline the emission loop and annotate it \
                     with #[emissions(per_iter = <expr>)]"
                ),
            ));
        } else if name.starts_with("emit_") {
            self.errors.push(syn::Error::new(
                node.method.span(),
                format!(
                    "`{name}` is not in #[emission_count]'s emission method tables; if it \
                     emits a fixed number of instructions, add it to the tables in \
                     macros/src/emission_count.rs"
                ),
            ));
        }
    }

    fn visit_expr_call_mut(&mut self, node: &mut syn::ExprCall) {
        visit_mut::visit_expr_call_mut(self, node);
        if let Expr::Path(func) = node.func.as_ref() {
            if let Some(segment) = func.path.segments.last() {
                if self.compose.contains(&segment.ident.to_string()) {
                    self.cost.terms.push(Term::Callee(func.path.clone()));
                }
            }
        }
    }

    fn visit_macro_mut(&mut self, mac: &mut syn::Macro) {
        // `emit_explain!` emits at most one instruction. Other macros' token trees
        // are not inspected; emissions hidden inside them are invisible to the
        // counter (the debug-build check will flag the resulting under-estimate).
        if mac
            .path
            .segments
            .last()
            .is_some_and(|segment| segment.ident == "emit_explain")
        {
            self.cost.fixed.insns += 1;
        }
    }

    fn visit_expr_if_mut(&mut self, node: &mut syn::ExprIf) {
        self.visit_expr_mut(&mut node.cond);
        let then_cost = self.collect_block(&mut node.then_branch);
        let else_cost = match node.else_branch.as_mut() {
            Some((_, else_expr)) => self.collect_expr(else_expr),
            None => Cost::default(),
        };
        self.push_branches(vec![then_cost, else_cost]);
    }

    fn visit_expr_match_mut(&mut self, node: &mut syn::ExprMatch) {
        self.visit_expr_mut(&mut node.expr);
        let mut arm_costs = Vec::with_capacity(node.arms.len());
        for arm in &mut node.arms {
            if let Some((_, guard)) = arm.guard.as_mut() {
                // A guard may run even when its arm is not taken, so guards are
                // counted unconditionally rather than inside the branch max.
                self.visit_expr_mut(guard);
            }
            arm_costs.push(self.collect_expr(&mut arm.body));
        }
        self.push_branches(arm_costs);
    }

    fn visit_expr_for_loop_mut(&mut self, node: &mut syn::ExprForLoop) {
        self.visit_expr_mut(&mut node.expr);
        let body_cost = self.collect_block(&mut node.body);
        let span = node.for_token.span;
        self.handle_loop(&mut node.attrs, body_cost, span);
    }

    fn visit_expr_while_mut(&mut self, node: &mut syn::ExprWhile) {
        // The condition re-runs every iteration, so its cost is per-iteration too.
        let mut per_iter_cost = self.collect_expr(&mut node.cond);
        per_iter_cost.add(self.collect_block(&mut node.body));
        let span = node.while_token.span;
        self.handle_loop(&mut node.attrs, per_iter_cost, span);
    }

    fn visit_expr_loop_mut(&mut self, node: &mut syn::ExprLoop) {
        let body_cost = self.collect_block(&mut node.body);
        let span = node.loop_token.span;
        self.handle_loop(&mut node.attrs, body_cost, span);
    }
}

fn is_program_builder_mut_ref(ty: &Type) -> bool {
    let Type::Reference(reference) = ty else {
        return false;
    };
    if reference.mutability.is_none() {
        return false;
    }
    let Type::Path(path) = reference.elem.as_ref() else {
        return false;
    };
    path.path
        .segments
        .last()
        .is_some_and(|segment| segment.ident == "ProgramBuilder")
}

pub(crate) fn emission_count_attribute(attr: TokenStream, input: TokenStream) -> TokenStream {
    let args = match syn::parse::<EmissionCountArgs>(attr) {
        Ok(args) => args,
        Err(err) => return err.to_compile_error().into(),
    };
    let mut function = match syn::parse::<ItemFn>(input) {
        Ok(function) => function,
        Err(err) => return err.to_compile_error().into(),
    };

    let mut errors = Vec::new();

    if function.sig.asyncness.is_some() {
        errors.push(syn::Error::new(
            function.sig.fn_token.span,
            "#[emission_count] does not support async functions",
        ));
    }

    // Locate the `&mut ProgramBuilder` parameter and the argument names to forward.
    let mut builder_ident = None;
    let mut arg_idents = Vec::new();
    for input in &function.sig.inputs {
        match input {
            FnArg::Receiver(receiver) => errors.push(syn::Error::new(
                receiver.self_token.span,
                "#[emission_count] only supports free functions (no self receiver)",
            )),
            FnArg::Typed(pat_ty) => {
                let Pat::Ident(pat_ident) = pat_ty.pat.as_ref() else {
                    errors.push(syn::Error::new(
                        pat_ty.pat.span(),
                        "#[emission_count] requires plain identifier parameters \
                         (no destructuring patterns)",
                    ));
                    continue;
                };
                arg_idents.push(pat_ident.ident.clone());
                if builder_ident.is_none() && is_program_builder_mut_ref(&pat_ty.ty) {
                    builder_ident = Some(pat_ident.ident.clone());
                }
            }
        }
    }
    if builder_ident.is_none() {
        errors.push(syn::Error::new(
            function.sig.ident.span(),
            "#[emission_count] requires a `&mut ProgramBuilder` parameter to snapshot \
             emission counts",
        ));
    }

    let mut collector = BodyCollector {
        compose: &args.compose,
        cost: Cost::default(),
        loops: Vec::new(),
        errors: Vec::new(),
    };
    collector.visit_block_mut(&mut function.block);
    errors.extend(collector.errors);
    if !errors.is_empty() {
        let errors = errors.into_iter().map(|err| err.to_compile_error());
        return quote!(#function #(#errors)*).into();
    }
    let builder_ident = builder_ident.expect("checked above");
    let static_cost = collector.cost.render();
    let loops = collector.loops;

    let fn_ident = function.sig.ident.clone();
    let const_ident = format_ident!("{}_EMISSIONS", fn_ident);
    let inner_ident = format_ident!("__{}_emission_count_inner", fn_ident);

    // The inner function keeps the original body (including early returns) and
    // parameter patterns; the outer function becomes an instrumented wrapper.
    // Lint-level attributes must move with the body to keep suppressing what they
    // suppressed before the split.
    let mut inner_attrs: Vec<Attribute> = vec![
        syn::parse_quote!(#[allow(clippy::too_many_arguments)]),
        syn::parse_quote!(#[inline(always)]),
    ];
    inner_attrs.extend(
        function
            .attrs
            .iter()
            .filter(|attr| attr.path().is_ident("allow") || attr.path().is_ident("expect"))
            .cloned(),
    );
    let inner_fn = ItemFn {
        attrs: inner_attrs,
        vis: syn::Visibility::Inherited,
        sig: syn::Signature {
            ident: inner_ident.clone(),
            ..function.sig.clone()
        },
        block: function.block.clone(),
    };

    // The wrapper only forwards its parameters, so `mut` bindings would be unused.
    for input in function.sig.inputs.iter_mut() {
        if let FnArg::Typed(pat_ty) = input {
            if let Pat::Ident(pat_ident) = pat_ty.pat.as_mut() {
                pat_ident.mutability = None;
            }
        }
    }

    let loop_const_idents: Vec<Ident> = (0..loops.len())
        .map(|i| format_ident!("{}_EMISSIONS_LOOP{}", fn_ident, i))
        .collect();
    let per_iter_exprs: Vec<&Expr> = loops.iter().map(|l| &l.per_iter).collect();
    let estimate = quote!(#const_ident #(.plus(#loop_const_idents.times(#per_iter_exprs)))*);

    let new_block: Block = syn::parse_quote!({
        #[cfg(debug_assertions)]
        let __emission_estimate = #estimate;
        #[cfg(debug_assertions)]
        let __emission_base = #builder_ident.emission_snapshot();

        #inner_fn

        #[allow(clippy::let_unit_value)]
        let __emission_ret = #inner_ident(#(#arg_idents),*);

        #[cfg(debug_assertions)]
        crate::vdbe::emission::check_emission_estimate(
            concat!(module_path!(), "::", stringify!(#fn_ident)),
            __emission_estimate,
            __emission_base,
            #builder_ident.emission_snapshot(),
        );

        __emission_ret
    });
    *function.block = new_block;
    function
        .attrs
        .push(syn::parse_quote!(#[allow(clippy::let_and_return)]));

    let vis = &function.vis;
    let static_doc = format!(
        "Auto-generated by `#[emission_count]` for [`{fn_ident}`]: upper bound on the \
         instructions/labels/cursors it emits outside of loops (branches contribute the \
         max of their arms). Loop bodies are bounded separately by the \
         `{fn_ident}_EMISSIONS_LOOP<i>` constants."
    );
    let loop_defs =
        loops
            .iter()
            .zip(&loop_const_idents)
            .enumerate()
            .map(|(i, (loop_info, loop_const))| {
                let cost = loop_info.cost.render();
                let doc = format!(
                    "Auto-generated by `#[emission_count]`: per-iteration emission bound of \
                 loop #{i} (in source order) of [`{fn_ident}`]."
                );
                quote! {
                    #[allow(non_upper_case_globals)]
                    #[doc = #doc]
                    #vis const #loop_const: crate::vdbe::emission::EmissionBound = #cost;
                }
            });

    quote! {
        #[allow(non_upper_case_globals)]
        #[doc = #static_doc]
        #vis const #const_ident: crate::vdbe::emission::EmissionBound = #static_cost;
        #(#loop_defs)*
        #function
    }
    .into()
}
