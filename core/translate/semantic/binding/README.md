# PR #8111 binder source

This folder contains the `core/translate/bind.rs` file from PR #8111 head,
commit `1d246f8165bacce26bd18a54f307f15d969a174b`.

The Rust files are contiguous, byte-for-byte slices of the original
8,691-line file. Their names describe the responsibility found in each slice;
they are not yet standalone Rust modules.

They are deliberately not wired as Rust modules yet. The split exposes the
binder's responsibilities so the HIR conversion can be agreed before code is
rewritten.
