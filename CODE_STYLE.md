# Code style

This repository follows the canonical ORE Software code-style policy:

https://github.com/ORESoftware/my-ai/blob/dev/code-style-guide.md

The policy is normative for production code and future changes.

For Rust in particular:

- named functions and methods should use explicit `return ...;` statements when returning a value;
- expression-oriented closures remain concise (for example, `.map(|value| value * 2)`);
- prefer early returns over deeply nested control flow;
- avoid unnecessary `mut` and hidden mutation;
- prefer explicit inputs/outputs and composable transformations;
- do not compress code merely to reduce line count.

The crate intentionally allows `clippy::needless_return` because the canonical ORE style requires explicit returns in named Rust functions, while Clippy's default stylistic preference recommends the opposite.
