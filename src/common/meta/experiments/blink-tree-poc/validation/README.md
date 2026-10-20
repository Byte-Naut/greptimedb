# Validation record

The implementation is from commit
`45e53a82115c49d8c7fc800688c5f7a80ee9f232`, based on GreptimeDB commit
`9a2ac7f9de29efded3b8a74200f7b04340c1c59e`. The documentation update adds this
record and the reading guide. `Cargo.toml`, `Cargo.lock`, and `src/lib.rs` retain
the implementation commit's contents.

The commands below run from the repository root on Ubuntu in WSL 2. Each linked
transcript records the command, timestamps, output, and exit code. Compiler and
runner versions and source checksums are in [`environment.txt`](environment.txt).

Both test runners passed all eight tests. Clippy and the formatting check
completed with exit code 0.

| Check | Command | Recorded output |
| --- | --- | --- |
| Rust tests | `cargo test --locked --manifest-path src/common/meta/experiments/blink-tree-poc/Cargo.toml` | [`cargo-test.txt`](cargo-test.txt) |
| Nextest | `cargo nextest run --locked --manifest-path src/common/meta/experiments/blink-tree-poc/Cargo.toml` | [`nextest.txt`](nextest.txt) |
| Clippy | `cargo clippy --locked --manifest-path src/common/meta/experiments/blink-tree-poc/Cargo.toml --all-targets -- -D warnings` | [`clippy.txt`](clippy.txt) |
| Formatting | `cargo fmt --manifest-path src/common/meta/experiments/blink-tree-poc/Cargo.toml -- --check` | [`fmt.txt`](fmt.txt) |

The experiment uses OpenDAL Memory. Its eight cases and object-operation model
are described in the [reading guide](../README.md#test-cases). The GreptimeDB
build and `common-meta` suite results were supplied separately in the
18 October application email as `GreptimeDB-proof-20261018-005345-463092.log`.
