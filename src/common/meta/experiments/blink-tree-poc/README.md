# B-link Tree split and transaction recovery

This Rust experiment exercises two parts of an object-storage metadata backend:
finding keys after an interrupted B-link Tree split, and reading a complete old
or new state after an interrupted transaction across two leaves. Its eight tests
use OpenDAL Memory and a simulated per-object CAS adapter.

Start with the [test cases](#test-cases), then read the implementation in
[`src/lib.rs`](src/lib.rs). The [validation record](validation/README.md) contains
the commands, environment, and test output. This directory is an independent
Cargo workspace; its commands build the experiment directly.

## Run

From the GreptimeDB repository root, with the toolchain in
[`rust-toolchain.toml`](../../../../../rust-toolchain.toml):

```sh
cargo test --locked --manifest-path src/common/meta/experiments/blink-tree-poc/Cargo.toml
cargo nextest run --locked --manifest-path src/common/meta/experiments/blink-tree-poc/Cargo.toml
cargo clippy --locked --manifest-path src/common/meta/experiments/blink-tree-poc/Cargo.toml --all-targets -- -D warnings
cargo fmt --manifest-path src/common/meta/experiments/blink-tree-poc/Cargo.toml -- --check
```

`cargo test` runs all eight tests with the standard Rust runner. The next command
runs the same tests with cargo-nextest. Add `--offline` to the first three
commands when the locked dependencies are already cached.

## Test cases

The tests use barriers and concurrent tasks to arrange the operations below.
Each test starts with four keys, a root, and one or two leaves.

| Test | Interrupted or concurrent operation | Expected result |
| --- | --- | --- |
| `cas_conflict_and_unknown_outcome` | Competing CAS writes, a stale version, and errors before or after a write | One CAS wins; stale versions fail; reads distinguish an unapplied write from a lost response |
| `split_crash_keeps_searchable` | The left leaf is split and linked before its parent separator is installed | Point reads and scans find every key and repair the parent |
| `split_conflict_preserves_writes` | A transaction changes a leaf after the split copied it | The stale split CAS fails and the transaction's values remain readable |
| `partial_prepare_reader_aborts` | A writer pauses after preparing the first of two leaves | Readers resolve Abort, return the old values, and prevent that transaction from committing |
| `committed_reader_repairs` | Commit succeeds but its response is lost; entries still contain intents | Readers return the new values, fold the intents, and allow an idempotent commit retry |
| `abort_commit_race_has_one_winner` | Commit competes with Abort in sixteen trials | One terminal decision wins and both leaves agree with it |
| `new_epoch_fences_old_leader` | Epoch 2 aborts an epoch-1 transaction after its first leaf is prepared | That transaction fails; an epoch-2 transaction commits |
| `scan_retries_across_commit` | A scan pauses between leaves while a transaction commits | Leaf-version validation retries the scan and returns the complete new state |

## How recovery works

### Split and parent repair

`split_right` creates a right leaf at a fresh object path. `split` then replaces
the original leaf with its truncated entries, exclusive `high_key`, and
`right_sibling` in one CAS. Installing the parent's separator is a separate CAS.

If the writer stops between the leaf update and parent update, readers follow
the sibling link and install the missing separator. If another write changes
the original leaf before the split CAS, the version check rejects the stale
copy. A fresh `Tree` over the retained Memory objects exercises recovery.

### Transaction decisions and reader repair

Each entry stores a committed value and an optional intent containing a
transaction ID, epoch, and next value. `prepare` installs intents while retaining
the committed values. After checking every requested intent, `commit` attempts
to create a Committed decision at `txns/{id}`.

A reader encountering an intent attempts to create Aborted at the same path.
Commit and Abort compete through conditional creation; the first terminal
decision wins. The reader uses that decision to select the old or new value
and folds the entry with a leaf CAS. Transaction IDs are unique, and terminal
objects are retained. A lost decision-write response is resolved by reading
the terminal object.

A scan buffers its result and validates the versions of all visited leaves
before returning. A changed version causes a retry. Each point-read call
resolves its own view.

### Object operations

OpenDAL 0.59.4 Memory provides conditional creation through
`write_with(...).if_not_exists(true)`. `CasStore` supplies conditional overwrite
for the experiment with a per-object mutex and a versioned JSON envelope.
Clients share this wrapper, and operations on different objects remain separate.
Every overwrite advances the version, including a write of an earlier payload.

Fault injection either rejects a write before application or applies it and
loses the response. The fixture uses byte-sized keys, a root and leaf chain,
and one active writer transaction, with concurrent readers and decision races.

## Next steps

Extend the object adapter to S3 and MinIO, connect the tree to `KvBackend` and
`TxnService`, and implement lease election with durable transaction registration
and a takeover barrier. Tree growth, deletion, and object reclamation complete
the path from this recovery experiment to the metadata backend.
