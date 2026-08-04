<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Optimizing Scalar Expressions

This guide describes how to optimize the native scalar expression implementations in the
`datafusion-comet-spark-expr` crate (`native/spark-expr/`). These are the per-row and per-batch
kernels that run inside DataFusion for Spark-compatible functions such as casts, string
functions, JSON encoders, and array functions.

The workflow is deliberate: **measure first, keep the output bit-identical, and prove the win
with a benchmark that covers the shapes where an optimization is most likely to backfire.** Every
step below exists because skipping it has produced either a no-op PR or a regression.

## When to optimize

Good candidates share these traits:

- The function is called once per row or once per element, so allocation and branching costs
  multiply by batch size.
- The hot path allocates (a `String`, a `Vec`, a sliced `ArrayRef`, a compiled `Regex`) on every
  row, or dispatches an Arrow compute kernel per element.
- The function already has correctness tests, so behavior is pinned.

Do not optimize speculatively. If you cannot write a benchmark that shows a meaningful
improvement, there is nothing to submit.

## The workflow

1. **Read the current implementation** in `native/spark-expr/src/`. Identify the per-row cost:
   allocation, kernel dispatch, UTF-8 decoding, regex compilation, bitmap reads.
2. **Write or extend a criterion benchmark** in `native/spark-expr/benches/` (see below). Run it
   on `main` to capture a baseline before changing any code.
3. **Apply the optimization**, preserving exact semantics (see [Correctness](#correctness-is-non-negotiable)).
4. **Run the existing unit tests** for the function. They must pass unchanged. Output must be
   bit-identical to `main`, including null placement and error behavior.
5. **Re-run the benchmark** against the baseline. Confirm a meaningful speedup on at least one
   shape **and no meaningful regression on any shape**.
6. **Submit** with the criterion output pasted into the PR description.

## Writing a benchmark

Each optimized expression should have a criterion benchmark under `native/spark-expr/benches/`
registered in `native/spark-expr/Cargo.toml`:

```toml
[[bench]]
name = "unhex"
harness = false
```

Model new benchmarks on an existing one such as `benches/unhex.rs` or `benches/array_size.rs`.
A benchmark builds representative Arrow arrays (typically 8192 rows, the default batch size),
wraps the call in `black_box`, and benches several input **shapes**:

```rust
fn criterion_benchmark(c: &mut Criterion) {
    let size = 8192;
    // ... build arrays for each shape ...
    let mut bench = |name: &str, arr: &ArrayRef| {
        let args = vec![ColumnarValue::Array(Arc::clone(arr))];
        c.bench_function(name, |b| {
            b.iter(|| black_box(spark_unhex(black_box(&args)).unwrap()))
        });
    };
    bench("spark_unhex: all valid", &all_valid);
    bench("spark_unhex: with nulls", &with_nulls);
    bench("spark_unhex: long strings", &long_valid);
    bench("spark_unhex: invalid inputs", &invalid);
    bench("spark_unhex: mixed hex column", &mixed);
}
```

### Cover the shapes that break optimizations

The most common way a "faster" change is actually a regression is that it wins on the shape you
looked at and loses on one you did not. Always include:

- **No nulls vs. sparse nulls vs. dense nulls.** Fast paths that assume no nulls, or that batch
  contiguous non-null runs, can be slower than the base loop when nulls are dense.
- **Short vs. long values.** Allocation savings shrink and copy costs grow as values get longer.
- **Valid vs. invalid inputs.** Error and fallback paths have different costs.
- **ASCII vs. non-ASCII** for string functions with an ASCII fast path.

### Running benchmarks

```sh
cd native
# capture the baseline on main
git checkout main
cargo bench --bench unhex -- --save-baseline main
# switch to your branch and compare
git checkout my-branch
cargo bench --bench unhex -- --baseline main
```

Criterion reports the percentage change and confidence interval per shape. A change inside the
noise threshold is not an improvement.

## Correctness is non-negotiable

The output of the optimized function must be **bit-identical** to `main` for every input:
same values, same null buffer, same errors. Comet does not ship a differential fuzz harness in
this repo, so the existing unit tests are your correctness gate. If coverage is thin, add tests
(or run `audit-comet-expression`) before optimizing.

Recurring correctness traps:

- **Null slots must not raise errors.** In ANSI/overflow paths, only apply the fallible
  conversion to non-null slots. Arrow's `try_unary` does this for you: a garbage value sitting
  under a null cannot raise a spurious overflow. Hand-rolled loops that read the value before
  checking the null bit will report errors Spark does not.
- **Preserve the exact eval-mode semantics.** For example, Legacy narrowing int casts keep the
  low-order bits (a wrapping `as` conversion), while ANSI raises on overflow. These are different
  kernels, not a single path with a flag.
- **Carry the null buffer through untouched** when the values transform is infallible and
  one-to-one. `unary`/`try_unary` do this; a rebuild-from-iterator does not, and can drop or
  shift nulls.
- **Zero-copy buffer reuse must respect offsets and slicing.** Reusing an input array's buffers
  is only valid when the layout genuinely matches the output.

## Proven techniques

These are the patterns that have produced real speedups in merged and in-flight Comet PRs. Reach
for the lightest one that fits.

| Technique                                                          | What it replaces                                                                                      | Example                                                                                                                                                                               |
| ------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Vectorized Arrow kernels** (`unary`, `try_unary`, `binary`)      | `iter().map().collect()` over `Option`/`Result`                                                       | `spark_cast_int_to_int` (up to 100x): map the values buffer in one pass, carry the null buffer over                                                                                   |
| **`unary_opt` for null-producing conversions**                     | A builder loop that appends null on overflow, or `unary` + sentinel + `null_if_overflow` (two passes) | int/float-to-decimal casts (28-50%): map overflow to null in one vectorized pass; for the ANSI throw-on-overflow variant, gate a rare element-wise rescan on an O(1) null-count check |
| **Zero-copy borrow with `Cow`**                                    | Allocating a new `String`/`Vec` per row when most rows are unchanged                                  | `to_json` `escape_string` (2x): borrow when nothing needs escaping, bulk-copy unescaped byte runs                                                                                     |
| **Zero-copy buffer reuse**                                         | Copying every value into a fresh builder                                                              | `cast_binary_to_string` default path (up to 7000x): reuse the binary array's buffers instead of copying                                                                               |
| **Preallocate builders to known size**                             | Repeated buffer growth/reallocation                                                                   | `spark_unhex`: preallocate `BinaryBuilder` to the known output length                                                                                                                 |
| **Compile-time lookup tables**                                     | Per-element range matches / branching                                                                 | `spark_unhex`: 256-entry hex table instead of per-digit range match                                                                                                                   |
| **Cache compiled regex** (thread-local, keyed by the constant arg) | `Regex::new()` per row                                                                                | `parse_url` QUERY-with-key (50x): the key is constant across a batch                                                                                                                  |
| **Read from the offset buffer directly**                           | `list_array.value(i)` allocating a sliced `ArrayRef` per row                                          | `spark_size`: compute list lengths from offsets, zero allocation                                                                                                                      |
| **Typed scans over flat values buffers + hash probe**              | A per-element Arrow `eq`/compute kernel that allocates per call                                       | `spark_arrays_overlap` (up to 18x): scan buffers directly, hash probe for large lists                                                                                                 |
| **ASCII / byte-offset fast path**                                  | `chars().count()` and per-char UTF-8 decoding                                                         | `substring` (up to 10x), `spark_lpad` (2x): slice by byte offset when input is ASCII                                                                                                  |
| **`memcpy` from a precomputed buffer**                             | Char-by-char `push` into a scratch `String`                                                           | `spark_lpad`: pad from a precomputed repeating pad buffer, write directly into the builder                                                                                            |

Cross-cutting principles behind the table:

- **Hoist work that is constant across the batch** out of the per-row loop (regex compilation,
  format setup, capacity that depends only on the whole column).
- **Avoid per-row heap allocation.** Reuse a scratch buffer, or write directly into the output
  builder.
- **Prefer bulk operations** (bulk-copy a run, one `extend` per contiguous run) over per-element
  operations, but verify the dense-null case does not regress.
- **Watch capacity hints.** A `rows * rows` capacity hint (instead of `rows`) silently
  over-allocates; fix these when you find them.

## The no-regression rule

**Do not submit an optimization if any benchmark shape is meaningfully slower**, even if another
shape is dramatically faster. A change that is 90% faster with no nulls but 30% slower with dense
nulls is a trade-off, not a win, and should either be gated behind a runtime check that picks the
right path per batch or not submitted at all. A Comet optimization PR was closed for exactly this
reason (a 30% dense-null regression alongside a 90% no-null win).

## Record the performance audit

Once a tuning change is merged, record it in the per-expression
[Expression Audits](expression-audits/index.md) so contributors can see which expressions have
already been optimized and avoid re-treading the same ground. Add a dated `Performance (tuned ...)`
line under the expression's heading on the relevant category page (create the heading if the
expression has no audit entry yet):

```markdown
## unhex

- Performance (tuned 2026-07-11, PR #4876): compile-time 256-entry hex lookup table plus
  preallocated `BinaryBuilder`; up to 31% faster on long strings. Benchmark: `benches/unhex.rs`.
```

Name the technique, the measured speedup, the PR, and the benchmark file. Keep it to one line
per tuning pass so the history stays scannable.

## PR conventions

Optimization PRs are small and follow a consistent shape:

- Title: ``perf: optimize `function_name` (Nx faster)`` with the headline speedup.
- Rationale: "Optimize existing expression."
- Changes: one or two sentences naming the specific technique and what per-row cost it removes.
- Testing: "Existing tests." plus the pasted criterion output for every shape.
- Keep the diff focused on one expression. Do not bundle unrelated changes.

Follow the standard [Pre-PR checklist](development.md): `make format`, build, and
`cargo clippy --all-targets --workspace -- -D warnings`.
