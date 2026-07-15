---
name: optimize-comet-expression
description: Use when optimizing the performance of an existing native scalar expression in the datafusion-comet-spark-expr crate (native/spark-expr/) — casts, string/JSON/array/math kernels that run per-row or per-batch. Covers benchmarking, keeping output bit-identical, and the no-regression gate. Not for adding new expressions (use implement-comet-expression) or wiring upstream functions (use wire-datafusion-function).
argument-hint: <expression-name>
---

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

Optimize the native `$ARGUMENTS` scalar expression in `native/spark-expr/`.

**The methodology, benchmark shapes, technique catalog, correctness traps, and PR conventions
live in one place for both humans and agents:
`docs/source/contributor-guide/optimizing_expressions.md`. Read it first — it is the source of
truth. This skill only adds the agent execution loop and the discipline that keeps a change from
being a no-op or a regression.**

## Execution loop

1. **Read** `docs/source/contributor-guide/optimizing_expressions.md`.
2. **Locate** the implementation under `native/spark-expr/src/` and identify the per-row cost
   (allocation, kernel dispatch per element, UTF-8 decoding, regex compilation, bitmap reads).
3. **Baseline first.** Add or extend a criterion benchmark in `native/spark-expr/benches/`
   (register it in `native/spark-expr/Cargo.toml`), covering the shapes from the guide:
   no-null / sparse-null / dense-null, short / long, valid / invalid, ASCII / non-ASCII. Run it
   on `main` before touching any source:
   ```sh
   cd native && cargo bench --bench <name> -- --save-baseline main
   ```
   The baseline must be **built from unmodified source**. `cargo bench` recompiles from whatever
   is on disk when it runs, so if your optimization is already in the working tree (or a slow
   build is still compiling when you start editing), the "baseline" measures the optimized code
   and every speedup looks like zero. Only the benchmark file and `Cargo.toml` bench registration
   may be present. If you have already edited the expression, `git stash push -- <source file>`,
   capture the baseline, then `git stash pop`. Confirm the run finished before editing.
4. **Optimize**, preserving exact semantics. Pick a technique from the catalog in the guide.
5. **Prove correctness.** Run the existing unit tests for the function; they must pass unchanged.
   Output must be bit-identical to `main` (values, null buffer, errors). There is no differential
   fuzz harness in this repo — the unit tests are the gate. If coverage is thin, add tests (or run
   `audit-comet-expression`) before claiming correctness.
6. **Re-measure.** `cargo bench --bench <name> -- --baseline main`. Criterion's "change" compares
   two separate process runs, so a small (±a few %) flag on a shape can be cross-run system noise
   (thermal, background load), not a real effect. Before trusting any flagged regression, take a
   **second independent sample** (`--baseline main` again) — real changes reproduce, noise does
   not. A shape whose code path you did not touch cannot truly regress: if it flags, it is noise,
   and the fix is another sample, not abandoning the change.
7. **Apply the no-regression gate** (below) before writing any PR.
8. **Finish:** `make format`, build, `cargo clippy --all-targets --workspace -- -D warnings`.
   PR title ``perf: optimize `$ARGUMENTS` (Nx faster)``, paste the criterion output for every
   shape.
9. **Record the performance audit.** Add a dated `Performance (tuned ...)` line under the
   expression's heading on the relevant page in
   `docs/source/contributor-guide/expression-audits/` (naming the technique, speedup, PR, and
   benchmark file) so contributors can see what has already been tuned. Check this page before
   starting, too — it tells you whether the expression was already optimized.

## The gate: what blocks a submission

- **A shape got meaningfully and reproducibly slower.** Do NOT submit. Even a 90%-faster-no-nulls
  win does not justify a 30%-slower-dense-nulls loss. Either gate the fast path behind a per-batch
  runtime check that picks the right path, or abandon the change. A Comet PR was closed for exactly
  this. "Reproducibly" matters: confirm a flagged regression on a second sample first (step 6) —
  a ~2% flag on a code path you did not touch is noise, not a blocker.
- **No meaningful speedup on any shape.** There is nothing to submit. A change inside criterion's
  noise threshold is not an improvement.
- **Output differs from `main`** on any input, including null placement or error behavior.
- **You only benchmarked one shape.** You have not shown the absence of a regression.

## Red flags — STOP

- "It's obviously faster, I don't need to benchmark all shapes" → dense nulls / long values / the
  error path are where fast paths regress. Benchmark them.
- "The values are right, I'll assume nulls are fine" → null-buffer bugs are the most common
  correctness failure. Diff the null buffer.
- "I'll apply the fallible conversion to every slot" → a garbage value under a null must not raise
  an error Spark does not. Use `try_unary` or skip null slots.
- "One benchmark improved, ship it" → re-read the gate. One improvement plus one regression is not
  a win.

## Rationalization table

| Excuse | Reality |
| --- | --- |
| "Measuring the baseline is overhead, I'll benchmark once at the end" | Without a `main` baseline you cannot report a change %, and you cannot tell a win from noise. Baseline first. |
| "Dense-null regression is an edge case" | Real columns have dense nulls. A regression there is a regression. Gate the path or drop it. |
| "Existing tests are enough proof, no need to check the null buffer" | Tests may not assert null placement. Confirm bit-identical output explicitly. |
| "This trades a bit of the null case for a big win elsewhere" | That is a trade-off, not an optimization. Only submit a strict improvement (or a correctly-gated per-batch path). |
| "I'll edit the code, then run the baseline" | `cargo bench` compiles from disk. A baseline built with your change already applied measures the optimized code and hides the real speedup. Baseline on unmodified source; stash the edit if needed. |
| "One shape flagged a 2% regression, abandon it" | Criterion compares separate runs; small flags on an untouched code path are cross-run noise. Take a second sample before deciding — real regressions reproduce. |

## Related skills

- `audit-comet-expression` — shore up test coverage before optimizing when the gate is thin.
- `implement-comet-expression` — for a brand-new expression, not an existing one.
- `wire-datafusion-function` — for wiring an existing upstream function.
