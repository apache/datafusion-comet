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

# Working with Arrow in Native Code

This page covers two decisions that come up in almost every native change: whether to write
array-manipulating code at all, and how to write it correctly when you must. Both are places
where Comet has repeatedly gotten it wrong. An audit of the native crates
([epic #5104](https://github.com/apache/datafusion-comet/issues/5104)) filed sixteen follow-ups
for hand-rolled logic that duplicates an existing arrow-rs or DataFusion implementation, two of
them carrying real correctness bugs that the upstream implementation does not have.

The rules below are grounded in specific files and issues. Where a rule cites a file, read that
file before relying on the summary here.

## Check for an existing kernel first

Before writing a loop over an Arrow array, work down this list and stop at the first hit:

1. **arrow-rs compute kernels.** The workspace is on arrow `58.4.0` (`native/Cargo.toml`). Kernel
   availability claims on this page are specific to that version. Check
   `arrow::compute::kernels` and the type-level helpers on `PrimitiveArray` before anything else.
2. **DataFusion.** If arrow has no kernel for the shape, DataFusion may already implement the
   function or a close relative.
3. **The `datafusion-spark` crate.** Comet depends on `datafusion-spark` `54.1.0`
   (`native/Cargo.toml`) and registers its functions in
   `native/core/src/execution/jni_api.rs`, which currently imports 33 of its functions. If the
   function you need exists upstream, wire it instead of writing it. See
   [Adding a New Expression](adding_a_new_expression.md).
4. **Write it in Comet**, and only then.

Run this check even when you are touching code that already exists. Some of Comet's hand-rolled
code predates the public availability of the API it duplicates, so the duplication was correct
when it was written and is not correct now. Verified examples:

- `native/spark-expr/src/timezone.rs` is a copy of arrow-array's `Tz` and `TzOffset`. Its own
  header says "This is basically from arrow-array::timezone (private)". In arrow 58 that module
  is public, and with the `chrono-tz` feature the workspace already enables it resolves named
  zones. `native/spark-expr/src/kernels/temporal.rs` already imports arrow's version. The crate now
  carries two parallel, incompatible `Tz` types
  ([#5088](https://github.com/apache/datafusion-comet/issues/5088)).
- `is_valid_decimal_precision` in `native/spark-expr/src/utils.rs` is a copy of arrow's
  implementation. Its comment says to remove it "once we upgrade to a version of arrow-rs that
  includes https://github.com/apache/arrow-rs/pull/6419". That PR shipped well before arrow 58
  ([#5089](https://github.com/apache/datafusion-comet/issues/5089)).
- `native/spark-expr/src/datetime_funcs/date_from_unix_date.rs` reinterprets Int32 as Date32 by
  hand, and `native/spark-expr/src/utils.rs` converts milliseconds to microseconds with
  `arity::unary(millis_array, |v| v * 1000)`. Arrow's `cast` kernel covers both; see
  [#5090](https://github.com/apache/datafusion-comet/issues/5090) for the equivalence argument and
  the timezone caveats. Note that the two are not equivalent on overflow: the plain `unary`
  multiply wraps, while arrow's timestamp unit cast uses `unary_opt` with `checked_mul` in safe
  mode (null on overflow) and `try_unary` with `mul_checked` otherwise (error). Confirm which
  behavior you want before swapping a loop for the kernel.

If you decide to keep a local implementation because the upstream one is wrong, record why in the
file, with a pointer to the upstream issue and the condition under which the file can be deleted.
`native/spark-expr/src/array_funcs/array_slice.rs` is the model: its header states the exact
behavioral difference from `datafusion-spark`'s `SparkSlice`, the upstream version it was checked
against, and that the file can be dropped once upstream is fixed.

### Where Comet diverges from arrow on purpose

Much of Comet's native code looks like a reimplementation of an arrow kernel but is not one. It
implements Spark semantics that arrow deliberately does not have: ANSI errors, HALF_UP rounding,
Java string formats, Spark timezone rules, and Spark-mandated byte formats. The #5104 audit
examined each of the following and confirmed it is intentional. These are settled. Do not re-raise
them in review, and do not "simplify" them into an arrow call without new evidence.

- **Cast string parsing and formatting** (`conversion_funcs/string.rs`, float and decimal to
  string): ports of `UTF8String.toInt`, `stringToDate`, `stringToTimestamp`, Java
  `Double.toString` and `BigDecimal.toString`. `arrow_cast::parse` and the arrow display
  implementations are not Spark-compatible for any of these.
- **Legacy wrap-around int casts, seconds-based numeric to timestamp, and NTZ/TZ timestamp
  handling**: arrow either has no wrapping mode, interprets values in target-unit ticks, or
  wall-clock-adjusts where Spark needs a relabel or a session-timezone conversion under Spark's
  DST rules.
- **Shuffle byte formats**: whole-stream compression framing (arrow IPC per-buffer compression is
  a different byte format and has no Snappy), the pre-encoded schema fast path (byte-identical to
  `StreamWriter`, exists to avoid re-encoding), radix sort over packed record pointers on the JVM
  shuffle path (not over arrow arrays), and counting-sort partition grouping (linear versus the
  kernel route's `n log n`). Row gather already uses `interleave_record_batch`.
- **columnar_to_row**: the `UnsafeRow` and `UnsafeArrayData` writers emit Spark's byte format.
  `arrow_row::RowConverter` produces arrow's row format and is not applicable.
- **Aggregates**: decimal sum and avg per-row overflow checks (batch-level checking would change
  which inputs go null), Spark ROUND_HALF_UP average, Welford recurrences, and
  percentile/HLL++/QuantileSummaries, which are Spark sketch formats. `GroupsAccumulator` per-group
  loops stay because arrow has no grouped kernels.
- **Hashes and bloom filter**: seed-chained murmur3 and xxhash64 dispatch, plus the Spark
  `BitArray` and bloom serialization formats, are Spark-mandated.
- **String functions with Spark grammars**: base64 (MIME 76-character CRLF chunking), the
  `regexp_extract` family (Spark group, no-match, and error semantics plus all-matches extraction
  that arrow lacks), `split` (Spark limit semantics), read-side padding (arrow has no lpad/rpad;
  this is a deliberate performance design), `is_nan` (null to false is Spark-specific), and
  `get_json_object` (Spark JSONPath).
- **Math type-widening and rounding**: abs, ceil, floor, round, decimal division, and wide decimal
  binary operations have no arrow kernel for the shape, or need Spark's type-widening and rounding
  rules. `checkoverflow` needs the offending value for its ANSI error text. `normalize_nan` and
  `unscaled_value` have no arrow equivalent. The array paths already use arity helpers.
- **Datetime grammars and floor division**: `unix_timestamp` needs floor division where arrow's
  cast truncates; `hours`, `seconds_to_timestamp`, `make_date`, `make_time`, `next_day`, `to_time`,
  and the day and month name functions carry Spark grammars, error texts, and locale tables.
  `extract_date_part` already uses arrow `date_part`.
- **Copy and scan operators**: these are already built on `MutableArrayData` and
  `cast_with_options`. Their extra copies exist to break FFI buffer ownership, not by oversight.
  See [Arrow FFI](ffi.md).

## Using Arrow APIs correctly

### Values under null slots are arbitrary

A null slot's underlying value buffer holds whatever bytes are there: zero, a leftover from
before a filter or a slice, or data that arrived over FFI. Nothing guarantees it is a benign
value. Two consequences:

**Fallible logic must consult the validity buffer before reading the value.** Comet has a live
bug from ignoring this. The `check_overflow!` macro in
`native/spark-expr/src/math_funcs/negative.rs` loops `for i in 0..typed_array.len()` and compares
`typed_array.value(i)` against the type's `MIN` with no validity check, so an ANSI-mode negation
raises `ARITHMETIC_OVERFLOW` when a `MIN` value happens to sit under a null slot. Spark returns
null for that row ([#5093](https://github.com/apache/datafusion-comet/issues/5093)).

`native/spark-expr/src/math_funcs/checked_arithmetic.rs` shows the correct shape, and the shape is
not "add a validity check to the loop". `checked_binary` returns early for the ANSI path at lines
64 to 72, delegating to `arrow::compute::kernels::arity::try_binary` and mapping the resulting
`ArrowError` back to a Spark error. The kernel is what guarantees the null behavior: when either
input has a non-zero null count, `try_binary` unions the null buffers and drives the closure
through `nulls.try_for_each_valid_idx`, so `op` is never applied to a null slot. The behavior is
pinned by `test_null_row_with_garbage_value_does_not_error_in_ansi_mode` in the same file. The
hand-rolled loop below the early return is the non-ANSI path, where an overflow marks the row null
instead of raising.

The fix proposed for #5093 is the same move: replace the manual scan with arrow's checked `neg`,
which skips invalid slots. Prefer delegating the fallible path to a kernel over guarding a loop by
hand, and add an equivalent garbage-value-under-a-null test whenever you write one.

**Arrow's infallible arity helpers do not save you.** In arrow 58.4.0, `unary` and `binary`
evaluate the closure on every element including nulls, by design: the arrow docs state the cost of
the operation is lower than the cost of branching, and require `op` to be infallible for all
possible input values. `try_unary` is the one that applies the closure only to valid rows
(`PrimitiveArray::try_unary` iterates via `try_for_each_valid_idx`). Use `try_unary` for anything
that can error, and never put a panicking or erroring operation inside `unary`.

### Rebuilding a nested array means rebuilding every null buffer in it

When you construct a nested array by hand rather than letting a kernel do it, each level of the
structure has its own validity, and each one has to be carried explicitly. Carrying only the
outermost one silently drops data.

`cast_map_to_map` in `native/spark-expr/src/conversion_funcs/cast.rs` does exactly this. It passes
`map_array.nulls().cloned()` when it builds the output `MapArray`, which is correct, but a few
lines earlier it passes `None` for the entries `StructArray`'s null buffer, dropping
`map_array.entries().nulls()`. It also propagates the source's `sorted` flag rather than the
target's ([#5097](https://github.com/apache/datafusion-comet/issues/5097)). Arrow's Map-to-Map
`cast_with_options` carries the entries validity through (`cast/map.rs` passes
`from.entries().nulls().cloned()`), and it refuses a sortedness change outright rather than
silently taking the source's: `cast/mod.rs` matches Map to Map only when the two `ordered` flags
are equal, in both `can_cast_types` and the cast dispatch. That is the other reason to prefer the
kernel: it carries structure you may not have thought about, and rejects what it cannot express.

The general rule follows from the first section. Prefer the kernel; reach for a hand-built array
only when Spark semantics require it, and when you do, enumerate every null buffer and every
metadata flag in the type you are constructing and account for each one.

## Further reading

- [Arrow FFI](ffi.md) for buffer ownership across the JVM and native boundary, and why some
  operators copy deliberately.
- [Optimizing Scalar Expressions](optimizing_expressions.md) for the benchmark and no-regression
  rules that apply when you replace hand-rolled code with a kernel for speed.
- [Epic #5104](https://github.com/apache/datafusion-comet/issues/5104) for the full audit,
  including the open issues for each duplicated site.
