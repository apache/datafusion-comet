---
name: review-comet-ffi-pr
description: Use when reviewing a DataFusion Comet pull request that crosses the JVM/native boundary, touching Arrow C Data or C Stream interface code, batch export and import, CometExecIterator, ScanExec, NativeUtil, CometVector subclasses, or jni_api. Load alongside review-comet-pr.
argument-hint: <pr-number>
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

FFI-specific review for Comet PR #$ARGUMENTS.

**REQUIRED BACKGROUND:** Use `review-comet-pr` for PR metadata, existing comments, CI, the review
bar, and the output format. This skill only covers the JVM/native boundary.

Bugs in this area do not produce wrong answers, they produce segfaults, leaks, and use-after-free
under load, often only on one platform or only when an operator buffers batches. Review it with
that in mind.

## Read the Contributor Guide First

| Doc                                                  | What you need from it                                                       |
| ---------------------------------------------------- | --------------------------------------------------------------------------- |
| `docs/source/contributor-guide/ffi.md`               | Both data-flow directions, ownership rules, lifecycle, offset normalization |
| `docs/source/contributor-guide/memory_management.md` | The "Crossing the FFI boundary" section: who is charged for a batch's bytes |

Read `ffi.md` in full before the diff. The two directions have different ownership semantics and
reviewing one with the other's mental model is the most common way to miss a bug.

## The Two Directions

| Direction                           | Mechanism                                  | Who owns the data                                                  |
| ----------------------------------- | ------------------------------------------ | ------------------------------------------------------------------ |
| JVM to native (`ScanExec`)          | Arrow C **Stream**, one per partition      | Native takes ownership by reference count when it imports a batch  |
| Native to JVM (`CometExecIterator`) | Arrow C **Data**, one array pair per batch | Native allocates, JVM holds pointers and must `close()` to release |

## 1. Ownership and Lifetime

- [ ] **JVM to native: no defensive deep copies.** The C Stream transfers ownership by reference
      count, so native can buffer imported batches in `SortExec` or the shuffle writer without
      copying. A new `.clone()` of the data, a `MutableArrayData` copy, or a "to be safe" deep copy
      on this path is a real throughput cost. Ask what it is protecting against.
- [ ] **JVM to native: dropping the reader is the release.** When `ScanExec` drops its
      `ArrowArrayStreamReader`, the stream's release callback fires synchronously back into the
      JVM and closes the `ArrowReader` and its `VectorSchemaRoot`. Anything that extends the
      reader's lifetime, stores it somewhere longer-lived, or drops it early changes when JVM
      off-heap buffers are freed.
- [ ] **JVM to native: buffering pins JVM memory.** An operator that holds many imported batches
      keeps the corresponding JVM-side off-heap buffers alive. A change that makes an operator
      buffer more is also a memory change.
- [ ] **Native to JVM: every export needs a matching close.** Native allocates, the JVM wraps the
      pointers in `ArrowBuf`s, and the bytes are freed only when the JVM calls `close()`. Trace the
      new export to the `close()` that releases it, including on the exception path.
- [ ] **Release callbacks run on the error path too.** A batch exported and then abandoned because
      the query failed still has to be released. Check the failure and cancellation paths, not just
      the happy path.
- [ ] **No unwinding across `extern "C"`.** A Rust panic crossing the FFI boundary is undefined
      behavior. New `#[no_mangle] extern "system"` entry points must not let a panic escape, and
      must not `unwrap()` on anything an input can make fail.
- [ ] **Null and error checks on every pointer received from the other side.**

## 2. The Stream Design

The JVM exports each per-partition iterator **once** as an `ArrowArrayStream`, and native pulls
every batch through the stream's `get_next` callback. There is no per-batch JNI call and no
per-column FFI export on this path.

A change that reintroduces per-batch or per-column export on the JVM-to-native path is a
performance regression even if it is correct. Flag it and ask why the stream could not carry it.

The reader implementations in `CometNativeArrowSource.scala` are `RowArrowReader` for
`Iterator[InternalRow]`, `SparkColumnarArrowReader` for a non-Arrow `ColumnarBatch`, and
`ColumnarBatchArrowReader` for an Arrow-backed `ColumnarBatch`, which transfers `VectorSchemaRoot`
ownership. A new input shape needs a reader, not a special case elsewhere.

## 3. Vector Types and Export Dispatch

`NativeUtil.exportBatch()` matches on the column type and has exactly two cases. A `CometVector`
exports its underlying `FieldVector` through `Data.exportVector`, passing the dictionary provider
only when the field carries a dictionary. Spark's own `ConstantColumnVector` is materialized into a
fresh Arrow vector first, because native takes Arrow arrays only. Anything else throws
`"Comet execution only takes Arrow Arrays"`.

The `CometVector` hierarchy is the abstract `CometVector`, the abstract `CometDecodedVector` under
it, and the concrete `CometPlainVector`, `CometDictionaryVector`, `CometListVector`,
`CometMapVector`, and `CometStructVector`. All of them export through the one `CometVector` case, so
a new subclass needs no new case as long as `getValueVector` returns an Arrow vector.

- [ ] A new column type that is **not** a `CometVector` needs its own case, and needs to say who
      owns the vector it materializes. The `ConstantColumnVector` case allocates a new Arrow vector
      per batch, which is a real cost on a hot path.
- [ ] A new `CometVector` subclass whose `getValueVector` is synthesized rather than owned has a
      defined lifetime relative to the export.
- [ ] Import is symmetric. `ScanExec::pull_next` runs every imported column through
      `import_column`, which decodes invalid UTF-8 to Spark's rendering and otherwise keeps the
      imported buffers. A PR that adds a column type has to say what that step does to it.
- [ ] The row-count check still holds. `exportBatch` requires every column to report the same value
      count and throws otherwise, which is the guard that catches a vector exported at the wrong
      length.

## 4. Alignment, Offsets, and Schema

- [ ] **JVM to native: arrow-rs does the alignment.** Java's allocator hands back `Decimal128`
      buffers at 8-byte rather than 16-byte alignment. Since arrow 59, `from_ffi` and
      `from_ffi_and_data_type` realign them on import
      ([arrow-rs#10030](https://github.com/apache/arrow-rs/pull/10030)), so `ScanExec` reads the
      stream with the stock `ArrowArrayStreamReader`. The `realigns_under_aligned_decimal128` test
      in `scan.rs` guards this. A PR that downgrades arrow, or that imports through anything other
      than those two functions, must keep that test passing.
- [ ] **Native to JVM: exported offsets must be zero.** Arrow Java ignores `ArrowArray.offset` at
      every level on import, so every array native exports to the JVM first goes through
      `zero_offsets` (in `native/common/src/ffi_offsets.rs`). `move_to_spark` applies it to
      executed batches and decoded shuffle blocks, and `JvmScalarUdfExpr` to the inputs of the
      JVM UDF bridge. A new export path has to call it too, or a sliced boolean, top-level or
      nested, reaches the JVM misaligned
      ([#6288](https://github.com/apache/datafusion-comet/issues/6288)).
- [ ] **Schema reconciliation stays truthful.** `CometArrowStream.reconcileStreamSchema` advertises
      the stream's schema from the actual `CometVector` types in the first batch rather than the
      consumer's Spark-declared types, so that the cast in native `build_record_batch` fires. A PR
      that changes either side needs to change both, and a new "just declare the Spark type" path
      quietly reintroduces the drift this exists to handle.
- [ ] **Dictionaries are decoded on the JVM.** No input stream carries a dictionary:
      `ColumnarBatchArrowReader` decodes dictionary-encoded columns before export, and
      `reconcileStreamSchema` advertises the value type. A new reader that exports a dictionary
      would reach `ScanExec` as is, and only the cast in `build_record_batch` would unpack it.
- [ ] **Timestamps cross unconverted.** Both directions pass the raw microseconds. JVM producers
      label `TimestampType` with `CometArrowStream.NATIVE_TIMEZONE`, which is `"UTC"`, and a new
      producer must use it too rather than the session timezone. A change that shifts values by a
      timezone at the boundary gives wrong answers. See "How Comet represents timestamps" in
      `docs/source/contributor-guide/timezones.md`.

## 5. Memory Accounting

An FFI change is usually also a memory change, and the two are easy to review separately and miss
the interaction. Imported JVM buffers come from `CometArrowAllocator`, which is an unbounded
`RootAllocator(Long.MaxValue)` that no budget sees. Exported native batches are usually no longer
pool-reserved by the time the JVM receives them, but they stay resident until the JVM closes them.

If the PR changes how long either side holds a batch, use `review-comet-memory-pr` as well.

## 6. Tests

FFI bugs rarely reproduce in a small unit test. Ask what evidence the PR offers:

- Does an existing suite exercise the new path with a non-trivial number of batches, rather than
  one batch that happens to work?
- Are nested and dictionary-encoded types covered, since they take different import paths?
- Are Decimal128 columns covered, since they need an alignment that Java's allocator does not
  guarantee?
- On the native to JVM side, are sliced inputs covered, since Arrow Java ignores a non-zero
  offset?
- For a lifecycle change, is there a test that closes or cancels early?
- Rust tests in `native/core` need `DYLD_LIBRARY_PATH=$JAVA_HOME/lib/server` on macOS and
  `LD_LIBRARY_PATH` on Linux. If the PR adds one that needs the JVM, check CI actually runs it.

## 7. Does the PR Make `ffi.md` Stale?

`ffi.md` documents mechanism, so mechanism changes invalidate it. Check specifically:

- The **Memory Ownership Rules** tables at the end, one per direction. A change to who owns or who
  frees rewrites a row.
- The **JVM to Native** and **Native to JVM** architecture diagrams, if a stage is added, removed,
  or renamed.
- The list of `ArrowReader` implementations, if the PR adds an input shape.
- The **Array Offsets** section, if the PR adds a native to JVM export path or changes what
  `zero_offsets` does.
- The **Schema Reconciliation** and **Ownership and Lifecycle** paragraphs, which state invariants
  rather than describing code. These are the easiest to leave quietly wrong.
- Class and file paths named in the prose, if the PR moves anything.

Check also whether the "Crossing the FFI boundary" section of `memory_management.md` still holds,
since it describes which operators reserve for imported batches by name.
