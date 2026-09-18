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
| `docs/source/contributor-guide/ffi.md`               | Both data-flow directions, ownership rules, lifecycle, alignment workaround |
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
      copying. A new `.clone()` of the data, a `copy_array`, or a "to be safe" deep copy on this
      path is a real throughput cost. Ask what it is protecting against.
- [ ] **JVM to native: dropping the reader is the release.** When `ScanExec` drops its
      `AlignedArrowStreamReader`, the stream's release callback fires synchronously back into the
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

`NativeUtil.exportBatch()` matches on the concrete vector type. The `CometVector` hierarchy is
`CometDecodedVector` with Plain, Dictionary, List, Map, and Struct subclasses, plus
`CometSelectionVector` and `CometDelegateVector`.

- [ ] A new `CometVector` subclass has a case in `exportBatch()`
- [ ] The case ordering is right. `CometSelectionVector` must be matched **before** the general
      `CometVector` case, or the selection is silently dropped and the exported batch has the wrong
      rows.
- [ ] Selection vectors are applied where `scan.rs` expects them, in `ScanExec::get_next()`

## 4. Alignment and Schema

- [ ] **`AlignedArrowStreamReader` is not dead code.** It calls `align_buffers` on every batch
      because Java's allocator hands back `Decimal128` buffers at 8-byte rather than 16-byte
      alignment, which the stock `ArrowArrayStreamReader` rejects
      ([arrow-rs#10028](https://github.com/apache/arrow-rs/issues/10028)). It can only be replaced
      with the stock reader once Comet is on arrow 59 or newer, where
      [arrow-rs#10030](https://github.com/apache/arrow-rs/pull/10030) aligns on import. If the PR
      removes it, check the arrow version in `native/Cargo.toml` actually supports that.
- [ ] **Schema reconciliation stays truthful.** `CometArrowStream.reconcileStreamSchema` advertises
      the stream's schema from the actual `CometVector` types in the first batch rather than the
      consumer's Spark-declared types, so that the cast in native `build_record_batch` fires. A PR
      that changes either side needs to change both, and a new "just declare the Spark type" path
      quietly reintroduces the drift this exists to handle.
- [ ] **Dictionary handling.** Import uses `CopyMode::UnpackOrClone`: dictionary columns are
      unpacked into new native arrays, everything else is an `Arc` clone. A change here affects both
      correctness and allocation volume.

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
- Are Decimal128 columns covered, since that is what the alignment workaround exists for?
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
- The **Buffer Alignment** section, which describes `AlignedArrowStreamReader` as a temporary
  workaround with a stated exit condition. If the PR removes the reader or bumps arrow past 59,
  that section has to go or change.
- The **Schema Reconciliation** and **Ownership and Lifecycle** paragraphs, which state invariants
  rather than describing code. These are the easiest to leave quietly wrong.
- Class and file paths named in the prose, if the PR moves anything.

Check also whether the "Crossing the FFI boundary" section of `memory_management.md` still holds,
since it describes which operators reserve for imported batches by name.
