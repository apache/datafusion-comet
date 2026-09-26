---
name: review-comet-iceberg-write-pr
description: Use when reviewing a DataFusion Comet pull request that touches Iceberg writes, the split-operator write plan (IcebergWriteStrategy, IcebergWriteExec, IcebergCommitExec), the native Iceberg writer (CometIcebergWriteExec, iceberg_write.rs, iceberg_partition_path.rs), its eligibility gate (CometIcebergNativeWrite), or the iceberg-rust pin. Load alongside review-comet-pr.
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

Iceberg-write-specific review for Comet PR #$ARGUMENTS.

**REQUIRED BACKGROUND:** Use `review-comet-pr` for PR metadata, existing comments, CI, the review
bar, and the output format. This skill only covers Iceberg writes.

## Read the Contributor Guide First

| Doc                                                    | What you need from it                                                                   |
| ------------------------------------------------------ | --------------------------------------------------------------------------------------- |
| `docs/source/contributor-guide/iceberg-writes.md`      | The two layers, the gate's properties, the wire format both ways, cleanup ownership     |
| `docs/source/user-guide/latest/iceberg-writes.md`      | The eligibility table and the accepted divergences from iceberg-java, as users see them |
| `docs/source/contributor-guide/iceberg-spark-tests.md` | What the Iceberg Spark test diffs enable, and what a green run does and does not prove  |

The organizing rule is that **the native writer produces what iceberg-java would have produced, or
declines at plan time.** Review every change against it. Iceberg write bugs are quiet: a divergent
data file or manifest entry commits successfully, and every later reader of the table, Comet or
not, inherits it. There is no query that fails to tell you.

## 1. Which Layer

| Layer                | Flag                                              | Code                                                                                                                         |
| -------------------- | ------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| Split-operator plan  | `spark.comet.write.iceberg.splitOperator.enabled` | `IcebergWriteStrategy`, `IcebergWriteLogical`, `IcebergWriteExec`, `IcebergCommitExec`, the `spark-*/.../iceberg/` shims     |
| Native writer        | `spark.comet.iceberg.write.enabled`               | `CometIcebergNativeWrite` (gate and serde), `IcebergWriteProtoTranslation`, `CometIcebergWriteExec`, `iceberg_write.rs`      |
| Shared with the scan | both                                              | `iceberg_common.rs` (`load_file_io`, `storage_factory_for`, `scheme_of`), `IcebergReflection`, `NativeConfig` S3 translation |

A change to the split plan affects every Iceberg write once that flag is on, including writes that
never reach the native writer. A change to a shared file affects the native Iceberg scan too.

## 2. Direction of the Gate Change

Establish first whether the PR widens or narrows what `CometIcebergNativeWrite.getSupportLevel`
admits. The two need different evidence.

- **Narrowing** (a new rule, a smaller allow list) costs performance only. Check that the rule
  reports a specific reason and has a `CometIcebergWriteDetectionSuite` case.
- **Widening** (lifting a rule, vetting a new property, a new scheme or type) means Comet now claims
  writes it previously refused. "The tests pass" is not evidence here, because the existing tests
  were written against the narrower gate. Ask for a test that writes the newly admitted
  configuration through both writers and compares the result, and for the user guide's eligibility
  table to change in the same PR.

For every gate change:

- [ ] **It stays an allow list.** New support is added by vetting a key or value, not by widening a
      prefix or turning a deny list around. Unknown keys in `write.parquet.*`, `parquet.*`, the
      session Hadoop `parquet.*` keys, and any other namespace that reaches the writer must still
      decline.
- [ ] **It reads the effective configuration** (`TriggerContext.properties`, table properties
      overlaid with `SparkWrite.writeProperties`), not the table properties alone.
- [ ] **It fails closed.** A reflection lookup that cannot answer returns a reason. Look for
      `getOrElse(None)`-shaped code, or a `catch` that returns `None`, in a new rule.
- [ ] **It agrees with the native side.** If the gate and the native code both interpret the same
      input (a location's scheme, a partition spec, a type, a property value), check that they
      interpret it identically on the edge cases: case, `scheme:/path` versus `scheme://path`,
      whitespace, `void` partition fields, missing source columns. A gate more permissive than the
      native code turns a fallback into a failed query
      ([#6140](https://github.com/apache/datafusion-comet/issues/6140)).
- [ ] **Settings the native writer cannot honour decline.** A table, `FileIO` or Hadoop setting
      that iceberg-java would apply and the native writer ignores produces a successful write that
      silently differs: credentials, encryption, ACLs, tags, storage class
      ([#6139](https://github.com/apache/datafusion-comet/issues/6139)). The `gs://` path is the
      model ([#5637](https://github.com/apache/datafusion-comet/issues/5637)).
- [ ] **New executor-side reflection is probed on the driver.** Any iceberg-java member the task
      closure or `IcebergReflection`'s commit-message assembly now calls must be added to what
      `requireExecutorReflectionResolvable` resolves, so a missing member is a plan-time fallback.

## 3. Output Parity with iceberg-java

For a change to the native writer, the metadata rebuild, or the pin, ask what iceberg-java writes
for the same input, and check it at the pinned Iceberg versions (1.5.2, 1.8.1, 1.10.0, 1.11.0), not
only the latest.

- [ ] **Logical content and manifest metadata match**: rows, partition values, record counts,
      metrics, spec id, sort order id. A difference here is a bug or a gate rule, never a new
      documented divergence.
- [ ] **Physical differences are justified.** A new file-level difference (encoding, footer
      metadata, roll points, file names) is added to the user guide's accepted-divergences list with
      the reason no reader can observe it.
- [ ] **Metrics stay iceberg-java's.** `rebuildDataFilesWithJavaMetrics` re-derives manifest
      metrics from each file's footer with `ParquetUtil.footerMetrics` and `MetricsConfig`. A PR
      that starts trusting iceberg-rust's metrics, or adds a native-tracked value beyond float and
      double NaN counts and bounds, needs a strong reason.
- [ ] **Value semantics match, not only formats.** iceberg-rust compares and hashes partition values
      with its own rules: `OrderedFloat` treats `-0.0` and `0.0` as equal where Java's
      `Float.compare` does not
      ([#6138](https://github.com/apache/datafusion-comet/issues/6138)). Check equality, hashing,
      ordering and rendering for float, double, timestamp, timestamptz, binary and decimal.
- [ ] **Partition paths use the Java renderers.** Directory names come from
      `CometLocationGenerator` / `partition_to_path` in `iceberg_partition_path.rs`, which follow
      iceberg-java's `partitionToPath`, with `java_float_string` for floats. A PR that calls
      iceberg-rust's `PartitionKey::to_path` or `format!` on a value reintroduces a fixed bug.
- [ ] **Nothing nondeterministic reaches the output.** Map iteration order leaks into file names,
      manifest order and the row order of unordered reads. The fanout arm sorts its `DataFile`s by
      path for this reason ([#5776](https://github.com/apache/datafusion-comet/issues/5776)).
- [ ] **Casts stay strict.** `decorate_batch_with_field_ids` casts with `safe: false`, so a type
      mismatch fails the task. A safe cast would write NULLs.

## 4. Failure Handling and Cleanup

Files must have exactly one owner at every moment. Read the ownership table in the contributor
guide before reviewing any change near `AbortOnDrop`, `TrackingLocationGenerator`,
`WrittenFileCleanup`, `drainNativePayload` or `IcebergCommitExec.collectAndCommit`.

- [ ] A new failure point between writing a file and the JVM taking the locations is covered by the
      native guard, including the path where the plan is dropped mid-write rather than returning an
      error.
- [ ] The locations column is still read, and ownership taken, **before** the manifest is decoded,
      so a decode failure still cleans up.
- [ ] Cleanup is best effort and never replaces the original exception (`addSuppressed`, logging).
- [ ] File names still carry the task attempt id, so a retried or speculative attempt cannot reuse
      another attempt's names.
- [ ] A job failure still aborts with the completed tasks' messages and deletes their files, and
      the commit path still hands Iceberg genuine `SparkWrite$TaskCommit` messages, so
      `SparkWrite.abort` keeps working.
- [ ] The user-visible exception type is still what Spark's own write path throws on each Spark
      version ([#6143](https://github.com/apache/datafusion-comet/issues/6143)).

## 5. Plan Shape

- [ ] **AQE off as well as on.** Transition and plan-shape changes must be tested with
      `spark.sql.adaptive.enabled=false`. Per-stage transition insertion with AQE on has hidden a
      real bug ([#5689](https://github.com/apache/datafusion-comet/issues/5689)), and Iceberg's own
      extension tests pick AQE at random per session, so an AQE-dependent bug shows up upstream as
      intermittent.
- [ ] **Transitions stay stripped the way they are.** `CometIcebergWriteExec` must not become a
      `ColumnarToRowTransition`, and the transition beneath it is removed in the separate pass in
      `EliminateRedundantTransitions`, before the main `transformUp`.
- [ ] **Exactly one commit.** The committer relies on `V2CommandExec` memoizing `run()` and on
      `IcebergWriteLogical` anchoring re-plans. A change here needs the AQE re-plan test to still
      pass.
- [ ] **Rules that rewrite plans keep the write node.** Anything that restores Spark operators from
      a Comet node's `originalPlan` must handle the write execs
      ([#5719](https://github.com/apache/datafusion-comet/issues/5719)).
- [ ] **The kill switch still works.** Code on the write path respects `spark.comet.enabled`
      ([#6142](https://github.com/apache/datafusion-comet/issues/6142)).
- [ ] **Credentials stay out of plan strings.** `CometIcebergWriteExec.stringArgs` must not print
      the proto; `catalog_properties` carries translated `fs.s3a.*` secrets.

## 6. The iceberg-rust Pin

`native/Cargo.toml` pins `iceberg` and `iceberg-storage-opendal` to a git revision.

- [ ] A pin bump is reviewed as a writer change: what changed upstream in the writers, `DataFile`,
      manifests, partition handling and storage, and does the PR run the write suites and the
      Iceberg Spark tests?
- [ ] Code that depends on iceberg-rust behaviour that is not an API, such as
      `clustered_write_err` matching the clustered writer's message text, still has the test that
      fails when upstream changes it.
- [ ] Nothing starts depending on a personal fork. A PR that needs an unmerged iceberg-rust change
      says so and is not merged until the pin moves to an upstream revision.

## 7. Tests

| Suite                               | Covers                                                                               |
| ----------------------------------- | ------------------------------------------------------------------------------------ |
| `CometIcebergWriteActionSuite`      | Both layers end to end, parity with iceberg-java, DML, failure cleanup, AQE re-plans |
| `CometIcebergWriteDetectionSuite`   | One case per eligibility rule                                                        |
| `CometIcebergSystemFunctionSuite`   | Native partition transforms keeping partitioned writes native                        |
| `CometIcebergRewriteActionSuite`    | `rewrite_data_files` through the split plan and the native writer                    |
| `IcebergWriteProtoTranslationSuite` | Property to `IcebergParquetWriteSettings` translation                                |
| Rust tests in `iceberg_write.rs`    | Rolling, fanout order, clustered checks, cleanup guard, manifest round trip          |
| `iceberg_partition_path.rs` tests   | Partition path rendering against iceberg-java                                        |
| `CometIcebergWriteBenchmark`        | Native versus iceberg-java throughput                                                |

Ask specifically:

- [ ] **Does the test prove the native writer ran?** Every fallback is silent to the query. A test
      that only compares results passes when both sides used iceberg-java. Look for
      `assertNativeWriteEngages` or a collected `CometIcebergWriteExec`.
- [ ] **Is the input native?** `withNativeEnabled` also sets `localTableScan`; a test that sets
      `spark.comet.iceberg.write.enabled` by hand and inserts `VALUES` usually tests the JVM writer.
- [ ] **Is the comparison against iceberg-java** (a sibling table written by the JVM writer) rather
      than hand-written expected values?
- [ ] **Enough partitions for an ordering bug?** Two partitions pass half the time; use eight or
      more.
- [ ] **Does a failure test check storage state**, the files under the data location against those
      the manifests reference, and not only the table contents?
- [ ] **Does it run on each Iceberg version the behaviour depends on?** Pull requests run only the
      default Spark profile. Reflection, metrics conventions and path spelling differ across
      Iceberg versions, so ask for the `run-iceberg-tests` label or a local run where they matter.
- [ ] **Is a green Iceberg Spark test run being cited as evidence?** It does not show which writer
      handled a test ([#6148](https://github.com/apache/datafusion-comet/issues/6148)).

## 8. Do the PR Changes Make the Docs Stale?

- **`contributor-guide/iceberg-writes.md`**: the component table and its paths, the gate
  properties, the proto description, the native adaptations list, the payload columns and the JVM
  steps after them, the cleanup ownership table, the test table, and the Iceberg version per Spark
  profile
- **`user-guide/latest/iceberg-writes.md`**: the eligibility table, the "falls back" lists, the
  failure-handling section, and the accepted-divergences list. A PR that lifts or adds a rule, or
  changes a divergence, must update these. A divergence the code has since fixed must be removed.
- **`user-guide/latest/iceberg.md`**: its unsupported-features list mentions writes
- **`contributor-guide/iceberg-spark-tests.md`**: the flags the diffs set
- `CometConf.scala` doc strings for the two flags, when defaults or scope change
