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

# Iceberg Compaction

This document is for contributors who change how Comet runs Iceberg's `rewrite_data_files`
procedure with the `binpack` strategy. It covers how a `CALL` reaches Comet, which steps Iceberg
keeps and which Comet runs, how a call is declined, and how to test a change. For the user-facing
view (configuration, supported options, and when Comet leaves a call to Iceberg), see
[Compaction](../user-guide/latest/iceberg.md#compaction).

The rule that governs the whole feature is the one that governs the [Iceberg
writes](iceberg-writes.md): **Comet must produce the outcome iceberg-java would have produced, or
decline.** A call Comet does not fully reproduce runs Iceberg's own procedure, and it is decided
before anything is written.

## Overview

```text
CALL system.rewrite_data_files(...)
  -> optimizer rule swaps Iceberg's procedure for CometRewriteDataFilesProcedure   (Spark 4.x)
       -> CometRewriteDataFiles.prepare   Iceberg's planner plans the groups, Comet plans the jobs
            declined: Iceberg's procedure runs the call, nothing has been written
       -> PreparedRewrite.execute         run the jobs, commit
            job:     CompactionSource scan, one partition per bin    (CometIcebergNativeScan)
                     Iceberg rewrite write                           (CometIcebergWrite when eligible)
            commit:  Iceberg's RewriteDataFilesCommitManager on the driver
```

Iceberg's own procedure already runs in Comet as far as the data goes. Since
[#4251](https://github.com/apache/datafusion-comet/pull/4251), `CometScanRule` converts Iceberg's
`SparkStagedScan`, so every file group of Iceberg's `RewriteDataFilesSparkAction` is read by
`CometIcebergNativeScan`, and its rewrite write goes to the native Iceberg writer when that is
eligible. `CometIcebergRewriteActionSuite` covers that path. What Comet's compaction changes is the
jobs. Iceberg runs a Spark job per file group, `max-concurrent-file-group-rewrites` at a time, so a
table with many small partitions runs many small jobs and leaves most of the cluster idle. Comet
runs all the groups of a commit in one job. Everything else is Iceberg's own code:

| Step                     | Iceberg's `RewriteDataFilesSparkAction`                          | Comet                                      |
| ------------------------ | ---------------------------------------------------------------- | ------------------------------------------ |
| Plan the file groups     | `BinPackRewriteFilePlanner`                                      | The same planner                           |
| Plan the read of a group | `SparkStagedScan` calls `TableScanUtil.planTaskGroups`           | The same call, for each group              |
| Run the groups           | One Spark job per group                                          | One Spark job per commit, one task per bin |
| Commit                   | `RewriteDataFilesCommitManager`                                  | The same commit manager                    |
| Remove dangling deletes  | `RemoveDanglingDeleteFiles`, with `remove-dangling-deletes=true` | The same action, with the same option      |

| Component                            | Location                                                    | Role                                                                                                               |
| ------------------------------------ | ----------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| `CometIcebergProcedureRule`          | `spark/src/main/scala/org/apache/comet/iceberg/compaction/` | Optimizer rule. Looks at the root of every optimized plan for a `CALL`.                                            |
| `CometProcedureShim`                 | `spark-4.x/` and `spark-3.x/`, same package                 | Swaps the procedure on Spark 4. A no-op on Spark 3.                                                                |
| `CometRewriteDataFilesProcedure`     | `spark-4.x/`, same package                                  | The `BoundProcedure` Spark invokes. Decodes the arguments, asks for a plan, and falls back to Iceberg's procedure. |
| `CometRewriteDataFiles`              | `spark/src/main/scala/org/apache/comet/iceberg/compaction/` | The rewrite: the decision to run it, the jobs, the commits.                                                        |
| `CompactionOptions`                  | same                                                        | The options of the action that Comet acts on, and the check for options Comet does not reproduce.                  |
| `RewriteJobs`                        | same                                                        | How the groups are split into jobs. Pure Scala, unit tested on every Spark profile.                                |
| `CompactionSource`, `CompactionScan` | same                                                        | The Spark data source of a job. A scan whose partitions are the bins of its groups.                                |
| `IcebergRewriteReflection`           | same                                                        | Every call Comet makes on Iceberg. Comet does not link Iceberg, so these are reflective.                           |

## How a `CALL` reaches Comet

Spark 4 runs a `CALL` while it analyzes the statement. The `InvokeProcedures` rule optimizes the
`Call` node and then invokes the procedure it holds, and no physical operator exists. Resolving,
binding and invoking the procedure happen in one pass of the analyzer, so a resolution rule would
run too late. The optimizer is the one place between binding and invoking. `CometIcebergProcedureRule`
is injected with `injectOptimizerRule`, and replaces the `BoundProcedure` inside the
`ResolvedProcedure` when it is Iceberg's `RewriteDataFilesProcedure` in the `system` namespace and
`spark.comet.iceberg.compaction.enabled` is on.

The rule runs on every optimized plan, so it checks only the root, and the conf lookup happens after
the match.

Spark 3 is different. With Iceberg's SQL extensions the statement becomes Iceberg's own `Call`
logical node, which the extension's planner strategy turns into `CallExec`. Intercepting it needs a
planner strategy that runs before Iceberg's, and reflection on Iceberg's `Procedure` interface,
since that interface ships in the Iceberg jar. This is not implemented, and `CometProcedureShim` on
Spark 3 returns the plan unchanged.

## Deciding whether Comet runs a call

`CometRewriteDataFilesProcedure.prepare` and `CometRewriteDataFiles.prepare` change nothing, so any
answer other than "run it" falls back to `delegate.call(input)` safely. A call is declined when:

- the result schema of Iceberg's procedure is not the five columns Comet produces. Iceberg keeps it
  in the private `OUTPUT_TYPE`, and a release that adds a column must not be answered with a row of
  the wrong shape
- the strategy is not `binpack`, or a sort order is given
- the call names a branch, with the `branch` argument or with a table identifier such as
  `db.events.branch_audit`. Iceberg 1.11 and later rewrite that branch, and Comet rewrites the main
  branch only
- the table is not in the procedure's catalog, or is not a `SparkTable`
- `CompactionOptions.parse` returns a reason: an option that neither the action nor Iceberg's
  planner reads, or a value of the action's options that Iceberg rejects
- Iceberg's planner rejects one of its options, which it checks in `init`
- Comet's native Iceberg scan is disabled, or the table is format version 3. A rewrite of a table
  with row lineage reads and writes the lineage columns, which only Iceberg's staged scan provides
- a file group has a file outside the group's output spec. Iceberg's write repartitions such a group
  with a shuffle, and a bin is written as it is read
- the native scan does not take over the read of every job. Each job's read is planned in `prepare`,
  which writes nothing, and the plan must contain `CometIcebergNativeScanExec`. The scan is declined
  for the same reasons as any Iceberg scan, such as a file that is not Parquet
- anything in this list throws. Any non-fatal exception while preparing is logged and handled as a
  decline, which gives the user Iceberg's error for a call that is invalid.

Once `prepare` returns, `execute` is the call: its failures are the call's failures.

## Planning

`IcebergRewriteReflection.newPlanner` makes Iceberg's `BinPackRewriteFilePlanner` the way Iceberg's
action makes it: for the current snapshot, with the call's `where` (or `Expressions.alwaysTrue()`)
and the session's case sensitivity. `plan` calls `init(options)` and `plan()`. Every planning option,
its default and its check is the one of the Iceberg release on the classpath, and so is an option a
later release adds, such as `max-file-group-input-files` in Iceberg 1.12. The groups come back in job
order, and `totalGroupCount` counts the groups before `max-files-to-rewrite` trimmed them, which sizes
the commits of partial progress.

The bins of a group are `TableScanUtil.planTaskGroups(group.fileScanTasks(), group.inputSplitSize(),
lookback, 0)`. That is the call `SparkStagedScan` makes for a group of Iceberg's own job, whose
runner reads it with a split size of `inputSplitSize` and a file open cost of 0, with the lookback of
the table's `read.split.planning-lookback`. It splits large files at their row groups and packs the
pieces by `sizeBytes()`, which includes the delete files a piece applies. So every bin is a Spark
partition of Iceberg's own job for the group, and becomes about one file.

## Running the jobs

`RewriteJobs.chunk` splits the groups, in job order, into jobs of at most `groupsPerJob` groups and
`MaxBinsPerJob` bins. Without partial progress `groupsPerJob` is unbounded. With it, it is the number
of groups in a commit, so that a job is a commit and a failed job fails one commit. `MaxBinsPerJob`
keeps the commit messages, which the driver holds until the job ends, under
`spark.driver.maxResultSize`. `max-concurrent-file-group-rewrites` jobs run at a time.

A job reads its bins through `CompactionSource`, a Spark `TableProvider` that reads a
`CompactionBatch` registered under a `batch-id` option, the way Iceberg's reader looks a staged group
up by id. Iceberg's staged scan cannot be used: it plans its own Spark partitions from all the tasks
it is given, so the bins of different groups, and of different partitions, would share one.

- `CompactionScan` has one `CompactionInputPartition` per bin and no reader, because only Comet
  can read it. `IcebergReflection.ICEBERG_SCAN_CLASSES` lists its class name, and it exposes the
  `table()`, `expectedSchema()` and `tasks()` accessors that `CometScanRule` reads off an Iceberg
  scan. From there the plan is the one of any Iceberg read: `CometIcebergNativeScan` serializes the
  tasks of each partition's `taskGroup()`. `CompactionSourceSuite` pins the names.
- The write is the one of Iceberg's bin-pack runner, `AppendData` with
  `rewritten-file-scan-task-set-id`, over the scan of the bins. Its files are staged in
  `FileRewriteCoordinator` under the batch id. `distribution-mode` is `none`, because every bin is
  within one group, whose files are all in its output spec. The groups of a job share the target
  file size and the output spec, which Iceberg's planner takes from the options.

The jobs run in a clone of the session with adaptive execution off, because it can change the output
partitioning of the write, and with Iceberg's executor cache for delete files off, because each
partition is rewritten once. Iceberg's action does the same.

The Iceberg write for a rewrite does not commit. `SparkWrite.RewriteFiles` stages its files in
`FileRewriteCoordinator`, so `IcebergCommitExec` only collects the task messages, and the files are
read back from the coordinator when the job ends. If the job fails, `IcebergCommitExec` deletes the
files of the tasks that finished.

The files of a job cannot be told apart by group, so they are handed to the job's first group with
`RewriteFileGroup.setOutputFiles`. That is safe because the groups of a job are always committed or
cleaned up together, and the result row adds the counts of every group.

## Committing

`RewriteDataFilesCommitManager` commits, made as Iceberg's action makes it: from the planning
snapshot, with `use-starting-sequence-number`. `commitOrClean` commits a set of groups as one
`RewriteFiles` snapshot validated against the starting snapshot, which deletes the rewritten data
files and their deletion vectors and adds the new files. It keeps the files when the commit state is
unknown and deletes them when the failure is cleanable. Comet turns a conflict into the message
Iceberg's action gives. When a job fails without partial progress, `abortFileGroup` deletes the files
of the jobs that finished.

With `partial-progress.enabled`, `BatchCommitter` commits finished jobs once they hold at least
`ceil(groups / partial-progress.max-commits)` groups, ignores a batch that fails, and fails the call
when more than `partial-progress.max-failed-commits` batches did. Iceberg's `CommitService` is not
used because it takes groups one at a time, so a commit could take part of a job, whose files are
all on its first group. A commit takes whole jobs, so it can hold more groups than that, but never
so many that there are more commits than the option allows.

With `remove-dangling-deletes=true`, the rewrite ends by running Iceberg's
`SparkActions.removeDanglingDeleteFiles(table)`, as Iceberg's action does, and adds what it removed
to `removed_delete_files_count`.

## Testing

| Suite                             | What it covers                                                                                                                                                                                                              |
| --------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `RewriteJobsSuite`                | How the groups are split into jobs, and how many groups a commit takes. No Iceberg needed.                                                                                                                                  |
| `CompactionOptionsSuite`          | The defaults of the action's options, the values Iceberg rejects, and the options Comet declines.                                                                                                                           |
| `CompactionSourceSuite`           | How a registered batch reads through Spark: its schema, one partition per bin, and the accessor names Comet reads. No Iceberg needed.                                                                                       |
| `CompactionScanIcebergSuite`      | Comet's native Iceberg scan over a `CompactionScan` of a real table: one native partition per bin, and the table's rows. Spark 4 only.                                                                                      |
| `CometIcebergCompactionSuite`     | `CALL` end to end on Spark 4: the result row, the job groups that show which engine ran and how many jobs, every decline, `where`, partial progress, deletes applied, and `remove-dangling-deletes`.                        |
| `CometIcebergCompactionBenchmark` | Iceberg's procedure on stock Spark and with Comet's native scan and writer, against Comet's compaction in one job. Many small partitions, a few larger ones, and one group read in several bins. It checks each case first. |

The planning is Iceberg's, so Iceberg's own `TestBinPackRewriteFilePlanner` covers it. When writing
a test for a change here, assert which engine ran. Every fallback is silent to the query, so a
passing comparison can mean Iceberg's procedure ran. `CometIcebergCompactionSuite` reads the job
group of the jobs that ran.

## Not done yet

- **Spark 3.x.** See above.
- **The `sort` and `zorder` strategies.** They shuffle, and they need `SparkShufflingDataRewritePlanner`'s
  options.
- **Branches.** A call that names a branch runs Iceberg's procedure.
- **Row lineage.** A format version 3 table runs Iceberg's procedure. The rewrite reads and writes
  the lineage columns, which only Iceberg's staged scan provides, and the native writer does not
  write them. See [#6669](https://github.com/apache/datafusion-comet/issues/6669).
- **Overlapping the commit of one job with the next job.** Partial progress commits as jobs finish,
  but a job covers a whole commit, so the first commit waits for its job.
