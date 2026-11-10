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

This document describes how Comet runs Iceberg's `rewrite_data_files` procedure with the `binpack`
strategy: how a `CALL` reaches Comet, how the rewrite is planned, run and committed, and how to test
a change. For the user-facing view (configuration, supported options, and when Comet leaves a call to
Iceberg), see [Compaction](../user-guide/latest/iceberg.md#compaction).

The rule that governs the whole feature is the one that governs the [Iceberg
writes](iceberg-writes.md): **Comet must produce the outcome iceberg-java would have produced, or
decline.** A call Comet does not fully reproduce runs Iceberg's own procedure, and it is decided
before anything is written.

## Overview

```text
CALL system.rewrite_data_files(...)
  -> optimizer rule swaps Iceberg's procedure for CometRewriteDataFilesProcedure   (Spark 4.x)
       -> eligible?  no: Iceberg's procedure runs the call
       -> CometRewriteDataFiles.prepare   plan the file groups, change nothing
       -> PreparedRewrite.execute         plan the jobs, run them, commit
            bins job:   CompactionSource scan, one partition per bin     (CometIcebergNativeScan)
                        Iceberg rewrite write                            (CometIcebergWrite when eligible)
            staged job: Iceberg staged scan of one group                 (CometIcebergNativeScan)
                        Iceberg rewrite write                            (CometIcebergWrite when eligible)
            commit:     Table.newRewrite() on the driver
```

| Component                             | Location                                                    | Role                                                                                                                           |
| ------------------------------------- | ----------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `CometIcebergProcedureRule`           | `spark/src/main/scala/org/apache/comet/iceberg/compaction/` | Optimizer rule. Looks at the root of every optimized plan for a `CALL`.                                                        |
| `CometProcedureShim`                  | `spark-4.x/` and `spark-3.x/`, same package                 | Swaps the procedure on Spark 4. A no-op on Spark 3.                                                                            |
| `CometRewriteDataFilesProcedure`      | `spark-4.x/`, same package                                  | The `BoundProcedure` Spark invokes. Decodes the arguments, asks for a plan, and falls back to Iceberg's procedure.             |
| `CometRewriteDataFiles`               | `spark/src/main/scala/org/apache/comet/iceberg/compaction/` | The rewrite: planning, running the groups, committing, cleaning up.                                                            |
| `BinPackPlanner`, `CompactionOptions` | same                                                        | The planning logic and the option parsing. Pure Scala with no Iceberg classes, so they are unit tested on every Spark profile. |
| `CompactionSource`, `CompactionScan`  | same                                                        | The Spark data source of a bins job. A scan whose partitions are the bins the planner chose.                                   |
| `IcebergRewriteReflection`            | same                                                        | Every call Comet makes on Iceberg. Comet does not link Iceberg, so these are reflective.                                       |

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

`CometRewriteDataFilesProcedure.prepare` changes nothing, so any answer other than "run it" falls
back to `delegate.call(input)` safely. It declines when:

- the result schema of Iceberg's procedure is not the five columns Comet produces. Iceberg keeps it
  in the private `OUTPUT_TYPE`, and a release that adds a column must not be answered with a row of
  the wrong shape
- the strategy is not `binpack`, or a sort order is given
- the table is not in the procedure's catalog, or is not a `SparkTable`
- `CompactionOptions.parse` returns a reason: an unknown option, a value Iceberg would reject, or
  an option Comet does not reproduce
- anything in this list throws. Any non-fatal exception while preparing is logged and handled as a
  decline, which gives the user Iceberg's error for a call that is invalid.

Once `prepare` returns, `execute` is the call: its failures are the call's failures.

## Planning

`BinPackPlanner` is `BinPackRewriteFilePlanner` from iceberg-java in Scala. The steps are the ones
of iceberg-go's `compaction.PlanCompaction`:

| Step                           | iceberg-go (`table/compaction`)                         | Comet                                       |
| ------------------------------ | ------------------------------------------------------- | ------------------------------------------- |
| Scan the snapshot              | `Analyze` plans the files of the table                  | `IcebergRewriteReflection.planFiles`        |
| Bucket by partition            | `partitionBucketKey(specID, partition)`                 | the `partition` key of `CompactionFile`     |
| Keep the files worth rewriting | `isCandidate`                                           | `isCandidate`                               |
| Pack a bucket                  | `SlicePacker.PackEnd` into bins of the target file size | `pack` into groups of `max-file-group-size` |
| Drop bins not worth a rewrite  | `MinInputFiles`                                         | `isWorthRewriting`                          |

Two differences are on purpose. Defaults and option names are iceberg-java's, because the `CALL`
surface is iceberg-java's: `delete-file-threshold` is unbounded there and 5 in iceberg-go. And
iceberg-java packs into groups of up to 100 GB, which are the unit that fails and commits together,
then splits a group into read tasks of `inputSplitSize` when it reads it. iceberg-go packs bins of the
target file size directly, each of which is about one output file. `BinPackPlanner.planReadTasks` is
the second step of the iceberg-java approach, and what a scan with explicit bins would use.

Every number comes from `SizeBasedFileRewritePlanner`: `expectedOutputFiles`, `inputSplitSize` and
the writer's rolling size `writeMaxFileSize`. The unit tests pin them with the examples in that
class's Javadoc.

## Running the groups

The rewrite is a list of jobs, and a job is one Spark job. `planJobs` makes the list, and
`max-concurrent-file-group-rewrites` of them run at a time. There are two kinds.

**A staged job** rewrites one group. It does what `SparkBinPackFileRewriteRunner` does, with
DataFrames: stage the group's `FileScanTask`s under a fresh id in Iceberg's `ScanTaskSetManager`,
read them back with the `scan-task-set-id` option, and write with `rewritten-file-scan-task-set-id`.
That keeps the data movement in the two paths that already carry the parity guarantees: the staged
scan is converted to `CometIcebergNativeScan` by `CometScanRule`, and the write goes through
`IcebergWriteStrategy` and the eligibility gate. `CometIcebergRewriteActionSuite` is the suite that
shows both engage on the groups of Iceberg's own action.

**A bins job** rewrites several groups in one job. Iceberg's staged scan packs the tasks of its
group into Spark partitions of about the split size, and a group is the unit it does that for, so
a table with a thousand partitions runs a thousand small jobs, a few at a time. A bins job instead
plans the partitions itself, with `BinPackPlanner.planReadTasks`, so that each is a bin of one
group, and reads them all in one job. The pieces are:

- `IcebergRewriteReflection.splitTasks` splits each task of a group into read tasks of about
  `inputSplitSize` with Iceberg's own `SplittableScanTask.split`, because `TableScanUtil.planTaskGroups`
  does, and reads each task's `sizeBytes()` and `filesCount()`, which include its delete files.
  Packing by the data file size alone would put bins in different places.
- `CompactionSource` is a Spark `TableProvider` that reads a `CompactionBatch` registered under a
  `batch-id` option, the way Iceberg's reader looks a staged group up by id. Its scan,
  `CompactionScan`, has one `CompactionInputPartition` per bin and no reader, because only Comet
  can read it. `IcebergReflection.ICEBERG_SCAN_CLASSES` lists its class name, and it exposes the
  `table()`, `expectedSchema()` and `tasks()` accessors that `CometScanRule` reads off an Iceberg
  scan. From there the plan is the one of any Iceberg read: `CometIcebergNativeScan` serializes the
  tasks of each partition's `taskGroup()`. `CompactionSourceSuite` pins the names.
- Because Spark cannot read a `CompactionScan`, the job is only used when Comet's native scan takes it
  over. `binsJob` plans the scan DataFrame and looks for a `CometIcebergNativeScanExec` first,
  which writes nothing, and a plan without one gives its groups staged jobs. The scan is declined for
  the same reasons as any Iceberg scan, such as a file that is not Parquet.
- The write is the one of a staged job, `AppendData` with `rewritten-file-scan-task-set-id`, over
  the scan of the bins. Its files are staged in `FileRewriteCoordinator` under the batch id.
  `distribution-mode` is `none`, because every bin is within one group, which is within one
  partition. A group whose files are not in the output spec needs a shuffle, so it gets a staged job.
  A table with row lineage (format version 3) reads and writes extra columns in a rewrite that
  only Iceberg's staged scan provides, so all of its groups get staged jobs.

A bins job covers at most `groupsPerJob` groups and `MaxBinsPerJob` bins. Without partial progress
`groupsPerJob` is unbounded. With it, it is the number of groups in a commit, so that a job is a
commit and a failed job fails one commit. `MaxBinsPerJob` keeps the commit messages, which the
driver holds until the job ends, under `spark.driver.maxResultSize`.

Both kinds of job run in a clone of the session with adaptive execution off, because it can change
the output partitioning of the write, and with Iceberg's executor cache for delete files off, because
each partition is rewritten once. Iceberg's action does the same.

The Iceberg write for a rewrite does not commit. `SparkWrite.RewriteFiles` stages its files in
`FileRewriteCoordinator`, so `IcebergCommitExec` only collects the task messages, and the files are
read back from the coordinator when the job ends. If the job fails, `IcebergCommitExec` deletes the
files of the tasks that finished.

## Committing

`IcebergRewriteReflection.commitRewrite` is `RewriteDataFilesCommitManager.commitFileGroups`:
`Table.newRewrite()` validated from the starting snapshot, optionally pinned to its data sequence
number, deleting the rewritten data files and the deletion vectors of the rewritten tasks, and
adding the new files. `commitOrClean` keeps the files when the commit state is unknown, deletes them
when the failure is cleanable, and turns a conflict into the same message Iceberg gives.

With `partial-progress.enabled`, `BatchCommitter` commits the finished jobs once they hold at
least `ceil(groups / partial-progress.max-commits)` groups, ignores a batch that fails, and fails the
call when more than `partial-progress.max-failed-commits` batches did. A commit takes whole jobs, so
it can hold more groups than that, but never so many that there are more commits than the option
allows.

Deletion vectors are identified by their location, offset and length, as Iceberg's `DeleteFileSet`
does, because the vectors of many data files can share one Puffin file. A key of the location alone
would commit one of them and leave the others pointing at removed data files.

## Testing

| Suite                         | What it covers                                                                                                                                                    |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `BinPackPlannerSuite`         | Which files are selected, how groups and read tasks are packed and ordered, and the size arithmetic. No Iceberg needed.                                           |
| `CompactionOptionsSuite`      | Defaults, the values Iceberg rejects, and the options Comet declines.                                                                                             |
| `CompactionSourceSuite`       | How a registered batch reads through Spark: its schema, one partition per bin, and the accessor names Comet reads. No Iceberg needed.                             |
| `CompactionScanIcebergSuite`  | Comet's native Iceberg scan over a `CompactionScan` of a real table: one native partition per bin, and the table's rows. Spark 4 only.                            |
| `CometIcebergCompactionSuite` | `CALL` end to end on Spark 4: the result row, the job group that shows which engine ran and how many jobs, the fallbacks, `where`, partial progress, and deletes. |

When writing a test for a change here, assert which engine ran. Every fallback is silent to the
query, so a passing comparison can mean Iceberg's procedure ran. `CometIcebergCompactionSuite` reads
the job group of the jobs that ran.

## Not done yet

- **Spark 3.x.** See above.
- **The `sort` and `zorder` strategies.** They shuffle, and they need `SparkShufflingDataRewritePlanner`'s
  options.
- **Writing row lineage.** On a format-version 3 table the Iceberg write for a rewrite adds the row
  lineage columns, which the native writer does not write, so the groups of such a table are written by
  Iceberg's writer. See [#6669](https://github.com/apache/datafusion-comet/issues/6669).
- **Overlapping the commit of one job with the next job.** Partial progress commits as jobs finish,
  but a bins job covers a whole commit, so the first commit waits for its job.
