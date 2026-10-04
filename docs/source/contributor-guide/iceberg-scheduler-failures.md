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

# Native Iceberg writes: Spark scheduler failure tests

`CometIcebergSchedulerFailureSuite` tests speculative execution and executor loss during
multi-task native Iceberg writes. It covers the scheduler scenarios in
[#5646](https://github.com/apache/datafusion-comet/issues/5646), complementing the mid-write,
post-native handoff and job/commit failure tests in `CometIcebergWriteActionSuite`.
The suite adds test instrumentation without changing Iceberg commit or cleanup semantics.

## Test scenarios

| Scenario                               | Required scheduler evidence                                                                                        | Storage expectation                                                                                           |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------- |
| Original attempt wins speculation      | Two attempts on distinct hosts finish native handoff; the original succeeds and its message is accepted            | Only accepted winner files are referenced; no orphan or missing files                                         |
| Speculative attempt wins               | Two attempts on distinct hosts finish native handoff; the speculative attempt succeeds and its message is accepted | Only accepted winner files are referenced; no orphan or missing files                                         |
| Executor-loss task replacement         | Executor removal, `ExecutorLostFailure`, and successful replacement of the affected partition on another executor  | Only accepted winner files are referenced; every orphan belongs to a known rejected attempt; no missing files |
| Executor loss with shuffle-output loss | The task-replacement evidence, plus `FetchFailed` and a higher writer `stageAttemptId`                             | The same storage assertions as executor-loss task replacement                                                 |

An increased task attempt number alone does not prove stage re-execution. The shuffle-output-loss
case fails if the cluster only replaces the task without the required stage and fetch-failure events.

## Common assertions

Every scenario writes to a new empty table and asserts:

- Exactly 48,000 rows and 48,000 distinct IDs. Bidirectional `EXCEPT ALL` comparisons check the
  complete expected ID multiset, rather than relying on counts alone.
- Exactly one new snapshot, one commit operator, and one successful commit marker.
- One driver-accepted message per logical writer partition. Each message's complete file set
  identifies exactly one native handoff attempt with a scheduler `Success` event.
- Manifest references equal the complete union of accepted winner files. Files reported by
  rejected attempts must not be referenced.
- Every referenced file exists under the data location.

The storage audit inventories all regular files under the data location, including unfinished
files, excluding only Hadoop CRC metadata sidecars. It computes:

```text
orphan  = physical files - manifest-referenced files
missing = manifest-referenced files - physical files
```

For speculation, both sets must be empty after the losing attempt terminates and cleanup finishes.
For executor loss, `missing` must be empty and `orphan` must be a subset of `rejectedFiles`, the
native-reported paths belonging to attempts whose messages were not accepted. Unknown orphan
files fail the test. This checks the storage state even when readers see the correct table rows.

`SIGKILL` bypasses executor-side cleanup, so known rejected-attempt orphans do not by themselves
fail executor-loss recovery. These scenarios do not require immediate `physical == referenced`
and do not introduce a surviving process's cleanup mechanism. Files are never removed before
storage verification to make the audit pass.

## Cluster requirements

This is a manual integration suite, excluded from automatic discovery by `@DoNotDiscover` and
from ordinary single-host CI through `dev/ci/check-suites.py`. Missing infrastructure fails the
suite; it does not produce a skipped pass.

Use a dedicated Spark Standalone cluster with:

- At least two Linux worker hosts, as seen by the Spark scheduler, with at least two cores per
  worker and executor replacement enabled. `local[...]` and `local-cluster[...]` are rejected.
- Matching Spark/Scala profiles, JDK, operating system and native architecture on the driver and
  executors. A macOS native build cannot serve Linux executors.
- A driver address reachable from both workers, configured through the usual Spark settings.
- A shared POSIX filesystem mounted at the same absolute path on the driver and all workers.
  It holds the warehouse, source files, attempt markers and executor classpath snapshot.
  Atomic rename must work. S3/HDFS warehouses are outside this harness's scope.

The suite requests one core per executor and up to four cores in total. A gated preflight job
checks shared-filesystem visibility and concurrent executor processes on at least two distinct
hosts before running the write scenarios.

AQE, partition coalescing, dynamic allocation, decommissioning, push shuffle and the external
shuffle service are disabled. The stage-loss case depends on executor-local shuffle output being
lost when its owning process terminates. Comet shuffle is disabled; the stage-loss fixture exposes
ordinary Spark shuffle lineage through an RDDScan so Spark-to-Arrow can feed the native writer.

### Speculation settings

`COMET_SCHEDULER_SPECULATION_ENABLED` controls `spark.speculation` before SparkContext creation
and defaults to `true`. Run speculation with `true` and executor-loss cases in separate Maven
invocations with `false`.

The initial loss-mode gates prevent speculation before any writer succeeds, but speculation can
still occur during recovery if enabled. Disabling it ensures the loss tests prove ordinary task
replacement. Run the filters below separately rather than running all scenarios with one setting.

## Failure injection and probes

`IcebergSchedulerTestProbe` is an explicitly scoped driver hook. Its serializable instance is
captured in task closures, so executors do not depend on a driver-installed JVM singleton.
`IcebergCommitExec` records messages actually accepted by Spark's `runJob` result handler and
records the successful commit. Duplicate accepted messages or commit markers fail the probe.

`CometIcebergWriteExec` installs per-attempt gates before parent input iteration and after native
payload handoff. An optional `IcebergWriteCommon.test_probe` also gates the native writer after a
successful `writer.write()`, before input EOF and `writer.close()`. The normal planner never
populates this field, and no production SQLConf enables it. JSON markers are published atomically
under unique run/task-attempt directories; all gate waits have bounded timeouts and diagnostics.

### Speculative attempts

Four source Parquet files contain disjoint ID ranges. The original attempt for writer partition 0
is held after native handoff while other partitions finish, allowing Spark to launch a speculative
attempt on another host. Both attempts must report nonempty, disjoint file sets for the same
stage, stage attempt and partition.

The tests control each winner direction by releasing only the selected attempt and requiring its
scheduler `Success` and accepted message. Both attempts completing native handoff does not mean
both succeed in the scheduler: Spark cancels the loser after accepting the winner.

### Executor process loss

Initial writer attempts pause after 2,000 rows, using 1,000-row batches and a one-byte target file
size. The second unit rolls the first file to disk; the first unit alone may remain buffered.
Before termination, the test independently requires physical bytes at a native-owned path, no
native handoff or terminal task event for the target, and progress on another writer partition
in a different executor process.

The ordinary loss case targets partition 0. The shuffle-output-loss case selects an active writer
executor that also owns completed upstream shuffle output. Two other reducers are gated before
constructing their parent iterators, preventing eager shuffle fetches. After executor removal,
releasing those reducers must cause `FetchFailed` and writer-stage re-execution.

The suite records the target's exact host, PID, executor ID and native-owned paths before invoking
the termination helper. `dev/iceberg-scheduler-kill-executor.py` is a Linux/SSH implementation that
checks `/proc/PID/cmdline` for `CoarseGrainedExecutorBackend` and the expected executor ID before
sending `SIGKILL`, then waits for process exit. It terminates the executor, not the worker or driver.

A custom executable may replace the SSH helper for containers or Kubernetes. Its argument contract
is `HOST PID EXECUTOR_ID RUN_DIRECTORY`; it must provide equivalent process-identity and exit
evidence. The SSH helper requires noninteractive SSH access as the executor owner and Python 3
on each worker.

## Build and run

Run commands from the repository root. These examples use Spark 4.1 explicitly; if selecting
another profile, use it consistently and provision matching cluster binaries.

```bash
make core
./mvnw test-compile -Pspark-4.1 -DskipTests
```

The local smoke test checks probe serialization, handoff, accepted files, completion markers,
rows and physical files. It does not prove multi-host scheduler behavior:

```bash
./mvnw test -Pspark-4.1 -Dtest=none \
  -Dsuites="org.apache.comet.CometIcebergWriteActionSuite scheduler probe"

./mvnw test -Pspark-4.1 -Dtest=none \
  -Dsuites="org.apache.comet.CometIcebergWriteActionSuite"
```

Replace the cluster settings below with your infrastructure:

```bash
export COMET_SCHEDULER_MASTER='spark://spark-master:7077'
export COMET_SCHEDULER_SHARED_ROOT='/shared/comet-scheduler'
export COMET_SCHEDULER_KILL_COMMAND="$PWD/dev/iceberg-scheduler-kill-executor.py"
export COMET_SCHEDULER_TIMEOUT_SECONDS=180

COMET_SCHEDULER_SPECULATION_ENABLED=true ./mvnw test -Pspark-4.1 -Dtest=none \
  -Dsuites="org.apache.comet.CometIcebergSchedulerFailureSuite speculative"

COMET_SCHEDULER_SPECULATION_ENABLED=false ./mvnw test -Pspark-4.1 -Dtest=none \
  -Dsuites="org.apache.comet.CometIcebergSchedulerFailureSuite task replacement"

COMET_SCHEDULER_SPECULATION_ENABLED=false ./mvnw test -Pspark-4.1 -Dtest=none \
  -Dsuites="org.apache.comet.CometIcebergSchedulerFailureSuite stage re-execution"
```

By default the suite snapshots the actual Maven test JVM's unshaded classpath to the shared root
before creating SparkContext. It includes current reactor classes, test helpers, dependencies and
native resources. A shaded Comet jar alone is insufficient for the unshaded task closures.
The snapshot is removed after SparkContext stops. To use a pre-provisioned matching unshaded
classpath instead, set `COMET_SCHEDULER_EXECUTOR_CLASSPATH` to paths visible on every worker.

## Diagnostics and teardown

Evidence is exported to `spark/target/iceberg-scheduler-artifacts/<runId>/`, including scheduler
events, plans, attempt markers, the process-termination log, file inventories and
`gates/storage-audit.json`. The audit records physical, referenced, winner, rejected, orphan and
missing file sets before storage assertions.

The suite releases gates, waits for or cancels the job, drains the listener bus and exports evidence
before deleting successful fixtures. Failed runs retain the shared warehouse for inspection.
Unique run IDs isolate subsequent invocations from earlier files.

Report scheduler recovery and storage results separately. A passing executor-loss test with known
orphans proves recovery and file attribution, not immediate cleanup. Do not claim stage
re-execution when only task attempts increased. Repeat the speculation filter when assessing race
stability, and retain each invocation's evidence.

## Regression checks

Compilation and formatting checks do not replace execution on the required cluster. An Iceberg
write-path change also needs an upstream Iceberg regression verdict before merging, using the
`run-iceberg-tests` label or an appropriate `dev/local-ci.sh iceberg` shard as described in
[Continuous Integration](ci.md). That regression run does not replace this scheduler suite.
