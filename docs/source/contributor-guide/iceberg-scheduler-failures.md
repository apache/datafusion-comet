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

# PR4 implementation plan: native Iceberg scheduler failures

Issue: [#5646](https://github.com/apache/datafusion-comet/issues/5646).
This plan covers speculative execution and executor process loss during concurrent native writes.
It complements the ordinary mid-write retry, post-native handoff failure and job/commit abort tests
in `CometIcebergWriteActionSuite`. It does not change Iceberg commit or cleanup semantics.

## Six required coverage improvements

1. **Task retry versus stage re-execution.** Run separate executor-loss cases. The ordinary case
   proves an affected writer partition succeeds on another executor. The shuffle-output-loss case
   additionally requires `FetchFailed` and an increased writer `stageAttemptId`. An ordinary task
   replacement is never reported as stage retry. If the configured cluster cannot provoke stage
   re-execution, that case fails with evidence rather than silently relaxing its assertions.
2. **Prove mid-write progress.** A native-only gate runs immediately after a successful
   `writer.write()` and before input EOF and `writer.close()`. It reports rows and locations from
   `TrackingLocationGenerator`. The target must have physical bytes, no handoff or terminal task
   event, and another executor must have progressed on another writer partition before SIGKILL.
   A task-start event or post-native handoff gate cannot substitute for this evidence.
3. **Compare complete winner file sets.** On a new empty table, the manifest's referenced set must
   equal the union of complete file sets in driver-accepted messages. Each message maps to exactly
   one native handoff attempt. Known losing/failed files must not intersect referenced files.
4. **Inspect accepted messages.** Record the actual `runJob` result-handler messages, keyed by
   logical partition, before driver commit. Duplicate accepted messages fail the probe. Require one
   message per writer partition and one commit invocation. `numCommittedMessages` is only an
   additional count check; its current implementation counts the partition-sized message array.
5. **Compare exact data.** Require 48,000 rows and 48,000 distinct IDs, and compare the complete
   expected ID multiset using `EXCEPT ALL` in both directions. Counts alone cannot pass the test.
6. **Wait for termination and audit storage separately.** Wait for all monitored task attempts to
   be terminal and completion markers from surviving executor processes, then drain the listener
   bus. Inspect all regular data-location files, including unfinished files; exclude only Hadoop
   CRC metadata sidecars. Persist `physical`, `referenced`, `missing`, `orphan`, winner and rejected
   sets before storage assertions. For executor loss, the #5646 acceptance policy requires no
   missing referenced files, manifest equality with accepted winner files, and no references to
   failed attempts. Every orphan must belong to the native-reported file set of a known
   rejected attempt (`orphan` must be a subset of `rejectedFiles`); unknown orphan files fail.
   Record hard-kill orphans separately; immediate `physical == referenced` is not an
   executor-loss requirement. Speculation retains its losing-attempt cleanup checks.
   Never remove files before the audit to make a scenario pass.

## Infrastructure and topology

`CometIcebergSchedulerFailureSuite` is a manual integration suite excluded from single-host CI via
`dev/ci/check-suites.py` and excluded from automatic discovery with `@DoNotDiscover`. It requires an external Spark cluster whose executors run on at least two
hosts as seen by the scheduler. `local[...]` and `local-cluster[...]` are rejected.

Use a dedicated Spark Standalone cluster with at least two Linux worker hosts, at least two cores
per worker, and executor replacement enabled. The suite requests one core per executor and up to
four cores in total, keeping the two active reducers in the stage-loss case on distinct processes. Driver and executors must use the same Spark/Scala profile, JDK, operating
system and native architecture. In particular, a macOS native build cannot serve Linux executors.
The driver must be reachable from workers; configure networking through the usual Spark settings.

Mount a genuinely shared POSIX filesystem at the same absolute path on driver and workers. This
stores the warehouse, source Parquet files, run-specific gates and a snapshot of the driver's
unshaded Maven test classpath. Atomic rename must work on this filesystem. The native gate uses
POSIX directly; S3/HDFS warehouses are outside this harness's scope.

Preflight runs a gated Spark RDD job. Workers read a driver challenge and publish executor/host/PID
markers; the driver requires at least two distinct hosts and executors before releasing them.
This checks cross-process filesystem visibility and available concurrency. The subsequent native
scenarios provide the actual scheduler-event and remote-probe evidence. Compilation alone does
not verify this topology or prove speculation/executor loss occurred.

Speculation settings are fixed before SparkContext creation. AQE and coalescing are disabled.
Dynamic allocation, decommissioning, push shuffle and the external shuffle service are disabled;
the stage-loss case depends on losing executor-local shuffle output.

`COMET_SCHEDULER_SPECULATION_ENABLED` controls `spark.speculation` before SparkContext creation
and defaults to `true`. Run the speculation filter with `true`, and each executor-loss filter in
an independent Maven invocation with `false`. Initial loss-mode gates prevent speculation before
any writer succeeds, but speculation can still occur during recovery if left enabled. Disabling it
keeps the loss cases focused on ordinary task replacement. Do not run all four cases together with
one speculation setting when collecting the separate scenario verdicts.

## Shared test instrumentation

- `IcebergTestFiles` supplies common recursive regular-file and Parquet-file inventories.
- `IcebergSchedulerTestProbe` is an explicitly scoped driver hook. Its serializable instance is
  captured in task closures; executors do not consult a driver-installed JVM singleton.
- `CometIcebergWriteExec` invokes the captured probe at task setup and after `cleanup.own(locations)`.
- An optional `IcebergWriteCommon.test_probe` supplies a per-attempt native progress gate. The
  normal planner never populates this field, and there is no production SQLConf to enable it.
- `IcebergCommitExec` records files from messages accepted by Spark and the successful commit.
- Shared markers are published atomically under unique run/task-attempt directories. Gate waits
  have bounded timeouts and diagnostics. `finally` releases gates, waits/cancels the job, removes
  the listener and exports evidence. Successful fixtures are deleted; failing warehouses remain
  for inspection and do not affect later runs with new IDs.

## Scenario A: two completed native speculative attempts

A new unpartitioned table consumes four Parquet source files with disjoint ID ranges. Native
handoff for writer partition 0 is gated; other partitions finish, making the original a straggler.
Spark must launch a speculative attempt on another host. Both attempts must report nonempty,
disjoint native file sets for the same stage, stage attempt and partition.

Run both winner directions: release the original first, then in another test release the
speculative attempt first. Require the selected winner's scheduler `Success`, one accepted message
for that partition, winner-only manifest references, one snapshot, exact IDs and zero orphan/missing
files. Native completion is not equivalent to two scheduler `Success` events: Spark cancels the
loser once it accepts the winner. Missing speculation or either handoff is a failure.

## Scenario B: executor process loss during concurrent native writes

Gate initial attempts after 2,000 rows have passed through native `writer.write()`. With 1,000-row
batches and a one-byte target file size, the second unit rolls the first data file to disk; the
first unit alone may remain buffered. Independently require a native-owned path with physical
bytes, and no native handoff or terminal task event before termination. Require
at least two executor processes writing different partitions. Select a live writer executor (partition 0 in the ordinary loss case),
record its exact ID/host/PID and native-owned files, and terminate only that process.

`dev/iceberg-scheduler-kill-executor.py` is a Linux/SSH implementation. It verifies the remote
`/proc/PID/cmdline` names `CoarseGrainedExecutorBackend` and the expected executor ID immediately
before SIGKILL, and waits for the process to exit. It does not kill workers or the driver. A custom
executable harness may replace it for container/Kubernetes deployments; its argument contract is
`HOST PID EXECUTOR_ID RUN_DIRECTORY`, and it must provide equivalent process-identity/exit evidence.

Require matching executor removal and `ExecutorLostFailure`, then release surviving gates. Require
the affected partition's replacement on a different executor and successful completion. Audit one
snapshot, exact IDs, complete accepted-message references and storage. SIGKILL can bypass native/JVM
cleanup. Preserve and report any resulting orphan as storage evidence; implementing a surviving
process's cleanup mechanism is outside this test PR. Under the intended policy, an unreferenced
hard-kill file alone does not fail executor-loss recovery.

The stage-reexecution variant repartitions the input through Spark shuffle and gates two reducers
before constructing their parent iterators, preventing eager shuffle fetches before the gate. The killed writer executor must also own upstream shuffle output.
Releasing delayed reducers after its loss must cause `FetchFailed` and a higher writer stage attempt.
A topology that merely replaces the task fails this additional case; inspect the event evidence.

## Build and test commands

Run from the repository root. The examples use the default Spark 4.1 profile; use a consistent
explicit profile for all build/test steps and matching cluster binaries if changing versions.

```bash
make core
./mvnw test-compile -DskipTests
```

Local smoke verifies serializable probe handoff, driver-accepted message files, completion markers,
rows and physical files. It makes no multi-host scheduler claims:

```bash
./mvnw test -Dtest=none \
  -Dsuites="org.apache.comet.CometIcebergWriteActionSuite scheduler probe"

./mvnw test -Dtest=none \
  -Dsuites="org.apache.comet.CometIcebergWriteActionSuite"
```

For the external cluster, replace these values with actual infrastructure. The SSH harness requires
noninteractive SSH access to the worker as the executor owner and Python 3 on each worker.

```bash
export COMET_SCHEDULER_MASTER='spark://spark-master:7077'
export COMET_SCHEDULER_SHARED_ROOT='/shared/comet-scheduler'
export COMET_SCHEDULER_KILL_COMMAND="$PWD/dev/iceberg-scheduler-kill-executor.py"
export COMET_SCHEDULER_TIMEOUT_SECONDS=180

COMET_SCHEDULER_SPECULATION_ENABLED=true ./mvnw test -Dtest=none \
  -Dsuites="org.apache.comet.CometIcebergSchedulerFailureSuite speculative"

COMET_SCHEDULER_SPECULATION_ENABLED=false ./mvnw test -Dtest=none \
  -Dsuites="org.apache.comet.CometIcebergSchedulerFailureSuite task replacement"

COMET_SCHEDULER_SPECULATION_ENABLED=false ./mvnw test -Dtest=none \
  -Dsuites="org.apache.comet.CometIcebergSchedulerFailureSuite stage re-execution"
```

By default the suite snapshots the actual Maven test JVM classpath to the shared root before
creating SparkContext. It includes current reactor classes, test helpers, runtime dependencies and
native resources, avoiding incompatible mixtures of shaded jars and unshaded task closures. The
snapshot is removed after SparkContext stops. For a pre-provisioned, matching unshaded classpath,
set `COMET_SCHEDULER_EXECUTOR_CLASSPATH` explicitly to paths visible on every worker.

Evidence is exported to `spark/target/iceberg-scheduler-artifacts/<runId>/`: scheduler events,
attempt markers, process termination log and storage audit. Failed shared run directories also
retain the warehouse. Do not claim storage cleanup passed if the audit reports orphans; do not
claim stage retry if only task attempts increased. Repeat the speculative filter several times
when evaluating race stability. No test is marked passed merely because infrastructure is absent.

## Regression and execution status

An Iceberg write-path change also needs the matching upstream Iceberg verdict before merging,
using the `run-iceberg-tests` label or an appropriate local `dev/local-ci.sh iceberg` shard per
`ci.md`. That ordinary regression run does not replace this multi-host scheduler suite.

The implementation can be compiled without a cluster; compilation alone is not a scheduler
verdict. Linux ARM64 Docker runs with Spark 4.1.3, Java 17 and Iceberg 1.11.0 on 2026-10-04 passed
both speculative winner directions. With speculation disabled, both executor-loss cases passed
scheduler recovery, exact-row, snapshot and accepted-file/manifest assertions. The stage-loss case
also recorded `FetchFailed` and an increased writer stage attempt. Each loss case reported one
orphan and zero missing referenced files, and failed the current unconditional zero-orphan
assertion used in that earlier run. After scoping the assertion to retain hard-kill orphans as
audit evidence, both loss cases passed in a subsequent run (2 tests passed, 0 failed). Each audit
still reported one orphan and zero missing referenced files, with no rejected attempt file
referenced. The stage-loss case again recorded `FetchFailed` and a higher writer stage attempt.
No files were removed before the audit. Successful fixtures were removed only after verification
and evidence export, using the suite's normal teardown.
A further rerun with `orphan.subsetOf(rejectedFiles)` also passed both loss cases (2 passed,
0 failed). Each audit reported one known rejected-attempt orphan, zero unknown orphans and zero
missing referenced files. The Docker runs used a container-specific kill harness; they do not
validate the Linux/SSH helper.
Record each scenario's event and storage outcomes separately for any subsequent run.
