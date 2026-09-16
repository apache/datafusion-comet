# GitHub Workflows

GitHub Actions only loads `*.yml` / `*.yaml` files in this directory as
workflows. This README is ignored by the runner.

## Pipeline overview

A single umbrella workflow (`ci.yml`) orchestrates everything that runs on
pull requests and in the merge queue. The umbrella runs cheap **preflight**
checks first, computes which heavy jobs are relevant to the change, and only
then fans out to the long-running test/build workflows. Each long workflow
is a `workflow_call` reusable invoked from the umbrella.

Merging goes through GitHub's merge queue, configured by the `Merge Queue`
ruleset in `.asf.yaml`. That splits CI into three tiers:

- **PR tier** (`pr`): fast feedback while a change is being iterated on.
  The Linux build, with the Comet test suites run against the default Spark
  profile (4.1) only, and nothing else.
- **Queue tier** (`queue`): the authoritative gate. The PR tier plus the
  macOS build, the benchmark compile check, the Delta contrib build gate, the
  PyArrow UDF suite, Spark SQL on Spark 4.1 and Iceberg 1.11, evaluated
  against the merge result rather than against the PR head. One Spark version
  and one Iceberg version, both the default profile's.
- **Nightly tier** (`nightly`): the regression sweep of everything else, once
  a day against `main` as it stands. The Comet test suites against the other
  four Spark profiles, Spark SQL on Spark 3.5 and 4.0, and Iceberg
  1.8/1.9/1.10. See [Nightly tier](#nightly-tier) below for how a failure
  surfaces.

Every queue-only and nightly job has a `run-*` label that opts a pull request
into it early, listed in the table below. The Lint Java matrix compiles
Spark 3.4/3.5/4.0 on every pull request, so a shim that fails to build is
caught there; only the runtime suites wait for the queue or the nightly.

`spark_3_4` is in none of the tiers. Spark 3.4 is deprecated, so its Spark SQL
suite no longer gates a merge; it runs only when a pull request carries
`run-spark-3.4-tests`, or from a `workflow_dispatch`. Anyone who wants to
check a change against 3.4 can still do so, but note when that result starts
to block a merge. Adding the label fires a `labeled` event, and those runs
publish the advisory `Required Checks (label run)` name rather than the
required one, so a red 3.4 there changes nothing. It is the next push with
the label still applied that runs 3.4 under `Required Checks`, and with the
queue run gone that push is the only thing that makes a 3.4 failure blocking.

Most heavy jobs have no `push` tier. The queue already tested the exact tree that
lands, so re-running them on push to main would double the cost of every
merge. Two routes are still on `push`: `docs`, because it deploys to `asf-site`
and has to run after the commit is on main, and `build_linux`, because of
`actions/cache` scoping. The Linux route selects `pr_build_linux_checks` and
`pr_build_linux`, with the shared `build_linux_native` producer supplying the
latter. Spark SQL and Iceberg consumers stay off on push. A pull request can
only restore caches saved on its own branch or on `main`, and the queue runs on a throwaway
`gh-readonly-queue/*` branch whose caches are deleted with it. Without a push
run, a `Cargo.lock` or `pom.xml` change would leave the cargo-ci, cargo-debug,
Maven and TPC-H/TPC-DS caches on `main` stale until the next unrelated change.

On a push to main, `build_linux` selects all three Linux workflows, but only
cache-refresh work survives their job guards:

- `build_linux_native.yml`: `build-native` refreshes `cargo-ci`.
- `pr_build_linux_checks.yml`: `lint` gates `linux-test-rust`, which refreshes
  `cargo-debug`. The Java/Scala lints, compile-only job and Celeborn checks skip.
- `pr_build_linux.yml`: the two TPC jobs populate their datasets and Maven
  caches, then skip query execution. Profile selection and JVM tests skip.

Both Linux callers receive `cache-refresh-only` and `profiles` from the same
routing outputs. `build_linux_full` enables the checks and tests;
`build_linux_all_profiles` also selects the Linux consumers for nightly and
profile-label runs. Those runs must stay out of cache-refresh mode even when
`build_linux_full` is false. `dev/ci/check-ci-config.py` checks the caller
inputs and the allowed jobs across all three workflows, so adding an
unguarded job cannot silently restore unnecessary work on every push.

The profile rows of the `linux-test` matrix live in
`dev/ci/linux-test-profiles.py` rather than in the workflow, because a
job-level `if:` cannot see `matrix`: the `prepare-matrix` job runs the script with the
`profiles` input and publishes the rows as a job output that the matrix reads
with `fromJSON`. Each row carries a tier, `pr` for the default build profile
and `nightly` for the other four, and `check-ci-config.py` asserts that the two
tiers partition the list and that the `pr` tier is exactly the default profile.

```
pull_request | merge_group | push to main | schedule | workflow_dispatch
                              |
                          preflight
                              |
                           changes
                              |
       +----------------------+-----------------------+
       |                      |                       |
  Linux checks         build_linux_native       macOS / benchmark /
  (independent)        (if any consumer          Delta / PyArrow / docs
                        is selected)            (if selected)
                              |
                +-------------+-------------+
                |             |             |
          pr_build_linux   spark_*       iceberg_*

Every gating job reports to required_checks. A failed scheduled run also
starts nightly_report. The table below lists each caller's event policy.
```

`build_linux_native.yml` builds the default Linux `libcomet.so` once per run
with JDK 17, the Cargo `ci` profile, and the existing x86-64-v3/bfd flags.
Every selected Linux test, Spark SQL, and Iceberg caller waits for that producer
and receives `native-lib-linux` through its required `native-library-artifact`
input. Consumers keep their own Spark/JDK versions and download the library
into `native/target/release/`, where Maven expects it. Spark still pre-compiles
and shares its JVM test classes separately for each Spark/JDK version.

`compute-changes.py` derives `build_linux_native` as the union of the selected
consumer outputs, after applying path and event/label policy. The workflow
reads that single output. A Spark-patch-only change therefore gets a native
build when its Spark caller is selected, even if the Linux build is not.
Documentation-only changes, benchmark-only changes, and unrelated label
events do not start an unused native build. The event-selection regression
test checks that the producer and its consumers stay in agreement across PR,
merge-group, push, nightly, and manual runs. A macOS-only or benchmark-only label run
also skips this producer because neither job consumes the Linux artifact.

Linux lint, compile-only checks, Celeborn compatibility tests, and Rust debug
tests run in `pr_build_linux_checks.yml` as soon as change selection completes.
They run alongside the native producer and still report results if it fails.
Only the JVM/TPC test consumers in `pr_build_linux.yml` wait for the shared
artifact. Both Linux callers use the same path and event selection. Regression
checks preserve this separation and prevent independent checks from acquiring
a native-build dependency.

Rust formatting runs before native compilation and before the independent
Linux build/test jobs. Rust debug tests, macOS, and feature-specific workflows
continue to build their own binaries. The shared producer is the only writer
of the Linux CI-profile Cargo cache, and only saves it on pushes to `main`.

## What runs when

| Job in `ci.yml`         | Triggered by                                                                                                                                                                                                                                           | Routing rule                        |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------- |
| `preflight`             | every PR / merge group / push / schedule / dispatch / label                                                                                                                                                                                            | none (always runs)                  |
| `changes`               | every PR / merge group / push / schedule / dispatch / label                                                                                                                                                                                            | runs `dev/ci/compute-changes.py`    |
| `build_linux_native`    | any selected Linux/Spark/Iceberg consumer, including nightly and profile-label runs                                                                                                                                                                    | `dev/ci/compute-changes.py`         |
| `pr_build_linux_checks` | same selection as `pr_build_linux`; cache-refresh and nightly inputs skip unnecessary checks                                                                                                                                                           | `dev/ci/compute-changes.py`         |
| `pr_build_linux`        | PR, merge group or push to main, paths matched; on push only the cache-writing jobs, via `build_linux_full`; the test matrix's non-default Spark profiles only in the nightly run **or** with `run-all-spark-profiles`, via `build_linux_all_profiles` | `dev/ci/compute-changes.py`         |
| `pr_build_macos`        | merge group, **or** PR with `run-macos-tests`                                                                                                                                                                                                          | `dev/ci/compute-changes.py`         |
| `pr_benchmark_check`    | merge group, **or** PR with `run-benchmark-check`                                                                                                                                                                                                      | benchmark sources only              |
| `delta_build_gate`      | merge group, **or** PR with `run-delta-build-gate`                                                                                                                                                                                                     | main sources, poms, `contrib/delta` |
| `pyarrow_udf_test`      | merge group, **or** PR with `run-pyarrow-udf-tests`                                                                                                                                                                                                    | map-in-batch and Python runner code |
| `docs`                  | push to main, paths matched                                                                                                                                                                                                                            | `.asf.yaml`, `docs/**`, `docs.yaml` |
| `spark_3_5`             | nightly, **or** PR with `run-spark-3.5-tests`                                                                                                                                                                                                          | Spark 3.5 sources                   |
| `spark_4_1`             | merge group, **or** PR with `run-spark-4.1-tests`; the `sql_hive` shards alone with `run-spark-4.1-hive-tests`                                                                                                                                         | Spark 4.1 sources                   |
| `spark_3_4`             | PR with `run-spark-3.4-tests`, or dispatch                                                                                                                                                                                                             | Spark 3.4 sources                   |
| `spark_4_0`             | nightly, **or** PR with `run-spark-4.0-tests`                                                                                                                                                                                                          | Spark 4.0 sources                   |
| `iceberg_1_11`          | merge group, **or** PR with `run-iceberg-tests`                                                                                                                                                                                                        | Iceberg sources                     |
| `iceberg_1_8`           | nightly, **or** PR with `run-iceberg-tests`                                                                                                                                                                                                            | Iceberg sources                     |
| `iceberg_1_9`           | nightly, **or** PR with `run-iceberg-tests`                                                                                                                                                                                                            | Iceberg sources                     |
| `iceberg_1_10`          | nightly, **or** PR with `run-iceberg-tests`                                                                                                                                                                                                            | Iceberg sources                     |
| `required_checks`       | always, after every job above except `docs`                                                                                                                                                                                                            | none (always runs)                  |
| `nightly_report`        | schedule only, after `required_checks`, when it is not green                                                                                                                                                                                           | none                                |

A heavy job appears in the PR's checks list as a `skipped` entry whenever
its path filter or event criteria don't match. Skipped checks count as
passing for branch protection, so a name that can report `skipped` is not
safe to make a required check.

### Label events

`ci.yml` also fires on `pull_request.types: [labeled]`, so applying
`run-spark-3.4-tests`, `run-spark-3.5-tests`, `run-spark-4.0-tests`,
`run-spark-4.1-tests`, `run-spark-4.1-hive-tests`, `run-all-spark-profiles`,
`run-iceberg-tests`, `run-macos-tests`, `run-delta-build-gate`,
`run-pyarrow-udf-tests`, or `run-benchmark-check` starts the jobs that label gates without needing a new
push. GitHub cannot filter a
`pull_request` trigger by label name, so **every** label added to a PR starts a
run, including labels that gate nothing.

Three rules keep those runs from corrupting the PR's status:

- `preflight` and `changes` carry no event guard and run every time. A job held
  back by `if:` still publishes a check run under its own name with conclusion
  `skipped`, and the newest check run for a name is what the merge box,
  `gh pr checks` and required-status-check evaluation read. Guarding
  `preflight` on the label name used to let any unrelated label overwrite the
  commit run's real `Preflight` verdict with `skipped`, see
  [#5007](https://github.com/apache/datafusion-comet/issues/5007).
- On a `labeled` event, `POLICY` reports false for every job the new label does
  not gate. Without that, applying a single label re-ran the entire heavy
  pipeline at a commit that had already been tested.
- On a `labeled` event, `required_checks` publishes its verdict as
  `Required Checks (label run)`, not `Required Checks`. Because the PR tier is
  skipped on that event, the label run's aggregate says nothing about the
  commit's applicable suites, and GitHub keeps only the most recent check run
  per name per commit. Under the real name, a `dependencies` label landing a
  minute after a push would mark the commit green while the commit run was
  still going. Skipping the job would not help either, since a skipped check
  run still carries the name and still counts as passing.

`run-spark-4.1-tests` and `run-spark-4.1-hive-tests` both feed the single
`spark_4_1` call. The first sets both of its POLICY outputs and runs every
module; the second sets only `spark_4_1_hive` and runs only the `sql_hive`
shards. Adding the hive label to a pull request that already carries the suite
label re-runs just the hive rows, since a `labeled` run selects what the new
label gates.

The opt-in labels have to exist in repository settings before they can be
applied; `contains()` on a label nobody can add is simply always false, which
makes the escape hatch look like it silently does nothing.

### Nightly tier

`ci.yml` also fires on a `schedule` (06:00 UTC daily). On that event `changes`
diffs `main` against the commit the last successful scheduled run tested
(`dev/ci/nightly-base.py` looks it up through the Actions API), so the nightly
is routed by the same `FILTERS` as every other event and `POLICY` selects the
`nightly` tier alone: the queue already ran every `queue` job against the tree
that is now `main`. Every commit is covered exactly once, and a red nightly
keeps its commits in scope until a green one supersedes it. A quiet day diffs
to nothing and runs nothing; a docs-only day runs nothing either.

When there is no such run, the API is unreachable, or the base is no longer on
`main`, there is no base to diff against and the run treats every tracked file
as changed, so the whole nightly tier runs. Guessing a narrower base — a fixed
time window, say — would be worse than useless: a window that starts after a
commit no nightly has covered yet skips the suites that commit needs, lets the
run go green, and then hands that green head to `nightly-base.py` as tomorrow's
base, so the coverage is dropped for good.

With `profiles: nightly`, `pr_build_linux.yml` runs only `prepare-matrix`
and `linux-test`, consuming the shared native producer's library. The
independent checks workflow runs only its short Rust formatting prerequisite.
Rust debug tests, other lints and both TPC jobs carry
`if: ${{ inputs.profiles != 'nightly' }}` because they already ran at this
commit. `check-ci-config.py` pins this scope across the three Linux workflows
alongside the cache-refresh guards.

A red nightly has no pull request to appear on, and GitHub only emails a
scheduled run's failure to whoever last touched the workflow file, so
`nightly_report` runs after `required_checks` on the scheduled event and, when
the aggregate is not green, opens an issue labelled `ci-nightly-failure` listing
the jobs that failed. If one is already open it comments there instead, so
consecutive red nights accumulate in one issue. Closing the issue is how the
failure is acknowledged; the next red night opens a new one. The label has to
exist in repository settings, like the `run-*` labels above.

## Standalone workflows (not under the umbrella)

These workflows have their own triggers because they fire on events the
umbrella doesn't watch, or operate independently of the rest of CI:

| File                   | Why standalone                                                                                       |
| ---------------------- | ---------------------------------------------------------------------------------------------------- |
| `pr_title_check.yml`   | Fires on `pull_request.types: [edited]` so it re-runs when a PR title is edited without a code push. |
| `codeql.yml`           | Security scanner; weekly schedule + on every push/PR.                                                |
| `miri.yml`             | Nightly Miri safety checks.                                                                          |
| `publish_snapshot.yml` | Nightly SNAPSHOT jars to repository.apache.org; skips when main has not changed. `dry_run` dispatch. |
| `stale.yml`            | Daily stale-PR closer.                                                                               |
| `take.yml`             | Issue-comment trigger for `take` / `untake`.                                                         |
| `label_new_issues.yml` | Issue trigger to apply `requires-triage`.                                                            |
| `label_prs.yml`        | Runs on `pull_request_target` so it can label pull requests opened from forks.                       |

## Reusable workflows (called by `ci.yml`)

| File                              | Called from `ci.yml` job(s)                                  |
| --------------------------------- | ------------------------------------------------------------ |
| `build_linux_native.yml`          | `build_linux_native`                                         |
| `pr_build_linux_checks.yml`       | `pr_build_linux_checks`                                      |
| `pr_build_linux.yml`              | `pr_build_linux`                                             |
| `pr_build_macos.yml`              | `pr_build_macos`                                             |
| `pr_benchmark_check.yml`          | `pr_benchmark_check`                                         |
| `delta_build_gate.yml`            | `delta_build_gate`                                           |
| `pyarrow_udf_test.yml`            | `pyarrow_udf_test`                                           |
| `docs.yaml`                       | `docs`                                                       |
| `spark_sql_test_reusable.yml`     | `spark_3_4`, `spark_3_5`, `spark_4_0`, `spark_4_1`           |
| `iceberg_spark_test_reusable.yml` | `iceberg_1_8`, `iceberg_1_9`, `iceberg_1_10`, `iceberg_1_11` |

## Changing what runs when

Consumer jobs in `ci.yml` use their routing outputs, for example:

```yaml
if: needs.changes.outputs.spark_3_5 == 'true'
```

Spark 4.1 has separate core and Hive outputs selecting the same caller:

```yaml
if: needs.changes.outputs.spark_4_1 == 'true' || needs.changes.outputs.spark_4_1_hive == 'true'
```

`NATIVE_CONSUMERS` in `dev/ci/compute-changes.py` maps each native consumer
job to all outputs that can select it. The shared native producer runs when
any of those outputs is true. A Hive-only label event has `spark_4_1=false`
and `spark_4_1_hive=true`, so it still starts the native producer. Likewise,
`build_linux_all_profiles=true` starts it when `build_linux=false`, covering
nightly runs and newly applied `run-all-spark-profiles` labels. The
configuration guard checks the exact callers, output exports, dependencies,
and gates against this mapping.

Each routing output folds together two separate decisions, both of which live
in `dev/ci/compute-changes.py`:

- **`FILTERS`** — which files the job covers. Pattern semantics match
  dorny/picomatch (`**` spans path segments, `*` stays within one, a leading
  `!` excludes).
- **`POLICY`** — which events may run it. `"pr"` for every pull request,
  `"queue"` for the merge queue, `"nightly"` for the scheduled run, `"push"`
  for push to main, `"label:<name>"` for opt-in on a labelled pull request.
  `"pr"` and `"label:"` are mutually exclusive. `workflow_dispatch` always
  runs everything.

Moving a suite between the PR, queue and nightly tiers is a one-word edit to
`POLICY`, plus the matching entry in `PR_TIER`, `QUEUE_TIER` or `NIGHTLY_TIER`
in `dev/ci/check-ci-config.py`, which is written out longhand on purpose so
that a tier change has to be stated twice.

An output does not have to map one-to-one onto a job. Two outputs can feed a
single call when part of a workflow belongs in a different tier from the rest:
`spark_4_1` / `spark_4_1_hive` select which module shards the one Spark 4.1
build runs, and `build_linux` / `build_linux_full` / `build_linux_all_profiles`
select whether the Linux build runs everything, only the jobs that populate
`main`'s caches, or the test matrix against every Spark profile rather than
the default one. Each group shares its `FILTERS` list by assignment so the
entries cannot drift.

So adding a suite, moving sources, or changing when something runs is an edit
to one of those two tables, not to ten `${{ }}` expressions. Keeping the policy
in Python is also what makes it testable: GitHub expressions cannot be
exercised outside a real workflow run, whereas `POLICY_CASES` in
`dev/ci/check-ci-config.py` pins the expected job set for each event shape.

A file that a job reads but that no filter lists is silent: the job skips,
and the edit merges with only `preflight` having looked at it. The shared
build inputs (`mvnw`, `.mvn/**`, the local composite actions) are pinned by
a routing table in `dev/ci/check-ci-config.py`, which `preflight` runs.

## Artifact names must be unique per producer

Artifact names are scoped to the workflow **run**, not to the calling
workflow. `build_linux_native.yml` is called exactly once and is the sole
producer of `native-lib-linux`. Its consumers declare a required
`native-library-artifact` input; `ci.yml` passes that name and makes every
consumer depend on the shared producer. They download the existing artifact
without publishing copies under version-specific names.

Artifacts with multiple producers still carry their version inputs. For
example, `spark_sql_test_reusable.yml` publishes
`jvm-compiled-spark-${{ inputs.spark-full }}-jdk${{ inputs.java }}` because
each Spark version has different compiled classes. Publishing two artifacts
under the same name can make a download select a sibling's artifact and let
an upload retry overwrite that sibling's output.

`dev/ci/check-ci-config.py` verifies both contracts: local uploads and
downloads must match, and shared native-library consumers must be wired to
the one declared producer. Artifact retention remains one day; a failed-job
rerun can reuse a successful producer's artifact during that retention
window. If it has expired, rerun the full workflow to rebuild it.

## Retrying flaky network operations

**Maven.** `.mvn/maven.config` tunes the Maven Resolver HTTP transport: six
retries instead of three, `408/429/500/502/503/504` retryable instead of only
`429/503`, a 30s connect timeout and a 10 minute socket read timeout. The
wrapper pins `maven.multiModuleProjectDirectory` to the directory holding
`.mvn`, so one file covers every `mvnw` invocation in CI (including
`cd spark && ../mvnw ...`) with no per-workflow wiring.

The Wagon transport (`-Dmaven.resolver.transport=wagon`, `maven.wagon.http.*`)
was evaluated and rejected: it is deprecated in Resolver 1.9 and removed in
Maven 4, its retry knobs mirror the native transport's, and its
service-unavailable retry strategy defaults to `none`, so adopting it would
first have to buy back the `429/503` retry we already get. The one thing it
can still do that the native transport cannot is shrink HttpClient's
non-retryable exception list (`retryHandler.class=default` plus
`retryHandler.nonRetryableClasses=...`), the only way to retry a connect or
read timeout. We size those timeouts not to fire instead.

**Artifact upload.** `actions/upload-artifact` fails the job when
`FinalizeArtifact` returns `(403) Forbidden: Error from intermediary`, even
though the content already uploaded. Its client only retries
`429/500/502/503/504`, exposes no input to widen that, and Actions has no
built-in step retry. Use `./.github/actions/upload-artifact-retry` instead for
any artifact a later job consumes: same inputs and outputs, three attempts,
15s then 45s backoff. Attempts 2 and 3 force `overwrite: true`, so the name
must belong to exactly one producer in the run (see above). The uploads inside
`./.github/actions/java-test` stay on the plain action, since a local action
calling another local action is untested here. Its two failure-only uploads run
on jobs that are already red. Its test-report upload also runs on green jobs
and is `continue-on-error: true`: nothing downstream consumes the reports, and
a `FinalizeArtifact` 403 must not turn a passing test run into a red check.

**Artifact download.** `actions/download-artifact` has the same narrow retry
list, so a `ListArtifacts` answered `(403) Forbidden: Error from intermediary`
fails the job before a byte is fetched, most often the first step of a test
shard that then never runs. `./.github/actions/download-artifact-retry` wraps
it the same way: three attempts, 15s then 45s backoff, same inputs and
`download-path` output. A retry has nothing to undo, since a failed attempt
leaves at most a partial extraction that the next one overwrites. The
`merge-fallback-logs` job stays on the plain action because it skips checkout,
which a local action needs; `dev/ci/check-ci-config.py` enforces that pairing
for every `uses: ./.github/actions/...` in a workflow, and treats both
spellings as a download when pairing consumers with producers.

**Tool downloads.** `Lint Scala (syntactic)` splits the coursier download from
the lint: a `Fetch scalafix` step retries a no-op `cs launch ... -- --version`
three times, and the check itself then runs `cs launch --mode offline` against
the populated cache, so a nonzero exit there can only be a lint violation.
`preflight` retries the actionlint download the same way, and fetches the
installer to a file rather than piping it into `bash` so a truncated download
cannot run a partial script.

Every one of these steps runs after the test verdict is already known, or
before any test has started. Once `Required Checks` is a required context
(see below), a red job from any of them evicts the PR from the merge queue,
which is why plain network flakes are worth retrying rather than re-running
the whole pipeline by hand.

**Maven wrapper bootstrap.** `./mvnw` downloads the Maven distribution itself on
a cold runner, and a blip from `repo.maven.apache.org` fails the job before
anything is compiled. `./.github/actions/maven-bootstrap` caches that
distribution under `~/.m2/wrapper/dists` (keyed on
`.mvn/wrapper/maven-wrapper.properties`, not `pom.xml`) and retries
`./mvnw --version` four times with exponential backoff. It retries only the
bootstrap, never compilation or test execution.

Any job whose first Maven use is a bare `./mvnw` needs this step before it.
`./.github/actions/java-test` carries its own inline copy rather than calling
the composite, because a local action invoking another local action is
deliberately avoided here (see the artifact-upload note above).

The independent Java lint, Spark 4.1 compile, and Celeborn compatibility jobs
in `pr_build_linux_checks.yml` each bootstrap Maven, as do the two TPC jobs
in `pr_build_linux.yml`. The configuration guard scans both workflows and
requires an earlier unconditional bootstrap step for every direct `./mvnw`
command; a bootstrap that ignores failure does not satisfy the guard.

## Merge queue

`.asf.yaml` declares a `Merge Queue` ruleset for the default branch, so `main`
is only writable through the queue. `.asf.yaml` rulesets accept a raw GitHub
Rulesets API payload, which is how a `merge_queue` rule gets set without an
INFRA ticket.

When a PR is queued, GitHub builds a temporary `gh-readonly-queue/main/...`
branch containing the PR's commits on top of the current `main` and of every
entry ahead of it in the queue, then fires a `merge_group` event. `ci.yml` runs
against that branch and the entry merges when `Required Checks` is green. That
is what makes the queue tier meaningful: it tests the merge result, not the PR
head, so a semantic conflict between two PRs that each pass in isolation is
caught before either lands.

The `merge_queue` rule parameters in `.asf.yaml` are the tuning dials, and
`max_entries_to_build: 2` is the one that matters. Every entry gets its own
`merge_group` build — [merge limits do not combine
builds](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/configuring-pull-request-merges/managing-a-merge-queue) —
so this caps how many pipelines are in flight, and with them how fast the queue
drains: roughly 19 merges a day at the ~2.5h pipeline we see today.
`max_entries_to_merge: 5` only says how many already-green entries land in one
merge operation, and saves no CI at all. The saving in this design comes from
the PR tier being small, not from batching inside the queue.
`check_response_timeout_minutes: 300` has to stay comfortably above the slowest
observed pipeline plus ASF runner scheduling delay, or healthy entries get
evicted.

A flaky test in the queue tier blocks everyone's merges, not just one PR. That
raises the bar on flakiness relative to when these suites only ran post-merge.

## Branch protection

`main` is protected by two things that GitHub evaluates together, applying the
most restrictive result:

- classic branch protection, from `github.protected_branches.main` in
  `.asf.yaml`: one approving review, and `Required Checks` as the sole
  required status check;
- the `Merge Queue` ruleset, from `github.rulesets` in the same file.

Release branches (`branch-N.M`) keep plain branch protection with no queue.
Merge queue rules do not accept wildcard ref patterns, so the ruleset targets
`~DEFAULT_BRANCH` only.

`apache/root` (ASF Infra, team id `118420`) is a bypass actor on the ruleset,
so a wedged queue can always be recovered without a Jira ticket.

`Required Checks` is the one context `main` requires, because a caller of a
reusable workflow publishes a _different check name_ depending on whether it
ran:

| Caller state     | Check runs published                                     |
| ---------------- | -------------------------------------------------------- |
| skipped by `if:` | one run named exactly `PR Build (Linux)`, `skipped`      |
| ran              | only `PR Build (Linux) / Spark 4.1, JDK 17 [exec]`, etc. |

No name is reported in both cases, so neither can be required directly.
`required_checks` is flat, reports on every event, runs `if: always()` and
treats `skipped` as a pass, so it only goes red on `failure` or `cancelled`.

Editing `required_status_checks` deserves care. A context that never reports
blocks every merge to `main`, including the merge that would revert the
mistake, and only INFRA can remove a required check by hand at that point.
`dev/ci/check-ci-config.py` enforces that every `ci.yml` job except `docs`
appears in `required_checks.needs`, that the job's `name:` is the expression
that routes `labeled` runs to a separate name (see "Label events" above), that
the commit-run name matches the context `.asf.yaml` requires for `main`, and
that the label-run name is never the one required. All of that is silent when
broken. What it cannot catch is a job that is configured never to run.
