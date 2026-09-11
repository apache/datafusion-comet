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
ruleset in `.asf.yaml`. That splits CI into two tiers:

- **PR tier** (`pr`): fast feedback while a change is being iterated on.
  The Linux build, Spark 4.1 and Iceberg 1.11.
- **Queue tier** (`queue`): the authoritative gate. Everything the PR tier
  runs, plus the macOS build, the benchmark compile check, Spark 3.4/3.5/4.0
  and Iceberg 1.8/1.9/1.10, evaluated against the merge result rather than
  against the PR head.

Every queue-only job has a `run-*` label that opts a pull request into it
early, listed in the table below.

Most heavy jobs have no `push` tier. The queue already tested the exact tree that
lands, so re-running them on push to main would double the cost of every
merge. Two routes are still on `push`: `docs`, because it deploys to `asf-site`
and has to run after the commit is on main, and `build_linux`, because of
`actions/cache` scoping. The Linux route selects `pr_build_linux_checks` and
`pr_build_linux`, with the shared `build_linux_native` producer supplying the
latter. Spark SQL and Iceberg consumers stay off on push. A pull request can
only restore caches saved on its own branch or on `main`, and the queue runs on a throwaway
`gh-readonly-queue/*` branch whose caches are deleted with it. Without a push
run, a `Cargo.lock` or `pom.xml` change would leave the cargo-registry, Maven
and TPC-H/TPC-DS caches on `main` stale until the next unrelated change.

```
pull_request | merge_group | push to main | workflow_dispatch
                       |
                   preflight
                       |
                    changes
                       |
       +---------------+--------------------+
       |                                    |
  Linux checks / macOS /               build_linux_native
  docs / benchmark (if selected)       (if any consumer is selected)
                                            |
                           +----------------+----------------+
                           |                |                |
                     pr_build_linux     spark_3_* /      iceberg_1_*
                                        spark_4_*

Every job above reports to required_checks (except the docs deployment).
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
merge-group, push, and manual runs. A macOS-only or benchmark-only label run
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
of the Linux CI-profile Cargo cache, and only writes on `main`.

## What runs when

| Job in `ci.yml`         | Triggered by                                      | Routing rule                        |
| ----------------------- | ------------------------------------------------- | ----------------------------------- |
| `preflight`             | every PR / merge group / push / dispatch / label  | none (always runs)                  |
| `changes`               | every PR / merge group / push / dispatch / label  | runs `dev/ci/compute-changes.py`    |
| `build_linux_native`    | any selected Linux/Spark/Iceberg consumer         | `dev/ci/compute-changes.py`         |
| `pr_build_linux_checks` | PR, merge group or push to main, paths matched    | `dev/ci/compute-changes.py`         |
| `pr_build_linux`        | PR, merge group or push to main, paths matched    | `dev/ci/compute-changes.py`         |
| `pr_build_macos`        | merge group, **or** PR with `run-macos-tests`     | `dev/ci/compute-changes.py`         |
| `pr_benchmark_check`    | merge group, **or** PR with `run-benchmark-check` | benchmark sources only              |
| `docs`                  | push to main, paths matched                       | `.asf.yaml`, `docs/**`, `docs.yaml` |
| `spark_3_5`             | merge group, **or** PR with `run-spark-3.5-tests` | Spark 3.5 sources                   |
| `spark_4_1`             | PR or merge group, paths matched                  | Spark 4.1 sources                   |
| `spark_3_4`             | merge group, **or** PR with `run-spark-3.4-tests` | Spark 3.4 sources                   |
| `spark_4_0`             | merge group, **or** PR with `run-spark-4.0-tests` | Spark 4.0 sources                   |
| `iceberg_1_11`          | PR or merge group, paths matched                  | Iceberg sources                     |
| `iceberg_1_8`           | merge group, **or** PR with `run-iceberg-tests`   | Iceberg sources                     |
| `iceberg_1_9`           | merge group, **or** PR with `run-iceberg-tests`   | Iceberg sources                     |
| `iceberg_1_10`          | merge group, **or** PR with `run-iceberg-tests`   | Iceberg sources                     |
| `required_checks`       | always, after every job above except `docs`       | none (always runs)                  |

A heavy job appears in the PR's checks list as a `skipped` entry whenever
its path filter or event criteria don't match. Skipped checks count as
passing for branch protection, so a name that can report `skipped` is not
safe to make a required check.

### Label events

`ci.yml` also fires on `pull_request.types: [labeled]`, so applying
`run-spark-3.4-tests`, `run-spark-3.5-tests`, `run-spark-4.0-tests`,
`run-iceberg-tests`, `run-macos-tests`, or `run-benchmark-check` starts the
jobs that label gates without needing a new push. GitHub cannot filter a
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

`run-spark-4.1-tests` gates nothing: `spark_4_1` already runs on every PR.

The opt-in labels have to exist in repository settings before they can be
applied; `contains()` on a label nobody can add is simply always false, which
makes the escape hatch look like it silently does nothing.

## Standalone workflows (not under the umbrella)

These workflows have their own triggers because they fire on events the
umbrella doesn't watch, or operate independently of the rest of CI:

| File                   | Why standalone                                                                                       |
| ---------------------- | ---------------------------------------------------------------------------------------------------- |
| `pr_title_check.yml`   | Fires on `pull_request.types: [edited]` so it re-runs when a PR title is edited without a code push. |
| `codeql.yml`           | Security scanner; weekly schedule + on every push/PR.                                                |
| `miri.yml`             | Nightly Miri safety checks.                                                                          |
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
| `docs.yaml`                       | `docs`                                                       |
| `spark_sql_test_reusable.yml`     | `spark_3_4`, `spark_3_5`, `spark_4_0`, `spark_4_1`           |
| `iceberg_spark_test_reusable.yml` | `iceberg_1_8`, `iceberg_1_9`, `iceberg_1_10`, `iceberg_1_11` |

## Changing what runs when

Consumer jobs in `ci.yml` use a single routing output, for example:

```yaml
if: needs.changes.outputs.spark_3_5 == 'true'
```

The shared native producer runs when any of its consumers' outputs is true.

That single boolean folds together two separate decisions, both of which live
in `dev/ci/compute-changes.py`:

- **`FILTERS`** — which files the job covers. Pattern semantics match
  dorny/picomatch (`**` spans path segments, `*` stays within one, a leading
  `!` excludes).
- **`POLICY`** — which events may run it. `"pr"` for every pull request,
  `"queue"` for the merge queue, `"push"` for push to main, `"label:<name>"`
  for opt-in on a labelled pull request. `"pr"` and `"label:"` are mutually
  exclusive. `workflow_dispatch` always runs everything.

Moving a suite between the PR and queue tiers is a one-word edit to `POLICY`.

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

**Maven wrapper bootstrap.** `./.github/actions/java-test` retries
`./mvnw --version` with exponential backoff, so a failed download of the Maven
distribution does not surface as a test failure.

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
