# GitHub Workflows

GitHub Actions only loads `*.yml` / `*.yaml` files in this directory as
workflows. This README is ignored by the runner.

## Pipeline overview

A single umbrella workflow (`ci.yml`) orchestrates everything that runs on
pull requests and pushes to `main`. The umbrella runs cheap **preflight**
checks first, computes which heavy jobs are relevant to the change, and only
then fans out to the long-running test/build workflows. Each long workflow
is a `workflow_call` reusable invoked from the umbrella.

```
                        pull_request | push to main | workflow_dispatch
                                            |
                                            v
                                +-----------------------+
                                |       preflight       |  ubuntu-slim
                                |  (RAT, prettier,      |
                                |   missing-suites,     |
                                |   actionlint)         |
                                +-----------+-----------+
                                            |  on success
                                            v
                                +-----------------------+
                                |        changes        |  ubuntu-slim
                                | (compute-changes.py:  |
                                |  one boolean per      |
                                |  heavy job)           |
                                +-----------+-----------+
                                            |
        +-----------------------------------+-----------------------------------+
        |                                   |                                   |
        v                                   v                                   v
  every PR + push                     push to main only         PR with label, or push
  ---------------                     -----------------         ----------------------
  pr_build_linux                      docs                      spark_3_4    run-spark-3.4-tests
  pr_build_macos                                                spark_4_0    run-spark-4.0-tests
  pr_benchmark_check                                            iceberg_1_8  run-iceberg-tests
  spark_3_5                                                     iceberg_1_9  run-iceberg-tests
  spark_4_1                                                     iceberg_1_10 run-iceberg-tests
  iceberg_1_11

        |                                   |                                   |
        +-----------------------------------+-----------------------------------+
                                            v
                                +-----------------------+
                                |    required_checks    |  ubuntu-slim
                                |  one flat name that   |
                                |  is safe to require   |
                                +-----------------------+

  reusable workflows invoked via `uses:`:
    pr_build_linux.yml         spark_sql_test_reusable.yml
    pr_build_macos.yml         iceberg_spark_test_reusable.yml
    pr_benchmark_check.yml
    docs.yaml
```

## What runs when

| Job in `ci.yml`      | Triggered by                                        | Routing rule                        |
| -------------------- | --------------------------------------------------- | ----------------------------------- |
| `preflight`          | every PR / push to main / dispatch / PR label added | none (always runs)                  |
| `changes`            | every PR / push to main / dispatch / PR label added | runs `dev/ci/compute-changes.py`    |
| `pr_build_linux`     | PR or push, paths matched                           | `dev/ci/compute-changes.py`         |
| `pr_build_macos`     | PR or push, paths matched                           | `dev/ci/compute-changes.py`         |
| `pr_benchmark_check` | PR or push, paths matched                           | benchmark sources only              |
| `docs`               | push to main, paths matched                         | `.asf.yaml`, `docs/**`, `docs.yaml` |
| `spark_3_5`          | PR or push, paths matched                           | Spark 3.5 sources                   |
| `spark_4_1`          | PR or push, paths matched                           | Spark 4.1 sources                   |
| `spark_3_4`          | push, **or** PR with `run-spark-3.4-tests` label    | Spark 3.4 sources                   |
| `spark_4_0`          | push, **or** PR with `run-spark-4.0-tests` label    | Spark 4.0 sources                   |
| `iceberg_1_11`       | PR or push, paths matched                           | Iceberg sources                     |
| `iceberg_1_8`        | push, **or** PR with `run-iceberg-tests` label      | Iceberg sources                     |
| `iceberg_1_9`        | push, **or** PR with `run-iceberg-tests` label      | Iceberg sources                     |
| `iceberg_1_10`       | push, **or** PR with `run-iceberg-tests` label      | Iceberg sources                     |
| `required_checks`    | always, after every job above except `docs`         | none (always runs)                  |

A heavy job appears in the PR's checks list as a `skipped` entry whenever
its path filter or event criteria don't match. Skipped checks count as
passing for branch protection, so a name that can report `skipped` is not
safe to make a required check.

### Label events

`ci.yml` also fires on `pull_request.types: [labeled]`, so applying
`run-spark-3.4-tests`, `run-spark-4.0-tests` or `run-iceberg-tests` starts the
job that label gates without needing a new push. GitHub cannot filter a
`pull_request` trigger by label name, so **every** label added to a PR starts a
run, including labels that gate nothing.

Two rules keep those runs from corrupting the PR's status:

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

`run-spark-4.1-tests` gates nothing: `spark_4_1` already runs on every PR.

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
| `pr_build_linux.yml`              | `pr_build_linux`                                             |
| `pr_build_macos.yml`              | `pr_build_macos`                                             |
| `pr_benchmark_check.yml`          | `pr_benchmark_check`                                         |
| `docs.yaml`                       | `docs`                                                       |
| `spark_sql_test_reusable.yml`     | `spark_3_4`, `spark_3_5`, `spark_4_0`, `spark_4_1`           |
| `iceberg_spark_test_reusable.yml` | `iceberg_1_8`, `iceberg_1_9`, `iceberg_1_10`, `iceberg_1_11` |

## Changing what runs when

Every heavy job in `ci.yml` is gated on exactly one thing:

```yaml
if: needs.changes.outputs.spark_3_5 == 'true'
```

That single boolean folds together two separate decisions, both of which live
in `dev/ci/compute-changes.py`:

- **`FILTERS`** — which files the job covers. Pattern semantics match
  dorny/picomatch (`**` spans path segments, `*` stays within one, a leading
  `!` excludes).
- **`POLICY`** — which events may run it. `"pr"` for every pull request,
  `"push"` for push to main, `"label:<name>"` for opt-in on a labelled pull
  request. `"pr"` and `"label:"` are mutually exclusive. `workflow_dispatch`
  always runs everything.

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
workflow. `ci.yml` calls `spark_sql_test_reusable.yml` once per Spark
version and `iceberg_spark_test_reusable.yml` once per Iceberg version, all
inside the same run, so an unqualified name like `native-lib-linux` would be
claimed by several producers at once. That breaks two things:

- `download-artifact` resolves a name to the highest matching artifact ID.
  Nothing ties it to the producer the consumer declared in `needs`.
- `upload-artifact` with `overwrite: true` deletes the newest record with
  that name before uploading, which can be a sibling's finished artifact.
  The retry wrapper below forces `overwrite` on attempts 2 and 3.

So every artifact published by a reusable workflow that `ci.yml` calls more
than once carries its version inputs, e.g.
`native-lib-spark-4.1.3-jdk17`. `dev/ci/check-ci-config.py` enforces this,
and also that every `download-artifact` name is produced by an upload in the
same workflow.

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
must belong to exactly one producer in the run (see above). The diagnostic
uploads inside `./.github/actions/java-test` stay on the plain action, since a
local action calling another local action is untested here and those run only
on already-failing jobs.

**Maven wrapper bootstrap.** `./.github/actions/java-test` retries
`./mvnw --version` with exponential backoff, so a failed download of the Maven
distribution does not surface as a test failure.

## Branch protection

`.asf.yaml` declares no `required_status_checks` for `main` today, only
`required_approving_review_count: 1`.

Adding one is not as simple as naming a job, because a caller of a reusable
workflow publishes a _different check name_ depending on whether it ran:

| Caller state     | Check runs published                                     |
| ---------------- | -------------------------------------------------------- |
| skipped by `if:` | one run named exactly `PR Build (Linux)`, `skipped`      |
| ran              | only `PR Build (Linux) / Spark 4.1, JDK 17 [exec]`, etc. |

No name is reported in both cases. Requiring the bare name would block every
code change; requiring a nested name would block every docs-only change. Both
hang waiting for a check that never arrives rather than failing, and a required
context that never reports also blocks the merge that would fix `.asf.yaml`.
Recovering from that needs an INFRA Jira ticket.

The `required_checks` job at the bottom of `ci.yml` exists to be the one name
that is safe to require. It is flat, so it reports on every event; it runs
`if: always()` and treats `skipped` as a pass, so it only goes red when an
upstream job reports `failure` or `cancelled`.

`dev/ci/check-ci-config.py` enforces that every `ci.yml` job except `docs`
appears in `required_checks.needs`. Once `.asf.yaml` does declare a required
context for `main`, it also enforces that the job's `name:` still matches it.
Both sides of that pair are silent when broken and expensive to recover from.
