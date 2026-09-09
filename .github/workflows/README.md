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

  reusable workflows invoked via `uses:`:
    pr_build_linux.yml         spark_sql_test_reusable.yml
    pr_build_macos.yml         iceberg_spark_test_reusable.yml
    pr_benchmark_check.yml
    docs.yaml
```

## What runs when

| Job in `ci.yml`      | Triggered by                                        | Path filter source                  |
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
- Every heavy job excludes `labeled` events unless the label just added is the
  one that gates it. Without that, applying a single label re-ran the entire
  heavy pipeline at a commit that had already been tested.

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

## Modifying path filters

Each long workflow's "what files trigger me" rules live in the `FILTERS`
dict at the top of `dev/ci/compute-changes.py`. The `changes` job in
`ci.yml` invokes that script and the gate `if:` on each long job consumes
`needs.changes.outputs.<name>`. When adding a new test suite or moving
sources, update the relevant filter entry there.

## Branch protection

Required-check names changed when these workflows were consolidated. The
umbrella exposes per-job names like `CI / pr_build_linux / Lint`,
`CI / spark_3_5 / linux-test (...)`, etc. Update repository branch
protection rules to point at the new names; the old standalone workflow
names (`Spark SQL Tests (Spark 3.5)`, `PR Build (Linux)`, ...) no longer
exist as top-level workflows.

`.asf.yaml` currently declares no `required_status_checks` for `main`, only
`required_approving_review_count: 1`. Anything added there must be a name that
never legitimately reports `skipped`, because GitHub counts a skipped check as
passing. The bare caller-job names (`PR Build (Linux)`, `Spark SQL Tests
(Spark 3.5)`, ...) are not such names: a reusable workflow that actually runs
publishes only its child jobs, so the bare name shows up solely when the caller
was skipped.
