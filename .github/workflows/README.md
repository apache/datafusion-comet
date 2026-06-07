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
        +-----------+-----------+-----------+-----------+-----------+-----------+
        |           |           |           |           |           |           |
        v           v           v           v           v           v           v
  pr_build_     pr_build_   pr_benchmark_  docs    spark_3_5    spark_4_0   iceberg_1_10
   linux         macos        check       (push)   (PR+push)   (PR+push)    (PR+push)
  (PR+push)    (PR+push)    (PR+push)
                                                       |           |           |
                                                       v           v           v
                                            spark_3_4 / spark_4_1   iceberg_1_8 / 1_9
                                            (push or PR + label)    (push only)

  reusable workflows invoked via `uses:`:
    pr_build_linux.yml         spark_sql_test_reusable.yml
    pr_build_macos.yml         iceberg_spark_test_reusable.yml
    pr_benchmark_check.yml
    docs.yaml
```

## What runs when

| Job in `ci.yml`      | Triggered by                                     | Path filter source                  |
| -------------------- | ------------------------------------------------ | ----------------------------------- |
| `preflight`          | every PR / push to main / dispatch               | none (always runs)                  |
| `changes`            | every PR / push to main / dispatch               | runs `dev/ci/compute-changes.py`    |
| `pr_build_linux`     | PR or push, paths matched                        | `dev/ci/compute-changes.py`         |
| `pr_build_macos`     | PR or push, paths matched                        | `dev/ci/compute-changes.py`         |
| `pr_benchmark_check` | PR or push, paths matched                        | benchmark sources only              |
| `docs`               | push to main, paths matched                      | `.asf.yaml`, `docs/**`, `docs.yaml` |
| `spark_3_5`          | PR or push, paths matched                        | Spark 3.5 sources                   |
| `spark_4_0`          | PR or push, paths matched                        | Spark 4.0 sources                   |
| `spark_3_4`          | push, **or** PR with `run-spark-3.4-tests` label | Spark 3.4 sources                   |
| `spark_4_1`          | push, **or** PR with `run-spark-4.1-tests` label | Spark 4.1 sources                   |
| `iceberg_1_10`       | PR or push, paths matched                        | Iceberg sources                     |
| `iceberg_1_8`        | push only                                        | Iceberg sources                     |
| `iceberg_1_9`        | push only                                        | Iceberg sources                     |

A heavy job appears in the PR's checks list as a `skipped` entry whenever
its path filter or event criteria don't match. Skipped checks count as
passing for branch protection.

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

## Reusable workflows (called by `ci.yml`)

| File                              | Called from `ci.yml` job(s)                        |
| --------------------------------- | -------------------------------------------------- |
| `pr_build_linux.yml`              | `pr_build_linux`                                   |
| `pr_build_macos.yml`              | `pr_build_macos`                                   |
| `pr_benchmark_check.yml`          | `pr_benchmark_check`                               |
| `docs.yaml`                       | `docs`                                             |
| `spark_sql_test_reusable.yml`     | `spark_3_4`, `spark_3_5`, `spark_4_0`, `spark_4_1` |
| `iceberg_spark_test_reusable.yml` | `iceberg_1_8`, `iceberg_1_9`, `iceberg_1_10`       |

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
