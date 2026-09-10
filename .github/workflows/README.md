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

Required-check names changed when these workflows were consolidated. The
umbrella exposes per-job names like `CI / pr_build_linux / Lint`,
`CI / spark_3_5 / linux-test (...)`, etc. Update repository branch
protection rules to point at the new names; the old standalone workflow
names (`Spark SQL Tests (Spark 3.5)`, `PR Build (Linux)`, ...) no longer
exist as top-level workflows.
