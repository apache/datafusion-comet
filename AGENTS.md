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

# Agent Guidelines for Apache DataFusion Comet

Read the [Contributor Guide](docs/source/contributor-guide/index.md) before making changes.
Relevant entry points:

- [Development Guide](docs/source/contributor-guide/development.md): build, test, and common
  pitfalls (including why `-pl` must not be used and the JVM/native build order).
- [Spark SQL Tests](docs/source/contributor-guide/spark-sql-tests.md): the only supported way
  to modify files under `dev/diffs/`. **Never hand-edit a diff file.** Clone Spark, apply the
  existing diff, modify the Spark source, then regenerate the diff as documented there.
- [Adding a New Expression](docs/source/contributor-guide/adding_a_new_expression.md) /
  [Adding a New Operator](docs/source/contributor-guide/adding_a_new_operator.md).
- [Debugging Guide](docs/source/contributor-guide/debugging.md).

When opening a pull request, use the [PR template](.github/pull_request_template.md) and fill
in every section.

Use `git push` for normal updates to a PR branch. If a rebase or amend requires a force push,
use `git push --force-with-lease`, never `--force` or `-f`, to reduce the risk of overwriting
another maintainer's commits. If the lease check rejects the push, inspect and integrate the
remote changes before retrying; do not bypass it with `--force`. See
[Submitting a Pull Request](docs/source/contributor-guide/development.md#submitting-a-pull-request).

## Checking a change against CI

A green pull request does not mean a change is safe to queue. The pull request tier runs the Comet
suites against the default Spark profile only. Spark's own SQL suite and the Iceberg suites first
report in the merge queue, where a failure evicts the pull request and blocks everyone else's
merges, or in the nightly run, after the change has already landed.

So when a change touches the serde, the planner, a native operator, a Spark shim, an Iceberg code
path, or anything under `dev/diffs/`, get a verdict first. Either run the suite locally:

```shell
dev/local-ci.sh spark sql_core-1     # one matrix row, or `spark` for all of them
dev/local-ci.sh iceberg shard-2      # one shard, or `iceberg` for every target
```

or apply the matching `run-*` label so CI runs it instead. Which changes warrant which suite, the
label names, and the script's caveats are in
[Continuous Integration](docs/source/contributor-guide/ci.md).

Pick one rather than both. A full local job is hours of compute and tens of GB of disk, so prefer
the shard that covers the change, and prefer the label when the change is broad. Two things matter
when running it unattended: preparing sweeps the whole local Maven repository, which is a shared
cache, and `SKIP_PREPARE=1` skips the Comet install so it must not be used after changing Comet.

## Skills

Repository-specific agent skills live under `.ai/skills/`. Each subdirectory is a single skill
with a `SKILL.md` (YAML frontmatter + body). Check that directory for an applicable skill before
starting a task; new skills go in `.ai/skills/<skill-name>/SKILL.md`.

For compatibility with agents that look in vendor-specific locations, `.claude/skills` is a
symlink to `.ai/skills` and `CLAUDE.md` is a symlink to this file. Add new content here rather
than to the symlinks.
