<!---
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

# Contributing to Apache DataFusion Comet

We welcome contributions to Comet in many areas, and encourage new contributors to get involved.

Here are some areas where you can help:

- Testing Comet with existing Spark jobs and reporting issues for any bugs or performance issues
- Contributing code to support Spark expressions, operators, and data types that are not currently supported
- Reviewing pull requests and helping to test new features for correctness and performance
- Improving documentation

## Finding issues to work on

We maintain a list of good first issues in GitHub [here](https://github.com/apache/datafusion-comet/issues?q=is%3Aopen+is%3Aissue+label%3A%22good+first+issue%22). We also have a [roadmap](roadmap.md).

To assign yourself an issue, comment `take` on the issue. To unassign yourself, comment `untake`.

## Reporting issues

We use [GitHub issues](https://github.com/apache/datafusion-comet/issues) for bug reports and feature requests.

## Running CI on your fork

Comet follows the same model as Apache Spark: the heavy CI (Linux build, macOS build, Spark SQL test matrix,
Iceberg compat, Miri) runs on **your fork's** GitHub Actions runners, not on the Apache runners. The upstream
`apache/datafusion-comet` repository only publishes a single aggregated `Build` check on your PR that mirrors
the status of your fork's run.

### One-time setup on your fork

1. Navigate to your fork on GitHub (e.g. `https://github.com/<you>/datafusion-comet`).
2. Open the **Actions** tab.
3. If you see a banner that says "Workflows aren't being run on this forked repository", click
   **I understand my workflows, go ahead and enable them**.

Without this step, no CI will run when you push, and the PR's `Build` check will show a red
`Workflow run detection failed` status with instructions to enable Actions.

### Normal workflow

- Push your branch to your fork. The `Build` workflow triggers automatically on any branch push and runs the
  full test matrix against your fork's HEAD merged with the latest `apache/datafusion-comet:main`.
- Open a pull request against `apache/datafusion-comet`. Within ~15 seconds, a `Build` check appears on the PR
  whose details link points to your fork's workflow run. The upstream repo polls the run every 15 minutes and
  updates the check's status and conclusion as your run progresses.
- If you push new commits, the cycle repeats: fork runs CI, upstream check is re-created pointing at the new run.

### Keeping your branch current

Upstream verification requires your branch to be mergeable with the current `main`. If `main` has moved on
since you branched, rebase and force-push:

```bash
git fetch upstream
git rebase upstream/main
git push origin YOUR_BRANCH --force
```

The `Build` check will fail with `unsynced commit pushed` if a new commit lands on your branch while the
notify workflow is mid-execution. This self-heals on the next push.

### Things that still run on upstream runners

A small number of lightweight, policy-style checks continue to run on Apache's runners from the
`pull_request` event: PR title validation, RAT license-header check, Markdown formatting, and the workflow
linter. These are not part of the aggregated `Build` check.

## Asking for Help

The Comet project uses the same Slack and Discord channels as the main Apache DataFusion project. See details at
[Apache DataFusion Communications]. There are dedicated Comet channels in both Slack and Discord.

## Regular public meetings

The Comet contributors hold regular video calls where new and current contributors are welcome to ask questions and
coordinate on issues that they are working on.

See the [Apache DataFusion Comet community meeting] Google document for more information.

[Apache DataFusion Communications]: https://datafusion.apache.org/contributor-guide/communication.html
[Apache DataFusion Comet community meeting]: https://docs.google.com/document/d/1NBpkIAuU7O9h8Br5CbFksDhX-L9TyO9wmGLPMe0Plc8/edit?usp=sharing
