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

# Benchmark Bot

The Comet benchmark bot automatically runs TPC-H and microbenchmarks against pull requests. Comment on
any Comet PR with a slash command and the bot will build the PR, run benchmarks in Kubernetes, and post
results back as a PR comment.

## Triggering Benchmarks

Add a comment on any open Comet PR with one of the following commands.

### TPC-H Benchmarks

Run the full TPC-H query suite (SF100) comparing the PR against the main branch:

```
/run tpch
```

Options:

| Flag | Description | Default |
|------|-------------|---------|
| `--iterations N` | Number of benchmark iterations (max 3) | 1 |
| `--baseline BRANCH` | Branch to compare against | `main` |
| `--conf KEY=VALUE` | Spark/Comet config override (only `spark.comet.*` keys allowed). Can be repeated. | — |

Examples:

```
/run tpch --iterations 3
/run tpch --baseline my-feature-branch
/run tpch --conf spark.comet.exec.enabled=true
/run tpch --iterations 2 --conf spark.comet.exec.enabled=true --conf spark.comet.exec.replaceSortMergeJoin=false
```

### Microbenchmarks

Run a specific JMH microbenchmark class:

```
/run micro <BenchmarkClassName>
```

Options:

| Flag | Description | Default |
|------|-------------|---------|
| `--baseline BRANCH` | Branch to compare against | `main` |

Examples:

```
/run micro CometStringExpressionBenchmark
/run micro CometStringExpressionBenchmark --baseline my-branch
```

Available microbenchmark classes are located in
[spark/src/test/scala/org/apache/spark/sql/benchmark/](https://github.com/apache/datafusion-comet/tree/main/spark/src/test/scala/org/apache/spark/sql/benchmark).

### Help

```
/help
```

Posts the usage reference as a PR comment.

## Understanding Results

### Reactions

The bot uses GitHub reactions to signal progress:

| Reaction | Meaning |
|----------|---------|
| :eyes: | Request acknowledged, job submitted |
| :rocket: | Job completed successfully |

### TPC-H Results

TPC-H results are posted as a comparison table showing baseline vs PR times for all 22 queries, with
percentage change indicators:

- Green: >5% faster than baseline
- Red: >5% slower than baseline
- White: within 5% (neutral)

The comment also includes a collapsible section with the full Spark configuration used.

### Microbenchmark Results

Microbenchmark results are posted as the raw JMH output showing timing comparisons between Spark and
Comet for each benchmark scenario.

## Authorization

Only DataFusion committers are authorized to trigger benchmarks. The list of authorized GitHub usernames
is maintained in `dev/benchmarking-bot/authorized_users.txt`. Unauthorized users receive a reply
explaining the restriction.

## Security Considerations

**This bot executes code from pull requests on a Kubernetes cluster.** Operators should understand the
following risks before deploying.

- **Code execution**: Benchmark jobs clone a PR branch, build it with `make release`, and run it. A
  malicious PR could contain arbitrary code that runs during the build or benchmark phase.
- **Authorization gate**: Only GitHub usernames in `dev/benchmarking-bot/authorized_users.txt` can trigger
  benchmarks. This is the primary security control. Keep the list limited to trusted committers.
- **GitHub token**: The `COMETBOT_GITHUB_TOKEN` is passed into benchmark containers to post results. Use a
  fine-grained token scoped only to the target repository with minimal permissions.
- **Network access**: Benchmark containers have access to the Kubernetes pod network. Apply NetworkPolicies
  to restrict egress if your cluster hosts sensitive services.
- **Host filesystem**: TPC-H data is mounted via `hostPath`. Do not widen this mount beyond the data
  directory.
- **No runtime sandboxing**: Jobs run as standard containers. Consider gVisor, Kata Containers, or a
  dedicated node pool for stronger isolation.

## Implementation Details

The bot source code lives in `dev/benchmarking-bot/`.

### Architecture

```
dev/benchmarking-bot/
├── src/cometbot/
│   ├── bot.py          # GitHub polling loop and comment parsing
│   ├── cli.py          # Click CLI (cometbot comet/bot commands)
│   ├── k8s.py          # Docker image builds and Kubernetes job management
│   └── slack.py        # Optional Slack notifications
├── Dockerfile          # Benchmark container (JDK 17, Rust, Maven, Spark 3.5)
├── k8s/
│   ├── comet-job-template.yaml   # K8s Job manifest template
│   └── bot-deployment.yaml       # K8s Deployment for the bot itself
├── deploy/
│   ├── deploy.sh        # Deployment script
│   └── cometbot-bot.service       # systemd unit file
├── authorized_users.txt
└── pyproject.toml
```

### How It Works

1. **Polling**: The bot polls the GitHub API every 60 seconds for new comments on open Comet PRs.
2. **Parsing**: Comments are checked for `/run` or `/help` slash commands. Quoted lines (starting with `>`) are
   ignored to prevent re-triggering on quoted text.
3. **Authorization**: The commenter's GitHub username is checked against `authorized_users.txt`.
4. **Image Build**: A Docker image is built containing JDK 17, Rust, Maven, and Spark 3.5. The image is tagged
   with the PR number and pushed to a local registry.
5. **Job Submission**: A Kubernetes Job is created from the `comet-job-template.yaml` template. The job
   clones the Comet repo, checks out the PR, builds Comet (`make release`), and runs the requested benchmark.
6. **Completion Tracking**: While jobs are running, the bot checks Kubernetes job status every 10 seconds.
   On completion, a :rocket: reaction is added to the original comment.
7. **Results Posting**: The benchmark container itself posts results as a PR comment using the GitHub API
   (the GitHub token is passed as an environment variable to the container).

### Concurrency

The bot enforces a maximum of 4 concurrent benchmark jobs. If the limit is reached, new requests are
deferred and retried on the next poll cycle.

### State Management

Processed comment IDs are tracked in `cometbot-bot-state.json` to avoid re-processing. As a fallback,
the bot also checks for the :eyes: reaction on comments — if present, the comment is skipped even if
the state file is lost.

### CLI

The `cometbot` CLI can also be used directly for manual benchmark runs:

```bash
# Install
cd dev/benchmarking-bot
pip install -e .

# Run a benchmark manually
cometbot comet run --pr 1234
cometbot comet run --pr 1234 --micro CometStringExpressionBenchmark

# Manage jobs
cometbot comet status
cometbot comet logs --pr 1234
cometbot comet delete --pr 1234

# Start the bot
cometbot bot start --github-token <token>
```

### TPC-H Data Prerequisite

The benchmark jobs expect TPC-H SF100 data to already exist on the Kubernetes nodes at `/mnt/bigdata/tpch`.
This path is mounted into the container as `/data/tpch` via a `hostPath` volume (see `k8s/comet-job-template.yaml`).

You must generate and place this data before running TPC-H benchmarks. Data generation scripts are
available in the [DataFusion Benchmarks](https://github.com/apache/datafusion-benchmarks) repository.

The expected layout on each node:

```
/mnt/bigdata/tpch/
└── sf100/
    ├── customer/
    ├── lineitem/
    ├── nation/
    ├── orders/
    ├── part/
    ├── partsupp/
    ├── region/
    └── supplier/
```

Each table directory should contain Parquet files. Microbenchmarks (`/run micro`) do not require TPC-H
data — they use internally generated datasets.

### Deployment

The bot is deployed to a Kubernetes host using the provided deploy script:

```bash
cd dev/benchmarking-bot
./deploy/deploy.sh [GITHUB_TOKEN]
```

This builds a Python wheel, copies it to the remote host, builds the Docker image, installs the
package in a virtualenv, and sets up a systemd service.

Required environment variables on the deployment host:

| Variable | Description |
|----------|-------------|
| `COMETBOT_GITHUB_TOKEN` | GitHub token for API access and posting results |
| `COMETBOT_SLACK_TOKEN` | (Optional) Slack bot token for notifications |
| `COMETBOT_SLACK_CHANNEL` | (Optional) Slack channel for notifications |
