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

# Comet Benchmark Automation Bot

Automated benchmarking for [Apache DataFusion Comet](https://github.com/apache/datafusion-comet) PRs. Runs TPC-H and microbenchmarks in Kubernetes, triggered by GitHub PR comments.

## GitHub Bot

Authorized users can trigger benchmarks by commenting on a Comet PR with slash commands. The bot picks up the comment, runs the benchmark in Kubernetes, and posts results back to the PR.

### Commands

```
/run tpch
/run tpch --iterations 3
/run tpch --baseline my-branch
/run tpch --conf spark.comet.exec.enabled=true
/run micro CometStringExpressionBenchmark
/run micro CometStringExpressionBenchmark --baseline my-branch
/help
```

### Reactions

The bot uses reactions to signal status:

- :eyes: -- request acknowledged, job submitted
- :rocket: -- job completed successfully
- :thumbsdown: -- job failed or invalid request

## CLI Usage

### Install

```bash
cd dev/benchmarking-bot
pip install -e .
```

### Build and run

```bash
# Build the Comet benchmark Docker image
cometbot comet build

# Run TPC-H benchmark for a PR
cometbot comet run --pr 1234

# With options
cometbot comet run --pr 1234 --iterations 3 --no-cleanup

# Run microbenchmark instead
cometbot comet run --pr 1234 --micro CometStringExpressionBenchmark

# Pass Spark/Comet configuration
cometbot comet run --pr 1234 --conf spark.comet.exec.enabled=true
```

### Manage jobs

```bash
cometbot comet status              # List all jobs
cometbot comet status --pr 1234    # Check specific PR
cometbot comet logs --pr 1234      # Get logs
cometbot comet logs --pr 1234 -f   # Follow logs
cometbot comet delete --pr 1234    # Delete job
```

### Run the bot

```bash
# Start continuous polling
cometbot bot start --github-token <token>

# Single poll (for testing)
cometbot bot poll-once --github-token <token>
```

## Deployment

Deploy to a remote host:

```bash
export COMETBOT_DEPLOY_HOST=myhost
export COMETBOT_DEPLOY_USER=myuser
export COMETBOT_DEPLOY_DIR=/home/myuser/cometbot
./deploy/deploy.sh
```

This script:

1. Builds the Python wheel
2. Copies files to the remote host
3. Builds the Docker image on the remote host
4. Installs the wheel in a virtualenv
5. Generates and installs a systemd service (`cometbot`)

### Environment variables

**Required for deployment:**

| Variable               | Description                           |
| ---------------------- | ------------------------------------- |
| `COMETBOT_DEPLOY_HOST` | Remote hostname to deploy to          |
| `COMETBOT_DEPLOY_USER` | SSH username on remote host           |
| `COMETBOT_DEPLOY_DIR`  | Installation directory on remote host |

**Runtime (set in `$COMETBOT_DEPLOY_DIR/env` on the deployment host):**

| Variable                 | Description                                                      |
| ------------------------ | ---------------------------------------------------------------- |
| `COMETBOT_GITHUB_TOKEN`  | GitHub token for API access and posting results                  |
| `COMETBOT_REGISTRY`      | Docker registry for benchmark images (default: `localhost:5000`) |
| `COMETBOT_SLACK_TOKEN`   | (Optional) Slack bot token for notifications                     |
| `COMETBOT_SLACK_CHANNEL` | (Optional) Slack channel for notifications                       |

## Security Considerations

**This bot executes arbitrary code from pull requests.** Anyone who can trigger a benchmark run can
execute code on your Kubernetes cluster. Before deploying, understand these risks:

- **Code execution**: Benchmark jobs clone a PR branch, build it, and run it inside a container. A
  malicious PR could contain code that exfiltrates secrets, attacks the network, or compromises the
  node.
- **Authorization is the primary control**: Only GitHub usernames listed in `authorized_users.txt`
  can trigger benchmarks. Keep this list to trusted committers and review it regularly.
- **GitHub token scope**: The `COMETBOT_GITHUB_TOKEN` is passed into benchmark containers so they
  can post results. Use a token with minimal scope (only `repo` or a fine-grained token limited to
  the target repository). Never use a token with org-wide admin permissions.
- **Network isolation**: Benchmark containers have access to the pod network. Use Kubernetes
  NetworkPolicies to restrict egress if your cluster contains sensitive services.
- **hostPath volumes**: The TPC-H data mount uses `hostPath`, which gives containers access to the
  host filesystem at that path. Do not widen this mount.
- **No sandboxing**: Jobs run as regular containers, not in a sandboxed runtime. Consider using
  gVisor, Kata Containers, or a dedicated node pool for benchmark workloads.
- **Resource limits**: The job template should include resource limits to prevent a single benchmark
  from consuming all cluster resources. Review `k8s/comet-job-template.yaml` before deploying.

## Architecture

- **Bot** (`src/cometbot/bot.py`): Polls GitHub for `/run` slash commands on open Comet PRs
- **K8s** (`src/cometbot/k8s.py`): Builds Docker images, creates/manages Kubernetes jobs
- **CLI** (`src/cometbot/cli.py`): Click-based CLI for manual benchmark runs and bot management
- **Dockerfile**: Builds a container with JDK 17, Rust, Maven, and Spark 3.5 for running benchmarks
- **K8s templates** (`k8s/`): Job and deployment manifests
