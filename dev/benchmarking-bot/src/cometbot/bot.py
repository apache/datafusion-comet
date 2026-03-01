# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""GitHub bot that monitors Comet PRs for benchmark requests."""

import json
import os
import re
import time
import traceback
from dataclasses import dataclass
from pathlib import Path

import requests
from rich.console import Console

from cometbot.slack import notify as slack_notify

console = Console()

# Configuration
GITHUB_API = "https://api.github.com"
POLL_INTERVAL = 60  # 1 minute
K8S_POLL_INTERVAL = 10  # 10 seconds
MAX_CONCURRENT_JOBS = 4

# Repos to monitor
REPOS = {
    "apache/datafusion-comet": "comet",
}

# Authorized users - loaded from file
def _load_authorized_users() -> set[str]:
    """Load authorized GitHub usernames from authorized_users.txt."""
    users_file = Path(os.environ.get("COMETBOT_PROJECT_DIR", Path(__file__).parent.parent.parent)) / "authorized_users.txt"
    users = set()
    if users_file.exists():
        with open(users_file) as f:
            for line in f:
                line = line.split("#")[0].strip()
                if line:
                    users.add(line)
    if not users:
        console.print("[bold red]WARNING: No authorized users loaded. No one will be able to trigger benchmarks.[/bold red]")
    return users

AUTHORIZED_USERS = _load_authorized_users()

# File to track processed comments (use COMETBOT_PROJECT_DIR if set, otherwise /tmp)
_state_dir = Path(os.environ.get("COMETBOT_PROJECT_DIR", "/tmp"))
STATE_FILE = _state_dir / "cometbot-bot-state.json"


@dataclass
class BenchmarkRequest:
    """Parsed benchmark request from a comment."""
    repo: str
    pr_number: int
    comment_id: int
    user: str
    benchmark_type: str  # "tpch" or "micro"
    args: list[str]


def load_state() -> dict:
    """Load processed comments state."""
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            state = json.load(f)
            # Ensure running_jobs exists (for backwards compatibility)
            if "running_jobs" not in state:
                state["running_jobs"] = {}
            return state
    return {"processed_comments": [], "running_jobs": {}}


def save_state(state: dict) -> None:
    """Save processed comments state. Prunes to last 10000 comment IDs."""
    max_comments = 10000
    if len(state["processed_comments"]) > max_comments:
        state["processed_comments"] = state["processed_comments"][-max_comments:]
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


def get_headers(token: str) -> dict:
    """Get GitHub API headers."""
    return {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
    }


def get_open_prs(token: str, repo: str) -> list[dict]:
    """Get list of open PRs for a repo."""
    url = f"{GITHUB_API}/repos/{repo}/pulls"
    params = {"state": "open", "per_page": 100}

    response = requests.get(url, headers=get_headers(token), params=params)
    response.raise_for_status()
    return response.json()


def get_pr_comments(token: str, repo: str, pr_number: int) -> list[dict]:
    """Get comments on a PR (issue comments, not review comments)."""
    url = f"{GITHUB_API}/repos/{repo}/issues/{pr_number}/comments"
    params = {"per_page": 100}

    response = requests.get(url, headers=get_headers(token), params=params)
    response.raise_for_status()
    return response.json()


def add_reaction(token: str, repo: str, comment_id: int, reaction: str) -> bool:
    """Add a reaction to a comment.

    Reactions: +1, -1, laugh, confused, heart, hooray, rocket, eyes
    """
    url = f"{GITHUB_API}/repos/{repo}/issues/comments/{comment_id}/reactions"
    headers = get_headers(token)
    headers["Accept"] = "application/vnd.github+json"

    response = requests.post(url, headers=headers, json={"content": reaction})
    return response.status_code in (200, 201)


def has_reaction(token: str, repo: str, comment_id: int, reaction: str) -> bool:
    """Check if a comment already has a specific reaction from the bot."""
    url = f"{GITHUB_API}/repos/{repo}/issues/comments/{comment_id}/reactions"
    headers = get_headers(token)
    headers["Accept"] = "application/vnd.github+json"

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        return False

    reactions = response.json()
    # Check if any reaction matches (we assume bot reactions are ours)
    for r in reactions:
        if r.get("content") == reaction:
            return True
    return False


def check_completed_jobs(token: str, state: dict) -> None:
    """Check for completed jobs and add appropriate reactions.

    This function checks all running jobs in state, and for any that have
    completed (succeeded or failed), adds the appropriate reaction and
    removes them from the running_jobs tracking.

    State format for running_jobs:
        {job_name: {"comment_id": int, "repo": str}}
    """
    from cometbot.k8s import get_job_status

    if not state.get("running_jobs"):
        return

    completed_jobs = []

    for job_name, job_info in list(state["running_jobs"].items()):
        # Handle both old format (just comment_id) and new format (dict with comment_id and repo)
        if isinstance(job_info, dict):
            comment_id = job_info["comment_id"]
            repo = job_info.get("repo", "apache/datafusion-comet")
            pr_number = job_info.get("pr_number")
        else:
            # Old format - just comment_id
            comment_id = job_info
            repo = "apache/datafusion-comet"
            pr_number = None

        status = get_job_status(job_name)

        if status["status"] == "Succeeded":
            console.print(f"[green]Job {job_name} completed successfully[/green]")
            add_reaction(token, repo, comment_id, "rocket")
            slack_notify(f"Job `{job_name}` completed successfully", "success")
            completed_jobs.append(job_name)
        elif status["status"] == "Failed":
            console.print(f"[red]Job {job_name} failed[/red]")
            if pr_number:
                post_comment(token, repo, pr_number, f"Benchmark job `{job_name}` failed due to an error.")
            slack_notify(f"Job `{job_name}` failed", "error")
            completed_jobs.append(job_name)
        elif status["status"] == "NotFound":
            # Job was deleted externally or never existed
            console.print(f"[yellow]Job {job_name} not found, removing from tracking[/yellow]")
            slack_notify(f"Job `{job_name}` not found, removing from tracking", "warning")
            completed_jobs.append(job_name)

    # Remove completed jobs from state
    for job_name in completed_jobs:
        del state["running_jobs"][job_name]

    if completed_jobs:
        save_state(state)


def post_comment(token: str, repo: str, pr_number: int, body: str) -> bool:
    """Post a comment on a PR."""
    url = f"{GITHUB_API}/repos/{repo}/issues/{pr_number}/comments"
    response = requests.post(url, headers=get_headers(token), json={"body": body})
    return response.status_code == 201


def get_help_text(repo: str) -> str:
    """Get help text for Comet benchmarks."""
    lines = [
        "## Benchmark Bot Usage",
        "",
        "This bot is whitelisted for DataFusion committers.",
        "",
        "### Commands",
        "",
        "```",
        "/run tpch [--iterations N] [--baseline BRANCH]",
        "/run tpch [--conf spark.comet.KEY=VALUE ...]",
        "/run micro <BenchmarkClassName> [--baseline BRANCH]",
        "/help",
        "```",
        "",
        "### Examples",
        "",
        "```",
        "/run tpch",
        "/run tpch --iterations 3",
        "/run tpch --conf spark.comet.exec.enabled=true",
        "/run micro CometStringExpressionBenchmark",
        "```",
        "",
        "---",
        "*Automated by cometbot*",
    ]

    return "\n".join(lines)


def parse_benchmark_request(comment: dict, repo: str, pr_number: int) -> BenchmarkRequest | None:
    """Parse a comment for benchmark request.

    Expected format:
    - /run tpch [--iterations N] [--query Q]
    - /run micro <BenchmarkClassName>

    Returns None if no valid request found.
    """
    body = comment.get("body", "")
    user = comment.get("user", {}).get("login", "")
    comment_id = comment.get("id")

    # Ignore comments posted by the bot itself
    if user == "cometbot":
        return None

    # Strip quoted lines (starting with '>') to avoid triggering on quoted text
    body = "\n".join(line for line in body.splitlines() if not line.lstrip().startswith(">"))

    # Check for help request
    help_pattern = r"/help\b"
    if re.search(help_pattern, body, re.MULTILINE):
        return BenchmarkRequest(
            repo=repo,
            pr_number=pr_number,
            comment_id=comment_id,
            user=user,
            benchmark_type="help",
            args=[],
        )

    # /run tpch|micro [args]
    pattern = r"/run\s+(tpch|micro)(\s+.*)?$"
    match = re.search(pattern, body, re.IGNORECASE | re.MULTILINE)

    if not match:
        if re.search(r"^/run\b", body, re.MULTILINE):
            return BenchmarkRequest(
                repo=repo,
                pr_number=pr_number,
                comment_id=comment_id,
                user=user,
                benchmark_type="invalid",
                args=[],
            )
        return None

    benchmark_type = match.group(1).lower()
    args_str = match.group(2) or ""
    args = args_str.split()

    return BenchmarkRequest(
        repo=repo,
        pr_number=pr_number,
        comment_id=comment_id,
        user=user,
        benchmark_type=benchmark_type,
        args=args,
    )


def _is_safe_git_ref(ref: str) -> bool:
    """Validate that a git ref name is safe (no shell metacharacters)."""
    return bool(re.match(r'^[a-zA-Z0-9._\-/]+$', ref)) and len(ref) <= 128


def _is_safe_class_name(name: str) -> bool:
    """Validate that a benchmark class name is safe (Java class name chars only)."""
    return bool(re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name)) and len(name) <= 128


def parse_comet_configs(args: list[str]) -> dict[str, str] | None:
    """Parse --conf arguments and filter to only spark.comet.* configs.

    Returns None if any config value fails validation.
    """
    configs = {}
    i = 0
    while i < len(args):
        if args[i] == "--conf" and i + 1 < len(args):
            conf = args[i + 1]
            if "=" in conf:
                key, value = conf.split("=", 1)
                # Only allow spark.comet.* configs for security
                if not key.startswith("spark.comet."):
                    console.print(f"[yellow]Ignoring non-comet config: {key}[/yellow]")
                    i += 2
                    continue
                # Validate key and value contain no shell metacharacters
                if not re.match(r'^[a-zA-Z0-9._\-]+$', key):
                    console.print(f"[red]Invalid config key: {key!r}[/red]")
                    return None
                if not re.match(r'^[a-zA-Z0-9._\-/=:]+$', value):
                    console.print(f"[red]Invalid config value for {key}: {value!r}[/red]")
                    return None
                configs[key] = value
            i += 2
        else:
            i += 1
    return configs


def run_tpch_benchmark(token: str, pr_number: int, comment_id: int, args: list[str]) -> str | None:
    """Run TPC-H benchmark for a PR.

    Builds the Docker image with PR-specific tag, submits the K8s job,
    and returns immediately without waiting for completion.

    Args:
        token: GitHub token
        pr_number: PR number
        comment_id: Comment ID that triggered this benchmark (used in job name)
        args: Command arguments

    Returns:
        The job name for tracking.
    """
    from cometbot.k8s import (
        build_comet_image,
        create_comet_job,
        get_comet_image_for_pr,
    )

    # Parse args
    iterations = 1
    baseline_branch = "main"
    max_iterations = 3  # Limit to prevent abuse
    for i, arg in enumerate(args):
        if arg in ("--iterations", "-i") and i + 1 < len(args):
            try:
                iterations = min(int(args[i + 1]), max_iterations)
            except ValueError:
                pass
        elif arg in ("--baseline", "-b") and i + 1 < len(args):
            baseline_branch = args[i + 1]

    # Validate baseline branch name to prevent injection
    if not _is_safe_git_ref(baseline_branch):
        console.print(f"[red]Invalid baseline branch name: {baseline_branch!r}[/red]")
        return None

    # Parse --conf arguments (only spark.comet.* allowed)
    comet_configs = parse_comet_configs(args)
    if comet_configs is None:
        return None  # Validation failed

    console.print(f"[bold blue]Running TPC-H benchmark for PR #{pr_number}[/bold blue]")
    console.print(f"Iterations: {iterations}, Baseline: {baseline_branch}")
    if comet_configs:
        console.print(f"Comet configs: {comet_configs}")

    # Get PR-specific image tags
    local_tag, registry_tag = get_comet_image_for_pr(pr_number)

    # Build and push image with PR-specific tag
    build_comet_image(tag=local_tag, push=True, registry_tag=registry_tag)

    # Create job with comment_id in name for uniqueness
    job_name = create_comet_job(
        pr_number=pr_number,
        image=registry_tag,
        benchmark_mode="tpch",
        iterations=iterations,
        comet_configs=comet_configs,
        github_token=token,
        comment_id=comment_id,
        baseline_branch=baseline_branch,
    )

    console.print(f"[green]Job {job_name} submitted, will track completion asynchronously[/green]")
    return job_name


def run_micro_benchmark(token: str, pr_number: int, comment_id: int, args: list[str]) -> str | None:
    """Run microbenchmark for a PR.

    Builds the Docker image with PR-specific tag, submits the K8s job,
    and returns immediately without waiting for completion.

    Args:
        token: GitHub token
        pr_number: PR number
        comment_id: Comment ID that triggered this benchmark (used in job name)
        args: Command arguments

    Returns:
        The job name for tracking, or None if args are invalid.
    """
    from cometbot.k8s import (
        build_comet_image,
        create_comet_job,
        get_comet_image_for_pr,
    )

    if not args:
        console.print("[red]No benchmark class specified for micro benchmark[/red]")
        return None

    benchmark_class = args[0]

    # Validate benchmark class name to prevent injection
    if not _is_safe_class_name(benchmark_class):
        console.print(f"[red]Invalid benchmark class name: {benchmark_class!r}[/red]")
        return None

    baseline_branch = "main"

    # Parse remaining args
    for i, arg in enumerate(args):
        if arg in ("--baseline", "-b") and i + 1 < len(args):
            baseline_branch = args[i + 1]

    # Validate baseline branch name to prevent injection
    if not _is_safe_git_ref(baseline_branch):
        console.print(f"[red]Invalid baseline branch name: {baseline_branch!r}[/red]")
        return None

    console.print(f"[bold blue]Running microbenchmark for PR #{pr_number}[/bold blue]")
    console.print(f"Benchmark: {benchmark_class}, Baseline: {baseline_branch}")

    # Get PR-specific image tags
    local_tag, registry_tag = get_comet_image_for_pr(pr_number)

    # Build and push image with PR-specific tag
    build_comet_image(tag=local_tag, push=True, registry_tag=registry_tag)

    # Create job with comment_id in name for uniqueness
    job_name = create_comet_job(
        pr_number=pr_number,
        image=registry_tag,
        benchmark_mode="micro",
        micro_benchmark=benchmark_class,
        github_token=token,
        comment_id=comment_id,
        baseline_branch=baseline_branch,
    )

    console.print(f"[green]Job {job_name} submitted, will track completion asynchronously[/green]")
    return job_name


def process_request(token: str, request: BenchmarkRequest, state: dict) -> bool:
    """Process a benchmark request.

    Returns:
        True if a job was submitted, False otherwise.
    """
    from cometbot.k8s import count_running_comet_jobs

    # Check if already processed (in state)
    if request.comment_id in state["processed_comments"]:
        return False

    # Check if already processed (has eyes reaction - handles state loss)
    if has_reaction(token, request.repo, request.comment_id, "eyes"):
        console.print(f"[dim]Skipping comment {request.comment_id} - already has eyes reaction[/dim]")
        state["processed_comments"].append(request.comment_id)
        save_state(state)
        return False

    console.print(f"\n[bold]Processing request from @{request.user} on {request.repo} PR #{request.pr_number}[/bold]")
    console.print(f"Type: {request.benchmark_type}, Args: {request.args}")

    # Handle help request (no auth required)
    if request.benchmark_type == "help":
        console.print("[blue]Posting help text[/blue]")
        add_reaction(token, request.repo, request.comment_id, "eyes")
        help_text = get_help_text(request.repo)
        post_comment(token, request.repo, request.pr_number, help_text)
        state["processed_comments"].append(request.comment_id)
        save_state(state)
        return False

    # Check authorization
    if request.user not in AUTHORIZED_USERS:
        console.print(f"[yellow]User @{request.user} not authorized[/yellow]")
        add_reaction(token, request.repo, request.comment_id, "eyes")
        post_comment(
            token, request.repo, request.pr_number,
            f"Sorry @{request.user}, this feature is restricted to DataFusion committers."
        )
        state["processed_comments"].append(request.comment_id)
        save_state(state)
        return False

    # Handle invalid format - respond with help text
    if request.benchmark_type == "invalid":
        console.print("[yellow]Invalid command format, posting help text[/yellow]")
        add_reaction(token, request.repo, request.comment_id, "eyes")
        help_text = get_help_text(request.repo)
        post_comment(token, request.repo, request.pr_number, help_text)
        state["processed_comments"].append(request.comment_id)
        save_state(state)
        return False

    # Check concurrent job limit
    running_comet = count_running_comet_jobs()
    if running_comet >= MAX_CONCURRENT_JOBS:
        console.print(f"[yellow]At max concurrent jobs ({running_comet}/{MAX_CONCURRENT_JOBS}), will retry later[/yellow]")
        return False

    # Add eyes reaction to acknowledge
    console.print("[blue]Adding eyes reaction to acknowledge request[/blue]")
    add_reaction(token, request.repo, request.comment_id, "eyes")

    # Mark as processed before running (to avoid re-triggering if bot restarts)
    state["processed_comments"].append(request.comment_id)
    save_state(state)

    # Run the benchmark (builds image and submits job, returns immediately)
    try:
        job_name = None
        if request.benchmark_type == "tpch":
            job_name = run_tpch_benchmark(token, request.pr_number, request.comment_id, request.args)
        elif request.benchmark_type == "micro":
            job_name = run_micro_benchmark(token, request.pr_number, request.comment_id, request.args)

        # Track the running job so we can add reaction when it completes
        if job_name:
            state["running_jobs"][job_name] = {"comment_id": request.comment_id, "repo": request.repo, "pr_number": request.pr_number}
            save_state(state)
            slack_notify(f"Job `{job_name}` submitted for {request.repo} PR #{request.pr_number} ({request.benchmark_type})")
            return True
        else:
            # Job wasn't created (e.g., invalid args for micro benchmark)
            post_comment(token, request.repo, request.pr_number, "Benchmark run failed due to an error: job could not be created (invalid arguments?).")
            return False

    except Exception as e:
        console.print(f"[red]Error running benchmark: {e}[/red]")
        console.print(f"[red]{traceback.format_exc()}[/red]")
        slack_notify(f"Error submitting benchmark for {request.repo} PR #{request.pr_number}: {e}", "error")
        # Post error comment on failure during build/submit
        post_comment(token, request.repo, request.pr_number, "Benchmark run failed due to an error.")
        return False


def poll_once(token: str, state: dict) -> None:
    """Poll GitHub once for new benchmark requests."""
    # First, check for completed jobs and add reactions
    check_completed_jobs(token, state)

    running_count = len(state.get("running_jobs", {}))
    console.print(f"[dim]Polling for new benchmark requests... ({running_count}/{MAX_CONCURRENT_JOBS} jobs running)[/dim]")

    try:
        for repo in REPOS:
            prs = get_open_prs(token, repo)
            console.print(f"[dim]{repo}: {len(prs)} open PRs[/dim]")

            for pr in prs:
                pr_number = pr["number"]
                comments = get_pr_comments(token, repo, pr_number)

                for comment in comments:
                    request = parse_benchmark_request(comment, repo, pr_number)
                    if request:
                        process_request(token, request, state)

    except requests.RequestException as e:
        console.print(f"[red]Error polling GitHub: {e}[/red]")
        slack_notify(f"GitHub API error: {e}", "error")


def run_bot(token: str, poll_interval: int = POLL_INTERVAL) -> None:
    """Run the bot continuously."""
    # Try to load build info
    try:
        from cometbot._build_info import BUILD_TIMESTAMP
    except ImportError:
        BUILD_TIMESTAMP = "unknown (dev mode)"

    console.print("[bold green]Starting cometbot[/bold green]")
    slack_notify("Bot started")
    console.print(f"Build: {BUILD_TIMESTAMP}")
    console.print(f"Repositories: {', '.join(REPOS.keys())}")
    console.print(f"GitHub poll interval: {poll_interval} seconds")
    console.print(f"K8s poll interval: {K8S_POLL_INTERVAL} seconds")
    console.print(f"Max concurrent jobs: {MAX_CONCURRENT_JOBS}")
    console.print(f"Authorized users: {', '.join(sorted(AUTHORIZED_USERS))}")
    console.print()

    state = load_state()
    console.print(f"[dim]Loaded state: {len(state['processed_comments'])} processed comments, {len(state.get('running_jobs', {}))} running jobs[/dim]")

    time_since_github_poll = poll_interval  # poll immediately on startup

    while True:
        try:
            # Poll GitHub for new requests on the full interval
            if time_since_github_poll >= poll_interval:
                poll_once(token, state)
                time_since_github_poll = 0
            elif state.get("running_jobs"):
                # Check K8s job status frequently while jobs are running
                check_completed_jobs(token, state)
        except KeyboardInterrupt:
            console.print("\n[yellow]Bot stopped by user[/yellow]")
            slack_notify("Bot stopped")
            break
        except Exception as e:
            console.print(f"[red]Unexpected error: {e}[/red]")
            console.print(f"[red]{traceback.format_exc()}[/red]")
            slack_notify(f"Unexpected error in bot main loop: {e}", "error")

        time.sleep(K8S_POLL_INTERVAL)
        time_since_github_poll += K8S_POLL_INTERVAL
