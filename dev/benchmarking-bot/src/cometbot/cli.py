"""CLI entry point for Comet benchmark automation."""

import click
from rich.console import Console

console = Console()


# =============================================================================
# Shared CLI helper functions for status/logs/delete commands
# =============================================================================


def _handle_status(pr: int | None, job_prefix: str, app_label: str, display_name: str) -> None:
    """Handle status command for any benchmark type.

    Args:
        pr: Optional PR number to check specific job
        job_prefix: Job name prefix (e.g., "comet")
        app_label: K8s app label for listing jobs
        display_name: Human-readable name for output
    """
    from cometbot.k8s import get_job_status, _list_jobs_by_label

    if pr:
        job_name = f"{job_prefix}-pr-{pr}"
        status = get_job_status(job_name)
        console.print(f"Job {job_name}: {status['status']}")
    else:
        jobs = _list_jobs_by_label(app_label)
        if not jobs:
            console.print(f"No {display_name} jobs found")
            return
        console.print(f"[bold]{display_name} jobs:[/bold]")
        for job in jobs:
            console.print(f"  {job['name']}: {job['status']}")


def _handle_logs(pr: int, job_prefix: str, follow: bool) -> None:
    """Handle logs command for any benchmark type.

    Args:
        pr: PR number to get logs for
        job_prefix: Job name prefix (e.g., "comet")
        follow: Whether to follow log output
    """
    from cometbot.k8s import stream_logs

    job_name = f"{job_prefix}-pr-{pr}"
    stream_logs(job_name, follow=follow)


def _handle_delete(pr: int, job_prefix: str) -> None:
    """Handle delete command for any benchmark type.

    Args:
        pr: PR number to delete job for
        job_prefix: Job name prefix (e.g., "comet")
    """
    from cometbot.k8s import delete_job

    job_name = f"{job_prefix}-pr-{pr}"
    delete_job(job_name)
    console.print(f"[green]Deleted job: {job_name}[/green]")


@click.group()
@click.version_option()
def main():
    """Comet Benchmark Automation CLI.

    Automates running Comet benchmarks comparing a GitHub PR against main.
    """
    pass


# Comet commands
@main.group()
def comet():
    """Comet benchmark commands for running TPC-H/TPC-DS benchmarks."""
    pass


@comet.command("build")
@click.option(
    "--tag",
    default="comet-benchmark-automation:latest",
    help="Docker image tag",
)
def comet_build(tag: str):
    """Build the Docker image for Comet benchmarks."""
    from cometbot.k8s import build_comet_image

    build_comet_image(tag)


@comet.command("run")
@click.option(
    "--pr",
    required=True,
    type=int,
    help="GitHub PR number to benchmark",
)
@click.option(
    "--image",
    default=None,  # Will use COMET_REGISTRY_IMAGE from k8s.py
    help="Docker image for K8s to pull (default: $COMETBOT_REGISTRY/comet-benchmark-automation:latest)",
)
@click.option(
    "--micro",
    default="",
    help="Run microbenchmark instead of TPC-H (e.g., 'CometStringExpressionBenchmark')",
)
@click.option(
    "--tpch-queries",
    default="/opt/tpch-queries",
    help="Path to TPC-H query files inside container (default: bundled queries)",
)
@click.option(
    "--iterations",
    default=1,
    type=int,
    help="Number of TPC-H benchmark iterations",
)
@click.option(
    "--no-build",
    is_flag=True,
    default=False,
    help="Skip building the Docker image",
)
@click.option(
    "--no-cleanup",
    is_flag=True,
    default=False,
    help="Don't delete the job after completion",
)
@click.option(
    "--spark-master",
    default="local[*]",
    help="Spark master URL (default: local[*])",
)
@click.option(
    "--github-token",
    envvar="COMETBOT_GITHUB_TOKEN",
    default="",
    help="GitHub token to post results as PR comment (or set COMETBOT_GITHUB_TOKEN env var)",
)
@click.option(
    "--conf",
    multiple=True,
    help="Spark/Comet config (e.g., --conf spark.comet.exec.enabled=true). Can be repeated.",
)
def comet_run(
    pr: int,
    image: str | None,
    micro: str,
    tpch_queries: str,
    iterations: int,
    no_build: bool,
    no_cleanup: bool,
    spark_master: str,
    github_token: str,
    conf: tuple[str, ...],
):
    """Run Comet benchmark in Kubernetes (TPC-H or microbenchmark)."""
    from cometbot.k8s import (
        COMET_REGISTRY_IMAGE,
        build_comet_image,
        create_comet_job,
        delete_job,
        stream_logs,
        wait_for_completion,
    )

    # Use default registry image if not specified
    if image is None:
        image = COMET_REGISTRY_IMAGE

    benchmark_mode = "micro" if micro else "tpch"

    # Parse --conf key=value pairs into a dict
    comet_configs = {}
    for c in conf:
        if "=" in c:
            k, v = c.split("=", 1)
            comet_configs[k] = v

    if micro:
        console.print(f"[bold]Running Comet microbenchmark in Kubernetes: PR #{pr}[/bold]")
        console.print(f"Benchmark: {micro}")
    else:
        console.print(f"[bold]Running Comet TPC-H benchmark in Kubernetes: PR #{pr}[/bold]")
        console.print(f"Iterations: {iterations}")
    console.print(f"Spark master: {spark_master}")
    if comet_configs:
        console.print(f"Configs: {comet_configs}")
    console.print()

    # Build image if needed
    if not no_build:
        console.print("[bold blue]Step 1: Building Comet Docker image...[/bold blue]")
        build_comet_image()  # Builds locally and pushes to registry
    else:
        console.print("[bold blue]Step 1: Skipping image build[/bold blue]")

    # Create the job
    console.print("[bold blue]Step 2: Creating Kubernetes job...[/bold blue]")
    if github_token:
        console.print("[dim]GitHub token provided - will post results to PR[/dim]")
    job_name = create_comet_job(
        pr_number=pr,
        image=image,
        benchmark_mode=benchmark_mode,
        tpch_queries=tpch_queries,
        iterations=iterations,
        micro_benchmark=micro,
        spark_master=spark_master,
        comet_configs=comet_configs or None,
        github_token=github_token,
    )

    try:
        # Stream logs
        console.print("[bold blue]Step 3: Streaming logs...[/bold blue]")
        stream_logs(job_name)

        # Wait for completion
        success = wait_for_completion(job_name)

        if success:
            console.print("[bold green]Comet benchmark completed successfully![/bold green]")
        else:
            console.print("[bold red]Comet benchmark failed![/bold red]")

    finally:
        if not no_cleanup:
            console.print("[bold blue]Step 4: Cleaning up...[/bold blue]")
            delete_job(job_name)


@comet.command("status")
@click.option(
    "--pr",
    type=int,
    default=None,
    help="Show status for a specific PR",
)
def comet_status(pr: int | None):
    """Check status of Comet benchmark jobs."""
    from cometbot.k8s import APP_LABEL_COMET

    _handle_status(pr, "comet", APP_LABEL_COMET, "Comet")


@comet.command("logs")
@click.option(
    "--pr",
    required=True,
    type=int,
    help="PR number to get logs for",
)
@click.option(
    "--follow",
    "-f",
    is_flag=True,
    default=False,
    help="Follow log output",
)
def comet_logs(pr: int, follow: bool):
    """Get logs from a Comet benchmark job."""
    _handle_logs(pr, "comet", follow)


@comet.command("delete")
@click.option(
    "--pr",
    required=True,
    type=int,
    help="PR number to delete job for",
)
def comet_delete(pr: int):
    """Delete a Comet benchmark job."""
    _handle_delete(pr, "comet")


# Bot commands
@main.group()
def bot():
    """GitHub bot commands for automated benchmark monitoring."""
    pass


@bot.command("start")
@click.option(
    "--poll-interval",
    default=60,
    type=int,
    help="GitHub polling interval in seconds (default: 60)",
)
@click.option(
    "--github-token",
    envvar="COMETBOT_GITHUB_TOKEN",
    required=True,
    help="GitHub token for API access (or set COMETBOT_GITHUB_TOKEN env var)",
)
def bot_start(poll_interval: int, github_token: str):
    """Start the benchmark bot to monitor PRs for requests.

    The bot polls GitHub for comments containing slash commands:

    \b
    - /run tpch [--iterations N]
    - /run micro <BenchmarkClassName>
    - /help

    Only authorized users can trigger benchmarks.
    """
    from cometbot.bot import run_bot

    run_bot(github_token, poll_interval)


@bot.command("poll-once")
@click.option(
    "--github-token",
    envvar="COMETBOT_GITHUB_TOKEN",
    required=True,
    help="GitHub token for API access (or set COMETBOT_GITHUB_TOKEN env var)",
)
def bot_poll_once(github_token: str):
    """Poll GitHub once for benchmark requests (for testing)."""
    from cometbot.bot import load_state, poll_once

    state = load_state()
    poll_once(github_token, state)


if __name__ == "__main__":
    main()
