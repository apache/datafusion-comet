"""Kubernetes operations for running Comet benchmarks in a cluster."""

import os
import subprocess
import time
from pathlib import Path

from rich.console import Console

console = Console()

# Path to the project directory
# Use COMETBOT_PROJECT_DIR env var if set (for deployed bot), otherwise use package location
PACKAGE_DIR = Path(os.environ.get("COMETBOT_PROJECT_DIR", Path(__file__).parent.parent.parent))
K8S_DIR = PACKAGE_DIR / "k8s"

# Job templates
COMET_JOB_TEMPLATE = K8S_DIR / "comet-job-template.yaml"

# Dockerfiles
DOCKERFILE = PACKAGE_DIR / "Dockerfile"

# Registry configuration (used for both push and pull)
REGISTRY = os.environ.get("COMETBOT_REGISTRY", "localhost:5000")

# Image name bases (without tags)
COMET_IMAGE_BASE = "comet-benchmark-automation"

# Default image names (for CLI use)
DEFAULT_COMET_IMAGE = f"{COMET_IMAGE_BASE}:latest"
COMET_REGISTRY_IMAGE = f"{REGISTRY}/{COMET_IMAGE_BASE}:latest"


def get_comet_image_for_pr(pr_number: int) -> tuple[str, str]:
    """Get local and registry image tags for a specific PR.

    Returns:
        Tuple of (local_tag, registry_tag)
    """
    local_tag = f"{COMET_IMAGE_BASE}:pr-{pr_number}"
    registry_tag = f"{REGISTRY}/{COMET_IMAGE_BASE}:pr-{pr_number}"
    return local_tag, registry_tag


# App labels for K8s job filtering
APP_LABEL_COMET = "comet-benchmark-automation"


def run_kubectl(*args: str, check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    """Run a kubectl command."""
    cmd = ["kubectl", *args]
    return subprocess.run(
        cmd,
        check=check,
        capture_output=capture,
        text=True,
    )


# =============================================================================
# Generic helper functions (used by benchmark-specific functions below)
# =============================================================================


def _build_and_push_image(
    dockerfile: Path,
    tag: str,
    registry_tag: str | None = None,
    name: str = "",
) -> None:
    """Generic function to build a Docker image and optionally push to registry.

    Args:
        dockerfile: Path to the Dockerfile
        tag: Local image tag
        registry_tag: If provided, also tag and push to this registry location
        name: Display name for console output (e.g., "Comet")
    """
    display_name = f"{name} " if name else ""
    console.print(f"[blue]Building {display_name}Docker image: {tag}[/blue]")

    # Build with explicit dockerfile path
    build_cmd = ["docker", "build", "-t", tag]
    if dockerfile != DOCKERFILE:
        build_cmd.extend(["-f", str(dockerfile)])
    build_cmd.append(str(PACKAGE_DIR))

    result = subprocess.run(build_cmd, check=False, capture_output=True, text=True)

    if result.returncode != 0:
        error_msg = result.stderr or result.stdout or "Unknown error"
        console.print(f"[red]Docker build failed:[/red]\n{error_msg}")
        raise RuntimeError(f"Failed to build {display_name}Docker image: {error_msg}")

    console.print(f"[green]Successfully built {display_name}image: {tag}[/green]")

    if registry_tag:
        ensure_registry_running()
        console.print(f"[blue]Tagging and pushing to registry: {registry_tag}[/blue]")
        subprocess.run(["docker", "tag", tag, registry_tag], check=True)
        result = subprocess.run(["docker", "push", registry_tag], check=False, capture_output=True, text=True)
        if result.returncode != 0:
            error_msg = result.stderr or result.stdout or "Unknown error"
            console.print(f"[red]Docker push failed:[/red]\n{error_msg}")
            raise RuntimeError(f"Failed to push {display_name}image to registry: {error_msg}")
        console.print("[green]Successfully pushed to registry[/green]")


def _list_jobs_by_label(app_label: str) -> list[dict]:
    """Generic function to list K8s jobs filtered by app label.

    Args:
        app_label: The value of the 'app' label to filter by

    Returns:
        List of dicts with 'name' and 'status' keys
    """
    result = subprocess.run(
        [
            "kubectl", "get", "jobs",
            "-l", f"app={app_label}",
            "-o", "jsonpath={range .items[*]}{.metadata.name},{.status.succeeded},{.status.failed}{\"\\n\"}{end}",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    if result.returncode != 0:
        return []

    jobs = []
    for line in result.stdout.strip().split("\n"):
        if not line:
            continue
        parts = line.split(",")
        name = parts[0]
        succeeded = parts[1] if len(parts) > 1 else "0"
        failed = parts[2] if len(parts) > 2 else "0"

        if succeeded == "1":
            status = "Succeeded"
        elif failed == "1":
            status = "Failed"
        else:
            status = "Running"

        jobs.append({"name": name, "status": status})

    return jobs


def count_running_jobs_by_label(app_label: str) -> int:
    """Count the number of running (not completed/failed) jobs for an app label."""
    jobs = _list_jobs_by_label(app_label)
    return sum(1 for job in jobs if job["status"] == "Running")


def count_running_comet_jobs() -> int:
    """Count the number of running Comet benchmark jobs."""
    return count_running_jobs_by_label(APP_LABEL_COMET)


def _apply_job_manifest(manifest: str, job_name: str, name: str = "") -> str:
    """Generic function to apply a K8s job manifest.

    Args:
        manifest: The rendered job manifest YAML
        job_name: Name of the job being created
        name: Display name for console output (e.g., "Comet")

    Returns:
        The job name
    """
    display_name = f"{name} " if name else ""
    console.print(f"[blue]Creating {display_name}Kubernetes job: {job_name}[/blue]")

    subprocess.run(
        ["kubectl", "apply", "-f", "-"],
        input=manifest,
        check=True,
        capture_output=True,
        text=True,
    )

    console.print(f"[green]{display_name}Job created: {job_name}[/green]")
    return job_name


def get_job_status(job_name: str) -> dict:
    """Get the status of a job."""
    result = subprocess.run(
        [
            "kubectl", "get", "job", job_name,
            "-o", "jsonpath={.status.conditions[0].type},{.status.succeeded},{.status.failed}",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    if result.returncode != 0:
        return {"status": "NotFound"}

    parts = result.stdout.split(",")
    condition = parts[0] if parts else ""
    succeeded = parts[1] if len(parts) > 1 else "0"
    failed = parts[2] if len(parts) > 2 else "0"

    if succeeded == "1":
        return {"status": "Succeeded"}
    elif failed == "1":
        return {"status": "Failed"}
    elif condition == "Complete":
        return {"status": "Succeeded"}
    else:
        return {"status": "Running"}


def get_pod_name(job_name: str) -> str | None:
    """Get the pod name for a job."""
    result = subprocess.run(
        [
            "kubectl", "get", "pods",
            "-l", f"job-name={job_name}",
            "-o", "jsonpath={.items[0].metadata.name}",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    if result.returncode != 0 or not result.stdout.strip():
        return None

    return result.stdout.strip()


def stream_logs(job_name: str, follow: bool = True) -> None:
    """Stream logs from a job's pod."""
    # Wait for pod to be created
    console.print("[blue]Waiting for pod to start...[/blue]")
    pod_name = None
    for _ in range(60):  # Wait up to 60 seconds
        pod_name = get_pod_name(job_name)
        if pod_name:
            break
        time.sleep(1)

    if not pod_name:
        console.print("[red]Timeout waiting for pod to start[/red]")
        return

    console.print(f"[blue]Streaming logs from pod: {pod_name}[/blue]")

    # Wait for pod to be running or completed
    for _ in range(120):  # Wait up to 2 minutes
        result = subprocess.run(
            [
                "kubectl", "get", "pod", pod_name,
                "-o", "jsonpath={.status.phase}",
            ],
            capture_output=True,
            text=True,
        )
        phase = result.stdout.strip()
        if phase in ("Running", "Succeeded", "Failed"):
            break
        time.sleep(1)

    # Stream logs
    cmd = ["kubectl", "logs", pod_name]
    if follow:
        cmd.append("-f")

    subprocess.run(cmd, check=False)


def wait_for_completion(job_name: str, timeout: int = 3600) -> bool:
    """Wait for a job to complete."""
    console.print(f"[blue]Waiting for job {job_name} to complete...[/blue]")

    start_time = time.time()
    while time.time() - start_time < timeout:
        status = get_job_status(job_name)
        if status["status"] == "Succeeded":
            console.print(f"[green]Job {job_name} completed successfully[/green]")
            return True
        elif status["status"] == "Failed":
            console.print(f"[red]Job {job_name} failed[/red]")
            return False
        time.sleep(5)

    console.print(f"[red]Timeout waiting for job {job_name}[/red]")
    return False


def delete_job(job_name: str) -> None:
    """Delete a job and its pods."""
    console.print(f"[blue]Deleting job: {job_name}[/blue]")

    subprocess.run(
        ["kubectl", "delete", "job", job_name, "--ignore-not-found"],
        check=False,
    )


# Registry functions

def ensure_registry_running() -> None:
    """Ensure the local Docker registry container is running."""
    # Check if registry container exists and is running
    result = subprocess.run(
        ["docker", "ps", "-q", "-f", "name=registry"],
        capture_output=True,
        text=True,
    )

    if result.stdout.strip():
        # Registry is running
        return

    # Check if container exists but is stopped
    result = subprocess.run(
        ["docker", "ps", "-aq", "-f", "name=registry"],
        capture_output=True,
        text=True,
    )

    if result.stdout.strip():
        # Container exists but stopped, start it
        console.print("[blue]Starting stopped registry container...[/blue]")
        subprocess.run(["docker", "start", "registry"], check=True)
        console.print("[green]Registry container started[/green]")
    else:
        # Container doesn't exist, create and run it
        console.print("[blue]Creating and starting registry container...[/blue]")
        subprocess.run(
            [
                "docker", "run", "-d",
                "-p", "5000:5000",
                "--restart=always",
                "--name", "registry",
                "registry:2.7",
            ],
            check=True,
        )
        console.print("[green]Registry container created and started[/green]")


# =============================================================================
# Comet benchmark functions
# =============================================================================


def build_comet_image(
    tag: str = DEFAULT_COMET_IMAGE,
    push: bool = True,
    registry_tag: str | None = None,
) -> None:
    """Build the Docker image for Comet benchmarks and optionally push to registry.

    Args:
        tag: Local image tag
        push: Whether to push to registry
        registry_tag: Override registry tag (if None, uses default COMET_REGISTRY_IMAGE)
    """
    if push and registry_tag is None:
        registry_tag = COMET_REGISTRY_IMAGE
    elif not push:
        registry_tag = None
    _build_and_push_image(DOCKERFILE, tag, registry_tag, name="Comet")


def render_comet_job_manifest(
    pr_number: int,
    job_name: str,
    image: str = DEFAULT_COMET_IMAGE,
    benchmark_mode: str = "tpch",
    tpch_queries: str = "/opt/tpch-queries",
    iterations: int = 1,
    micro_benchmark: str = "",
    spark_master: str = "local[*]",
    comet_configs: dict[str, str] | None = None,
    github_token: str = "",
    baseline_branch: str = "main",
) -> str:
    """Render the Comet job template with the given parameters."""
    with open(COMET_JOB_TEMPLATE) as f:
        template = f.read()

    # Convert comet_configs dict to comma-separated key=value string
    comet_configs_str = ""
    if comet_configs:
        comet_configs_str = ",".join(f"{k}={v}" for k, v in comet_configs.items())

    # Simple string replacement
    manifest = template.format(
        pr_number=pr_number,
        job_name=job_name,
        image=image,
        benchmark_mode=benchmark_mode,
        tpch_queries=tpch_queries,
        iterations=iterations,
        micro_benchmark=micro_benchmark,
        spark_master=spark_master,
        comet_configs=comet_configs_str,
        github_token=github_token,
        baseline_branch=baseline_branch,
    )

    return manifest


def create_comet_job(
    pr_number: int,
    image: str = DEFAULT_COMET_IMAGE,
    benchmark_mode: str = "tpch",
    tpch_queries: str = "/opt/tpch-queries",
    iterations: int = 1,
    micro_benchmark: str = "",
    spark_master: str = "local[*]",
    comet_configs: dict[str, str] | None = None,
    github_token: str = "",
    comment_id: int | None = None,
    baseline_branch: str = "main",
) -> str:
    """Create a Kubernetes job for Comet benchmark."""
    # Include comment_id in job name for uniqueness (truncate to keep name reasonable)
    if comment_id:
        job_name = f"comet-pr-{pr_number}-c{comment_id}"
    else:
        job_name = f"comet-pr-{pr_number}"

    console.print(f"[blue]Creating Comet Kubernetes job: {job_name}[/blue]")

    manifest = render_comet_job_manifest(
        pr_number=pr_number,
        job_name=job_name,
        image=image,
        benchmark_mode=benchmark_mode,
        tpch_queries=tpch_queries,
        iterations=iterations,
        micro_benchmark=micro_benchmark,
        spark_master=spark_master,
        comet_configs=comet_configs,
        github_token=github_token,
        baseline_branch=baseline_branch,
    )

    # Apply the manifest
    result = subprocess.run(
        ["kubectl", "apply", "-f", "-"],
        input=manifest,
        check=True,
        capture_output=True,
        text=True,
    )

    console.print(f"[green]Comet job created: {job_name}[/green]")
    return job_name


def list_comet_jobs() -> list[dict]:
    """List all Comet benchmark jobs."""
    return _list_jobs_by_label(APP_LABEL_COMET)
