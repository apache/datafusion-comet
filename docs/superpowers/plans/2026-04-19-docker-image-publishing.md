# Docker Image Publishing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a manual release-time script (`dev/release/build-docker-images.sh`) plus a thin `kube/release.Dockerfile` that publishes five multi-arch (linux/amd64 + linux/arm64) Comet images to Docker Hub, replacing the existing `docker-publish.yml` GHCR workflow.

**Architecture:** The script reuses multi-arch uber-jars already produced by `dev/release/build-release-comet.sh` (each jar bundles `linux/amd64` + `linux/aarch64` native libs internally). The Dockerfile is a trivial `FROM <spark-base> + COPY comet.jar` so `docker buildx` only does `COPY` work per architecture, not Rust compilation. One `docker buildx build --platform linux/amd64,linux/arm64 --push` per combo produces a multi-arch manifest list on Docker Hub.

**Tech Stack:** Bash, `docker buildx`, Docker Hub, existing Apache Spark base images.

**Reference spec:** `docs/superpowers/specs/2026-04-19-docker-image-publishing-design.md`

---

## File Structure

| File | Responsibility |
|------|----------------|
| `kube/release.Dockerfile` (new) | Thin base-image + COPY-jar layer for release images. Parameterized via `SPARK_IMAGE` and `COMET_JAR` build args. |
| `dev/release/build-docker-images.sh` (new) | Manual orchestration: iterate the 5-combo matrix, resolve jar paths from a staging Maven repo, invoke `docker buildx` once per combo. |
| `.github/workflows/docker-publish.yml` (delete) | Obsolete GHCR workflow; replaced by manual script. |
| `docs/source/contributor-guide/release_process.md` (modify) | Document the new manual step; remove references to the deleted GHCR workflow. |

No test files — a bash release-tool of this size does not warrant a bats test harness. Verification is via `--dry-run` output inspection and a documented manual end-to-end procedure (Task 9). The release itself is the final validation.

---

## Task 1: Add `kube/release.Dockerfile`

**Files:**
- Create: `kube/release.Dockerfile`

- [ ] **Step 1: Write the Dockerfile**

Create `kube/release.Dockerfile` with contents:

```dockerfile
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Thin release image: drops a pre-built multi-arch Comet uber-jar into an
# Apache Spark base image.  Expected to be built by
# dev/release/build-docker-images.sh, which supplies SPARK_IMAGE and
# COMET_JAR build args.
#
# The jar already contains native libs for linux/amd64 and linux/aarch64, so
# the same jar works for both platforms under `docker buildx build
# --platform linux/amd64,linux/arm64`.

ARG SPARK_IMAGE
FROM ${SPARK_IMAGE}

ARG COMET_JAR
USER root

COPY ${COMET_JAR} $SPARK_HOME/jars/
```

- [ ] **Step 2: Verify the Dockerfile parses**

Create a stub jar and build for a single platform (amd64) to confirm the
Dockerfile syntax is valid:

```bash
TMPDIR=$(mktemp -d)
touch "$TMPDIR/comet.jar"
cp kube/release.Dockerfile "$TMPDIR/"
docker build \
  --build-arg SPARK_IMAGE=apache/spark:3.5.8-java17 \
  --build-arg COMET_JAR=comet.jar \
  -f "$TMPDIR/release.Dockerfile" \
  -t comet-release-test \
  "$TMPDIR"
docker image rm comet-release-test
rm -rf "$TMPDIR"
```

Expected: build completes successfully, `comet.jar` appears under
`$SPARK_HOME/jars/` inside the image. If the base image tag does not exist,
note it as a finding for Task 6's pre-flight work — do not block on this
task.

- [ ] **Step 3: Commit**

```bash
git add kube/release.Dockerfile
git commit -m "feat: add kube/release.Dockerfile for thin multi-arch release images"
```

---

## Task 2: Scaffold `build-docker-images.sh` with arg parsing

**Files:**
- Create: `dev/release/build-docker-images.sh`

- [ ] **Step 1: Write the script skeleton**

Create `dev/release/build-docker-images.sh` with the license header, strict
mode, a `usage()` function, and `--version` / `--maven-repo` / `--dry-run` /
`--help` argument parsing. Matrix iteration and docker invocations come in
later tasks.

```bash
#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Build and push multi-arch (linux/amd64 + linux/arm64) Docker images to
# Docker Hub for the five supported Spark x Scala combos, using uber-jars
# produced by dev/release/build-release-comet.sh.
#
# See docs/source/contributor-guide/release_process.md for the full flow.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." >/dev/null && pwd)"

IMAGE_REPO="apache/datafusion-comet"
DOCKERFILE="$REPO_ROOT/kube/release.Dockerfile"

usage() {
  cat <<EOF
Usage: $(basename "$0") --version <comet-version> --maven-repo <path> [--dry-run]

Build and push multi-arch Comet Docker images to Docker Hub.

Required:
  --version <v>       Comet version, e.g. 0.10.0 or 0.10.0-rc1
  --maven-repo <p>    Path to the staging Maven repo printed by
                      build-release-comet.sh, e.g. /tmp/comet-staging-repo-XXXXX

Optional:
  --dry-run           Print the docker buildx commands without executing
  -h, --help          Show this help and exit

Prerequisites:
  - Ran dev/release/build-release-comet.sh to produce the uber-jars
  - docker login to an account with push access to ${IMAGE_REPO}
  - docker buildx with a builder that supports linux/amd64 and linux/arm64

Published tags (one per combo):
  ${IMAGE_REPO}:<version>-spark3.4.3-scala2.12-java17
  ${IMAGE_REPO}:<version>-spark3.4.3-scala2.13-java17
  ${IMAGE_REPO}:<version>-spark3.5.8-scala2.12-java17
  ${IMAGE_REPO}:<version>-spark3.5.8-scala2.13-java17
  ${IMAGE_REPO}:<version>-spark4.0.1-scala2.13-java17
EOF
}

VERSION=""
MAVEN_REPO=""
DRY_RUN=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version)
      VERSION="${2:-}"
      shift 2
      ;;
    --maven-repo)
      MAVEN_REPO="${2:-}"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Error: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$VERSION" ]]; then
  echo "Error: --version is required" >&2
  usage >&2
  exit 2
fi

if [[ -z "$MAVEN_REPO" ]]; then
  echo "Error: --maven-repo is required" >&2
  usage >&2
  exit 2
fi

if [[ ! -d "$MAVEN_REPO" ]]; then
  echo "Error: --maven-repo path does not exist: $MAVEN_REPO" >&2
  exit 2
fi

echo "Version:    $VERSION"
echo "Maven repo: $MAVEN_REPO"
echo "Dry run:    $DRY_RUN"
```

- [ ] **Step 2: Mark executable and run verifications**

```bash
chmod +x dev/release/build-docker-images.sh

# 1. --help prints usage and exits 0
./dev/release/build-docker-images.sh --help
echo "exit=$?"

# 2. Missing args exit non-zero
./dev/release/build-docker-images.sh || echo "exit=$?"

# 3. Unknown arg exits 2
./dev/release/build-docker-images.sh --bogus || echo "exit=$?"

# 4. Happy-path arg parsing (use an existing directory so --maven-repo check passes)
./dev/release/build-docker-images.sh --version 0.0.0-test --maven-repo /tmp --dry-run
```

Expected:
- (1) usage text prints; `exit=0`
- (2) "Error: --version is required" on stderr; `exit=2`
- (3) "Error: unknown argument: --bogus"; `exit=2`
- (4) "Version: 0.0.0-test", "Maven repo: /tmp", "Dry run: 1" on stdout; exit 0

- [ ] **Step 3: Commit**

```bash
git add dev/release/build-docker-images.sh
git commit -m "feat: scaffold dev/release/build-docker-images.sh"
```

---

## Task 3: Add matrix iteration and dry-run command emission

**Files:**
- Modify: `dev/release/build-docker-images.sh`

- [ ] **Step 1: Append matrix and per-combo loop to the script**

Append the following to the end of `dev/release/build-docker-images.sh`
(after the echo of parsed args from Task 2):

```bash
# spark_short spark_full scala base_image
COMBOS=(
  "3.4 3.4.3 2.12 apache/spark:3.4.3-java17"
  "3.4 3.4.3 2.13 apache/spark:3.4.3-scala2.13-java17"
  "3.5 3.5.8 2.12 apache/spark:3.5.8-java17"
  "3.5 3.5.8 2.13 apache/spark:3.5.8-scala2.13-java17"
  "4.0 4.0.1 2.13 apache/spark:4.0.1"
)

PUSHED_TAGS=()

for combo in "${COMBOS[@]}"; do
  read -r spark_short spark_full scala base_image <<<"$combo"

  artifact="comet-spark-spark${spark_short}_${scala}"
  jar_path="$MAVEN_REPO/org/apache/comet/${artifact}/${VERSION}/${artifact}-${VERSION}.jar"
  tag="${IMAGE_REPO}:${VERSION}-spark${spark_full}-scala${scala}-java17"

  echo
  echo "=== Combo: Spark ${spark_full} / Scala ${scala} ==="
  echo "  Base image: ${base_image}"
  echo "  Jar path:   ${jar_path}"
  echo "  Tag:        ${tag}"

  # JAR check and temp-context staging come in Tasks 4-5.
  # For now under --dry-run, just print what the build command would be.

  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "  docker buildx build \\"
    echo "    --platform linux/amd64,linux/arm64 \\"
    echo "    --build-arg SPARK_IMAGE=${base_image} \\"
    echo "    --build-arg COMET_JAR=comet.jar \\"
    echo "    -f ${DOCKERFILE} \\"
    echo "    -t ${tag} \\"
    echo "    --push \\"
    echo "    <temp-context>"
  else
    echo "  (real build not yet wired up; will be added in Task 6)"
  fi

  PUSHED_TAGS+=("$tag")
done
```

- [ ] **Step 2: Verify dry-run output**

```bash
./dev/release/build-docker-images.sh \
  --version 0.10.0 \
  --maven-repo /tmp \
  --dry-run
```

Expected: five "=== Combo: ..." blocks are printed. Verify each shows the
correct tag, e.g. the first is
`apache/datafusion-comet:0.10.0-spark3.4.3-scala2.12-java17` and the last is
`apache/datafusion-comet:0.10.0-spark4.0.1-scala2.13-java17`. Jar paths look
like `/tmp/org/apache/comet/comet-spark-spark3.4_2.12/0.10.0/comet-spark-spark3.4_2.12-0.10.0.jar`.

- [ ] **Step 3: Commit**

```bash
git add dev/release/build-docker-images.sh
git commit -m "feat: iterate release matrix and emit dry-run build commands"
```

---

## Task 4: Add JAR existence check with clear error

**Files:**
- Modify: `dev/release/build-docker-images.sh`

- [ ] **Step 1: Add the check inside the loop**

In the per-combo loop, add a jar-existence check right after the
`tag=` line and before the `echo "=== ..."` block (the error needs to
point at the specific jar, so raise it before printing the combo summary).
Replace this portion of the loop:

```bash
  tag="${IMAGE_REPO}:${VERSION}-spark${spark_full}-scala${scala}-java17"

  echo
  echo "=== Combo: Spark ${spark_full} / Scala ${scala} ==="
```

with:

```bash
  tag="${IMAGE_REPO}:${VERSION}-spark${spark_full}-scala${scala}-java17"

  if [[ ! -f "$jar_path" ]]; then
    echo "Error: Comet jar not found for Spark ${spark_full} / Scala ${scala}" >&2
    echo "  Expected: $jar_path" >&2
    echo "  Run dev/release/build-release-comet.sh first, and pass its" >&2
    echo "  printed LOCAL_REPO path as --maven-repo." >&2
    exit 1
  fi

  echo
  echo "=== Combo: Spark ${spark_full} / Scala ${scala} ==="
```

- [ ] **Step 2: Verify the check fires for a missing jar**

```bash
./dev/release/build-docker-images.sh \
  --version 0.10.0 \
  --maven-repo /tmp \
  --dry-run
```

Expected: exits with code 1 and prints
"Error: Comet jar not found for Spark 3.4.3 / Scala 2.12", followed by the
expected path and remediation hint.

- [ ] **Step 3: Verify the happy path with stub jars**

Stage stub jars matching the expected layout:

```bash
STUB_REPO=$(mktemp -d)
for combo in \
  "3.4 0.10.0 2.12" \
  "3.4 0.10.0 2.13" \
  "3.5 0.10.0 2.12" \
  "3.5 0.10.0 2.13" \
  "4.0 0.10.0 2.13"
do
  read -r s v c <<<"$combo"
  art="comet-spark-spark${s}_${c}"
  d="$STUB_REPO/org/apache/comet/${art}/${v}"
  mkdir -p "$d"
  touch "$d/${art}-${v}.jar"
done

./dev/release/build-docker-images.sh \
  --version 0.10.0 \
  --maven-repo "$STUB_REPO" \
  --dry-run

rm -rf "$STUB_REPO"
```

Expected: all five combos process successfully and print their dry-run
buildx commands.

- [ ] **Step 4: Commit**

```bash
git add dev/release/build-docker-images.sh
git commit -m "feat: verify Comet jar exists before invoking docker buildx"
```

---

## Task 5: Wire up real buildx invocation with temp-context staging

**Files:**
- Modify: `dev/release/build-docker-images.sh`

- [ ] **Step 1: Add a trap for cleanup and replace the placeholder branch**

Add near the top of the script (just after the argument-validation block
from Task 2, before the matrix definition):

```bash
BUILD_CONTEXTS=()
cleanup_contexts() {
  for ctx in "${BUILD_CONTEXTS[@]:-}"; do
    [[ -n "${ctx:-}" && -d "${ctx}" ]] && rm -rf "${ctx}"
  done
}
trap cleanup_contexts EXIT
```

Then replace the entire `if [[ "$DRY_RUN" -eq 1 ]]; then ... else ... fi`
block from Task 3 with:

```bash
  build_context=$(mktemp -d)
  BUILD_CONTEXTS+=("$build_context")
  cp "$jar_path" "$build_context/comet.jar"

  buildx_cmd=(
    docker buildx build
    --platform linux/amd64,linux/arm64
    --build-arg "SPARK_IMAGE=${base_image}"
    --build-arg "COMET_JAR=comet.jar"
    -f "${DOCKERFILE}"
    -t "${tag}"
    --push
    "${build_context}"
  )

  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "  Would run:"
    for arg in "${buildx_cmd[@]}"; do
      printf '    %q\n' "$arg"
    done
  else
    "${buildx_cmd[@]}"
  fi
```

- [ ] **Step 2: Verify dry-run still works and shows a real command**

Using the stub repo from Task 4, Step 3:

```bash
STUB_REPO=$(mktemp -d)
for combo in \
  "3.4 0.10.0 2.12" \
  "3.4 0.10.0 2.13" \
  "3.5 0.10.0 2.12" \
  "3.5 0.10.0 2.13" \
  "4.0 0.10.0 2.13"
do
  read -r s v c <<<"$combo"
  art="comet-spark-spark${s}_${c}"
  d="$STUB_REPO/org/apache/comet/${art}/${v}"
  mkdir -p "$d"
  touch "$d/${art}-${v}.jar"
done

./dev/release/build-docker-images.sh \
  --version 0.10.0 \
  --maven-repo "$STUB_REPO" \
  --dry-run

rm -rf "$STUB_REPO"
```

Expected: each combo prints a single-line `docker buildx build ...
--platform linux/amd64,linux/arm64 ... --push /tmp/tmp.XXXX` command with
shell-escaped arguments. The script exits 0. Temp contexts under `/tmp`
are removed by the EXIT trap (verify with
`ls /tmp/tmp.*/comet.jar 2>/dev/null || echo "none (good)"`).

- [ ] **Step 3: Commit**

```bash
git add dev/release/build-docker-images.sh
git commit -m "feat: stage jar into temp build context and invoke docker buildx"
```

---

## Task 6: Add pre-flight checks and final summary

**Files:**
- Modify: `dev/release/build-docker-images.sh`

- [ ] **Step 1: Add pre-flight checks**

Insert the following block after the `echo "Dry run: $DRY_RUN"` line and
before the `BUILD_CONTEXTS=()` declaration from Task 5:

```bash
if ! command -v docker >/dev/null 2>&1; then
  echo "Error: docker is not on PATH" >&2
  exit 2
fi

if ! docker buildx version >/dev/null 2>&1; then
  echo "Error: docker buildx is not available" >&2
  echo "  Install: https://docs.docker.com/build/install-buildx/" >&2
  exit 2
fi

builder_platforms=$(docker buildx inspect --bootstrap 2>/dev/null | awk '/Platforms:/{print $2}' | tr ',' '\n' | tr -d ' ')
if ! grep -q '^linux/amd64$' <<<"$builder_platforms" \
   || ! grep -q '^linux/arm64$' <<<"$builder_platforms"; then
  echo "Error: active buildx builder does not support both linux/amd64 and linux/arm64" >&2
  echo "  Current platforms: $(tr '\n' ',' <<<"$builder_platforms" | sed 's/,$//')" >&2
  echo "  Create one with: docker buildx create --use --name multiarch" >&2
  exit 2
fi

if [[ "$DRY_RUN" -eq 0 ]] && ! grep -q '"auths"' "${HOME}/.docker/config.json" 2>/dev/null; then
  echo "Warning: no docker credentials found in ~/.docker/config.json." >&2
  echo "  Run 'docker login' if push fails." >&2
fi
```

- [ ] **Step 2: Add summary output at the end of the script**

Append to the very end of the script (after the `for combo in "${COMBOS[@]}"; do ... done` loop):

```bash
echo
echo "=== Summary ==="
if [[ "$DRY_RUN" -eq 1 ]]; then
  echo "Dry run — nothing was pushed. Would have pushed:"
else
  echo "Pushed tags:"
fi
for t in "${PUSHED_TAGS[@]}"; do
  echo "  $t"
done
```

- [ ] **Step 3: Verify pre-flight and summary**

```bash
STUB_REPO=$(mktemp -d)
for combo in \
  "3.4 0.10.0 2.12" \
  "3.4 0.10.0 2.13" \
  "3.5 0.10.0 2.12" \
  "3.5 0.10.0 2.13" \
  "4.0 0.10.0 2.13"
do
  read -r s v c <<<"$combo"
  art="comet-spark-spark${s}_${c}"
  d="$STUB_REPO/org/apache/comet/${art}/${v}"
  mkdir -p "$d"
  touch "$d/${art}-${v}.jar"
done

./dev/release/build-docker-images.sh \
  --version 0.10.0 \
  --maven-repo "$STUB_REPO" \
  --dry-run

rm -rf "$STUB_REPO"
```

Expected: pre-flight checks pass silently (credentials warning is suppressed
under `--dry-run`), five combos print dry-run buildx commands one arg per
line, and a final "=== Summary ===" block lists all five tags under the
"Dry run" header.

- [ ] **Step 4: Commit**

```bash
git add dev/release/build-docker-images.sh
git commit -m "feat: add buildx pre-flight checks and final summary"
```

---

## Task 7: Remove the obsolete GHCR workflow

**Files:**
- Delete: `.github/workflows/docker-publish.yml`

- [ ] **Step 1: Delete the file**

```bash
git rm .github/workflows/docker-publish.yml
```

- [ ] **Step 2: Confirm no other references remain in workflows**

```bash
grep -RIn 'docker-publish' .github/ || echo "no references (good)"
```

Expected: "no references (good)". If any are found, note them and stop
— raise it to the user before continuing.

- [ ] **Step 3: Commit**

```bash
git commit -m "chore: remove obsolete docker-publish GHCR workflow"
```

---

## Task 8: Update `release_process.md`

**Files:**
- Modify: `docs/source/contributor-guide/release_process.md`

Two edits: add a new "Publish Docker images to Docker Hub" section, and
update the two existing lines that refer to the deleted GHCR workflow.

- [ ] **Step 1: Locate the two existing references**

Run and record line numbers:

```bash
grep -n 'GitHub Container Registry' docs/source/contributor-guide/release_process.md
```

Expected: two matches (originally around lines 222 and 393, but confirm via
the grep output).

- [ ] **Step 2: Replace both existing lines**

For each of the two matches, replace the existing sentence (which says
"Note that pushing a [release candidate|release] tag will trigger a GitHub
workflow that will build a Docker image and publish it to GitHub Container
Registry at https://github.com/apache/datafusion-comet/pkgs/container/datafusion-comet")
with:

```
After pushing the tag, publish the multi-arch Docker images to Docker Hub
using `dev/release/build-docker-images.sh` (see the "Publish Docker images
to Docker Hub" section below).
```

Use the Edit tool, not sed. Show the old sentence and new sentence exactly
so the diff is reviewable.

- [ ] **Step 3: Add the new section**

Add a new top-level section at the end of the file (before any trailing
whitespace), with the following content:

```markdown
## Publish Docker images to Docker Hub

After running `build-release-comet.sh` and pushing the release (or release
candidate) tag, publish the multi-arch Docker images to Docker Hub.

Prerequisites:

1. The `LOCAL_REPO` path printed by `build-release-comet.sh`, e.g.
   `/tmp/comet-staging-repo-XXXXX`. The uber-jars produced by that script
   already bundle `linux/amd64` and `linux/aarch64` native libraries, so a
   single jar works for both container architectures.

2. Log in to Docker Hub with an account that has write access to
   `apache/datafusion-comet`:

   ```shell
   docker login
   ```

3. A `docker buildx` builder that supports both `linux/amd64` and
   `linux/arm64`. One-time setup:

   ```shell
   docker buildx create --use --name multiarch
   docker buildx inspect --bootstrap
   ```

Run the publish script:

```shell
./dev/release/build-docker-images.sh \
  --version 0.10.0 \
  --maven-repo /tmp/comet-staging-repo-XXXXX
```

The script builds and pushes five multi-arch manifests:

- `apache/datafusion-comet:<version>-spark3.4.3-scala2.12-java17`
- `apache/datafusion-comet:<version>-spark3.4.3-scala2.13-java17`
- `apache/datafusion-comet:<version>-spark3.5.8-scala2.12-java17`
- `apache/datafusion-comet:<version>-spark3.5.8-scala2.13-java17`
- `apache/datafusion-comet:<version>-spark4.0.1-scala2.13-java17`

Use `--dry-run` to preview the commands without pushing.

For release candidates, pass the full candidate version (e.g.
`--version 0.10.0-rc1`). The same script is used for both RCs and final
releases.
```

- [ ] **Step 4: Verify rendered markdown**

```bash
grep -n 'Publish Docker images to Docker Hub' docs/source/contributor-guide/release_process.md
grep -n 'GitHub Container Registry' docs/source/contributor-guide/release_process.md || echo "no GHCR references (good)"
```

Expected: the new section appears; no remaining GHCR references.

- [ ] **Step 5: Commit**

```bash
git add docs/source/contributor-guide/release_process.md
git commit -m "docs: document manual Docker image publishing for releases"
```

---

## Task 9: Document the manual end-to-end verification procedure

This task does not add code — it captures the validation procedure from the
spec so the first real user of the script has a checklist. It lives as
comments in the script (easier to find than a separate doc).

**Files:**
- Modify: `dev/release/build-docker-images.sh`

- [ ] **Step 1: Add a "Testing this script" header comment**

Insert the following block at the top of the script, immediately after the
Apache license header and before the existing `# Build and push ...`
comment:

```bash
# Manual verification (before first use on a real release):
#
#   1. Run dev/release/build-release-comet.sh against a test branch to
#      produce staging jars.
#
#   2. Dry-run against the resulting LOCAL_REPO:
#        ./dev/release/build-docker-images.sh \
#          --version 0.0.0-test \
#          --maven-repo /tmp/comet-staging-repo-XXXXX \
#          --dry-run
#      Confirms arg parsing, jar path resolution, and buildx command shape
#      without touching Docker Hub.
#
#   3. End-to-end smoke test using a personal Docker Hub namespace:
#        - Temporarily edit IMAGE_REPO below to <myuser>/datafusion-comet.
#        - Run the script without --dry-run.
#        - Verify all five manifests pushed with both architectures via:
#            docker buildx imagetools inspect <tag>
#        - Pull one image for each arch and check the jar is present:
#            docker run --rm --platform=linux/amd64 <tag> \
#              ls /opt/spark/jars/ | grep comet
#            docker run --rm --platform=linux/arm64 <tag> \
#              ls /opt/spark/jars/ | grep comet
#        - Revert IMAGE_REPO before committing.
```

- [ ] **Step 2: Verify the comment did not change behavior**

Re-run the dry-run verification from Task 6, Step 3. Expected: same output
as before.

- [ ] **Step 3: Commit**

```bash
git add dev/release/build-docker-images.sh
git commit -m "docs: document manual verification procedure for image publishing"
```

---

## Task 10: Open PR

- [ ] **Step 1: Push the branch and open a PR using the repo template**

Use the project's PR template (`.github/pull_request_template.md`). Fill in
all required sections. Do not mention Claude or AI-generation.

Title: `feat: add manual multi-arch Docker Hub release tooling`

PR body sections:

- **Which issue does this PR close?** — link any related issue; otherwise
  state "No existing issue; follow-up to the release-tooling cleanup
  discussion."
- **Rationale for this change** — the existing GHCR workflow only
  published a single Spark 3.5 / Scala 2.12 variant, and Docker Hub
  publication of the full matrix has been a manual off-repo process.
  Script makes it repeatable and replaces the obsolete workflow.
- **What changes are included in this PR?** — new
  `kube/release.Dockerfile`, new `dev/release/build-docker-images.sh`,
  removal of `.github/workflows/docker-publish.yml`, updated
  `release_process.md`.
- **How are these changes tested?** — describe `--dry-run` verification
  performed locally and the manual end-to-end procedure (per the script's
  top-of-file comment). Note that the first real release is the
  first production use.

---

## Self-Review Notes

Checked against the spec (`docs/superpowers/specs/2026-04-19-docker-image-publishing-design.md`):

- **Matrix (5 combos, all Java 17):** Task 3 hardcodes the exact 5 combos.
- **Tag scheme `<version>-spark<X.Y.Z>-scala<X.Y>-java17`:** Task 3
  computes this.
- **No `:latest` / short tag:** Not added anywhere.
- **Reuse multi-arch uber-jars:** Task 5 copies the jar unchanged into a
  temp context; buildx uses the same jar for both platforms.
- **`kube/release.Dockerfile` thin, parameterized:** Task 1.
- **Pre-flight checks (docker, buildx, platforms, login):** Task 6.
- **`--dry-run`:** Tasks 2 (flag) + 5 (expanded to shell-escaped command).
- **Delete old GHCR workflow:** Task 7.
- **Update `release_process.md`:** Task 8.
- **Manual verification procedure documented:** Task 9.

Open items from the spec ("confirm base-image tags exist", "hard fail vs
warn on login") are handled inline — Task 1 Step 2 validates the base
image; Task 6 Step 1 makes the login check a warning under `--dry-run=0`.
