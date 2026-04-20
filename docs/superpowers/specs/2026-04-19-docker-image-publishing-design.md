# Manual Docker Image Publishing for Releases

Date: 2026-04-19
Status: Approved, ready for implementation planning

## Goal

Give the release manager a single manual script that builds and publishes
multi-arch (`linux/amd64` + `linux/arm64`) Docker images for Apache
DataFusion Comet to Docker Hub (`apache/datafusion-comet`), covering the full
supported Spark × Scala matrix, as part of the existing release process.

## Non-goals

- Automated publishing from a GitHub Actions workflow. The existing
  `.github/workflows/docker-publish.yml` is removed; publishing is manual.
- A `:latest` tag or short `<version>` tag. Only fully-qualified
  `spark<X.Y.Z>-scala<X.Y>-java17` tags are published.
- Changes to the existing user-facing `kube/Dockerfile` (the build-from-source
  image documented in `kubernetes.md`). It stays as-is for users who want to
  build their own image from source.
- Changes to the benchmark images under `benchmarks/`.

## Matrix

Five multi-arch manifests are published per release, one per supported
Spark × Scala combination. All use Java 17.

| # | Spark | Scala | Java | Spark base image        |
|---|-------|-------|------|-------------------------|
| 1 | 3.4.3 | 2.12  | 17   | `apache/spark:3.4.3-java17` |
| 2 | 3.4.3 | 2.13  | 17   | `apache/spark:3.4.3-scala2.13-java17` |
| 3 | 3.5.8 | 2.12  | 17   | `apache/spark:3.5.8-java17` |
| 4 | 3.5.8 | 2.13  | 17   | `apache/spark:3.5.8-scala2.13-java17` |
| 5 | 4.0.1 | 2.13  | 17   | `apache/spark:4.0.1`    |

Base images above are the target; the implementation task must verify each tag
actually exists on Docker Hub and adjust (e.g., swap `-java17` for
`-scala2.13-java17`) per what Apache Spark publishes.

## Tag scheme

```
apache/datafusion-comet:<version>-spark<X.Y.Z>-scala<X.Y>-java17
```

`<version>` is taken verbatim from the release tag:

- Final release:      `0.10.0-spark3.5.8-scala2.12-java17`
- Release candidate:  `0.10.0-rc1-spark3.5.8-scala2.12-java17`

## Approach: reuse multi-arch uber-jars

The existing `dev/release/build-release-comet.sh` already builds multi-arch
Comet uber-jars. Each jar bundles both `linux/amd64` and `linux/aarch64`
native libraries under `org/apache/comet/linux/<arch>/libcomet.so`. The same
jar works inside an amd64 or arm64 container.

This means the Docker build does not need to compile anything: it is a thin
`FROM <spark-base> + COPY comet.jar` image. The multi-arch manifest is
constructed by `docker buildx` running the same trivial build for each
platform in parallel and pushing both as a single manifest list.

This sidesteps the cost of QEMU-emulated Rust compiles and keeps the image
minimal (no build toolchain left behind).

## Components

### 1. `kube/release.Dockerfile` (new)

```dockerfile
ARG SPARK_IMAGE
FROM ${SPARK_IMAGE}

ARG COMET_JAR
USER root

COPY ${COMET_JAR} $SPARK_HOME/jars/
```

Notes:
- `SPARK_IMAGE` is a build-arg, not a runtime env var, so it is resolved at
  build time and each combo pulls a different base.
- `COMET_JAR` is a path relative to the build context. The orchestrating
  script stages the correct jar into a temp context before invoking buildx.
- Kept separate from `kube/Dockerfile` so the user-facing build-from-source
  flow is untouched.

### 2. `dev/release/build-docker-images.sh` (new)

#### Interface

```
Usage: build-docker-images.sh --version <comet-version> --maven-repo <path> [--dry-run]

  --version      Comet version, e.g. 0.10.0 or 0.10.0-rc1 (required)
  --maven-repo   Path to the staging Maven repo printed by
                 build-release-comet.sh, e.g. /tmp/comet-staging-repo-XXXXX
                 (required)
  --dry-run      Print the `docker buildx` commands without executing them
```

#### Flow

1. **Validate args and environment.** Fail fast if:
   - `--version` or `--maven-repo` missing
   - `docker` or `docker buildx` not on `$PATH`
   - Active buildx builder does not support both `linux/amd64` and
     `linux/arm64` (check via `docker buildx inspect`)
   - No active Docker Hub credentials (best-effort check of
     `~/.docker/config.json`; if ambiguous, print a warning but proceed)

2. **Resolve matrix.** Hardcoded in the script:
   ```
   combos=(
     "3.4 3.4.3 2.12 apache/spark:3.4.3-java17"
     "3.4 3.4.3 2.13 apache/spark:3.4.3-scala2.13-java17"
     "3.5 3.5.8 2.12 apache/spark:3.5.8-java17"
     "3.5 3.5.8 2.13 apache/spark:3.5.8-scala2.13-java17"
     "4.0 4.0.1 2.13 apache/spark:4.0.1"
   )
   ```
   Each row expands to `spark_short`, `spark_full`, `scala`, `base_image`.

3. **For each combo (sequential, fail-fast):**
   a. Compute `jar_path = $MAVEN_REPO/org/apache/comet/comet-spark-spark${spark_short}_${scala}/${version}/comet-spark-spark${spark_short}_${scala}-${version}.jar`
   b. Verify the jar exists; fail with a clear error if not (prints the
      expected path and suggests re-running `build-release-comet.sh`)
   c. Stage into a temp build context: `mktemp -d` and copy the jar as
      `comet.jar`
   d. Compute `tag = apache/datafusion-comet:${version}-spark${spark_full}-scala${scala}-java17`
   e. Run (or print under `--dry-run`):
      ```
      docker buildx build \
        --platform linux/amd64,linux/arm64 \
        --build-arg SPARK_IMAGE=${base_image} \
        --build-arg COMET_JAR=comet.jar \
        -f kube/release.Dockerfile \
        -t ${tag} \
        --push \
        <temp-context>
      ```
   f. On success, append tag to a list; on failure, exit non-zero with the
      combo identifier in the error message

4. **Print summary** of all pushed tags at the end.

### 3. `.github/workflows/docker-publish.yml` (delete)

Removed entirely. The workflow currently publishes a single-variant image to
GHCR on release tags; it is superseded by the manual script.

### 4. `docs/source/contributor-guide/release_process.md` (update)

Add a new section, "Publish Docker images to Docker Hub", after the existing
`build-release-comet.sh` step. Contents:

- Prereqs: `docker login`, buildx with multi-arch builder (one-time setup
  shown inline)
- The exact command to run, using the `LOCAL_REPO` path printed by
  `build-release-comet.sh`
- The list of 5 expected output tags

Also remove the two existing lines (around 222 and 393) that describe the
GHCR workflow as being triggered by pushing a tag, replacing them with a
pointer to the new manual step.

## Testing plan

This cannot be CI-tested without publishing; validation happens on a
maintainer's machine before the first real release using the tool.

1. Run `build-release-comet.sh` against a test branch to produce staging jars.
2. Run `build-docker-images.sh --dry-run --version 0.0.0-test --maven-repo
   <path>`. Confirms path resolution, tag construction, and command formation
   without touching Docker Hub.
3. Log into a personal Docker Hub namespace, temporarily edit the script's
   image prefix to `<myuser>/datafusion-comet`, run the real script
   end-to-end. Verify:
   - All 5 manifests pushed, each with 2 architectures, via
     `docker buildx imagetools inspect <tag>`
   - `docker run --platform=linux/amd64` and `--platform=linux/arm64` on one
     combo each, confirm `ls $SPARK_HOME/jars/comet-*.jar` shows the
     expected jar
   - `spark-submit` on a trivial job still starts and the Comet plugin loads

## Open items for the implementation plan

- Confirm the exact apache/spark base-image tags that exist for each combo;
  swap any missing tags for the closest available variant.
- Decide whether `docker login` check is a hard fail or a warning (leaning
  warning, since the error from `buildx --push` is already clear).
