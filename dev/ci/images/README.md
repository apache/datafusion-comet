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

# Linux Spark CI image pilot

This directory implements the image recipe and offline verification for
[GH-5490](https://github.com/apache/datafusion-comet/issues/5490). It does not
publish images or change the images used by the existing Spark test jobs.

The pilot is Linux/amd64 with x86-64-v3 support, Spark 4.1.3, and Zulu JDK 17.
The Dockerfile pins the
Rust and JDK base images by digest. `versions.env` pins the tool versions,
clang/protoc Debian packages, Maven wrapper distribution, and Spark's SBT version.
`RUSTUP_TOOLCHAIN` selects the pinned compiler instead of the checkout's floating
`stable` selector, including when networking is disabled.
The Spark source archive is pinned by commit and SHA-256, and the SBT launcher
has a pinned SHA-256. The Debian package inventory is recorded in
`/opt/comet-ci/os-packages.txt`.

## Build

Run from the Comet repository root, with Docker and BuildKit available:

```bash
bash dev/ci/images/build.sh comet-ci:spark-4.1.3-jdk17
```

To check just the toolchain, without the dependency build:

```bash
bash dev/ci/images/build.sh comet-ci:toolchain toolchain
```

`COMET_CI_BUILD_JOBS` controls native compilation parallelism (default: 4).
Set it to 2 on a runner with limited memory. The complete build compiles native
Comet, runs the full Maven reactor with the Spark 4.1 release profile, compiles
Spark's catalyst/sql/hive test classes, and runs two smoke suites. Allow enough
time and disk space for both the image build and the later independent rebuild.

The recipe uses a local BuildKit cache for native compiler output to make
iteration practical. This cache is not copied into the image, and the offline
validator never mounts it. Do not publish the intermediate
`dependencies` stage or export its layers as a public build cache.

## Verify the final image offline

Stage any new source files first: the validator archives tracked working files,
including modifications, but excludes untracked files and host build caches.
It downloads and verifies the pinned Spark sources before disabling networking.

```bash
bash dev/ci/images/validate.sh comet-ci:spark-4.1.3-jdk17
```

The validator starts a new container with `--network=none`, an empty
`/github/home`, and a different workspace path from the warmup stage.
Only Comet and Spark source archives are mounted from the host. It then:

1. Audits the image for forbidden artifacts and compares its dependency input
   fingerprints against the current checkout.
2. Boots the Maven wrapper without downloading its distribution.
3. Builds native Comet with Cargo offline and the lockfile enforced.
4. Installs the current Comet JVM artifacts with Maven offline, from the
   repository root. It does not use `-pl`.
5. Compiles Spark's catalyst, SQL, and Hive test classes with SBT offline.
6. Runs `LiteralExpressionSuite` and `MathFunctionsSuite` with Comet enabled,
   checking that each suite actually ran tests.

GitHub checkout and artifact transport are deliberately outside this offline
test. Loopback networking remains available for local Spark services. This is
a dependency-completeness check for the pinned pilot, not a claim that every
Spark test or every changed dependency graph can run offline.

A missing dependency is a validation failure. Do not mount a host Maven/Cargo
cache or turn networking back on to make it pass. Warm the required dependency
in the recipe, rebuild the image, and repeat the fresh-container check instead.

## What is in the image

Toolchains and downloaded third-party dependencies live under
`/opt/comet-ci`. Explicit Cargo, Maven, SBT, Coursier, and Ivy paths survive
GitHub's mounted home and workspace directories. Java's `user.home` is also
set explicitly, and its `.m2` points to the same repository used by Maven.
No container entrypoint is required to initialize these paths.

Maven and SBT's standard resolver use Maven Central, plus the job's local Maven
repository for freshly built Comet artifacts. The checked-in, credential-free
Maven settings redirect Spark's Google Maven Central mirror to the canonical
repository; SBT uses the explicit `repositories` file. This keeps Coursier's
URL-based cache paths consistent. Spark's BOM plugin constructs a separate Ivy
resolver, so the runner copies `CometCiRepositories.scala` into the temporary
Spark build definition to direct that resolver to the same repositories. Its
Ivy cache is exported and checked by the offline build too. No existing CI
workflow's repository configuration changes.

The dependency stage may compile Comet and Spark to discover the dependencies
actually used by the build. The final stage starts again from the toolchain
image and copies only the exported caches. It excludes:

- Comet Maven artifacts, including released versions, and all SNAPSHOT artifacts.
- Locally installed Maven artifacts, identified by Maven Resolver origin records.
- User Maven settings/credentials, Ivy local publications, Coursier file repositories,
  and SBT's locally compiled Zinc bridge cache.
- Comet and Spark build workspaces and outputs, including Comet libraries/JARs, Spark test
  classes, and native compiler output.

The exporter keeps Maven's downloaded dependencies before applying the existing
partial-POM/Parquet workaround. Warmup and offline verification both apply that
workaround before SBT resolution, ensuring Parquet test classifiers come from
the warmed Coursier cache. The SBT launcher is also stored explicitly because
Spark otherwise downloads it into its source checkout.

Downloads of Spark sources and the SBT launcher have bounded curl retries.
Cargo's download retry count and Maven's HTTP transfer retry count are bounded.
No compilation or test command is retried.

## Maintenance and scope

Update the pins in `versions.env`, the matching Dockerfile image digests, and
`RUSTUP_TOOLCHAIN` together, build, and rerun offline validation.
If a pinned Debian package disappears from the configured repositories, update
the pin; do not silently fall back to an unpinned version. The base digest and
tool pins do not make all OS dependencies reproducible: apt may install newer
transitive dependencies, whose versions are recorded in the image inventory.

`inputs.sha256` records the Maven POMs, Cargo manifests and lockfiles, Maven
wrapper properties, Spark patch, image recipe and scripts, repository settings,
and version manifest. `source-revision`
records the Comet revision used for warmup, with a dirty suffix when applicable.
The fingerprint deliberately excludes source files and generated output.

Future PRs will add trusted main-only publishing, scheduled refreshes,
digest-based adoption, rollback, and measurements against the existing CI
workflow. New dependency downloads must remain possible in normal PR jobs.
The image is CI infrastructure, not a Comet release artifact.

## Fast regression checks

```bash
python3 dev/ci/images/test_image.py
```

These standard-library tests cover cache filtering, Maven origin records,
snapshot/local-artifact rejection, missing classifiers, the Parquet workaround,
dependency fingerprints, and shell syntax. They do not require Docker and do
not replace the full offline validation. Wiring these checks into CI is left
to the workflow follow-up.
