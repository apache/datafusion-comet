#!/usr/bin/env python3
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

"""Fingerprint the clean Linux checkout and toolchain used by Comet CI.

Run after setup-builder and before Cargo generates source files. This helper
supports the official Rust container, setup-builder's JDK/packages, and the
build commands in our workflows; it is not a general local-build cache.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess


SOURCE_PREFIXES = ("native/", "contrib/", "common/", ".cargo/", ".mvn/",
                   ".github/actions/", ".github/workflows/", "dev/ci/")
SOURCE_FILES = {"Makefile", "pom.xml", "mvnw", "rust-toolchain", "rust-toolchain.toml"}


def digest(value):
    """Return a stable SHA-256 for JSON-compatible build inputs."""
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def command(args, cwd):
    """Read command stdout in cwd; missing tools or unsuccessful commands fail CI."""
    return subprocess.check_output(args, cwd=cwd, text=True).strip()


def source_inputs(root):
    """Read tracked build files and return dependency and complete input maps.

    Each map contains relative names, Git modes and content digests. Untracked
    generated Rust and target files are excluded. Trust only this checkout for
    the Git read: container steps can run as a different owner than checkout.
    """
    inventory = command(["git", "-c", f"safe.directory={root}",
                         "ls-files", "--stage", "-z"], root)
    sources = {}
    for record in inventory.split("\0"):
        if not record:
            continue
        metadata, name = record.split("\t", 1)
        if name in SOURCE_FILES or name.startswith(SOURCE_PREFIXES):
            sources[name] = [metadata.split()[0],
                             hashlib.sha256((root / name).read_bytes()).hexdigest()]
    dependencies = {name: value for name, value in sources.items()
                    if Path(name).name in {"Cargo.toml", "Cargo.lock"}}
    return dependencies, sources


def environment_inputs(root, env):
    """Identify the official tools installed by setup-builder without modifying them.

    Rust's versions include the compiler commit; dpkg identifies the installed
    C/C++/protobuf tools and system libraries. The JDK release file identifies
    the vendor/build supplying JNI headers and libjvm. Paths and RUSTFLAGS are
    included because linking can embed them. Workflow/action files in the source
    map cover changes to how these tools are installed and invoked.
    """
    java_home = Path(env["JAVA_HOME"])
    return {
        "workspace": str(root),
        "architecture": command(["uname", "-m"], root),
        "rust": {tool: command([tool, flag], root / "native")
                 for tool, flag in (("rustc", "-vV"), ("cargo", "--version"),
                                    ("rustfmt", "--version"))},
        "packages": sorted(command(["dpkg-query", "-W",
                                    "-f=${binary:Package}\t${Version}\t${Architecture}\n"], root).splitlines()),
        "java_home": str(java_home),
        "java_release": (java_home / "release").read_text(),
        "cargo_home": env.get("CARGO_HOME", str(Path.home() / ".cargo")),
        "rustflags": env["RUSTFLAGS"],
    }


def cache_keys(profile, dependencies, sources, environment):
    """Return output keys for one pre-build snapshot.

    Only the incremental Cargo cache has a source-independent restore prefix.
    The library key includes all tracked build inputs and never uses fallback.
    """
    prefix = f"Linux-cargo-{profile}-v3-{digest([environment, dependencies])}-"
    return {
        "cargo-home": environment["cargo_home"],
        "source-key": prefix + digest(sources),
        "restore-prefix": prefix,
        "binary-key": f"Linux-native-ci-v2-{digest([environment, sources])}" if profile == "ci" else "",
    }


def main():
    """Snapshot the checkout root and publish keys after all reads succeed.

    Run from the repository root. --github-output appends the same key=value
    records printed to stdout. Git/tool/file failures propagate and fail CI.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True, choices=("ci", "debug"))
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args()
    cwd = Path.cwd().resolve()
    root = Path(command(["git", "-c", f"safe.directory={cwd}",
                         "rev-parse", "--show-toplevel"], cwd))
    dependencies, sources = source_inputs(root)
    environment = environment_inputs(root, os.environ)
    keys = cache_keys(args.profile, dependencies, sources, environment)
    output = "".join(f"{key}={value}\n" for key, value in keys.items())
    if args.github_output:
        with args.github_output.open("a") as stream:
            stream.write(output)
    print(output, end="")


if __name__ == "__main__":
    main()
