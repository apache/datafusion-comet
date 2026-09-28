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
import importlib.util
import inspect
import json
import os
from pathlib import Path
import subprocess


# Share both the input patterns and their glob semantics with main's warmer.
SPEC = importlib.util.spec_from_file_location("compute_changes", Path(__file__).with_name("compute-changes.py"))
CHANGES = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHANGES)

# These are the outputs of native/proto/build.rs, included by proto/src/lib.rs.
# Their tracked protobuf inputs and generator are fingerprinted instead.
GENERATED_PROTO_FILES = {
    f"native/proto/src/generated/spark.spark_{name}.rs"
    for name in ("expression", "partitioning", "operator", "metric", "config")
}


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def command(args, cwd):
    return subprocess.check_output(args, cwd=cwd, text=True).strip()


def source_inputs(root, profile="ci"):
    """Return dependency and source maps for the selected native build profile.

    Each map contains relative names, Git modes and content digests. Untracked
    generated Rust, target files and documentation are excluded. CI library
    builds omit benchmarks; debug checks compile them. Trust only this checkout
    for the Git read: container steps can run as a different owner than checkout.
    """
    patterns = CHANGES.NATIVE_LIBRARY_INPUTS if profile == "ci" else CHANGES.NATIVE_BUILD_INPUTS
    matches = CHANGES.compile_matcher(patterns)
    inventory = command(["git", "-c", f"safe.directory={root}",
                         "ls-files", "--stage", "-z"], root)
    # Hash the imported rules themselves, not unrelated workflow routing in
    # compute-changes.py. Include the predicate as well as the glob translator:
    # changing include/exclude semantics must invalidate an existing library.
    sources = {"@native-input-rules": digest([
        patterns, inspect.getsource(CHANGES.glob_to_regex),
        inspect.getsource(CHANGES.compile_matcher),
    ])}
    dependencies = {}
    for record in inventory.split("\0"):
        if not record:
            continue
        metadata, name = record.split("\t", 1)
        if matches(name):
            value = [metadata.split()[0], hashlib.sha256((root / name).read_bytes()).hexdigest()]
            sources[name] = value
            if Path(name).name in {"Cargo.toml", "Cargo.lock"}:
                dependencies[name] = value
    return dependencies, sources


def depinfo_inputs(text):
    """Read Cargo's top-level .d rule (not rustc's deps/*.d format).

    Cargo emits one rule with absolute paths in our build configuration. It
    escapes spaces with a backslash, but leaves #, $, colons and other
    backslashes literal. A shell or generic Makefile parser would corrupt
    those names. Reject other formats rather than silently drop dependencies.
    """
    lines = text.splitlines()
    if len(lines) != 1 or "\0" in text:
        raise ValueError("expected one Cargo dependency rule")
    target, separator, prerequisites = lines[0].partition(": ")
    if not separator or not target or not prerequisites:
        raise ValueError("Cargo dependency rule has no target or inputs")
    paths, word = [], []
    index = 0
    while index < len(prerequisites):
        character = prerequisites[index]
        if character == "\\" and prerequisites[index:index + 2] == "\\ ":
            word.append(" ")
            index += 2
            continue
        if character == " ":
            if word:
                paths.append("".join(word))
                word = []
        else:
            word.append(character)
        index += 1
    if word:
        paths.append("".join(word))
    if not paths:
        raise ValueError("Cargo dependency rule has no inputs")
    return paths


def check_depinfo(root, depinfo, env):
    """Fail if Cargo consumed files outside the tracked library fingerprint.

    Generated protobuf outputs and the installed JDK are explicit exceptions;
    their inputs/tool identity are recorded by the pre-build fingerprint. A
    build-script directory declaration covers its files recursively. This is
    a guard for declared Cargo inputs, not an audit of undeclared script reads.
    """
    root = root.resolve()
    java_home = Path(env["JAVA_HOME"]).resolve(strict=True)
    _, sources = source_inputs(root)
    matches = CHANGES.compile_matcher(CHANGES.NATIVE_LIBRARY_INPUTS)
    uncovered = set()

    def covered(path):
        resolved = path.resolve(strict=True)
        if resolved.is_relative_to(java_home):
            return True
        # Check both the named and resolved location. In particular, a symlink
        # below the generated directory must not admit an arbitrary input.
        if not path.is_relative_to(root) or not resolved.is_relative_to(root):
            return False
        name = path.relative_to(root).as_posix()
        resolved_name = resolved.relative_to(root).as_posix()
        if resolved.is_dir():
            if path.is_symlink() or not matches(name) or not matches(resolved_name):
                return False
            return all(covered(child) for child in path.iterdir())
        return (name in sources and resolved_name in sources
                or name in GENERATED_PROTO_FILES and resolved_name == name)

    for name in depinfo_inputs(depinfo.read_text()):
        path = Path(name)
        # build.dep-info-basedir can rewrite paths relative to an arbitrary
        # directory. Our official build does not set it; do not guess a base.
        if not path.is_absolute():
            uncovered.add(f"{name} (relative dep-info paths are unsupported)")
            continue
        try:
            if not covered(path):
                uncovered.add(name)
        except (OSError, RuntimeError):
            uncovered.add(f"{name} (missing or unresolvable)")
    if uncovered:
        raise ValueError("Cargo inputs missing from the native cache fingerprint:\n  "
                         + "\n  ".join(sorted(uncovered)))


def environment_inputs(root, env):
    """Identify the official tools installed by setup-builder without modifying them.

    Rust's versions include the compiler commit; dpkg identifies the installed
    C/C++/protobuf tools and system libraries. The JDK release file identifies
    the vendor/build supplying JNI headers and libjvm. Record build overrides,
    including target-qualified cc variables and HDFS linking options, without
    including unrelated per-run GitHub variables. The shared setup/build actions
    are hashed separately; caller test configuration does not affect the library.
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
        "java_release": (java_home / "release").read_text(),
        "env": {name: value for name, value in env.items()
                if name.startswith(("CARGO_", "RUST", "HOST_", "TARGET_", "HDFS_"))
                or name.split("_", 1)[0] in {"CC", "CXX", "CFLAGS", "CXXFLAGS", "CXXSTDLIB",
                                           "LDFLAGS", "AR", "ARFLAGS", "RANLIB", "RANLIBFLAGS", "PROTOC"}
                or name in {"JAVA_HOME", "PATH", "HADOOP_HOME", "DOCS_RS",
                            "CRATE_CC_NO_DEFAULTS", "CROSS_COMPILE"}},
    }


def cache_keys(profile, dependencies, sources, environment):
    """Return output keys for one pre-build snapshot.

    Only the incremental Cargo cache has a source-independent restore prefix.
    The library key includes all tracked build inputs and never uses fallback.
    Both retain the environment: native build scripts can reuse C objects
    without detecting changes to external compiler binaries or JNI headers.
    """
    prefix = f"Linux-cargo-{profile}-v3-{digest([environment, dependencies])}-"
    keys = {
        "cargo-key": prefix + digest(sources),
        "restore-prefix": prefix,
    }
    if profile == "ci":
        keys["library-key"] = f"Linux-native-ci-v2-{digest([environment, sources])}"
    return keys


def main():
    """Snapshot the checkout root and publish keys after all reads succeed.

    --profile snapshots inputs; --check-depinfo validates a completed build.
    --github-output appends the same key=value records printed to stdout.
    Git/tool/file failures propagate and fail CI.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--profile", choices=("ci", "debug"))
    mode.add_argument("--check-depinfo", type=Path)
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args()
    if args.check_depinfo and args.github_output:
        parser.error("--github-output requires --profile")
    cwd = Path.cwd().resolve()
    root = Path(command(["git", "-c", f"safe.directory={cwd}",
                         "rev-parse", "--show-toplevel"], cwd))
    if args.check_depinfo:
        try:
            check_depinfo(root, args.check_depinfo, os.environ)
        except (ValueError, OSError, KeyError) as error:
            parser.exit(1, f"{error}\n")
        print("Cargo dependency inputs are covered by the native cache fingerprint.")
        return
    dependencies, sources = source_inputs(root, args.profile)
    environment = environment_inputs(root, os.environ)
    keys = cache_keys(args.profile, dependencies, sources, environment)
    output = "".join(f"{key}={value}\n" for key, value in keys.items())
    if args.github_output:
        with args.github_output.open("a") as stream:
            stream.write(output)
    print(output, end="")


if __name__ == "__main__":
    main()
