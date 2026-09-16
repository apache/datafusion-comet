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

"""Snapshot Linux CI cache keys before Cargo creates generated sources.

The dependency prefix permits Cargo to rebuild changed source incrementally.
The compact library key is exact-only: a hit permits skipping Cargo altogether.
Neither key contains the commit SHA, so unrelated Spark edits can reuse native
outputs. Unknown build environments fail before any outputs are written.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import stat
import subprocess
import sys
import tomllib


SOURCE_PREFIXES = ("native/", "contrib/", "common/", ".cargo/", ".mvn/",
                   ".github/actions/", ".github/workflows/", "dev/ci/")
SOURCE_FILES = {"Makefile", "pom.xml", "mvnw", "rust-toolchain", "rust-toolchain.toml"}
# These can select arbitrary executable/source files outside the tracked input
# set. Supporting one requires adding its transitive inputs to the identity.
UNSUPPORTED_ENV = {
    "RUSTC", "RUSTDOC", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER",
    "CARGO_ENCODED_RUSTFLAGS", "CARGO_TARGET_DIR", "CARGO_BUILD_TARGET",
    "CARGO_BUILD_RUSTC", "CARGO_BUILD_RUSTDOC", "CARGO_BUILD_RUSTC_WRAPPER",
    "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER", "CARGO_BUILD_TARGET_DIR",
    "CC", "CXX", "AR", "LD", "PROTOC", "PROTOC_INCLUDE", "LIBCLANG_PATH",
    "DOCS_RS", "HDFS_LIB_DIR", "HADOOP_HOME", "HDFS_STATIC",
    "CFLAGS", "CXXFLAGS", "CPPFLAGS", "LDFLAGS", "LIBRARY_PATH", "CPATH",
    "C_INCLUDE_PATH", "CPLUS_INCLUDE_PATH", "OBJC_INCLUDE_PATH", "LD_PRELOAD",
    "CRATE_CC_NO_DEFAULTS", "CMAKE", "MAKE", "MAKEFLAGS",
}
BUILD_ENV_PREFIXES = ("CARGO_", "RUST", "CC_", "CXX_", "AR_", "CFLAGS", "CXXFLAGS",
                      "CPPFLAGS", "LDFLAGS", "BINDGEN_", "PKG_CONFIG", "OPENSSL_",
                      "ZSTD_", "LZ4_", "SNAPPY_", "HDFS_", "COMET_")
BUILD_ENV_NAMES = {"PATH", "JAVA_HOME", "CARGO_HOME", "HOME", "LIBRARY_PATH",
                   "LD_LIBRARY_PATH", "CPATH", "C_INCLUDE_PATH", "CPLUS_INCLUDE_PATH",
                   "SOURCE_DATE_EPOCH"}
TOOLS = {
    "rustc": ("-vV",), "cargo": ("--version",), "rustfmt": ("--version",),
    "protoc": ("--version",),
    "cc": ("--version",), "c++": ("--version",), "clang": ("--version",),
    "ld.bfd": ("--version",), "ar": ("--version",), "pkg-config": ("--version",),
}


def digest(value):
    """Return SHA-256 of a JSON-compatible value with stable map ordering.

    Values stay in memory; callers publish only the digest, never raw build
    environment values, which may contain credentials in Cargo settings.
    """
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"))
                          .encode("utf-8")).hexdigest()


def command(args, cwd, env):
    """Return nonempty command stdout as bytes using an explicit cwd/environment.

    Nonzero status, missing tools, or empty output raises ValueError without
    echoing stdout/stderr or environment values. No shell interpolation occurs.
    """
    try:
        result = subprocess.run(args, cwd=cwd, env=env, check=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except (OSError, subprocess.CalledProcessError) as exc:
        raise ValueError(f"cannot fingerprint required command {args[0]}") from exc
    if not result.stdout.strip():
        raise ValueError(f"empty fingerprint from required command {args[0]}")
    return result.stdout


def file_identity(path):
    """Return mode and content digest for one required regular input file.

    Symlinks and missing files fail closed: hashing a link alone would omit
    mutable external inputs. Files are read without modifying their contents.
    """
    mode = path.lstat().st_mode
    if not stat.S_ISREG(mode):
        raise ValueError(f"unsupported non-regular input: {path}")
    return [stat.S_IMODE(mode), hashlib.sha256(path.read_bytes()).hexdigest()]


def directory_identity(path):
    """Return every regular file's relative name and digest below a required tree.

    Directory symlinks are rejected as well as file symlinks, so an external JNI
    include tree cannot silently escape the snapshot. The tree is read only.
    """
    if path.is_symlink() or not path.is_dir():
        raise ValueError(f"unsupported input directory: {path}")
    result = {}
    for child in sorted(path.rglob("*")):
        if child.is_symlink() or not child.is_dir():
            result[str(child.relative_to(path))] = file_identity(child)
    return result


def tracked_inputs(root, env):
    """Return dependency and source snapshots from Git's tracked file inventory.

    Read worktree bytes and modes, so staged or unstaged edits invalidate keys.
    The inventory excludes generated/untracked files and target directories.
    Missing tracked inputs, conflicts, symlinks, or submodules fail closed.
    Trust this checkout for this command: container CI can run under a different
    owner than checkout, whose temporary global Git configuration is not kept.
    """
    inventory = command(["git", "-c", f"safe.directory={root}", "ls-files", "--stage", "-z"], root, env)
    sources = {}
    dependencies = {}
    for record in inventory.split(b"\0"):
        if not record:
            continue
        metadata, raw_path = record.split(b"\t", 1)
        mode, _, stage = metadata.decode("ascii").split()
        name = os.fsdecode(raw_path)
        if name not in SOURCE_FILES and not name.startswith(SOURCE_PREFIXES):
            continue
        if stage != "0" or mode not in {"100644", "100755"}:
            raise ValueError(f"unsupported tracked input: {name}")
        identity = file_identity(root / name)
        sources[name] = identity
        if Path(name).name in {"Cargo.toml", "Cargo.lock"}:
            dependencies[name] = identity
    for name in ("native/Cargo.toml", "native/Cargo.lock"):
        if name not in dependencies:
            raise ValueError(f"missing tracked build input: {name}")
    return dependencies, sources


def cargo_config_directories(root, cargo_home):
    """Return Cargo's ordered search locations without reading or writing them.

    Cargo starts at the native workspace and visits ancestor .cargo directories,
    plus CARGO_HOME. Returning a set in stable order avoids duplicate reads.
    """
    directories = {cargo_home}
    directories.update(path / ".cargo" for path in (root / "native", root, *root.parents))
    return sorted(directories)


def cargo_configs(root, cargo_home):
    """Return content identities for Cargo configs in its search locations.

    Include both supported filenames from native/ through filesystem root and
    CARGO_HOME, including untracked external configs. Reject config features
    that refer to extra executable/source files outside this input snapshot;
    adding support for them requires extending this fingerprint first.
    """
    result = {}
    for directory in cargo_config_directories(root, cargo_home):
        for name in ("config", "config.toml"):
            path = directory / name
            if not path.exists():
                continue
            content = tomllib.loads(path.read_text(encoding="utf-8"))
            # Registry/transport settings do not select source files outside
            # Cargo.lock. Build flags are content-addressed; wrappers, custom
            # targets/linkers, source replacement, and config includes are not.
            if set(content) - {"build", "net", "http", "registries", "registry"}:
                raise ValueError(f"unsupported Cargo config section: {path}")
            if set(content.get("build", {})) - {"rustflags", "rustdocflags", "jobs", "incremental"}:
                raise ValueError(f"unsupported Cargo build configuration: {path}")
            if "target-cpu=native" in path.read_text(encoding="utf-8"):
                raise ValueError(f"host-specific CPU flags are not reusable: {path}")
            result[str(path)] = file_identity(path)
    return result


def environment_identity(root, profile, env):
    """Return the required Linux toolchain, platform, JDK and build-env snapshot.

    The supplied mapping is the caller's effective environment before building.
    This supports the repository's fixed Linux CI commands, not arbitrary local
    tool overrides. Missing fingerprints raise before a reusable key can exist.
    Package versions cover linker/compiler libraries in the mutable CI image.
    Compiler/include/library overrides are rejected because hashing a path or
    flag such as `-include /tmp/header.h` does not fingerprint the file it reads.
    The only extra library search path supported is the fingerprinted JDK's
    server directory. Rustup proxies are resolved to their actual tool binaries.
    """
    for name in UNSUPPORTED_ENV:
        if env.get(name):
            raise ValueError(f"unsupported build override: {name}")
    for name in env:
        if name.startswith(("CARGO_TARGET_", "CARGO_PROFILE_", "CC_", "CXX_", "AR_",
                            "CFLAGS_", "CXXFLAGS_", "CPPFLAGS_", "LDFLAGS_", "HDFS_",
                            "HOST_CC", "HOST_CXX", "HOST_AR", "HOST_CFLAGS", "HOST_CXXFLAGS",
                            "TARGET_CC", "TARGET_CXX", "TARGET_AR", "TARGET_CFLAGS", "TARGET_CXXFLAGS",
                            "CMAKE_", "HOST_CMAKE", "TARGET_CMAKE",
                            "OPENSSL_", "PKG_CONFIG", "BINDGEN_", "ZSTD_", "LZ4_", "SNAPPY_")):
            raise ValueError(f"unsupported build override: {name}")
        if name.startswith("CARGO_BUILD_") and name not in {"CARGO_BUILD_JOBS", "CARGO_BUILD_INCREMENTAL"}:
            raise ValueError(f"unsupported build override: {name}")
    flags = shlex.split(env.get("RUSTFLAGS", ""))
    if profile == "ci" and flags != ["-Ctarget-cpu=x86-64-v3", "-Clink-arg=-fuse-ld=bfd"]:
        raise ValueError("CI library reuse requires the fixed x86-64-v3/bfd RUSTFLAGS")
    if profile == "debug" and flags != ["-Clink-arg=-fuse-ld=bfd"]:
        raise ValueError("debug cache reuse requires the fixed bfd RUSTFLAGS")
    if not env.get("JAVA_HOME"):
        raise ValueError("JAVA_HOME is required")
    java_home = Path(env["JAVA_HOME"]).resolve(strict=True)
    library_path = env.get("LD_LIBRARY_PATH", "")
    if library_path and library_path not in {str(java_home / "lib/server"),
                                            str(Path(env["JAVA_HOME"]) / "lib/server")}:
        raise ValueError("unsupported build override: LD_LIBRARY_PATH")
    if not (java_home / "include/jni.h").is_file():
        raise ValueError("JAVA_HOME must contain JNI headers")
    cargo_home = Path(env.get("CARGO_HOME") or str(Path(env["HOME"]) / ".cargo")).resolve()
    if "\n" in str(cargo_home) or "\r" in str(cargo_home):
        raise ValueError("CARGO_HOME must fit one GitHub output line")
    versions = {}
    for tool, args in TOOLS.items():
        executable = shutil.which(tool, path=env.get("PATH"))
        if not executable:
            raise ValueError(f"missing required tool: {tool}")
        launcher = Path(executable).resolve(strict=True)
        resolved = launcher
        if tool in {"rustc", "cargo", "rustfmt"}:
            resolved = Path(command(["rustup", "which", tool], root / "native", env).decode().strip())
            if not resolved.is_absolute():
                raise ValueError(f"rustup returned a non-absolute path for {tool}")
            resolved = resolved.resolve(strict=True)
        versions[tool] = {
            "launcher_path": str(launcher), "launcher": file_identity(launcher),
            "path": str(resolved), "binary": file_identity(resolved),
            "version": command([tool, *args], root / "native", env).decode("utf-8"),
        }
    system = command(["uname", "-s"], root, env).decode().strip()
    architecture = command(["uname", "-m"], root, env).decode().strip()
    if system != "Linux" or architecture != "x86_64":
        raise ValueError("native cache identity supports Linux x86_64 only")
    packages = command(["dpkg-query", "-W", "-f=${binary:Package}\t${Version}\t${Architecture}\n"],
                       root, env).decode("utf-8").splitlines()
    return cargo_home, {
        "profile": profile, "root": str(root), "system": system,
        "architecture": architecture, "packages": sorted(packages), "tools": versions,
        "java_home": str(java_home), "java_release": file_identity(java_home / "release"),
        "libjvm": file_identity(java_home / "lib/server/libjvm.so"),
        "jni_headers": directory_identity(java_home / "include"),
        "cargo_configs": cargo_configs(root, cargo_home),
        "environment": {key: value for key, value in env.items()
                        if key in BUILD_ENV_NAMES or key.startswith(BUILD_ENV_PREFIXES)},
    }


def cache_keys(profile, dependencies, sources, environment, cargo_home):
    """Return immutable GitHub output strings for a complete input snapshot.

    dependency-key includes environment and manifests; source-key additionally
    includes tracked source/build files. Only ci has a usable exact binary-key.
    The restore-prefix intentionally excludes source so Cargo can rebuild it.
    """
    dependency_key = f"Linux-cargo-{profile}-v2-{digest([environment, dependencies])}"
    source_key = f"{dependency_key}-{digest(sources)}"
    return {
        "cargo-home": str(cargo_home), "dependency-key": dependency_key,
        "source-key": source_key, "restore-prefix": f"{dependency_key}-",
        "binary-key": f"Linux-native-ci-v1-{digest([environment, sources])}" if profile == "ci" else "",
    }


def main():
    """Snapshot keys at repository root and publish them only after full success.

    --github-output optionally appends the same key=value records printed on
    stdout. Failure returns status 1 with a concise error and writes no outputs.
    Invoke from the checkout root; Git trusts only that directory for these
    reads without changing global configuration or trusting other checkouts.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True, choices=("ci", "debug"))
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args()
    try:
        env = dict(os.environ)
        cwd = Path.cwd().resolve()
        root = Path(command(["git", "-c", f"safe.directory={cwd}", "rev-parse", "--show-toplevel"], cwd, env)
                    .decode().strip()).resolve()
        dependencies, sources = tracked_inputs(root, env)
        cargo_home, environment = environment_identity(root, args.profile, env)
        keys = cache_keys(args.profile, dependencies, sources, environment, cargo_home)
        output = "".join(f"{key}={value}\n" for key, value in keys.items())
        if args.github_output:
            with args.github_output.open("a", encoding="utf-8") as stream:
                stream.write(output)
        print(output, end="")
    except (OSError, ValueError, KeyError) as exc:
        print(f"Native cache identity unavailable: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
