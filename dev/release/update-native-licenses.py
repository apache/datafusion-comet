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

"""Refresh the checked-in native license bundle; --check needs only Python."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
DIRECTORY = Path("dev/licenses")
CONFIG = Path("dev/release/native-licenses.toml")
RECEIPT = DIRECTORY / "native-licenses.json"
VERSION = "0.9.3"
OUTPUTS = ("native-dependencies.txt", "native-NOTICE.txt")

# Both published packages and their publishing commits omit a license/copyright
# file. Preserve the MIT declaration and identify the missing upstream notice;
# do not turn Cargo authors into an invented copyright statement. A new version
# or another missing permissive license must be reviewed, not silently accepted.
UNATTRIBUTED_MIT = {("typemap-ors", "1.0.0"), ("unsafe-any-ors", "1.0.0")}


def digest(data):
    return hashlib.sha256(data).hexdigest()


def inputs(root=ROOT):
    paths = {Path("native/Cargo.lock"), CONFIG, Path(__file__).resolve().relative_to(ROOT)}
    for directory in ("native", "contrib"):
        for path in (root / directory).rglob("Cargo.toml"):
            relative = path.relative_to(root)
            if "target" not in relative.parts:
                paths.add(relative)
    return {str(p): digest((root / p).read_bytes()) for p in sorted(paths)}


def check(root=ROOT):
    path = root / RECEIPT
    if not path.exists():
        raise ValueError("native license receipt is missing; run dev/release/update-native-licenses.py")
    receipt = json.loads(path.read_text())
    if receipt["inputs"] != inputs(root):
        raise ValueError("native license inputs changed; regenerate and review the license bundle")
    for name in OUTPUTS:
        path = root / DIRECTORY / name
        if not path.exists() or digest(path.read_bytes()) != receipt["outputs"][name]:
            raise ValueError(f"native license bundle changed or is missing: {name}; regenerate it")
    return receipt


def package_name(package):
    return f"{package['name']} {package['version']}"


def license_bundle(data):
    # cargo-about returns every distinct upstream text, including multiple BSD
    # notices in a -sys crate. Deduplicate identical texts, not just SPDX IDs:
    # different copyright owners under the same license must all survive.
    groups = {}
    for license in data["licenses"]:
        text = license["text"].replace("\r\n", "\n").strip()
        users = [u["crate"] for u in license["used_by"]]
        missing_notice = False
        if license.get("source_path") is None and license["id"] != "Apache-2.0":
            names = {(p["name"], p["version"]) for p in users}
            if license["id"] != "MIT" or not names <= UNATTRIBUTED_MIT:
                raise ValueError(f"review missing upstream {license['id']} text for {sorted(names)}")
            # This is a canonical permission text, not an upstream attribution.
            # Remove its unfilled example copyright line, leaving the grant and
            # conditions intact, and state the limitation next to the packages.
            text = "\n".join(line for line in text.splitlines()
                             if not (line.startswith("Copyright") and "<" in line))
            missing_notice = True
        key = digest(text.encode())
        group = groups.setdefault(key, {"text": text, "ids": set(), "users": {},
                                        "missing_notice": missing_notice})
        group["ids"].add(license["id"])
        for package in users:
            group["users"][package_name(package)] = package
    lines = [
        "THIRD-PARTY LICENSES FOR COMET'S NATIVE LIBRARIES",
        "",
        f"Generated with cargo-about {VERSION} from native/Cargo.lock.",
        "Includes transitive normal dependencies of datafusion-comet's default",
        "features for Linux and macOS, x86_64 and aarch64. Build-only and test-only",
        "dependencies are excluded. See the contributor release guide to refresh.",
        "All upstream license texts and copyright notices below retain their terms.",
        "",
    ]
    for group in sorted(groups.values(), key=lambda g: (sorted(g["ids"]), g["text"])):
        lines += ["=" * 80, "Licenses: " + ", ".join(sorted(group["ids"])), "Used by:"]
        lines += ["  " + name for name in sorted(group["users"])]
        if group["missing_notice"]:
            lines += ["", "Upstream declares MIT but supplies no copyright/license file.",
                      "The canonical MIT permission text follows; no copyright holder",
                      "has been inferred. Package metadata identifies these authors:"]
            for name, package in sorted(group["users"].items()):
                lines += [f"  {name}: {', '.join(package['authors'])}",
                          f"  Repository: {package['repository']}"]
        lines += ["", group["text"], ""]
    return "\n".join(lines).rstrip() + "\n"


def notice_bundle(data):
    notices = {}
    for entry in data["crates"]:
        package = entry["package"]
        if not package.get("source"):
            continue  # Comet's own NOTICE is assembled separately.
        directory = Path(package["manifest_path"]).parent
        directories = [directory]
        if package["source"].startswith("git+"):
            # Git workspace members can inherit their repository's NOTICE.
            parent = directory
            while parent != parent.parent:
                if (parent / ".git").exists():
                    break
                parent = parent.parent
                directories.append(parent)
            if not (parent / ".git").exists():
                raise ValueError(f"cannot locate Git source root for {package_name(package)}")
        for directory in directories:
            for path in sorted(directory.glob("NOTICE*")):
                if not path.is_file():
                    continue
                text = path.read_text().replace("\r\n", "\n").strip()
                notice = notices.setdefault(digest(text.encode()), {"text": text, "users": set()})
                notice["users"].add(package_name(package))
    lines = ["", "Notices from dependencies of Comet's native libraries", ""]
    for notice in sorted(notices.values(), key=lambda n: (sorted(n["users"]), n["text"])):
        lines += ["-" * 80, "Used by: " + ", ".join(sorted(notice["users"])), "", notice["text"], ""]
    return "\n".join(lines).rstrip() + "\n"


def generate(root=ROOT):
    version = subprocess.check_output(["cargo-about", "--version"], text=True).strip()
    if version != f"cargo-about {VERSION}":
        raise ValueError(f"expected cargo-about {VERSION}, found {version}")
    before = inputs(root)
    command = ["cargo-about", "generate", "--manifest-path", "native/core/Cargo.toml",
               "--config", str(CONFIG), "--locked", "--fail", "--format", "json"]
    data = json.loads(subprocess.check_output(command, cwd=root, text=True))
    texts = dict(zip(OUTPUTS, (license_bundle(data), notice_bundle(data))))
    if before != inputs(root):
        raise ValueError("native license inputs changed while collecting licenses")
    directory = root / DIRECTORY
    directory.mkdir(exist_ok=True)
    for name, text in texts.items():
        (directory / name).write_text(text)
    receipt = {"cargo_about": VERSION, "configuration": "default features; targets in native-licenses.toml",
               "inputs": before, "outputs": {n: digest(t.encode()) for n, t in texts.items()},
               "packages": len(data["crates"])}
    (root / RECEIPT).write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="check freshness without Cargo, cargo-about or network")
    args = parser.parse_args()
    try:
        receipt = check() if args.check else generate()
    except (ValueError, OSError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"{error}\n")
    print(f"Native licenses current: {receipt['packages']} packages")


if __name__ == "__main__":
    main()
