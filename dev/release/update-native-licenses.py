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
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
DIRECTORY = Path("dev/licenses")
CONFIG = Path("dev/release/native-licenses.toml")
REVIEW = Path("dev/release/native-license-review.toml")
RECEIPT = DIRECTORY / "native-licenses.json"
VERSION = "0.9.3"
OUTPUTS = ("native-dependencies.txt", "native-dependencies-linux.txt",
           "native-dependencies-darwin.txt", "native-NOTICE.txt", "native-inventory.json")
TARGETS = {
    "linux": ("x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu"),
    "darwin": ("x86_64-apple-darwin", "aarch64-apple-darwin"),
}

# Both published packages and their publishing commits omit a license/copyright
# file. Preserve the MIT declaration and identify the missing upstream notice;
# do not turn Cargo authors into an invented copyright statement. A new version
# or another missing permissive license must be reviewed, not silently accepted.
UNATTRIBUTED_MIT = {("typemap-ors", "1.0.0"), ("unsafe-any-ors", "1.0.0")}


def digest(data):
    return hashlib.sha256(data).hexdigest()


def inputs(root=ROOT):
    paths = {Path("native/Cargo.lock"), CONFIG, REVIEW, Path("Makefile"),
             Path("dev/release/build-release-comet.sh"),
             Path("dev/release/check-native-licenses.py"),
             Path(__file__).resolve().relative_to(ROOT)}
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


def clean(text):
    return "\n".join(line.rstrip() for line in text.replace("\r\n", "\n").strip().splitlines())


def consolidate_apache(text):
    """Remove only the common terms/appendix, preserving exceptions and notices."""
    appendix = re.compile(r"APPENDIX: How to apply the Apache License to your work\..*?"
                          r"limitations under the License\.", re.S | re.I)

    def keep_copyright(match):
        return "\n".join(line.strip() for line in match[0].splitlines()
                         if re.match(r"\s*Copyright\b", line, re.I)
                         and not re.search(r"[\[{<](yyyy|year|name|copyright)", line, re.I))

    text = appendix.sub(keep_copyright, text)
    text = re.sub(r"Apache License\s+Version 2\.0, January 2004.*?"
                  r"END OF TERMS AND CONDITIONS",
                  "[Apache-2.0 license terms: see META-INF/LICENSE.]", text,
                  flags=re.S | re.I)
    return re.sub(r"\n{3,}", "\n\n", clean(text))


def vendor_text(text, users, review):
    if (re.search(r"Apache License\s+Version 2\.0, January 2004.*?END OF TERMS AND CONDITIONS",
                  text, re.S | re.I) and digest(text.encode()) not in review["apache"]["texts"]):
        raise ValueError("review changed Apache text for embedded third-party exceptions: "
                         f"{digest(text.encode())}, {[package_name(p) for p in users]}")
    aws = next((p for p in users if p["name"] == "aws-lc-sys"), None)
    if aws:
        rule = review["aws_lc"]
        source = (Path(aws["manifest_path"]).parent / "LICENSE").read_bytes()
        if package_name(aws) != rule["package"] or digest(source) != rule["sha256"]:
            raise ValueError("review the updated AWS-LC vendor license")
        if text != source.decode().replace("\r\n", "\n").strip():
            raise ValueError("unexpected AWS-LC aggregate license input")
        start = "=" * 80 + "\nThird-Party Libraries (NOT compiled into libcrypto/libssl)"
        end = "=" * 80 + "\nFull License Texts"
        first, last = text.index(start), text.index(end)
        text = text[:first] + text[last:]
    return consolidate_apache(text)


def license_bundle(data):
    # cargo-about returns every distinct upstream text, including multiple BSD
    # notices in a -sys crate. Deduplicate identical texts, not just SPDX IDs:
    # different copyright owners under the same license must all survive.
    groups = {}
    for license in data["licenses"]:
        text = clean(license["text"])
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
        "Common materials for the default native features. The matching platform",
        "supplement is selected from the libcomet payloads actually in the jar.",
        "Apache-2.0 terms appear once in META-INF/LICENSE. Copyright notices and",
        "third-party exceptions below retain their original terms.",
        "Build/dev-only dependencies and reviewed host-only tooling are excluded.",
        "Attribution for reviewed code-generator output is included.",
        "See the contributor release guide and the separate audit inventory.",
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


def notice_bundle(data, review):
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
                sha = digest(text.encode())
                action = review["notices"].get(sha)
                if not action:
                    raise ValueError(f"review changed or new NOTICE for {package_name(package)}: {sha}")
                if action == "moka":
                    text = ("Moka\n\n- src/common/frequency_sketch.rs\n"
                            "- src/common/timer_wheel.rs\n\n"
                            "These files were ported from the Java Caffeine library and are not dual-licensed.")
                else:
                    text = re.sub(r"\nThis product includes software developed at\n"
                                  r"The Apache Software Foundation \(https?://www.apache.org/\)\.\n?",
                                  "\n", text)
                notices[sha] = re.sub(r"\n{3,}", "\n\n", clean(text))
    return "\n" + "\n\n".join(sorted(notices.values())) + "\n"


def runtime_packages(root, target):
    command = ["cargo", "tree", "--manifest-path", "native/core/Cargo.toml",
               "--package", "datafusion-comet", "--target", target,
               "--edges", "normal,no-proc-macro", "--prefix", "none", "--format", "{p}", "--locked"]
    text = subprocess.check_output(command, cwd=root, text=True)
    return {f"{name} {version}" for name, version in re.findall(r"^(\S+) v(\S+)", text, re.M)}


def select_packages(data, runtime, decisions):
    selected, roles = set(), {}
    for entry in data["crates"]:
        package = entry["package"]
        name = package_name(package)
        macro = any("proc-macro" in target["kind"] for target in package["targets"])
        if macro and name not in decisions:
            raise ValueError(f"review new or changed code generator: {name}")
        role = ("generated-code" if macro and decisions[name] == "generated-code"
                else "target" if name in runtime else "host-only")
        roles[name] = role
        if package.get("source") and role != "host-only":
            selected.add(name)
    return selected, roles


def selected_data(data, selected, review):
    licenses = []
    for item in data["licenses"]:
        users = [u for u in item["used_by"] if package_name(u["crate"]) in selected]
        if users:
            text = item["text"].replace("\r\n", "\n").strip()
            licenses.append(dict(item, used_by=users,
                                 text=vendor_text(text, [u["crate"] for u in users], review)))
    return {"licenses": licenses, "crates": [e for e in data["crates"]
                                              if package_name(e["package"]) in selected]}


def packaged_bundle(root, platforms):
    if not platforms:
        return "This artifact contains no Comet native library payload.\n"
    text = (root / DIRECTORY / "native-dependencies.txt").read_text()
    for platform in sorted(platforms):
        text += "\n" + (root / DIRECTORY / f"native-dependencies-{platform}.txt").read_text()
    return text


def generate(root=ROOT):
    import tomllib  # Regeneration only; package/check paths need no TOML parser.

    version = subprocess.check_output(["cargo-about", "--version"], text=True).strip()
    if version != f"cargo-about {VERSION}":
        raise ValueError(f"expected cargo-about {VERSION}, found {version}")
    before = inputs(root)
    review = tomllib.loads((root / REVIEW).read_text())
    decisions = {name: group["disposition"] for group in review["codegen"] for name in group["packages"]}
    if len(decisions) != sum(len(g["packages"]) for g in review["codegen"]):
        raise ValueError("duplicate code-generator review")
    if set(decisions.values()) - {"generated-code", "host-only"}:
        raise ValueError("invalid code-generator disposition")
    datasets, selected, audit_roles = {}, {}, {}
    for platform, targets in TARGETS.items():
        memberships = []
        for target in targets:
            print(f"Collecting native licenses for {target}", file=sys.stderr, flush=True)
            command = ["cargo-about", "generate", "--manifest-path", "native/core/Cargo.toml",
                       "--config", str(CONFIG), "--target", target, "--locked", "--fail", "--format", "json"]
            data = json.loads(subprocess.check_output(command, cwd=root, text=True))
            members, roles = select_packages(data, runtime_packages(root, target), decisions)
            memberships.append(members)
            audit_roles[target] = roles
            datasets[platform] = data
        if memberships[0] != memberships[1]:
            raise ValueError(f"review differing architecture licenses for {platform}; split its supplement")
        selected[platform] = memberships[0]
    common = selected["linux"] & selected["darwin"]
    common_linux = selected_data(datasets["linux"], common, review)
    common_darwin = selected_data(datasets["darwin"], common, review)
    if license_bundle(common_linux) != license_bundle(common_darwin):
        raise ValueError("review platform-dependent license terms in shared components")
    texts = {"native-dependencies.txt": license_bundle(common_linux)}
    for platform in TARGETS:
        supplement = license_bundle(selected_data(datasets[platform], selected[platform] - common, review))
        # The supplement's scope is explicit; its content is not a second inventory.
        texts[f"native-dependencies-{platform}.txt"] = (f"Platform supplement: {platform}\n\n"
                                                       + supplement[supplement.index("=" * 80):]
                                                       if "=" * 80 in supplement else f"Platform supplement: {platform}\n")
    notices = [notice_bundle(selected_data(datasets[p], selected[p], review), review) for p in TARGETS]
    if notices[0] != notices[1]:
        raise ValueError("review platform-dependent NOTICE; split it before packaging")
    texts["native-NOTICE.txt"] = notices[0]
    lock = tomllib.loads((root / "native/Cargo.lock").read_text())
    if len({package_name(p) for p in lock["package"]}) != len(lock["package"]):
        raise ValueError("review ambiguous package name/version from different sources")
    expressions = {package_name(e["package"]): e["package"]["license"]
                   for data in datasets.values() for e in data["crates"]}
    inventory = []
    for package in lock["package"]:
        name = package_name(package)
        inventory.append({"package": name, "source": package.get("source", "workspace"),
                          "license_metadata": expressions.get(name),
                          "roles": {t: roles[name] for t, roles in audit_roles.items() if name in roles}})
    texts["native-inventory.json"] = json.dumps({
        "scope": "Full lockfile audit inventory; not a list of bundled components or legal notices",
        "features": "core defaults (hdfs-opendal, delta)", "targets": TARGETS,
        "codegen_review": review["codegen"], "packages": sorted(inventory, key=lambda p: p["package"])},
        indent=2, sort_keys=True) + "\n"
    if before != inputs(root):
        raise ValueError("native license inputs changed while collecting licenses")
    directory = root / DIRECTORY
    directory.mkdir(exist_ok=True)
    for name, text in texts.items():
        (directory / name).write_text(text)
    receipt = {"cargo_about": VERSION, "configuration": "default features; common plus payload-selected platform supplement",
               "inputs": before, "outputs": {n: digest(t.encode()) for n, t in texts.items()},
               "packages": {p: len(s) for p, s in selected.items()}, "common_packages": len(common)}
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
