#!/usr/bin/env python3
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

"""Prepare or restore one Linux libcomet.so cache entry without executing it.

The workflow supplies the complete native-input key and controls which runs may
save caches. A checksum detects incomplete or corrupted entries; it is not a
substitute for restricting cache writers to trusted builds. Restore treats bad
cache contents as a miss, removes any previous destination, and publishes
hit=true only after installing all verified bytes. Destination I/O failures are
fatal so permission or disk failures cannot masquerade as cache misses.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import stat
import sys
import tempfile


def open_regular_file(path):
    """Return an owned binary stream for a Linux regular, non-symlink file.

    The caller closes the stream. O_NOFOLLOW rejects symlinks, and O_NONBLOCK
    prevents a malformed cache FIFO from hanging before fstat can reject it.
    Missing/unreadable paths raise OSError; other file types raise ValueError.
    """
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError(f"Not a regular file: {path}")
        return os.fdopen(descriptor, "rb")
    except BaseException:
        os.close(descriptor)
        raise


def copy_library(source, destination, expected_sha256=None):
    """Atomically copy an open binary stream to destination and return its SHA256.

    Reads start at the stream's current offset; ownership stays with the caller.
    If supplied, expected_sha256 must match the copied bytes before replacement.
    Bad source reads/checksums raise ValueError; destination I/O errors propagate.
    Temporary files are always removed and an incomplete copy is never installed.
    """
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as output:
            temporary = Path(output.name)
            digest = hashlib.sha256()
            while True:
                try:
                    chunk = source.read(1024 * 1024)
                except OSError as error:
                    raise ValueError("Cannot read native library") from error
                if not chunk:
                    break
                digest.update(chunk)
                output.write(chunk)
        checksum = digest.hexdigest()
        if expected_sha256 is not None and checksum != expected_sha256:
            raise ValueError("Native library checksum mismatch")
        temporary.replace(destination)
        return checksum
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def prepare(key, cache_dir, library):
    """Write libcomet.so and its key/checksum manifest into cache_dir.

    library is a complete build output; all paths are pathlib Paths. Both files
    are installed atomically, with the manifest last. A prior manifest is removed
    first so an interrupted refresh cannot advertise an old successful entry.
    Returns nothing; invalid source files and all write failures propagate.
    """
    cache_dir.mkdir(parents=True, exist_ok=True)
    manifest = cache_dir / "manifest.json"
    manifest.unlink(missing_ok=True)
    with open_regular_file(library) as source:
        checksum = copy_library(source, cache_dir / "libcomet.so")
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=cache_dir, delete=False) as output:
            temporary = Path(output.name)
            json.dump({"key": key, "sha256": checksum}, output, sort_keys=True)
            output.write("\n")
        temporary.replace(manifest)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def restore(key, cache_dir, library):
    """Install a matching cached library and return True, or return False on a miss.

    key is the exact native-input fingerprint; paths are pathlib Paths. Removes
    the previous library before checking the cache, including dangling symlinks.
    Missing, unreadable, malformed, symlinked or mismatched cache data is a miss.
    Directory creation, destination removal and write failures propagate so the
    workflow fails instead of using stale output. No cached code is executed.
    """
    library.unlink(missing_ok=True)
    try:
        with open_regular_file(cache_dir / "manifest.json") as source:
            manifest = json.load(source)
        if not isinstance(manifest, dict) or manifest.get("key") != key:
            return False
        checksum = manifest.get("sha256")
        if not isinstance(checksum, str) or len(checksum) != 64:
            return False
        source = open_regular_file(cache_dir / "libcomet.so")
    except (OSError, ValueError):
        return False
    with source:
        try:
            copy_library(source, library, checksum)
        except ValueError:
            return False
    return True


def main(argv):
    """Run prepare/restore using CLI arguments and return a process exit status.

    Restore appends hit=true/false to --github-output and prints the same value.
    Invalid cache data is a successful miss; operational failures return 1.
    Output-file write failures also fail the command rather than report a hit.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("prepare", "restore"))
    parser.add_argument("--key", required=True)
    parser.add_argument("--cache-dir", required=True, type=Path)
    parser.add_argument("--library", required=True, type=Path)
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.operation == "prepare":
            prepare(args.key, args.cache_dir, args.library)
        else:
            hit = restore(args.key, args.cache_dir, args.library)
            result = f"hit={str(hit).lower()}\n"
            if args.github_output:
                with args.github_output.open("a", encoding="utf-8") as output:
                    output.write(result)
            print(result, end="")
    except (OSError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
