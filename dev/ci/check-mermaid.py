# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Guards the two halves of the site's ```mermaid pipeline, both of them silent.

The site draws its diagrams at build time -- `mermaid_output_format = 'svg'` in
docs/source/conf.py, because the ASF Content-Security-Policy blocks the
client-side renderer (issue #6020). When mmdc is missing, cannot launch Chrome,
or chokes on a diagram, sphinxcontrib-mermaid logs a warning and drops that
diagram. The build stays green, the deploy goes ahead, and the only symptom is
a published page with a diagram-shaped hole in it, which nobody sees until they
open the page.

That is not hypothetical: it is how all three of the site's diagrams came to be
missing between #6021, which introduced build-time rendering, and the change
that added this check. See issue #6062.

Two modes, because there are two ways to lose a diagram:

    python3 dev/ci/check-mermaid.py
        Render every fence under docs/source/ with mmdc, using the arguments
        docs/source/conf.py gives the build. Catches a diagram mmdc rejects and
        an mmdc that cannot run on this runner at all. Cheap enough for
        preflight, so it fails on the pull request rather than after the merge.
        Needs mmdc on PATH (`npm install -g @mermaid-js/mermaid-cli`).

    python3 dev/ci/check-mermaid.py --built docs/build/html
        Assert the built site actually carries an SVG for every fence, and that
        each one is referenced by a page. Catches the other half: a diagram that
        renders fine but never reaches the HTML. Run before publishing.

The pinned mermaid-cli version lives here too, and `--cli-spec` prints it, so
that preflight and the docs deploy install the same one from one place. Two
workflows pinning it separately is the drift that lets the pull-request check
pass while the deploy drops a diagram.

Run from the repository root.
"""

import argparse
import importlib.util
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DOCS_SOURCE = REPO_ROOT / "docs" / "source"
CONF_PY = DOCS_SOURCE / "conf.py"

# The one place the mermaid-cli version is pinned; both workflows install
# `--cli-spec` rather than repeating it. See the module docstring.
MERMAID_CLI_PACKAGE = "@mermaid-js/mermaid-cli"
MERMAID_CLI_VERSION = "11.17.0"

# An opening ```mermaid fence through to the closing fence at the same indent.
# Matches what myst_fence_as_directive hands to sphinxcontrib-mermaid.
FENCE = re.compile(
    r"^(?P<indent>[ \t]*)```mermaid[ \t]*$(?P<body>.*?)^(?P=indent)```[ \t]*$", re.M | re.S
)

# sphinxcontrib-mermaid names each output after a hash of the diagram source,
# so identical diagrams collapse onto one file. The check below compares
# distinct sources against distinct files for that reason.
BUILT_SVG = "mermaid-*.svg"


def load_conf():
    """Import docs/source/conf.py for the mmdc arguments the real build uses.

    Reading them rather than repeating them is the point: a check that renders
    with different flags than the build can pass while the build drops the
    diagram. conf.py is plain assignments plus function definitions, and its one
    module-level call is exception-safe, so importing it is side-effect free.
    """
    spec = importlib.util.spec_from_file_location("comet_docs_conf", CONF_PY)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def source_files():
    return sorted(DOCS_SOURCE.rglob("*.md")) + sorted(DOCS_SOURCE.rglob("*.rst"))


def fences(path):
    """Yield (line number, diagram source) for each mermaid fence in `path`."""
    text = path.read_text(encoding="utf-8")
    for match in FENCE.finditer(text):
        yield text.count("\n", 0, match.start()) + 1, match.group("body")


def all_fences():
    """Yield (path, line number, diagram source) across the whole doc source."""
    for path in source_files():
        for line, code in fences(path):
            yield path, line, code


def render(code, params, workdir):
    """Render one diagram; return None on success or the failure text."""
    source = workdir / "diagram.mmd"
    output = workdir / "diagram.svg"
    source.write_text(code, encoding="utf-8")
    if output.exists():
        output.unlink()

    result = subprocess.run(
        ["mmdc", *params, "-i", str(source), "-o", str(output)],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return annotate(
            (result.stderr or result.stdout or "").strip() or f"mmdc exited {result.returncode}"
        )
    # mmdc has been seen to exit 0 having written nothing, which the build would
    # turn into an <object> pointing at an empty file.
    if not output.exists() or output.stat().st_size == 0:
        return "mmdc exited 0 but wrote no SVG"
    return None


def annotate(error):
    """Append the fix for failures whose message does not suggest one."""
    if "Could not find" in error and "cache path" in error:
        return (
            f"{error}\n"
            f"No browser is installed for mmdc to drive. `npm install -g {MERMAID_CLI_PACKAGE}` "
            f"is supposed to fetch one through puppeteer's postinstall, but that script catches "
            f"its own download failures and exits 0, so the install step goes green without a "
            f"browser. Run `npx puppeteer browsers install chrome-headless-shell` from the "
            f"mermaid-cli install directory."
        )
    return error


def check_renders():
    if shutil.which("mmdc") is None:
        print(
            "::error::mmdc is not on PATH. The docs build needs it to draw the "
            "```mermaid fences; without it the build still succeeds and silently "
            "publishes those pages with the diagrams missing. Install it with "
            f"`npm install -g {MERMAID_CLI_PACKAGE}` (see docs/README.md)."
        )
        return False

    # CI installs `--cli-spec`, so a mismatch only happens locally; say so
    # rather than failing, since the local build is not what publishes.
    installed = subprocess.run(
        ["mmdc", "--version"], capture_output=True, text=True, check=False
    ).stdout.strip()
    if installed and installed != MERMAID_CLI_VERSION:
        print(
            f"mermaid: note, mmdc {installed} is on PATH but CI renders with "
            f"{MERMAID_CLI_VERSION} (pinned in {Path(__file__).name})"
        )

    conf = load_conf()
    if conf.mermaid_output_format == "raw":
        print(
            "::error::docs/source/conf.py sets mermaid_output_format = 'raw', which "
            "renders in the reader's browser. The ASF Content-Security-Policy blocks "
            "that script, so no diagram reaches a reader (issue #6020)."
        )
        return False
    params = list(conf.mermaid_params)

    failed = 0
    checked = 0
    with tempfile.TemporaryDirectory() as tmp:
        workdir = Path(tmp)
        for path, line, code in all_fences():
            checked += 1
            error = render(code, params, workdir)
            if error:
                failed += 1
                relative = path.relative_to(REPO_ROOT)
                print(f"::error file={relative},line={line}::mmdc cannot render this diagram")
                print(f"mermaid: {relative}:{line} does not render:\n{error}\n")

    if failed:
        print(f"mermaid: {failed} of {checked} diagrams failed to render")
        return False
    if not checked:
        print("mermaid: no ```mermaid fences found under docs/source (FENCE no longer matches?)")
        return False
    print(f"mermaid: {checked} diagrams render")
    return True


def check_built(built):
    """The built site carries a non-empty SVG per distinct fence, each referenced."""
    failures = []
    if not built.is_dir():
        print(f"::error::{built} is not a directory; nothing was built there")
        return False

    expected = {code for _, _, code in all_fences()}
    svgs = sorted(built.rglob(BUILT_SVG))
    empty = [svg for svg in svgs if svg.stat().st_size == 0]
    if empty:
        failures.append(
            f"{len(empty)} rendered diagram(s) are empty files: "
            f"{', '.join(str(svg.relative_to(built)) for svg in empty)}"
        )
    if len(svgs) < len(expected):
        failures.append(
            f"docs/source has {len(expected)} distinct ```mermaid fences but the "
            f"build produced {len(svgs)} SVG(s) under {built}. sphinxcontrib-mermaid "
            f"downgrades a render failure to a warning and drops the diagram, so the "
            f"missing ones would publish as a hole in the page. Check the build log "
            f"for mermaid warnings"
        )

    # A file nothing points at is as invisible as a missing one.
    html = "\n".join(page.read_text(encoding="utf-8", errors="ignore") for page in built.rglob("*.html"))
    unreferenced = [svg.name for svg in svgs if svg.name not in html]
    if unreferenced:
        failures.append(
            f"rendered but referenced by no page: {', '.join(unreferenced)}"
        )

    for failure in failures:
        print(f"::error::mermaid: {failure}")
    if failures:
        return False
    print(f"mermaid: {len(svgs)} diagrams rendered into {built} and referenced")
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--built",
        type=Path,
        help="check a built HTML tree (e.g. docs/build/html) instead of rendering the fences",
    )
    parser.add_argument(
        "--cli-spec",
        action="store_true",
        help="print the pinned mermaid-cli npm spec and exit, for `npm install -g`",
    )
    args = parser.parse_args()
    if args.cli_spec:
        print(f"{MERMAID_CLI_PACKAGE}@{MERMAID_CLI_VERSION}")
        return 0
    ok = check_built(args.built) if args.built else check_renders()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
