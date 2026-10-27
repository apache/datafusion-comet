---
name: review-comet-docs
description: Use when reviewing the documentation in a DataFusion Comet pull request or branch, or when auditing an existing page or directory under docs/source or README.md. Checks wording against the documentation style guide and checks that config keys, operator names, links, images and generated content match the code. Advisory only, it never edits files or posts comments. Load alongside review-comet-pr when a PR changes docs.
argument-hint: <pr-number | branch | docs-path>
---

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

Review Comet docs: $ARGUMENTS

Pick the mode from the argument:

- **PR mode**: a number. Review the docs a pull request changes.
- **Audit mode**: a file or directory that exists in the working tree. Review it as it is now.
- **Branch mode**: a branch, tag, or commit that `git rev-parse --verify` accepts. Review what it
  changes compared with `main`, for example before opening a PR. Otherwise, ask which one is meant.

In PR mode, **REQUIRED BACKGROUND:** use `review-comet-pr` for its steps 2 to 4 (PR metadata,
existing comments, and the diff) and its CI check. This skill only covers documentation.

This skill gives advice to a human. Never edit files, commit, push, or post comments.

**Shell notes.** Run every command from the repository root. Each tool call may start a new shell,
so save the variables you set in a state file named for this run, and load it in later calls:

```bash
STATE="$(git rev-parse --git-dir)/review-comet-docs-<pr-or-name>.env"
{ printf 'R=%q\n' "$R"; printf 'HEAD_SHA=%q\n' "$HEAD_SHA"; printf 'BASE=%q\n' "$BASE";
  printf 'TARGET=%q\n' "$TARGET"; printf 'ADDED=%q\n' "$ADDED"; } > "$STATE"
. "$STATE"
```

In zsh, `$HEAD_SHA:spark/...` is read as a history modifier, so always write `"${HEAD_SHA}:<path>"`.
Stop and report the review as incomplete if any command in steps 1 to 4 fails.

## 1. Select the Revisions

Find the remote that points at `apache/datafusion-comet`, because its name differs between clones:

```bash
R=$(git remote -v | awk '/apache\/datafusion-comet(\.git)? \(fetch\)/ {print $1; exit}')
```

If `R` is empty, use `https://github.com/apache/datafusion-comet.git` in its place.

**PR mode.** Read the PR's base branch and head commit, then fetch both without touching the working
tree. The leading `+` lets the fetch replace a ref after a force push.

```bash
gh pr view <pr> --repo apache/datafusion-comet --json baseRefName,headRefOid
git fetch "$R" "+refs/heads/<base>:refs/review/base-<pr>" "+pull/<pr>/head:refs/review/pr-<pr>"
TARGET=refs/review/base-<pr>; HEAD_SHA=$(git rev-parse refs/review/pr-<pr>)
```

Check that `HEAD_SHA` equals `headRefOid`. If it does not, the PR changed while you fetched it: run
both commands again, at most twice. From here on, read everything at `HEAD_SHA`, not at the ref,
which a later fetch can move.

**Branch mode.** Fetch `main` the same way. `TARGET=refs/review/main` and
`HEAD_SHA=$(git rev-parse --verify <branch>)`:

```bash
git fetch "$R" "+refs/heads/main:refs/review/main"
```

**Both modes.** Find the merge base. It fails in a shallow clone, so deepen the history and try again,
or report the comparison as incomplete:

```bash
BASE=$(git merge-base "$TARGET" "$HEAD_SHA") || git fetch --deepen=1000 "$R"
```

If `git rev-list --count "$BASE..$TARGET"` is not zero, the target branch has moved since the change
branched. Check the claims against `$TARGET` too, and report what has changed there.

**Audit mode.** There is no base. List the files, including new files that are not committed yet:

```bash
git ls-files --cached --others --exclude-standard -- <path>
```

## 2. Read the Style Guide

Read `docs/source/contributor-guide/style_guide.md` in full on every run, from the revision under
review: `git show "${HEAD_SHA}:docs/source/contributor-guide/style_guide.md"` in PR and branch mode,
or the working tree in audit mode. If it does not exist there, stop and say so.

The guide is the rulebook, and its first section says what it covers. Cite its rule IDs (`T3`, `N1`,
`P16`, and so on), and do not restate or reinterpret its rules here. If the guide and this skill
disagree, the guide wins.

## 3. Select the Files

In PR and branch mode, list the changed files from Git, which is complete even for large PRs:

```bash
git -c core.quotePath=false diff --name-only "$BASE" "$HEAD_SHA"
```

From that list, `<paths>` below means:

- `.md` and `.rst` files under `docs/source`, and `README.md`;
- files that change `.doc(...)` strings in `CometConf.scala` or in contrib config classes;
- files that change doc comments (Scaladoc, Javadoc, and Rust `///` comments);
- `AGENTS.md` and `.ai/skills/**/SKILL.md`.

For doc comments, `AGENTS.md`, and skills, check the vocabulary and accuracy rules the guide names
for them, not the page conventions. Check the changed lines and the paragraphs around them, not the
whole page. Report a problem in unchanged text only when the change makes it wrong, or list it
separately as already on the page. Also read the code diff for the behavior the changed docs
describe, because docs are usually only as accurate as the code change beside them.

In every mode:

- Skip the content the guide's **Generated Content** section lists as generated or as records.
  Review the hand-written text around the generated markers. Text between the markers that differs
  from what `GenerateDocs` produces is a hand edit, and a finding (`P5`).
- Do the judgment checks in step 5 for at most 10 pages per run. List any pages you skipped so the
  user can run the skill again on them.

## 4. Mechanical Checks

These checks produce candidates, not findings. Read each hit in context before reporting it. For the
wording checks, ignore hits inside fenced code blocks, inline code, quoted plan output or logs, and
text that quotes a phrase in order to discuss it (`N6`), such as the style guide's own examples. The
accuracy checks still apply to code font: config keys, operator names, and code examples are usually
written that way. For each check, record what it checked, the number of hits, and how many of them
are findings.

The commands use `git grep -E` to find candidate lines and `perl` to match them. They do not need a
Git built with PCRE, and they avoid two traps: the system `grep` on macOS has no `-P`, and `\b`
misses Markdown italics such as `_native execution_`, because `_` counts as a word character.

**Lines to check.** In PR and branch mode, list the added lines with their file and line number, and
run the line checks below on this list only. In audit mode, run them on the files with
`git grep -n --untracked`.

```bash
ADDED=$(mktemp "$(git rev-parse --git-dir)/review-added.XXXXXX")
git -c core.quotePath=false diff --no-ext-diff --src-prefix=a/ --dst-prefix=b/ -U0 \
    "$BASE" "$HEAD_SHA" -- <paths> \
  | awk '/^--- /{h=1; next}
         h && /^\+\+\+ b\//{f=substr($0,7); sub(/\t$/,"",f); h=0; next}
         /^@@/{h=0; split($3,a,/[+,]/); n=a[2]; next}
         /^\+/{print f":"n": "substr($0,2); n++}' > "$ADDED"
```

**"Native" and "vectorized".** Check each hit against `N1`-`N6` and `T10`. Which page the hit is on
matters, because `N3` allows more in the contributor guide:

```bash
P='(?:(?<![A-Za-z0-9])native(?:ly)?(?![A-Za-z0-9])|vectori[sz]ed)'
perl -ne "print if /^.+?:\d+: .*$P/i" "$ADDED"
git grep -n -i --untracked -E 'native|vectori[sz]ed' -- <files> | perl -ne "print if /^.+?:\d+:.*$P/i"  # audit
```

**Config keys.** Every `spark.comet.*` key a page names must exist (`P13`). List the keys the text
names, then the keys the code defines at the revision under review. `<rev>` is `$HEAD_SHA`, or
nothing in audit mode. The second command reads definitions that span two lines, expands
`createExecEnabledConfig`, and leaves out deprecated alternative names and unexpanded templates:

```bash
perl -ne 'print "$1\n" while /(spark\.comet\.[\w.<>]*[\w>])/g' "$ADDED" | sort -u
git grep -h -A1 -E '(conf|ConfigBuilder|createExecEnabledConfig)\(' <rev> -- spark/src/main contrib \
  | perl -0777 -ne '
      while (/\b(?:conf|ConfigBuilder)\(\s*s?"([^"]+)"/g) {
        ($k = $1) =~ s/\$\{?COMET_EXEC_CONFIG_PREFIX\}?/spark.comet.exec/;
        $k =~ s/\$\{?COMET_PREFIX\}?/spark.comet/;
        print "$k\n" if $k =~ /^spark\.comet\./ && $k !~ /\$/;
      }
      while (/\bcreateExecEnabledConfig\(\s*"([^"]+)"/g) { print "spark.comet.exec.$1.enabled\n" }' \
  | sort -u
```

If the second command prints nothing, it is broken: stop and report that, rather than flagging every
key. Some keys are built from a name at run time:

- `spark.comet.expression.<Name>.enabled` and `.allowIncompatible`: `<Name>` is the Spark expression
  class name, as in the `QueryPlanSerde` serde maps, not a Comet or protobuf name. Some expressions
  use their own opt-in key instead, set by `nativeOptInConfigKeyOverride`, such as
  `spark.comet.caseConversion.enabled`.
- `spark.comet.operator.<Name>.allowIncompatible`: `<Name>` is the Spark operator class name, such as
  `ProjectExec`.
- `spark.comet.datafusion.<key>`: not a Comet config. Comet passes it to DataFusion only when
  `spark.comet.exec.respectDataFusionConfigs=true`, which is off by default. Check the key against
  DataFusion.

Before reporting a missing key, search the code for it, because a few keys are defined in other
ways, such as `spark.comet.version` in `Plugins.scala`. A deprecated key that a page still uses is a
finding (`P16`).

**Operator names.** Check operator names against `O1`, class names against `O2`, and expressions
against `O4`. List the concrete operator classes and the classes that override their printed name:

```bash
git grep -h -E 'case class (Comet|Iceberg)' <rev> -- spark/src/main contrib \
  | perl -ne 'print "$1\n" if /case class ((?:Comet|Iceberg)[A-Za-z]+Exec)\b/' | sort -u
git grep -n -E 'override (def|val|lazy val) (nodeName|simpleString)' <rev> -- spark/src/main contrib
```

`spark/src/main` includes the per-Spark-version source directories. If the first command prints
nothing, it is broken: stop and report that. A class's `nodeName` is its name without `Exec`, unless
it overrides `nodeName`. Compare each name in the text with these, as `O1` describes, not with a rule
about suffixes. When the page is an operator reference, also report concrete operators that it
leaves out. Leave out planning markers such as `CometScanExec`, which never appear in a final plan,
and contrib operators, which belong on their contrib page.

**Docs build.** Links, anchors, images, toctrees, and page structure, in Markdown and
reStructuredText, are checked by building the docs the way the published site is built. This
catches what a reading of the changed lines cannot, such as a link on an unchanged page that a
renamed heading breaks, or a page that no toctree includes. Install the docs requirements once, in a
virtual environment, with `pip install -r docs/requirements.txt`. Then build the base and the head
and compare their warnings. The function copies the expression templates into the per-version pages,
as `docs/build.sh` does, so links to those pages resolve:

```bash
build() {  # $1 = revision, $2 = output directory
  mkdir -p "$2/src" && git archive "$1" docs/source | tar -x -C "$2/src"
  T="$2/src/docs/source/user-guide/latest/compatibility/expressions"
  for p in $(git show "$1:docs/build.sh" | sed -n 's/^SPARK_PROFILES=(\(.*\))/\1/p'); do
    mkdir -p "$T/$p" && cp "$T/_category_template"/*.md "$T/$p/"
  done
  rm -rf "$T/_category_template"
  sphinx-build -q -b html -n -D mermaid_output_format=raw "$2/src/docs/source" "$2/html" 2>&1 \
    | sed -e "s#$2/src/##" -e 's/:[0-9]*: /: /' | sort > "$2/warnings.txt"
}
WORK=$(mktemp -d)
build "$BASE" "$WORK/base" && build "$HEAD_SHA" "$WORK/head"
comm -13 "$WORK/base/warnings.txt" "$WORK/head/warnings.txt"
```

Every warning that only the head build has is a finding: a broken link, anchor, or image, a page
missing from a toctree (`P6`), or a heading problem (`P2`). In audit mode, build the working tree's
`docs/source` the same way and report the warnings for the audited pages. If the docs requirements
cannot be installed, say that the build was not checked, and check that each changed link's target
file exists.

Sphinx does not count H1s. Check that each new or changed Markdown page has one H1 with the parser
the docs build uses, which knows where code blocks start and end:

```bash
git show "${HEAD_SHA}:<file>" | python -c 'import sys; from markdown_it import MarkdownIt
for t in MarkdownIt("commonmark").parse(sys.stdin.read()):
    print(t.map[0] + 1, t.tag) if t.type == "heading_open" else None'
```

**Other structure checks.** A new file has the ASF header (`P1`). A link to Comet's published docs by
URL breaks `P4` on pages under `docs/source`, but not in `README.md`. List the pages the change
renames or deletes; each one needs an entry in `redirects` in `docs/source/conf.py` (`P17`):

```bash
git diff --name-status --diff-filter=DR "$BASE" "$HEAD_SHA" -- docs/source
```

**External links.** For external links on the lines under review (at most 20 in audit mode), check
the status with `curl -s -o /dev/null -w '%{http_code}' -I -L --max-time 10 <url>`. Report a 404 as
a finding. If the request fails or the network is unavailable, say the link was not checked.

**Version claims and markers.** Find version claims to check against `P8`, and `TODO` or `XXX`
markers, which break `P7`:

```bash
V='(?:(?:Spark|Iceberg) [0-9]\.[0-9x]+\+?|\b(?:TODO|XXX)\b)'
perl -ne "print if /^.+?:\d+: .*$V/" "$ADDED"
git grep -n --untracked -E 'Spark [0-9]|Iceberg [0-9]|TODO|XXX' -- <files> | perl -ne "print if /$V/"  # audit
```

CI runs prettier and the Apache RAT license check on PRs, so do not repeat them. If CI has not run
them yet, for example on a draft, say they were not checked. Mention them otherwise only when CI has
failed on them.

## 5. Judgment Checks

- **Is it true?** For every sentence that names a class, rule, config, default, or file, read the
  code it describes at `HEAD_SHA`. A sentence can follow every wording rule and
  still be wrong: a scan that no longer exists, a config default that has changed, or an operator
  described as Rust-implemented that runs on the JVM. Check numbers, versions, and names first, and
  check that a condition in the text is the whole condition in the code. Pages that describe
  architecture or test infrastructure go stale most often. A false or outdated claim breaks `P16`.
  This is the most valuable check in this skill.
- **Does it agree with the rest of the docs?** A new sentence can contradict an older one on the
  same page, or a count such as "five configs" can stop matching the list below it (`P16`). When a
  change rewrites the description of a config or class, search `docs/source` for the same name and
  check the other descriptions.
- **Which axis?** When you suggest a replacement for vague "native", choose the term the code
  supports: _in the Comet pipeline_ (`T1`), _Rust-implemented_ (`T3`), or _JVM-side_ (`T4`). Never
  suggest _Rust-implemented_ without checking the code.
- **Page conventions.** Check `P3`, `P7`, `P8`, `P9`, `P10`, `P11`, `P12`, `P14`, and `P18` by
  reading the page. For plan output (`P12`, `P16`), check that the operator names exist, that the
  tree shape is one Spark could print, and that messages in it match the strings in the code. Do not
  ask for new plan output to be invented.
- **Same-change updates.** If the change adds an operator or expression, check the pages `P15`
  lists. If it adds a config, check that its `.doc(...)` string follows the guide, since
  `GenerateDocs` publishes it, and that any page that documents the config by hand mentions it.

## 6. Severity

- **Must-fix.** The page is wrong or broken: a false or outdated claim (`P16`), a config key, class,
  or file that does not exist (`P13`), a broken link, anchor, or image, a hand edit to generated
  content, a new file without the ASF header, or a new page missing from a toctree. When a sentence
  is both false and badly worded, report it once, as must-fix, with a rewrite that fixes both.
- **Should-fix.** The page breaks a style guide rule, but what it says is true. Most `T`, `N`, `O`,
  and `P` findings belong here.
- **Nit.** A one-off change that would make the text clearer, but that no rule requires. Nits carry
  no rule ID.

A problem that keeps coming up and that no rule covers is a **guide gap**, not a finding. Report it
so the guide can be extended.

## 7. Output

When this skill runs as part of a `review-comet-pr` review, put its findings into that skill's
**Findings** and **Suggested Review Comments**, and drop the nits. That skill's review bar has no
third tier, and its rules win for anything posted to a PR. On its own, use this format:

1. **Scope.** The mode, the revisions reviewed (the base and head commits, and which guide version),
   the files reviewed, and the files skipped, with the reason.
2. **Mechanical checks.** A table with each check, its hit count, and how many hits are findings.
3. **Findings**, grouped as must-fix, should-fix, and nit. For each one give the `path:line`, the
   rule ID, the text quoted from the page, and a suggested rewrite. Keep rewrites short and plain,
   and change only what the rule requires.
4. **Guide gaps.**

If there are no findings, say so.

## Checking This Skill

After changing this skill or the style guide, run it on these cases and compare the results. The
expected results were checked on `main` in October 2026, so a later fix to a page can change them.

| Run                                                                | Expected                                                                                                                         |
| ------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------- |
| PR mode on #6786                                                   | Must-fix: `datasources.md` and the new `.doc` string claim more than `shouldBridgeUnsupportedScan` does                          |
| Audit `docs/source/user-guide/latest/understanding-comet-plans.md` | No finding for Comet's _native shuffle_, `CometNativeScan`, `CometWindowExec`, or config keys; `N5` for "Spark's native shuffle" |
| Audit `docs/source/contributor-guide/timezones.md`                 | No finding for _native code_ or _native plan_, or for `ToJson` and `ToCsv`, which are protobuf message names                     |
| Audit `docs/source/user-guide/latest/compatibility/expressions`    | Lists all 16 files, reviews 10 of the templates and index pages, names the skipped ones, and skips only text between markers     |
| Audit a scratch page containing `_native execution_`               | `N1` finding                                                                                                                     |
