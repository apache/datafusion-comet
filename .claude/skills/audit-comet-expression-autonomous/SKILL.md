---
name: audit-comet-expression-autonomous
description: Audit a single Comet expression autonomously and produce a draft PR plus any bug-tracking issues. Intended to be invoked one expression at a time by `audit-comet-expression-batch`, by `/loop`, or manually for a single expression. Non-interactive variant of `audit-comet-expression`.
argument-hint: <expression-name>
---

Run a fully autonomous audit of the `$ARGUMENTS` expression and produce one draft PR. File a separate `requires-triage` issue for any behavioural divergence the audit uncovers.

## Pre-flight

Before any other work, verify:

1. `gh auth status` succeeds. If it fails, abort with the error and ask the user to authenticate.
2. `git status --porcelain` in the current repo is empty. If not, abort: the worktree setup needs a clean parent state.
3. The expression name in `$ARGUMENTS` matches an `[x]`-marked entry in `docs/source/contributor-guide/spark_expressions_support.md`. If not, abort with "expression not found or not marked implemented".
4. The branch `autonomous-audit/$ARGUMENTS-snake-case` does NOT already exist on `origin`. Check with:

   ```bash
   gh api -X GET "repos/apache/datafusion-comet/branches/autonomous-audit/<expr-snake-case>" 2>&1 | grep -q '"name"' && echo EXISTS || echo OK
   ```

   If `EXISTS`, abort cleanly: another audit is in flight for this expression.

Abort means: print the reason, exit without modifying anything. Do not push partial work.

## Step 1: Worktree setup

Invoke the `superpowers:using-git-worktrees` skill to create a fresh worktree off `origin/main`:

- Branch name: `autonomous-audit/<expression-snake-case>` (lowercase, hyphens, e.g. `autonomous-audit/array_contains`)
- All subsequent commands run inside that worktree

## Step 2: Run the audit analysis

Read `.claude/skills/audit-comet-expression/SKILL.md` and execute its Steps 1 through 6 against `$ARGUMENTS`. Do NOT execute its Step 7 (interactive offer) or Step 8 (doc update). Both are handled below in a non-interactive form.

The output of Steps 1-6 must include:

- The expression summary (inputs, output, null behaviour)
- A list of behavioural differences across Spark 3.4.3, 3.5.8, 4.0.1
- The coverage gap matrix (Step 5)
- The implementation-gap findings (Step 5)
- The prioritized recommendations (Step 6)

Keep this output in memory. You will reference it when categorising findings and when writing the PR body.

## Step 3: Categorise findings into four buckets

From the audit output, sort every finding into exactly one of:

- **DOC**: audit sub-bullets to add under the expression's entry in `docs/source/contributor-guide/spark_expressions_support.md`. Always at least one per Spark version checked (3.4.3, 3.5.8, 4.0.1).
- **MECHANICAL**: code changes that are mechanical, such as missing `getIncompatibleReasons()` or `getUnsupportedReasons()` overrides when `getSupportLevel()` returns `Incompatible(Some(...))` or `Unsupported(Some(...))`, or reason text that exists in `getSupportLevel` but is not exposed through the get*Reasons methods.
- **TEST-GAP**: missing test cases in the Comet SQL Tests or Comet Scala Tests.
- **BUG**: behavioural divergence between Comet and Spark that goes beyond a missing test, such as wrong result, wrong type dispatch, missing ANSI branch, missing shim, or panic on a supported input.

If a finding does not cleanly fit one bucket, prefer the more conservative classification (BUG over TEST-GAP, TEST-GAP over MECHANICAL, MECHANICAL over DOC).

## Step 4: Apply DOC + MECHANICAL changes

### DOC changes

Append sub-bullets under the expression's entry in `docs/source/contributor-guide/spark_expressions_support.md`. One sub-bullet per Spark version checked. Each sub-bullet includes:

- Spark version (3.4.3, 3.5.8, 4.0.1)
- Today's date in `YYYY-MM-DD` (use `date -u +%Y-%m-%d`, not memory)
- A brief note for any version-specific finding. Omit the note if nothing notable.

Example for an expression with no version-specific differences:

```markdown
- [x] some_expression
  - Spark 3.4.3 (2026-05-26)
  - Spark 3.5.8 (2026-05-26)
  - Spark 4.0.1 (2026-05-26)
```

### MECHANICAL changes

For each mechanical finding, apply the fix. The common pattern is the missing `get*Reasons()` override. Follow the existing project pattern: extract the reason as a `private val` and reference it from both `getSupportLevel` and the get*Reasons method. See `spark/src/main/scala/org/apache/comet/serde/structs.scala::CometStructsToJson` or `spark/src/main/scala/org/apache/comet/serde/datetime.scala::CometHour` for the reference pattern.

Run `./mvnw spotless:apply -DskipTests -Dscalastyle.skip=true` after any Scala edit to keep formatting clean.

## Step 5: Apply TEST-GAP findings

For each TEST-GAP finding, prefer a Comet SQL test over a Comet Scala test (per project convention in `docs/source/contributor-guide/sql-file-tests.md`).

For each test:

1. Write the test in `spark/src/test/resources/sql-tests/expressions/<category>/$ARGUMENTS.sql` (or extend an existing file for that expression).
2. Run JUST that test:

   ```bash
   ./mvnw test -DwildcardSuites=CometSqlFileTestSuite \
               -Dsuites="org.apache.comet.CometSqlFileTestSuite $ARGUMENTS" \
               -Dtest=none -Dscalastyle.skip=true
   ```

3. If the test passes: keep the test as-written and move on.
4. If the test fails (Comet result differs from Spark): this is a real divergence.
   - File a GitHub issue (see Step 6 below) describing the divergence. Capture the new issue number.
   - Re-add the test in SQL `ignore` mode (or wrap a Scala test in `IgnoreComet`) with the issue URL as the reason. Per `feedback_ignorecomet_link_issue`, the reason MUST be a tracking-issue URL.

Run the full set of added tests once more at the end of this step to confirm everything still passes (or is correctly marked `ignore`).

## Step 6: Apply BUG findings (issue filing)

For each BUG finding that is NOT already covered by a test-failure-derived issue from Step 5:

File a GitHub issue:

```bash
gh issue create \
  --repo apache/datafusion-comet \
  --title "$ARGUMENTS: <one-line divergence summary>" \
  --label "requires-triage,area:expressions" \
  --body-file /tmp/issue-body-$$.md
```

The issue body must include:

- Expression name and the affected Spark version(s)
- Minimal SQL repro (or Scala if SQL is impractical)
- Observed Comet result vs expected Spark result
- A back-reference to the autonomous-audit PR (added in Step 8) using a placeholder you fix up after the PR is open

Capture every new issue URL for the PR body.

## Step 7: Commit

One commit per audit. Use conventional-commit format and pick the title based on what was changed:

- Tests added: `chore(audit): audit <Expression> and expand tests`
- Mechanical fixes only, no tests: `chore(audit): audit <Expression>`
- Doc sub-bullets only: `chore(audit): record audit of <Expression>`

```bash
git add docs/source/contributor-guide/spark_expressions_support.md \
        spark/src/main/scala/org/apache/comet/serde \
        spark/src/test/resources/sql-tests \
        spark/src/test/scala
git commit -m "chore(audit): audit <Expression> and expand tests"
```

(Adjust the `git add` paths to what actually changed.)

## Step 8: Push branch and open draft PR

```bash
git push -u origin HEAD
gh pr create --draft \
  --base main \
  --title "chore(audit): audit <Expression> and expand tests" \
  --body-file /tmp/pr-body-$$.md
```

PR body template (substitute `<...>` placeholders):

```
## Which issue does this PR close?
N/A. Autonomous audit pass.

## Rationale for this change
Audit of the `<Expression>` expression against Spark 3.4.3, 3.5.8, and 4.0.1.

## What changes are included in this PR?
- Audit sub-bullets in `spark_expressions_support.md`
- <N> new SQL/Scala tests: <comma-separated list of test names>
- <Mechanical fixes, if any. Omit this line if none.>

## How are these changes tested?
- `./mvnw test -DwildcardSuites=CometSqlFileTestSuite ...` (passes locally)
- <X> tests added as `ignore` / `IgnoreComet` pending fix: #<issue1>, #<issue2>
  (omit this line if no tests were ignored)

Scaffolded by the `audit-comet-expression-autonomous` skill.
```

After the PR is open:

- Edit each filed issue from Step 5/6 to replace the PR placeholder with the real PR URL using `gh issue edit <N> --body-file ...`.
- Capture the PR URL.

## Step 9: Exit worktree

The `superpowers:using-git-worktrees` skill handles cleanup. If the worktree has uncommitted changes (it should not at this point), do not let it auto-delete. Surface the state to the user.

## Output to the user

Print exactly:

```
Audit complete: <Expression>
PR: <PR URL>
Bug issues filed: <issue URL>, <issue URL>  (or "none")
Tests added: <N> (<M> passing, <K> ignored pending fix)
Mechanical fixes: <count>
```

## What this skill must not do

- Do not open a non-draft PR. The reviewer marks ready when satisfied.
- Do not add AI/Claude attribution anywhere (commit message, PR body, issue body). The "Scaffolded by the ... skill" footer is required by `feedback_acknowledge_skills_in_pr` and nothing more.
- Do not skip the failing-test issue filing. Every failing test must point to a real tracking issue.
- Do not modify the existing `audit-comet-expression` skill.
- Do not re-audit an expression that already has sub-bullets in the support doc. The pre-flight checks this implicitly via the branch-exists guard. Re-audit policy is explicit future work.
