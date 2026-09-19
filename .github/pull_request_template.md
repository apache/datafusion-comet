## Which issue does this PR close?

<!--
We generally require a GitHub issue to be filed for all bug fixes and enhancements and this helps us generate change logs for our releases. You can link an issue to this PR using the GitHub syntax. For example `Closes #123` indicates that this PR will close issue #123.
-->

Closes #.

## Rationale for this change

<!--
 Why are you proposing this change? If this is already explained clearly in the issue then this section is not needed.
 Explaining clearly why changes are proposed helps reviewers understand your changes and offer better suggestions for fixes.
-->

## What changes are included in this PR?

<!--
There is no need to duplicate the description in the issue here but it is sometimes worth providing a summary of the individual changes in this PR.
-->

## How are these changes tested?

<!--
We typically require tests for all PRs in order to:
1. Prevent the code from being accidentally broken by subsequent changes
2. Serve as another way to document the expected behavior of the code

If tests are not included in your PR, please explain why (for example, are they covered by existing tests)?
-->

## Have you run the Comet review skill on this PR?

<!--
This repository ships an agent skill, `review-comet-pr`, that checks a change for the problems we
most often find in review: null and NaN handling, type coercion, ANSI mode, overflow, missing data
types, thin test coverage, and behavior differences between supported Spark versions.

If you use an AI coding agent, please run it on your own changes and address what it finds before
asking for review. There are two ways to run it:

- Before opening the PR, from your branch, run the skill with no argument (in Claude Code:
  `/review-comet-pr`). It reviews your committed and uncommitted changes against `main`.
- On an open PR, pass the PR number (`/review-comet-pr 1234`). This also picks up CI results and
  existing review comments.

If your agent does not load skills automatically, point it at `.ai/skills/review-comet-pr/SKILL.md`
and ask it to follow that file.

The skill only reports findings. It will not edit your code or post comments, so you stay in
control of what changes.

This is a request, not a requirement. If you do not use an AI coding agent, just say so below.
-->

- [ ] Yes, I ran `review-comet-pr` and addressed the findings
- [ ] No, I do not use an AI coding agent
