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

# AI-Assisted Contributions

Many Comet contributors use AI coding tools to write code, tests and documentation, and to help
review pull requests. That is welcome, and this page sets out what the project expects when you do.

In short, you are responsible for everything you submit, whichever tool helped you write it.
Automated reviews can be helpful, but a pull request must always be approved by a human.

## Writing Code with AI Tools

Reviewers treat you as the author of your pull request, whoever or whatever wrote the code. Before
you open it:

- Understand the change end to end. Be ready to explain the design and the code in review, and to
  justify the choices it makes.
- Point out anything you are unsure about, in the description or in a comment on your own pull
  request, so that reviewers know where to look. For example: "this passes the tests, but I don't
  know whether it is safe to call from more than one thread."
- Test it. Comet has to return the same results as Spark, including for nulls, overflow, ANSI mode
  and time zones, and code that looks right can still get those wrong. Test those cases, and run the
  suites that your change affects. [Continuous Integration](ci.md) explains which changes need which
  suites.
- Read the diff the way a reviewer will. Remove comments that narrate how the change was developed,
  tests that don't cover anything new, and edits unrelated to the change.
- Keep the pull request focused. A tool can produce a large change in minutes, but reviewing it
  takes much longer, and small pull requests get reviewed sooner.

The same goes for issues and comments that you write with a tool's help. They are posted under your
name, so check them before you post them.

You are also responsible for making sure that your contribution can be licensed under the Apache
License. The ASF's [Generative Tooling Guidance] explains what that involves when a tool wrote part
of it.

The repository has instructions for coding agents in [`AGENTS.md`] and skills for common Comet
tasks, such as adding an expression or reviewing a pull request, under [`.ai/skills`]. For more on
why understanding your change matters, see DataFusion's [policy on AI-assisted contributions].

## Automated Reviews

A pull request may get an automated review from a bot or a GitHub app, or from an AI agent that a
contributor runs. These reviews can be a useful first pass, and they often find real problems, such
as a missed null case or a result that differs from Spark.

Treat their findings like any reviewer's comments. If you are the author, check each one, fix the
ones that are real, and reply to the ones that are not with the reason. Don't change your code just
because a tool suggested it.

If you are reviewing, read what the tool found and how the author responded, and build on it rather
than raising the same points again. Don't treat a clean automated review as evidence that the
change is correct, though. A tool can miss context that a maintainer has, and it may not have built
the change or run the test suites.

## A Human Must Approve Every Pull Request

A pull request needs an approving review from a committer before it can merge to `main` or to a
release branch. That approval is a person's judgment that the change is ready. They have read it,
they understand it, and they share responsibility for it with the author. A tool cannot take on that
responsibility, so **every approval must come from a human who reviewed the change.**

In practice:

- Don't let a tool approve pull requests for you. If a review tool posts under your GitHub account,
  configure it to post comment reviews only. GitHub shows its approvals as yours, and other
  reviewers cannot tell them apart.
- Using AI to help you review is fine. Approve once you have reviewed the change yourself and are
  prepared to vouch for it. The review skills under [`.ai/skills`] are built for this. They report
  their findings to you, and you decide what to post.
- If you are a committer, don't merge a pull request on the strength of a tool's approval. GitHub
  counts it, but the pull request still needs an approval from a person.

[Generative Tooling Guidance]: https://www.apache.org/legal/generative-tooling.html
[`AGENTS.md`]: https://github.com/apache/datafusion-comet/blob/main/AGENTS.md
[`.ai/skills`]: https://github.com/apache/datafusion-comet/tree/main/.ai/skills
[policy on AI-assisted contributions]: https://datafusion.apache.org/contributor-guide/index.html#ai-assisted-contributions
