<!---
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

# AI Policy

Comet follows the same policy for AI-assisted contributions as the
[Apache DataFusion AI policy], adapted below for this repository.

- We welcome AI-assisted PRs from anyone. We do not welcome unreviewed "AI dumps"
  (defined below).
- The PR author should have personally read the entire PR they submit and
  **understand the core ideas end-to-end**. Authors should be ready to justify
  and help reviewers understand the design and code during review.
- **Call out unknowns and assumptions**. It is okay to not fully understand some bits of
  AI-generated code. Please point these cases out so we can work together to clear up any
  concerns.
- **Follow the [ASF Generative Tooling Guidance]**. Among other things, the author must ensure
  the PR does not include undisclosed third-party code or copyrighted material.

[Apache DataFusion AI policy]: https://datafusion.apache.org/contributor-guide/ai-policy.html
[ASF Generative Tooling Guidance]: https://www.apache.org/legal/generative-tooling.html

"Understand the core ideas" means more than being able to follow the diff textually. We expect
the author to take an active role in responding to feedback and shaping the PR so that it fits the
project as a whole. The worst outcome is a reviewer who ends up driving the contributor's AI tool.

## What is an "AI dump" and why it is not helpful

An "AI dump" is a PR, or a series of PRs, made up largely of AI-generated code and descriptions that
the author has not reviewed and does not understand. The code may even be correct. The problem is
that all the work of understanding falls on the reviewer.

Code review serves two purposes: finishing the intended task, and sharing knowledge between authors
and reviewers as a long-term investment in the project. An AI dump serves neither. Maintainers could
finish the task faster by running the AI tool themselves, and an author who only relays the tool's
output learns little from the review.

Review capacity is **very limited**, so PRs that appear to be AI dumps may not be reviewed and may
be closed. Several PRs opened in a short time, especially by a first-time contributor, that in our
judgment show a lack of understanding or author engagement may be treated as spam and closed. One
high-quality PR that you work with maintainers to merge is worth far more than ten PRs generated and
submitted by an agent.

## Limits on open PRs

To keep the number of open PRs within review capacity, the following measures apply to authors without
write access:

- Users without write access are limited to **3 open, non-draft PRs** (see
  [Submitting a Pull Request](development.md#submitting-a-pull-request)). This is enforced by
  the `creation_cap` setting in `.asf.yaml`.
- A workflow closes a newly opened PR automatically when its author has a very large number of open
  PRs across many GitHub repositories, which is the signature of automated bulk submission. If you
  are a person who intends to follow the change through review, reply on the PR or write to
  dev@datafusion.apache.org, and a committer can reopen it.

## Responding to review comments

Reviewers want to talk to **you**, not to your AI tool. Please do not answer review comments with
unreviewed, fully AI-generated responses. Using AI to help prepare a response is fine (for example
to understand a comment, or to translate your reply into English), but the reply should be in your own
words. Examples of unreviewed AI-generated replies:

- Summarizing the diff instead of answering the question that was asked.
- Listing the commands run locally (for example `cargo fmt` or `cargo test`) and whether they
  passed. CI already runs these checks, so this is not useful to reviewers.
- Statements that do not make sense in context, such as claiming tests could not be run because
  `cargo` is not installed.

## AI-assisted reviews

The same standard applies to AI-generated reviews as to AI-generated code: you should have read and
understood anything you post. Raw AI review output is often verbose and can take substantial effort
to interpret. If you review PRs with the help of an AI tool, read all comments first, remove detail
that is unnecessary or that you do not understand, and explain the rest in your own words so that
each comment makes a clear, specific request.

## Better ways to contribute than an "AI dump"

Write a high-quality issue with a clear problem statement and a minimal, reproducible example. The
reproducer should focus on the end-user-visible behavior rather than the details of a code defect.
This makes it easier for others to contribute and for reviewers to understand the problem.
