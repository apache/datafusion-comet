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

# Contributing to Apache DataFusion Comet

We welcome contributions to Comet in many areas, and encourage new contributors to get involved.

Here are some areas where you can help:

- Testing Comet with existing Spark jobs and reporting issues for any bugs or performance issues
- Contributing code to support Spark expressions, operators, and data types that are not currently supported
- Reviewing pull requests and helping to test new features for correctness and performance
- Improving documentation

## Finding issues to work on

We maintain a list of good first issues in GitHub [here](https://github.com/apache/datafusion-comet/issues?q=is%3Aopen+is%3Aissue+label%3A%22good+first+issue%22). We also have a [roadmap](roadmap.md).

To assign yourself an issue, comment `take` on the issue. To unassign yourself, comment `untake`.

## Reporting issues

We use [GitHub issues](https://github.com/apache/datafusion-comet/issues) for bug reports and feature requests.

## Review expectations

Comet follows the usual Apache model: a pull request needs an approval from a committer before it
can be merged, and any committer can give it. What that rule does not capture is that the people
who know a given area are spread across many time zones. A change queued a few hours after it was
opened has only been seen by whoever happened to be awake.

So leave a non-trivial pull request open for at least 24 hours after it is ready for review, even
once it has an approval. The delay is not the point; giving a full day means everyone who might
recognize a problem gets a chance to look, including whoever wrote the code being changed.

Read "non-trivial" generously. A second pair of eyes is usually worth waiting for on:

- new or changed behavior in the planner, the serde, a native operator, or a Spark shim
- a change to a default, a configuration name, or a public API
- a new dependency, or a bump that crosses a major version
- performance work whose numbers a reviewer might want to reproduce
- anything under `dev/diffs/`

Changes that do not need the wait: documentation and comment fixes, test-only additions, routine
dependency bumps, and repairs to a broken build or a red `main`, where waiting costs more than the
review would catch.

Nothing enforces this. No check fails if you merge early, and the merge queue does not know how
long a pull request has been open. It is a convention, and it exists because the costs are not
symmetric: an hour saved on a merge is worth much less than a design problem caught before it
lands.

If a change is not getting the attention you think it needs, ask for it — in the Comet Slack or
Discord channel, or on the community call below.

## Asking for Help

The Comet project uses the same Slack and Discord channels as the main Apache DataFusion project. See details at
[Apache DataFusion Communications]. There are dedicated Comet channels in both Slack and Discord.

## Regular public meetings

The Comet contributors hold regular video calls where new and current contributors are welcome to ask questions and
coordinate on issues that they are working on.

The call is held weekly on Thursdays from 4:00 PM to 5:00 PM PST (America/Los_Angeles). Join via the
[Google Meet video call link](https://meet.google.com/hjb-waqs-hfv), or add the meeting to your
calendar by subscribing to the [Comet community meeting Google Calendar].

See the [Comet community meeting notes] for more information.

[Apache DataFusion Communications]: https://datafusion.apache.org/contributor-guide/communication.html
[Comet community meeting notes]: https://docs.google.com/document/d/1IQeSIwKqKncXsReGl18MwooyQjAYL9HTDCktYB64FlA/edit?usp=sharing
[Comet community meeting Google Calendar]: https://calendar.google.com/calendar/u/0?cid=OWEwNjZlOTkzMDQ3MmE0NTlmMDQ0NTY5NGRlOGE3NGY2MjZkZWNiNjFlYzUxOWYwMGFlMmFhOWE0MTQ4NThiZkBncm91cC5jYWxlbmRhci5nb29nbGUuY29t
