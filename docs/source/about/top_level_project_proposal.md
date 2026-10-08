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

# Proposal to Establish Apache Comet as a Top-Level Project

This page is a draft of the proposal and board resolution to promote Comet from a sub-project of
Apache DataFusion to a top-level Apache project, Apache Comet. It is adapted from the
[proposal][datafusion-proposal] that established Apache DataFusion as a top-level project in 2024,
when DataFusion graduated from Apache Arrow. Progress is tracked in [#6793][epic].

The DataFusion PMC has not voted on this proposal yet.

## Board Resolution

```text
X. Establish the Apache Comet Project

WHEREAS, the Board of Directors deems it to be in the best interests of the
Foundation and consistent with the Foundation's purpose to establish a Project
Management Committee charged with the creation and maintenance of open-source
software related to accelerating Apache Spark using native, columnar
execution, for distribution at no charge to the public.

NOW, THEREFORE, BE IT RESOLVED, that a Project Management Committee (PMC), to
be known as the "Apache Comet Project", be and hereby is established pursuant
to Bylaws of the Foundation; and be it further

RESOLVED, that the Apache Comet Project be and hereby is responsible for the
creation and maintenance of software related to accelerating Apache Spark
using native, columnar execution; and be it further

RESOLVED, that the office of "Vice President, Apache Comet" be and hereby is
created, the person holding such office to serve at the direction of the Board
of Directors as the chair of the Apache Comet Project, and to have primary
responsibility for management of the projects within the scope of
responsibility of the Apache Comet Project; and be it further

RESOLVED, that the persons listed immediately below be and hereby are
appointed to serve as the initial members of the Apache Comet Project:

 * Andy Grove <agrove@apache.org>
 * Andrew Lamb <alamb@apache.org>
 * Liang-Chi Hsieh <viirya@apache.org>
 * Matt Butrovich <mbutrovich@apache.org>
 * Oleks V. <comphead@apache.org>

NOW, THEREFORE, BE IT FURTHER RESOLVED, that Andy Grove be appointed to the
office of Vice President, Apache Comet, to serve in accordance with and
subject to the direction of the Board of Directors and the Bylaws of the
Foundation until death, resignation, retirement, removal or disqualification,
or until a successor is appointed; and be it further

RESOLVED, that the Apache Comet Project be and hereby is tasked with the
migration and rationalization of the Apache DataFusion Comet sub-project; and
be it further

RESOLVED, that all responsibilities pertaining to the Apache DataFusion Comet
sub-project encumbered upon the Apache DataFusion Project are hereafter
discharged.
```

The scope statement is under discussion in [#6789][scope-issue].

## Summary

We propose creating a new top-level project, Apache Comet, from an existing sub-project of Apache
DataFusion to facilitate additional community and project growth.

## Abstract

[Apache DataFusion Comet][comet-docs] is a high-performance accelerator for Apache Spark. It
replaces Spark operators and expressions with implementations that process Apache Arrow columnar
data, most of them native Rust code built on Apache DataFusion, and it runs existing Spark SQL,
DataFrame, and PySpark workloads without code changes. Comet accelerates Parquet and Apache Iceberg
scans, shuffle, joins, aggregations, and hundreds of Spark expressions, and it is about 1.8 times
faster than Spark 4.2 on TPC-DS at scale factor 1000.

## Proposal

We propose creating a new top-level ASF project, Apache Comet, governed initially by existing
Apache DataFusion PMC members and committers. The project's code is in one existing git
repository, currently governed by Apache DataFusion, which would transfer to the new top-level
project.

## Background

Apple donated Comet to the Apache Arrow project in early 2024 ([IP clearance][ip-clearance]), and
it became a sub-project of Apache DataFusion when DataFusion became a top-level project in
April 2024. Starting as a DataFusion sub-project made sense because of the overlap in contributors.

Comet is different from the other DataFusion sub-projects, which extend DataFusion: Python and Java
bindings, and distributed execution with Ballista. Comet is an accelerator for Apache Spark that
uses DataFusion as its execution engine. DataFusion and Comet now have largely distinct and strong
communities, though there is overlap, and the Comet community is large and active enough to stand
on its own. Focused governance will make it easier to grow that community and to recognize
contributors whose work is mainly on Comet.

The community has discussed this idea publicly since July 2026 ([#5184][discussion]). As of the
time of this writing the reactions have been exclusively positive, including from members of the
DataFusion PMC.

Several current members of the DataFusion PMC are active in Comet and understand and believe deeply
in the Apache Way. They have served as PMC chairs, managed and voted on every Comet release, and
guided new contributors. With this existing governance experience, the new top-level project will
be able to function well immediately and independently.

## Current Status

### Meritocracy

Comet has been developed as part of Apache DataFusion, and before that Apache Arrow, and thus has
been operating as a meritocracy. Many Comet contributors have been recognized as DataFusion
committers and PMC members.

This proposal does not promote anyone. The initial PMC members and committers are existing
DataFusion PMC members and committers. Once Apache Comet is established, its PMC will vote on new
committers and PMC members from the Comet community.

### Community

Comet has an active and growing community:

- 24 releases since 0.1.0 in July 2024, about one a month. 1.0.0 was released on 2026-08-07 and
  1.1.0 on 2026-10-06.
- 146 people have authored merged pull requests, 81 of them in the last 12 months.
- The 1.1.0 release included contributions from 40 people.
- The 1.0.0 and 1.1.0 release votes each received six binding votes.

We hope that becoming a separate project will help both the DataFusion and Comet communities by
letting each focus on its own users and contributors.

### Alignment

The ASF is a natural home for Comet. It accelerates Apache Spark, it is built on Apache DataFusion
and Apache Arrow, and it integrates with Apache Parquet, Apache Iceberg, and Apache Celeborn.

## Project Leadership

### Proposed Initial PMC

We propose the following people as the initial Comet PMC members. Andy Grove, Liang-Chi Hsieh,
Matt Butrovich, and Oleks V. are the existing DataFusion PMC members who contribute to Comet.
Andrew Lamb is the DataFusion PMC chair and an ASF Member. He has taken part in the release vote for
nearly every Comet release, and he led DataFusion's own move to a top-level project.

| Name            | Apache ID  | Other ASF roles                                                         | Affiliation |
| --------------- | ---------- | ----------------------------------------------------------------------- | ----------- |
| Andy Grove      | agrove     | DataFusion PMC, Arrow PMC (former chair), ASF Member                    | Apple       |
| Andrew Lamb     | alamb      | DataFusion PMC chair, Arrow PMC (former chair), Parquet PMC, ASF Member | InfluxData  |
| Liang-Chi Hsieh | viirya     | DataFusion PMC, Arrow PMC, Spark PMC, ASF Member                        | Databricks  |
| Matt Butrovich  | mbutrovich | DataFusion PMC                                                          | Teradata    |
| Oleks V.        | comphead   | DataFusion PMC                                                          | Apple       |

We propose Andy Grove, a former chair of the Apache Arrow PMC, as the initial chair (and therefore
ASF Vice President) of the Comet project.

### Proposed Initial Committers

In addition to the PMC, we propose the following people as the initial Comet committers. They are
the existing DataFusion committers who have contributed to Comet.

| Name               | Apache ID        |
| ------------------ | ---------------- |
| Bhargava Vadlamani | bhargava         |
| Chao Sun           | sunchao          |
| Dmitrii Blaginin   | blaginin         |
| Huaxin Gao         | huaxingao        |
| Kazuyuki Tanimura  | kazuyukitanimura |
| Manu Zhang         | mauzhang         |
| Martin Grigorov    | mgrigorov        |
| Parth Chandra      | parthc           |
| Raz Luvaton        | rluvaton         |
| Zhen Wang          | wangzhen         |

## Risk Assessments

### Naming and Trademarks

As a sub-project of Arrow and then DataFusion, the Comet name has been used since early 2024
without any known issues. A name search for "Apache Comet" was approved in
[PODLINGNAMESEARCH-255][name-search], and the research is in [#5291][name-issue].

### Legal and IP Clearance

All Comet code has either been donated to the Apache Arrow project with appropriate
[IP clearance][ip-clearance] or has been developed directly under ASF processes and procedures.
Thus creating a new top-level project poses no new legal or IP risks.

### Code Extraction

The relevant code is already in a separate repository,
[apache/datafusion-comet](https://github.com/apache/datafusion-comet). We foresee no issues with
code extraction and propose that the repository be renamed to reflect the new top-level project.

Comet depends on the `datafusion-spark` crate in
[apache/datafusion](https://github.com/apache/datafusion), which would remain part of the DataFusion
project.

### Orphaned Products

Comet has had multiple commits daily for more than two years and its number of contributors is
growing. We do not foresee the project being orphaned in the next several years.

### Inexperience with Open Source

The proposed PMC includes three ASF Members, the current chair of Apache DataFusion, and two former
chairs of Apache Arrow, and its members have managed and voted on every Comet release. The proposed
committers include members of the Spark, Arrow, Drill, Hadoop, and Hive PMCs. The Comet PMC and more
experienced committers will continue to coach new community members who may be less familiar with
the Apache Way.

### Homogeneous Developers

The five proposed PMC members work for four different employers: Apple, Databricks, InfluxData,
and Teradata. No employer has more than two proposed PMC members.

### Reliance on Salaried Developers

A substantial amount of work on Comet has been done by salaried developers, but it also attracts
contributions from students and volunteers, and we plan no changes in contribution structure.

### Relationships with Other Apache Products

Comet will continue to have a strong relationship with Apache DataFusion and Apache Arrow, which it
is built on and contributes improvements to, and with Apache Spark, which it accelerates. Proposed
PMC member Liang-Chi Hsieh and proposed committers Chao Sun and Huaxin Gao are members of the Spark
PMC. Comet also integrates with Apache Parquet, Apache Iceberg, and Apache Celeborn.

Apache Gluten and Apache Auron also accelerate Apache Spark with native execution. The projects
have separate communities and different designs, and Comet's documentation includes a
[comparison with Gluten](gluten_comparison.md).

### Cryptography

Comet does not implement cryptographic algorithms, but its native library includes third-party Rust
crates that do, such as rustls and ring. They are used for TLS connections to object stores and for
reading Parquet files that use Parquet modular encryption. As part of setting up the new project,
we will follow the ASF process for [handling cryptography][crypto] and file the export
notification.

## Required Resources

### Mailing Lists

- private@comet.apache.org for private PMC discussions (with moderated subscriptions)
- dev@comet.apache.org
- commits@comet.apache.org
- github@comet.apache.org for GitHub issue and pull request notifications

### Version Control

We propose to continue to use git for source control and GitHub for hosting and testing resources,
and to rename the repository to reflect the new top-level name:

- [apache/datafusion-comet](https://github.com/apache/datafusion-comet) → apache/comet

### Issue Tracking

Comet would continue to use GitHub for its issue tracking and communications.

### Website

We propose to publish the documentation at https://comet.apache.org, with redirects from
https://datafusion.apache.org/comet.

### Other Resources

The existing repository already makes use of Apache infrastructure, and we expect no change in the
initial resource usage. As the project continues to grow, we expect continued infrastructure demand
growth.

## FAQ

### Has a sub-project been promoted to a top-level project before?

Yes, and it happens regularly. DataFusion itself was promoted from a sub-project of Apache Arrow
in 2024. Arrow was created as a top-level project from work that started in Apache Drill, and
several Hadoop sub-projects became top-level projects, including Mahout, Avro, and HBase.

## Related Material

- Tracking issue: [#6793][epic]
- Discussion about creating an Apache Comet top-level project: [#5184][discussion]
- Name search request: [PODLINGNAMESEARCH-255][name-search], with research in [#5291][name-issue]
- Discussion on the Incubator list: [general@incubator.apache.org][incubator-thread]
- Comet IP clearance: [arrow-datafusion-comet][ip-clearance]
- DataFusion top-level project proposal, used as a template for this one:
  [proposal][datafusion-proposal], [vote][datafusion-vote], and the
  [resolution adopted by the board on 2024-04-17][datafusion-resolution]

[comet-docs]: https://datafusion.apache.org/comet/
[crypto]: https://infra.apache.org/crypto.html
[datafusion-proposal]: https://docs.google.com/document/d/11WTNYS8KWScOt3ySTX39WVS6krPhUvHsuJRY9PZQx4g
[datafusion-resolution]: https://www.apache.org/foundation/records/minutes/2024/board_minutes_2024_04_17.txt
[datafusion-vote]: https://lists.apache.org/thread/tv8s8ootxf7nrsp3vo1mt8mtxxt5qcor
[discussion]: https://github.com/apache/datafusion-comet/issues/5184
[epic]: https://github.com/apache/datafusion-comet/issues/6793
[incubator-thread]: https://lists.apache.org/thread/pdgry1ft252joqrbzdmog6134djo9xyc
[ip-clearance]: https://incubator.apache.org/ip-clearance/arrow-datafusion-comet.html
[name-issue]: https://github.com/apache/datafusion-comet/issues/5291
[name-search]: https://issues.apache.org/jira/browse/PODLINGNAMESEARCH-255
[scope-issue]: https://github.com/apache/datafusion-comet/issues/6789
