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

# Comet Release Process

This documentation is for creating an official source release of Apache DataFusion Comet.

The release process is based on the parent Apache DataFusion project, so please refer to the
[DataFusion Release Process](https://github.com/apache/datafusion/blob/main/dev/release/README.md) for detailed
instructions if you are not familiar with the release process here.

Here is a brief overview of the steps involved in creating a release:

## Creating the Release Candidate

This part of the process can be performed by any committer.

- Create and merge a PR to update the version number & update the changelog
- Push a release candidate tag (e.g. 0.1.0-rc1) to the Apache repository

## Publishing the Release Candidate

This part of the process can mostly only be performed by a PMC member.

- Run the create-tarball script to create the source tarball and upload it to the dev subversion repository
- Start an email voting thread
- Once the vote passes, run the release-tarball script to move the tarball to the release subversion repository
- Register the release with the [Apache Reporter Service](https://reporter.apache.org/addrelease.html?datafusion) using
  a version such as `COMET-0.1.0`
- Delete old release candidates and releases from the subversion repositories
- Push a release tag (e.g. 0.1.0) to the Apache repository
- Reply to the vote thread to close the vote and announce the release

## Publishing JAR Files to Maven

The process for publishing JAR files to Maven is not defined yet.

## Publishing to crates.io

We may choose to publish the `datafusion-comet` to crates.io so that other Rust projects can leverage the
Spark-compatible operators and expressions outside of Spark.

## Verifying Release Candidates

The vote email will link to this section of this document, so this is where we will need to provide instructions for
verifying a release candidate.

The `dev/release/verify-release-candidate.sh` is a script in this repository that can assist in the verification
process. It checks the hashes and runs the build. It does not run the test suite because this takes a long time
for this project and the test suites already run in CI before we create the release candidate, so running them
again is somewhat redundant.

```shell
./dev/release/verify-release-candidate.sh 0.1.0 1
```

We hope that users will verify the release beyond running this script by testing the release candidate with their
existing Spark jobs and report any functional issues or performance regressions.

Another way of verifying the release is to follow the
[Comet Benchmarking Guide](https://datafusion.apache.org/comet/contributor-guide/benchmarking.html) and compare
performance with the previous release.

## Post Release Activities

Writing a blog post about the release is a great way to generate more interest in the project. We typically create a
Google document where the community can collaborate on a blog post. Once the content is agreed then a PR can be
created against the [datafusion-site](https://github.com/apache/datafusion-site) repository to add the blog post. Any
contributor can drive this process.
