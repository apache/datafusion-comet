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

# Aapche DataFusion Comet: Source Release Process

This documentation is for creating an official source release of Apache DataFusion Comet.

## Creating the Release Candidate

This part of the process can be performed by any committer.

Here are the steps, using the 0.1.0 release as an example.

### Create Release Branch

This document assumes that GitHub remotes are set up as follows:

```shell
$ git remote -v
apache	git@github.com:apache/datafusion-comet.git (fetch)
apache	git@github.com:apache/datafusion-comet.git (push)
origin	git@github.com:yourgithubid/datafusion-comet.git (fetch)
origin	git@github.com:yourgithubid/datafusion-comet.git (push)
```

Create a release branch from the latest commit in main and push to the `apache` repo:

```shell
get fetch apache
git checkout main
git reset --hard apache/main
git checkout -b branch-0.1
git push apache branch-0.1
```

Create and merge a PR against the release branch to update the Maven version from `0.1.0-SNAPSHOT` to `0.1.0`

### Generate the Change Log

Generate a change log to cover changes between the previous release and the release branch HEAD by running
the provided `generate-changelog.py` script.

It is recommended that you set up a virtual Python environment and then install the dependencies:

```shell
python3 -m venv venv
source venv/bin/activate
pip3 install -r requirements.txt
```

To generate the changelog, set the `GITHUB_TOKEN` environment variable to a valid token and then run the script
providing two commit ids or tags followed by the version number of the release being created. The following
example generates a change log of all changes between the previous version and the current release branch HEAD revision.

```shell
export GITHUB_TOKEN=<your-token-here>
python3 generate-changelog.py 52241f44315fd1b2fd6cd9031bb05f046fe3a5a3 branch-0.1 0.0.0 > ../changelog/0.1.0.md
```

Create a PR against the _main_ branch to add this change log and once this is approved and merged, cherry-pick the
commit into the release branch.

### Tag the Release Candidate

Tag the release branch with `0.1.0-rc1` and push to the `apache` repo

```shell
git fetch apache
git checkout branch-0.1
git reset --hard apache/branch-0.1
git tag 0.1.0-rc1
git push apache 0.1.0-rc1
```

### Update Version in main

Create a PR against the main branch to update the Rust crate version to `0.2.0` and the Maven version to `0.2.0-SNAPHOT`.

## Publishing the Release Candidate

This part of the process can mostly only be performed by a PMC member.

### Create the Release Candidate Tarball

Run the create-tarball script on the release candidate tag (`0.1.0-rc1`) to create the source tarball and upload it to the dev subversion repository

```shell
GH_TOKEN=<TOKEN> ./dev/release/create-tarball.sh 0.1.0 1
```

### Start an Email Voting Thread

Send the email that is generated in the previous step to `dev@datafusion.apache.org`.

### Publish the Release Tarball

Once the vote passes, run the release-tarball script to move the tarball to the release subversion repository.

```shell
./dev/release/create-tarball.sh 0.1.0 1
```

Push a release tag (`0.1.0`) to the `apache` repository.

```shell
git fetch apache
git checkout 0.1.0-rc1
git tag 0.1.0
git push apache 0.1.0
```

Reply to the vote thread to close the vote and announce the release.

## Post Release Admin

Register the release with the [Apache Reporter Service](https://reporter.apache.org/addrelease.html?datafusion) using
a version such as `COMET-0.1.0`.

### Delete old RCs and Releases

See the ASF documentation on [when to archive](https://www.apache.org/legal/release-policy.html#when-to-archive)
for more information.

#### Deleting old release candidates from `dev` svn

Release candidates should be deleted once the release is published.

Get a list of DataFusion Comet release candidates:

```shell
svn ls https://dist.apache.org/repos/dist/dev/datafusion | grep comet
```

Delete a release candidate:

```shell
svn delete -m "delete old DataFusion Comet RC" https://dist.apache.org/repos/dist/dev/datafusion/apache-datafusion-comet-0.1.0-rc1/
```

#### Deleting old releases from `release` svn

Only the latest release should be available. Delete old releases after publishing the new release.

Get a list of DataFusion releases:

```shell
svn ls https://dist.apache.org/repos/dist/release/datafusion | grep comet
```

Delete a release:

```shell
svn delete -m "delete old DataFusion Comet release" https://dist.apache.org/repos/dist/release/datafusion-comet/datafusion-comet-0.0.0
```

## Publishing Binary Releases

### Publishing JAR Files to Maven

The process for publishing JAR files to Maven is not defined yet.

### Publishing to crates.io

We may choose to publish the `datafusion-comet` to crates.io so that other Rust projects can leverage the
Spark-compatible operators and expressions outside of Spark.

## Post Release Activities

Writing a blog post about the release is a great way to generate more interest in the project. We typically create a
Google document where the community can collaborate on a blog post. Once the content is agreed then a PR can be
created against the [datafusion-site](https://github.com/apache/datafusion-site) repository to add the blog post. Any
contributor can drive this process.
