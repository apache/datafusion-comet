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

# Apache DataFusion Comet (Apple Edition)

<img src="docs/source/_static/images/DataFusionComet-Logo-Light.png" width="512" alt="logo"/>

> [!NOTE]
> This is the Apple-specific README for Apache DataFusion Comet. The README for the open source version is located at [README.md](README.md).

## Differences between Apache and Apple versions of Comet

The Apple version of Comet differs from the Apache version in the following ways:

- Releases are cut at different times and there is no correlation between Apple Comet version numbers and Apache
  version numbers.
- The Apache version typically has all features enabled by default but we only enable a subset of features in the Apple
  version so that we can roll out features gradually once they are production-ready.

## Release Notes 

Release notes are available at [https://github.pie.apple.com/IPR/apache-arrow-datafusion-comet/releases](https://github.pie.apple.com/IPR/apache-arrow-datafusion-comet/releases).

## How to Release

Here are the steps to cut a new Comet release.

1. After everything is committed to the `main-apple` branch. Checkout
   `main-apple-release` branch and rebase it on top of `main-apple`:
   ```
   git rebase main-apple
   ```
   And then push the `main-apple-release` branch to remote. This will trigger the
   release job.

2. Make sure the [release job](https://rio.apple.com/projects/aci-ipr-apache-arrow-datafusion-comet)
   finishes successfully.

3. Go back to the `main-apple` branch and bump the SNAPSHOT version in the `pom.xml` file and `rio.yaml` file, and
   push the `main-apple` branch to remote.

4. Create release notes in [Releases](https://github.pie.apple.com/IPR/apache-arrow-datafusion-comet/releases).

