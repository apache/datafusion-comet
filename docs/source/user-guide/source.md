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

# Building Comet From Source

It is sometimes preferable to build from source for a specific platform.

## Using a Published Source Release

Official source releases can be downloaded from https://dist.apache.org/repos/dist/release/datafusion/

```console
# Pick the latest version
export COMET_VERSION=0.3.0
# Download the tarball
curl -O "https://dist.apache.org/repos/dist/release/datafusion/datafusion-comet-$COMET_VERSION/apache-datafusion-comet-$COMET_VERSION.tar.gz"
# Unpack
tar -xzf apache-datafusion-comet-$COMET_VERSION.tar.gz
cd apache-datafusion-comet-$COMET_VERSION
```

Build

```console
make release-nogit PROFILES="-Pspark-3.4"
```

## Building from the GitHub repository

Clone the repository:

```console
git clone https://github.com/apache/datafusion-comet.git
```

Build Comet for a specific Spark version:

```console
cd datafusion-comet
make release PROFILES="-Pspark-3.4"
```

Note that the project builds for Scala 2.12 by default but can be built for Scala 2.13 using an additional profile:

```console
make release PROFILES="-Pspark-3.4 -Pscala-2.13"
```

To build Comet from the source distribution on an isolated environment without an access to `github.com` it is necessary to disable `git-commit-id-maven-plugin`, otherwise you will face errors that there is no access to the git during the build process. In that case you may use:

```console
make release-nogit PROFILES="-Pspark-3.4"
```
