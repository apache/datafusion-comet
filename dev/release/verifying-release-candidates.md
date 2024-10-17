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

# Verifying DataFusion Comet Release Candidates

The `dev/release/verify-release-candidate.sh` script in this repository can assist in the verification
process. It checks the hashes and runs the build. It does not run the test suite because this takes a long time
for this project and the test suites already run in CI before we create the release candidate, so running them
again is somewhat redundant.

```shell
./dev/release/verify-release-candidate.sh 0.1.0 1
```

The following command can be used to build a release for testing.

```shell
make release-nogit
```

We hope that users will verify the release beyond running this script by testing the release candidate with their
existing Spark jobs and report any functional issues or performance regressions.

The email announcing the vote should contain a link to pre-built jar files in a Maven staging repository.

Another way of verifying the release is to follow the
[Comet Benchmarking Guide](https://datafusion.apache.org/comet/contributor-guide/benchmarking.html) and compare
performance with the previous release.
