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

# Agent Guidelines for Apache DataFusion Comet

Read the [Contributor Guide](docs/source/contributor-guide/index.md) before making changes.
Relevant entry points:

- [Development Guide](docs/source/contributor-guide/development.md): build, test, and common
  pitfalls (including why `-pl` must not be used and the JVM/native build order).
- [Spark SQL Tests](docs/source/contributor-guide/spark-sql-tests.md): the only supported way
  to modify files under `dev/diffs/`. **Never hand-edit a diff file.** Clone Spark, apply the
  existing diff, modify the Spark source, then regenerate the diff as documented there.
- [Adding a New Expression](docs/source/contributor-guide/adding_a_new_expression.md) /
  [Adding a New Operator](docs/source/contributor-guide/adding_a_new_operator.md).
- [Debugging Guide](docs/source/contributor-guide/debugging.md).

When opening a pull request, use the [PR template](.github/pull_request_template.md) and fill
in every section.
