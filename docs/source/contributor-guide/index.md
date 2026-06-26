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

# Comet Contributor Guide

The Comet contributor guide is for developers working on Comet itself. It covers the project
architecture, the JVM and native code layout, the Arrow FFI bridge, JVM and native shuffle, and
how data and plans flow between Spark and the DataFusion execution engine.

It also documents day-to-day workflows including building and testing locally, debugging,
benchmarking, profiling, tracing, running the SQL test suites, adding new operators and
expressions, triaging bugs, and the Comet release process.

New contributors should start with the Getting Started page. Select a topic from the navigation
menu to read more.

```{toctree}
:maxdepth: 2
:caption: Getting Started
:hidden:

Getting Started <contributing>
Development Guide <development>
```

```{toctree}
:maxdepth: 2
:caption: Project Architecture
:hidden:

Comet Plugin Overview <plugin_overview>
Arrow FFI <ffi>
JVM Shuffle <jvm_shuffle>
Native Shuffle <native_shuffle>
ANSI Error Propagation <sql_error_propagation>
S3 Credential Provider Design <s3-credential-provider-design>
```

```{toctree}
:maxdepth: 2
:caption: Adding Functionality
:hidden:

Adding a New Operator <adding_a_new_operator>
Adding a New Expression <adding_a_new_expression>
Adding a New Spark Version <adding_a_new_spark_version>
```

```{toctree}
:maxdepth: 2
:caption: Testing
:hidden:

Comet SQL Tests <sql-file-tests.md>
Spark SQL Tests <spark-sql-tests.md>
Iceberg Spark Tests <iceberg-spark-tests.md>
```

```{toctree}
:maxdepth: 2
:caption: Debugging and Performance
:hidden:

Debugging Guide <debugging>
Benchmarking Guide <benchmarking>
Profiling <profiling>
Tracing <tracing>
```

```{toctree}
:maxdepth: 2
:caption: Reference
:hidden:

Expression Audits <expression-audits/index>
Supported Spark Configurations <spark_configs_support>
```

```{toctree}
:maxdepth: 2
:caption: Project Mechanics
:hidden:

Bug Triage <bug_triage>
Release Process <release_process>
Roadmap <roadmap.md>
Github and Issue Tracker <https://github.com/apache/datafusion-comet>
```
