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

# Data Source Audits

This section records audits of Comet's native data source paths, the scans and writers for Parquet
and Iceberg, in the same way that the [Expression Audits](../expression-audits/index.md) record
audits of expressions. Each page holds dated entries per component. An entry records:

- what was audited, at which commit, against which Spark and library versions, and how
- the issues the audit filed, and the open issues it confirmed
- what was checked and matched, so that it is not re-audited without a reason
- the candidates that did not reproduce, so that they are not chased again
- the differences that are bugs in Spark or iceberg-java rather than in Comet
- what the audit did not cover

A later audit of the same area adds a new dated entry instead of editing an old one, so the history
stays visible. The open issues that users can hit are listed in the user guide, in
[Known Issues in Parquet and Iceberg Reads and Writes](../../user-guide/latest/compatibility/parquet-iceberg-known-issues.md).

```{toctree}
:maxdepth: 1

Parquet <parquet>
Iceberg <iceberg>
```
