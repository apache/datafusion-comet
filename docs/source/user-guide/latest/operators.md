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

# Supported Spark Operators

The following Spark operators are currently replaced with native versions. Query stages that contain any operators
not supported by Comet will fall back to regular Spark execution.

| Operator                | Spark-Compatible? | Compatibility Notes                                                                                                |
| ----------------------- | ----------------- | ------------------------------------------------------------------------------------------------------------------ |
| BatchScanExec           | Yes               | Supports Parquet files and Apache Iceberg Parquet scans. See the [Comet Compatibility Guide] for more information. |
| BroadcastExchangeExec   | Yes               |                                                                                                                    |
| BroadcastHashJoinExec   | Yes               |                                                                                                                    |
| ExpandExec              | Yes               |                                                                                                                    |
| FileSourceScanExec      | Yes               | Supports Parquet files. See the [Comet Compatibility Guide] for more information.                                  |
| FilterExec              | Yes               |                                                                                                                    |
| GlobalLimitExec         | Yes               |                                                                                                                    |
| HashAggregateExec       | Yes               |                                                                                                                    |
| LocalLimitExec          | Yes               |                                                                                                                    |
| ObjectHashAggregateExec | Yes               | Supports a limited number of aggregates, such as `bloom_filter_agg`.                                               |
| ProjectExec             | Yes               |                                                                                                                    |
| ShuffleExchangeExec     | Yes               |                                                                                                                    |
| ShuffledHashJoinExec    | Yes               |                                                                                                                    |
| SortExec                | Yes               |                                                                                                                    |
| SortMergeJoinExec       | Yes               |                                                                                                                    |
| UnionExec               | Yes               |                                                                                                                    |
| WindowExec              | Yes               |                                                                                                                    |

[Comet Compatibility Guide]: compatibility.md
