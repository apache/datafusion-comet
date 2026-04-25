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

# Compatibility Guide

Comet aims to provide consistent results with the version of Apache Spark that is being used.

This guide documents areas where Comet's behavior is known to differ from Spark. Topics are grouped by subsystem:

- **Parquet**: limitations when reading Parquet files (both scan implementations, shared and per-implementation).
- **Floating-point comparison**: NaN and signed-zero handling in comparisons.
- **Regular expressions**: differences between the Rust regexp crate and Java's regex engine.
- **Operators**: operator-level compatibility notes, including window functions and round-robin partitioning.
- **Expressions**: per-expression compatibility notes, including cast.
- **Spark versions**: differences between supported Spark versions, including Spark 4.0 specific gaps.

```{toctree}
:maxdepth: 1

scans
floating-point
regex
operators
expressions/index
spark-versions
```
