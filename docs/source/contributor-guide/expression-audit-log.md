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

# Expression Audit Log

This document tracks which Comet expressions have been audited against their Spark
implementations for correctness and test coverage.

Each audit compares the Comet implementation against the Spark source code for the listed
versions, reviews existing test coverage, identifies gaps, and adds missing tests where needed.

## Audited Expressions

| Expression     | Spark Versions Checked | Date       | Findings                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| -------------- | ---------------------- | ---------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `array_insert` | 3.4.3, 3.5.8, 4.0.1    | 2026-04-02 | No behavioral differences across Spark versions. Fixed `nullable()` metadata bug (did not account for `pos_expr`). Added SQL tests for multiple types (string, boolean, double, float, long, short, byte), literal arguments, null handling, negative indices, out-of-bounds padding, special float values (NaN, Infinity), multibyte UTF-8, and legacy negative index mode. Known incompatibility: pos=0 error message differs from Spark's `INVALID_INDEX_OF_ZERO`. |
