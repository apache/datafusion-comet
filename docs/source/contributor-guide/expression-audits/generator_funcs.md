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

# generator_funcs Expression Audits

> Audit notes for expressions in this category that have been audited. Absence of an entry means the expression has not been audited yet, not that it is unsupported. See the user guide [Spark Expression Support] for current support status.

## explode

- Handled at the operator level as a `GenerateExec` (`CometExplodeExec`), not via the expression serde maps, so it is not auto-detected by the function-registry checkbox logic. Compatible for array inputs; map inputs fall back ([#2837](https://github.com/apache/datafusion-comet/issues/2837)).

## explode_outer

- Same `CometExplodeExec` path as `explode`, but the `outer=true` case is `Incompatible` (empty arrays are not preserved as null outputs) and falls back unless `spark.comet.expr.allowIncompatible=true` ([datafusion#19053](https://github.com/apache/datafusion/issues/19053)).

## posexplode

- Handled at the operator level as a `GenerateExec` (`CometExplodeExec`), like `explode`. Compatible for array inputs; map inputs fall back ([#2837](https://github.com/apache/datafusion-comet/issues/2837)).

## posexplode_outer

- Same `CometExplodeExec` path as `posexplode`, but the `outer=true` case is `Incompatible` and falls back unless `spark.comet.expr.allowIncompatible=true` ([datafusion#19053](https://github.com/apache/datafusion/issues/19053)).

[Spark Expression Support]: ../../user-guide/latest/expressions.md
