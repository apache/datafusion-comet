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

- Handled at the operator level as a `GenerateExec` (`CometExplodeExec`), not via the expression serde maps, so it is not auto-detected by the function-registry checkbox logic. Compatible for array and map inputs. Maps emit key/value columns, with a position column for `posexplode`.

## explode_outer

- Same `CometExplodeExec` path as `explode`. Compatible for array and map inputs; empty and NULL collections both emit one null-valued row per Spark's `outer` semantics via the `ListEmptyToNullExpr` planner bridge (works around [datafusion#19053](https://github.com/apache/datafusion/issues/19053)). Map entries reuse the list unnest path and expand into key/value columns.

## posexplode

- Handled at the operator level as a `GenerateExec` (`CometExplodeExec`), like `explode`. Compatible for array and map inputs. Maps emit key/value columns, with a position column for `posexplode`.

## posexplode_outer

- Same `CometExplodeExec` path as `posexplode`. Compatible for array and map inputs; empty and NULL arrays or maps both emit one row with null `pos` and null generated columns per Spark's `outer` semantics via the `ListEmptyToNullExpr` planner bridge (works around [datafusion#19053](https://github.com/apache/datafusion/issues/19053)).

[Spark Expression Support]: ../../user-guide/latest/expressions.md
