-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- `any` is registered in Spark's FunctionRegistry as a SQL alias of `BoolOr`,
-- which extends `RuntimeReplaceableAggregate` with `replacement = Max(child)`.
-- The analyzer rewrites `any(x)`, `some(x)`, and `bool_or(x)` to `max(x)`
-- before Comet sees the plan. These tests exercise the rewrite end-to-end.

statement
CREATE TABLE test_any(k int, v boolean) USING parquet

statement
INSERT INTO test_any VALUES
  (1, true), (1, false),
  (2, true),
  (3, false), (3, NULL),
  (4, NULL), (4, NULL),
  (5, NULL), (5, true), (5, false)

-- column argument, no grouping (mixed true/false/NULL)
query
SELECT any(v), some(v), bool_or(v) FROM test_any

-- column argument with group-by
query
SELECT k, any(v), some(v), bool_or(v) FROM test_any GROUP BY k ORDER BY k

-- all-NULL group (k = 4) should return NULL
query
SELECT any(v), some(v), bool_or(v) FROM test_any WHERE k = 4

-- empty input should return NULL
query
SELECT any(v), some(v), bool_or(v) FROM test_any WHERE 1 = 0

-- literal arguments (constant folding is disabled by CometSqlFileTestSuite,
-- so these are evaluated by the native engine)
query
SELECT any(true), any(false), any(NULL)

-- HAVING on the aggregate
query
SELECT k, any(v) FROM test_any GROUP BY k HAVING any(v) = true ORDER BY k
