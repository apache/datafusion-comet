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

-- ANSI mode with a raising subtree inside the struct: `to_csv` routes through the codegen
-- dispatcher by default, and the whole tree (StructsToCsv over CreateNamedStruct over Cast) is
-- bound into one kernel, so the Cast's ANSI throw has to survive the dispatcher rather than being
-- swallowed into a NULL. That is the failure mode #5219 fixed for `CometBatchKernelCodegen`'s null
-- short-circuit, which skipped a subtree Spark would have evaluated when every input ordinal was
-- tested up front.
--
-- `to_csv` does not reach the short-circuit today (CreateNamedStruct is not NullIntolerant, so
-- `allNullIntolerant` already fails), but the shape is one wire-up change away from mattering and
-- was untested for this expression, so these queries pin it. The second query is the exact #5219
-- shape: two input ordinals where the non-cast one is NULL on the row whose cast raises.
-- Config: spark.sql.ansi.enabled=true
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true

statement
CREATE TABLE test_to_csv_ansi(s string, i int) USING parquet

statement
INSERT INTO test_to_csv_ansi VALUES
  ('1', 10),
  ('notanint', NULL)

query expect_error(CAST_INVALID_INPUT)
SELECT to_csv(named_struct('a', CAST(s AS INT))) FROM test_to_csv_ansi

query expect_error(CAST_INVALID_INPUT)
SELECT to_csv(named_struct('a', CAST(s AS INT), 'b', i)) FROM test_to_csv_ansi

-- The struct field's cast is the only raising node, so restricting to the well-formed row makes
-- the expression succeed. Doubles as the sentinel required by `ExpectError` fixtures: it uses
-- checkSparkAnswerAndOperator, so a silent dispatcher rejection (which would make the queries
-- above pass vacuously via a Spark fallback that raises the same error) fails here instead.
query
SELECT to_csv(named_struct('a', CAST(s AS INT), 'b', i)) FROM test_to_csv_ansi WHERE i IS NOT NULL

-- try_cast does not raise under ANSI, so the surrounding to_csv must render the NULL field with
-- the CSV nullValue rather than propagating an error.
query
SELECT to_csv(named_struct('a', TRY_CAST(s AS INT), 'b', i)) FROM test_to_csv_ansi
