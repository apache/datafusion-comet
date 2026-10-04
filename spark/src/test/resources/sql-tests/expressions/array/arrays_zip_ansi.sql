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

-- Spark's generated code stops evaluating the arguments at the first NULL one, so an expression
-- that can only fail after it (here a division by zero under ANSI) must not fail natively either.

-- Config: spark.sql.ansi.enabled=true

statement
CREATE TABLE test_arrays_zip_ansi(id bigint) USING parquet

-- One partition, so the four rows share one file and one batch; with a row per batch the guard
-- would skip the whole batch of a NULL row and never reach the division.
statement
INSERT INTO test_arrays_zip_ansi SELECT id FROM range(0, 4, 1, 1)

-- Even rows have a NULL first argument and a zero divisor in the second.
query expect_native(arrays_zip)
SELECT id, arrays_zip(IF(id % 2 = 0, CAST(NULL AS ARRAY<BIGINT>), array(id)), array(10 div (id % 2))) FROM test_arrays_zip_ansi

-- Spark evaluates the subtree of a CodegenFallback expression through `eval`, even inside
-- generated code; here that is the `filter` that `array_compact` becomes. The interpreted
-- `ArraysZip.eval` evaluates every argument before it looks for a NULL one, so the division
-- raises in Spark, and `arrays_zip` falls back to Spark under such an expression.
query expect_error(DIVIDE_BY_ZERO)
SELECT id, array_compact(arrays_zip(IF(id % 2 = 0, CAST(NULL AS ARRAY<BIGINT>), array(id)), array(10 div (id % 2)))) FROM test_arrays_zip_ansi

query expect_fallback(falls back to Spark where Spark evaluates it without generated code)
SELECT id, array_compact(arrays_zip(array(id), array(id + 1))) FROM test_arrays_zip_ansi

-- An imperative aggregate (`collect_list`) evaluates its input through `eval` under every codegen
-- setting, so the division raises in Spark here too.
query expect_error(DIVIDE_BY_ZERO)
SELECT collect_list(arrays_zip(IF(id % 2 = 0, CAST(NULL AS ARRAY<BIGINT>), array(id)), array(10 div (id % 2)))) FROM test_arrays_zip_ansi

query expect_fallback(falls back to Spark where Spark evaluates it without generated code)
SELECT collect_list(arrays_zip(array(id), array(id + 1))) FROM test_arrays_zip_ansi

-- A Generate whose expressions hold an expression with no generated code (here `transform`) is
-- left out of a whole-stage stage, and then evaluates its generator through `eval`, so the
-- `arrays_zip` under the `explode_outer` evaluates every argument even with codegen on.
query expect_error(DIVIDE_BY_ZERO)
SELECT id, explode_outer(arrays_zip(IF(id % 2 = 0, CAST(NULL AS ARRAY<BIGINT>), array(id)), transform(array(id), x -> 10 div (x % 2)))) FROM test_arrays_zip_ansi
