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

-- Spark does not evaluate the values of a row whose keys are NULL, so an expression that can
-- only fail there (here a division by zero under ANSI) must not fail natively either.
-- `CometMapExpressionSuite` covers native values; this covers values produced by a lambda the JVM
-- codegen dispatcher evaluates, whose struct result carries a NullType field.

-- Config: spark.sql.ansi.enabled=true

statement
CREATE TABLE test_map_from_arrays_ansi(id bigint) USING parquet

-- One partition, so the four rows share one file and one batch; with a row per batch the guard
-- would skip the whole batch of a NULL-key row and never reach the division.
statement
INSERT INTO test_map_from_arrays_ansi SELECT id FROM range(0, 4, 1, 1)

-- Even rows have NULL keys and a zero divisor; odd rows have both.
query expect_native(map_from_arrays)
SELECT id, map_from_arrays(IF(id % 2 = 0, CAST(NULL AS ARRAY<BIGINT>), array(id)), transform(array(id), x -> named_struct('q', 10 div (x % 2), 'n', NULL))) FROM test_map_from_arrays_ansi
