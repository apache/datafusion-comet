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

-- Without generated code, Spark evaluates `arrays_zip` through its interpreted `eval`, which
-- evaluates every argument before it looks for a NULL one. The native null guard follows the
-- generated code, which stops at the first NULL argument, so `arrays_zip` over several
-- arguments falls back to Spark here; otherwise the division below would raise in Spark only.

-- Config: spark.sql.ansi.enabled=true
-- Config: spark.sql.codegen.wholeStage=false
-- Config: spark.sql.codegen.factoryMode=NO_CODEGEN

statement
CREATE TABLE test_arrays_zip_interpreted(id bigint) USING parquet

-- One partition, so the four rows share one batch.
statement
INSERT INTO test_arrays_zip_interpreted SELECT id FROM range(0, 4, 1, 1)

-- Even rows have a NULL first argument and a zero divisor in the second.
query expect_error(DIVIDE_BY_ZERO)
SELECT id, arrays_zip(IF(id % 2 = 0, CAST(NULL AS ARRAY<BIGINT>), array(id)), array(10 div (id % 2))) FROM test_arrays_zip_interpreted

query expect_fallback(falls back to Spark where Spark evaluates it without generated code)
SELECT id, arrays_zip(array(id), array(id + 1)) FROM test_arrays_zip_interpreted

-- A single argument has nothing to skip, so it stays native.
query expect_native(arrays_zip)
SELECT id, arrays_zip(array(id)) FROM test_arrays_zip_interpreted

-- A NullType-bearing map runs through the codegen dispatcher as a whole, so the `arrays_zip`
-- inside it never reaches its own gate. The dispatcher's kernel evaluates the tree through
-- `eval` here, as Spark does, so the division raises in both.
query expect_error(DIVIDE_BY_ZERO)
SELECT id, map(0, arrays_zip(IF(id % 2 = 0, CAST(NULL AS ARRAY<BIGINT>), array(id)), array(10 div (id % 2)), array(NULL))) FROM test_arrays_zip_interpreted

-- The same shape over valid input, to show the map really is dispatched.
query expect_dispatch(map)
SELECT id, map(0, arrays_zip(IF(id % 2 = 0, CAST(NULL AS ARRAY<BIGINT>), array(id)), array(id), array(NULL))) FROM test_arrays_zip_interpreted
