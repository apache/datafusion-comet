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

-- Outside a whole-stage stage, GenerateExec evaluates its generator through `eval`, and so the
-- `arrays_zip` under it; projections still use generated code.

-- Config: spark.sql.ansi.enabled=true
-- Config: spark.sql.codegen.wholeStage=false

statement
CREATE TABLE test_arrays_zip_generate(id bigint) USING parquet

-- One partition, so the four rows share one batch.
statement
INSERT INTO test_arrays_zip_generate SELECT id FROM range(0, 4, 1, 1)

-- Even rows have a NULL first argument and a zero divisor in the second.
query expect_error(DIVIDE_BY_ZERO)
SELECT id, explode(arrays_zip(IF(id % 2 = 0, CAST(NULL AS ARRAY<BIGINT>), array(id)), array(10 div (id % 2)))) FROM test_arrays_zip_generate

query expect_fallback(falls back to Spark where Spark evaluates it without generated code)
SELECT id, explode(arrays_zip(array(id), array(id + 1))) FROM test_arrays_zip_generate

-- A projection is still generated code without whole-stage codegen, so it stays native and the
-- division is skipped on the rows with a NULL first argument, as in Spark.
query expect_native(arrays_zip)
SELECT id, arrays_zip(IF(id % 2 = 0, CAST(NULL AS ARRAY<BIGINT>), array(id)), array(10 div (id % 2))) FROM test_arrays_zip_generate
