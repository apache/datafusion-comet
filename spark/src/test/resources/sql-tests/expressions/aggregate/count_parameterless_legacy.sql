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

-- When `spark.sql.legacy.allowParameterlessCount=true`, Spark accepts `count()` (no arguments)
-- and treats it as `count(*)`. Comet's native planner asserts non-empty children on Count, so
-- `CometCount.getSupportLevel` marks parameterless Count `Unsupported` and lets the aggregate
-- fall back to Spark. Aggregate serdes do not have a JVM codegen dispatcher path, so the
-- Spark fallback is the correct outcome.

-- Config: spark.sql.legacy.allowParameterlessCount=true

statement
CREATE TABLE test_count_parameterless(i int, grp string) USING parquet

statement
INSERT INTO test_count_parameterless VALUES (1, 'x'), (2, 'x'), (NULL, 'y'), (3, 'y'), (NULL, 'y')

-- Parameterless count() falls back to Spark; the aggregate result must still match Spark.
query expect_fallback(spark.sql.legacy.allowParameterlessCount=true)
SELECT count() FROM test_count_parameterless

-- Parameterless count() with GROUP BY.
query expect_fallback(spark.sql.legacy.allowParameterlessCount=true)
SELECT grp, count() FROM test_count_parameterless GROUP BY grp ORDER BY grp

-- Parameterless count() on empty table.
statement
CREATE TABLE test_count_empty(i int) USING parquet

query expect_fallback(spark.sql.legacy.allowParameterlessCount=true)
SELECT count() FROM test_count_empty
