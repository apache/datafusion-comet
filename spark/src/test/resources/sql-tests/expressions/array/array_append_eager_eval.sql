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

-- ArrayAppend is RuntimeReplaceable from Spark 4.0, so CometArrayAppend is only reachable here.
-- MaxSparkVersion: 3.5

-- Spark's generated code for array_append evaluates the item on every row and guards only the
-- assignment of the result, so an item that raises under ANSI raises on a row whose array is NULL
-- as well. https://github.com/apache/datafusion-comet/issues/6086
-- Config: spark.sql.ansi.enabled=true

statement
CREATE TABLE array_append_eager(_1 INT) USING parquet

statement
INSERT INTO array_append_eager VALUES (0), (1), (2), (3)

-- Control: the ANSI divide raises on its own, so the query below is not vacuous
query expect_error(DIVIDE_BY_ZERO)
SELECT _1, 1 / (_1 - 1) AS d FROM array_append_eager

-- At _1 = 1 the array is NULL and the item divides by zero
query expect_error(DIVIDE_BY_ZERO)
SELECT _1, array_append(IF(_1 % 2 = 0, array(1), CAST(NULL AS ARRAY<INT>)), 1 / (_1 - 1)) AS a
FROM array_append_eager

-- The item still raises on a row whose array is not NULL
query expect_error(DIVIDE_BY_ZERO)
SELECT _1, array_append(IF(_1 % 2 = 1, array(1), CAST(NULL AS ARRAY<INT>)), 1 / (_1 - 1)) AS a
FROM array_append_eager

-- An item that does not raise on any row is unaffected, and stays native
query expect_native(array_append)
SELECT _1, array_append(IF(_1 % 2 = 0, array(1), CAST(NULL AS ARRAY<INT>)), 1 / (_1 + 1)) AS a
FROM array_append_eager

-- A column or literal item cannot raise, so it needs no extra evaluation
query expect_native(array_append)
SELECT _1, array_append(IF(_1 % 2 = 0, array(1), CAST(NULL AS ARRAY<INT>)), _1) AS a,
       array_append(IF(_1 % 2 = 0, array(1), CAST(NULL AS ARRAY<INT>)), 5) AS b
FROM array_append_eager

-- A nondeterministic item is declined when the array can be NULL: the native guard would
-- serialize it twice, and each copy would see only a subset of the rows.
query expect_fallback(nondeterministic item)
SELECT _1, array_append(IF(_1 % 2 = 0, array(1), CAST(NULL AS ARRAY<INT>)), CAST(rand(7L) * 3 AS INT)) AS a
FROM array_append_eager
