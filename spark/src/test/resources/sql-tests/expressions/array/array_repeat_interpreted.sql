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

-- Spark's generated code for `array_repeat` evaluates the element and the count, but its `eval`
-- evaluates the count first and skips the element when the count is NULL. Beneath an expression
-- with no generated code (here the `filter` that `array_compact` becomes) Spark uses `eval`, so
-- an element that raises an error must not raise on the rows with a NULL count. A nullable count
-- runs through the codegen dispatcher there, which evaluates the tree the same way.

-- Config: spark.sql.ansi.enabled=true

statement
CREATE TABLE test_array_repeat_interpreted(id bigint) USING parquet

-- One partition, so the four rows share one batch.
statement
INSERT INTO test_array_repeat_interpreted SELECT id FROM range(0, 4, 1, 1)

-- Even rows have a NULL count and a zero divisor in the element.
query expect_dispatch(array_repeat)
SELECT id, array_compact(array_repeat(10 div (id % 2), IF(id % 2 = 0, CAST(NULL AS INT), 1))) FROM test_array_repeat_interpreted

-- Outside such an expression Spark uses the generated code, which evaluates the element.
query expect_error(DIVIDE_BY_ZERO)
SELECT id, array_repeat(10 div (id % 2), IF(id % 2 = 0, CAST(NULL AS INT), 1)) FROM test_array_repeat_interpreted

-- An imperative aggregate evaluates its input through `eval`, but its FILTER predicate is
-- generated code, so the element is evaluated there and raises, as in Spark.
query expect_error(DIVIDE_BY_ZERO)
SELECT sort_array(collect_list(id) FILTER (WHERE size(array_repeat(10 div (id % 2), IF(id % 2 = 0, CAST(NULL AS INT), 1))) > 0)) FROM test_array_repeat_interpreted

-- The same shape over valid input, to show the aggregate and its FILTER run natively.
query expect_native(array_repeat)
SELECT sort_array(collect_list(id) FILTER (WHERE size(array_repeat(id, IF(id % 2 = 0, CAST(NULL AS INT), 1))) > 0)) FROM test_array_repeat_interpreted
