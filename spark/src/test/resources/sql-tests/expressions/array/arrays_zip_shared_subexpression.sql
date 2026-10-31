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

-- A nondeterministic argument forces the dispatcher, which compiles each call alone and so can
-- skip a subexpression that Spark's projection shares. `rand(7) * 0.0D` changes no value.

-- Config: spark.sql.ansi.enabled=true
-- Config: spark.sql.subexpressionElimination.enabled=true

-- On the last row both `1.0D / x` and `1.0D / (x + 1.0D)` find a key.
statement
CREATE TABLE test_arrays_zip_shared(
  a1 array<int>, a2 array<int>, m1 map<double, int>, m2 map<double, int>, x double)
USING parquet

statement
INSERT INTO test_arrays_zip_shared VALUES
  (array(1), array(2), CAST(NULL AS MAP<DOUBLE, INT>), CAST(NULL AS MAP<DOUBLE, INT>), 0.0D),
  (array(3), array(4), map(0.5D, 5), map(0.5D, 6), 2.0D),
  (array(5), array(6), map(1.0D, 7, 0.5D, 8), map(1.0D, 9, 0.5D, 10), 1.0D)

query expect_error(DIVIDE_BY_ZERO)
SELECT arrays_zip(a1, array(element_at(m1, 1.0D / x + rand(7) * 0.0D))),
  arrays_zip(a2, array(element_at(m2, 1.0D / x + rand(7) * 0.0D)))
FROM test_arrays_zip_shared

-- A single call shares nothing, so Spark skips the divide on the NULL map too.
query expect_dispatch(arrays_zip)
SELECT arrays_zip(a1, array(element_at(m1, 1.0D / x + rand(7) * 0.0D))) FROM test_arrays_zip_shared

-- `x + 1.0D` is never zero, yet the shared key still declines the dispatcher
query expect_fallback(holds a subexpression that the operator's other expressions share)
SELECT arrays_zip(a1, array(element_at(m1, 1.0D / (x + 1.0D) + rand(7) * 0.0D))),
  arrays_zip(a2, array(element_at(m2, 1.0D / (x + 1.0D) + rand(7) * 0.0D)))
FROM test_arrays_zip_shared

-- `arrays_zip` skips its second argument, the shared `1.0D / x`, when the first is NULL
statement
CREATE TABLE test_arrays_zip_shared_null_array(a1 array<int>, a2 array<int>, x double)
USING parquet

statement
INSERT INTO test_arrays_zip_shared_null_array VALUES
  (CAST(NULL AS ARRAY<INT>), CAST(NULL AS ARRAY<INT>), 0.0D),
  (array(3), array(4), 2.0D)

query expect_error(DIVIDE_BY_ZERO)
SELECT arrays_zip(a1, array(1.0D / x + rand(7) * 0.0D)),
  arrays_zip(a2, array(1.0D / x + rand(7) * 0.0D))
FROM test_arrays_zip_shared_null_array

query expect_dispatch(arrays_zip)
SELECT arrays_zip(a1, array(1.0D / x + rand(7) * 0.0D)) FROM test_arrays_zip_shared_null_array

query expect_fallback(holds a subexpression that the operator's other expressions share)
SELECT arrays_zip(a1, array(1.0D / (x + 1.0D) + rand(7) * 0.0D)),
  arrays_zip(a2, array(1.0D / (x + 1.0D) + rand(7) * 0.0D))
FROM test_arrays_zip_shared_null_array
