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

-- A nondeterministic argument sends `arrays_zip` through the JVM codegen dispatcher, which
-- compiles the whole call alone. `arrays_zip` skips the arguments after a NULL one, and a map
-- lookup nested in an argument skips its key on a row whose map is NULL. Spark's projection
-- evaluates a subexpression that several of its expressions share for every row, so under ANSI
-- mode it raises on such a row where the dispatched call would not. Such a call falls back to
-- Spark instead.
--
-- `rand(7) * 0.0D` leaves the value it is added to unchanged.

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

-- The shared `1.0D / x` divides by zero on the row whose maps are NULL.
query expect_error(DIVIDE_BY_ZERO)
SELECT arrays_zip(a1, array(element_at(m1, 1.0D / x + rand(7) * 0.0D))),
  arrays_zip(a2, array(element_at(m2, 1.0D / x + rand(7) * 0.0D)))
FROM test_arrays_zip_shared

-- A single call shares nothing, so Spark skips the divide on the NULL map too.
query expect_dispatch(arrays_zip)
SELECT arrays_zip(a1, array(element_at(m1, 1.0D / x + rand(7) * 0.0D))) FROM test_arrays_zip_shared

-- With a divisor of `x + 1.0D`, which is never zero, the divide cannot fail, and the shared key
-- still sends the calls back to Spark.
query expect_fallback(holds a subexpression that the operator's other expressions share)
SELECT arrays_zip(a1, array(element_at(m1, 1.0D / (x + 1.0D) + rand(7) * 0.0D))),
  arrays_zip(a2, array(element_at(m2, 1.0D / (x + 1.0D) + rand(7) * 0.0D)))
FROM test_arrays_zip_shared

-- The shared `1.0D / x` sits directly in the second argument of each call, which `arrays_zip`
-- skips on the row whose first argument is NULL.
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

-- With a divisor that cannot be zero, the shared subexpression still sends the calls back to
-- Spark.
query expect_fallback(holds a subexpression that the operator's other expressions share)
SELECT arrays_zip(a1, array(1.0D / (x + 1.0D) + rand(7) * 0.0D)),
  arrays_zip(a2, array(1.0D / (x + 1.0D) + rand(7) * 0.0D))
FROM test_arrays_zip_shared_null_array
