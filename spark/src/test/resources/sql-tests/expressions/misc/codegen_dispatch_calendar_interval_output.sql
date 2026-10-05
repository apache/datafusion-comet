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

-- The JVM codegen dispatcher writes a calendar interval's microseconds to Arrow as nanoseconds,
-- which overflows a long beyond about 292 years, a range Spark represents
-- (https://github.com/apache/datafusion-comet/issues/5279). An expression that would only
-- run through the dispatcher declines it when its result holds a calendar interval at any
-- nesting depth, so the projection falls back to Spark. The small values fall back as well,
-- since the decision is made on the type.

statement
CREATE TABLE test_interval_output_big(s decimal(18, 6)) USING parquet

statement
INSERT INTO test_interval_output_big VALUES (10000000000.000000), (-10000000000.000000), (NULL)

statement
CREATE TABLE test_interval_output_small(s decimal(18, 6)) USING parquet

statement
INSERT INTO test_interval_output_small VALUES (1.000000), (-2.500001), (NULL)

query expect_fallback(holds a calendar interval)
SELECT arrays_zip(array(make_interval(0, 0, 0, 0, 0, 0, s))) FROM test_interval_output_big

-- An interval inside a struct inside the zipped array.
query expect_fallback(holds a calendar interval)
SELECT arrays_zip(array(named_struct('i', make_interval(0, 0, 0, 0, 0, 0, s)))) FROM test_interval_output_big

-- An interval as a map value inside the zipped array.
query expect_fallback(holds a calendar interval)
SELECT arrays_zip(array(map(1, make_interval(0, 0, 0, 0, 0, 0, s)))) FROM test_interval_output_big

-- A floating-point map key has no native lookup, so these would dispatch.
query expect_fallback(holds a calendar interval)
SELECT element_at(map_from_arrays(array(1.0D), array(make_interval(0, 0, 0, 0, 0, 0, s))), 1.0D)
FROM test_interval_output_big

query expect_fallback(holds a calendar interval)
SELECT map_from_arrays(array(1.0D), array(make_interval(0, 0, 0, 0, 0, 0, s)))[1.0D]
FROM test_interval_output_big

-- A nondeterministic child has no native path, so this would dispatch.
query expect_fallback(holds a calendar interval)
SELECT map_from_arrays(array(monotonically_increasing_id()), array(make_interval(0, 0, 0, 0, 0, 0, s)))
FROM test_interval_output_big

query expect_fallback(holds a calendar interval)
SELECT arrays_zip(array(make_interval(0, 0, 0, 0, 0, 0, s))) FROM test_interval_output_small

query expect_fallback(holds a calendar interval)
SELECT arrays_zip(array(named_struct('i', make_interval(0, 0, 0, 0, 0, 0, s)))) FROM test_interval_output_small

-- Without a calendar interval in the result, the same expression still dispatches.
query expect_dispatch(arrays_zip)
SELECT arrays_zip(array(map(1, s))) FROM test_interval_output_small
