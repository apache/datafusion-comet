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

-- The dispatcher's interval conversion overflows past about 292 years (#5279), so a would-be
-- dispatched expression whose result holds a calendar interval falls back to Spark.

statement
CREATE TABLE test_interval_output_big(s decimal(18, 6)) USING parquet

statement
INSERT INTO test_interval_output_big VALUES (10000000000.000000), (-10000000000.000000), (NULL)

query expect_fallback(holds a calendar interval)
SELECT arrays_zip(array(make_interval(0, 0, 0, 0, 0, 0, s))) FROM test_interval_output_big

query expect_fallback(holds a calendar interval)
SELECT arrays_zip(array(named_struct('i', make_interval(0, 0, 0, 0, 0, 0, s)))) FROM test_interval_output_big

query expect_fallback(holds a calendar interval)
SELECT arrays_zip(array(map(1, make_interval(0, 0, 0, 0, 0, 0, s)))) FROM test_interval_output_big

-- A floating-point map key has no native lookup, so these would dispatch.
query expect_fallback(holds a calendar interval)
SELECT element_at(map_from_arrays(array(1.0D), array(make_interval(0, 0, 0, 0, 0, 0, s))), 1.0D)
FROM test_interval_output_big

query expect_fallback(holds a calendar interval)
SELECT map_from_arrays(array(1.0D), array(make_interval(0, 0, 0, 0, 0, 0, s)))[1.0D]
FROM test_interval_output_big

-- Native map construction compares floating-point keys by bits, so this would dispatch.
query expect_fallback(holds a calendar interval)
SELECT map_from_arrays(array(1.0D), array(make_interval(0, 0, 0, 0, 0, 0, s)))
FROM test_interval_output_big

-- A nondeterministic child has no native path, so this would dispatch.
query expect_fallback(holds a calendar interval)
SELECT map_from_arrays(array(monotonically_increasing_id()), array(make_interval(0, 0, 0, 0, 0, 0, s)))
FROM test_interval_output_big

query expect_dispatch(arrays_zip)
SELECT arrays_zip(array(map(1, s))) FROM test_interval_output_big

-- Under LAST_WIN `map_from_arrays` has no native path for any key type, so this would dispatch.
statement
SET spark.sql.mapKeyDedupPolicy=LAST_WIN

query expect_fallback(holds a calendar interval)
SELECT map_from_arrays(array('a'), array(make_interval(0, 0, 0, 0, 0, 0, s)))
FROM test_interval_output_big
