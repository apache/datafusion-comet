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

-- A lambda body with an expression that the native side cannot evaluate like Spark stays on the
-- dispatcher, which runs it in Spark's own code. An array argument or a body can hold a TRY cast
-- that makes a null map key (#6172), which Arrow cannot represent, and a cast between a date or
-- timestamp and another type fails natively for years beyond chrono's range (#7471).

-- ConfigMatrix: spark.sql.ansi.enabled=true,false

statement
CREATE TABLE test_filter_lambda_unsupported(id int, longs array<bigint>, dates array<date>, tss array<timestamp>) USING parquet

statement
INSERT INTO test_filter_lambda_unsupported VALUES
  (1, array(1L, 9999999999L), array(date_from_unix_date(100000000), DATE '2020-01-01', NULL),
   array(timestamp_micros(9000000000000000000), TIMESTAMP '2020-01-01 00:00:00', NULL)),
  (2, NULL, NULL, NULL),
  (3, array(2L), array(DATE '2024-02-29'), array(TIMESTAMP '2024-02-29 12:34:56'))

-- The key cast makes a null key for 9999999999 under TRY, which Spark's filter drops.
query expect_dispatch(filter)
SELECT id, filter(longs, x -> array_contains(map_keys(try_cast(map_from_arrays(array(x), array(1)) AS MAP<INT,INT>)), 1)) FROM test_filter_lambda_unsupported

query expect_dispatch(exists)
SELECT id, exists(longs, x -> array_contains(map_keys(try_cast(map_from_arrays(array(x), array(1)) AS MAP<INT,INT>)), 1)) FROM test_filter_lambda_unsupported

-- A widening key cast cannot make a null key.
query expect_native(filter)
SELECT id, filter(longs, x -> array_contains(map_keys(cast(map_from_arrays(array(x), array(1)) AS MAP<BIGINT,INT>)), 1L)) FROM test_filter_lambda_unsupported

query expect_dispatch(filter)
SELECT id, filter(dates, x -> CAST(x AS STRING) <> '') FROM test_filter_lambda_unsupported

query expect_dispatch(filter)
SELECT id, filter(dates, x -> year(CAST(x AS TIMESTAMP)) > 2000) FROM test_filter_lambda_unsupported

query expect_dispatch(filter)
SELECT id, filter(tss, x -> year(CAST(x AS DATE)) > 2000) FROM test_filter_lambda_unsupported

query expect_dispatch(exists)
SELECT id, exists(tss, x -> CAST(x AS STRING) <> '') FROM test_filter_lambda_unsupported

-- A cast between a timestamp and a number is not affected.
query expect_native(filter)
SELECT id, filter(tss, x -> CAST(x AS BIGINT) > 0) FROM test_filter_lambda_unsupported
