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

-- ANSI casts of NaN and infinities to TIMESTAMP (#7001). Spark raises SparkDateTimeException
-- with CAST_INVALID_INPUT and prints the value as a DOUBLE, also for FLOAT input.

-- Config: spark.sql.ansi.enabled=true

statement
CREATE TABLE test_cast_float_ts_ansi(id int, d double, f float) USING parquet

statement
INSERT INTO test_cast_float_ts_ansi VALUES (1, 1.5, 1.5), (2, double('NaN'), float('NaN')), (3, double('Infinity'), float('Infinity')), (4, double('-Infinity'), float('-Infinity'))

-- sentinel: a finite value casts natively
query
SELECT cast(d AS timestamp), cast(f AS timestamp) FROM test_cast_float_ts_ansi WHERE id = 1

-- try_cast returns NULL for every row
query
SELECT id, try_cast(d AS timestamp), try_cast(f AS timestamp) FROM test_cast_float_ts_ansi ORDER BY id

-- same exception class, error class and SQLSTATE as Spark
query expect_error_class(CAST_INVALID_INPUT)
SELECT cast(d AS timestamp) FROM test_cast_float_ts_ansi WHERE id = 2

query expect_error_class(CAST_INVALID_INPUT)
SELECT cast(f AS timestamp) FROM test_cast_float_ts_ansi WHERE id = 4

-- same message text as Spark
query expect_error(The value Infinity of the type "DOUBLE" cannot be cast to "TIMESTAMP")
SELECT cast(d AS timestamp) FROM test_cast_float_ts_ansi WHERE id = 3

query expect_error(The value NaN of the type "DOUBLE" cannot be cast to "TIMESTAMP")
SELECT cast(f AS timestamp) FROM test_cast_float_ts_ansi WHERE id = 2

query expect_error(The value -Infinity of the type "DOUBLE" cannot be cast to "TIMESTAMP")
SELECT cast(d AS timestamp) FROM test_cast_float_ts_ansi WHERE id = 4

query expect_error(The value Infinity of the type "DOUBLE" cannot be cast to "TIMESTAMP")
SELECT cast(f AS timestamp) FROM test_cast_float_ts_ansi WHERE id = 3
