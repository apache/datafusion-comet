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

-- Config: spark.sql.session.timeZone=UTC
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true
-- ConfigMatrix: parquet.enable.dictionary=false,true

-- bigint column
statement
CREATE TABLE test_ts_seconds_bigint(c0 bigint) USING parquet

statement
INSERT INTO test_ts_seconds_bigint VALUES (0), (1640995200), (-86400), (4102444800), (-2208988800), (NULL)

query expect_native(timestamp_seconds)
SELECT c0, timestamp_seconds(c0) FROM test_ts_seconds_bigint

-- int column
statement
CREATE TABLE test_ts_seconds_int(c0 int) USING parquet

statement
INSERT INTO test_ts_seconds_int VALUES (0), (1640995200), (-86400), (NULL)

query expect_native(timestamp_seconds)
SELECT c0, timestamp_seconds(c0) FROM test_ts_seconds_int

-- double column
statement
CREATE TABLE test_ts_seconds_double(c0 double) USING parquet

statement
INSERT INTO test_ts_seconds_double VALUES (0.0), (1640995200.123), (-86400.5), (NULL)

query expect_native(timestamp_seconds)
SELECT c0, timestamp_seconds(c0) FROM test_ts_seconds_double

-- float column
statement
CREATE TABLE test_ts_seconds_float(c0 float) USING parquet

statement
INSERT INTO test_ts_seconds_float VALUES (0.0), (1.5), (-86400.5), (1640995200.0), (CAST('NaN' AS FLOAT)), (CAST('Infinity' AS FLOAT)), (CAST('-Infinity' AS FLOAT)), (NULL)

query expect_native(timestamp_seconds)
SELECT c0, timestamp_seconds(c0) FROM test_ts_seconds_float

-- tinyint, smallint and decimal have no native implementation and run through the JVM codegen
-- dispatcher, which executes Spark's own doGenCode (issue #5588).

-- tinyint column
statement
CREATE TABLE test_ts_seconds_tinyint(c0 tinyint) USING parquet

statement
INSERT INTO test_ts_seconds_tinyint VALUES (0), (1), (127), (-128), (NULL)

query expect_dispatch(timestamp_seconds)
SELECT c0, timestamp_seconds(c0) FROM test_ts_seconds_tinyint

-- smallint column
statement
CREATE TABLE test_ts_seconds_smallint(c0 smallint) USING parquet

statement
INSERT INTO test_ts_seconds_smallint VALUES (0), (1), (32767), (-32768), (NULL)

query expect_dispatch(timestamp_seconds)
SELECT c0, timestamp_seconds(c0) FROM test_ts_seconds_smallint

-- decimal column, whole seconds
statement
CREATE TABLE test_ts_seconds_dec10_0(c0 decimal(10, 0)) USING parquet

statement
INSERT INTO test_ts_seconds_dec10_0 VALUES (0), (1640995200), (-86400), (9999999999), (-9999999999), (NULL)

query expect_dispatch(timestamp_seconds)
SELECT c0, timestamp_seconds(c0) FROM test_ts_seconds_dec10_0

-- decimal column at exactly microsecond precision
statement
CREATE TABLE test_ts_seconds_dec20_6(c0 decimal(20, 6)) USING parquet

statement
INSERT INTO test_ts_seconds_dec20_6 VALUES (0), (1640995200.123456), (-86400.5), (-0.000001), (0.000001), (4102444800.999999), (-2208988800.000001), (NULL)

query expect_dispatch(timestamp_seconds)
SELECT c0, timestamp_seconds(c0) FROM test_ts_seconds_dec20_6

-- decimal column with a wider scale whose digits past the sixth are zero
statement
CREATE TABLE test_ts_seconds_dec38_18(c0 decimal(38, 18)) USING parquet

statement
INSERT INTO test_ts_seconds_dec38_18 VALUES (0), (1.5), (1640995200.123456), (-86400.000001), (NULL)

query expect_dispatch(timestamp_seconds)
SELECT c0, timestamp_seconds(c0) FROM test_ts_seconds_dec38_18

-- Spark converts decimal input with longValueExact, so a nonzero digit past microsecond precision
-- raises rather than rounds. Spark raises a bare java.lang.ArithmeticException with no error class
-- on 3.4 through 4.1, so the expect_error patterns below match the JDK message.
statement
CREATE TABLE test_ts_seconds_dec20_7(c0 decimal(20, 7)) USING parquet

statement
INSERT INTO test_ts_seconds_dec20_7 VALUES (1.1234567)

query expect_error(Rounding necessary)
SELECT timestamp_seconds(c0) FROM test_ts_seconds_dec20_7

-- and a result outside the long range raises as well
statement
CREATE TABLE test_ts_seconds_dec38_0(c0 decimal(38, 0)) USING parquet

statement
INSERT INTO test_ts_seconds_dec38_0 VALUES (99999999999999999999)

query expect_error(Overflow)
SELECT timestamp_seconds(c0) FROM test_ts_seconds_dec38_0

-- The row that raises must not be evaluated when a conditional does not select it, matching
-- Spark, which never evaluates an unselected branch
statement
CREATE TABLE test_ts_seconds_conditional(c0 decimal(20, 7), k int) USING parquet

statement
INSERT INTO test_ts_seconds_conditional VALUES (1.5, 1), (1.1234567, 2), (NULL, 3)

query expect_dispatch(timestamp_seconds)
SELECT k, CASE WHEN k = 1 THEN timestamp_seconds(c0) END FROM test_ts_seconds_conditional

query expect_dispatch(timestamp_seconds)
SELECT k, CASE WHEN k <> 2 THEN timestamp_seconds(c0) ELSE timestamp('2020-01-01 00:00:00') END FROM test_ts_seconds_conditional

query expect_dispatch(timestamp_seconds)
SELECT k, IF(k = 1, timestamp_seconds(c0), NULL) FROM test_ts_seconds_conditional

query expect_dispatch(timestamp_seconds)
SELECT k, coalesce(timestamp('2020-01-01 00:00:00'), timestamp_seconds(c0)) FROM test_ts_seconds_conditional

-- literal arguments
query expect_dispatch(timestamp_seconds)
SELECT timestamp_seconds(CAST(1.5 AS DECIMAL(10, 1))), timestamp_seconds(CAST(127 AS TINYINT)), timestamp_seconds(CAST(-32768 AS SMALLINT))

query expect_error(Rounding necessary)
SELECT timestamp_seconds(CAST(1.1234567 AS DECIMAL(20, 7)))

query
SELECT timestamp_seconds(0)

query
SELECT timestamp_seconds(1640995200)

-- negative value (before epoch)
query
SELECT timestamp_seconds(-86400)

-- decimal seconds (fractional)
query
SELECT timestamp_seconds(CAST(1640995200.123 AS DOUBLE))

-- null handling
query
SELECT timestamp_seconds(NULL)

-- NaN input (should return null)
query
SELECT timestamp_seconds(CAST('NaN' AS DOUBLE))

-- Infinity input (should return null)
query
SELECT timestamp_seconds(CAST('Infinity' AS DOUBLE))

-- Negative infinity input (should return null)
query
SELECT timestamp_seconds(CAST('-Infinity' AS DOUBLE))
