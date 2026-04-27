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

-- ConfigMatrix: parquet.enable.dictionary=false,true

statement
CREATE TABLE test_cast_decimal(d10 decimal(10,2), d5 decimal(5,0)) USING parquet

statement
INSERT INTO test_cast_decimal VALUES
  (123.45, 123),
  (-67.89, -67),
  (0.00, 0),
  (0.01, 1),
  (-0.01, -1),
  (99999999.99, 99999),
  (-99999999.99, -99999),
  (NULL, NULL)

-- decimal(10,2) column to FLOAT
query
SELECT cast(d10 as float) FROM test_cast_decimal

-- decimal(10,2) column to DOUBLE
query
SELECT cast(d10 as double) FROM test_cast_decimal

-- decimal(10,2) column to INT
query
SELECT cast(d10 as int) FROM test_cast_decimal

-- decimal(10,2) column to LONG
query
SELECT cast(d10 as long) FROM test_cast_decimal

-- decimal(10,2) column to BOOLEAN
query
SELECT cast(d10 as boolean) FROM test_cast_decimal

-- decimal(5,0) column to FLOAT
query
SELECT cast(d5 as float) FROM test_cast_decimal

-- decimal(5,0) column to DOUBLE
query
SELECT cast(d5 as double) FROM test_cast_decimal

-- decimal(5,0) column to INT
query
SELECT cast(d5 as int) FROM test_cast_decimal

-- decimal(5,0) column to LONG
query
SELECT cast(d5 as long) FROM test_cast_decimal

-- decimal(5,0) column to BOOLEAN
query
SELECT cast(d5 as boolean) FROM test_cast_decimal

-- decimal(38,18) table: covers boundary values that exercise the i128 code path
statement
CREATE TABLE test_cast_decimal_high_precision(d38 decimal(38,18)) USING parquet

statement
INSERT INTO test_cast_decimal_high_precision VALUES
  (CAST('99999999999999999999.999999999999999999' AS decimal(38,18))),
  (CAST('-99999999999999999999.999999999999999999' AS decimal(38,18))),
  (CAST('9223372036854775807.000000000000000000' AS decimal(38,18))),
  (CAST('-9223372036854775808.000000000000000000' AS decimal(38,18))),
  (CAST('1.000000000000000000' AS decimal(38,18))),
  (CAST('-1.000000000000000000' AS decimal(38,18))),
  (CAST('0.000000000000000000' AS decimal(38,18))),
  (NULL)

-- decimal(38,18) column to FLOAT
query
SELECT cast(d38 as float) FROM test_cast_decimal_high_precision

-- decimal(38,18) column to DOUBLE
query
SELECT cast(d38 as double) FROM test_cast_decimal_high_precision

-- decimal(38,18) column to INT
query
SELECT cast(d38 as int) FROM test_cast_decimal_high_precision

-- decimal(38,18) column to LONG
query
SELECT cast(d38 as long) FROM test_cast_decimal_high_precision

-- decimal(38,18) column to BOOLEAN
query
SELECT cast(d38 as boolean) FROM test_cast_decimal_high_precision

-- additional precision/scale combinations: decimal(15,5) has fractional part with int overflow
-- possible; decimal(20,0) has no fractional part with long overflow possible
statement
CREATE TABLE test_cast_decimal_extra(
  d15_5 decimal(15,5),
  d20_0 decimal(20,0)
) USING parquet

statement
INSERT INTO test_cast_decimal_extra VALUES
  (2147483648.12345, 9223372036854775808),    -- d15_5 overflows INT; d20_0 overflows LONG
  (-2147483649.12345, -9223372036854775809),
  (123.45678, 2147483648),                    -- fractional truncation; d20_0 overflows INT only
  (0.00001, 1),
  (-0.00001, -1),
  (0.00000, 0),
  (NULL, NULL)

-- decimal(15,5) to INT (exercises fractional truncation and int overflow)
query
SELECT cast(d15_5 as int) FROM test_cast_decimal_extra

-- decimal(15,5) to LONG
query
SELECT cast(d15_5 as long) FROM test_cast_decimal_extra

-- decimal(15,5) to BOOLEAN
query
SELECT cast(d15_5 as boolean) FROM test_cast_decimal_extra

-- decimal(20,0) to INT
query
SELECT cast(d20_0 as int) FROM test_cast_decimal_extra

-- decimal(20,0) to LONG (exercises long overflow)
query
SELECT cast(d20_0 as long) FROM test_cast_decimal_extra

-- decimal(20,0) to BOOLEAN
query
SELECT cast(d20_0 as boolean) FROM test_cast_decimal_extra

-- literal casts: decimal(10,2) to float
-- IgnoreFromSparkVersion: 4.1 https://github.com/apache/datafusion-comet/issues/4098
query
SELECT cast(cast(1.50 as decimal(10,2)) as float),
       cast(cast(0.00 as decimal(10,2)) as float),
       cast(cast(-1.50 as decimal(10,2)) as float),
       cast(cast(NULL as decimal(10,2)) as float)

-- literal casts: decimal(5,0) to float
query
SELECT cast(cast(123 as decimal(5,0)) as float),
       cast(cast(0 as decimal(5,0)) as float),
       cast(cast(-123 as decimal(5,0)) as float),
       cast(cast(NULL as decimal(5,0)) as float)

-- literal casts: decimal(10,2) to boolean
query
SELECT cast(cast(1.50 as decimal(10,2)) as boolean),
       cast(cast(0.00 as decimal(10,2)) as boolean),
       cast(cast(NULL as decimal(10,2)) as boolean)

-- literal casts: decimal(5,0) to boolean
query
SELECT cast(cast(1 as decimal(5,0)) as boolean),
       cast(cast(0 as decimal(5,0)) as boolean),
       cast(cast(NULL as decimal(5,0)) as boolean)
