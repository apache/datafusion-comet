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

-- ANSI edge cases for Boolean -> Decimal. Comet has no native path for this cast; the
-- `CodegenDispatchFallback` mixin runs Spark's own generated code inside the Comet pipeline, so
-- the ANSI overflow errors must match Spark exactly. The non-error queries act as sentinels
-- proving the cast really executed natively rather than falling the whole plan back to Spark.

-- Config: spark.sql.ansi.enabled=true

statement
CREATE TABLE test_cast_bool_to_decimal_ansi(id int, b boolean) USING parquet

statement
INSERT INTO test_cast_bool_to_decimal_ansi VALUES (1, true), (2, false), (3, NULL)

-- sentinel: a cast that always fits must run natively under ANSI mode
query
SELECT id, cast(b as decimal(10,2)), cast(b as decimal(38,0))
FROM test_cast_bool_to_decimal_ansi ORDER BY id

-- sentinel: decimal(1,0) is the tightest type that can hold 1
query
SELECT id, cast(b as decimal(1,0)), cast(b as decimal(2,1))
FROM test_cast_bool_to_decimal_ansi ORDER BY id

-- decimal(1,1) holds at most 0.9, so casting true overflows and must throw under ANSI mode
query expect_error(NUMERIC_VALUE_OUT_OF_RANGE)
SELECT cast(b as decimal(1,1)) FROM test_cast_bool_to_decimal_ansi WHERE id = 1

-- decimal(2,2) holds at most 0.99: same overflow, larger precision
query expect_error(NUMERIC_VALUE_OUT_OF_RANGE)
SELECT cast(b as decimal(2,2)) FROM test_cast_bool_to_decimal_ansi WHERE id = 1

-- all-scale decimal at maximum precision still cannot represent 1
query expect_error(NUMERIC_VALUE_OUT_OF_RANGE)
SELECT cast(b as decimal(38,38)) FROM test_cast_bool_to_decimal_ansi WHERE id = 1

-- literal true overflows the same way
query expect_error(NUMERIC_VALUE_OUT_OF_RANGE)
SELECT cast(true as decimal(1,1))

-- overflow is value dependent: false scales to 0, which fits every decimal type, so rows that
-- only contain false must not throw
query
SELECT cast(b as decimal(1,1)), cast(b as decimal(38,38))
FROM test_cast_bool_to_decimal_ansi WHERE id = 2

-- NULL input short-circuits before the precision check, so it must not throw either
query
SELECT cast(b as decimal(1,1)), cast(b as decimal(38,38))
FROM test_cast_bool_to_decimal_ansi WHERE id = 3

-- try_cast suppresses the ANSI overflow and yields NULL
query
SELECT id, try_cast(b as decimal(1,1)), try_cast(b as decimal(38,38))
FROM test_cast_bool_to_decimal_ansi ORDER BY id

-- overflow raised from inside an aggregate input
query expect_error(NUMERIC_VALUE_OUT_OF_RANGE)
SELECT sum(cast(b as decimal(1,1))) FROM test_cast_bool_to_decimal_ansi

-- overflow raised from inside a filter predicate
query expect_error(NUMERIC_VALUE_OUT_OF_RANGE)
SELECT id FROM test_cast_bool_to_decimal_ansi WHERE cast(b as decimal(1,1)) > 0.5
