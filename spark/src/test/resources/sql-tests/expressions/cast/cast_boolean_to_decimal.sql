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

-- Boolean -> Decimal has no native path in Comet. `CometCast` reports it as unsupported so the
-- `CodegenDispatchFallback` mixin routes it through Spark's own generated code inside the Comet
-- pipeline. The default `query` mode therefore still asserts fully native execution.

statement
CREATE TABLE test_cast_bool_to_decimal(id int, b boolean) USING parquet

statement
INSERT INTO test_cast_bool_to_decimal VALUES (1, true), (2, false), (3, NULL)

-- basic precision/scale combinations, including the long fast path (precision <= 18) and the
-- BigDecimal path (precision > 18)
query
SELECT id, cast(b as decimal(10,2)), cast(b as decimal(14,4)), cast(b as decimal(30,0))
FROM test_cast_bool_to_decimal ORDER BY id

-- smallest precision/scale that can represent 1
query
SELECT id, cast(b as decimal(1,0)), cast(b as decimal(2,1))
FROM test_cast_bool_to_decimal ORDER BY id

-- maximum precision, with and without scale
query
SELECT id, cast(b as decimal(38,0)), cast(b as decimal(38,37))
FROM test_cast_bool_to_decimal ORDER BY id

-- overflow: decimal(1,1) holds at most 0.9, so true does not fit and returns NULL in non-ANSI
-- mode while false and NULL are unaffected
query
SELECT id, cast(b as decimal(1,1)), cast(b as decimal(2,2)), cast(b as decimal(38,38))
FROM test_cast_bool_to_decimal ORDER BY id

-- literal arguments
query
SELECT cast(true as decimal(10,2)), cast(false as decimal(10,2)),
       cast(cast(NULL as boolean) as decimal(10,2))

-- literal overflow
query
SELECT cast(true as decimal(1,1)), cast(false as decimal(1,1))

-- try_cast returns NULL on overflow rather than throwing
query
SELECT id, try_cast(b as decimal(10,2)), try_cast(b as decimal(1,1))
FROM test_cast_bool_to_decimal ORDER BY id

-- comparison predicates on the result of the cast
query
SELECT id FROM test_cast_bool_to_decimal
WHERE cast(b as decimal(10,2)) > 0.5 ORDER BY id

-- the cast feeding an aggregate
query
SELECT sum(cast(b as decimal(10,2))), count(cast(b as decimal(1,1)))
FROM test_cast_bool_to_decimal
