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

statement
CREATE TABLE test_round(d double, i int) USING parquet

statement
INSERT INTO test_round VALUES (2.5, 0), (3.5, 0), (-2.5, 0), (123.456, 2), (123.456, -1), (NULL, 0), (cast('NaN' as double), 0), (cast('Infinity' as double), 0), (0.0, 0)

query
SELECT round(d, 0) FROM test_round WHERE i = 0

query
SELECT round(d, 2) FROM test_round WHERE i = 2

query
SELECT round(d, -1) FROM test_round WHERE i = -1

query
SELECT round(d) FROM test_round

-- literal + literal
query
SELECT round(123.456, 2), round(2.5, 0), round(3.5, 0), round(-2.5, 0), round(NULL, 0)

-- HALF_UP semantics: .5 always rounds away from zero
statement
CREATE TABLE test_round_half_up(d double) USING parquet

statement
INSERT INTO test_round_half_up VALUES (0.5), (1.5), (2.5), (-0.5), (-1.5), (-2.5)

query
SELECT d, round(d, 0) FROM test_round_half_up

-- various scales on a single value
query
SELECT round(123.456, 0), round(123.456, 1), round(123.456, 2), round(123.456, 3), round(123.456, 5)

query
SELECT round(123.456, -1), round(123.456, -2), round(123.456, -3)

-- special values
query
SELECT round(cast('NaN' as double), 2), round(cast('Infinity' as double), 2), round(cast('-Infinity' as double), 2)

query
SELECT round(0.0, 5), round(-0.0, 5)

-- very small values
query
SELECT round(1.0E-10, 15), round(1.0E-10, 10), round(1.0E-10, 5)

-- negative scale on doubles
query
SELECT round(9999.9, -1), round(9999.9, -2), round(9999.9, -3), round(9999.9, -4)

query
SELECT round(-9999.9, -1), round(-9999.9, -2), round(-9999.9, -3), round(-9999.9, -4)

-- float type
statement
CREATE TABLE test_round_float(f float) USING parquet

statement
INSERT INTO test_round_float VALUES (cast(2.5 as float)), (cast(3.5 as float)), (cast(-2.5 as float)), (cast(0.125 as float)), (cast(0.785 as float)), (cast(123.456 as float)), (cast('NaN' as float)), (cast('Infinity' as float)), (NULL)

query
SELECT round(f, 0) FROM test_round_float

query
SELECT round(f, 2) FROM test_round_float

query
SELECT round(f, -1) FROM test_round_float

-- BigDecimal rounding edge case from Spark
statement
CREATE TABLE test_round_edge(d double) USING parquet

statement
INSERT INTO test_round_edge VALUES (-5.81855622136895E8, 6.1317116247283497E18, 6.13171162472835E18)

query
SELECT round(d, 4), round(d, 5), round(d, 6) FROM test_round_edge

query
SELECT round(cast('-8316362075006449156' as double), -5)

-- round with column from table (not literals)
query
SELECT d, round(d, 0), round(d, 2), round(d, -1) FROM test_round
