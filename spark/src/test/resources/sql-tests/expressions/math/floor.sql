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
CREATE TABLE test_floor(f float, d double, dec DECIMAL(5, 2)) USING parquet

statement
INSERT INTO test_floor VALUES (1.9, 1.9, 1.90), (-1.1, -1.1, -1.10), (0.0, 0.0, 0.00), (1.0, 1.0, 1.00), (NULL, NULL, NULL), (cast('NaN' as float), cast('NaN' as double), NULL), (cast('Infinity' as float), cast('Infinity' as double), NULL)

query
SELECT floor(f), floor(d), floor(dec) FROM test_floor

-- literal arguments
query
SELECT floor(1.9), floor(-1.1), floor(0.0), floor(NULL), floor(cast(1.90 as DECIMAL(5, 2))), floor(cast(-1.10 as DECIMAL(5, 2)))

-- bigint and wide decimals. Unscaled DECIMAL(38, 6) values cross i64::MAX and i64::MIN, and
-- DECIMAL(38, 19) has a divisor that does not fit in an i64
statement
CREATE TABLE test_floor_wide(l bigint, d6 DECIMAL(38, 6), d19 DECIMAL(38, 19)) USING parquet

statement
INSERT INTO test_floor_wide VALUES (9223372036854775807, 9223372036854.775807, 2000.0000000000000000000), (-9223372036854775808, 9223372036854.775808, 2000.0000000000000000001), (0, -9223372036854.775808, -2000.0000000000000000001), (1, -9223372036854.775809, 9999999999999999999.9999999999999999999), (-1, 20000000000000.000001, -9999999999999999999.9999999999999999999), (NULL, -20000000000000.000001, 0.0000000000000000001), (42, 99999999999999999999999999999999.999999, -0.0000000000000000001), (-42, -99999999999999999999999999999999.999999, NULL)

query
SELECT floor(l), floor(d6), floor(d19) FROM test_floor_wide
