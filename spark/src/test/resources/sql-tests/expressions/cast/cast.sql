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
CREATE TABLE test_cast(i int, l long, f float, d double, s string, b boolean) USING parquet

statement
INSERT INTO test_cast VALUES (1, 1, 1.5, 1.5, '123', true), (0, 0, 0.0, 0.0, '0', false), (NULL, NULL, NULL, NULL, NULL, NULL), (-1, -1, -1.5, -1.5, '-1', true), (2147483647, 9223372036854775807, cast('NaN' as float), cast('Infinity' as double), 'abc', false)

query
SELECT cast(i as long), cast(i as double), cast(i as string) FROM test_cast

query
SELECT cast(l as int), cast(l as double), cast(l as string) FROM test_cast

query
SELECT cast(f as double), cast(f as int), cast(f as string) FROM test_cast

query
SELECT cast(d as float), cast(d as int), cast(d as string) FROM test_cast

query
SELECT cast(s as int), cast(s as double) FROM test_cast

query
SELECT cast(b as int), cast(b as string), cast(i as boolean) FROM test_cast

-- literal arguments
query
SELECT cast(1 as long), cast(1 as double), cast(1 as string), cast('123' as int), cast('3.14' as double), cast(true as int), cast(NULL as int)

-- string to timestamp: common formats
query
SELECT cast('2020-01-01' as timestamp)

query
SELECT cast('2020-01-01 12:34:56' as timestamp)

query
SELECT cast('2020-01-01T12:34:56' as timestamp)

query
SELECT cast('2020-01-01T12:34:56.123456' as timestamp)

-- string to timestamp: Z and offset suffixes
query
SELECT cast('2020-01-01T12:34:56Z' as timestamp)

query
SELECT cast('2020-01-01T12:34:56+05:30' as timestamp)

query
SELECT cast('2020-01-01T12:34:56-07:00' as timestamp)

-- string to timestamp: space separator and UTC/GMT zone suffixes
query
SELECT cast('2020-01-01 12:34:56 UTC' as timestamp)

query
SELECT cast('2020-01-01 12:34:56GMT+08:00' as timestamp)

-- string to timestamp: time-only and T-prefixed time-only
query
SELECT cast('T12:34:56' as timestamp)

-- string to timestamp: year-only and year-month
query
SELECT cast('2020' as timestamp)

query
SELECT cast('2020-06' as timestamp)

-- string to timestamp: negative year
query
SELECT cast('-0100-01-01' as timestamp)

-- string to timestamp: invalid values return null (non-ANSI)
query
SELECT cast('not-a-timestamp' as timestamp)

query
SELECT cast('2020-13-01' as timestamp)

query
SELECT cast('2020-01-32' as timestamp)
