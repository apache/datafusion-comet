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
CREATE TABLE test_datediff(d1 date, d2 date) USING parquet

statement
INSERT INTO test_datediff VALUES (date('2024-01-15'), date('2024-01-10')), (date('2024-01-10'), date('2024-01-15')), (date('2024-01-15'), date('2024-01-15')), (NULL, date('2024-01-15')), (date('2024-01-15'), NULL)

query
SELECT datediff(d1, d2) FROM test_datediff

-- column + literal
query
SELECT datediff(d1, date('2024-01-10')) FROM test_datediff

-- literal + column
query
SELECT datediff(date('2024-01-20'), d2) FROM test_datediff

-- literal + literal
query
SELECT datediff(date('2024-01-15'), date('2024-01-10')), datediff(date('2024-01-10'), date('2024-01-15')), datediff(NULL, date('2024-01-15'))

-- Dates whose day difference overflows a 32-bit int. Spark's DateDiff does plain JVM int
-- subtraction, which wraps; the native path must wrap to match rather than panic in debug builds.
-- date_add materializes day values near +/- 2e9 (2000000000 - (-2000000000) = 4000000000, which
-- wraps to -294967296).
statement
CREATE TABLE test_datediff_overflow(d1 date, d2 date) USING parquet

statement
INSERT INTO test_datediff_overflow VALUES (date_add(date('1970-01-01'), 2000000000), date_add(date('1970-01-01'), -2000000000)), (date_add(date('1970-01-01'), -2000000000), date_add(date('1970-01-01'), 2000000000))

query
SELECT datediff(d1, d2) FROM test_datediff_overflow
