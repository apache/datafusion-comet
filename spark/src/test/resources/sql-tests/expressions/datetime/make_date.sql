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
CREATE TABLE test_make_date(year int, month int, day int) USING parquet

statement
INSERT INTO test_make_date VALUES
  (2023, 12, 25),
  (1970, 1, 1),
  (2000, 6, 15),
  (1999, 12, 31),
  (2024, 2, 29),
  (NULL, 1, 1),
  (2023, NULL, 1),
  (2023, 1, NULL),
  (NULL, NULL, NULL)

-- column arguments
query
SELECT year, month, day, make_date(year, month, day) FROM test_make_date ORDER BY year, month, day

-- literal year, column month and day
query
SELECT make_date(2023, month, day) FROM test_make_date ORDER BY month, day

-- column year, literal month and day
query
SELECT make_date(year, 6, 15) FROM test_make_date ORDER BY year

-- column year and month, literal day
query
SELECT make_date(year, month, 1) FROM test_make_date ORDER BY year, month

-- literal values
query
SELECT make_date(2023, 12, 25)

query
SELECT make_date(1970, 1, 1)

-- null handling with literals
query
SELECT make_date(NULL, 1, 1)

query
SELECT make_date(2023, NULL, 1)

query
SELECT make_date(2023, 1, NULL)

-- leap year edge cases
-- 2000 WAS a leap year (divisible by 400)
query
SELECT make_date(2000, 2, 29)

-- 2004 was a leap year (divisible by 4, not by 100)
query
SELECT make_date(2004, 2, 29)

-- 2023 is NOT a leap year - Feb 29 should return NULL
query
SELECT make_date(2023, 2, 29)

-- 1900 was NOT a leap year (divisible by 100 but not 400) - Feb 29 should return NULL
query
SELECT make_date(1900, 2, 29)

-- 2100 will NOT be a leap year (divisible by 100 but not 400)
query
SELECT make_date(2100, 2, 29)

-- invalid date handling - should return NULL
query
SELECT make_date(2023, 2, 30)

query
SELECT make_date(2023, 2, 31)

query
SELECT make_date(2023, 4, 31)

query
SELECT make_date(2023, 6, 31)

query
SELECT make_date(2023, 9, 31)

query
SELECT make_date(2023, 11, 31)

-- boundary values - invalid month/day values should return NULL
query
SELECT make_date(2023, 0, 15)

query
SELECT make_date(2023, 13, 15)

query
SELECT make_date(2023, -1, 15)

query
SELECT make_date(2023, 6, 0)

query
SELECT make_date(2023, 6, 32)

query
SELECT make_date(2023, 6, -1)

-- extreme years
query
SELECT make_date(1, 1, 1)

query
SELECT make_date(9999, 12, 31)

query
SELECT make_date(0, 1, 1)

query
SELECT make_date(-1, 1, 1)

-- month boundaries - last day of each month
query
SELECT make_date(2023, 1, 31)

query
SELECT make_date(2023, 3, 31)

query
SELECT make_date(2023, 4, 30)

query
SELECT make_date(2023, 5, 31)

query
SELECT make_date(2023, 6, 30)

query
SELECT make_date(2023, 7, 31)

query
SELECT make_date(2023, 8, 31)

query
SELECT make_date(2023, 9, 30)

query
SELECT make_date(2023, 10, 31)

query
SELECT make_date(2023, 11, 30)

query
SELECT make_date(2023, 12, 31)

query
SELECT make_date(2024, 2, 29)

query
SELECT make_date(2023, 2, 28)
