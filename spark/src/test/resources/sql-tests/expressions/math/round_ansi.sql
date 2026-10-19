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

-- Config: spark.sql.ansi.enabled=true

statement
CREATE TABLE test_round_ansi(l bigint) USING parquet

statement
INSERT INTO test_round_ansi VALUES
 (-5000000000000000000L), (-4999999999999999999L), (0L),
 (4999999999999999999L), (5000000000000000000L), (NULL)

-- Values just below the half-way boundary round to zero; NULL stays NULL.
query
SELECT l, round(l, -19) FROM test_round_ansi
WHERE l BETWEEN -4999999999999999999L AND 4999999999999999999L OR l IS NULL

-- A larger negative scale rounds even the overflow inputs below to zero.
query
SELECT l, round(l, -20) FROM test_round_ansi

-- At scale -19, +/-5e18 rounds to +/-1e19, which cannot fit in a long (#5070).
query expect_error(ARITHMETIC_OVERFLOW)
SELECT round(l, -19) FROM test_round_ansi WHERE l = 5000000000000000000L

query expect_error(ARITHMETIC_OVERFLOW)
SELECT round(l, -19) FROM test_round_ansi WHERE l = -5000000000000000000L

query expect_error(ARITHMETIC_OVERFLOW)
SELECT round(5000000000000000000L, -19)

query expect_error(ARITHMETIC_OVERFLOW)
SELECT round(-5000000000000000000L, -19)
