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
-- A TRY cast that narrows a map key turns an out-of-range key into a null key, and Spark keeps the
-- row. Arrow's map format cannot hold a null key, so these casts must fall back to Spark rather
-- than fail. See #5995. What Spark returns is itself inconsistent between readers: its map holds a
-- null key internally, map_keys shows the null, and the other readers see 0.

statement
CREATE TABLE test_try_cast_map_key(id int, m map<bigint, int>) USING parquet

statement
INSERT INTO test_try_cast_map_key VALUES
  (1, map(1, 10, 9999999999, 20)),
  (2, map(2, 30)),
  (3, map()),
  (4, NULL)

query expect_fallback(map key that can fail)
SELECT id, try_cast(m AS map<int, int>) FROM test_try_cast_map_key ORDER BY id

query expect_fallback(map key that can fail)
SELECT id, map_keys(try_cast(m AS map<int, int>)) FROM test_try_cast_map_key ORDER BY id

query expect_fallback(map key that can fail)
SELECT id, element_at(try_cast(m AS map<int, int>), 0) FROM test_try_cast_map_key ORDER BY id

-- A plain cast wraps the key instead of nulling it, so it stays native
query expect_native(cast)
SELECT id, cast(m AS map<int, int>) FROM test_try_cast_map_key ORDER BY id

statement
CREATE TABLE test_try_cast_map_key_widen(id int, m map<int, int>) USING parquet

statement
INSERT INTO test_try_cast_map_key_widen VALUES (1, map(1, 10, 2147483647, 20)), (2, NULL)

-- A widening key cast cannot fail, so it cannot produce a null key and stays native
query expect_native(try_cast)
SELECT id, try_cast(m AS map<bigint, bigint>) FROM test_try_cast_map_key_widen ORDER BY id

-- Spark counts date to timestamp as an upcast, but it overflows for extreme dates and so can
-- still produce a null key under TRY
statement
CREATE TABLE test_try_cast_map_key_date(id int, m map<date, int>) USING parquet

statement
INSERT INTO test_try_cast_map_key_date VALUES
  (1, map(date_from_unix_date(1), 10, date_from_unix_date(2000000000), 20)),
  (2, NULL)

query expect_fallback(map key that can fail)
SELECT id, map_keys(try_cast(m AS map<timestamp, int>)) FROM test_try_cast_map_key_date ORDER BY id
