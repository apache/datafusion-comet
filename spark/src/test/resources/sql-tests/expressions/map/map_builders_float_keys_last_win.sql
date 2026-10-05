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

-- MinSparkVersion: 4.0
-- Config: spark.sql.mapKeyDedupPolicy=LAST_WIN

-- From Spark 4.0 a `FLOAT` or `DOUBLE` key is normalized before `ArrayBasedMapBuilder` looks for a
-- duplicate, so under `LAST_WIN` `0.0` and `-0.0` are one key, and so are all NaNs. A repeated key
-- keeps the position of its first occurrence and takes the last value. Spark then builds the map
-- from the normalized keys, so a `-0.0` that came first is stored as `0.0`. When none of a row's
-- keys repeats, `map_from_arrays` returns the keys as given, so rows of the same batch differ.
-- `map_builders_float_keys.sql` covers the default `EXCEPTION` policy.

statement
CREATE TABLE test_map_float_keys_last_win(id int, a double, b double, af float, bf float)
USING parquet

statement
INSERT INTO test_map_float_keys_last_win VALUES
  (1, 0.0D, double('-0.0'), float('0.0'), float('-0.0')),
  (2, double('-0.0'), 0.0D, float('-0.0'), float('0.0')),
  (3, double('-0.0'), 1.5D, float('-0.0'), float('1.5')),
  (4, double('NaN'), double('NaN'), float('NaN'), float('NaN'))

query
SELECT id, map_keys(m), map_values(m)
FROM (SELECT id, map_from_arrays(array(a, b), array(1, 2)) AS m FROM test_map_float_keys_last_win)

query
SELECT id, map_keys(m), map_values(m)
FROM (
  SELECT id,
    map_from_entries(array(named_struct('k', a, 'v', 1), named_struct('k', b, 'v', 2))) AS m
  FROM test_map_float_keys_last_win)

query
SELECT id, map_keys(m), map_values(m)
FROM (SELECT id, map_from_arrays(array(af, bf), array(1, 2)) AS m FROM test_map_float_keys_last_win)

-- `-b` turns the NaN of row 4 into a NaN with the sign bit set, which is still the same key.
query
SELECT id, map_keys(m), map_values(m)
FROM (SELECT id, map_from_arrays(array(a, -b), array(1, 2)) AS m FROM test_map_float_keys_last_win)

query
SELECT id, map_keys(m), map_values(m)
FROM (
  SELECT id,
    map_from_entries(array(named_struct('k', -af, 'v', 1), named_struct('k', bf, 'v', 2))) AS m
  FROM test_map_float_keys_last_win)

-- A key that repeats exactly still makes Spark build the map from the normalized keys, so the
-- `-0.0` beside it is stored as `0.0`.
query
SELECT id, map_keys(m), map_values(m)
FROM (
  SELECT id, map_from_arrays(array(a, b, b), array(1, 2, 3)) AS m
  FROM test_map_float_keys_last_win WHERE id = 3)
