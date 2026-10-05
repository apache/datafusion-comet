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

-- From Spark 4.0, `ArrayBasedMapBuilder` normalizes a `FLOAT` or `DOUBLE` key before it looks for a
-- duplicate, unless `spark.sql.legacy.disableMapKeyNormalization` is set: `-0.0` becomes `0.0`
-- and every NaN the canonical NaN. `map_from_entries` stores the normalized key, while
-- `map_from_arrays` returns the keys it was given when none of them repeats. A collected map
-- merges `-0.0` and `0.0` keys, so the queries return `map_keys` and `map_values`. Spark's Parquet
-- writer canonicalizes NaN, so a NaN with the sign bit set is made at query time by negating a
-- stored NaN. `map_builders_float_keys_last_win.sql` covers `LAST_WIN`, and
-- `map_builders_float_keys_legacy.sql` covers Spark 3.4 and 3.5 and the legacy flag.

statement
CREATE TABLE test_map_float_keys(k array<double>, kf array<float>, v array<int>) USING parquet

statement
INSERT INTO test_map_float_keys VALUES
  (array(double('-0.0'), 1.5D), array(float('-0.0'), float('1.5')), array(10, 20))

-- The `-0.0` key is stored as `0.0`.
query
SELECT map_keys(m), map_values(m)
FROM (SELECT map_from_entries(arrays_zip(k, v)) AS m FROM test_map_float_keys)

query
SELECT key FROM test_map_float_keys
LATERAL VIEW explode(map_keys(map_from_entries(arrays_zip(k, v)))) e AS key

query
SELECT map_keys(map_from_entries(arrays_zip(kf, v))) FROM test_map_float_keys

-- No key repeats, so `map_from_arrays` keeps the `-0.0` it was given.
query
SELECT map_keys(map_from_arrays(k, v)), map_keys(map_from_arrays(kf, v))
FROM test_map_float_keys

statement
CREATE TABLE test_map_float_key_pairs(id int, a double, b double, af float, bf float)
USING parquet

statement
INSERT INTO test_map_float_key_pairs VALUES
  (1, 0.0D, double('-0.0'), float('0.0'), float('-0.0')),
  (2, double('-0.0'), 1.5D, float('-0.0'), float('1.5')),
  (3, double('NaN'), double('NaN'), float('NaN'), float('NaN')),
  (4, 1.0D, 1.0D, float('1.0'), float('1.0'))

-- `0.0` and `-0.0` are one key. Spark names the repeated key as it was passed.
query expect_error(Duplicate map key -0.0 was found)
SELECT map_from_arrays(array(a, b), array(1, 2)) FROM test_map_float_key_pairs WHERE id = 1

query expect_error(Duplicate map key -0.0 was found)
SELECT map_from_entries(array(named_struct('k', a, 'v', 1), named_struct('k', b, 'v', 2)))
FROM test_map_float_key_pairs WHERE id = 1

query expect_error(Duplicate map key -0.0 was found)
SELECT map_from_arrays(array(af, bf), array(1, 2)) FROM test_map_float_key_pairs WHERE id = 1

-- A NaN with the sign bit set is the same key as the canonical NaN.
query expect_error(Duplicate map key NaN was found)
SELECT map_from_arrays(array(a, -b), array(1, 2)) FROM test_map_float_key_pairs WHERE id = 3

query expect_error(Duplicate map key NaN was found)
SELECT map_from_entries(array(named_struct('k', -af, 'v', 1), named_struct('k', bf, 'v', 2)))
FROM test_map_float_key_pairs WHERE id = 3

-- Spark writes a repeated key with `Double.toString` and `Float.toString`.
query expect_error(Duplicate map key 1.0 was found)
SELECT map_from_arrays(array(a, b), array(1, 2)) FROM test_map_float_key_pairs WHERE id = 4

query expect_error(Duplicate map key 1.0 was found)
SELECT map_from_entries(array(named_struct('k', af, 'v', 1), named_struct('k', bf, 'v', 2)))
FROM test_map_float_key_pairs WHERE id = 4

-- Without a repeat, `map_from_entries` stores `0.0` for `-0.0` and `map_from_arrays` keeps it.
query
SELECT id,
  map_keys(map_from_entries(array(named_struct('k', a, 'v', 1), named_struct('k', 2.5D, 'v', 2)))),
  map_keys(map_from_arrays(array(a, 2.5D), array(1, 2))),
  map_keys(map_from_entries(array(named_struct('k', af, 'v', 1)))),
  map_keys(map_from_arrays(array(af), array(1)))
FROM test_map_float_key_pairs WHERE id IN (2, 3)

-- A NULL keys array is a NULL map, so its repeated key is never inserted.
query
SELECT id, map_keys(m)
FROM (
  SELECT id, map_from_arrays(IF(id = 1, NULL, array(a, b)), array(1, 2)) AS m
  FROM test_map_float_key_pairs WHERE id IN (1, 2))
