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

-- Config: spark.sql.legacy.disableMapKeyNormalization=true

-- Spark 3.4 and 3.5, and Spark 4.0+ with `spark.sql.legacy.disableMapKeyNormalization`, find
-- duplicate `FLOAT` and `DOUBLE` keys in a `HashMap` of boxed keys, whose `Double.equals` and
-- `Float.equals` compare `doubleToLongBits` and `floatToIntBits`: every NaN is one key, while
-- `-0.0` and `0.0` are two. Spark 3.4 and 3.5 have no such flag and always behave this way, so this
-- file runs on every version. A collected map merges `-0.0` and `0.0` keys, so the queries return
-- `map_keys` and `map_values`. `map_builders_float_keys_legacy_last_win.sql` covers `LAST_WIN`, and
-- `map_builders_float_keys.sql` the normalization of Spark 4.0+.

statement
CREATE TABLE test_map_float_keys_legacy(k array<double>, v array<int>) USING parquet

statement
INSERT INTO test_map_float_keys_legacy VALUES (array(double('-0.0'), 1.5D), array(10, 20))

-- The `-0.0` key is stored as it was passed.
query
SELECT map_keys(m), map_values(m)
FROM (SELECT map_from_entries(arrays_zip(k, v)) AS m FROM test_map_float_keys_legacy)

query
SELECT key FROM test_map_float_keys_legacy
LATERAL VIEW explode(map_keys(map_from_entries(arrays_zip(k, v)))) e AS key

statement
CREATE TABLE test_map_float_key_pairs_legacy(id int, a double, b double, af float, bf float)
USING parquet

statement
INSERT INTO test_map_float_key_pairs_legacy VALUES
  (1, 0.0D, double('-0.0'), float('0.0'), float('-0.0')),
  (2, double('NaN'), double('NaN'), float('NaN'), float('NaN')),
  (3, 1.0D, 1.0D, float('1.0'), float('1.0'))

-- `0.0` and `-0.0` are two keys.
query
SELECT map_keys(m1), map_values(m1), map_keys(m2), map_values(m2), map_keys(m3)
FROM (
  SELECT map_from_arrays(array(a, b), array(1, 2)) AS m1,
    map_from_entries(array(named_struct('k', a, 'v', 1), named_struct('k', b, 'v', 2))) AS m2,
    map_from_arrays(array(af, bf), array(1, 2)) AS m3
  FROM test_map_float_key_pairs_legacy WHERE id = 1)

-- A NaN with the sign bit set is the same key as the canonical NaN.
query expect_error(Duplicate map key NaN was found)
SELECT map_from_arrays(array(a, -b), array(1, 2)) FROM test_map_float_key_pairs_legacy WHERE id = 2

query expect_error(Duplicate map key NaN was found)
SELECT map_from_entries(array(named_struct('k', -a, 'v', 1), named_struct('k', b, 'v', 2)))
FROM test_map_float_key_pairs_legacy WHERE id = 2

query expect_error(Duplicate map key NaN was found)
SELECT map_from_arrays(array(-af, bf), array(1, 2)) FROM test_map_float_key_pairs_legacy WHERE id = 2

-- Spark writes a repeated key with `Double.toString` and `Float.toString`.
query expect_error(Duplicate map key 1.0 was found)
SELECT map_from_arrays(array(a, b), array(1, 2)) FROM test_map_float_key_pairs_legacy WHERE id = 3

query expect_error(Duplicate map key 1.0 was found)
SELECT map_from_entries(array(named_struct('k', af, 'v', 1), named_struct('k', bf, 'v', 2)))
FROM test_map_float_key_pairs_legacy WHERE id = 3
