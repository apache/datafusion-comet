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
-- Config: spark.sql.mapKeyDedupPolicy=LAST_WIN

-- Without key normalization (Spark 3.4 and 3.5, or the legacy flag on Spark 4.0+), every NaN is
-- one key and `-0.0` and `0.0` are two. Under `LAST_WIN` a repeated key keeps its first occurrence,
-- bits included, and takes the last value. Spark 3.4 and 3.5 have no such flag and always behave
-- this way, so this file runs on every version. `map_builders_float_keys_legacy.sql` covers the
-- default `EXCEPTION` policy.

statement
CREATE TABLE test_map_float_keys_legacy_last_win(id int, a double, b double, af float, bf float)
USING parquet

statement
INSERT INTO test_map_float_keys_legacy_last_win VALUES
  (1, 0.0D, double('-0.0'), float('0.0'), float('-0.0')),
  (2, double('NaN'), double('NaN'), float('NaN'), float('NaN')),
  (3, double('-0.0'), 1.5D, float('-0.0'), float('1.5'))

query
SELECT id, map_keys(m), map_values(m)
FROM (
  SELECT id, map_from_arrays(array(a, b), array(1, 2)) AS m
  FROM test_map_float_keys_legacy_last_win)

-- `-a` and `-b` give row 2 a NaN with the sign bit set, first in one map and last in the other.
query
SELECT id, map_keys(m1), map_values(m1), map_keys(m2), map_values(m2)
FROM (
  SELECT id,
    map_from_arrays(array(-a, b), array(1, 2)) AS m1,
    map_from_arrays(array(a, -b), array(1, 2)) AS m2
  FROM test_map_float_keys_legacy_last_win)

query
SELECT id, map_keys(m1), map_values(m1), map_keys(m2), map_values(m2)
FROM (
  SELECT id,
    map_from_entries(array(named_struct('k', -a, 'v', 1), named_struct('k', b, 'v', 2))) AS m1,
    map_from_entries(array(named_struct('k', af, 'v', 1), named_struct('k', -bf, 'v', 2))) AS m2
  FROM test_map_float_keys_legacy_last_win)
