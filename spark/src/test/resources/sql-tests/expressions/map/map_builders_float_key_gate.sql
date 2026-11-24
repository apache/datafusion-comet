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

-- The native builders support top-level FLOAT and DOUBLE keys (#6549). Struct and array keys
-- containing floats use Spark's SQL ordering and go through the codegen dispatcher.

-- Parquet canonicalizes NaN, so negating a stored NaN gives one with the sign bit set.
statement
CREATE TABLE float_map_keys(id int, d double, f float) USING parquet

statement
INSERT INTO float_map_keys VALUES
  (1, 0.0, 0.0),
  (2, double('NaN'), float('NaN')),
  (3, 1.5, 1.5)

-- Spark 4.0+ stores a normalized key in map_from_entries. Earlier versions keep its bits.
query expect_native(map_from_entries)
SELECT id,
  map_keys(map_from_entries(array(named_struct('k', -d, 'v', 1), named_struct('k', 2.5D, 'v', 2)))),
  map_keys(map_from_entries(array(named_struct('k', -f, 'v', 1), named_struct('k', 2.5F, 'v', 2))))
FROM float_map_keys

-- With distinct keys, map_from_arrays returns the original keys on every version.
query expect_native(map_from_arrays)
SELECT id,
  map_keys(map_from_arrays(array(-d, 2.5D), array(1, 2))),
  map_keys(map_from_arrays(array(-f, 2.5F), array(1, 2)))
FROM float_map_keys

-- NaNs that differ only in the sign bit are one key on every version.
query expect_error_class(DUPLICATED_MAP_KEY)
SELECT map_from_entries(arrays_zip(array(d, -d, CAST(NULL AS DOUBLE)), array(1, 2, 3)))
FROM float_map_keys WHERE id = 2

query expect_error_class(DUPLICATED_MAP_KEY)
SELECT map_from_entries(array(named_struct('k', f, 'v', 1), named_struct('k', -f, 'v', 2)))
FROM float_map_keys WHERE id = 2

query expect_error_class(DUPLICATED_MAP_KEY)
SELECT map_from_arrays(array(d, -d, CAST(NULL AS DOUBLE)), array(1, 2, 3))
FROM float_map_keys WHERE id = 2

query expect_error_class(DUPLICATED_MAP_KEY)
SELECT map_from_arrays(array(f, -f), array(1, 2)) FROM float_map_keys WHERE id = 2

-- Spark reports the first invalid key. Moving NULL between the two NaNs changes the error.
query expect_error_class(NULL_MAP_KEY)
SELECT map_from_entries(arrays_zip(array(d, CAST(NULL AS DOUBLE), -d), array(1, 2, 3)))
FROM float_map_keys WHERE id = 2

query expect_error_class(NULL_MAP_KEY)
SELECT map_from_arrays(array(d, CAST(NULL AS DOUBLE), -d), array(1, 2, 3))
FROM float_map_keys WHERE id = 2

-- Struct and array keys compare in Spark's SQL ordering, where -0.0 equals 0.0, on every version.
query expect_error_class(DUPLICATED_MAP_KEY)
SELECT map_from_entries(array(
  named_struct('k', named_struct('x', d), 'v', 1),
  named_struct('k', named_struct('x', -d), 'v', 2)))
FROM float_map_keys WHERE id = 1

query expect_error_class(DUPLICATED_MAP_KEY)
SELECT map_from_entries(array(
  named_struct('k', array(f), 'v', 1),
  named_struct('k', array(-f), 'v', 2)))
FROM float_map_keys WHERE id = 1

query expect_error_class(DUPLICATED_MAP_KEY)
SELECT map_from_arrays(array(named_struct('x', d), named_struct('x', -d)), array(1, 2))
FROM float_map_keys WHERE id = 1

query expect_error_class(DUPLICATED_MAP_KEY)
SELECT map_from_arrays(array(array(d), array(-d)), array(1, 2)) FROM float_map_keys WHERE id = 1

-- NaNs that differ only in the sign bit are one key inside an array key too.
query expect_error_class(DUPLICATED_MAP_KEY)
SELECT map_from_arrays(array(array(d), array(-d)), array(1, 2)) FROM float_map_keys WHERE id = 2

-- Distinct keys still build a map.
query expect_native(map_from_entries)
SELECT id, map_from_entries(array(named_struct('k', d, 'v', 1), named_struct('k', d + 1, 'v', 2)))
FROM float_map_keys WHERE id IN (1, 3)

query expect_native(map_from_arrays)
SELECT id, map_from_arrays(array(d, d + 1), array(1, 2)) FROM float_map_keys WHERE id IN (1, 3)

-- Only the key type matters: a FLOAT or DOUBLE value stays native.
query expect_native(map_from_entries)
SELECT id, map_from_entries(array(named_struct('k', id, 'v', d), named_struct('k', id + 10, 'v', -d)))
FROM float_map_keys

query
SELECT id, map_from_arrays(array(id, id + 10), array(d, -d)) FROM float_map_keys
