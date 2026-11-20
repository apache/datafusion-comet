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

-- Under LAST_WIN the first key keeps its position and the last value wins. The native builders
-- handle top-level FLOAT and DOUBLE keys (#6549). String keys use the codegen dispatcher.

-- Config: spark.sql.mapKeyDedupPolicy=LAST_WIN
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true

-- Parquet canonicalizes NaN, so negating a stored NaN gives one with the sign bit set.
statement
CREATE TABLE float_map_keys_last_win(id int, d double, f float, k array<string>, v array<int>)
USING parquet

statement
INSERT INTO float_map_keys_last_win SELECT /*+ COALESCE(1) */ * FROM VALUES
  (1, 0.0, 0.0, array('a', 'a', 'b'), array(1, 2, 3)),
  (2, double('NaN'), float('NaN'), array('x', 'x'), array(10, 20)),
  (3, 1.5, 1.5, array('a', 'b', 'c'), array(1, 2, 3))

-- A repeated string key: the last value wins.
query expect_dispatch(map_from_arrays)
SELECT id, map_keys(map_from_arrays(k, v)), map_values(map_from_arrays(k, v))
FROM float_map_keys_last_win

query expect_dispatch(map_from_entries)
SELECT id,
  map_keys(map_from_entries(arrays_zip(k, v))),
  map_values(map_from_entries(arrays_zip(k, v)))
FROM float_map_keys_last_win

-- NaNs with different sign bits are one DOUBLE or FLOAT key: one entry, holding the last value.
query expect_native(map_from_arrays)
SELECT map_keys(map_from_arrays(array(d, -d), array(1, 2))),
  map_values(map_from_arrays(array(d, -d), array(1, 2))),
  map_keys(map_from_arrays(array(f, -f), array(1, 2))),
  map_values(map_from_arrays(array(f, -f), array(1, 2)))
FROM float_map_keys_last_win WHERE id = 2

query expect_native(map_from_entries)
SELECT map_keys(map_from_entries(array(named_struct('k', d, 'v', 1), named_struct('k', -d, 'v', 2)))),
  map_values(map_from_entries(array(named_struct('k', d, 'v', 1), named_struct('k', -d, 'v', 2)))),
  map_keys(map_from_entries(array(named_struct('k', f, 'v', 1), named_struct('k', -f, 'v', 2)))),
  map_values(map_from_entries(array(named_struct('k', f, 'v', 1), named_struct('k', -f, 'v', 2))))
FROM float_map_keys_last_win WHERE id = 2

-- 0.0 and -0.0: one key holding the last value from Spark 4.0, two keys before that.
query expect_native(map_from_arrays)
SELECT map_keys(map_from_arrays(array(d, -d), array(1, 2))),
  map_values(map_from_arrays(array(d, -d), array(1, 2))),
  map_keys(map_from_arrays(array(f, -f), array(1, 2))),
  map_values(map_from_arrays(array(f, -f), array(1, 2)))
FROM float_map_keys_last_win WHERE id = 1

query expect_native(map_from_entries)
SELECT map_keys(map_from_entries(array(named_struct('k', d, 'v', 1), named_struct('k', -d, 'v', 2)))),
  map_values(map_from_entries(array(named_struct('k', d, 'v', 1), named_struct('k', -d, 'v', 2)))),
  map_keys(map_from_entries(array(named_struct('k', f, 'v', 1), named_struct('k', -f, 'v', 2)))),
  map_values(map_from_entries(array(named_struct('k', f, 'v', 1), named_struct('k', -f, 'v', 2))))
FROM float_map_keys_last_win WHERE id = 1

-- The stored key, when -0.0 is the first key: 0.0 from Spark 4.0 (normalized), -0.0 before that.
query expect_native(map_from_arrays)
SELECT map_keys(map_from_arrays(array(-d, d), array(1, 2))),
  map_values(map_from_arrays(array(-d, d), array(1, 2)))
FROM float_map_keys_last_win WHERE id = 1

query expect_native(map_from_entries)
SELECT map_keys(map_from_entries(array(named_struct('k', -d, 'v', 1), named_struct('k', d, 'v', 2)))),
  map_values(map_from_entries(array(named_struct('k', -d, 'v', 1), named_struct('k', d, 'v', 2))))
FROM float_map_keys_last_win WHERE id = 1

statement
CREATE TABLE float_map_inputs USING parquet AS
SELECT /*+ COALESCE(1) */ id, array(3.0D, double('-0.0'), double(id)) AS keys,
  array(k, k, CAST(NULL AS ARRAY<STRING>)) AS vals, CAST(array() AS ARRAY<DOUBLE>) AS empty_keys
FROM float_map_keys_last_win

-- Only row 3 repeats the key 3.0, replacing its value with NULL at position 0. On Spark 4.0+
-- map_from_arrays normalizes -0.0 in that row alone. map_from_entries normalizes it in every row.
query expect_native(map_from_arrays, map_from_entries)
SELECT id, map_entries(map_from_arrays(keys, vals)), map_entries(map_from_entries(arrays_zip(keys, vals)))
FROM float_map_inputs

query
SELECT size(map_from_arrays(empty_keys, arrays_zip())) FROM float_map_inputs

-- Dispatch the whole expression so discarded strings need no intermediate batch buffer.
query expect_dispatch(map_from_arrays, map_from_entries)
SELECT map_from_arrays(array(d, d), array(array('discarded'), k)),
  map_from_entries(arrays_zip(array(d, d), array('discarded', 'kept')))
FROM float_map_keys_last_win

-- The row counter advances once per row, including when the values array is NULL.
query expect_dispatch(map_from_entries)
SELECT id, map_from_entries(arrays_zip(array(d), IF(monotonically_increasing_id() % 2 = 0, array(7), NULL)))
FROM float_map_keys_last_win

-- The string map keeps the second value for 'a' before the double map consumes its values.
query expect_dispatch(map_from_arrays)
SELECT id, map_from_arrays(array(d), map_values(str_to_map(concat('a:0,a:', CAST(id AS STRING)))))
FROM float_map_keys_last_win

statement
SET spark.sql.ansi.enabled=true

-- Spark evaluates the shared id + 1 on every row and skips the division when the keys are NULL.
query expect_dispatch(map_from_arrays)
SELECT id + 1, map_from_arrays(IF(id = 1, NULL, array(d)), array((id + 1) / (id - 1)))
FROM float_map_keys_last_win
