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

-- Folding turns the literal maps into literals that the dispatched lookup receives. The
-- constructor forms are in `element_at_map.sql` and `get_map_value.sql`.

-- ConstantFolding: enabled

statement
CREATE TABLE test_map_lookup_folded(_1 int, _2 bigint) USING parquet

statement
INSERT INTO test_map_lookup_folded VALUES (1, 1), (2, 2), (3, 3)

-- A per-row key keeps the lookup itself from folding; `-0.0` must find the `+0.0` key
query expect_dispatch(element_at)
SELECT _1 AS id, element_at(map(CAST(0 AS DOUBLE), 7),
  CAST(concat('-', CAST(_1 - 1 AS STRING), '.0') AS DOUBLE)) AS v
FROM test_map_lookup_folded

-- The DOUBLE-keyed map is nested in a map value, so its keys never reach `CometLiteral`; the
-- decline must come from the lookup whose map argument is `MAP<DOUBLE, INT>`
query expect_dispatch(element_at)
SELECT _1 AS id, element_at(element_at(map(1, map(CAST(0 AS DOUBLE), 7)), _1),
  CAST(concat('-', CAST(_1 - 1 AS STRING), '.0') AS DOUBLE)) AS v
FROM test_map_lookup_folded

-- Spark permits a lookup array holding a NULL element
query expect_dispatch(element_at)
SELECT _1 AS id, element_at(map(array(1), 7), array(IF(_1 = 2, CAST(NULL AS INT), _1))) AS v
FROM test_map_lookup_folded
