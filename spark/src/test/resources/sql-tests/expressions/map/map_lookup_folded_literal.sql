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

-- With constant folding on, a map built from literals folds to a literal. A lookup whose key type
-- has no native kernel runs through the JVM codegen dispatcher, which then receives the folded
-- map as part of the lookup. The constructor forms, where the harness excludes `ConstantFolding`,
-- live in `element_at_map.sql` and `get_map_value.sql`.

-- ConstantFolding: enabled

statement
CREATE TABLE test_map_lookup_folded(_1 int, _2 bigint) USING parquet

statement
INSERT INTO test_map_lookup_folded VALUES (1, 1), (2, 2), (3, 3)

-- Native `map_extract` compares floating-point keys by their bits, so a `-0.0` lookup would miss
-- Spark's `+0.0` key, and the lookup is dispatched. Spark returns 7, NULL, NULL.
query expect_dispatch(element_at)
SELECT _1 AS id, element_at(map(CAST(0 AS DOUBLE), 7),
  CAST(concat('-', CAST(_1 - 1 AS STRING), '.0') AS DOUBLE)) AS v
FROM test_map_lookup_folded

-- The DOUBLE-keyed map is nested in a map value, so the dispatcher receives it whole and its keys
-- never reach `CometLiteral`. The lookup on the folded `map(1, ...)` has INT keys, so the decline
-- has to come from the lookup whose map argument is `MAP<DOUBLE, INT>`, and it takes the whole
-- nested lookup with it.
query expect_dispatch(element_at)
SELECT _1 AS id, element_at(element_at(map(1, map(CAST(0 AS DOUBLE), 7)), _1),
  CAST(concat('-', CAST(_1 - 1 AS STRING), '.0') AS DOUBLE)) AS v
FROM test_map_lookup_folded

-- Spark permits a lookup array holding a NULL element. Native `map_extract` cannot reproduce
-- Spark's equality for a complex key, so the lookup is dispatched. Spark returns 7, NULL, NULL.
query expect_dispatch(element_at)
SELECT _1 AS id, element_at(map(array(1), 7), array(IF(_1 = 2, CAST(NULL AS INT), _1))) AS v
FROM test_map_lookup_folded
