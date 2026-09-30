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

-- ANSI-mode element_at over maps. The non-ANSI forms live in element_at_map.sql.

-- Config: spark.sql.ansi.enabled=true

statement
CREATE TABLE test_element_at_nested_ansi(id int) USING parquet

statement
INSERT INTO test_element_at_nested_ansi VALUES (1), (2), (3)

-- Nested INT-keyed map lookup with a per-row key. The inner element_at returns NULL for ids not in
-- the outer map (2, 3); the outer element_at looks it up with `id % (id - 2)`, which is 2 % 0 at
-- id = 2. Spark's ElementAt is a BinaryExpression that short-circuits the NULL inner map and never
-- evaluates the throwing key, so under ANSI it returns 7, NULL, NULL. CometElementAt reproduces the
-- short-circuit natively with a CASE WHEN <map> IS NOT NULL guard, so the key runs only where the
-- map is non-null and the query runs with native acceleration.
query
SELECT id, element_at(element_at(map(1, map(0, 7)), id), id % (id - 2)) AS v
FROM test_element_at_nested_ansi
