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

-- Spark orders a null element of an array key, or a null field of a struct key, below every
-- other value, whatever the key's NULLS FIRST or NULLS LAST, which places only a null key itself.
-- For nondefault root NULL placement, Comet wraps the key so only root NULLs use that placement;
-- nested NULLs keep Spark's direction-dependent order in Sort, TopK and window keys.

statement
CREATE TABLE nested_null_order(id INT, g INT, i INT) USING parquet

statement
INSERT INTO nested_null_order VALUES (1, 1, 1), (2, 1, NULL), (3, 2, 3), (4, 2, NULL), (5, 1, 2)

-- Rows 2 and 4 hold a null element, which Spark sorts first under ASC and last under DESC.
query
SELECT id FROM nested_null_order ORDER BY array(i) DESC NULLS FIRST, id

query
SELECT id FROM nested_null_order ORDER BY named_struct('x', i) NULLS LAST, id

query
SELECT id FROM nested_null_order ORDER BY array(array(i)) NULLS LAST, id

-- A root NULL and an interior NULL must be placed independently.
query
SELECT id FROM nested_null_order
ORDER BY IF(id = 1, CAST(NULL AS ARRAY<INT>), array(i)) ASC NULLS LAST, id

query
SELECT id FROM nested_null_order
ORDER BY IF(id = 1, CAST(NULL AS STRUCT<x:INT>), named_struct('x', i)) DESC NULLS FIRST, id

-- TopK
query
SELECT id FROM nested_null_order ORDER BY array(i) NULLS LAST, id LIMIT 3

-- Window order keys
query
SELECT id, RANK() OVER (ORDER BY array(i) DESC NULLS FIRST) AS r FROM nested_null_order

query
SELECT id,
  DENSE_RANK() OVER (PARTITION BY g ORDER BY named_struct('x', i) NULLS LAST) AS r
FROM nested_null_order

-- The default null orders stay native
query
SELECT id FROM nested_null_order ORDER BY array(i), id

query
SELECT id FROM nested_null_order ORDER BY named_struct('x', i) DESC, id

query
SELECT id FROM nested_null_order ORDER BY array(i) DESC NULLS LAST, id LIMIT 3

query
SELECT id,
  RANK() OVER (ORDER BY array(i) DESC) AS r,
  DENSE_RANK() OVER (PARTITION BY g ORDER BY named_struct('x', i)) AS dr
FROM nested_null_order

-- A key whose type cannot hold a null element stays native under any null order. Both engines
-- place a null key itself by its null order.
query
SELECT id FROM nested_null_order ORDER BY array(coalesce(i, 0)) NULLS LAST, id

query
SELECT id FROM nested_null_order
ORDER BY IF(i IS NULL, NULL, array(coalesce(i, 0))) DESC NULLS FIRST, id
