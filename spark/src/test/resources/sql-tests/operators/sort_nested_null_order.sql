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
-- Comet's native sort places a nested null by the key's null order instead, so under ASC NULLS
-- LAST and DESC NULLS FIRST a key whose type can hold one falls back to Spark, in a Sort, a TopK
-- or a window order key (https://github.com/apache/datafusion-comet/issues/6476). The default
-- null orders, ASC NULLS FIRST and DESC NULLS LAST, put it where Spark does and stay native.
--
-- The test harness admits incompatible sort orders by default, so turn that off to check the
-- shipped policy.
-- Config: spark.comet.expression.SortOrder.allowIncompatible=false

statement
CREATE TABLE nested_null_order(id INT, g INT, i INT) USING parquet

statement
INSERT INTO nested_null_order VALUES (1, 1, 1), (2, 1, NULL), (3, 2, 3), (4, 2, NULL), (5, 1, 2)

-- Rows 2 and 4 hold a null element, which Spark sorts first under ASC and last under DESC.
-- Native, these put them at the other end.
query expect_fallback(on an array or struct that can hold a null element or field)
SELECT id FROM nested_null_order ORDER BY array(i) DESC NULLS FIRST, id

query expect_fallback(on an array or struct that can hold a null element or field)
SELECT id FROM nested_null_order ORDER BY named_struct('x', i) NULLS LAST, id

query expect_fallback(on an array or struct that can hold a null element or field)
SELECT id FROM nested_null_order ORDER BY array(array(i)) NULLS LAST, id

-- TopK
query expect_fallback(on an array or struct that can hold a null element or field)
SELECT id FROM nested_null_order ORDER BY array(i) NULLS LAST, id LIMIT 3

-- Window order keys
query expect_fallback(on an array or struct that can hold a null element or field)
SELECT id, RANK() OVER (ORDER BY array(i) DESC NULLS FIRST) AS r FROM nested_null_order

query expect_fallback(on an array or struct that can hold a null element or field)
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
