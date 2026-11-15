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

-- RANGE window frames over an array or struct ORDER BY key that can hold a null element or field.
-- DataFusion finds a frame's CURRENT ROW bound by comparing keys with an ordering that puts a
-- null element or field above every other value, while the sort put it first, as Spark does.
-- Once the search passes such a row, the frame of every later row ran to the end of the
-- partition, so these frames fall back to Spark
-- (https://github.com/apache/datafusion-comet/issues/6477). Ranking functions, ROWS frames,
-- CUME_DIST and a RANGE frame unbounded on both sides never search for a bound and stay native,
-- as does a key whose type cannot hold a null element or field.

statement
CREATE TABLE nested_null_frame(id INT, p INT, i INT) USING parquet

statement
INSERT INTO nested_null_frame VALUES
  (1, 1, NULL), (2, 1, 1), (3, 1, 1), (4, 1, 2), (5, 2, NULL), (6, 2, 3)

-- Native, every row of partition 1 got 10, the sum of the whole partition. Spark gives 1, 6, 6
-- and 10.
query expect_fallback(can hold a null element or field)
SELECT id, SUM(id) OVER (PARTITION BY p ORDER BY array(i)) AS running FROM nested_null_frame

query expect_fallback(can hold a null element or field)
SELECT id, SUM(id) OVER (PARTITION BY p ORDER BY named_struct('x', i)) AS running
FROM nested_null_frame

query expect_fallback(can hold a null element or field)
SELECT id, SUM(id) OVER (ORDER BY array(i) DESC) AS running FROM nested_null_frame

query expect_fallback(can hold a null element or field)
SELECT id,
  COUNT(id) OVER (PARTITION BY p ORDER BY array(i)
                  RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS remaining
FROM nested_null_frame

-- The null is a level further down here
query expect_fallback(can hold a null element or field)
SELECT id,
  MAX(id) OVER (PARTITION BY p ORDER BY named_struct('a', named_struct('b', i))) AS mx,
  LAST_VALUE(id) OVER (PARTITION BY p ORDER BY named_struct('a', named_struct('b', i))) AS lv
FROM nested_null_frame

-- These never search for a frame bound, so they stay native over the same keys
query
SELECT id,
  RANK() OVER (PARTITION BY p ORDER BY array(i)) AS r,
  DENSE_RANK() OVER (ORDER BY named_struct('x', i) DESC, id) AS dr,
  ROW_NUMBER() OVER (PARTITION BY p ORDER BY array(i), id) AS rn
FROM nested_null_frame

query
SELECT id,
  SUM(id) OVER (PARTITION BY p ORDER BY array(i), id
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rows_sum,
  SUM(id) OVER (PARTITION BY p ORDER BY array(i)
                RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total
FROM nested_null_frame

query tolerance=1e-6
SELECT id, CUME_DIST() OVER (PARTITION BY p ORDER BY array(i)) AS cd FROM nested_null_frame

-- A key whose type cannot hold a null element stays native. coalesce turns rows 1 and 5 into
-- zeros.
query
SELECT id,
  SUM(id) OVER (PARTITION BY p ORDER BY array(coalesce(i, 0))) AS running,
  SUM(id) OVER (PARTITION BY p ORDER BY named_struct('x', coalesce(i, 0)) DESC) AS by_struct
FROM nested_null_frame
