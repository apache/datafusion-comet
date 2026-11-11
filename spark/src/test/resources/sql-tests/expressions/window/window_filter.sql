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

-- Spark 4.2 allows a FILTER (WHERE ...) clause on a window aggregate. DataFusion window
-- expressions have no filter, so Comet must fall back to Spark for these rather than aggregating
-- over the unfiltered frame and returning wrong results.

-- MinSparkVersion: 4.2

statement
CREATE TABLE test_window_filter(id INT, val INT, dec_val DECIMAL(8,2), cate STRING) USING parquet

statement
INSERT INTO test_window_filter VALUES
  (1, 10, 10.25, 'a'),
  (2, 20, 20.50, 'b'),
  (3, 30, 30.75, 'a'),
  (4, 40, 40.00, 'b'),
  (5, NULL, NULL, 'a')

query expect_fallback(FILTER (WHERE ...))
SELECT id, val, cate,
  sum(val) FILTER (WHERE cate = 'a')
    OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sum_a
FROM test_window_filter ORDER BY id

query expect_fallback(FILTER (WHERE ...))
SELECT id, val, cate,
  count(val) FILTER (WHERE val > 15) OVER (PARTITION BY cate ORDER BY id) AS cnt_gt15
FROM test_window_filter ORDER BY id

query expect_fallback(FILTER (WHERE ...))
SELECT id, val, cate,
  first_value(val) FILTER (WHERE cate = 'a')
    OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_a,
  last_value(val) FILTER (WHERE cate = 'a')
    OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_a
FROM test_window_filter ORDER BY id

-- Spark's DecimalAggregates rule rewrites a low-precision decimal SUM / AVG into a MakeDecimal /
-- Cast(Divide(...)) wrapper, and Comet unwraps that into a rebuilt window expression before
-- serializing it. The fallback reason must still reach the Window operator from that rebuilt tree.
query expect_fallback(FILTER (WHERE ...))
SELECT id, dec_val, cate,
  sum(dec_val) FILTER (WHERE cate = 'a')
    OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS dec_sum_a
FROM test_window_filter ORDER BY id

query expect_fallback(FILTER (WHERE ...))
SELECT id, dec_val, cate,
  avg(dec_val) FILTER (WHERE cate = 'a')
    OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS dec_avg_a
FROM test_window_filter ORDER BY id

-- An unfiltered window aggregate over the same table still runs natively.
query
SELECT id,
  sum(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum
FROM test_window_filter ORDER BY id

-- The same decimal SUM / AVG without a FILTER still runs natively, so the fallbacks above are
-- attributable to the filter rather than to the DecimalAggregates unwrapping.
query
SELECT id,
  sum(dec_val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS dec_sum,
  avg(dec_val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS dec_avg
FROM test_window_filter ORDER BY id
