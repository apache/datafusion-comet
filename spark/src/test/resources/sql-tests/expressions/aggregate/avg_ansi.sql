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


-- Config: spark.sql.ansi.enabled=true
-- Config: spark.sql.adaptive.enabled=false
-- Config: spark.comet.shuffle.mode=native
-- Config: spark.sql.execution.useObjectHashAggregateExec=true

statement
CREATE TABLE test_avg_wide_division(k int, v decimal(38,18)) USING parquet

statement
INSERT INTO test_avg_wide_division SELECT /*+ COALESCE(1) */ * FROM VALUES
  (1, 9000000000000000), (1, 9000000000000000), (2, 100000000000000000)

-- The sum's rescale exceeds i128, but division by the count produces a valid average.
query
SELECT k, AVG(v) FROM test_avg_wide_division WHERE k = 1 GROUP BY k

-- TRY_AVG keeps the valid group and returns NULL for final result overflow in either ANSI mode.
query
SELECT k, TRY_AVG(v) FROM test_avg_wide_division GROUP BY k

-- The final DECIMAL(38,22) result cannot hold 1e17, even though the sum fits.
query expect_error(NUMERIC_VALUE_OUT_OF_RANGE)
SELECT k, AVG(v) FROM test_avg_wide_division WHERE k = 2 GROUP BY k

-- Ungrouped maximum-precision AVG still needs Spark's unbounded aggregation buffer.
query expect_fallback(Ungrouped decimal AVG with a maximum-precision sum)
SELECT AVG(v) FROM test_avg_wide_division WHERE k = 1

-- collect_list selects ObjectHashAggregate, whose buffer can recover from the intermediate 1.2.
statement
CREATE TABLE test_avg_object(k int, v decimal(38,38)) USING parquet

statement
INSERT INTO test_avg_object SELECT /*+ COALESCE(1) */ * FROM VALUES
  (1, 0.6), (1, 0.6), (1, -0.4)

query expect_fallback(Decimal AVG with a maximum-precision sum cannot match Spark's unbounded object aggregation buffer)
SELECT k, AVG(v), sort_array(collect_list(k)) FROM test_avg_object GROUP BY k
