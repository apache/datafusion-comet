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

-- Routes multiply_ym_interval through the codegen dispatcher; produces YearMonthIntervalType.
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true

statement
CREATE TABLE test_multiply_ym_interval(y int, m int, i int, l long, f float, d double, dec decimal(10,2)) USING parquet

statement
INSERT INTO test_multiply_ym_interval VALUES
  (1, 2, 2, CAST(3 AS BIGINT), CAST(1.5 AS FLOAT), CAST(2.5 AS DOUBLE), CAST(2.50 AS DECIMAL(10, 2))),
  (-1, 1, -2, CAST(-3 AS BIGINT), CAST(-1.5 AS FLOAT), CAST(-2.5 AS DOUBLE), CAST(-2.50 AS DECIMAL(10, 2))),
  (0, 6, 0, CAST(0 AS BIGINT), CAST(0.5 AS FLOAT), CAST(0.5 AS DOUBLE), CAST(0.50 AS DECIMAL(10, 2))),
  (2, -6, NULL, NULL, NULL, NULL, NULL)

query
SELECT
  make_ym_interval(y, m) * i,
  make_ym_interval(y, m) * l,
  make_ym_interval(y, m) * f,
  make_ym_interval(y, m) * d,
  make_ym_interval(y, m) * dec
FROM test_multiply_ym_interval

-- literal interval input
query
SELECT INTERVAL '1-2' YEAR TO MONTH * i FROM test_multiply_ym_interval

-- numeric on the left is normalized by Spark to multiply_ym_interval.
query
SELECT i * make_ym_interval(y, m), 2 * INTERVAL '1-2' YEAR TO MONTH
FROM test_multiply_ym_interval

-- literal multipliers, including half-up rounding for fractional months.
query
SELECT
  make_ym_interval(1, 2) * 2,
  make_ym_interval(1, 2) * 1.5D,
  make_ym_interval(1, 2) * CAST(1.50 AS DECIMAL(10, 2)),
  make_ym_interval(-1, 1) * 1.5D
