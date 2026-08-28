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

-- MinSparkVersion: 3.5
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true
-- Config: spark.sql.optimizer.windowGroupLimitThreshold=1000
-- Config: spark.sql.execution.topKSortFallbackThreshold=0
-- ConfigMatrix: spark.sql.adaptive.enabled=false,true
-- ConfigMatrix: spark.sql.ansi.enabled=false,true

-- Range supplies ordered rows in one partition. WindowGroupLimit stops after the first
-- decoded row and never reaches the malformed second row. Disable the optimizer's ordinary
-- LIMIT alternative above so this specifically exercises WindowGroupLimit. FIRST_VALUE
-- forces a decoder projection below the window rather than in its output.
query expect_fallback(unbase64 requires Spark evaluation below LIMIT)
SELECT hex(decoded) FROM (
  SELECT first_value(unbase64(CASE WHEN id = 0 THEN 'YWJj' ELSE 'A' END)) OVER (
    ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS decoded,
    row_number() OVER (ORDER BY id) AS rn
  FROM range(0, 2, 1, 1)
) WHERE rn <= 1

-- Valid inputs without a limiting consumer still use native dispatch.
query
SELECT hex(unbase64(CASE WHEN id = 0 THEN 'YWJj' ELSE 'ZGVm' END))
FROM range(0, 2, 1, 1)

query expect_fallback(unbase64 requires Spark evaluation below LIMIT)
SELECT id FROM (
  SELECT id, row_number() OVER (ORDER BY id) AS rn
  FROM range(0, 2, 1, 1)
  WHERE hex(unbase64(CASE WHEN id = 0 THEN 'YWJj' ELSE 'A' END)) = '616263'
) WHERE rn <= 1

-- Consuming the malformed row must still report the decoder error.
query expect_error(Last unit does not have enough valid bits)
SELECT id FROM (
  SELECT id, row_number() OVER (ORDER BY id) AS rn
  FROM range(0, 2, 1, 1)
  WHERE hex(unbase64(CASE WHEN id = 0 THEN 'YWJj' ELSE 'A' END)) = '616263'
) WHERE rn <= 2

-- Reversing the order requires a sort above the filter, which consumes the malformed row.
query expect_error(Last unit does not have enough valid bits)
SELECT id FROM (
  SELECT id, row_number() OVER (ORDER BY id DESC) AS rn
  FROM range(0, 2, 1, 1)
  WHERE hex(unbase64(CASE WHEN id = 0 THEN 'YWJj' ELSE 'A' END)) = '616263'
) WHERE rn <= 1
