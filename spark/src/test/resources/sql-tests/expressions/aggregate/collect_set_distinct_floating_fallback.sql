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

-- MaxSparkVersion: 4.1
-- Config: spark.comet.exec.strictFloatingPoint=false
-- Config: spark.comet.exec.localTableScan.enabled=false
-- Config: spark.sql.shuffle.partitions=2
-- ConfigMatrix: spark.sql.execution.useObjectHashAggregateExec=true,false
-- ConfigMatrix: spark.sql.adaptive.enabled=true,false

statement
CREATE TABLE cs_distinct_float(grp int, id int, f float, d double) USING parquet

statement
INSERT INTO cs_distinct_float VALUES
  (1, 1, CAST('NaN' AS FLOAT), CAST('NaN' AS DOUBLE)),
  (1, 2, CAST('NaN' AS FLOAT), CAST('NaN' AS DOUBLE)),
  (1, 3, CAST('-0.0' AS FLOAT), CAST('-0.0' AS DOUBLE)),
  (1, 4, 0.0, 0.0), (1, 5, 1.0, 1.0), (1, 6, NULL, NULL),
  (1, 7, CAST('Infinity' AS FLOAT), CAST('Infinity' AS DOUBLE)),
  (1, 8, CAST('-Infinity' AS FLOAT), CAST('-Infinity' AS DOUBLE)),
  (2, 1, NULL, NULL),
  (3, 1, 1.0, 1.0), (3, 2, 1.0, 1.0), (3, 3, 2.0, 2.0)

-- Issue #7203: even with strict floating-point checks disabled, the distinct chain must keep
-- Spark's signed-zero and repeated-NaN semantics. Check FLOAT and DOUBLE independently.
query expect_fallback(collect_set on FLOAT/DOUBLE in PartialMerge requires Spark 4.2)
SELECT grp, count(DISTINCT id), sort_array(collect_set(f))
FROM cs_distinct_float GROUP BY grp

query expect_fallback(collect_set on FLOAT/DOUBLE in PartialMerge requires Spark 4.2)
SELECT grp, count(DISTINCT id), sort_array(collect_set(d))
FROM cs_distinct_float GROUP BY grp

-- VALUES keeps the partial aggregate in Spark and exercises its serialized buffer boundary.
query expect_fallback(collect_set on FLOAT/DOUBLE in PartialMerge requires Spark 4.2)
SELECT grp, count(DISTINCT id), sort_array(collect_set(v))
FROM VALUES
  (1, 1, CAST('NaN' AS FLOAT)), (1, 2, CAST('NaN' AS FLOAT)),
  (1, 3, CAST('-0.0' AS FLOAT)), (1, 4, CAST(0.0 AS FLOAT)),
  (1, 5, CAST(1.0 AS FLOAT)), (1, 6, CAST(NULL AS FLOAT)),
  (2, 1, CAST(NULL AS FLOAT)) AS t(grp, id, v)
GROUP BY grp

query expect_fallback(collect_set on FLOAT/DOUBLE in PartialMerge requires Spark 4.2)
SELECT grp, count(DISTINCT id), sort_array(collect_set(v))
FROM VALUES
  (1, 1, CAST('NaN' AS DOUBLE)), (1, 2, CAST('NaN' AS DOUBLE)),
  (1, 3, CAST('-0.0' AS DOUBLE)), (1, 4, CAST(0.0 AS DOUBLE)),
  (1, 5, CAST(1.0 AS DOUBLE)), (1, 6, CAST(NULL AS DOUBLE)),
  (2, 1, CAST(NULL AS DOUBLE)) AS t(grp, id, v)
GROUP BY grp

-- The new restriction must leave these native paths enabled. Ordinary collect_set on older
-- Spark still has documented floating-point differences, so use finite nonzero controls.
query
SELECT grp, sort_array(collect_set(f)), sort_array(collect_set(d))
FROM cs_distinct_float WHERE grp = 3 GROUP BY grp

query
SELECT grp, count(DISTINCT id), sort_array(collect_list(f)), sort_array(collect_list(d))
FROM cs_distinct_float WHERE grp = 3 GROUP BY grp

query
SELECT grp, count(DISTINCT id),
  sort_array(collect_set(named_struct('v', f))),
  sort_array(collect_set(named_struct('v', d)))
FROM cs_distinct_float WHERE grp = 3 GROUP BY grp
