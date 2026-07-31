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
-- Config: spark.comet.exec.strictFloatingPoint=true

statement
CREATE TABLE cs_fallback_float(v float, grp string) USING parquet

statement
INSERT INTO cs_fallback_float VALUES
  (1.5, 'a'), (2.5, 'a'), (1.5, 'a'), (NULL, 'a'),
  (CAST('NaN' AS FLOAT), 'b'), (CAST('NaN' AS FLOAT), 'b'), (1.0, 'b'),
  (CAST('Infinity' AS FLOAT), 'c'), (CAST('-Infinity' AS FLOAT), 'c'),
  (CAST('Infinity' AS FLOAT), 'c'),
  (CAST(0.0 AS FLOAT), 'd'), (CAST(-0.0 AS FLOAT), 'd'), (1.0, 'd'), (NULL, 'd')

query expect_fallback(not fully compatible with Spark)
SELECT grp, sort_array(collect_set(v))
FROM cs_fallback_float GROUP BY grp ORDER BY grp

statement
CREATE TABLE cs_fallback_double(v double, grp string) USING parquet

statement
INSERT INTO cs_fallback_double VALUES
  (1.1, 'a'), (2.2, 'a'), (1.1, 'a'), (NULL, 'a'),
  (CAST('NaN' AS DOUBLE), 'b'), (CAST('NaN' AS DOUBLE), 'b'), (1.0, 'b'),
  (CAST('Infinity' AS DOUBLE), 'c'), (CAST('-Infinity' AS DOUBLE), 'c'),
  (CAST('Infinity' AS DOUBLE), 'c'),
  (0.0, 'd'), (-0.0, 'd'), (1.0, 'd'), (NULL, 'd')

query expect_fallback(not fully compatible with Spark)
SELECT grp, sort_array(collect_set(v))
FROM cs_fallback_double GROUP BY grp ORDER BY grp
