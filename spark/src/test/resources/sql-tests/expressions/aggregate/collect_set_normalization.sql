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

-- MinSparkVersion: 4.2
-- Config: spark.comet.exec.strictFloatingPoint=true

statement
CREATE TABLE cs_norm_scalar(grp string, f float, d double) USING parquet

statement
INSERT INTO cs_norm_scalar VALUES
  ('nan', CAST('NaN' AS FLOAT), CAST('NaN' AS DOUBLE)),
  ('nan', CAST('NaN' AS FLOAT), CAST('NaN' AS DOUBLE)),
  ('zero', CAST(-0.0 AS FLOAT), CAST(-0.0 AS DOUBLE)),
  ('zero', CAST(0.0 AS FLOAT), CAST(0.0 AS DOUBLE)),
  ('mixed', CAST('NaN' AS FLOAT), CAST('NaN' AS DOUBLE)),
  ('mixed', CAST('NaN' AS FLOAT), CAST('NaN' AS DOUBLE)),
  ('mixed', CAST(-0.0 AS FLOAT), CAST(-0.0 AS DOUBLE)),
  ('mixed', CAST(0.0 AS FLOAT), CAST(0.0 AS DOUBLE)),
  ('mixed', CAST(1.0 AS FLOAT), CAST(1.0 AS DOUBLE))

-- Repartition so equal values must also deduplicate across partial buffers.
query
SELECT grp, size(collect_set(f)), size(collect_set(d))
FROM (SELECT /*+ REPARTITION(3) */ * FROM cs_norm_scalar)
GROUP BY grp
ORDER BY grp

-- Spark 4.2 canonicalizes the surviving signed zero to positive zero.
query
SELECT collect_set(f), collect_set(d) FROM cs_norm_scalar WHERE grp = 'zero'

statement
CREATE TABLE cs_norm_nested(
  grp string,
  s struct<v:double>,
  a array<float>) USING parquet

statement
INSERT INTO cs_norm_nested VALUES
  ('nan', named_struct('v', CAST('NaN' AS DOUBLE)), array(CAST('NaN' AS FLOAT))),
  ('nan', named_struct('v', CAST('NaN' AS DOUBLE)), array(CAST('NaN' AS FLOAT))),
  ('zero', named_struct('v', CAST(-0.0 AS DOUBLE)), array(CAST(-0.0 AS FLOAT))),
  ('zero', named_struct('v', CAST(0.0 AS DOUBLE)), array(CAST(0.0 AS FLOAT)))

query
SELECT grp, size(collect_set(s)), size(collect_set(a))
FROM (SELECT /*+ REPARTITION(3) */ * FROM cs_norm_nested)
GROUP BY grp
ORDER BY grp
