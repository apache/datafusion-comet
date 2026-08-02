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
-- ConfigMatrix: parquet.enable.dictionary=false,true

statement
CREATE TABLE cs_norm_scalar(grp string, f float, d double) USING parquet

statement
INSERT INTO cs_norm_scalar VALUES
  ('ordinary', CAST(1.5 AS FLOAT), CAST(1.1 AS DOUBLE)),
  ('ordinary', CAST(2.5 AS FLOAT), CAST(2.2 AS DOUBLE)),
  ('ordinary', CAST(1.5 AS FLOAT), CAST(1.1 AS DOUBLE)),
  ('ordinary', CAST(NULL AS FLOAT), CAST(NULL AS DOUBLE)),
  ('nan', CAST('NaN' AS FLOAT), CAST('NaN' AS DOUBLE)),
  ('nan', CAST('NaN' AS FLOAT), CAST('NaN' AS DOUBLE)),
  ('nan', CAST(1.0 AS FLOAT), CAST(1.0 AS DOUBLE)),
  ('infinity', CAST('Infinity' AS FLOAT), CAST('Infinity' AS DOUBLE)),
  ('infinity', CAST('-Infinity' AS FLOAT), CAST('-Infinity' AS DOUBLE)),
  ('infinity', CAST('Infinity' AS FLOAT), CAST('Infinity' AS DOUBLE)),
  ('zero', CAST('-0.0' AS FLOAT), CAST('-0.0' AS DOUBLE)),
  ('zero', CAST(0.0 AS FLOAT), CAST(0.0 AS DOUBLE)),
  ('zero', CAST(1.0 AS FLOAT), CAST(1.0 AS DOUBLE)),
  ('zero', CAST(NULL AS FLOAT), CAST(NULL AS DOUBLE))

-- Repartition so equal values must also deduplicate across partial buffers.
query
SELECT grp, sort_array(collect_set(f)), sort_array(collect_set(d))
FROM (SELECT /*+ REPARTITION(3) */ * FROM cs_norm_scalar)
GROUP BY grp
ORDER BY grp

-- Exercise the no-GROUP BY aggregate shape while merging partial buffers.
query
SELECT sort_array(collect_set(f)), sort_array(collect_set(d))
FROM (
  SELECT /*+ REPARTITION(3) */ f, d
  FROM cs_norm_scalar
  WHERE grp IN ('nan', 'zero')
)

statement
CREATE TABLE cs_norm_nested(
  grp string,
  s struct<v:double>,
  a array<float>,
  deep_a array<struct<v:double>>,
  deep_s struct<a:array<double>>) USING parquet

statement
INSERT INTO cs_norm_nested VALUES
  ('nan',
    named_struct('v', CAST('NaN' AS DOUBLE)),
    array(CAST('NaN' AS FLOAT)),
    array(named_struct('v', CAST('NaN' AS DOUBLE))),
    named_struct('a', array(CAST('NaN' AS DOUBLE)))),
  ('nan',
    named_struct('v', CAST('NaN' AS DOUBLE)),
    array(CAST('NaN' AS FLOAT)),
    array(named_struct('v', CAST('NaN' AS DOUBLE))),
    named_struct('a', array(CAST('NaN' AS DOUBLE)))),
  ('zero',
    named_struct('v', CAST('-0.0' AS DOUBLE)),
    array(CAST('-0.0' AS FLOAT)),
    array(named_struct('v', CAST('-0.0' AS DOUBLE))),
    named_struct('a', array(CAST('-0.0' AS DOUBLE)))),
  ('zero',
    named_struct('v', CAST(0.0 AS DOUBLE)),
    array(CAST(0.0 AS FLOAT)),
    array(named_struct('v', CAST(0.0 AS DOUBLE))),
    named_struct('a', array(CAST(0.0 AS DOUBLE)))),
  ('null',
    CAST(NULL AS STRUCT<v:DOUBLE>),
    CAST(NULL AS ARRAY<FLOAT>),
    CAST(NULL AS ARRAY<STRUCT<v:DOUBLE>>),
    CAST(NULL AS STRUCT<a:ARRAY<DOUBLE>>)),
  ('null',
    named_struct('v', CAST(NULL AS DOUBLE)),
    array(CAST(NULL AS FLOAT)),
    array(CAST(NULL AS STRUCT<v:DOUBLE>)),
    named_struct('a', array(CAST(NULL AS DOUBLE)))),
  ('null',
    named_struct('v', CAST(NULL AS DOUBLE)),
    array(CAST(NULL AS FLOAT)),
    array(CAST(NULL AS STRUCT<v:DOUBLE>)),
    named_struct('a', array(CAST(NULL AS DOUBLE))))

query
SELECT grp,
  sort_array(collect_set(s)),
  sort_array(collect_set(a)),
  sort_array(collect_set(deep_a)),
  sort_array(collect_set(deep_s))
FROM (SELECT /*+ REPARTITION(3) */ * FROM cs_norm_nested)
GROUP BY grp
ORDER BY grp
