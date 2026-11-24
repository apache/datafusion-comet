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

-- CometSort combines small input batches before sorting them. These queries give each sort task
-- tiny shuffle blocks of nested and boundary values and check that the combined result matches
-- Spark. CometTaskMetricsSuite covers the default gate, spilling and the config toggle.

-- Many small scan partitions, so each sort task reads small blocks from many map tasks.
-- Config: spark.sql.files.maxPartitionBytes=1024
-- Config: spark.sql.files.openCostInBytes=1
-- Config: spark.sql.shuffle.partitions=4
-- Config: spark.sql.adaptive.coalescePartitions.enabled=false
-- A combined batch holds at most this many rows.
-- Config: spark.comet.batchSize=64
-- Config: spark.comet.exec.respectDataFusionConfigs=true
-- 0 starts combining with the first block and makes the sort merge instead of sorting in place.
-- Config: spark.comet.datafusion.execution.sort_in_place_threshold_bytes=0
-- Write year 1 and year 9999 values without the legacy calendar rebase.
-- Config: spark.sql.parquet.datetimeRebaseModeInWrite=CORRECTED
-- Config: spark.sql.parquet.int96RebaseModeInWrite=CORRECTED
-- Config: spark.comet.exec.sort.coalesceInput.enabled=true
-- Config: spark.sql.session.timeZone=America/Los_Angeles

statement
CREATE TABLE sort_coalesce(
  id INT,
  k STRING,
  d DECIMAL(38, 10),
  ts TIMESTAMP,
  tsn TIMESTAMP_NTZ,
  dt DATE,
  s STRUCT<a: INT, m: MAP<STRING, INT>, l: ARRAY<STRUCT<x: BIGINT, y: STRING>>>,
  m MAP<STRING, ARRAY<INT>>,
  arr ARRAY<STRING>) USING parquet

-- 20 write tasks give 20 small files, so each sort task reads blocks of about 12 rows, well
-- under the size that gets combined.
statement
INSERT INTO sort_coalesce
SELECT
  CAST(id AS INT),
  CASE id % 7
    WHEN 0 THEN NULL
    WHEN 1 THEN concat('東京', id % 50)
    WHEN 2 THEN concat('Zürich-', id % 50)
    WHEN 3 THEN concat('москва', id % 50)
    WHEN 4 THEN concat('😀', id % 50)
    WHEN 5 THEN ''
    ELSE concat('k', id % 50)
  END,
  CASE id % 5
    WHEN 0 THEN CAST('999999999999999999999999999.9999999999' AS DECIMAL(38, 10))
    WHEN 1 THEN CAST('-999999999999999999999999999.9999999999' AS DECIMAL(38, 10))
    WHEN 2 THEN NULL
    ELSE CAST(id AS DECIMAL(38, 10)) / 7
  END,
  CASE id % 4
    WHEN 0 THEN TIMESTAMP '0001-01-01 00:00:00'
    WHEN 1 THEN TIMESTAMP '9999-12-31 23:59:59.999999'
    WHEN 2 THEN NULL
    ELSE timestamp_micros(id * 3600000000)
  END,
  CASE id % 4
    WHEN 0 THEN TIMESTAMP_NTZ '0001-01-01 00:00:00'
    WHEN 1 THEN TIMESTAMP_NTZ '9999-12-31 23:59:59.999999'
    WHEN 2 THEN NULL
    ELSE TIMESTAMP_NTZ '2024-03-10 02:30:00' + make_interval(0, 0, 0, 0, CAST(id AS INT), 0, 0)
  END,
  CASE id % 3
    WHEN 0 THEN DATE '0001-01-01'
    WHEN 1 THEN DATE '9999-12-31'
    ELSE date_add(DATE '1970-01-01', CAST(id AS INT))
  END,
  IF(id % 11 = 0, NULL, named_struct(
    'a', CAST(id % 97 AS INT),
    'm', map(concat('键', id % 3), CAST(id AS INT)),
    'l', array(
      named_struct('x', id, 'y', concat('值', id % 9)),
      named_struct('x', -id, 'y', CAST(NULL AS STRING))))),
  IF(id % 13 = 0, NULL, map('a', array(CAST(id AS INT), NULL), 'b', array())),
  IF(id % 17 = 0, NULL, array(concat('é', id % 5), NULL, 'z'))
FROM range(0, 1000, 1, 20)

-- A single string key with non-ASCII values, an empty string and nulls.
query
SELECT * FROM sort_coalesce ORDER BY k NULLS FIRST, id

query
SELECT * FROM sort_coalesce ORDER BY k DESC NULLS LAST, id DESC

-- Several keys of different types and directions.
query
SELECT id, dt, d, ts, tsn FROM sort_coalesce
ORDER BY dt DESC NULLS LAST, d ASC NULLS FIRST, tsn, id

-- Timestamp key, which depends on the session time zone only through the input values.
query
SELECT id, ts, k FROM sort_coalesce ORDER BY ts NULLS LAST, id

-- An expression over a struct field as the key, with the nested columns carried through.
query
SELECT id, s, m, arr FROM sort_coalesce ORDER BY s.a DESC NULLS FIRST, id

-- ORDER BY with LIMIT plans as a TopK, which is never combined. It shares the input above.
query
SELECT id, k, s FROM sort_coalesce ORDER BY k NULLS LAST, id LIMIT 25
