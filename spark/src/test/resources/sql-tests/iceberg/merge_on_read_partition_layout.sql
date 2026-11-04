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

-- The native scan ships a partition value, delete file or residual once in the common block when
-- every Spark partition uses it, and otherwise with each Spark partition whose tasks use it (see
-- IcebergScanPools). Grouping the read by partition key makes every partition value and delete
-- file belong to one Spark partition. Without grouping, the tiny split size puts each data file
-- in its own Spark partition, so the two data files of a partition both get its value and its
-- delete file. The matrix runs both layouts against Spark, with and without a coalesce, which
-- puts several partitions' plan data in one task. Identity partition values become column
-- constants in the native scan, so a value resolved from the wrong pool entry shows up in the
-- results. CometIcebergNativeSuite checks what each partition ships.

-- No Iceberg spark-runtime is published for Spark 4.2 yet, and the 4.0 runtime the build reuses
-- is binary-incompatible with it. See https://github.com/apache/datafusion-comet/issues/4969.
-- MaxSparkVersion: 4.1

-- Config: spark.sql.catalog.test_cat=org.apache.iceberg.spark.SparkCatalog
-- Config: spark.sql.catalog.test_cat.type=hadoop
-- Config: spark.sql.catalog.test_cat.warehouse=/tmp/comet-iceberg-sql-test
-- Config: spark.comet.scan.icebergNative.enabled=true
-- Config: spark.sql.sources.v2.bucketing.enabled=true
-- Config: spark.sql.session.timeZone=America/Los_Angeles
-- ConfigMatrix: spark.sql.iceberg.planning.preserve-data-grouping=true,false

statement
DROP TABLE IF EXISTS test_cat.db.mor_partition_layout

-- Hash distribution writes one data file per partition for each insert, and one delete file per
-- partition for the delete.
statement
CREATE TABLE test_cat.db.mor_partition_layout (
  id INT,
  region STRING,
  d DATE,
  amount DECIMAL(18, 4),
  ts TIMESTAMP,
  ts_ntz TIMESTAMP_NTZ,
  payload STRING
) USING iceberg
PARTITIONED BY (region, d, amount, ts, ts_ntz)
TBLPROPERTIES (
  'format-version' = '2',
  'write.delete.mode' = 'merge-on-read',
  'write.delete.granularity' = 'partition',
  'write.distribution-mode' = 'hash',
  'write.delete.distribution-mode' = 'hash',
  'read.split.target-size' = '1',
  'read.split.open-file-cost' = '1')

-- Five partitions: non-ASCII strings, dates and timestamps before 1970, the epoch, a leap day,
-- both ends of the decimal range, timestamps in a DST gap and overlap of the session timezone,
-- and a partition where every value is null. Each insert writes three rows to each partition.
statement
INSERT INTO test_cat.db.mor_partition_layout
SELECT CAST(t.k * 3 + r.id + 1 AS INT), t.region, t.d, t.amount, t.ts, t.ts_ntz,
  concat('p', CAST(t.k * 3 + r.id + 1 AS STRING))
FROM VALUES
  (0, '東京', DATE '1969-12-31', CAST(-99999999999999.9999 AS DECIMAL(18, 4)),
    TIMESTAMP '1969-07-20 20:17:40', TIMESTAMP_NTZ '1900-01-01 00:00:00'),
  (1, 'Zürich', DATE '1900-01-01', CAST(0.0001 AS DECIMAL(18, 4)),
    TIMESTAMP '2024-03-10 02:30:00', TIMESTAMP_NTZ '2024-03-10 02:30:00'),
  (2, 'São Paulo', DATE '2024-02-29', CAST(99999999999999.9999 AS DECIMAL(18, 4)),
    TIMESTAMP '2024-11-03 01:30:00', TIMESTAMP_NTZ '2024-11-03 01:30:00'),
  (3, 'eng', DATE '1970-01-01', CAST(0 AS DECIMAL(18, 4)),
    TIMESTAMP '1970-01-01 00:00:00', TIMESTAMP_NTZ '1970-01-01 00:00:00'),
  (4, CAST(NULL AS STRING), CAST(NULL AS DATE), CAST(NULL AS DECIMAL(18, 4)),
    CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP_NTZ))
  AS t(k, region, d, amount, ts, ts_ntz)
CROSS JOIN range(3) AS r

statement
INSERT INTO test_cat.db.mor_partition_layout
SELECT CAST(15 + t.k * 3 + r.id + 1 AS INT), t.region, t.d, t.amount, t.ts, t.ts_ntz,
  concat('p', CAST(15 + t.k * 3 + r.id + 1 AS STRING))
FROM VALUES
  (0, '東京', DATE '1969-12-31', CAST(-99999999999999.9999 AS DECIMAL(18, 4)),
    TIMESTAMP '1969-07-20 20:17:40', TIMESTAMP_NTZ '1900-01-01 00:00:00'),
  (1, 'Zürich', DATE '1900-01-01', CAST(0.0001 AS DECIMAL(18, 4)),
    TIMESTAMP '2024-03-10 02:30:00', TIMESTAMP_NTZ '2024-03-10 02:30:00'),
  (2, 'São Paulo', DATE '2024-02-29', CAST(99999999999999.9999 AS DECIMAL(18, 4)),
    TIMESTAMP '2024-11-03 01:30:00', TIMESTAMP_NTZ '2024-11-03 01:30:00'),
  (3, 'eng', DATE '1970-01-01', CAST(0 AS DECIMAL(18, 4)),
    TIMESTAMP '1970-01-01 00:00:00', TIMESTAMP_NTZ '1970-01-01 00:00:00'),
  (4, CAST(NULL AS STRING), CAST(NULL AS DATE), CAST(NULL AS DECIMAL(18, 4)),
    CAST(NULL AS TIMESTAMP), CAST(NULL AS TIMESTAMP_NTZ))
  AS t(k, region, d, amount, ts, ts_ntz)
CROSS JOIN range(3) AS r

-- Removes one row from each data file, so each partition's delete file covers both of its data
-- files.
statement
DELETE FROM test_cat.db.mor_partition_layout WHERE id % 3 = 2

query
SELECT * FROM test_cat.db.mor_partition_layout ORDER BY id

query
SELECT id, _partition FROM test_cat.db.mor_partition_layout ORDER BY id

query
SELECT region, d, amount, ts, ts_ntz, count(*) AS n, sum(id) AS s
FROM test_cat.db.mor_partition_layout
GROUP BY region, d, amount, ts, ts_ntz
ORDER BY s

query
SELECT count(*), sum(id) FROM test_cat.db.mor_partition_layout

-- Every task carries the same residual on a data column.
query
SELECT id, region, amount FROM test_cat.db.mor_partition_layout
WHERE payload IN ('p1', 'p17', 'p29')
ORDER BY id

-- The residual depends on the partition: true where region matches, id > 25 elsewhere.
query
SELECT id, region, d FROM test_cat.db.mor_partition_layout
WHERE region = '東京' OR id > 25
ORDER BY id

query
SELECT id, d, ts FROM test_cat.db.mor_partition_layout
WHERE d < DATE '1970-01-01'
ORDER BY id

query
SELECT id, payload FROM test_cat.db.mor_partition_layout
WHERE region IS NULL
ORDER BY id

-- A coalesce without a shuffle runs several partitions in one task.
query
SELECT /*+ COALESCE(1) */ * FROM test_cat.db.mor_partition_layout ORDER BY id

query
SELECT /*+ COALESCE(2) */ id, _partition FROM test_cat.db.mor_partition_layout
WHERE region = '東京' OR id > 25
ORDER BY id

statement
DROP TABLE test_cat.db.mor_partition_layout
