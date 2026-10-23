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

-- input_file_name, input_file_block_start and input_file_block_length read InputFileBlockHolder,
-- which Iceberg's Spark reader sets as it opens each data file. The native Iceberg scan does not,
-- so a plan that uses them falls back to Spark's reader
-- (https://github.com/apache/datafusion-comet/issues/6707).

-- Native Iceberg scan is unsupported on Spark 4.2: no Iceberg spark-runtime is published for
-- 4.2 yet. See https://github.com/apache/datafusion-comet/issues/4969.
-- MaxSparkVersion: 4.1

-- Config: spark.sql.catalog.test_cat=org.apache.iceberg.spark.SparkCatalog
-- Config: spark.sql.catalog.test_cat.type=hadoop
-- Config: spark.sql.catalog.test_cat.warehouse=/tmp/comet-iceberg-sql-test
-- Config: spark.comet.enabled=true
-- Config: spark.comet.exec.enabled=true
-- Config: spark.comet.scan.icebergNative.enabled=true

statement
DROP TABLE IF EXISTS test_cat.db.input_file

statement
CREATE TABLE test_cat.db.input_file (id BIGINT) USING iceberg

-- Three inserts write three data files
statement
INSERT INTO test_cat.db.input_file SELECT id FROM range(0, 1000)

statement
INSERT INTO test_cat.db.input_file SELECT id FROM range(1000, 2000)

statement
INSERT INTO test_cat.db.input_file SELECT id FROM range(2000, 3000)

query expect_fallback(Native V2 scan is not compatible with input_file_name)
SELECT input_file_name(), input_file_block_start(), input_file_block_length(), id
FROM test_cat.db.input_file

-- A Comet filter above the scan
query expect_fallback(Native V2 scan is not compatible with input_file_name)
SELECT input_file_name(), input_file_block_start(), input_file_block_length(), id
FROM test_cat.db.input_file WHERE id >= 0

-- Without these functions the scan stays native
query
SELECT id FROM test_cat.db.input_file WHERE id >= 0
