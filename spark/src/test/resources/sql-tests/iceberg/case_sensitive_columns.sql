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

-- Columns `a` and `A` in one Iceberg table under spark.sql.caseSensitive=true. The native scan
-- has to bind each projected column and each filter to its own field rather than match names
-- case-insensitively (https://github.com/apache/datafusion-comet/issues/5540).
-- CometIcebergNativeSuite covers equality deletes, name mapping, and metadata columns.

-- No Iceberg spark-runtime is published for Spark 4.2 yet, and the 4.0 runtime the build reuses
-- is binary-incompatible with it. See https://github.com/apache/datafusion-comet/issues/4969.
-- MaxSparkVersion: 4.1

-- Config: spark.sql.catalog.test_cat=org.apache.iceberg.spark.SparkCatalog
-- Config: spark.sql.catalog.test_cat.type=hadoop
-- Config: spark.sql.catalog.test_cat.warehouse=/tmp/comet-iceberg-sql-test
-- Config: spark.comet.enabled=true
-- Config: spark.comet.exec.enabled=true
-- Config: spark.comet.scan.icebergNative.enabled=true
-- Config: spark.sql.caseSensitive=true

statement
DROP TABLE IF EXISTS test_cat.db.case_pair

statement
CREATE TABLE test_cat.db.case_pair (a INT, A INT) USING iceberg

-- `a` spans [1, 3] and `A` spans [20, 40], so a filter bound to the other column prunes the
-- whole row group or keeps the wrong row.
statement
INSERT INTO test_cat.db.case_pair VALUES (1, 20), (3, 40)

-- A plain query fails on any operator that is not Comet's, so each one also checks that the
-- Iceberg scan ran natively.
query
SELECT a, A FROM test_cat.db.case_pair ORDER BY a

query
SELECT A, a FROM test_cat.db.case_pair ORDER BY a

query
SELECT A FROM test_cat.db.case_pair ORDER BY A

query
SELECT a FROM test_cat.db.case_pair ORDER BY a

query
SELECT * FROM test_cat.db.case_pair ORDER BY a

query
SELECT a FROM test_cat.db.case_pair WHERE A = 40

query
SELECT A FROM test_cat.db.case_pair WHERE a = 3

query
SELECT A FROM test_cat.db.case_pair WHERE A = 40

query
SELECT a FROM test_cat.db.case_pair WHERE a = 3

query
SELECT a, A FROM test_cat.db.case_pair WHERE A = 40

query
SELECT a, A FROM test_cat.db.case_pair WHERE a = 3

query
SELECT a FROM test_cat.db.case_pair WHERE A > 30

query
SELECT a FROM test_cat.db.case_pair WHERE A IN (20)

query
SELECT A FROM test_cat.db.case_pair WHERE a < 2

statement
DROP TABLE test_cat.db.case_pair
