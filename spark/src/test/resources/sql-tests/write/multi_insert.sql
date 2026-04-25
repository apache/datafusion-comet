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

-- Test multi-insert with Comet native writer (issue #3430, SPARK-48817)
-- Validates that data written via multi-insert SQL is correct when
-- the native writer handles ReusedExchangeExec in the plan.

-- Config: spark.comet.parquet.write.enabled=true
-- Config: spark.comet.operator.DataWritingCommandExec.allowIncompatible=true
-- Config: spark.comet.exec.enabled=true
-- Config: spark.sql.adaptive.enabled=false

statement
CREATE TABLE multi_src(c1 INT) USING PARQUET

statement
INSERT INTO multi_src VALUES (1), (2), (3), (4), (5)

statement
CREATE TABLE multi_dst1(c1 INT) USING PARQUET

statement
CREATE TABLE multi_dst2(c1 INT) USING PARQUET

-- Multi-insert: single plan with ReusedExchangeExec
statement
FROM (SELECT /*+ REPARTITION(3) */ c1 FROM multi_src)
INSERT OVERWRITE TABLE multi_dst1 SELECT c1
INSERT OVERWRITE TABLE multi_dst2 SELECT c1

-- Validate data in both destination tables
query
SELECT c1 FROM multi_dst1 ORDER BY c1

query
SELECT c1 FROM multi_dst2 ORDER BY c1

-- Verify both tables have equal content
query
SELECT count(*) FROM multi_dst1

query
SELECT count(*) FROM multi_dst2

-- Multi-insert with filtered inserts into different targets
statement
CREATE TABLE multi_dst3(c1 INT) USING PARQUET

statement
CREATE TABLE multi_dst4(c1 INT) USING PARQUET

statement
FROM (SELECT /*+ REPARTITION(2) */ c1 FROM multi_src)
INSERT OVERWRITE TABLE multi_dst3 SELECT c1 WHERE c1 <= 3
INSERT OVERWRITE TABLE multi_dst4 SELECT c1 WHERE c1 > 3

query
SELECT c1 FROM multi_dst3 ORDER BY c1

query
SELECT c1 FROM multi_dst4 ORDER BY c1
