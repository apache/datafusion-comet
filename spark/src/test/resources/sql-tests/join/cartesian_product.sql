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

-- Tests for CartesianProductExec, which Spark plans for a cross join with no equi key when
-- nothing is broadcast. Comet runs it natively only for a condition-free product of
-- single-partition inputs. A file scan reports UnknownPartitioning(0), so each side is
-- coalesced to one partition. AQE is off so it cannot coalesce the REPARTITION(2) case.

-- Config: spark.comet.exec.cartesianProduct.enabled=true
-- Config: spark.sql.autoBroadcastJoinThreshold=-1
-- Config: spark.sql.adaptive.autoBroadcastJoinThreshold=-1
-- Config: spark.sql.adaptive.enabled=false

statement
CREATE TABLE cp_left(k int, v int) USING parquet

statement
INSERT INTO cp_left VALUES (0, 10), (NULL, 11), (2, NULL), (NULL, NULL)

statement
CREATE TABLE cp_right(k int, v int) USING parquet

statement
INSERT INTO cp_right VALUES (0, 100), (NULL, NULL)

statement
CREATE TABLE cp_empty(k int, v int) USING parquet

statement
CREATE TABLE cp_large(k int, v int) USING parquet

-- More rows than the default batch size of 8192.
statement
INSERT INTO cp_large SELECT id, id FROM range(10000)

-- Native, with NULLs on both sides
query
SELECT * FROM (SELECT /*+ COALESCE(1) */ * FROM cp_left) l
CROSS JOIN (SELECT /*+ COALESCE(1) */ * FROM cp_right) r

-- Native, empty left side (the side CrossJoinExec buffers)
query
SELECT * FROM (SELECT /*+ COALESCE(1) */ * FROM cp_empty) l
CROSS JOIN (SELECT /*+ COALESCE(1) */ * FROM cp_right) r

-- Native, empty right side
query
SELECT * FROM (SELECT /*+ COALESCE(1) */ * FROM cp_left) l
CROSS JOIN (SELECT /*+ COALESCE(1) */ * FROM cp_empty) r

-- Native, right side spanning several batches
query
SELECT * FROM (SELECT /*+ COALESCE(1) */ * FROM cp_left) l
CROSS JOIN (SELECT /*+ COALESCE(1) */ * FROM cp_large) r

-- A join condition falls back to Spark
query expect_fallback(CartesianProduct with a join condition is not supported natively)
SELECT * FROM (SELECT /*+ COALESCE(1) */ * FROM cp_left) l
JOIN (SELECT /*+ COALESCE(1) */ * FROM cp_right) r ON l.v > r.v

-- A multi-partition input falls back to Spark
query expect_fallback(Native CartesianProduct requires single-partition inputs)
SELECT * FROM (SELECT /*+ REPARTITION(2) */ * FROM cp_left) l
CROSS JOIN (SELECT /*+ COALESCE(1) */ * FROM cp_right) r
