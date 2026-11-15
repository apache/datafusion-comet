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

-- AQE must not split a single-partition input of a native CartesianProductExec. REBALANCE(1)
-- reports one partition, so the product goes native, and the tiny advisory size makes
-- OptimizeSkewInRebalancePartitions try to split it afterwards. Spark ignores REBALANCE when
-- AQE is off, so this file only runs with AQE on.

-- Config: spark.comet.exec.cartesianProduct.enabled=true
-- Config: spark.sql.autoBroadcastJoinThreshold=-1
-- Config: spark.sql.adaptive.autoBroadcastJoinThreshold=-1
-- Config: spark.sql.adaptive.enabled=true
-- Config: spark.sql.adaptive.advisoryPartitionSizeInBytes=1k

statement
CREATE TABLE cpa_large(k int, v int) USING parquet

-- Far more data than the 1k advisory size, so AQE wants to split the REBALANCE(1) partition.
statement
INSERT INTO cpa_large SELECT id, id FROM range(10000)

statement
CREATE TABLE cpa_right(k int, v int) USING parquet

statement
INSERT INTO cpa_right VALUES (0, 100), (NULL, NULL)

query
SELECT * FROM (SELECT /*+ REBALANCE(1) */ * FROM cpa_large) l
CROSS JOIN (SELECT /*+ COALESCE(1) */ * FROM cpa_right) r
