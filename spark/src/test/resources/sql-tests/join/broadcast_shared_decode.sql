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

-- Compare broadcast join results with Spark whether the tasks of an executor that read the
-- broadcast share one decoded copy of it or decode it for themselves. The repartition gives the
-- probe side eight tasks, which read the broadcast at once, and the build side has struct and
-- array columns as well as nulls.
-- Config: spark.sql.adaptive.enabled=false
-- Config: spark.sql.autoBroadcastJoinThreshold=-1
-- ConfigMatrix: spark.comet.exec.broadcastExchange.shareDecoded.enabled=true,false

statement
CREATE TABLE shared_fact(k INT, payload STRING) USING parquet

statement
INSERT INTO shared_fact SELECT CAST(id % 400 AS INT), concat('fact-', id) FROM range(4000)

statement
CREATE TABLE shared_dim(k INT, name STRING, pair STRUCT<a: INT, b: STRING>, nums ARRAY<INT>) USING parquet

statement
INSERT INTO shared_dim
SELECT CAST(id AS INT), IF(id % 9 = 0, NULL, concat('dim-', id)),
  named_struct('a', CAST(id AS INT), 'b', concat('s', id)),
  array(CAST(id AS INT), CAST(id + 1 AS INT))
FROM range(300)

query
SELECT /*+ BROADCAST(d) */ *
FROM (SELECT /*+ REPARTITION(8) */ * FROM shared_fact) f JOIN shared_dim d ON f.k = d.k

query
SELECT /*+ BROADCAST(d) */ *
FROM (SELECT /*+ REPARTITION(8) */ * FROM shared_fact) f LEFT JOIN shared_dim d ON f.k = d.k

query
SELECT /*+ BROADCAST(d) */ *
FROM (SELECT /*+ REPARTITION(8) */ * FROM shared_fact) f LEFT SEMI JOIN shared_dim d ON f.k = d.k

query
SELECT /*+ BROADCAST(d) */ *
FROM (SELECT /*+ REPARTITION(8) */ * FROM shared_fact) f LEFT ANTI JOIN shared_dim d ON f.k = d.k

query
SELECT /*+ BROADCAST(d) */ *
FROM shared_dim d RIGHT JOIN (SELECT /*+ REPARTITION(8) */ * FROM shared_fact) f ON f.k = d.k

query
SELECT /*+ BROADCAST(d) */ *
FROM (SELECT /*+ REPARTITION(8) */ * FROM shared_fact) f JOIN shared_dim d ON f.k < d.k AND d.k < 5
