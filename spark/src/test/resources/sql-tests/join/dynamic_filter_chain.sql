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

-- Compare complete join results with Spark with runtime filtering enabled and disabled.
-- Keep the join order fixed so both broadcast build sides and stacked ancestor consumers run.
-- Config: spark.sql.adaptive.enabled=false
-- Config: spark.sql.autoBroadcastJoinThreshold=-1
-- Config: spark.sql.cbo.joinReorder.enabled=false
-- Config: spark.comet.batchSize=16
-- ConfigMatrix: spark.comet.exec.join.dynamicFilter.enabled=false,true

statement
CREATE TABLE chain_fact(k INT, payload INT) USING parquet

statement
INSERT INTO chain_fact VALUES (1, 10), (1, 11), (2, 20), (3, 30), (4, 40), (NULL, 50)

statement
CREATE TABLE chain_dimension(k INT, detail INT) USING parquet

statement
INSERT INTO chain_dimension VALUES (1, 100), (1, 101), (2, 200), (3, 300), (NULL, 500)

statement
CREATE TABLE chain_selection(k INT, selected INT) USING parquet

statement
INSERT INTO chain_selection VALUES (1, 1000), (1, 1001), (2, 2000), (NULL, 5000)

statement
CREATE TABLE chain_final(k INT, selected INT) USING parquet

statement
INSERT INTO chain_final VALUES (1, 10000), (1, 10001), (3, 30000), (NULL, 50000)

-- Duplicate keys multiply matches, while NULL keys never match each other.
query
SELECT /*+ BROADCAST(d), BROADCAST(s) */ f.*, d.detail, s.selected
FROM chain_fact f JOIN chain_dimension d ON f.k = d.k
JOIN chain_selection s ON f.k = s.k

query
SELECT /*+ BROADCAST(d), BROADCAST(s) */ f.*, d.detail, s.selected
FROM chain_dimension d JOIN chain_fact f ON f.k = d.k
JOIN chain_selection s ON f.k = s.k

-- Two ancestors install early consumers below the innermost join.
query
SELECT /*+ BROADCAST(d), BROADCAST(s), BROADCAST(t) */ f.*, d.detail, s.selected, t.selected
FROM chain_fact f JOIN chain_dimension d ON f.k = d.k
JOIN chain_selection s ON f.k = s.k
JOIN chain_final t ON f.k = t.k

query
SELECT /*+ BROADCAST(d), BROADCAST(s), BROADCAST(t) */ f.*, d.detail, s.selected, t.selected
FROM chain_dimension d JOIN chain_fact f ON f.k = d.k
JOIN chain_selection s ON f.k = s.k
JOIN chain_final t ON f.k = t.k
