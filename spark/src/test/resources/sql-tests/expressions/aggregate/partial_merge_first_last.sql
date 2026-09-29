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

-- Exercise FIRST/LAST partial-state merging across multiple batches.
-- https://github.com/apache/datafusion-comet/issues/4131
-- Config: spark.comet.batchSize=128
-- Config: spark.sql.adaptive.coalescePartitions.enabled=true
-- Config: parquet.enable.dictionary=false

statement
CREATE TABLE pm_first_last(i int, grp int) USING parquet

statement
INSERT INTO pm_first_last
SELECT CAST(id AS int), CAST(id % 100 AS int) FROM range(10000)

-- Hash aggregation does not preserve input order. Use a value constant within
-- each group so FIRST/LAST agree regardless of the engines' processing order.
query
SELECT grp, first(grp), last(grp), count(DISTINCT i)
FROM pm_first_last GROUP BY grp ORDER BY grp
