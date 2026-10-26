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

-- Struct min/max over many small batches and many groups, so the accumulator carries each group's
-- winner from batch to batch instead of seeing all rows at once. range() produces non-nullable
-- values, so the struct stays Compatible.

-- Config: spark.comet.batchSize=64

query
SELECT g, min(struct(coalesce(id % 7, 0L) AS k, CAST(id AS STRING) AS s)),
  max(struct(coalesce(id % 7, 0L) AS k, CAST(id AS STRING) AS s))
FROM (SELECT id, id % 1000 AS g FROM range(0, 200000, 1, 8)) GROUP BY g ORDER BY g

query
SELECT max(struct(coalesce(id % 7, 0L) AS k, CAST(id AS STRING) AS s)) FROM range(0, 200000, 1, 8)

query
SELECT g, max(struct(coalesce(id % 7, 0L) AS k, CAST(id AS STRING) AS s)) FILTER (WHERE id % 3 = 0)
FROM (SELECT id, id % 1000 AS g FROM range(0, 200000, 1, 8)) GROUP BY g ORDER BY g
