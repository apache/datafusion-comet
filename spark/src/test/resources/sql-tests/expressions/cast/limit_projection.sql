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

-- Config: spark.sql.ansi.enabled=true
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true
-- OFFSET needs its limit + offset folded, while a failing conditional cast stays unfolded.
-- ConstantFolding: enabled
-- ConfigMatrix: spark.sql.adaptive.enabled=false,true
-- ConfigMatrix: spark.comet.batchSize=2,8192

statement
CREATE TABLE limit_projection(id BIGINT, value INT) USING parquet

statement
INSERT INTO limit_projection SELECT /*+ COALESCE(1) */ * FROM VALUES
  (0, 0), (1, 1), (2, 2), (3, 3), (4, 4)

-- The outer predicate keeps LIMIT inside a subquery, on the non-collect path.
query expect_dispatch(cast)
SELECT parsed FROM (
  SELECT IF(id = 1L, CAST('bad' AS INT), value) AS parsed
  FROM limit_projection LIMIT 1
) q WHERE parsed >= 0

query expect_error_class(CAST_INVALID_INPUT)
SELECT parsed FROM (
  SELECT IF(id = 1L, CAST('bad' AS INT), value) AS parsed
  FROM limit_projection LIMIT 2
) q WHERE parsed >= 0

-- A batch size of two makes the cap stop partway through the second batch.
query expect_dispatch(cast)
SELECT parsed FROM (
  SELECT IF(id = 3L, CAST('bad' AS INT), value) AS parsed
  FROM limit_projection LIMIT 3
) q WHERE parsed >= 0

query expect_error_class(CAST_INVALID_INPUT)
SELECT parsed FROM (
  SELECT IF(id = 3L, CAST('bad' AS INT), value) AS parsed
  FROM limit_projection LIMIT 4
) q WHERE parsed >= 0

-- The cap includes the OFFSET prefix, but stops before the following invalid row.
query expect_dispatch(cast)
SELECT parsed FROM (
  SELECT IF(id = 3L, CAST('bad' AS INT), value) AS parsed
  FROM limit_projection LIMIT 2 OFFSET 1
) q WHERE parsed >= 0

query expect_error_class(CAST_INVALID_INPUT)
SELECT parsed FROM (
  SELECT IF(id = 3L, CAST('bad' AS INT), value) AS parsed
  FROM limit_projection LIMIT 3 OFFSET 1
) q WHERE parsed >= 0

-- The projection precedes the shuffle, so Spark also evaluates the skipped prefix.
query expect_error_class(CAST_INVALID_INPUT)
SELECT parsed FROM (
  SELECT IF(id = 0L, CAST('bad' AS INT), value) AS parsed
  FROM limit_projection LIMIT 2 OFFSET 1
) q WHERE parsed >= 0

-- A consumed valid value retains the native projection and matches Spark.
query
SELECT parsed FROM (
  SELECT IF(id = 3L, 1, value) AS parsed
  FROM limit_projection LIMIT 3 OFFSET 1
) q WHERE parsed >= 0
