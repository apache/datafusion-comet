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

-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true
-- Config: spark.comet.expression.RLike.allowIncompatible=false
-- Config: spark.comet.batchSize=64

statement
CREATE TABLE test_rlike_java_batches(id int, s string) USING parquet

-- One input partition ensures the 5,000 rows span multiple 64-row Arrow batches.
-- Matching, nonmatching and NULL subjects recur across the batch boundaries.
statement
INSERT INTO test_rlike_java_batches
SELECT CAST(id AS INT), CASE
  WHEN id % 7 = 0 THEN NULL
  WHEN id % 4 = 0 THEN 'aa'
  WHEN id % 4 = 1 THEN 'bb'
  WHEN id % 4 = 2 THEN 'ab'
  ELSE concat('row_', CAST(id AS STRING), '_aa')
END
FROM range(0, 5000, 1, 1)

query expect_dispatch(rlike)
SELECT id, s, s RLIKE r'^(\w)\1$' FROM test_rlike_java_batches

query expect_dispatch(rlike)
SELECT id, s FROM test_rlike_java_batches WHERE s RLIKE r'^(\w)\1$'
