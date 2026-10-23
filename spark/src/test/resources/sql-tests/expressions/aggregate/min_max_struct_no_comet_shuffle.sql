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

-- Without Comet shuffle, the struct buffers of a native partial min/max cross a Spark shuffle,
-- so the partial and final can run on different engines. Min and Max allow both directions.
-- Which side each stage lands on depends on the planner, so only the answer is checked.

-- Config: spark.comet.shuffle.enabled=false
-- ConfigMatrix: spark.sql.execution.useObjectHashAggregateExec=true,false

statement
CREATE TABLE test_mm_struct_ns(g int, ts bigint, s string) USING parquet

statement
INSERT INTO test_mm_struct_ns VALUES
  (1, 5, 'b'), (1, 9, '日本語'), (1, 9, '中文'),
  (2, 9223372036854775807, 'a'), (2, -9223372036854775808, 'c'),
  (3, NULL, NULL)

query spark_answer_only
SELECT g, min(struct(coalesce(ts, 0L) AS ts, coalesce(s, '') AS s)),
  max(struct(coalesce(ts, 0L) AS ts, coalesce(s, '') AS s))
FROM test_mm_struct_ns GROUP BY g ORDER BY g

query spark_answer_only
SELECT g, max(CASE WHEN ts > 0 THEN struct(coalesce(ts, 0L) AS ts, coalesce(s, '') AS s) END)
FROM test_mm_struct_ns GROUP BY g ORDER BY g

query spark_answer_only
SELECT g, count(DISTINCT s), min(struct(coalesce(ts, 0L) AS ts, coalesce(s, '') AS s))
FROM test_mm_struct_ns GROUP BY g ORDER BY g
