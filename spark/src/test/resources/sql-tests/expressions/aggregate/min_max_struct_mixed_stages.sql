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

-- Struct min/max with its partial and final stages on different engines. The test-only configs
-- below keep one stage on Spark, so the struct buffer crosses between Spark and Comet in both
-- directions: native partial -> Spark final, and Spark partial -> native final. Min and Max allow
-- both. The plan is mixed by design, so only the answer is checked.

-- ConfigMatrix: spark.comet.testing.aggregate.partialMode.enabled=true,false
-- ConfigMatrix: spark.comet.testing.aggregate.finalMode.enabled=true,false

statement
CREATE TABLE test_mm_struct_mixed(g int, ts bigint, s string, x int) USING parquet

statement
INSERT INTO test_mm_struct_mixed VALUES
  (1, 5, 'b', 1), (1, 9, '日本語', 2), (1, 9, '中文', 3),
  (2, 9223372036854775807, 'a', 4), (2, -9223372036854775808, 'c', 5),
  (3, NULL, NULL, NULL)

query spark_answer_only
SELECT g, min(struct(coalesce(ts, 0L) AS ts, coalesce(s, '') AS s)),
  max(struct(coalesce(ts, 0L) AS ts, coalesce(s, '') AS s))
FROM test_mm_struct_mixed GROUP BY g ORDER BY g

query spark_answer_only
SELECT g, max(CASE WHEN ts > 0 THEN struct(coalesce(ts, 0L) AS ts, coalesce(s, '') AS s) END)
FROM test_mm_struct_mixed GROUP BY g ORDER BY g

query spark_answer_only
SELECT max(struct(coalesce(ts, 0L) AS ts, coalesce(s, '') AS s)) FROM test_mm_struct_mixed

-- count(DISTINCT) adds PartialMerge stages between the partial and final ones.
query spark_answer_only
SELECT g, count(DISTINCT s), min(struct(coalesce(ts, 0L) AS ts, coalesce(s, '') AS s))
FROM test_mm_struct_mixed GROUP BY g ORDER BY g

-- collect_list makes Spark plan an ObjectHashAggregate instead of a sort aggregate.
query spark_answer_only
SELECT g, max(struct(coalesce(ts, 0L) AS ts, coalesce(s, '') AS s)), sort_array(collect_list(x))
FROM test_mm_struct_mixed GROUP BY g ORDER BY g
