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

-- MinSparkVersion: 4.0

-- shuffle keeps its own random state whatever its argument holds. Its argument here is
-- non-deterministic only through spark_partition_id(), which holds no row state, and the operator
-- runs a codegen dispatcher kernel with a NullType result, so it falls back to Spark as on main.
-- shuffle takes a seed from Spark 4.0.

statement
CREATE TABLE test_nulltype_stateful_shuffle(id bigint) USING parquet

-- One file, so the rows share a partition and the random state.
statement
INSERT INTO test_nulltype_stateful_shuffle SELECT id FROM range(0, 64, 1, 1)

query expect_fallback(a non-deterministic expression that does not run entirely in the codegen dispatcher)
SELECT id, IF(id = 0, CAST(NULL AS BIGINT), id) + element_at(shuffle(array(id, CAST(spark_partition_id() AS BIGINT), id + 10), 42), 1) AS v, transform(array(id), x -> NULL) AS nulls FROM test_nulltype_stateful_shuffle
