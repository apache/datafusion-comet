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

-- Config: spark.sql.ansi.enabled=false

-- Two identical nondeterministic dispatched subtrees in one projection must each keep their own
-- state, as they do in Spark. round over a double goes through the codegen dispatcher, so a and b
-- below are two copies of the same dispatched round(rand(1), 2). With ANSI off, Round carries no
-- query context, so both copies serialize to the same bytes; if they shared one kernel, b would
-- continue a's random sequence instead of restarting it.

-- One partition writes one file, so all 8 rows draw from a single seeded generator per column.
statement
CREATE TABLE test_round_rand(id bigint) USING parquet

statement
INSERT INTO test_round_rand SELECT id FROM range(0, 8, 1, 1)

query expect_dispatch(round, rand)
SELECT id, round(rand(1), 2) AS a, round(rand(1), 2) AS b FROM test_round_rand
