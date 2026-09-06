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

-- A delimiter or replacement that can throw or carry state is routed to the codegen dispatcher,
-- because DataFusion evaluates every argument up front while Spark short-circuits past them
-- (#3178). These must return Spark's answers rather than raising INVALID_INDEX_OF_ZERO.

statement
CREATE TABLE test_aj_eager(arr array<string>, delims array<string>, nullrep string, flag boolean) USING parquet

statement
INSERT INTO test_aj_eager VALUES
  (NULL, array(','), NULL, true),
  (array('a', 'b'), array(','), NULL, true),
  (array('a', NULL, 'b'), array(','), 'X', true),
  (array('a', 'b'), array(','), 'X', false)

-- element_at is 1-based, so index 0 throws whenever it is evaluated. A *foldable* throwing
-- delimiter is pinned in CometArrayExpressionSuite instead: the dispatcher cannot compile that
-- shape today, and the same failure reproduces on main.
query
SELECT array_join(arr, element_at(delims, 0)) FROM test_aj_eager WHERE arr IS NULL

-- doGenCode evaluates the replacement before the delimiter, so a null replacement wins
query
SELECT array_join(arr, element_at(delims, 0), nullrep) FROM test_aj_eager WHERE nullrep IS NULL

-- a non-deterministic replacement is evaluated once per row by Spark
query
SELECT array_join(arr, ',', cast(monotonically_increasing_id() as string)) IS NOT NULL FROM test_aj_eager WHERE arr IS NOT NULL

-- the rows that do join still produce the right answer
query
SELECT array_join(arr, element_at(delims, 1), nullrep) FROM test_aj_eager WHERE arr IS NOT NULL
