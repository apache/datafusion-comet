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

-- elt executes Spark's generated code inside Comet's codegen dispatcher.
statement
CREATE TABLE test_elt_ansi(n int) USING parquet

statement
INSERT INTO test_elt_ansi VALUES (1), (2), (0), (-1), (3), (NULL)

-- Valid one-based indices, a NULL index and a selected NULL value stay inside Comet.
query
SELECT n, elt(n, 'a', 'b'), elt(n, 'a', CAST(NULL AS STRING))
FROM test_elt_ansi WHERE n IN (1, 2) OR n IS NULL

-- Zero, negative and past-the-end indices raise INVALID_ARRAY_INDEX under ANSI.
query expect_error(INVALID_ARRAY_INDEX)
SELECT elt(n, 'a', 'b') FROM test_elt_ansi WHERE n = 0

query expect_error(INVALID_ARRAY_INDEX)
SELECT elt(n, 'a', 'b') FROM test_elt_ansi WHERE n = -1

query expect_error(INVALID_ARRAY_INDEX)
SELECT elt(n, 'a', 'b') FROM test_elt_ansi WHERE n = 3

query expect_error(INVALID_ARRAY_INDEX)
SELECT elt(0, 'a', 'b')

query expect_error(INVALID_ARRAY_INDEX)
SELECT elt(-1, 'a', 'b')

query expect_error(INVALID_ARRAY_INDEX)
SELECT elt(3, 'a', 'b')
