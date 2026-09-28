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

-- A branch that fails for a row that chooses it still fails, and so does the first WHEN, which
-- Spark evaluates for every row. case_when_lazy.sql covers the rows that must not fail.

-- Config: spark.sql.ansi.enabled=true

statement
CREATE TABLE test_case_ansi(a bigint, b bigint) USING parquet

statement
INSERT INTO test_case_ansi VALUES (10, 2), (20, 0), (9223372036854775807, 1), (NULL, 0)

query expect_error(DIVIDE_BY_ZERO)
SELECT CASE WHEN a > 0 THEN a div b ELSE 0 END FROM test_case_ansi

query expect_error(DIVIDE_BY_ZERO)
SELECT CASE WHEN a div b > 0 THEN 1 ELSE 0 END FROM test_case_ansi

query expect_error(ARITHMETIC_OVERFLOW)
SELECT IF(a > 0, a + 9223372036854775000, 0) FROM test_case_ansi
