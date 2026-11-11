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

-- Spark evaluates a branch's value only for the rows that choose the branch, and a WHEN after
-- the first only for the rows that no earlier WHEN matched. Each query here has a branch or a WHEN
-- that would fail for a row that the WHEN before it rules out. With ANSI mode on, Comet has to
-- evaluate those only for the rows that reach them. With ANSI mode off they cannot fail, and
-- Comet evaluates them for every row of the batch instead.

-- ConfigMatrix: spark.sql.ansi.enabled=false,true

statement
CREATE TABLE test_case_lazy(a bigint, b bigint, i int, s string) USING parquet

statement
INSERT INTO test_case_lazy VALUES
  (10, 2, 1, '12'), (20, 0, 2147483647, 'x'), (9223372036854775807, 1, -5, '7'),
  (-9223372036854775808, 3, 0, NULL), (NULL, 0, NULL, '  3'), (5, NULL, 100, '2147483648'),
  (0, 0, -2147483648, ''), (-7, -1, 7, '-8')

-- a division by a zero that the WHEN excludes
query
SELECT a, b, CASE WHEN b <> 0 THEN a div b ELSE -1 END FROM test_case_lazy

query
SELECT a, b, IF(b = 0, NULL, a % b) FROM test_case_lazy

-- a later WHEN that divides, which only sees the rows the first WHEN does not match
query
SELECT a, b, CASE WHEN b = 0 OR b IS NULL THEN 0 WHEN a div b > 3 THEN 1 ELSE 2 END
FROM test_case_lazy

-- an overflow that the WHEN excludes
query
SELECT a, CASE WHEN a < 1000000 THEN a + 9000000000000000000 ELSE 0 END FROM test_case_lazy

query
SELECT i, IF(i > 0 AND i < 1000, i * 1000000, -1) FROM test_case_lazy

-- a cast that would fail for the strings the WHEN excludes
query
SELECT s, CASE WHEN s IN ('12', '7', '-8') THEN CAST(s AS INT) ELSE -1 END FROM test_case_lazy

-- the inner IF divides only for the rows that the outer CASE passes to it
query
SELECT a, b, CASE WHEN b IS NULL THEN NULL ELSE IF(b <> 0, a div b, 0) END FROM test_case_lazy

-- COALESCE evaluates an argument only for the rows where every earlier one is NULL
query
SELECT a, b, coalesce(IF(b = 0, 0, NULL), a div b) FROM test_case_lazy
