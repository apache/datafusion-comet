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

-- to_csv runs through the codegen dispatcher by default so results match Spark exactly, including
-- quoting and escaping. The native path is opt-in via
-- spark.comet.expression.StructsToCsv.allowIncompatible.

statement
CREATE TABLE test_to_csv(a int, b string, c double) USING parquet

statement
INSERT INTO test_to_csv VALUES
  (1, 'x', 2.5),
  (-3, 'hello,world', 0.0),
  (0, 'has "quote"', -1.5),
  (NULL, NULL, NULL),
  (7, '', 3.0)

-- column struct: values with delimiters and quotes exercise Spark's CSV quoting rules
query
SELECT to_csv(named_struct('a', a, 'b', b, 'c', c)) FROM test_to_csv

-- literal struct (constant folding is disabled by the test suite)
query
SELECT
  to_csv(named_struct('a', 1, 'b', 'x', 'c', 2.5)),
  to_csv(named_struct('s', 'a,b', 'n', CAST(NULL AS INT)))
