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

-- array_join runs natively by default (CometArrayJoin reports Compatible for non-collated
-- input); collated input still routes through the JVM codegen dispatcher and is covered by
-- array_join_collation.sql.
--
-- Spark's array_join semantics (#3178):
--   * without nullReplacement, null elements are skipped entirely
--   * with nullReplacement, null elements are replaced by that string
--   * a null array or a null delimiter yields null
--   * an empty array yields the empty string
-- Null placement matters: a leading or trailing null must not leave a dangling delimiter.

statement
CREATE TABLE test_array_join(arr array<string>, delim string, nullrep string) USING parquet

statement
INSERT INTO test_array_join VALUES
  (array('a', 'b', 'c'), ',', 'N'),
  (array('hello', 'world'), ' ', 'N'),
  (array(), ',', 'N'),
  (NULL, ',', 'N'),
  (array('a', NULL, 'c'), ',', 'N'),
  (array(NULL, 'b', 'c'), ',', 'N'),
  (array('a', 'b', NULL), ',', 'N'),
  (array(NULL), ',', 'N'),
  (array(NULL, NULL), ',', 'N'),
  (array('', 'b'), ',', 'N'),
  (array('a', 'b'), NULL, 'N')

-- column array, literal delimiter, no null replacement (nulls skipped)
query
SELECT array_join(arr, ',') FROM test_array_join

-- column array, literal delimiter and null replacement (nulls replaced)
query
SELECT array_join(arr, ',', 'NULL') FROM test_array_join

-- all three arguments as columns, including null delimiter and null replacement rows
query
SELECT array_join(arr, delim, nullrep) FROM test_array_join

-- column array with a column delimiter but a literal replacement
query
SELECT array_join(arr, delim, 'N') FROM test_array_join

-- multi-character and empty delimiters
query
SELECT array_join(arr, ' -- '), array_join(arr, '') FROM test_array_join

-- the exact cases named in #3178, as literals (constant folding is disabled by the suite)
query
SELECT array_join(array('a', NULL, 'b'), ','), array_join(array('a', NULL, 'b'), ',', 'X')

query
SELECT array_join(array('hello', NULL, 'world'), ' '), array_join(array('hello', NULL, 'world'), ' ', 'NULL')

-- literal null placement: leading, trailing, only-null, all-null
query
SELECT array_join(array(NULL, 'b'), ','), array_join(array('a', NULL), ','), array_join(cast(array(NULL) as array<string>), ','), array_join(cast(array(NULL, NULL) as array<string>), ',')

query
SELECT array_join(array(NULL, 'b'), ',', 'X'), array_join(array('a', NULL), ',', 'X'), array_join(cast(array(NULL) as array<string>), ',', 'X'), array_join(cast(array(NULL, NULL) as array<string>), ',', 'X')

-- empty string elements are not nulls and must survive both paths
query
SELECT array_join(array('', 'b'), ','), array_join(array('', NULL, 'b'), ','), array_join(array('', NULL, 'b'), ',', 'X')

-- empty array and null array as literals
query
SELECT array_join(cast(array() as array<string>), ','), array_join(cast(NULL as array<string>), ','), array_join(cast(array() as array<string>), ',', 'X'), array_join(cast(NULL as array<string>), ',', 'X')

-- null delimiter as a literal
query
SELECT array_join(array('a', 'b'), cast(NULL as string))
