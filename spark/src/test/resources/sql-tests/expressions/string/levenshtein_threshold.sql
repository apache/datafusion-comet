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

-- MinSparkVersion: 3.5

statement
CREATE TABLE test_levenshtein(s1 string, s2 string) USING parquet

statement
INSERT INTO test_levenshtein VALUES ('kitten', 'sitting'), ('frog', 'fog'), ('abc', 'abc'), ('', 'hello'), ('hello', ''), ('', ''), (NULL, 'test'), ('hello', NULL), (NULL, NULL)

-- three argument version with threshold
query
SELECT levenshtein('kitten', 'sitting', 2), levenshtein('kitten', 'sitting', 3), levenshtein('kitten', 'sitting', 4)

-- threshold with column arguments
query
SELECT levenshtein(s1, s2, 2) FROM test_levenshtein

-- threshold edge cases
query
SELECT levenshtein('abc', 'abc', 0), levenshtein('abc', 'adc', 0), levenshtein('', '', 0)

-- threshold with NULL
query
SELECT levenshtein('abc', 'adc', NULL), levenshtein(NULL, 'test', 2)
