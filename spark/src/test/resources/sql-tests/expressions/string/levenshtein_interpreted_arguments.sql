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
-- Config: spark.sql.ansi.enabled=true
-- Config: spark.sql.codegen.factoryMode=FALLBACK
-- Config: spark.sql.codegen.wholeStage=true
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true

statement
CREATE TABLE test_levenshtein_arguments(s1 string, s2 string, threshold int) USING parquet

statement
INSERT INTO test_levenshtein_arguments VALUES ('', '', NULL), ('a', 'b', NULL), ('a', 'b', 1)

-- ArrayFilter interprets its argument even when codegen is enabled. NULL thresholds become
-- zero, so array_compact must retain [0] and [-1]. Ordinary inputs still use the native fast path.
query
SELECT array_compact(array(levenshtein(s1 COLLATE UTF8_LCASE, s2, threshold))),
 array_compact(array(threshold, CAST(NULL AS INT))) FROM test_levenshtein_arguments

-- Imperative aggregates call eval on their argument: NULL thresholds become zero, yielding
-- distances 0, -1 and 1. Generating the argument instead would discard the first two values.
query expect_fallback(interpreted evaluation in aggregate arguments)
SELECT sort_array(collect_list(levenshtein(s1 COLLATE UTF8_LCASE, s2, threshold))) FROM test_levenshtein_arguments

-- Cover the ordinary imperative aggregate path as well as collect_list's object buffer.
query expect_fallback(interpreted evaluation in aggregate arguments)
SELECT approx_count_distinct(levenshtein(s1 COLLATE UTF8_LCASE, s2, threshold)) FROM test_levenshtein_arguments

-- A nonnullable threshold is safe for collect_list. SUM uses generated arguments in this mode,
-- so its nullable-threshold argument also stays accelerated.
query
SELECT sort_array(collect_list(levenshtein(s1 COLLATE UTF8_LCASE, s2, coalesce(threshold, 0)))),
 sum(levenshtein(s1 COLLATE UTF8_LCASE, s2, threshold)) FROM test_levenshtein_arguments
