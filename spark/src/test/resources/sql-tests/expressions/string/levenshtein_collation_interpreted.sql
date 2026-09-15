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
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true
-- Config: spark.sql.codegen.factoryMode=NO_CODEGEN
-- Config: spark.sql.codegen.wholeStage=false

-- Spark's interpreted Levenshtein unboxes a NULL threshold as zero, while its generated
-- code returns NULL. Keep nullable thresholds on Spark when interpreted execution is requested.
statement
CREATE TABLE test_levenshtein_collated_interpreted(s1 string, s2 string, threshold int) USING parquet

statement
INSERT INTO test_levenshtein_collated_interpreted VALUES ('', '', NULL), ('a', 'b', NULL), ('kitten', 'sitting', 3)

query expect_fallback(NO_CODEGEN)
SELECT levenshtein(s1 COLLATE UTF8_LCASE, s2, threshold) FROM test_levenshtein_collated_interpreted

-- Hypot dispatches its whole subtree, so a check only on the Levenshtein serde is insufficient.
query expect_fallback(NO_CODEGEN)
SELECT hypot(levenshtein(s1 COLLATE UTF8_LCASE, s2, threshold), 1.0) FROM test_levenshtein_collated_interpreted

-- Both safe forms still use the dispatcher in this mode.
query
SELECT levenshtein(s1 COLLATE UTF8_LCASE, s2), levenshtein(s1 COLLATE UTF8_LCASE, s2, coalesce(threshold, 0)) FROM test_levenshtein_collated_interpreted
