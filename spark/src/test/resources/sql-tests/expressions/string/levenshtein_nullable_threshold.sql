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
-- ConfigMatrix: spark.sql.codegen.factoryMode=FALLBACK,CODEGEN_ONLY,NO_CODEGEN

statement
CREATE TABLE levenshtein_nullable(s STRING, t STRING, threshold INT) USING parquet

statement
INSERT INTO levenshtein_nullable VALUES ('same', 'same', NULL), ('a', 'b', 2)

-- A nullable threshold stays with Spark, including the default FALLBACK mode where a
-- runtime code-generation failure can switch Spark from generated to interpreted results.
query expect_fallback(levenshtein with a nullable threshold requires Spark evaluation)
SELECT levenshtein(s, t, threshold) FROM levenshtein_nullable

-- The two-argument and known non-null threshold forms retain native execution.
query expect_native(levenshtein)
SELECT levenshtein(s, t), levenshtein(s, t, 2) FROM levenshtein_nullable
