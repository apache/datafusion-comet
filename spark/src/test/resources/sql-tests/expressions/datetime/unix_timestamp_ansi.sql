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
-- Config: spark.sql.legacy.timeParserPolicy=CORRECTED
-- Config: spark.comet.exec.scalaUDF.codegen.enabled=true

statement
CREATE TABLE test_unix_ts_ansi(s string, fmt string) USING parquet

statement
INSERT INTO test_unix_ts_ansi VALUES ('not a date', 'yyyy-MM-dd')

query expect_error(could not be parsed)
SELECT unix_timestamp(s, 'yyyy-MM-dd') FROM test_unix_ts_ansi

query expect_error(could not be parsed)
SELECT unix_timestamp(s, fmt) FROM test_unix_ts_ansi

query expect_error(could not be parsed)
SELECT unix_timestamp('2024-13-99', 'yyyy-MM-dd')

-- Valid queries require Comet execution, so fallback cannot hide an error-path regression.
query
SELECT unix_timestamp('2024-06-15', 'yyyy-MM-dd'), unix_timestamp(CAST(NULL AS STRING))

query
SELECT unix_timestamp('2024-06-15', fmt) FROM test_unix_ts_ansi
