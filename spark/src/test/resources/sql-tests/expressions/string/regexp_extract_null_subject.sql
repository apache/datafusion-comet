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

-- Config: spark.comet.expression.RegExpExtract.allowIncompatible=true
-- Config: spark.comet.expression.RegExpExtractAll.allowIncompatible=true
-- ConfigMatrix: spark.sql.ansi.enabled=false,true

statement
CREATE TABLE test_regexp_extract_null_subject(s string) USING parquet

statement
INSERT INTO test_regexp_extract_null_subject VALUES (NULL), ('abc'), (NULL)

-- Spark compiles the pattern only for a row whose subject is not NULL, so a pattern that does
-- not compile is not an error when every subject is NULL. '(unclosed' is invalid in Java and
-- Rust, and the lookahead is valid in Java but not in Rust.
query expect_native(regexp_extract)
SELECT regexp_extract(s, '(unclosed', 1), regexp_extract(s, '(?=\\d)\\w+', 0)
FROM test_regexp_extract_null_subject WHERE s IS NULL

query expect_native(regexp_extract_all)
SELECT regexp_extract_all(s, '(unclosed', 1), regexp_extract_all(s, '(?=\\d)\\w+', 0)
FROM test_regexp_extract_null_subject WHERE s IS NULL

-- A non-NULL subject still fails on a pattern that does not compile
query expect_error(regexp_extract)
SELECT regexp_extract(s, '(unclosed', 1) FROM test_regexp_extract_null_subject

query expect_error(regexp_extract_all)
SELECT regexp_extract_all(s, '(unclosed', 1) FROM test_regexp_extract_null_subject
