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

-- Test regexp_extract default behaviour: Comet marks the expression Incompatible
-- (Rust regex engine differs from Java) and should fall back to Spark unless the user
-- opts in via spark.comet.expression.RegExpExtract.allowIncompatible=true.

statement
CREATE TABLE test_regexp_extract(s string) USING parquet

statement
INSERT INTO test_regexp_extract VALUES ('100-200'), ('abc'), (''), (NULL), ('phone 123-456-7890')

query expect_fallback(Rust regexp engine)
SELECT regexp_extract(s, '(\\d+)-(\\d+)', 1) FROM test_regexp_extract

query expect_fallback(Rust regexp engine)
SELECT regexp_extract(s, '(\\d+)-(\\d+)', 2) FROM test_regexp_extract

query expect_fallback(Rust regexp engine)
SELECT regexp_extract(s, '(\\d+)-(\\d+)') FROM test_regexp_extract
