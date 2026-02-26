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

-- Test regexp_replace() with regexp allowIncompatible enabled (happy path)
-- Config: spark.comet.expression.regexp.allowIncompatible=true
-- ConfigMatrix: parquet.enable.dictionary=false,true

statement
CREATE TABLE test_regexp_replace_enabled(s string) USING parquet

statement
INSERT INTO test_regexp_replace_enabled VALUES ('100-200'), ('abc'), (''), (NULL), ('phone 123-456-7890')

query
SELECT regexp_replace(s, '(\d+)', 'X') FROM test_regexp_replace_enabled

query
SELECT regexp_replace(s, '(\d+)', 'X', 1) FROM test_regexp_replace_enabled

-- literal + literal + literal
query
SELECT regexp_replace('100-200', '(\d+)', 'X'), regexp_replace('abc', '(\d+)', 'X'), regexp_replace(NULL, '(\d+)', 'X')
