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

-- Exercises the native parse_url implementation. Inputs are restricted to
-- URLs with explicit paths because the native implementation diverges from
-- Spark for empty-string input and for FILE extraction on path-less URLs.
-- Tracked upstream at https://github.com/apache/datafusion/issues/21943.

-- Config: spark.comet.expression.ParseUrl.allowIncompatible=true
-- Config: spark.sql.ansi.enabled=true

statement
CREATE TABLE test_urls_native(url string) USING parquet

statement
INSERT INTO test_urls_native VALUES
  ('http://spark.apache.org/path?query=1'),
  ('http://user:password@host:8080/path?key=value&key2=value2#ref'),
  ('http://example.com/path'),
  (NULL)

query
SELECT parse_url(url, 'HOST') FROM test_urls_native

query
SELECT parse_url(url, 'PATH') FROM test_urls_native

query
SELECT parse_url(url, 'QUERY') FROM test_urls_native

query
SELECT parse_url(url, 'REF') FROM test_urls_native

query
SELECT parse_url(url, 'PROTOCOL') FROM test_urls_native

query
SELECT parse_url(url, 'FILE') FROM test_urls_native

query
SELECT parse_url(url, 'AUTHORITY') FROM test_urls_native

query
SELECT parse_url(url, 'USERINFO') FROM test_urls_native

query
SELECT parse_url(url, 'QUERY', 'query') FROM test_urls_native

query
SELECT parse_url(url, 'QUERY', 'key') FROM test_urls_native

query
SELECT parse_url(url, 'QUERY', 'key2') FROM test_urls_native

query
SELECT parse_url(url, 'QUERY', 'nonexistent') FROM test_urls_native

query
SELECT parse_url('http://spark.apache.org/path?query=1', 'HOST')

query
SELECT parse_url('http://spark.apache.org/path?query=1', 'QUERY', 'query')

query
SELECT parse_url(NULL, 'HOST')

-- ANSI-mode invalid URL: parse_url's failOnError is driven by spark.sql.ansi.enabled
-- (set above). Both Spark (INVALID_URL error class) and Comet's native impl
-- produce a message starting "The url is invalid".
query expect_error(The url is invalid)
SELECT parse_url('inva lid://user:pass@host/file', 'HOST')
