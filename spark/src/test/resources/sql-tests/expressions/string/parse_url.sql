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

-- ConfigMatrix: parquet.enable.dictionary=false,true
-- MinSparkVersion: 3.4

statement
CREATE TABLE test_parse_url(url string) USING parquet

statement
INSERT INTO test_parse_url VALUES
  ('http://spark.apache.org/path?query=1'),
  ('https://spark.apache.org/path/to/page?query=1&k2=v2'),
  ('ftp://user:pwd@ftp.example.com:2121/files?x=1#frag'),
  (NULL)

query
SELECT parse_url(url, 'HOST') FROM test_parse_url

query
SELECT parse_url(url, 'QUERY') FROM test_parse_url

query
SELECT parse_url(url, 'PROTOCOL') FROM test_parse_url

query
SELECT parse_url(url, 'QUERY', 'query'), parse_url(url, 'QUERY', 'k2') FROM test_parse_url

query
SELECT parse_url(url, 'PATH') FROM test_parse_url

query
SELECT parse_url(url, 'FILE') FROM test_parse_url

query
SELECT parse_url(url, 'REF') FROM test_parse_url

query
SELECT parse_url(url, 'AUTHORITY') FROM test_parse_url

query
SELECT parse_url(url, 'USERINFO') FROM test_parse_url

-- Literal arguments: exercises the constant-folding code path which may differ from column inputs
query
SELECT parse_url('http://spark.apache.org/path?query=1', 'HOST')

query
SELECT parse_url('http://spark.apache.org/path?query=1', 'QUERY')

query
SELECT parse_url('http://spark.apache.org/path?query=1', 'QUERY', 'query')

query
SELECT parse_url('http://spark.apache.org/path?query=1', 'PROTOCOL')

query
SELECT parse_url('http://spark.apache.org/path?query=1', 'PATH')

query
SELECT parse_url('http://spark.apache.org/path?query=1', 'FILE')

query
SELECT parse_url('ftp://user:pwd@ftp.example.com:2121/files?x=1#frag', 'REF')

query
SELECT parse_url('ftp://user:pwd@ftp.example.com:2121/files?x=1#frag', 'AUTHORITY')

query
SELECT parse_url('ftp://user:pwd@ftp.example.com:2121/files?x=1#frag', 'USERINFO')

-- Note: try_parse_url is a Comet-internal DataFusion function name used when serializing
-- parse_url with failOnError=false. It is not a registered Spark SQL function and cannot
-- be called directly from SQL. The NULL-on-error behaviour is covered by the
-- "parse_url with invalid URL in legacy mode" Scala test.
