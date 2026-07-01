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

-- Test parse_url() - native execution (Compatible)

statement
CREATE TABLE test_parse_url(url string) USING parquet

statement
INSERT INTO test_parse_url VALUES
  ('http://spark.apache.org/path?query=1'),
  ('https://user:pass@host:8080/path?k=v#ref'),
  ('http://example.com/path?a=1&b=2&a=3'),
  ('ftp://ftp.example.com/dir/file.txt'),
  (NULL)

-- HOST
query
SELECT parse_url(url, 'HOST') FROM test_parse_url

-- PATH
query
SELECT parse_url(url, 'PATH') FROM test_parse_url

-- QUERY (no key)
query
SELECT parse_url(url, 'QUERY') FROM test_parse_url

-- QUERY with key
query
SELECT parse_url(url, 'QUERY', 'k') FROM test_parse_url

-- PROTOCOL
query
SELECT parse_url(url, 'PROTOCOL') FROM test_parse_url

-- REF (fragment)
query
SELECT parse_url(url, 'REF') FROM test_parse_url

-- AUTHORITY
query
SELECT parse_url(url, 'AUTHORITY') FROM test_parse_url

-- USERINFO
query
SELECT parse_url(url, 'USERINFO') FROM test_parse_url

-- FILE
query
SELECT parse_url(url, 'FILE') FROM test_parse_url

-- literal arguments
query
SELECT parse_url('http://example.com/path?foo=bar', 'HOST')

query
SELECT parse_url('http://example.com/path?foo=bar', 'PATH')

query
SELECT parse_url('http://example.com/path?foo=bar', 'QUERY')

query
SELECT parse_url('http://example.com/path?foo=bar', 'QUERY', 'foo')

query
SELECT parse_url('http://example.com/path?foo=bar', 'PROTOCOL')

-- NULL handling
query
SELECT parse_url(NULL, 'HOST')

query
SELECT parse_url('http://example.com', NULL)

-- invalid part key
query
SELECT parse_url('http://example.com', 'INVALID')

-- malformed URL returns NULL
query
SELECT parse_url('not a url at all', 'HOST')

query
SELECT parse_url('', 'HOST')

-- column-valued part key
statement
CREATE TABLE test_parse_url_parts(url string, part string, key string) USING parquet

statement
INSERT INTO test_parse_url_parts VALUES
  ('http://example.com/path?foo=bar', 'HOST', NULL),
  ('http://example.com/path?foo=bar', 'PATH', NULL),
  ('http://example.com/path?foo=bar', 'QUERY', 'foo'),
  ('https://user:pw@host:9090/p?a=1#frag', 'REF', NULL),
  ('https://user:pw@host:9090/p?a=1#frag', 'USERINFO', NULL)

query
SELECT parse_url(url, part) FROM test_parse_url_parts

query
SELECT parse_url(url, 'QUERY', key) FROM test_parse_url_parts WHERE key IS NOT NULL

-- previously divergent edge cases (now fixed)
query
SELECT parse_url('http://example.com//double//slashes', 'PATH')

query
SELECT parse_url('http://example.com/path?key=value%20encoded', 'QUERY', 'key')

query
SELECT parse_url('http://example.com/path?', 'QUERY')

query
SELECT parse_url('http://example.com#frag', 'FILE')

query
SELECT parse_url('http://[::1]:8080/path', 'HOST')

query
SELECT parse_url('http://example.com/path?a=1&a=2', 'QUERY', 'a')

-- Fix #2: FILE without explicit path
query
SELECT parse_url('http://example.com?foo=bar', 'FILE')

-- Fix #3: PATH on trailing slash
query
SELECT parse_url('http://example.com/', 'PATH')

-- query value containing '='
query
SELECT parse_url('http://host/p?a=b=c', 'QUERY', 'a')

-- 3-arg with non-QUERY part returns NULL
query
SELECT parse_url('http://host/path', 'HOST', 'key')

-- empty port
query
SELECT parse_url('http://host:/path', 'HOST')

-- regex metachar in query key (Spark treats key as regex pattern)
query
SELECT parse_url('http://h/p?abc=1', 'QUERY', '.bc')

-- non-digit port returns NULL for HOST
query
SELECT parse_url('http://host:abc/', 'HOST')

-- non-digit port: other parts still work
query
SELECT parse_url('http://host:abc/', 'AUTHORITY')

-- unbalanced IPv6 bracket is invalid
query
SELECT parse_url('http://[::1/path', 'AUTHORITY')
