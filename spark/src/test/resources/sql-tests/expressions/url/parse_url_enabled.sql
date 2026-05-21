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

-- Test parse_url() with allowIncompatible enabled (native execution)
-- Config: spark.comet.expression.ParseUrl.allowIncompatible=true

statement
CREATE TABLE test_parse_url_enabled(url string) USING parquet

statement
INSERT INTO test_parse_url_enabled VALUES
  ('http://spark.apache.org/path?query=1'),
  ('https://user:pass@host:8080/path?k=v#ref'),
  ('http://example.com/path?a=1&b=2&a=3'),
  ('ftp://ftp.example.com/dir/file.txt'),
  (NULL)

-- HOST
query
SELECT parse_url(url, 'HOST') FROM test_parse_url_enabled

-- PATH
query
SELECT parse_url(url, 'PATH') FROM test_parse_url_enabled

-- QUERY (no key)
query
SELECT parse_url(url, 'QUERY') FROM test_parse_url_enabled

-- QUERY with key
query
SELECT parse_url(url, 'QUERY', 'k') FROM test_parse_url_enabled

-- PROTOCOL
query
SELECT parse_url(url, 'PROTOCOL') FROM test_parse_url_enabled

-- REF (fragment)
query
SELECT parse_url(url, 'REF') FROM test_parse_url_enabled

-- AUTHORITY
query
SELECT parse_url(url, 'AUTHORITY') FROM test_parse_url_enabled

-- USERINFO
query
SELECT parse_url(url, 'USERINFO') FROM test_parse_url_enabled

-- FILE
query
SELECT parse_url(url, 'FILE') FROM test_parse_url_enabled

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

-- malformed URL returns NULL in non-ANSI mode (#7)
query
SELECT parse_url('not a url at all', 'HOST')

query
SELECT parse_url('://missing-scheme', 'HOST')

query
SELECT parse_url('', 'HOST')

-- column-valued part key (#5)
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

-- edge cases for known divergences (#6)
query
SELECT parse_url('http://example.com//double//slashes', 'PATH')

query ignore(known divergence: native decodes percent-encoding in QUERY values)
SELECT parse_url('http://example.com/path?key=value%20encoded', 'QUERY', 'key')

query
SELECT parse_url('http://example.com/path?', 'QUERY')

query ignore(known divergence: native returns "/" for FILE when URL has no path)
SELECT parse_url('http://example.com#frag', 'FILE')

query
SELECT parse_url('http://[::1]:8080/path', 'HOST')

query
SELECT parse_url('http://example.com/path?a=1&a=2', 'QUERY', 'a')
