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

-- Test parse_url() in ANSI mode (failOnError=true -> native "parse_url" path)
-- Config: spark.sql.ansi.enabled=true

-- valid URLs should work identically in ANSI mode
query
SELECT parse_url('http://example.com/path?foo=bar', 'HOST')

query
SELECT parse_url('http://example.com/path?foo=bar', 'PATH')

query
SELECT parse_url('http://example.com/path?foo=bar', 'QUERY', 'foo')

query
SELECT parse_url('https://user:pass@host:8080/p?k=v#ref', 'AUTHORITY')

query
SELECT parse_url('https://user:pass@host:8080/p?k=v#ref', 'USERINFO')

query
SELECT parse_url('https://user:pass@host:8080/p?k=v#ref', 'REF')

-- NULL inputs still return NULL in ANSI mode
query
SELECT parse_url(NULL, 'HOST')

-- invalid URL throws in ANSI mode
query expect_error(not a url at all)
SELECT parse_url('not a url at all', 'HOST')

query expect_error(://missing-scheme)
SELECT parse_url('://missing-scheme', 'HOST')

-- empty string is a valid URI (no throw), just returns NULL for HOST
query
SELECT parse_url('', 'HOST')

-- unbalanced IPv6 bracket throws in ANSI mode
query expect_error(http://[::1/path)
SELECT parse_url('http://[::1/path', 'AUTHORITY')
