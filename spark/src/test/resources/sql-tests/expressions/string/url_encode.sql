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

-- Note: UrlEncode is a RuntimeReplaceable expression that delegates to UrlCodec.encode

-- ConfigMatrix: parquet.enable.dictionary=false,true

statement
CREATE TABLE test_url_encode(url_string string) USING parquet

statement
INSERT INTO test_url_encode VALUES ('Hello World'), ('foo=bar&baz=qux'), ('https://example.com/path?query=value'), (''), (NULL), ('no encoding needed'), ('100%'), ('a+b=c'), ('special!@#$%^&*()'), ('space test')

query
SELECT url_string, url_encode(url_string) FROM test_url_encode

query
SELECT url_encode('Hello World'), url_encode('foo=bar'), url_encode(''), url_encode(NULL)

query
SELECT url_encode('?'), url_encode('&'), url_encode('='), url_encode('#'), url_encode('/')

query
SELECT url_encode('%'), url_encode('100%'), url_encode('already%20encoded')

query
SELECT url_encode(' '), url_encode('+'), url_encode('a b+c')

statement
CREATE TABLE test_url_encode_unicode(url_string string) USING parquet

statement
INSERT INTO test_url_encode_unicode VALUES ('café'), ('hello世界'), ('日本語'), ('emoji😀test'), ('తెలుగు'), (NULL)

query
SELECT url_string, url_encode(url_string) FROM test_url_encode_unicode

query
SELECT url_encode('Hello World!'), url_encode('email@example.com'), url_encode('price=$100')

query
SELECT url_encode(':'), url_encode('/'), url_encode('?'), url_encode('#'), url_encode('['), url_encode(']'), url_encode('@')

query
SELECT url_encode('!'), url_encode('$'), url_encode('&'), url_encode("'"), url_encode('('), url_encode(')'), url_encode('*'), url_encode('+'), url_encode(','), url_encode(';'), url_encode('=')