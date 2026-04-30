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

-- url_encode function
statement
CREATE TABLE test_encode(s string) USING parquet

statement
INSERT INTO test_encode VALUES
  ('https://spark.apache.org'),
  ('hello world'),
  ('a+b=c&d=e'),
  ('café'),
  (''),
  (NULL),
  ('foo bar/baz?x=1&y=2'),
  ('~*''()'),
  ('a%20b'),
  ('\t\n\r')

query
SELECT url_encode(s) FROM test_encode

-- literal arguments
query
SELECT url_encode('https://spark.apache.org')

query
SELECT url_encode('hello world')

query
SELECT url_encode('')

query
SELECT url_encode(NULL)

-- special characters
query
SELECT url_encode('a b+c&d=e/f')

-- multibyte UTF-8
query
SELECT url_encode('日本語テスト')

-- boundary characters in the preserved set
query
SELECT url_encode('~*''()')

-- already-encoded input (verify double-encoding of percent)
query
SELECT url_encode('a%20b')

-- whitespace control characters
query
SELECT url_encode('\t\n\r')
