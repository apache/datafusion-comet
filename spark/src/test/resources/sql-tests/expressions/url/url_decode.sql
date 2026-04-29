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

-- url_decode function
statement
CREATE TABLE test_decode(s string) USING parquet

statement
INSERT INTO test_decode VALUES
  ('https%3A%2F%2Fspark.apache.org'),
  ('hello+world'),
  ('a%2Bb%3Dc%26d%3De'),
  ('caf%C3%A9'),
  (''),
  (NULL),
  ('no+encoding+needed')

query
SELECT url_decode(s) FROM test_decode

-- literal arguments
query
SELECT url_decode('https%3A%2F%2Fspark.apache.org')

query
SELECT url_decode('hello+world')

query
SELECT url_decode('')

query
SELECT url_decode(NULL)

-- roundtrip: encode then decode
query
SELECT url_decode(url_encode('hello world & goodbye'))

-- multibyte UTF-8
query
SELECT url_decode('%E6%97%A5%E6%9C%AC%E8%AA%9E%E3%83%86%E3%82%B9%E3%83%88')
