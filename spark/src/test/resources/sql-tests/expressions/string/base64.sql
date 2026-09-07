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

statement
CREATE TABLE test_base64(s string) USING parquet

-- Inputs chosen to exercise Spark's default chunked encoding (spark.sql.chunkBase64String.enabled):
-- 'abc'/'hello' stay under one line; the 57-char value encodes to exactly 76 base64 characters (the
-- line limit, so no separator); the 58-char value wraps once; the 120-char value wraps twice.
statement
INSERT INTO test_base64 VALUES
  ('abc'), ('hello'), (''), (NULL),
  ('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'),
  ('bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'),
  ('cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc')

query
SELECT s, base64(cast(s AS binary)) FROM test_base64

-- literal arguments
query
SELECT base64(cast('Spark SQL' AS binary)), base64(cast('' AS binary))

-- round trip through unbase64
query
SELECT s, cast(unbase64(base64(cast(s AS binary))) AS string) FROM test_base64
