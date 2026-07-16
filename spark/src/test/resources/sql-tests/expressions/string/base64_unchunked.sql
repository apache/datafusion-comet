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

-- Exercises the unchunked base64 path (chunk = false in the native function).
-- On Spark 3.5+ this config produces single-line output with no CRLF separators.
-- On Spark 3.4 the config does not exist and is ignored, so both engines still chunk;
-- the file is harmless there and Comet still matches Spark.
-- Config: spark.sql.chunkBase64String.enabled=false

statement
CREATE TABLE test_base64_unchunked(s string) USING parquet

-- The 58-char and 120-char values encode to more than 76 base64 characters, so they would
-- wrap under the default chunked encoding but stay on a single line here.
statement
INSERT INTO test_base64_unchunked VALUES
  ('abc'), (''), (NULL),
  ('bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'),
  ('cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc')

query
SELECT s, base64(cast(s AS binary)) FROM test_base64_unchunked

-- round trip through unbase64
query
SELECT s, cast(unbase64(base64(cast(s AS binary))) AS string) FROM test_base64_unchunked
