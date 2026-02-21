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
-- MinSparkVersion: 3.5

statement
CREATE TABLE test_parse_url(url string) USING parquet

statement
INSERT INTO test_parse_url VALUES
  ('http://spark.apache.org/path?query=1'),
  ('https://spark.apache.org/path/to/page?query=1&k2=v2'),
  (NULL)

query
SELECT parse_url(url, 'HOST') FROM test_parse_url

query
SELECT parse_url(url, 'QUERY') FROM test_parse_url

query
SELECT parse_url(url, 'PROTOCOL') FROM test_parse_url

query
SELECT parse_url(url, 'QUERY', 'query'), parse_url(url, 'QUERY', 'k2') FROM test_parse_url
