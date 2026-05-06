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

-- parse_url is marked Incompatible (see CometParseUrl). Known divergences from
-- Spark, tracked upstream at https://github.com/apache/datafusion/issues/21943:
--   1. empty-string URL returns NULL instead of "" for any part
--   2. FILE on a URL without an explicit path returns "/?..." instead of "?..."
--   3. PATH on a URL with a bare trailing slash returns "" instead of "/"
-- In the default configuration, Comet falls back to Spark. The two queries below
-- both verify a normal-shape URL takes the fallback path, and exercise one of
-- the divergent shapes (trailing-slash PATH) to lock in that fallback handles
-- it correctly. See parse_url_native.sql for native-execution coverage.

query expect_fallback(not fully compatible with Spark)
SELECT parse_url('http://spark.apache.org/path?query=1', 'HOST')

query expect_fallback(not fully compatible with Spark)
SELECT parse_url('http://spark.apache.org/path?query=1', 'QUERY', 'query')

-- Trailing-slash PATH: Spark returns "/", native impl returns "". Verifying
-- the fallback path emits Spark's "/".
query expect_fallback(not fully compatible with Spark)
SELECT parse_url('http://example.com/', 'PATH')
