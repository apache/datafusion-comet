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

-- parse_url is marked Incompatible (see CometParseUrl) because the native
-- implementation diverges from Spark for empty-string input and for FILE
-- extraction on path-less URLs. Tracked upstream at
-- https://github.com/apache/datafusion/issues/21943. In the default
-- configuration, Comet falls back to Spark. See parse_url_native.sql for
-- coverage of the native implementation.

query expect_fallback(not fully compatible with Spark)
SELECT parse_url('http://spark.apache.org/path?query=1', 'HOST')

query expect_fallback(not fully compatible with Spark)
SELECT parse_url('http://spark.apache.org/path?query=1', 'QUERY', 'query')
