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

-- try_parse_url is Spark 4.0+. It rewrites to ParseUrl(_, failOnError=false),
-- so Comet emits the native try_parse_url scalar function. parse_url is marked
-- Incompatible (see CometParseUrl), so this test opts in via allowIncompatible.

-- MinSparkVersion: 4.0
-- Config: spark.comet.expression.ParseUrl.allowIncompatible=true

-- Valid URL: same answer as parse_url.
query
SELECT try_parse_url('http://spark.apache.org/path?query=1', 'HOST')

query
SELECT try_parse_url('http://spark.apache.org/path?query=1', 'QUERY', 'query')

-- Malformed URL with a scheme: Spark returns NULL, Comet's try_parse_url
-- returns NULL (failOnError=false propagates through CometParseUrl to the
-- native try_parse_url UDF).
query
SELECT try_parse_url('inva lid://user:pass@host/file', 'HOST')

-- NULL input.
query
SELECT try_parse_url(NULL, 'HOST')
