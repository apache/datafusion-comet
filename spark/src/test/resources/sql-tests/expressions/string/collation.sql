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

-- MinSparkVersion: 4.0

-- collation(expr) returns the collation name of a string expression.
-- It folds to a string literal at planning time, so Comet evaluates it natively.

-- default collation on a string literal
query
SELECT collation('abc')

-- collation of an explicit UTF8_BINARY string
query
SELECT collation('hello' COLLATE UTF8_BINARY)

-- collation of a NULL string
query
SELECT collation(CAST(NULL AS STRING))

-- concat preserves a non-default collation in its result type, which Comet's native concat does
-- not, so concat is Incompatible. It is enrolled in the JVM codegen dispatcher, which runs Spark's
-- own doGenCode inside the Comet pipeline, so a collated concat is evaluated natively and matches
-- Spark.
query
SELECT concat('Hello' COLLATE UTF8_LCASE, 'World' COLLATE UTF8_LCASE)

-- reverse is enrolled in the JVM codegen dispatcher, which runs Spark's own doGenCode inside the
-- Comet pipeline, so a collated string is evaluated natively and matches Spark.
query
SELECT reverse('Hello' COLLATE UTF8_LCASE)

-- A standard ICU collation (UNICODE_CI) also dispatches and matches Spark, confirming the path
-- covers any non-UTF8_BINARY collation rather than just UTF8_LCASE.
query
SELECT concat('Hello' COLLATE UNICODE_CI, 'World' COLLATE UNICODE_CI)

query
SELECT reverse('Hello' COLLATE UNICODE_CI)
