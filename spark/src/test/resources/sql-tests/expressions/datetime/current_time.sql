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

-- MinSparkVersion: 4.1
-- Config: spark.sql.timeType.enabled=true

-- current_time() is CodegenFallback but foldable; Spark's optimizer folds it to a Time
-- literal before Comet sees the plan, so it never actually hits the codegen dispatcher.
-- These tests pin behavior rather than a specific timestamp value.

query
SELECT current_time() IS NOT NULL

query
SELECT current_time(0) IS NOT NULL

query
SELECT current_time(6) IS NOT NULL

-- Two calls in the same query return the same value
query
SELECT current_time() = current_time()
