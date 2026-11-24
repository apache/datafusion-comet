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

-- The full-sort case of large_scalar_string_join_expand.sql with a session batch size at which a
-- batch of its 256 KiB key literal takes 1 GiB. A smaller batch does not keep the native sort's
-- keys within memory, since its merge holds a batch of keys for every run, so the sort still
-- falls back to Spark's.
-- See https://github.com/apache/datafusion-comet/issues/7689.
-- Config: spark.sql.adaptive.enabled=false
-- Config: spark.comet.batchSize=4096
-- Config: spark.comet.exec.range.enabled=true
-- ConstantFolding: enabled

query expect_fallback(A sort key expands a string or binary literal of 262144 bytes)
SELECT id FROM range(0, 8192, 1, 1) SORT BY concat(repeat('x', 262144), CAST(id AS STRING))
