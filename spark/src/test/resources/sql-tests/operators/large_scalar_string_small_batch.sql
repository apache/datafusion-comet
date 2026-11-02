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

-- The filter case of large_scalar_string.sql with a session batch size that a 256 KiB scalar
-- fits as is. The projection above the filter still bounds its input, whatever the filter
-- emits.
-- See https://github.com/apache/datafusion-comet/issues/7247.
-- Config: spark.sql.adaptive.enabled=false
-- Config: spark.comet.batchSize=256
-- ConstantFolding: enabled

statement
CREATE TABLE test_large_scalar_small_batch(k int) USING parquet

statement
INSERT INTO test_large_scalar_small_batch SELECT /*+ COALESCE(1) */ CAST(id % 2 AS INT) FROM range(8192)

query
SELECT sum(length(c)) FROM (SELECT concat(repeat('x', 262144), CAST(k AS STRING)) AS c FROM test_large_scalar_small_batch WHERE k >= 0)
