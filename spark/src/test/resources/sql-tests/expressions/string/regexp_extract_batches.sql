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

-- Config: spark.comet.batchSize=64

-- One Parquet file keeps enough rows in one task to cross Arrow batch boundaries.
statement
CREATE TABLE test_regexp_extract_batches(id int, s string) USING parquet

statement
INSERT INTO test_regexp_extract_batches
SELECT CAST(id AS INT),
  CASE WHEN id % 7 = 0 THEN NULL ELSE concat('item_', CAST(id AS STRING), '_value') END
FROM range(0, 5000, 1, 1)

-- Keep the row ID so a shifted extraction or null bitmap cannot pass as the same result set.
query expect_dispatch(regexp_extract)
SELECT id, s, regexp_extract(s, r'item_(\d+)_value', 1) FROM test_regexp_extract_batches
