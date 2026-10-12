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

-- CTAS preserves REQUIRED children. CREATE + INSERT can rebuild them as nullable,
-- which would hide #5754: a child can contain a value underneath a NULL parent.
-- Write with Spark so these tests exercise the native reader and hash kernels.
statement
SET spark.comet.enabled=false

statement
CREATE TABLE hash_required_child USING parquet AS SELECT /*+ COALESCE(1) */ c FROM VALUES (named_struct('a', 1)), (NULL), (named_struct('a', 3)), (NULL) AS v(c)

-- The parent's null mask must also reach grandchildren through a REQUIRED struct.
statement
CREATE TABLE hash_required_nested_child USING parquet AS SELECT /*+ COALESCE(1) */ c FROM VALUES (named_struct('b', named_struct('x', 1))), (NULL), (named_struct('b', named_struct('x', 3))), (NULL) AS v(c)

statement
SET spark.comet.enabled=true

query expect_native(hash,xxhash64)
SELECT hash(c), xxhash64(c) FROM hash_required_child ORDER BY 1, 2

query expect_native(hash,xxhash64)
SELECT hash(c), xxhash64(c) FROM hash_required_nested_child ORDER BY 1, 2

-- A valid array element wrapping the NULL struct preserves its hidden child values.
-- A NULL array element itself is rebuilt and would not exercise the same path.
query expect_native(hash,xxhash64)
SELECT hash(array(named_struct('tag', 1, 'b', c))), xxhash64(array(named_struct('tag', 1, 'b', c))) FROM hash_required_child ORDER BY 1, 2

-- Two elements exercise chaining the second element's hash onto the first.
query expect_native(hash,xxhash64)
SELECT hash(array(named_struct('tag', 1, 'b', c), named_struct('tag', 2, 'b', c))), xxhash64(array(named_struct('tag', 1, 'b', c), named_struct('tag', 2, 'b', c))) FROM hash_required_child ORDER BY 1, 2
