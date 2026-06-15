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

-- Routes bround (banker's rounding) through the codegen dispatcher so behavior matches Spark exactly.

statement
CREATE TABLE test_bround(v double) USING parquet

statement
INSERT INTO test_bround VALUES (2.5), (3.5), (2.4), (-2.5), (0.0), (NULL)

query
SELECT v, bround(v) FROM test_bround

-- with scale
query
SELECT bround(2.555, 2), bround(2.565, 2), bround(25.0, -1)
