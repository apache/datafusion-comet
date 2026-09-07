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

-- Spark hashes a collated (non-UTF8_BINARY) string via its collation sort key, which Comet's
-- plain-bytes native xxhash64 does not reproduce, so approx_count_distinct on a collated column
-- must fall back to Spark. Default UTF8_BINARY strings still run natively. Collation syntax
-- requires Spark 4.0+.

-- MinSparkVersion: 4.0

statement
CREATE TABLE acd_coll(s string COLLATE UTF8_LCASE, s_bin string) USING parquet

statement
INSERT INTO acd_coll VALUES ('a', 'a'), ('A', 'A'), ('b', 'b')

-- default UTF8_BINARY collation: runs natively and matches Spark ({a, A, b} = 3)
query
SELECT approx_count_distinct(s_bin) FROM acd_coll

-- UTF8_LCASE collation: Spark hashes via the collation sort key, so Comet falls back
query expect_fallback(Unsupported input data type)
SELECT approx_count_distinct(s) FROM acd_coll
