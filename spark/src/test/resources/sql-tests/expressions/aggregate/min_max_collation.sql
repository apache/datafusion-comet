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

-- Spark orders a string with a non-default collation by that collation, while Comet's native
-- min/max compare raw bytes, so min/max over a collated string must fall back to Spark.
-- Collation syntax requires Spark 4.0+.

-- MinSparkVersion: 4.0

statement
CREATE TABLE test_min_max_collation(grp string, s string) USING parquet

statement
INSERT INTO test_min_max_collation VALUES ('x', 'a'), ('x', 'B'), ('x', NULL), ('y', 'A'), ('y', 'b')

-- Under UTF8_LCASE, group x has min 'a' and max 'B', the reverse of their byte order.
query expect_fallback(min/max does not support non-UTF8_BINARY collated operands)
SELECT grp, min(CAST(s AS STRING COLLATE UTF8_LCASE)), max(CAST(s AS STRING COLLATE UTF8_LCASE))
FROM test_min_max_collation GROUP BY grp ORDER BY grp
