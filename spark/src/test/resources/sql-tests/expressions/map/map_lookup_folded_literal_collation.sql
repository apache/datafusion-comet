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

-- The collated counterpart of `map_lookup_folded_literal.sql`. Native `map_extract` compares
-- string keys bytewise, so a case-insensitive `a1` lookup cannot match a stored `A1`, and the
-- lookup is dispatched to compare under the declared collation. The constructor form lives in
-- `element_at_map_collation.sql`.

-- ConstantFolding: enabled

statement
CREATE TABLE test_map_lookup_folded_collation(_1 string, _2 int) USING parquet

statement
INSERT INTO test_map_lookup_folded_collation VALUES ('a1', 1), ('zz', 1), ('A1', 2)

query expect_dispatch(element_at)
SELECT element_at(map(CAST('A1' AS STRING COLLATE UTF8_LCASE), 7),
  CAST(_1 AS STRING COLLATE UTF8_LCASE)) AS v
FROM test_map_lookup_folded_collation

-- The collated map is nested in a map value, and the column key `_2` keeps the lookup on the
-- folded `map(1, ...)` from folding too. That lookup has INT keys, so the decline has to come from
-- the lookup whose map argument is `MAP<STRING COLLATE UTF8_LCASE, INT>`.
query expect_dispatch(element_at)
SELECT element_at(element_at(map(1, map(CAST('A1' AS STRING COLLATE UTF8_LCASE), 7)), _2),
  CAST(_1 AS STRING COLLATE UTF8_LCASE)) AS v
FROM test_map_lookup_folded_collation
