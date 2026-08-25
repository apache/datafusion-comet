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

statement
CREATE TABLE test_map_entries(m map<string, int>) USING parquet

statement
INSERT INTO test_map_entries VALUES (map('a', 1, 'b', 2)), (map()), (NULL)

query
SELECT map_entries(m) FROM test_map_entries

-- `MapType(_, _, valueContainsNull = false)`, which every `map(...)` over non-null values
-- produces. DataFusion's `map_entries` declares the entry `value` field nullable but reuses the
-- input map's entries array, so the planner widens the argument before the call.
query
SELECT map_entries(map(1, 2, 3, 4))

query
SELECT map_entries(map('a', array(1, 2)))

-- A map nested in a map value keeps `valueContainsNull = false` through the extract.
query
SELECT map_entries(element_at(map(1, map(1, 2)), 1))
