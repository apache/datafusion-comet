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
CREATE TABLE test_map(m map<string, int>) USING parquet

statement
INSERT INTO test_map VALUES (map('a', 1, 'b', 2, 'c', 3)), (map('x', 10)), (NULL)

query spark_answer_only
SELECT m['a'], m['b'], m['c'] FROM test_map

query spark_answer_only
SELECT m['x'], m['missing'] FROM test_map

-- literal arguments
query spark_answer_only
SELECT map('a', 1, 'b', 2)['a'], map('a', 1, 'b', 2)['missing'], map('a', 1, 'b', 2)[NULL]

-- Map key types whose Spark equality Comet's native `map_extract` cannot reproduce fall back to
-- Spark. `x[key]` routes through `GetMapValue` / `CometMapExtract` (the `element_at` form in
-- `element_at_map.sql` exercises the same guard on `CometElementAt`). A `map<double,int>` column
-- is used rather than a `map(...)` literal because Spark's `SimplifyExtractValueOps` rewrites
-- `map(...)[key]` over a literal map into a `CASE` before it can reach the native lookup. Spark
-- stores a `-0.0` key as `+0.0` and finds it with `nanSafeCompareDoubles`, so it returns `7` for a
-- `-0.0` lookup; native lookup compares the raw Arrow values.
statement
CREATE TABLE test_map_double(m map<double, int>) USING parquet

statement
INSERT INTO test_map_double VALUES (map(CAST(0 AS DOUBLE), 7)), (map(CAST(1 AS DOUBLE), 8)), (NULL)

query expect_fallback(Spark normalizes floating-point map keys)
SELECT m[CAST(-0.0 AS DOUBLE)] FROM test_map_double
