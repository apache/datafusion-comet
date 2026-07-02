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

-- Native approx_percentile via a GK (Greenwald-Khanna) quantile summary port,
-- matching Spark's algorithm and default relative error, so results are
-- bit-identical. Only byte, short, int, long, float, and double inputs are
-- supported. Every query below uses the default `query` mode, which asserts
-- native execution, so an all-fallback run of this file cannot vacuously pass.

-- scalar percentile over a bigint (long) column
query
SELECT approx_percentile(id, 0.5) FROM range(1000)

-- explicit, non-default accuracy
query
SELECT approx_percentile(id, 0.9, 100) FROM range(1000)

-- array of percentiles
query
SELECT approx_percentile(id, array(0.25, 0.5, 0.75)) FROM range(1000)

-- group by
query
SELECT id % 3 AS g, approx_percentile(id, 0.5) FROM range(1000) GROUP BY g ORDER BY g

-- doubles
query
SELECT approx_percentile(cast(id AS double) / 7.0, 0.5) FROM range(1000)

-- floats
query
SELECT approx_percentile(cast(id AS float), 0.5) FROM range(1000)

-- byte input type
query
SELECT approx_percentile(cast(id % 100 AS byte), 0.5) FROM range(1000)

-- short input type
query
SELECT approx_percentile(cast(id AS short), 0.5) FROM range(1000)

statement
CREATE TABLE test_approx_percentile_nulls(v int) USING parquet

statement
INSERT INTO test_approx_percentile_nulls VALUES (1), (2), (null), (3), (4)

-- nulls are ignored
query
SELECT approx_percentile(v, 0.5) FROM test_approx_percentile_nulls

-- empty input yields null
query
SELECT approx_percentile(v, 0.5) FROM (SELECT id AS v FROM range(1000) WHERE id < 0)
