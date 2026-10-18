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

-- Config: spark.sql.ansi.enabled=true
-- Config: spark.sql.adaptive.enabled=false
-- Config: spark.comet.batchSize=2

statement
CREATE TABLE test_filter_projection(a int, b string, c int, d int) USING parquet

statement
INSERT INTO test_filter_projection VALUES (1, '7', 1, 3), (2, 'bad', 0, 0), (3, '9', 3, 3), (4, NULL, 4, 0)

-- Empty output still carries the filtered row count.
query
SELECT count(*) FROM test_filter_projection WHERE a + c + d > 4

query
SELECT c AS x FROM test_filter_projection WHERE a + c + d > 4

-- Output order, aliases and duplicate columns survive pruning.
query
SELECT c AS x, b AS y, c AS z FROM test_filter_projection WHERE a + c + d > 4

-- There may be more output columns than input columns.
query
SELECT c AS a, b AS b, c AS c, b AS d, c AS e FROM test_filter_projection WHERE a + c + d > 4

-- Full projections retain every column, including reordered output.
query
SELECT * FROM test_filter_projection WHERE a + c + d > 4

query
SELECT d, c, b, a FROM test_filter_projection WHERE a + c + d > 4

-- The rejected row must not reach this fallible computed projection.
query
SELECT cast(b AS INT) AS x FROM test_filter_projection WHERE a + c + d > 4

-- No matching rows and NULL values must also preserve the result schema.
query
SELECT c AS x, b AS y, c AS z FROM test_filter_projection WHERE a + c + d < 0
