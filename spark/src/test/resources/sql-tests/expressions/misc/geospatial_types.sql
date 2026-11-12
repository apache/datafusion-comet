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

-- Confirms Comet falls back to Spark for a parquet scan whose table contains a GEOMETRY or
-- GEOGRAPHY column (Spark 4.2+ data types that Comet does not support). The native scan
-- serializes the full data schema rather than just the projected columns, so it must decline
-- even when the geospatial column is not selected.

-- MinSparkVersion: 4.2

statement
CREATE TABLE test_geometry_scan(id INT, g GEOMETRY(4326)) USING parquet

statement
INSERT INTO test_geometry_scan VALUES (1, NULL), (2, NULL)

query expect_fallback(does not support data type geometry)
SELECT id FROM test_geometry_scan ORDER BY id

query expect_fallback(does not support data type geometry)
SELECT count(*) FROM test_geometry_scan

statement
CREATE TABLE test_geography_scan(id INT, g GEOGRAPHY(4326)) USING parquet

statement
INSERT INTO test_geography_scan VALUES (1, NULL), (2, NULL)

query expect_fallback(does not support data type geography)
SELECT id FROM test_geography_scan ORDER BY id
