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

-- Spark returns NULL for a NULL map without evaluating the key, so a key that raises, here an ANSI
-- cast of a malformed string, cannot raise on a row whose map is NULL. map_contains_key is
-- array_contains(map_keys(m), k) after analysis. The array counterparts, and why each query names
-- its expression with expect_native, are in array/array_null_short_circuit.sql.
-- https://github.com/apache/datafusion-comet/issues/6613

-- Config: spark.sql.ansi.enabled=true

statement
CREATE TABLE map_null_short_circuit(m map<int, int>, s string) USING parquet

-- `s` is malformed only where the map is NULL. COALESCE(1) writes a single file, so the NULL maps
-- share a batch with the others.
statement
INSERT INTO map_null_short_circuit SELECT /*+ COALESCE(1) */ * FROM VALUES
  (NULL, 'bad'),
  (map(1, 10, 2, 20), '2'),
  (map(3, 30), '4'),
  (NULL, 'worse')
  AS v(m, s)

query expect_native(array_contains)
SELECT map_contains_key(m, CAST(s AS INT)) FROM map_null_short_circuit

query expect_native(getmapvalue)
SELECT m[CAST(s AS INT)] FROM map_null_short_circuit

-- Where the map is not NULL, the key is evaluated and raises as in Spark
query expect_error(CAST_INVALID_INPUT)
SELECT m[CAST(s || 'x' AS INT)] FROM map_null_short_circuit
