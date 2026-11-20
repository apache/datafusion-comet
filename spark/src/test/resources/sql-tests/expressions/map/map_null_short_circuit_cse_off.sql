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

-- With spark.sql.subexpressionElimination.enabled off, Spark's projection evaluates `1 div i`
-- only where the map is not NULL, though both lookups share it. Its generated code for an
-- aggregate shares it whatever the setting says, so there the division by zero still raises. The
-- array counterparts are in array/array_null_short_circuit_cse_off.sql.
-- https://github.com/apache/datafusion-comet/issues/6972

-- Config: spark.sql.ansi.enabled=true
-- Config: spark.sql.subexpressionElimination.enabled=false
-- Config: spark.sql.codegen.wholeStage=true

statement
CREATE TABLE map_null_short_circuit_cse_off(m1 map<int, int>, m2 map<int, int>, i int)
USING parquet

-- `i` is 0 only where the maps are NULL
statement
INSERT INTO map_null_short_circuit_cse_off SELECT /*+ COALESCE(1) */ * FROM VALUES
  (NULL, NULL, 0),
  (map(1, 5), map(1, 6), 1),
  (map(2, 7), map(1, 8), 1)
  AS v(m1, m2, i)

query expect_native(getmapvalue)
SELECT m1[1 div i], m2[1 div i] FROM map_null_short_circuit_cse_off

query expect_error_class(DIVIDE_BY_ZERO)
SELECT max(m1[1 div i]), min(m2[1 div i]) FROM map_null_short_circuit_cse_off

-- Where the whole lookup is shared rather than the division, the lookup skips it on its own
query expect_native(getmapvalue)
SELECT max(m1[1 div i]), min(m1[1 div i]) FROM map_null_short_circuit_cse_off
