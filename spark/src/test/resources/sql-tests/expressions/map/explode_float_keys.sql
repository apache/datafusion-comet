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

-- Config: spark.comet.exec.explode.enabled=true
-- Config: spark.sql.legacy.disableMapKeyNormalization=true

-- Preserve -0.0 when writing the fixture on Spark 4.0+. The generator must return
-- the stored keys unchanged. Native map construction's normalization is tracked
-- separately in https://github.com/apache/datafusion-comet/issues/6549.
statement
CREATE TABLE explode_float_maps(id int, m map<double, int>) USING parquet

statement
INSERT INTO explode_float_maps VALUES
  (1, map(cast('-0.0' AS double), 10, cast('NaN' AS double), 20)),
  (2, map(cast('NaN' AS double), NULL)), (3, map()), (4, NULL)

-- Render keys as strings too, so a comparison cannot silently equate the two zero signs.
query
SELECT id, cast(key AS string), value
FROM explode_float_maps LATERAL VIEW explode(m) e AS key, value

query
SELECT id, cast(key AS string), value
FROM explode_float_maps LATERAL VIEW OUTER explode(m) e AS key, value

query
SELECT id, pos, cast(key AS string), value
FROM explode_float_maps LATERAL VIEW posexplode(m) e AS pos, key, value

query
SELECT id, pos, cast(key AS string), value
FROM explode_float_maps LATERAL VIEW OUTER posexplode(m) e AS pos, key, value
