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

-- With spark.sql.legacy.disableMapKeyNormalization, Spark 4.0+ leaves FLOAT and DOUBLE map keys as
-- they are, as Spark 3.4 and 3.5 always do (they don't have the flag). So 0.0 and -0.0 are two
-- keys and a -0.0 key stays -0.0. map_from_entries and map_from_arrays run Spark's code in the
-- codegen dispatcher, which has to see the session's flag.

-- Config: spark.sql.legacy.disableMapKeyNormalization=true

statement
CREATE TABLE float_map_keys_legacy(d double, f float) USING parquet

statement
INSERT INTO float_map_keys_legacy VALUES (0.0, 0.0)

query expect_dispatch(map_from_entries)
SELECT
  map_keys(map_from_entries(array(named_struct('k', d, 'v', 1), named_struct('k', -d, 'v', 2)))),
  map_keys(map_from_entries(array(named_struct('k', -f, 'v', 1), named_struct('k', 2.5F, 'v', 2))))
FROM float_map_keys_legacy

query expect_dispatch(map_from_arrays)
SELECT
  map_keys(map_from_arrays(array(d, -d), array(1, 2))),
  map_keys(map_from_arrays(array(f, -f), array(1, 2)))
FROM float_map_keys_legacy
