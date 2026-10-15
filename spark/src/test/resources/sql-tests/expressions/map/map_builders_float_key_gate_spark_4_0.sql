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

-- From Spark 4.0, ArrayBasedMapBuilder normalizes a top-level FLOAT or DOUBLE key before it looks
-- for a duplicate, so 0.0 and -0.0 are one key (#6549). Earlier releases keep them apart, and so
-- does spark.sql.legacy.disableMapKeyNormalization (map_builders_float_key_gate_legacy.sql).

-- MinSparkVersion: 4.0

statement
CREATE TABLE float_map_keys_zeros(d double, f float) USING parquet

statement
INSERT INTO float_map_keys_zeros VALUES (0.0, 0.0)

query expect_error_class(DUPLICATED_MAP_KEY)
SELECT map_from_entries(array(named_struct('k', d, 'v', 1), named_struct('k', -d, 'v', 2)))
FROM float_map_keys_zeros

query expect_error_class(DUPLICATED_MAP_KEY)
SELECT map_from_entries(array(named_struct('k', f, 'v', 1), named_struct('k', -f, 'v', 2)))
FROM float_map_keys_zeros

query expect_error_class(DUPLICATED_MAP_KEY)
SELECT map_from_arrays(array(d, -d), array(1, 2)) FROM float_map_keys_zeros

query expect_error_class(DUPLICATED_MAP_KEY)
SELECT map_from_arrays(array(f, -f), array(1, 2)) FROM float_map_keys_zeros
