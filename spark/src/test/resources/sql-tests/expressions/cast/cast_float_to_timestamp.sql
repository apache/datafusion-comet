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

-- Config: spark.sql.ansi.enabled=false

statement
CREATE TABLE test_float_to_timestamp(d double, f float, id int) USING parquet

statement
INSERT INTO test_float_to_timestamp VALUES
  (1.5, float(1.5), 1),
  (-1.5, float(-1.5), 2),
  (1.0E16, float(1.0E16), 3),
  (-1.0E16, float(-1.0E16), 4),
  (9.3E12, float(9.3E12), 5),
  (1.7976931348623157E308, float('3.4028235E38'), 6),
  (cast('NaN' as double), cast('NaN' as float), 7),
  (cast('Infinity' as double), cast('-Infinity' as float), 8),
  (NULL, NULL, 9)

-- Legacy saturates a finite value whose microseconds do not fit in a long
query
SELECT id, cast(cast(d as timestamp) as long), cast(cast(f as timestamp) as long)
FROM test_float_to_timestamp ORDER BY id

-- try_cast returns NULL for the same values
query
SELECT id, cast(try_cast(d as timestamp) as long), cast(try_cast(f as timestamp) as long)
FROM test_float_to_timestamp ORDER BY id
