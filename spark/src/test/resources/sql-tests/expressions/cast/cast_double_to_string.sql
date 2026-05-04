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
CREATE TABLE test_double_to_string(d double, id int) USING parquet

statement
INSERT INTO test_double_to_string VALUES
  (-0.0, 1),
  (0.0, 2),
  (1.5, 3),
  (-1.5, 4),
  (cast('NaN' as double), 5),
  (cast('Infinity' as double), 6),
  (cast('-Infinity' as double), 7),
  (NULL, 8),
  (1.0E20, 9),
  (1.0E-20, 10),
  (-1.0E20, 11),
  (0.001, 12),
  (123456789.0, 13),
  (1.23456789E10, 14)

query
SELECT cast(d as string), id FROM test_double_to_string ORDER BY id

query
SELECT cast(-0.0 as string), cast(0.0 as string)
