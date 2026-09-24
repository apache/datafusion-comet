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

-- Config: spark.comet.exec.strictFloatingPoint=true

-- strict floating-point mode declines floating-point keys, top level or nested, and no others

statement
CREATE TABLE test_map_from_arrays_strict_fp(
  d array<double>,
  st array<struct<a: float>>,
  i array<int>,
  v array<string>) USING parquet

statement
INSERT INTO test_map_from_arrays_strict_fp VALUES
  (array(double('NaN'), double('-0.0'), double('Infinity')),
   array(named_struct('a', float('NaN')), named_struct('a', float('-0.0')),
     named_struct('a', float('Infinity'))),
   array(1, 2, 3),
   array('a', NULL, 'c')),
  (array(double('0.0'), double('-Infinity')),
   array(named_struct('a', float('0.0')), named_struct('a', float('-Infinity'))),
   array(4, 5),
   array('d', 'e')),
  (NULL, NULL, NULL, NULL)

query
SELECT map_from_arrays(i, v) FROM test_map_from_arrays_strict_fp

query expect_fallback(strictFloatingPoint=true)
SELECT map_from_arrays(d, v) FROM test_map_from_arrays_strict_fp

query expect_fallback(strictFloatingPoint=true)
SELECT map_from_arrays(st, v) FROM test_map_from_arrays_strict_fp
