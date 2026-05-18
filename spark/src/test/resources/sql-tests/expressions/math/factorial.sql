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

-- ConfigMatrix: parquet.enable.dictionary=false,true

statement
CREATE TABLE test_factorial(
  b tinyint,
  s smallint,
  i int,
  l bigint,
  f float,
  d double,
  dec38 decimal(38, 0),
  dec10_2 decimal(10, 2)
) USING parquet

statement
INSERT INTO test_factorial VALUES
  (cast(0 as tinyint),   cast(0 as smallint),   0,           cast(0 as bigint),           cast(0.0 as float),         0.0,                            cast(0 as decimal(38, 0)),  cast(0.00 as decimal(10, 2))),
  (cast(1 as tinyint),   cast(1 as smallint),   1,           cast(1 as bigint),           cast(1.0 as float),         1.0,                            cast(1 as decimal(38, 0)),  cast(1.49 as decimal(10, 2))),
  (cast(5 as tinyint),   cast(5 as smallint),   5,           cast(5 as bigint),           cast(5.7 as float),         5.7,                            cast(5 as decimal(38, 0)),  cast(5.50 as decimal(10, 2))),
  (cast(20 as tinyint),  cast(20 as smallint),  20,          cast(20 as bigint),          cast(20.0 as float),        20.0,                           cast(20 as decimal(38, 0)), cast(20.99 as decimal(10, 2))),
  (cast(21 as tinyint),  cast(21 as smallint),  21,          cast(21 as bigint),          cast(21.0 as float),        21.0,                           cast(21 as decimal(38, 0)), cast(21.00 as decimal(10, 2))),
  (cast(-1 as tinyint),  cast(-1 as smallint),  -1,          cast(-1 as bigint),          cast(-1.0 as float),        -1.0,                           cast(-1 as decimal(38, 0)), cast(-1.50 as decimal(10, 2))),
  (NULL,                 NULL,                  NULL,        NULL,                        NULL,                       NULL,                           NULL,                       NULL),
  (cast(127 as tinyint), cast(32767 as smallint), 2147483647, 9223372036854775807,         cast('Infinity' as float),  cast('Infinity' as double),     cast(99999999999999999999999999999999999999 as decimal(38, 0)), cast(99999999.99 as decimal(10, 2))),
  (cast(-128 as tinyint),cast(-32768 as smallint),-2147483648,-9223372036854775808,        cast('-Infinity' as float), cast('-Infinity' as double),    cast(-99999999999999999999999999999999999999 as decimal(38, 0)),cast(-99999999.99 as decimal(10, 2))),
  (cast(0 as tinyint),   cast(0 as smallint),   0,           cast(0 as bigint),           cast('NaN' as float),       cast('NaN' as double),          cast(0 as decimal(38, 0)),  cast(0.00 as decimal(10, 2))),
  (cast(0 as tinyint),   cast(0 as smallint),   0,           cast(0 as bigint),           cast(-0.0 as float),        cast(-0.0 as double),           cast(0 as decimal(38, 0)),  cast(0.00 as decimal(10, 2)))

query
SELECT b, factorial(b) FROM test_factorial

query
SELECT s, factorial(s) FROM test_factorial

query
SELECT i, factorial(i) FROM test_factorial

query
SELECT l, factorial(l) FROM test_factorial

query
SELECT f, factorial(f) FROM test_factorial

query
SELECT d, factorial(d) FROM test_factorial

query
SELECT dec38, factorial(dec38) FROM test_factorial

query
SELECT dec10_2, factorial(dec10_2) FROM test_factorial
