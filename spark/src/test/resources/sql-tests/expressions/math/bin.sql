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
CREATE TABLE test_bin(i int, l long, f float, d double) USING parquet

statement
INSERT INTO test_bin VALUES
(13, 1, 1.5, 1.5),
(1, 1, 0.1, 0.1),
(100, 100, 99.99, 99.99),
(255, 255, 255.255, 255.255),
(-13, -1, -1.5, -1.5),
(-1, -1, -0.1, -0.1),
(-100, -100, -99.99, -99.99),
(-255, -255, -255.255, -255.255),
(0, 0, 0.0, 0.0),
(0, 0, -0.0, -0.0),
(NULL, NULL, NULL, NULL),
(2147483647, 9223372036854775807, cast('Infinity' as float), cast('Infinity' as double)),
(-2147483648, -9223372036854775808, cast('-Infinity' as float), cast('-Infinity' as double)),
(100, 100, cast('Infinity' as float), cast('Infinity' as double)),
(200, 200, cast('-Infinity' as float), cast('-Infinity' as double)),
(300, 300, cast('NaN' as float), cast('NaN' as double)),
(5, 5, 1.401298464e-45, 4.9406564584124654e-324),
(1000, 1000, 3.402823466e+38, 1.7976931348623157e+308),
(7, 7, 0.1, 0.1),
(8, 8, 0.2, 0.2),
(9, 9, 0.3, 0.3),
(10, 10, 0.3333333333333333, 0.3333333333333333),
(500, 500, cast('NaN' as float), cast('Infinity' as double)),
(501, 501, cast('Infinity' as float), cast('NaN' as double)),
(502, 502, cast('-Infinity' as float), cast('NaN' as double)),
(503, 503, cast('NaN' as float), cast('-Infinity' as double)),
(123456789, 123456789, 123456.789, 123456789.123456789),
(-123456789, -123456789, -123456.789, -123456789.123456789),
(2147483646, 9223372036854775806, 999999.999, 999999999.999999999),
(-2147483647, -9223372036854775807, -999999.999, -999999999.999999999),
(999, NULL, NULL, 999.999),
(NULL, 888, 888.888, NULL),
(777, 777, NULL, 777.777),
(666, 666, 666.666, NULL),
(15, 15, 1.23456789, 1.2345678901234567),
(16, 16, -1.23456789, -1.2345678901234567),
(314, 314, 3.14159265, 3.14159265358979323846),
(271, 271, 2.71828183, 2.71828182845904523536);

query
SELECT bin(i), bin(l), bin(f), bin(d) FROM test_bin

-- literal arguments
query
SELECT bin(-5), bin(-1.5), bin(0), bin(NULL)
