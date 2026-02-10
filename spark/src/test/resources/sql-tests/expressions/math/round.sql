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
CREATE TABLE test_round(d double, i int) USING parquet

statement
INSERT INTO test_round VALUES (2.5, 0), (3.5, 0), (-2.5, 0), (123.456, 2), (123.456, -1), (NULL, 0), (cast('NaN' as double), 0), (cast('Infinity' as double), 0), (0.0, 0)

query expect_fallback(BigDecimal rounding)
SELECT round(d, 0) FROM test_round WHERE i = 0

query expect_fallback(BigDecimal rounding)
SELECT round(d, 2) FROM test_round WHERE i = 2

query expect_fallback(BigDecimal rounding)
SELECT round(d, -1) FROM test_round WHERE i = -1

query expect_fallback(BigDecimal rounding)
SELECT round(d) FROM test_round

-- literal + literal
query expect_fallback(BigDecimal rounding)
SELECT round(123.456, 2), round(2.5, 0), round(3.5, 0), round(-2.5, 0), round(NULL, 0)
