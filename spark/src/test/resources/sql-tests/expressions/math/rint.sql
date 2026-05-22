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

-- Spark's Rint extends UnaryMathExpression with inputTypes = Seq(DoubleType).
-- It returns the double value closest to the argument equal to a mathematical integer
-- (Java's Math.rint, IEEE 754 round-half-to-even / banker's rounding).

statement
CREATE TABLE test_rint(v double) USING parquet

statement
INSERT INTO test_rint VALUES
  (0.0),
  (-0.0),
  (1.0),
  (-1.0),
  (0.4),
  (0.5),
  (0.6),
  (1.5),
  (2.5),
  (3.5),
  (-0.4),
  (-0.5),
  (-0.6),
  (-1.5),
  (-2.5),
  (-3.5),
  (12.3456),
  (-12.3456),
  (1.7976931348623157E308),
  (-1.7976931348623157E308),
  (4.9E-324),
  (cast('NaN' as double)),
  (cast('Infinity' as double)),
  (cast('-Infinity' as double)),
  (NULL)

query
SELECT rint(v) FROM test_rint

-- column with arithmetic
query
SELECT rint(v + 0.5) FROM test_rint
