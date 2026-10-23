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

-- From 4.2, Spark matches the values of a pivot column with a non-UTF8_BINARY collation by
-- that collation, so under UTF8_LCASE the pivot value 'Q1' collects the 'q1' rows. 4.0 and 4.1
-- match the bytes. The native lookup compares bytes, so PivotFirst falls back for these pivot
-- columns on every version. Collation syntax requires Spark 4.0+.

-- MinSparkVersion: 4.0

statement
CREATE TABLE pf_lcase (quarter string COLLATE UTF8_LCASE, course string, earnings int) USING parquet

statement
INSERT INTO pf_lcase VALUES
  ('q1', 'dotNET', 10000),
  ('q2', 'dotNET', 15000),
  ('q1', 'Java',   20000),
  ('q2', 'Java',   30000)

query expect_fallback(Unsupported pivot column data type)
SELECT * FROM pf_lcase
  PIVOT (sum(earnings) FOR quarter IN ('Q1' AS Q1, 'Q2' AS Q2))
  ORDER BY course
