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

-- Before Spark 3.5.2 (SPARK-45599), mode keeps its counts in an OpenHashMap that matches keys with
-- `==` but hashes their bits, so no two NaNs match and each NaN counts as a key of its own. Native
-- mode counts all NaNs as one value, so FLOAT and DOUBLE input falls back even with
-- allowIncompatible set (#6738). The native check on other types is in mode.sql.
-- MaxSparkVersion: 3.4
-- Config: spark.comet.expression.Mode.allowIncompatible=true

statement
CREATE TABLE mode_nan_keys(d double, f float, grp string) USING parquet

-- Three NaNs and two 1.0s. Spark 3.4 counts the NaNs separately, so the mode is 1.0. Counting
-- them together would make it NaN.
statement
INSERT INTO mode_nan_keys VALUES
  (CAST('NaN' AS DOUBLE), CAST('NaN' AS FLOAT), 'a'),
  (CAST('NaN' AS DOUBLE), CAST('NaN' AS FLOAT), 'a'),
  (CAST('NaN' AS DOUBLE), CAST('NaN' AS FLOAT), 'a'),
  (1.0D, CAST(1.0 AS FLOAT), 'a'),
  (1.0D, CAST(1.0 AS FLOAT), 'a')

query expect_fallback(counts each NaN as a key of its own)
SELECT grp, mode(d) FROM mode_nan_keys GROUP BY grp ORDER BY grp

query expect_fallback(counts each NaN as a key of its own)
SELECT grp, mode(f) FROM mode_nan_keys GROUP BY grp ORDER BY grp

-- Without NaN there is nothing to diverge, but the type is still not supported natively.
query expect_fallback(counts each NaN as a key of its own)
SELECT mode(d) FROM mode_nan_keys WHERE d = 1.0D
