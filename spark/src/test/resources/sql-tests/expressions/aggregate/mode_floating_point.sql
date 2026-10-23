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

-- Comet's `mode` is opt-in via allowIncompatible because Spark breaks ties non-deterministically.
-- Every compared query below has a single value with the strictly-highest frequency per group.
--
-- Native mode on FLOAT and DOUBLE needs Spark 3.5.2 or later (SPARK-45599), where the frequency map
-- matches keys with `Double.equals`. MinSparkVersion only reads the minor version, so a run on
-- 3.5.0 or 3.5.1 falls back and fails the native check. Earlier releases are in
-- mode_floating_point_spark_3_4.sql.
-- MinSparkVersion: 3.5
-- Config: spark.comet.expression.Mode.allowIncompatible=true

-- ============================================================
-- Float / Double
--
-- Whether `-0.0` and `0.0` share a frequency-map key is version-dependent: Spark 3.5.2-4.1 key on
-- `java.lang.Double.equals` and keep them apart, while Spark 4.2.0+ folds `-0.0` into `0.0`
-- (SPARK-57329). Each group below still has a unique winner under both behaviours.
--
-- Negative zero must be written as `-0.0D`, not `CAST(-0.0 AS DOUBLE)`: an unsuffixed `-0.0` is a
-- DecimalType literal, and Decimal has no signed zero, so the cast yields `+0.0` and the column
-- would silently contain no negative zeros at all.
-- ============================================================

statement
CREATE TABLE mode_double(v double, grp string) USING parquet

statement
INSERT INTO mode_double VALUES
  (1.5, 'a'), (1.5, 'a'), (2.5, 'a'), (NULL, 'a'),
  (0.0D, 'b'), (-0.0D, 'b'), (-0.0D, 'b'), (7.0, 'b')

query
SELECT grp, mode(v) FROM mode_double GROUP BY grp ORDER BY grp

-- ============================================================
-- Signed zeros: which value wins depends on whether -0.0 and 0.0 share a key
--
-- The counts are -0.0:2, 0.0:2, 5.0:3 (the SPARK-57329 reproducer). Keeping the zeros apart makes
-- 5.0 the unique winner; folding them together makes the zero win with 4. The two candidate
-- answers differ in magnitude, so this catches the divergence even where a `-0.0` vs `0.0`
-- difference would not be visible. Neither behaviour produces a tie, so there is no dependence on
-- Spark's non-deterministic tie-break.
-- ============================================================

statement
CREATE TABLE mode_signed_zero(v double, grp string) USING parquet

statement
INSERT INTO mode_signed_zero VALUES
  (-0.0D, 'a'), (-0.0D, 'a'),
  (0.0D, 'a'), (0.0D, 'a'),
  (5.0D, 'a'), (5.0D, 'a'), (5.0D, 'a')

query
SELECT grp, mode(v) FROM mode_signed_zero GROUP BY grp ORDER BY grp

-- ============================================================
-- NaN collapses to a single key on every version this file runs on
--
-- `doubleToLongBits` maps every NaN bit pattern to one value, so the two NaNs outvote the single
-- 1.0 and the mode is NaN.
-- ============================================================

statement
CREATE TABLE mode_nan(v double, grp string) USING parquet

statement
INSERT INTO mode_nan VALUES
  (CAST('NaN' AS DOUBLE), 'a'), (CAST('NaN' AS DOUBLE), 'a'), (1.0D, 'a')

query
SELECT grp, mode(v) FROM mode_nan GROUP BY grp ORDER BY grp
