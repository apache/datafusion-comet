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

-- The ordered forms of `mode` set `reverseOpt`, which gives them deterministic tie-breaking that
-- Comet does not implement, so they must fall back to Spark. `allowIncompatible` is enabled so
-- that the ordering check is the only thing that can trigger a fallback; without it every query
-- here would fall back for the unrelated tie-break reason and the assertions would be vacuous.
--
-- `mode(col, reverse)` and `mode() WITHIN GROUP (ORDER BY col)` are Spark 4.0 features; on Spark
-- 3.x `Mode` only has the plain `mode(col)` form, which mode.sql covers.
-- MinSparkVersion: 4.0
-- Config: spark.comet.expression.Mode.allowIncompatible=true

statement
CREATE TABLE mode_ordered(v int, grp string) USING parquet

statement
INSERT INTO mode_ordered VALUES
  (10, 'a'), (10, 'a'), (7, 'a'), (NULL, 'a'),
  (5, 'b'), (5, 'b'), (5, 'b'), (9, 'b')

-- ============================================================
-- Sentinel: the plain form still runs natively under this config, so a silent whole-expression
-- regression cannot make the fallback assertions below pass vacuously.
-- ============================================================

query
SELECT grp, mode(v) FROM mode_ordered GROUP BY grp ORDER BY grp

-- ============================================================
-- Deterministic-flag form
--
-- `ModeBuilder` only sets `reverseOpt` for `mode(col, true)`; it rewrites `mode(col, false)` to the
-- plain `Mode(child)` (Mode.scala, `ModeBuilder.build`), so the `false` form must still run
-- natively. That asymmetry is the reason `modeHasUnsupportedOrdering` keys off `reverseOpt` rather
-- than off the argument count.
-- ============================================================

query
SELECT mode(v, false) FROM mode_ordered

query expect_fallback(mode with a deterministic flag or WITHIN GROUP ordering is not supported)
SELECT mode(v, true) FROM mode_ordered

-- ============================================================
-- WITHIN GROUP form, ascending and descending
-- ============================================================

query expect_fallback(mode with a deterministic flag or WITHIN GROUP ordering is not supported)
SELECT mode() WITHIN GROUP (ORDER BY v) FROM mode_ordered

query expect_fallback(mode with a deterministic flag or WITHIN GROUP ordering is not supported)
SELECT mode() WITHIN GROUP (ORDER BY v DESC) FROM mode_ordered

query expect_fallback(mode with a deterministic flag or WITHIN GROUP ordering is not supported)
SELECT grp, mode() WITHIN GROUP (ORDER BY v) FROM mode_ordered GROUP BY grp ORDER BY grp
