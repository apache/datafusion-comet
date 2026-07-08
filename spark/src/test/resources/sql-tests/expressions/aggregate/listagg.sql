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

-- listagg / string_agg is available starting in Spark 4.0.
-- MinSparkVersion: 4.0
-- ConfigMatrix: parquet.enable.dictionary=false,true

-- ============================================================
-- Setup
-- ============================================================

statement
CREATE TABLE la_src(v string, grp string) USING parquet

statement
INSERT INTO la_src VALUES
  ('a', 'g1'), ('b', 'g1'), ('c', 'g1'),
  ('x', 'g2'), (NULL, 'g2'), ('y', 'g2'),
  (NULL, 'g3'), (NULL, 'g3'),
  ('', 'g4'), ('z', 'g4')

statement
CREATE TABLE la_empty(v string) USING parquet

statement
CREATE TABLE la_dupes(v string, grp string) USING parquet

statement
INSERT INTO la_dupes VALUES
  ('a', 'g1'), ('a', 'g1'), ('b', 'g1'),
  ('c', 'g2'), ('c', 'g2'), ('c', 'g2')

statement
CREATE TABLE la_utf8(v string, grp string) USING parquet

-- Multibyte UTF-8: `café` (composed é U+00E9), `naïve` (composed ï U+00EF),
-- `日本語` (three CJK codepoints), and an emoji sequence.
statement
INSERT INTO la_utf8 VALUES
  ('café', 'g1'), ('naïve', 'g1'), ('日本語', 'g1'),
  ('한글', 'g2'), ('☕️', 'g2'), (NULL, 'g2')

statement
CREATE TABLE la_bin(v binary, grp string) USING parquet

statement
INSERT INTO la_bin VALUES
  (X'DEAD', 'g1'), (X'BEEF', 'g1'), (NULL, 'g1'),
  (X'CAFE', 'g2')

-- ============================================================
-- Basic: literal delimiter, sort the group so results are
-- deterministic across shuffles.
-- ============================================================

query
SELECT grp, listagg(v, ',') FROM (SELECT * FROM la_src ORDER BY grp, v) GROUP BY grp ORDER BY grp

-- Alias `string_agg` should route through the same expression class.
query
SELECT grp, string_agg(v, ',') FROM (SELECT * FROM la_src ORDER BY grp, v) GROUP BY grp ORDER BY grp

-- ============================================================
-- No delimiter (defaults to empty string / NULL delimiter)
-- ============================================================

query
SELECT grp, listagg(v) FROM (SELECT * FROM la_src ORDER BY grp, v) GROUP BY grp ORDER BY grp

-- ============================================================
-- All-NULL group returns NULL; empty table returns NULL.
-- ============================================================

query
SELECT listagg(v, ',') FROM la_empty

query
SELECT grp, listagg(v, '-') FROM (SELECT * FROM la_src WHERE grp = 'g3' ORDER BY v) GROUP BY grp

-- ============================================================
-- DISTINCT falls back to Spark: Comet rejects multi-column
-- distinct aggregates (listagg has two children: value and
-- delimiter), so `listagg(DISTINCT v, ',')` is not run natively.
-- ============================================================

query expect_fallback(Multi-column distinct aggregate not supported)
SELECT grp, listagg(DISTINCT v, ',') FROM (SELECT * FROM la_dupes ORDER BY grp, v) GROUP BY grp ORDER BY grp

-- ============================================================
-- Global aggregate (no GROUP BY)
-- ============================================================

query
SELECT listagg(v, '|') FROM (SELECT * FROM la_src WHERE grp = 'g1' ORDER BY v)

-- ============================================================
-- Multi-char delimiter and empty-string delimiter
-- ============================================================

query
SELECT grp, listagg(v, ' -> ') FROM (SELECT * FROM la_src WHERE grp = 'g1' ORDER BY v) GROUP BY grp

query
SELECT grp, listagg(v, '') FROM (SELECT * FROM la_src WHERE grp = 'g1' ORDER BY v) GROUP BY grp

-- ============================================================
-- Empty-string values inside the group
-- ============================================================

query
SELECT grp, listagg(v, ',') FROM (SELECT * FROM la_src WHERE grp = 'g4' ORDER BY v) GROUP BY grp

-- ============================================================
-- Multibyte UTF-8 values (composed accents, CJK, emoji)
-- ============================================================

query
SELECT grp, listagg(v, ',') FROM (SELECT * FROM la_utf8 ORDER BY grp, v) GROUP BY grp ORDER BY grp

-- Multi-byte delimiter
query
SELECT grp, listagg(v, '·') FROM (SELECT * FROM la_utf8 ORDER BY grp, v) GROUP BY grp ORDER BY grp

-- ============================================================
-- WITHIN GROUP (ORDER BY ...) is not implemented natively;
-- both ascending and descending must fall back to Spark.
-- ============================================================

query expect_fallback(`WITHIN GROUP (ORDER BY ...)` is not supported)
SELECT grp, listagg(v, ',') WITHIN GROUP (ORDER BY v) FROM la_src GROUP BY grp ORDER BY grp

query expect_fallback(`WITHIN GROUP (ORDER BY ...)` is not supported)
SELECT grp, listagg(v, ',') WITHIN GROUP (ORDER BY v DESC) FROM la_src GROUP BY grp ORDER BY grp

-- ============================================================
-- BinaryType inputs are not supported natively; must fall back.
-- ============================================================

query expect_fallback(Unsupported child data type: BinaryType)
SELECT grp, listagg(v) FROM la_bin GROUP BY grp ORDER BY grp

query expect_fallback(Unsupported child data type: BinaryType)
SELECT grp, listagg(v, X'42') FROM la_bin GROUP BY grp ORDER BY grp
