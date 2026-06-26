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

-- ============================================================
-- Setup: tables
-- ============================================================

statement
CREATE TABLE cl_src(k int, v string, grp string) USING parquet

statement
INSERT INTO cl_src VALUES
  (1, 'b', 'g1'),
  (1, 'a', 'g1'),
  (2, 'b', 'g1'),
  (NULL, NULL, 'g1'),
  (3, 'c', 'g2'),
  (3, 'c', 'g2'),
  (4, 'd', 'g2'),
  (NULL, 'z', 'g3')

statement
CREATE TABLE cl_empty(v string) USING parquet

-- ============================================================
-- Basic: global collect_list keeps duplicate non-null values
-- ============================================================

query
SELECT sort_array(collect_list(v)) FROM cl_src

-- ============================================================
-- GROUP BY: collect_list per group
-- ============================================================

query
SELECT grp, sort_array(collect_list(v)) FROM cl_src GROUP BY grp ORDER BY grp

-- ============================================================
-- Alias: array_agg uses the collect_list implementation
-- ============================================================

query
SELECT grp, sort_array(array_agg(k)) FROM cl_src GROUP BY grp ORDER BY grp

-- ============================================================
-- Empty table: returns empty array
-- ============================================================

query
SELECT sort_array(collect_list(v)) FROM cl_empty

-- ============================================================
-- PartialMerge: collect_list combined with a distinct aggregate
-- exercises native intermediate ArrayType state through shuffle
-- ============================================================

query
SELECT grp, count(DISTINCT k), sort_array(collect_list(v))
FROM cl_src GROUP BY grp ORDER BY grp

-- ============================================================
-- PartialMerge: multiple collect-style aggregates with distinct
-- ============================================================

query
SELECT grp,
       count(DISTINCT k),
       sort_array(collect_list(v)),
       sort_array(collect_set(v))
FROM cl_src GROUP BY grp ORDER BY grp
