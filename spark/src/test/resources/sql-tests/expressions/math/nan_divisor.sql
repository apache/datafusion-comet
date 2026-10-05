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

-- Division and remainder pass a NaN divisor through, so `1.0D / (-d)` of a NaN is a NaN with the
-- sign bit set, the NaN that arithmetic produces on x86-64. Spark hashes every NaN alike, and
-- orders every NaN above every other value. ANSI mode divides through a different native path.
-- ConfigMatrix: spark.sql.ansi.enabled=false,true

statement
CREATE TABLE nan_divisor(d double, f float) USING parquet

statement
INSERT INTO nan_divisor VALUES (double('NaN'), float('NaN')), (1.5, float(1.5)), (NULL, NULL)

query
SELECT hash(1.0D / (-d)), xxhash64(1.0D / (-d)), hash(1.0D % (-d)), xxhash64(1.0D % (-d))
FROM nan_divisor

query
SELECT hash(1.0F % (-f)), xxhash64(1.0F % (-f)) FROM nan_divisor

query
SELECT least(1.0D / (-d), 0.0D), greatest(1.0D / (-d), 0.0D), least(1.0D % (-d), 0.0D),
  greatest(1.0F % (-f), 0.0F)
FROM nan_divisor

-- The union keeps the quotient in a projection below the aggregate.
query
SELECT max(q), min(q)
FROM (SELECT 1.0D / (-d) AS q FROM nan_divisor UNION ALL SELECT 0.0D AS q FROM nan_divisor)

-- percentile_approx orders a NaN with the sign bit set below every other value
-- (https://github.com/apache/datafusion-comet/issues/6519). Until that is fixed, Comet keeps
-- normalizing the divisor, which makes the quotient a canonical NaN.
query
SELECT percentile_approx(q, 1.0D), percentile_approx(q, 0.0D)
FROM (SELECT /*+ COALESCE(1) */ q
  FROM (SELECT 1.0D / (-d) AS q FROM nan_divisor UNION ALL SELECT 0.0D AS q FROM nan_divisor))
