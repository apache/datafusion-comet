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

-- MinSparkVersion: 4.0

-- Spark 4.0+ widens ArrayIntersect's input to collated strings. Set membership is collation-aware,
-- so Comet's native byte-based dedup would be wrong. Comet instead routes collated array_intersect
-- through the JVM codegen dispatcher (Spark's own doGenCode), which performs collation-aware
-- membership and matches Spark. Only the output elements' collation metadata is dropped (#2190).

-- UTF8_LCASE: 'A' and 'a' are equal, so the intersection is non-empty and case is taken from the
-- left array.
query
SELECT array_intersect(array('A' COLLATE UTF8_LCASE, 'b' COLLATE UTF8_LCASE), array('a' COLLATE UTF8_LCASE))

-- UTF8_LCASE with duplicates and a non-matching element
query
SELECT array_intersect(array('Hello' COLLATE UTF8_LCASE, 'WORLD' COLLATE UTF8_LCASE), array('hello' COLLATE UTF8_LCASE, 'x' COLLATE UTF8_LCASE))
