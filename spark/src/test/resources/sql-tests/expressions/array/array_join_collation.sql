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

-- Spark 4.0+ widens ArrayJoin's input to collated strings. Comet's native array_to_string matches
-- raw UTF8_BINARY bytes and does not propagate non-default collations, so collated inputs fall
-- back to Spark.

-- collated array elements
query expect_fallback(array_join on collated strings is not supported)
SELECT array_join(array('a' COLLATE UTF8_LCASE, 'b' COLLATE UTF8_LCASE), ',')

-- collated elements with null replacement
query expect_fallback(array_join on collated strings is not supported)
SELECT array_join(array('a' COLLATE UTF8_LCASE, NULL, 'c' COLLATE UTF8_LCASE), ',', 'NULL')
