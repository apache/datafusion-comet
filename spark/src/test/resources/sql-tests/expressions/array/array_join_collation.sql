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

-- Spark 4.0+ widens ArrayJoin's input to collated strings. Concatenation is collation-independent,
-- so the joined value is always correct; Comet routes collated array_join through the JVM codegen
-- dispatcher (Spark's own doGenCode), keeping it native and matching Spark. Only the output
-- string's collation metadata is dropped (tracked by #2190).

-- collated array elements
query
SELECT array_join(array('a' COLLATE UTF8_LCASE, 'b' COLLATE UTF8_LCASE), ',')

-- collated elements with null replacement
query
SELECT array_join(array('a' COLLATE UTF8_LCASE, NULL, 'c' COLLATE UTF8_LCASE), ',', 'NULL')
