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

-- Spark 4.0+ applies collation-aware delimiter matching in str_to_map. Comet's native
-- implementation matches raw delimiter bytes, so non-UTF8_BINARY collations must fall back.

-- collated input string
query expect_fallback(`str_to_map` does not support non-UTF8_BINARY collations)
SELECT str_to_map('a:1,b:2' COLLATE UTF8_LCASE, ',', ':')

-- collated pair delimiter
query expect_fallback(`str_to_map` does not support non-UTF8_BINARY collations)
SELECT str_to_map('aX1,bX2', 'x' COLLATE UTF8_LCASE, ':')

-- collated key-value delimiter
query expect_fallback(`str_to_map` does not support non-UTF8_BINARY collations)
SELECT str_to_map('aX1,bX2', ',', 'x' COLLATE UTF8_LCASE)
