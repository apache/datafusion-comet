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

-- hash functions
statement
CREATE TABLE test(col string, a int, b float) USING parquet

statement
INSERT INTO test VALUES ('Spark SQL  ', 10, 1.2), (NULL, NULL, NULL), ('', 0, 0.0), ('苹果手机', NULL, 3.999999), ('Spark SQL  ', 10, 1.2), (NULL, NULL, NULL), ('', 0, 0.0), ('苹果手机', NULL, 3.999999)

query
SELECT md5(col), md5(cast(a as string)), md5(cast(b as string)), hash(col), hash(col, 1), hash(col, 0), hash(col, a, b), hash(b, a, col), xxhash64(col), xxhash64(col, 1), xxhash64(col, 0), xxhash64(col, a, b), xxhash64(b, a, col), sha2(col, 0), sha2(col, 256), sha2(col, 224), sha2(col, 384), sha2(col, 512), sha2(col, 128), sha2(col, -1), sha1(col), sha1(cast(a as string)), sha1(cast(b as string)) FROM test

-- literal arguments
query ignore(https://github.com/apache/datafusion-comet/issues/3340)
SELECT md5('Spark SQL'), sha1('test'), sha2('test', 256), hash('test'), xxhash64('test')
