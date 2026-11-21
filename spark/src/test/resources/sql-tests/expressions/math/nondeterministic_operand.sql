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

-- Config: spark.sql.ansi.enabled=false
-- Config: spark.comet.batchSize=2

statement
CREATE TABLE test_operand(id BIGINT) USING parquet

-- One file makes monotonically_increasing_id() equal to id. Evaluating it again on just
-- the rows that passed a domain check shifts the sequence, including in subsequent batches.
statement
INSERT INTO test_operand SELECT id FROM range(0, 8, 1, 1)

query expect_native(divide, integraldivide, remainder)
SELECT id, id / monotonically_increasing_id(),
       id DIV monotonically_increasing_id(), id % monotonically_increasing_id()
FROM test_operand

-- Cycle through -1, 0, 1 so the expected results are NULL, NULL, and exactly zero.
query expect_native(ln, log10, log2)
SELECT id, ln(monotonically_increasing_id() % 3 - 1),
       log10(monotonically_increasing_id() % 3 - 1),
       log2(monotonically_increasing_id() % 3 - 1)
FROM test_operand
