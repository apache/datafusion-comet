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

-- Spark 4.0+ widens NextDay's dayOfWeek argument to collated strings, but NextDay resolves it
-- through DateTimeUtils.getDayOfWeekFromString, which upper-cases with Locale.ROOT and matches a
-- fixed literal set. That match takes no collation and the result is a DateType, so collation
-- cannot change the answer. The native kernel reads the argument as raw bytes though, so a
-- collated dayOfWeek is reported as Incompatible and routed through the JVM codegen dispatcher,
-- which runs Spark's own doGenCode inside the Comet pipeline. That keeps the projection in Comet
-- and is measurably faster than the native path: 70ms against 89ms for Spark over 1M rows. See
-- https://github.com/apache/datafusion-comet/issues/5591.

-- This harness disables ConstantFolding, so `x COLLATE ...` survives as a `Collate` node rather
-- than folding into a collated literal. Comet has no serde for `Collate`, but that does not matter
-- here: the dispatcher compiles the whole bound subtree with Spark's own doGenCode, `Collate`
-- included. That is one reason dispatching covers every spelling of a collated argument.

statement
CREATE TABLE test_next_day_collated(d date, dow string) USING parquet

statement
INSERT INTO test_next_day_collated VALUES
 (date('2023-01-01'), 'Monday'),
 (date('2024-02-29'), 'MON'),
 (date('1969-12-31'), 'mo'),
 (date('2024-06-15'), 'notaday'),
 (date('2024-01-01'), NULL),
 (NULL, 'Monday')

-- collated column argument: proves a collated column reaches the Comet operator at all
query
SELECT d, next_day(d, dow COLLATE UTF8_LCASE) FROM test_next_day_collated

-- collated literal argument
query
SELECT d, next_day(d, 'Monday' COLLATE UTF8_LCASE) FROM test_next_day_collated

-- Spark upper-cases the day name before matching, so all three spellings must agree under a
-- case-insensitive collation exactly as they do under UTF8_BINARY.
query
SELECT next_day(d, 'monday' COLLATE UTF8_LCASE), next_day(d, 'MoNdAy' COLLATE UTF8_LCASE), next_day(d, 'mO' COLLATE UTF8_LCASE) FROM test_next_day_collated

-- a second ICU collation, to show the guard is not UTF8_LCASE-only
query
SELECT d, next_day(d, dow COLLATE UNICODE_CI) FROM test_next_day_collated

query
SELECT next_day(d, 'Monday' COLLATE UNICODE_CI) FROM test_next_day_collated

-- unrecognised day name returns NULL outside ANSI mode (ANSI is covered by next_day_ansi.sql)
query
SELECT next_day(date('2024-01-01'), 'notaday' COLLATE UTF8_LCASE)

-- Collation does not introduce trimming: getDayOfWeekFromString matches character for character,
-- so a padded value is still NULL. Mirrors the whitespace case in next_day.sql.
query
SELECT next_day(date('2024-01-01'), ' MO ' COLLATE UTF8_LCASE), next_day(date('2024-01-01'), 'MO ' COLLATE UTF8_LCASE)

-- RTRIM collations are the case where trimming looks plausible: NextDay's inputTypes accept them
-- (StringTypeWithCollation(supportsTrimCollation = true)), and CollationFactory right-trims when
-- building a *collation key*. getDayOfWeekFromString never builds one, so a padded day name is
-- still unmatched and the answer is still NULL, exactly as under UTF8_BINARY.
query
SELECT next_day(date('2024-01-01'), 'MON' COLLATE UTF8_BINARY_RTRIM), next_day(date('2024-01-01'), 'MON ' COLLATE UTF8_BINARY_RTRIM)

query
SELECT next_day(date('2024-01-01'), 'mon' COLLATE UTF8_LCASE_RTRIM), next_day(date('2024-01-01'), 'mon ' COLLATE UTF8_LCASE_RTRIM)

-- literal + literal
query
SELECT next_day(date('2023-01-01'), 'Monday' COLLATE UTF8_LCASE), next_day(date('2023-01-01'), 'Sun' COLLATE UNICODE_CI)

-- A NULL-literal dayOfWeek is omitted: NextDay is nullIntolerant, so NullPropagation folds the
-- whole call to a NULL literal before Comet sees it. The NULL rows in the table above cover
-- NULL-in-data for both arguments.
