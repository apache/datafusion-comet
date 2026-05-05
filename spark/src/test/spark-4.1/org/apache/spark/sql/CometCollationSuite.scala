/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql

class CometCollationSuite extends CometTestBase {

  // Queries that group, sort, or shuffle on a non-default collated string must fall back to
  // Spark because Comet's shuffle/sort/aggregate compare raw bytes rather than collation-aware
  // keys. The shuffle-exchange rule is the primary line of defense (see #1947), so these tests
  // pin down the fallback reason it emits.
  private val hashShuffleCollationReason =
    "unsupported hash partitioning data type for columnar shuffle"
  private val rangeShuffleCollationReason =
    "unsupported range partitioning data type for columnar shuffle"

  test("listagg DISTINCT with utf8_lcase collation (issue #1947)") {
    checkSparkAnswerAndFallbackReason(
      "SELECT lower(listagg(DISTINCT c1 COLLATE utf8_lcase) " +
        "WITHIN GROUP (ORDER BY c1 COLLATE utf8_lcase)) " +
        "FROM (VALUES ('a'), ('B'), ('b'), ('A')) AS t(c1)",
      hashShuffleCollationReason)
  }

  test("DISTINCT on utf8_lcase collated string groups case-insensitively") {
    checkSparkAnswerAndFallbackReason(
      "SELECT DISTINCT c1 COLLATE utf8_lcase AS c " +
        "FROM (VALUES ('a'), ('A'), ('b'), ('B')) AS t(c1) ORDER BY c",
      hashShuffleCollationReason)
  }

  test("GROUP BY utf8_lcase collated string groups case-insensitively") {
    checkSparkAnswerAndFallbackReason(
      "SELECT lower(c1 COLLATE utf8_lcase) AS k, count(*) " +
        "FROM (VALUES ('a'), ('A'), ('b'), ('B')) AS t(c1) " +
        "GROUP BY c1 COLLATE utf8_lcase ORDER BY k",
      hashShuffleCollationReason)
  }

  test("ORDER BY utf8_lcase collated string sorts case-insensitively") {
    checkSparkAnswerAndFallbackReason(
      "SELECT c1 COLLATE utf8_lcase AS c " +
        "FROM (VALUES ('A'), ('b'), ('a'), ('B')) AS t(c1) ORDER BY c",
      rangeShuffleCollationReason)
  }

  test("default UTF8_BINARY string still runs through Comet") {
    // Sanity check that the collation fallback does not over-block the default string type.
    withParquetTable(Seq(("a", 1), ("b", 2), ("a", 3)), "tbl") {
      checkSparkAnswerAndOperator("SELECT DISTINCT _1 FROM tbl ORDER BY _1")
    }
  }
}
