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

  test("listagg DISTINCT with utf8_lcase collation (issue #1947)") {
    checkSparkAnswer(
      "SELECT lower(listagg(DISTINCT c1 COLLATE utf8_lcase) " +
        "WITHIN GROUP (ORDER BY c1 COLLATE utf8_lcase)) " +
        "FROM (VALUES ('a'), ('B'), ('b'), ('A')) AS t(c1)")
  }

  test("DISTINCT on utf8_lcase collated string groups case-insensitively") {
    checkSparkAnswer(
      "SELECT DISTINCT c1 COLLATE utf8_lcase AS c " +
        "FROM (VALUES ('a'), ('A'), ('b'), ('B')) AS t(c1) ORDER BY c")
  }

  test("GROUP BY utf8_lcase collated string groups case-insensitively") {
    checkSparkAnswer(
      "SELECT lower(c1 COLLATE utf8_lcase) AS k, count(*) " +
        "FROM (VALUES ('a'), ('A'), ('b'), ('B')) AS t(c1) " +
        "GROUP BY c1 COLLATE utf8_lcase ORDER BY k")
  }

  test("default UTF8_BINARY string still runs through Comet") {
    // Sanity check that the collation fallback does not over-block the default string type.
    checkSparkAnswer("SELECT DISTINCT c1 FROM (VALUES ('a'), ('b'), ('a')) AS t(c1) ORDER BY c1")
  }
}
