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

package org.apache.comet

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.internal.SQLConf

class CometConfSuite extends AnyFunSuite {

  test("primary key wins over alternative when both are set") {
    val entry = CometConf
      .conf("spark.comet.testing.alias.primaryWins")
      .withAlternative("spark.comet.testing.alias.primaryWins.old")
      .category("testing")
      .booleanConf
      .createWithDefault(false)

    val conf = new SQLConf
    conf.setConfString(entry.key, "true")
    conf.setConfString(entry.alternatives.head, "false")

    assert(entry.get(conf))
  }

  test("alternative is read when primary key is unset, with expected value") {
    val entry = CometConf
      .conf("spark.comet.testing.alias.readsAlternative")
      .withAlternative("spark.comet.testing.alias.readsAlternative.old")
      .category("testing")
      .intConf
      .createWithDefault(0)

    val conf = new SQLConf
    conf.setConfString(entry.alternatives.head, "42")

    assert(entry.get(conf) == 42)
  }

  test("default is returned when neither primary nor alternative is set") {
    val entry = CometConf
      .conf("spark.comet.testing.alias.defaultOnly")
      .withAlternative("spark.comet.testing.alias.defaultOnly.old")
      .category("testing")
      .booleanConf
      .createWithDefault(true)

    val conf = new SQLConf
    assert(entry.get(conf))
  }

  test("multiple alternatives are checked in the order provided") {
    val entry = CometConf
      .conf("spark.comet.testing.alias.multi")
      .withAlternative(
        "spark.comet.testing.alias.multi.older",
        "spark.comet.testing.alias.multi.oldest")
      .category("testing")
      .intConf
      .createWithDefault(0)

    val conf = new SQLConf
    conf.setConfString(entry.alternatives.head, "1")
    conf.setConfString(entry.alternatives(1), "2")

    // First alternative wins, not the second.
    assert(entry.get(conf) == 1)
  }

  test("OptionalConfigEntry reads through an alternative") {
    val entry = CometConf
      .conf("spark.comet.testing.alias.optional")
      .withAlternative("spark.comet.testing.alias.optional.old")
      .category("testing")
      .stringConf
      .createOptional

    val conf = new SQLConf
    assert(entry.get(conf).isEmpty)

    conf.setConfString(entry.alternatives.head, "hello")
    assert(entry.get(conf).contains("hello"))
  }

  test("value from an alternative goes through the type converter") {
    val entry = CometConf
      .conf("spark.comet.testing.alias.typed")
      .withAlternative("spark.comet.testing.alias.typed.old")
      .category("testing")
      .booleanConf
      .createWithDefault(false)

    val conf = new SQLConf
    conf.setConfString(entry.alternatives.head, "TRUE")

    // The boolean converter accepts case-insensitive "TRUE"/"FALSE"; if the alternative
    // were returned raw, this assertion would fail.
    assert(entry.get(conf))
  }

  test("COMET_FORCE_SHJ reads the deprecated replaceSortMergeJoin key as an alias") {
    val conf = new SQLConf
    conf.setConfString(s"${CometConf.COMET_EXEC_CONFIG_PREFIX}.replaceSortMergeJoin", "true")

    assert(CometConf.COMET_FORCE_SHJ.get(conf))
  }

  test("COMET_EXPLAIN_CODEGEN_ENABLED reads deprecated explainCodegen.enabled as an alias") {
    val conf = new SQLConf
    conf.setConfString("spark.comet.explainCodegen.enabled", "true")

    assert(CometConf.COMET_EXPLAIN_CODEGEN_ENABLED.get(conf))
  }

  test("COMET_EXPLAIN_FALLBACK_ENABLED reads deprecated explainFallback.enabled as an alias") {
    val conf = new SQLConf
    conf.setConfString("spark.comet.explainFallback.enabled", "true")

    assert(CometConf.COMET_EXPLAIN_FALLBACK_ENABLED.get(conf))
  }

  test(
    "COMET_EXPLAIN_FALLBACK_LOG_ENABLED reads deprecated logFallbackReasons.enabled as alias") {
    val conf = new SQLConf
    conf.setConfString("spark.comet.logFallbackReasons.enabled", "true")

    assert(CometConf.COMET_EXPLAIN_FALLBACK_LOG_ENABLED.get(conf))
  }
}
