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

import org.apache.logging.log4j.Level
import org.apache.spark.sql.CometTestBase

class CometConfValidatorSuite extends CometTestBase {

  private def warningsDuring(keys: String*)(f: => Unit): Seq[String] = {
    keys.foreach(CometConfValidator.checkedKeys.remove)
    // Logging derives the logger name by stripping the object's trailing '$'
    val logger = CometConfValidator.getClass.getName.stripSuffix("$")
    val appender = new LogAppender("unrecognized Comet configuration warning")
    withLogAppender(appender, Seq(logger), Some(Level.WARN))(f)
    // Keys that the test base sets for every test may warn too, so keep only these keys
    appender.loggingEvents
      .map(_.getMessage.getFormattedMessage)
      .filter(message => keys.exists(key => message.contains(s"'$key'")))
      .toSeq
  }

  test("warns once per key set on the session, with a suggestion") {
    val key = "spark.comet.exec.enabeld"
    val warnings = warningsDuring(key) {
      withSQLConf(key -> "true") {
        spark.range(1).count()
        spark.range(2).count()
      }
    }
    assert(
      warnings == Seq(s"Comet configuration '$key' is not recognized. " +
        "Did you mean 'spark.comet.exec.enabled'?"))
  }

  test("warns when Comet is disabled, since a misspelled key can be why") {
    val key = "spark.comet.foo.bar"
    val warnings = warningsDuring(key) {
      withSQLConf(CometConf.COMET_ENABLED.key -> "false", key -> "true") {
        spark.range(1).count()
      }
    }
    assert(
      warnings == Seq(s"Comet configuration '$key' is not recognized. Check the key for typos."))
  }

  test("does not warn for recognized keys") {
    val keys = Seq("spark.comet.expression.Cast.enabled", CometConf.COMET_EXEC_ENABLED.key)
    val warnings = warningsDuring(keys: _*) {
      withSQLConf(keys.map(_ -> "true"): _*) {
        spark.range(1).count()
      }
    }
    assert(warnings.isEmpty)
  }
}
