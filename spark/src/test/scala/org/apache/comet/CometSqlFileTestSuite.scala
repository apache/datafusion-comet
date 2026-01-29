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

import java.io.File

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

class CometSqlFileTestSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      withSQLConf(CometConf.COMET_NATIVE_SCAN_IMPL.key -> CometConf.SCAN_AUTO) {
        testFun
      }
    }
  }

  /** Check if the current Spark version meets a minimum version requirement. */
  private def meetsMinSparkVersion(minVersion: String): Boolean = {
    val current = org.apache.spark.SPARK_VERSION.split("[.-]").take(2).map(_.toInt)
    val required = minVersion.split("[.-]").take(2).map(_.toInt)
    (current(0) > required(0)) ||
    (current(0) == required(0) && current(1) >= required(1))
  }

  private val testResourceDir = {
    val url = getClass.getClassLoader.getResource("sql-tests")
    assert(url != null, "Could not find sql-tests resource directory")
    new File(url.toURI)
  }

  private def discoverTestFiles(dir: File): Seq[File] = {
    if (!dir.exists()) return Seq.empty
    val files = dir.listFiles().toSeq
    val sqlFiles = files.filter(f => f.isFile && f.getName.endsWith(".sql"))
    val subDirFiles = files.filter(_.isDirectory).flatMap(discoverTestFiles)
    sqlFiles ++ subDirFiles
  }

  /** Generate all config combinations from a ConfigMatrix specification. */
  private def configMatrix(matrix: Seq[(String, Seq[String])]): Seq[Seq[(String, String)]] = {
    if (matrix.isEmpty) return Seq(Seq.empty)
    val (key, values) = matrix.head
    val rest = configMatrix(matrix.tail)
    for {
      value <- values
      combo <- rest
    } yield (key, value) +: combo
  }

  private def runTestFile(file: SqlTestFile): Unit = {
    val allConfigs = file.configs
    withSQLConf(allConfigs: _*) {
      withTable(file.tables: _*) {
        file.records.foreach {
          case SqlStatement(sql) =>
            spark.sql(sql)
          case SqlQuery(sql, mode) =>
            mode match {
              case CheckOperator =>
                checkSparkAnswerAndOperator(sql)
              case SparkAnswerOnly =>
                checkSparkAnswer(sql)
              case WithTolerance(tol) =>
                checkSparkAnswerWithTolerance(sql, tol)
              case ExpectFallback(reason) =>
                checkSparkAnswerAndFallbackReason(sql, reason)
              case Ignore(reason) =>
                logInfo(s"IGNORED query (${reason}): $sql")
            }
        }
      }
    }
  }

  // Discover and register all .sql test files
  discoverTestFiles(testResourceDir).foreach { file =>
    val relativePath = testResourceDir.toURI.relativize(file.toURI).getPath
    val parsed = SqlFileTestParser.parse(file)
    val combinations = configMatrix(parsed.configMatrix)

    // Skip tests that require a newer Spark version
    val skip = parsed.minSparkVersion.exists(!meetsMinSparkVersion(_))

    if (combinations.size <= 1) {
      // No matrix or single combination
      test(s"sql-file: $relativePath") {
        if (skip) {
          logInfo(s"SKIPPED (requires Spark ${parsed.minSparkVersion.get}): $relativePath")
        } else {
          val effectiveConfigs = parsed.configs ++ combinations.headOption.getOrElse(Seq.empty)
          runTestFile(parsed.copy(configs = effectiveConfigs))
        }
      }
    } else {
      // Multiple combinations: generate one test per combination
      combinations.foreach { matrixConfigs =>
        val label = matrixConfigs.map { case (k, v) => s"$k=$v" }.mkString(", ")
        test(s"sql-file: $relativePath [$label]") {
          if (skip) {
            logInfo(s"SKIPPED (requires Spark ${parsed.minSparkVersion.get}): $relativePath")
          } else {
            runTestFile(parsed.copy(configs = parsed.configs ++ matrixConfigs))
          }
        }
      }
    }
  }
}
