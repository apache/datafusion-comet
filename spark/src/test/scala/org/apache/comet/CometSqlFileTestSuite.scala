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

import org.apache.comet.CometSparkSessionExtensions.isSpark41Plus

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

  // Disable constant folding so that literal expressions are evaluated by Comet's
  // native engine rather than being folded away by Spark's optimizer at plan time.
  private val constantFoldingExcluded = Seq(
    "spark.sql.optimizer.excludedRules" ->
      "org.apache.spark.sql.catalyst.optimizer.ConstantFolding")

  private def runTestFile(relativePath: String, file: SqlTestFile): Unit = {
    val allConfigs = file.configs ++ constantFoldingExcluded
    withSQLConf(allConfigs: _*) {
      withTable(file.tables: _*) {
        file.records.foreach {
          case SqlStatement(sql, line) =>
            val location = if (line > 0) s"$relativePath:$line" else relativePath
            withClue(s"In SQL file $location, executing statement:\n$sql\n") {
              spark.sql(sql)
            }
          case SqlQuery(sql, mode, line) =>
            val location = if (line > 0) s"$relativePath:$line" else relativePath
            withClue(s"In SQL file $location, executing query:\n$sql\n") {
              mode match {
                case CheckCoverageAndAnswer =>
                  checkSparkAnswerAndOperator(sql)
                case SparkAnswerOnly =>
                  checkSparkAnswer(sql)
                case WithTolerance(tol) =>
                  checkSparkAnswerWithTolerance(sql, tol)
                case ExpectFallback(reason) =>
                  checkSparkAnswerAndFallbackReason(sql, reason)
                case Ignore(reason) =>
                  logInfo(s"IGNORED query (${reason}): $sql")
                case ExpectError(pattern) =>
                  val (sparkError, cometError) = checkSparkAnswerMaybeThrows(spark.sql(sql))
                  assert(
                    sparkError.isDefined,
                    s"Expected Spark to throw an error matching '$pattern' but query succeeded")
                  assert(
                    cometError.isDefined,
                    s"Expected Comet to throw an error matching '$pattern' but query succeeded")
                  assert(
                    sparkError.get.getMessage.contains(pattern),
                    s"Spark error '${sparkError.get.getMessage}' does not contain '$pattern'")
                  assert(
                    cometError.get.getMessage.contains(pattern),
                    s"Comet error '${cometError.get.getMessage}' does not contain '$pattern'")
              }
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
    val skip = parsed.minSparkVersion.exists(!meetsMinSparkVersion(_)) || isSpark41Plus

    if (combinations.size <= 1) {
      // No matrix or single combination
      test(s"sql-file: $relativePath") {
        if (skip) {
          if (isSpark41Plus) {
            logInfo(s"SKIPPED for Spark 4.1: $relativePath")
          } else {
            logInfo(s"SKIPPED (requires Spark ${parsed.minSparkVersion.get}): $relativePath")
          }
        } else {
          val effectiveConfigs = parsed.configs ++ combinations.headOption.getOrElse(Seq.empty)
          runTestFile(relativePath, parsed.copy(configs = effectiveConfigs))
        }
      }
    } else {
      // Multiple combinations: generate one test per combination
      combinations.foreach { matrixConfigs =>
        val label = matrixConfigs.map { case (k, v) => s"$k=$v" }.mkString(", ")
        test(s"sql-file: $relativePath [$label]") {
          if (skip) {
            if (isSpark41Plus) {
              logInfo(s"SKIPPED for Spark 4.1: $relativePath")
            } else {
              logInfo(s"SKIPPED (requires Spark ${parsed.minSparkVersion.get}): $relativePath")
            }
          } else {
            runTestFile(relativePath, parsed.copy(configs = parsed.configs ++ matrixConfigs))
          }
        }
      }
    }
  }
}
