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

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf

class CometSqlFileTestSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  /** Check if the current Spark version meets a minimum version requirement. */
  private def meetsMinSparkVersion(minVersion: String): Boolean = {
    val current = org.apache.spark.SPARK_VERSION.split("[.-]").take(2).map(_.toInt)
    val required = minVersion.split("[.-]").take(2).map(_.toInt)
    (current(0) > required(0)) ||
    (current(0) == required(0) && current(1) >= required(1))
  }

  /**
   * Check if the current Spark version is at or below a maximum version. Used by paired fixtures
   * where each version range has its own expected error class or output format.
   */
  private def meetsMaxSparkVersion(maxVersion: String): Boolean = {
    val current = org.apache.spark.SPARK_VERSION.split("[.-]").take(2).map(_.toInt)
    val ceiling = maxVersion.split("[.-]").take(2).map(_.toInt)
    (current(0) < ceiling(0)) ||
    (current(0) == ceiling(0) && current(1) <= ceiling(1))
  }

  /**
   * Build a human-readable reason string describing why a fixture is skipped on the current Spark
   * version. Returns None when both constraints are satisfied.
   */
  private def skipReason(parsed: SqlTestFile): Option[String] = {
    val minViolation = parsed.minSparkVersion.filter(!meetsMinSparkVersion(_))
    val maxViolation = parsed.maxSparkVersion.filter(!meetsMaxSparkVersion(_))
    (minViolation, maxViolation) match {
      case (Some(m), _) => Some(s"requires Spark >= $m")
      case (_, Some(m)) => Some(s"requires Spark <= $m")
      case _ => None
    }
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

  // Most SQL fixtures here predate Spark 4 ANSI default and expect non-ANSI semantics
  // (silent overflow/null on bad input). Individual files can opt in via their own
  // --CONFIG line, which appears later in the pair list and wins.
  private val ansiDisabled = Seq(SQLConf.ANSI_ENABLED.key -> "false")

  /**
   * Pin the sentinel-query convention for fixtures that route an `expect_error` through the
   * codegen dispatcher. See [[ExpectError]] for the failure mode this guards against.
   */
  private def requireSentinelForCodegenExpectError(
      relativePath: String,
      file: SqlTestFile): Unit = {
    val codegenFlagOn = file.configs.exists { case (k, v) =>
      k == CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key && v.equalsIgnoreCase("true")
    }
    if (!codegenFlagOn) return
    val hasExpectError = file.records.exists {
      case SqlQuery(_, _: ExpectError, _) => true
      case _ => false
    }
    if (!hasExpectError) return
    val hasSentinel = file.records.exists {
      case SqlQuery(_, CheckCoverageAndAnswer, _) => true
      case _ => false
    }
    assert(
      hasSentinel,
      s"SQL fixture $relativePath combines `expect_error` with " +
        s"${CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key}=true but is missing a non-error " +
        "sentinel query. Without one, a silent dispatcher fallback to Spark would let the " +
        "`expect_error` queries pass vacuously (Spark raises the same error on the fallback " +
        "path). Add at least one `query` over valid input so `checkSparkAnswerAndOperator` " +
        "fails if the expression did not execute natively.")
  }

  private def runTestFile(relativePath: String, file: SqlTestFile): Unit = {
    requireSentinelForCodegenExpectError(relativePath, file)
    val allConfigs = ansiDisabled ++ file.configs ++ constantFoldingExcluded
    withSQLConf(allConfigs: _*) {
      withTable(file.tables: _*) {
        file.records.foreach {
          case SqlStatement(sql, line) =>
            try {
              val location = if (line > 0) s"$relativePath:$line" else relativePath
              withClue(s"In SQL file $location, executing statement:\n$sql\n") {
                spark.sql(sql)
              }
            } catch {
              case e: Exception =>
                throw new RuntimeException(s"Error executing SQL '$sql' ${e.getMessage}", e)
            }
          case SqlQuery(sql, mode, line) =>
            try {
              val location = if (line > 0) s"$relativePath:$line" else relativePath
              withClue(s"In SQL file $location, executing query:\n$sql\n") {
                mode match {
                  case CheckCoverageAndAnswer =>
                    checkSparkAnswerAndOperator(sql)
                  case SparkAnswerOnly =>
                    checkSparkAnswer(sql)
                  case WithTolerance(tol) =>
                    checkSparkAnswerAndOperatorWithTolerance(sql, tol)
                  case ExpectFallback(reason) =>
                    checkSparkAnswerAndFallbackReason(sql, reason)
                  case Ignore(reason) =>
                    logInfo(s"IGNORED query ($reason): $sql")
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

            } catch {
              case e: Exception =>
                throw new RuntimeException(s"Error executing SQL '$sql' ${e.getMessage}", e)
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

    // Skip tests that fall outside the file's declared Spark version range.
    val skip = skipReason(parsed)

    if (combinations.size <= 1) {
      // No matrix or single combination
      test(s"sql-file: $relativePath") {
        skip match {
          case Some(reason) =>
            logInfo(s"SKIPPED ($reason): $relativePath")
          case None =>
            val effectiveConfigs = parsed.configs ++ combinations.headOption.getOrElse(Seq.empty)
            runTestFile(relativePath, parsed.copy(configs = effectiveConfigs))
        }
      }
    } else {
      // Multiple combinations: generate one test per combination
      combinations.foreach { matrixConfigs =>
        val label = matrixConfigs.map { case (k, v) => s"$k=$v" }.mkString(", ")
        test(s"sql-file: $relativePath [$label]") {
          skip match {
            case Some(reason) =>
              logInfo(s"SKIPPED ($reason): $relativePath")
            case None =>
              runTestFile(relativePath, parsed.copy(configs = parsed.configs ++ matrixConfigs))
          }
        }
      }
    }
  }
}
