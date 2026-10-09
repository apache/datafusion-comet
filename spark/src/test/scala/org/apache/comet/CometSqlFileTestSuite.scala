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

import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.expressions.{Abs, CreateArray, Literal}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.ArrayType

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
  private val constantFoldingRule = "org.apache.spark.sql.catalyst.optimizer.ConstantFolding"

  // Most SQL fixtures here predate Spark 4 ANSI default and expect non-ANSI semantics
  // (silent overflow/null on bad input). Individual files can opt in via their own
  // --CONFIG line, which appears later in the pair list and wins.
  private val ansiDisabled = Seq(SQLConf.ANSI_ENABLED.key -> "false")

  /** Apply file settings and restore every SQLConf change, including mid-file SET and RESET. */
  private def withFixtureConfigs(file: SqlTestFile)(body: => Unit): Unit = {
    val conf = spark.sessionState.conf
    val original = conf.getAllConfs
    val optimizerKey = SQLConf.OPTIMIZER_EXCLUDED_RULES.key
    // Matrix settings are appended to configs, so the last value for a key wins as before.
    val configuredRules = file.configs.reverse
      .collectFirst { case (`optimizerKey`, value) =>
        value
      }
      .toSeq
      .flatMap(_.split(","))
    val rules = (configuredRules ++ file.excludedRules ++
      (if (file.constantFoldingEnabled) Seq.empty else Seq(constantFoldingRule)))
      .map(_.trim)
      .filter(_.nonEmpty)
      .distinct
    require(
      !file.constantFoldingEnabled || !rules.contains(constantFoldingRule),
      "ConstantFolding: enabled conflicts with an explicit ConstantFolding exclusion")
    // Spark silently ignores unknown and non-excludable rules. A fixture must fail instead of
    // claiming to test a path that its optimizer settings never reached.
    val optimizer = spark.sessionState.optimizer
    val knownRules = optimizer.defaultBatches.flatMap(_.rules).map(_.ruleName).toSet
    rules.foreach { rule =>
      require(knownRules.contains(rule), s"Unknown optimizer rule: $rule")
      require(
        !optimizer.nonExcludableRules.contains(rule),
        s"Cannot exclude optimizer rule: $rule")
    }
    val configs = ansiDisabled ++ file.configs.filterNot(_._1 == optimizerKey) :+
      (optimizerKey -> rules.mkString(","))
    try {
      withSQLConf(configs: _*)(body)
    } finally {
      val current = conf.getAllConfs
      (current.keySet -- original.keySet).foreach(conf.unsetConf)
      original.foreach { case (key, value) =>
        if (current.get(key) != Some(value)) conf.setConfString(key, value)
      }
    }
  }

  /**
   * Pin the sentinel-query convention for fixtures that route an `expect_error` through the
   * codegen dispatcher. See [[ExpectError]] for the failure mode this guards against.
   */
  private def requireSentinelForCodegenExpectError(
      relativePath: String,
      file: SqlTestFile): Unit = {
    val codegenFlagOn = spark.sessionState.conf.getAllConfs
      .get(CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key)
      .exists(_.equalsIgnoreCase("true"))
    if (!codegenFlagOn) return
    val hasExpectError = file.records.exists {
      case SqlQuery(_, _: ExpectError, _) => true
      case _ => false
    }
    if (!hasExpectError) return
    // `ExpectDispatch` and `ExpectNative` both run `checkSparkAnswerAndImpl`, which performs the
    // same answer and operator checks as a plain `query` before asserting anything extra, so
    // either is a strictly stronger sentinel than `CheckCoverageAndAnswer`. Not accepting them
    // meant upgrading a file's last positive query to one of the new modes made preflight reject
    // the file for having no sentinel, forcing a redundant plain query alongside it.
    val hasSentinel = file.records.exists {
      case SqlQuery(_, CheckCoverageAndAnswer | _: ExpectDispatch | _: ExpectNative, _) => true
      case _ => false
    }
    assert(
      hasSentinel,
      s"SQL fixture $relativePath combines `expect_error` with " +
        s"${CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key}=true but is missing a non-error " +
        "sentinel query. Without one, a silent dispatcher fallback to Spark would let the " +
        "`expect_error` queries pass vacuously (Spark raises the same error on the fallback " +
        "path). Add at least one `query`, `expect_dispatch` or `expect_native` over valid " +
        "input so the operator check fails if the expression did not execute natively.")
  }

  private def runTestFile(relativePath: String, file: SqlTestFile): Unit = {
    withFixtureConfigs(file) {
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
                  case ExpectDispatch(names) =>
                    checkSparkAnswerAndImpl(sql, native = Seq.empty, dispatched = names)
                  case ExpectNative(names) =>
                    checkSparkAnswerAndImpl(sql, native = names, dispatched = Seq.empty)
                  case Ignore(reason) =>
                    logInfo(s"IGNORED query ($reason): $sql")
                  case ExpectError(pattern) =>
                    // Check the effective setting after any mid-file SET or RESET.
                    requireSentinelForCodegenExpectError(relativePath, file)
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

  test("SQL fixture optimizer directives control the optimized expression") {
    val default = SqlFileTestParser.parse(Seq.empty)
    withFixtureConfigs(default) {
      val plan = spark.sql("SELECT array(1, 2)").queryExecution.optimizedPlan
      assert(plan.expressions.exists(_.exists(_.isInstanceOf[CreateArray])))
      val nullPlan = spark.sql("SELECT abs(CAST(NULL AS INT))").queryExecution.optimizedPlan
      assert(!nullPlan.expressions.exists(_.exists(_.isInstanceOf[Abs])))
    }
    val folded = SqlFileTestParser.parse(Seq("-- ConstantFolding: enabled"))
    withFixtureConfigs(folded) {
      val plan = spark.sql("SELECT array(1, 2)").queryExecution.optimizedPlan
      assert(plan.expressions.exists(_.exists {
        case Literal(_, _: ArrayType) => true
        case _ => false
      }))
      assert(!plan.expressions.exists(_.exists(_.isInstanceOf[CreateArray])))
    }
    val excluded = SqlFileTestParser.parse(
      Seq("-- ExcludeRules: org.apache.spark.sql.catalyst.optimizer.NullPropagation"))
    withFixtureConfigs(excluded) {
      val plan = spark.sql("SELECT abs(CAST(NULL AS INT))").queryExecution.optimizedPlan
      assert(plan.expressions.exists(_.exists(_.isInstanceOf[Abs])))
      assert(spark.conf.get(SQLConf.OPTIMIZER_EXCLUDED_RULES.key).contains(constantFoldingRule))
    }
  }

  test("SQL fixture optimizer Config and matrix settings retain precedence") {
    val optimizerKey = SQLConf.OPTIMIZER_EXCLUDED_RULES.key
    val nullPropagation = "org.apache.spark.sql.catalyst.optimizer.NullPropagation"
    val eliminateSorts = "org.apache.spark.sql.catalyst.optimizer.EliminateSorts"
    val file = SqlFileTestParser.parse(
      Seq(
        s"-- Config: $optimizerKey=$nullPropagation",
        s"-- ConfigMatrix: $optimizerKey=$eliminateSorts,$nullPropagation",
        s"-- ExcludeRules: $nullPropagation, $nullPropagation"))
    val original = spark.sessionState.conf.getAllConfs
    configMatrix(file.configMatrix).foreach { matrix =>
      withFixtureConfigs(file.copy(configs = file.configs ++ matrix)) {
        val selected = matrix.head._2
        assert(
          spark.conf.get(optimizerKey).split(",").toSeq ==
            Seq(selected, nullPropagation, constantFoldingRule).distinct)
      }
      assert(spark.sessionState.conf.getAllConfs == original)
    }
  }

  test("SQL fixture optimizer directives reject ineffective exclusions") {
    Seq(
      "org.apache.spark.sql.catalyst.optimizer.NoSuchRule" -> "Unknown optimizer rule",
      spark.sessionState.optimizer.nonExcludableRules.head -> "Cannot exclude optimizer rule")
      .foreach { case (rule, message) =>
        val file = SqlFileTestParser.parse(Seq(s"-- ExcludeRules: $rule"))
        assert(
          intercept[IllegalArgumentException](withFixtureConfigs(file) {}).getMessage
            .contains(message))
      }
    val conflict = SqlFileTestParser.parse(
      Seq("-- ConstantFolding: enabled", s"-- ExcludeRules: $constantFoldingRule"))
    assert(
      intercept[IllegalArgumentException](withFixtureConfigs(conflict) {}).getMessage
        .contains("conflicts"))
  }

  test("SQL fixture SET and RESET restore existing and previously unset keys") {
    val key = "spark.comet.sqlFixture.isolation"
    val original = spark.sessionState.conf.getAllConfs
    assert(!original.contains(key))
    val file = SqlFileTestParser.parse(Seq("statement", s"SET $key=first"))
    runTestFile("set-isolation", file)
    assert(spark.sessionState.conf.getAllConfs == original)
    withSQLConf(key -> "original") {
      val before = spark.sessionState.conf.getAllConfs
      runTestFile("set-existing", file)
      assert(spark.sessionState.conf.getAllConfs == before)
      runTestFile("reset-existing", SqlFileTestParser.parse(Seq("statement", s"RESET $key")))
      assert(spark.sessionState.conf.getAllConfs == before)
      runTestFile("reset-all", SqlFileTestParser.parse(Seq("statement", "RESET")))
      assert(spark.sessionState.conf.getAllConfs == before)
    }
  }

  test("SQL fixture settings are restored after a failing statement or query") {
    val key = "spark.comet.sqlFixture.isolation"
    Seq("statement", "query").foreach { directive =>
      val original = spark.sessionState.conf.getAllConfs
      val file = SqlFileTestParser.parse(
        Seq(
          "statement",
          s"SET $key=changed",
          "",
          directive,
          "SELECT * FROM missing_sql_fixture_isolation_table"))
      intercept[RuntimeException](runTestFile("failure-isolation", file))
      assert(spark.sessionState.conf.getAllConfs == original)
    }
  }

  test("SQL fixture SET changes take effect between queries") {
    val file = SqlFileTestParser.parse(
      Seq(
        "statement",
        "CREATE TABLE sql_fixture_set(v STRING) USING parquet",
        "",
        "statement",
        "INSERT INTO sql_fixture_set VALUES ('invalid'), ('1')",
        "",
        "query",
        "SELECT CAST(v AS INT) FROM sql_fixture_set",
        "",
        "statement",
        "SET spark.sql.ansi.enabled=true",
        "",
        "query expect_error(CAST_INVALID_INPUT)",
        "SELECT CAST(v AS INT) FROM sql_fixture_set"))
    val original = spark.sessionState.conf.getAllConfs
    runTestFile("set-between-queries", file)
    assert(spark.sessionState.conf.getAllConfs == original)
  }

  test("SQL fixture SET cannot bypass the dispatcher error sentinel") {
    val file = SqlFileTestParser.parse(
      Seq(
        "-- Config: spark.sql.ansi.enabled=true",
        s"-- Config: ${CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key}=false",
        "statement",
        s"SET ${CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key}=true",
        "",
        "query expect_error(CAST_INVALID_INPUT)",
        "SELECT CAST('invalid' AS INT)"))
    val original = spark.sessionState.conf.getAllConfs
    val error = intercept[Exception](runTestFile("set-dispatcher-sentinel", file))
    assert(error.getMessage.contains("missing a non-error"))
    assert(spark.sessionState.conf.getAllConfs == original)
  }

  test("SQL fixture dispatcher sentinel follows SET before an error query") {
    val file = SqlFileTestParser.parse(
      Seq(
        "-- Config: spark.sql.ansi.enabled=true",
        s"-- Config: ${CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key}=true",
        "statement",
        s"SET ${CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key}=false",
        "",
        "query expect_error(CAST_INVALID_INPUT)",
        "SELECT CAST('invalid' AS INT)"))
    val original = spark.sessionState.conf.getAllConfs
    runTestFile("set-disable-dispatcher", file)
    assert(spark.sessionState.conf.getAllConfs == original)
  }

  // Discover and register all .sql test files
  discoverTestFiles(testResourceDir).foreach { file =>
    val relativePath = testResourceDir.toURI.relativize(file.toURI).getPath
    Try(SqlFileTestParser.parse(file)) match {
      case Failure(error) =>
        // A malformed file must not prevent every other fixture from being registered.
        test(s"sql-file: $relativePath") { throw error }
      case Success(parsed) =>
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
                val effectiveConfigs =
                  parsed.configs ++ combinations.headOption.getOrElse(Seq.empty)
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
                  runTestFile(
                    relativePath,
                    parsed.copy(configs = parsed.configs ++ matrixConfigs))
              }
            }
          }
        }
    }
  }
}
