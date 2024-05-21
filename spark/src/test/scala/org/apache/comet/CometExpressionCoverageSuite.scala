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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import scala.collection.mutable
import scala.sys.process._

import org.scalatest.Ignore
import org.scalatest.exceptions.TestFailedException
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{CometTestBase, DataFrame}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.collect_list

import org.apache.comet.CoverageResultStatus.{CoverageResultStatus, Passed}

/**
 * Manual test to calculate Spark builtin expressions coverage support by the Comet
 *
 * The test will update files docs/spark_builtin_expr_coverage.txt
 */
@Ignore
class CometExpressionCoverageSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  import testImplicits._

  private val projectDocFolder = "docs"
  private val rawCoverageFilePath = s"$projectDocFolder/spark_builtin_expr_coverage.txt"
  private val mdCoverageFilePath = s"$projectDocFolder/spark_expressions_support.md"
  private val DATAFUSIONCLI_PATH_ENV_VAR = "DATAFUSIONCLI_PATH"
  private val queryPattern = """(?i)SELECT (.+?);""".r
  private val valuesPattern = """(?i)FROM VALUES(.+?);""".r
  private val selectPattern = """(i?)SELECT(.+?)FROM""".r

  // key - function name
  // value - examples
  def getExamples(): Map[FunctionInfo, List[String]] =
    spark.sessionState.functionRegistry
      .listFunction()
      .map(spark.sessionState.catalog.lookupFunctionInfo(_))
      .filter(_.getSource.toLowerCase == "built-in")
      // exclude spark streaming functions, Comet has no plans to support streaming in near future
      .filter(f =>
        !List("window", "session_window", "window_time").contains(f.getName.toLowerCase))
      .map(f => {
        val selectRows = queryPattern.findAllMatchIn(f.getExamples).map(_.group(0)).toList
        (FunctionInfo(f.getName, f.getGroup), selectRows.filter(_.nonEmpty))
      })
      .toMap

  /**
   * Manual test to calculate Spark builtin expressions coverage support by the Comet
   *
   * The test will update files doc/spark_builtin_expr_coverage.txt,
   * doc/spark_builtin_expr_coverage_agg.txt
   */
  test("Test Spark builtin expressions coverage") {
    val builtinExamplesMap = getExamples()

    // key - function name
    // value - list of result shows if function supported by Comet
    val resultsMap = new mutable.HashMap[String, CoverageResult]()

    builtinExamplesMap.foreach {
      case (func, q :: _) =>
        var dfMessage: Option[String] = None
        val queryResult =
          try {
            // Example with predefined values
            // e.g. SELECT bit_xor(col) FROM VALUES (3), (5) AS tab(col)
            // better option is probably to parse the query and iterate through expressions
            // but this is adhoc coverage test
            if (q.toLowerCase.contains(" from values")) {
              val select = selectPattern.findFirstMatchIn(q).map(_.group(0))
              val values = valuesPattern.findFirstMatchIn(q).map(_.group(0))
              (select, values) match {
                case (Some(s), Some(v)) =>
                  withTempDir { dir =>
                    val path = new Path(dir.toURI.toString).toUri.getPath
                    spark.sql(s"select * $v").repartition(1).write.mode("overwrite").parquet(path)
                    dfMessage = runDatafusionCli(s"""$s '$path/*.parquet'""")
                  }

                  testSingleLineQuery(s"select * $v", s"$s tbl")
                case _ =>
                  sys.error("Cannot parse properly")
              }
            } else {
              // Process the simple example like `SELECT cos(0);`
              //
              // The execution disables constant folding. This optimization rule precomputes and selects the value as literal
              // which subsequently leads to false positives
              //
              // ConstantFolding is a operator optimization rule in Catalyst that replaces expressions
              // that can be statically evaluated with their equivalent literal values.
              dfMessage = runDatafusionCli(q)
              testSingleLineQuery(
                "select 'dummy' x",
                s"${q.dropRight(1)}, x from tbl",
                excludedOptimizerRules =
                  Some("org.apache.spark.sql.catalyst.optimizer.ConstantFolding"))
            }
            CoverageResult(
              q,
              CoverageResultStatus.Passed,
              CoverageResultDetails(
                cometMessage = "OK",
                datafusionMessage = dfMessage.getOrElse("OK")),
              group = func.group)

          } catch {
            case e: TestFailedException
                if e.getMessage.contains("Expected only Comet native operators") =>
              CoverageResult(
                q,
                CoverageResultStatus.Failed,
                CoverageResultDetails(
                  cometMessage =
                    "Unsupported: Expected only Comet native operators but found Spark fallback",
                  datafusionMessage = dfMessage.getOrElse("")),
                group = func.group)

            case e if e.getMessage.contains("CometNativeException") =>
              CoverageResult(
                q,
                CoverageResultStatus.Failed,
                CoverageResultDetails(
                  cometMessage = "Failed on native side: found CometNativeException",
                  datafusionMessage = dfMessage.getOrElse("")),
                group = func.group)

            case e =>
              CoverageResult(
                q,
                CoverageResultStatus.Failed,
                CoverageResultDetails(
                  cometMessage = e.getMessage,
                  datafusionMessage = dfMessage.getOrElse("")),
                group = func.group)
          }
        resultsMap.put(func.name, queryResult)

      // Function with no examples
      case (func, List()) =>
        resultsMap.put(
          func.name,
          CoverageResult(
            "",
            CoverageResultStatus.Skipped,
            CoverageResultDetails(
              cometMessage = "No examples found in spark.sessionState.functionRegistry",
              datafusionMessage = ""),
            group = func.group))
    }

    resultsMap.toSeq.toDF("name", "details").createOrReplaceTempView("t")

    val str = showString(
      spark.sql(
        "select name, details.query, details.result, details.details.cometMessage, details.details.datafusionMessage from t order by 1"),
      1000,
      0)
    Files.write(Paths.get(rawCoverageFilePath), str.getBytes(StandardCharsets.UTF_8))
    Files.write(
      Paths.get(mdCoverageFilePath),
      generateMarkdown(spark.sql("select * from t")).getBytes(StandardCharsets.UTF_8))
  }

  test("Test markdown") {
    val map = new scala.collection.mutable.HashMap[String, CoverageResult]()
    map.put(
      "f1",
      CoverageResult("q1", CoverageResultStatus.Passed, CoverageResultDetails("", ""), "group1"))
    map.put(
      "f2",
      CoverageResult(
        "q2",
        CoverageResultStatus.Failed,
        CoverageResultDetails("err", "err"),
        "group1"))
    map.put(
      "f3",
      CoverageResult("q3", CoverageResultStatus.Passed, CoverageResultDetails("", ""), "group2"))
    map.put(
      "f4",
      CoverageResult(
        "q4",
        CoverageResultStatus.Failed,
        CoverageResultDetails("err", "err"),
        "group2"))
    map.put(
      "f5",
      CoverageResult("q5", CoverageResultStatus.Passed, CoverageResultDetails("", ""), "group3"))
    val str = generateMarkdown(map.toSeq.toDF("name", "details"))
    str shouldBe s"${getLicenseHeader()}\n# Supported Spark Expressions\n\n### group1\n - [x] f1\n - [ ] f2\n\n### group2\n - [x] f3\n - [ ] f4\n\n### group3\n - [x] f5"
  }

  def generateMarkdown(df: DataFrame): String = {
    val groupedDF = df
      .orderBy("name")
      .groupBy("details.group")
      .agg(collect_list("name").as("names"), collect_list("details.result").as("statuses"))
      .orderBy("group")
    val sb = new StringBuilder(s"${getLicenseHeader()}\n# Supported Spark Expressions")
    groupedDF.collect().foreach { row =>
      val groupName = row.getAs[String]("group")
      val names = row.getAs[Seq[String]]("names")
      val statuses = row.getAs[Seq[String]]("statuses")

      val passedMarks = names
        .zip(statuses)
        .map(x =>
          x._2 match {
            case s if s == Passed.toString => s" - [x] ${x._1}"
            case _ => s" - [ ] ${x._1}"
          })

      sb.append(s"\n\n### $groupName\n" + passedMarks.mkString("\n"))
    }

    sb.result()
  }

  private def getLicenseHeader(): String = {
    """<!---
      |  Licensed to the Apache Software Foundation (ASF) under one
      |  or more contributor license agreements.  See the NOTICE file
      |  distributed with this work for additional information
      |  regarding copyright ownership.  The ASF licenses this file
      |  to you under the Apache License, Version 2.0 (the
      |  "License"); you may not use this file except in compliance
      |  with the License.  You may obtain a copy of the License at
      |
      |    http://www.apache.org/licenses/LICENSE-2.0
      |
      |  Unless required by applicable law or agreed to in writing,
      |  software distributed under the License is distributed on an
      |  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
      |  KIND, either express or implied.  See the License for the
      |  specific language governing permissions and limitations
      |  under the License.
      |-->
      |""".stripMargin
  }

  // Returns execution error, None means successful execution
  private def runDatafusionCli(sql: String): Option[String] = {

    val datafusionCliPath = sys.env.getOrElse(
      DATAFUSIONCLI_PATH_ENV_VAR,
      return Some(s"$DATAFUSIONCLI_PATH_ENV_VAR env variable not set"))

    val tempFilePath = Files.createTempFile("temp-", ".sql")
    val stdout = new StringBuilder
    val stderr = new StringBuilder
    try {
      Files.write(tempFilePath, sql.getBytes)

      val command = s"""$datafusionCliPath/datafusion-cli -f $tempFilePath"""
      command.!(
        ProcessLogger(
          out => stdout.append(out).append("\n"), // stdout
          err => stderr.append(err).append("\n") // stderr
        ))
    } finally {
      Files.delete(tempFilePath)
    }

    val err = stderr.toString
    val out = stdout.toString

    if (err.nonEmpty)
      return Some(s"std_err: $err")

    if (out.toLowerCase.contains("error"))
      return Some(s"std_out: $out")

    None
  }
}

object CoverageResultStatus extends Enumeration {
  type CoverageResultStatus = Value

  val Failed: Value = Value("FAILED")
  val Passed: Value = Value("PASSED")
  val Skipped: Value = Value("SKIPPED")
}

case class CoverageResult(
    query: String,
    result: CoverageResultStatus,
    details: CoverageResultDetails,
    group: String)

case class CoverageResultDetails(cometMessage: String, datafusionMessage: String)

case class FunctionInfo(name: String, group: String)
