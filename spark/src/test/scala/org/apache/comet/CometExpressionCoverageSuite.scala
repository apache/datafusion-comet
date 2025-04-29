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

import org.scalatest.exceptions.TestFailedException
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{CometTestBase, DataFrame}
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
import org.apache.spark.sql.catalyst.expressions.LambdaFunction
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.collect_list

import org.apache.comet.CoverageResultStatus.{CoverageResultStatus, Passed}

/**
 * Manual test to calculate Spark builtin expressions coverage support by the Comet
 *
 * The test will update files docs/spark_builtin_expr_coverage.txt
 */
//@Ignore
class CometExpressionCoverageSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  import testImplicits._

  private val projectDocFolder = "docs"
  private val rawCoverageFilePath = s"$projectDocFolder/spark_builtin_expr_coverage.txt"
  private val mdCoverageFilePath = s"$projectDocFolder/spark_expressions_support.md"
  private val DATAFUSIONCLI_PATH_ENV_VAR = "DATAFUSIONCLI_PATH"
  private val queryPattern = """(?i)SELECT (.+?);""".r
  private val valuesPattern = """(?i)FROM VALUES(.+?);""".r
  private val selectPattern = """(i?)SELECT(.+?)FROM""".r

  // exclude builtin function which Comet has no plans to support in near future
  // like spark streaming functions, java calls, catalog, etc
  private val outOfRoadmapFuncs =
    List(
      "window",
      "session_window",
      "window_time",
      "java_method",
      "reflect",
      "current_catalog",
      "current_user",
      "current_schema",
      "current_database")
  // Spark Comet configuration to run the tests
  private val sqlConf = Seq(
    "spark.sql.optimizer.excludedRules" -> "org.apache.spark.sql.catalyst.optimizer.ConstantFolding",
    "spark.sql.adaptive.optimizer.excludedRules" -> "org.apache.spark.sql.catalyst.optimizer.ConstantFolding")

  // Tests to run manually as its syntax is different from usual or nested
  // This can be simplified once comet supports MemoryScan, now Comet triggers from the FileScan
  // If MemoryScan supported we can just run Spark examples as is
  val manualTests: Map[String, (String, String)] = Map(
    ("!", ("select true a", "select ! true from tbl")),
    ("%", ("select 1 a, 2 b", "select a % b from tbl")),
    ("&", ("select 1 a, 2 b", "select a & b from tbl")),
    ("*", ("select 1 a, 2 b", "select a * b from tbl")),
    ("+", ("select 1 a, 2 b", "select a + b from tbl")),
    ("-", ("select 1 a, 2 b", "select a - b from tbl")),
    ("/", ("select 1 a, 2 b", "select a / b from tbl")),
    ("<", ("select 1 a, 2 b", "select a < b from tbl")),
    ("<=", ("select 1 a, 2 b", "select a <= b from tbl")),
    ("<=>", ("select 1 a, 2 b", "select a <=> b from tbl")),
    ("=", ("select 1 a, 2 b", "select a = b from tbl")),
    ("==", ("select 1 a, 2 b", "select a == b from tbl")),
    (">", ("select 1 a, 2 b", "select a > b from tbl")),
    (">=", ("select 1 a, 2 b", "select a >= b from tbl")),
    ("^", ("select 1 a, 2 b", "select a ^ b from tbl")),
    ("|", ("select 1 a, 2 b", "select a | b from tbl")),
    ("try_multiply", ("select 2000000 a, 30000000 b", "select try_multiply(a, b) from tbl")),
    ("try_add", ("select 2147483647 a, 1 b", "select try_add(a, b) from tbl")),
    (
      "try_subtract",
      (
        "select cast(-2147483647 as int) a, cast(1 as int) b",
        "select try_subtract(a, b) from tbl")),
    ("stack", ("select 1 a, 2 b", "select stack(1, a, b) from tbl")),
    ("~", ("select 1 a", "select ~ a from tbl")),
    ("unhex", ("select '537061726B2053514C' a", "select unhex(a) from tbl")),
    ("when", ("select 1 a, 2 b, 3 c, 4 d", "select case a > b then c else d end from tbl")),
    ("case", ("select 1 a, 2 b, 3 c, 4 d", "select case a when 1 then c else d end from tbl")),
    (
      "transform_values",
      (
        "select array(1, 2, 3) a",
        "select transform_values(map_from_arrays(a, a), (k, v), v + 1) from tbl")),
    (
      "transform_keys",
      (
        "select array(1, 2, 3) a",
        "select transform_keys(map_from_arrays(a, a), (k, v), v + 1) from tbl")),
    ("transform", ("select array(1, 2, 3) a", "select transform(a, (k, v), v + 1) from tbl")),
    ("reduce", ("select array(1, 2, 3) a", "select reduce(a, 0, (acc, x), acc + x) from tbl")),
    ("struct", ("select 1 a, 2 b", "select struct(a, b) from tbl")),
    ("space", ("select 1 a", "select space(a) from tbl")),
    ("sort_array", ("select array('b', 'd', null, 'c', 'a') a", "select sort_array(a) from tbl")),
    ("or", ("select true a, false b", "select a or b from tbl")),
    ("overlay", ("select 'Spark SQL' a", "select overlay(a PLACING '_' FROM 6) from tbl")),
    ("nvl", ("select 1 a, cast(null as int) b", "select nvl(b, a) from tbl")),
    (
      "nvl2",
      ("select 1 a, cast(null as int) b, cast(null as int) c", "select nvl2(c, b, a) from tbl")),
    (
      "coalesce",
      (
        "select 1 a, cast(null as int) b, cast(null as int) c",
        "select coalesce(c, b, a) from tbl")),
    ("and", ("select true a, false b", "select a and b from tbl")),
    ("not", ("select true a", "select not a from tbl")),
    ("named_struct", ("select 1 a", "select named_struct('a', a) from tbl")),
    ("mod", ("select 1 a, 1 b", "select mod(b, a) from tbl")),
    ("div", ("select 1 a, 1 b", "select div(b, a) from tbl")),
    (
      "map_zip_with",
      (
        "select map(1, 'a', 2, 'b') a, map(1, 'x', 2, 'y') b",
        "SELECT map_zip_with(a, b, (k, v1, v2), concat(v1, v2)) from tbl")),
    (
      "map_filter",
      ("select map(1, 0, 2, 2, 3, -1) a", "SELECT map_filter(a, (k, v), k > v) from tbl")),
    ("in", ("select 1 a", "SELECT a in ('1', '2', '3') from tbl")),
    ("ifnull", ("select 1 a, cast(null as int) b", "SELECT ifnull(b, a) from tbl")),
    (
      "from_json",
      ("select '{\"a\":1, \"b\":0.8}' a", "SELECT from_json(a, 'a INT, b DOUBLE') from tbl")),
    ("from_csv", ("select '1, 0.8' a", "SELECT from_csv(a, 'a INT, b DOUBLE') from tbl")),
    ("forall", ("select array(1, 2, 3) a", "SELECT forall(a, x, x % 2 == 0) from tbl")),
    ("filter", ("select array(1, 2, 3) a", "SELECT filter(a, x, x % 2 == 1) from tbl")),
    ("exists", ("select array(1, 2, 3) a", "SELECT filter(a, x, x % 2 == 0) from tbl")),
    (
      "aggregate",
      ("select array(1, 2, 3) a", "SELECT aggregate(a, 0, (acc, x), acc + x) from tbl")),
    (
      "extract",
      (
        "select TIMESTAMP '2019-08-12 01:00:00.123456' a",
        "SELECT extract(YEAR FROM a) from tbl")),
    (
      "datepart",
      ("select TIMESTAMP '2019-08-12 01:00:00.123456' a", "SELECT datepart('YEAR', a) from tbl")),
    (
      "date_part",
      (
        "select TIMESTAMP '2019-08-12 01:00:00.123456' a",
        "SELECT date_part('YEAR', a) from tbl")),
    ("cast", ("select '10' a", "SELECT cast(a as int) from tbl")),
    (
      "aes_encrypt",
      ("select 'Spark' a, '0000111122223333' b", "SELECT aes_encrypt(a, b) from tbl")))

  // key - function name
  // value - examples
  def getExamples: Map[FunctionInfo, List[String]] =
    spark.sessionState.functionRegistry
      .listFunction()
      .map(spark.sessionState.catalog.lookupFunctionInfo(_))
      .filter(_.getSource.toLowerCase == "built-in")
      .filter(f => !outOfRoadmapFuncs.contains(f.getName.toLowerCase))
      .map(f => {
        val selectRows = queryPattern.findAllMatchIn(f.getExamples).map(_.group(0)).toList
        (FunctionInfo(f.getName, f.getGroup), selectRows.filter(_.nonEmpty))
      })
      .toMap

  /**
   * Manual test to calculate Spark builtin expressions coverage support by the Comet
   *
   * The test updates files doc/spark_builtin_expr_coverage.txt,
   * doc/spark_builtin_expr_coverage_agg.txt
   */
  test("Test Spark builtin expressions coverage") {
    val builtinExamplesMap = getExamples

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

                  testSingleLineQuery(s"select * $v", s"$s tbl", sqlConf = sqlConf)
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

              manualTests.get(func.name) match {
                // the test is manual query
                case Some(test) => testSingleLineQuery(test._1, test._2, sqlConf = sqlConf)
                case None =>
                  // extract function arguments as a sql text
                  // example:
                  // cos(0) -> 0
                  // explode_outer(array(10, 20)) -> array(10, 20)
                  val args = getSqlFunctionArguments(q.dropRight(1))
                  val (aliased, aliases) =
                    if (Seq(
                        "bround",
                        "rlike",
                        "round",
                        "to_binary",
                        "to_char",
                        "to_number",
                        "try_to_binary",
                        "try_to_number",
                        "xpath",
                        "xpath_boolean",
                        "xpath_double",
                        "xpath_double",
                        "xpath_float",
                        "xpath_int",
                        "xpath_long",
                        "xpath_number",
                        "xpath_short",
                        "xpath_string").contains(func.name.toLowerCase)) {
                      // c0 column, c1 foldable literal(cannot be from column)
                      (
                        Seq(s"${args.head} as c0").mkString(","),
                        Seq(s"c0, ${args(1)}").mkString(","))
                    } else {
                      (
                        args.zipWithIndex.map(x => s"${x._1} as c${x._2}").mkString(","),
                        args.zipWithIndex.map(x => s"c${x._2}").mkString(","))
                    }

                  val select = s"select ${func.name}($aliases)"

                  testSingleLineQuery(
                    s"select ${if (aliased.nonEmpty) aliased else 1}",
                    s"$select from tbl",
                    sqlConf = sqlConf)
              }
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

            case e: TestFailedException
                if e.getMessage.contains("Results do not match for query") =>
              CoverageResult(
                q,
                CoverageResultStatus.Failed,
                CoverageResultDetails(
                  cometMessage = "Unsupported: Results do not match for query",
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

            case e: Throwable =>
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
    str shouldBe s"$getLicenseHeader\n# Supported Spark Expressions\n\n### group1\n - [x] f1\n - [ ] f2\n\n### group2\n - [x] f3\n - [ ] f4\n\n### group3\n - [x] f5"
  }

  test("get sql function arguments") {
    getSqlFunctionArguments(
      "SELECT unix_seconds(TIMESTAMP('1970-01-01 00:00:01Z'))") shouldBe Seq(
      "TIMESTAMP('1970-01-01 00:00:01Z')")
    getSqlFunctionArguments("SELECT decode(unhex('537061726B2053514C'), 'UTF-8')") shouldBe Seq(
      "unhex('537061726B2053514C')",
      "'UTF-8'")
    getSqlFunctionArguments(
      "SELECT extract(YEAR FROM TIMESTAMP '2019-08-12 01:00:00.123456')") shouldBe Seq(
      "'YEAR'",
      "TIMESTAMP '2019-08-12 01:00:00.123456'")
    getSqlFunctionArguments("SELECT exists(array(1, 2, 3), x -> x % 2 == 0)") shouldBe Seq(
      "array(1, 2, 3)")
    getSqlFunctionArguments("select to_char(454, '999')") shouldBe Seq("454", "'999'")
  }

  def getSqlFunctionArguments(sql: String): Seq[String] = {
    val plan = spark.sessionState.sqlParser.parsePlan(sql)
    plan match {
      case Project(projectList, _) =>
        // unwrap projection to get first expression arguments
        // assuming first expression is Unresolved function
        val projection = projectList.head.children.head.asInstanceOf[UnresolvedFunction].arguments
        projection.filter(!_.isInstanceOf[LambdaFunction]).map(_.sql)
    }
  }

  def generateMarkdown(df: DataFrame): String = {
    val groupedDF = df
      .orderBy("name")
      .groupBy("details.group")
      .agg(collect_list("name").as("names"), collect_list("details.result").as("statuses"))
      .orderBy("group")
    val sb = new StringBuilder(s"$getLicenseHeader\n# Supported Spark Expressions")
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

  private def getLicenseHeader: String = {
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
