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
import org.scalatest.exceptions.TestFailedException
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.scalatest.Ignore

/**
 *  Manual test to calculate Spark builtin functions coverage support by the Comet
 *
 *  The test will update files doc/spark_coverage.txt, doc/spark_coverage_agg.txt
 */

@Ignore
class CometExpressionCoverageSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  import testImplicits._

  test("Test Spark builtin functions coverage") {
    val queryPattern = """(?i)SELECT (.+?);""".r
    val valuesPattern = """(?i)FROM VALUES(.+?);""".r
    val selectPattern = """(i?)SELECT(.+?)FROM""".r
    val builtinExamplesMap = spark.sessionState.functionRegistry
      .listFunction()
      .map(spark.sessionState.catalog.lookupFunctionInfo(_))
      .filter(_.getSource.toLowerCase == "built-in")
      .filter(f =>
        !List("window").contains(f.getName.toLowerCase)) // exclude exotics, will run it manually
      .map(f => {
        val selectRows = queryPattern.findAllMatchIn(f.getExamples).map(_.group(0)).toList
        (f.getName, selectRows.filter(_.nonEmpty))
      })
      .toMap

    // key - function name
    // value - list of result shows if function supported by Comet
    val resultsMap = new mutable.HashMap[String, CoverageResult]()

    builtinExamplesMap.foreach {
      case (funcName, q :: _) =>
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
                  testSingleLineQuery(s"select * $v", s"$s tbl")

                case _ => sys.error(s"Query $q cannot be parsed properly")
              }
            } else {
              // Plain example like SELECT cos(0);
              testSingleLineQuery("select 'dummy' x", s"${q.dropRight(1)}, x from tbl")
            }
            CoverageResult("PASSED", Seq((q, "OK")))
          } catch {
            case e: TestFailedException
                if e.message.getOrElse("").contains("Expected only Comet native operators") =>
              CoverageResult("FAILED", Seq((q, "Unsupported")))
            case e if e.getMessage.contains("CometNativeException") =>
              CoverageResult("FAILED", Seq((q, "Failed on native side")))
            case _ =>
              CoverageResult("FAILED", Seq((q, "Failed on something else. Check query manually")))
          }
        resultsMap.put(funcName, queryResult)
      case (funcName, List()) =>
        resultsMap.put(funcName, CoverageResult("SKIPPED", Seq.empty))
    }

    // later we Convert resultMap into some HTML
    resultsMap.toSeq.toDF("name", "details").createOrReplaceTempView("t")
    val str_agg = showString(
      spark.sql(
        "select result, d._2 as reason, count(1) cnt from (select name, t.details.result, explode_outer(t.details.details) as d from t) group by 1, 2"),
      500,
      0)
    Files.write(Paths.get("doc/spark_coverage_agg.txt"), str_agg.getBytes(StandardCharsets.UTF_8))

    val str = showString(spark.sql("select * from t order by 1"), 500, 0)
    Files.write(Paths.get("doc/spark_coverage.txt"), str.getBytes(StandardCharsets.UTF_8))
  }
}

case class CoverageResult(result: String, details: Seq[(String, String)])
