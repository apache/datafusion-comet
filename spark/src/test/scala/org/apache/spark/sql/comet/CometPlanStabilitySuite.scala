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

package org.apache.spark.sql.comet

import java.io.File
import java.nio.charset.StandardCharsets

import scala.collection.mutable

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.TPCDSBase
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.execution.{FormattedMode, ReusedSubqueryExec, SparkPlan, SubqueryBroadcastExec, SubqueryExec}
import org.apache.spark.sql.execution.adaptive.DisableAdaptiveExecutionSuite
import org.apache.spark.sql.execution.exchange.{Exchange, ReusedExchangeExec, ValidateRequirements}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.TestSparkSession

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.{isSpark34Plus, isSpark35Plus, isSpark40Plus}

/**
 * Similar to [[org.apache.spark.sql.PlanStabilitySuite]], checks that TPC-DS Comet plans don't
 * change.
 *
 * If there are plan differences, the error message looks like this: Plans did not match: last
 * approved simplified plan: /path/to/tpcds-plan-stability/approved-plans-xxx/q1/simplified.txt
 * last approved explain plan: /path/to/tpcds-plan-stability/approved-plans-xxx/q1/explain.txt
 * [last approved simplified plan]
 *
 * actual simplified plan: /path/to/tmp/q1.actual.simplified.txt actual explain plan:
 * /path/to/tmp/q1.actual.explain.txt [actual simplified plan]
 *
 * To run the entire test suite, for instance `CometTPCDSV2_7_PlanStabilitySuite`:
 * {{{
 *   mvn -pl spark -Dsuites="org.apache.spark.sql.comet.CometTPCDSV2_7_PlanStabilitySuite" test
 * }}}
 *
 * To re-generate golden files for entire suite, run:
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 mvn -pl spark -Dsuites="org.apache.spark.sql.comet.CometTPCDSV2_7_PlanStabilitySuite" test
 * }}}
 */
trait CometPlanStabilitySuite extends DisableAdaptiveExecutionSuite with TPCDSBase {
  protected val baseResourcePath: File = {
    getWorkspaceFilePath("spark", "src", "test", "resources", "tpcds-plan-stability").toFile
  }

  private val referenceRegex = "#\\d+".r
  private val normalizeRegex = "#\\d+L?".r
  private val planIdRegex = "plan_id=\\d+".r

  private val clsName = this.getClass.getCanonicalName

  def goldenFilePath: String

  private val approvedAnsiPlans: Seq[String] = Seq("q83", "q83.sf100")

  private def getDirForTest(name: String): File = {
    val goldenFileName = if (SQLConf.get.ansiEnabled && approvedAnsiPlans.contains(name)) {
      name + ".ansi"
    } else {
      name
    }
    new File(goldenFilePath, goldenFileName)
  }

  private def isApproved(
      dir: File,
      actualSimplifiedPlan: String,
      actualExplain: String): Boolean = {
    val simplifiedFile = new File(dir, "simplified.txt")
    val expectedSimplified = FileUtils.readFileToString(simplifiedFile, StandardCharsets.UTF_8)
    lazy val explainFile = new File(dir, "explain.txt")
    lazy val expectedExplain = FileUtils.readFileToString(explainFile, StandardCharsets.UTF_8)
    expectedSimplified == actualSimplifiedPlan && expectedExplain == actualExplain
  }

  /**
   * Serialize and save this SparkPlan. The resulting file is used by [[checkWithApproved]] to
   * check stability.
   *
   * @param plan
   *   the SparkPlan
   * @param name
   *   the name of the query
   * @param explain
   *   the full explain output; this is saved to help debug later as the simplified plan is not
   *   too useful for debugging
   */
  private def generateGoldenFile(plan: SparkPlan, name: String, explain: String): Unit = {
    val dir = getDirForTest(name)
    val simplified = getSimplifiedPlan(plan)
    val foundMatch = dir.exists() && isApproved(dir, simplified, explain)

    if (!foundMatch) {
      FileUtils.deleteDirectory(dir)
      if (!dir.mkdirs()) {
        fail(s"Could not create dir: $dir")
      }

      val file = new File(dir, "simplified.txt")
      FileUtils.writeStringToFile(file, simplified, StandardCharsets.UTF_8)
      val fileOriginalPlan = new File(dir, "explain.txt")
      FileUtils.writeStringToFile(fileOriginalPlan, explain, StandardCharsets.UTF_8)
      logDebug(s"APPROVED: $file $fileOriginalPlan")
    }
  }

  private def checkWithApproved(plan: SparkPlan, name: String, explain: String): Unit = {
    val dir = getDirForTest(name)
    val tempDir = FileUtils.getTempDirectory
    val actualSimplified = getSimplifiedPlan(plan)
    val foundMatch = isApproved(dir, actualSimplified, explain)

    if (!foundMatch) {
      // show diff with last approved
      val approvedSimplifiedFile = new File(dir, "simplified.txt")
      val approvedExplainFile = new File(dir, "explain.txt")

      val actualSimplifiedFile = new File(tempDir, s"$name.actual.simplified.txt")
      val actualExplainFile = new File(tempDir, s"$name.actual.explain.txt")

      val approvedSimplified =
        FileUtils.readFileToString(approvedSimplifiedFile, StandardCharsets.UTF_8)
      // write out for debugging
      FileUtils.writeStringToFile(actualSimplifiedFile, actualSimplified, StandardCharsets.UTF_8)
      FileUtils.writeStringToFile(actualExplainFile, explain, StandardCharsets.UTF_8)

      fail(s"""
           |Plans did not match:
           |last approved simplified plan: ${approvedSimplifiedFile.getAbsolutePath}
           |last approved explain plan: ${approvedExplainFile.getAbsolutePath}
           |
           |$approvedSimplified
           |
           |actual simplified plan: ${actualSimplifiedFile.getAbsolutePath}
           |actual explain plan: ${actualExplainFile.getAbsolutePath}
           |
           |$actualSimplified
        """.stripMargin)
    }
  }

  /**
   * Get the simplified plan for a specific SparkPlan. In the simplified plan, the node only has
   * its name and all the sorted reference and produced attributes names(without ExprId) and its
   * simplified children as well. And we'll only identify the performance sensitive nodes, e.g.,
   * Exchange, Subquery, in the simplified plan. Given such a identical but simplified plan, we'd
   * expect to avoid frequent plan changing and catch the possible meaningful regression.
   */
  private def getSimplifiedPlan(plan: SparkPlan): String = {
    val exchangeIdMap = new mutable.HashMap[Int, Int]()
    val subqueriesMap = new mutable.HashMap[Int, Int]()

    def getId(plan: SparkPlan): Int = plan match {
      case exchange: Exchange =>
        exchangeIdMap.getOrElseUpdate(exchange.id, exchangeIdMap.size + 1)
      case ReusedExchangeExec(_, exchange) =>
        exchangeIdMap.getOrElseUpdate(exchange.id, exchangeIdMap.size + 1)
      case subquery: SubqueryExec =>
        subqueriesMap.getOrElseUpdate(subquery.id, subqueriesMap.size + 1)
      case subquery: SubqueryBroadcastExec =>
        subqueriesMap.getOrElseUpdate(subquery.id, subqueriesMap.size + 1)
      case ReusedSubqueryExec(subquery) =>
        subqueriesMap.getOrElseUpdate(subquery.id, subqueriesMap.size + 1)
      case _ => -1
    }

    /**
     * Some expression names have ExprId in them due to using things such as
     * "sum(sr_return_amt#14)", so we remove all of these using regex
     */
    def cleanUpReferences(references: AttributeSet): String = {
      referenceRegex.replaceAllIn(references.map(_.name).mkString(","), "")
    }

    /**
     * Generate a simplified plan as a string Example output: TakeOrderedAndProject
     * [c_customer_id] WholeStageCodegen Project [c_customer_id]
     */
    def simplifyNode(node: SparkPlan, depth: Int): String = {
      val padding = "  " * depth
      var thisNode = node.nodeName
      if (node.references.nonEmpty) {
        thisNode += s" [${cleanUpReferences(node.references)}]"
      }
      if (node.producedAttributes.nonEmpty) {
        thisNode += s" [${cleanUpReferences(node.producedAttributes)}]"
      }
      val id = getId(node)
      if (id > 0) {
        thisNode += s" #$id"
      }
      val childrenSimplified = node.children.map(simplifyNode(_, depth + 1))
      val subqueriesSimplified = node.subqueries.map(simplifyNode(_, depth + 1))
      s"$padding$thisNode\n${subqueriesSimplified.mkString("")}${childrenSimplified.mkString("")}"
    }

    simplifyNode(plan, 0)
  }

  private def normalizeIds(plan: String): String = {
    val map = new mutable.HashMap[String, String]()
    normalizeRegex
      .findAllMatchIn(plan)
      .map(_.toString)
      .foreach(map.getOrElseUpdate(_, (map.size + 1).toString))
    val exprIdNormalized =
      normalizeRegex.replaceAllIn(plan, regexMatch => s"#${map(regexMatch.toString)}")

    // Normalize the plan id in Exchange nodes. See `Exchange.stringArgs`.
    val planIdMap = new mutable.HashMap[String, String]()
    planIdRegex
      .findAllMatchIn(exprIdNormalized)
      .map(_.toString)
      .foreach(planIdMap.getOrElseUpdate(_, (planIdMap.size + 1).toString))
    planIdRegex.replaceAllIn(
      exprIdNormalized,
      regexMatch => s"plan_id=${planIdMap(regexMatch.toString)}")
  }

  private def normalizeLocation(plan: String): String = {
    plan.replaceAll(
      s"Location.*$clsName/",
      "Location [not included in comparison]/{warehouse_dir}/")
  }

  /**
   * Test a TPC-DS query. Depending on the settings this test will either check if the plan
   * matches a golden file or it will create a new golden file.
   */
  protected def testQuery(tpcdsGroup: String, query: String, suffix: String = ""): Unit = {
    // Only run the tests in Spark 3.4+
    assume(isSpark34Plus)

    val queryString = resourceToString(
      s"$tpcdsGroup/$query.sql",
      classLoader = Thread.currentThread().getContextClassLoader)
    // Disable char/varchar read-side handling for better performance.
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_DPP_FALLBACK_ENABLED.key -> "false",
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_CAST_ALLOW_INCOMPATIBLE.key -> "true", // needed for v1.4/q9, v1.4/q44, v2.7.0/q6, v2.7.0/q64
      "spark.sql.readSideCharPadding" -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB") {
      val qe = sql(queryString).queryExecution
      val plan = qe.executedPlan
      val explain = normalizeLocation(normalizeIds(qe.explainString(FormattedMode)))

      assert(ValidateRequirements.validate(plan))

      if (shouldRegenerateGoldenFiles) {
        generateGoldenFile(plan, query + suffix, explain)
      } else {
        checkWithApproved(plan, query + suffix, explain)
      }
    }
  }

  protected override def createSparkSession: TestSparkSession = {
    val conf = super.sparkConf
    conf.set("spark.sql.extensions", "org.apache.comet.CometSparkSessionExtensions")
    conf.set(
      "spark.shuffle.manager",
      "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
    conf.set(CometConf.COMET_ENABLED.key, "true")
    conf.set(CometConf.COMET_EXEC_ENABLED.key, "true")
    conf.set(CometConf.COMET_MEMORY_OVERHEAD.key, "1g")
    conf.set(CometConf.COMET_EXEC_SHUFFLE_ENABLED.key, "true")

    new TestSparkSession(new SparkContext("local[1]", this.getClass.getCanonicalName, conf))
  }

  // TODO: remove once Spark 3.3 is no longer supported
  private val shouldRegenerateGoldenFiles: Boolean =
    System.getenv("SPARK_GENERATE_GOLDEN_FILES") == "1"
}

class CometTPCDSV1_4_PlanStabilitySuite extends CometPlanStabilitySuite {
  private val planName = if (isSpark40Plus) {
    "approved-plans-v1_4-spark4_0"
  } else if (isSpark35Plus) {
    "approved-plans-v1_4-spark3_5"
  } else {
    "approved-plans-v1_4"
  }
  override val goldenFilePath: String =
    new File(baseResourcePath, planName).getAbsolutePath

  tpcdsQueries.foreach { q =>
    test(s"check simplified (tpcds-v1.4/$q)") {
      testQuery("tpcds", q)
    }
  }
}

class CometTPCDSV2_7_PlanStabilitySuite extends CometPlanStabilitySuite {
  private val planName = if (isSpark40Plus) {
    "approved-plans-v2_7-spark4_0"
  } else if (isSpark35Plus) {
    "approved-plans-v2_7-spark3_5"
  } else {
    "approved-plans-v2_7"
  }
  override val goldenFilePath: String =
    new File(baseResourcePath, planName).getAbsolutePath

  tpcdsQueriesV2_7_0.foreach { q =>
    test(s"check simplified (tpcds-v2.7.0/$q)") {
      testQuery("tpcds-v2.7.0", q)
    }
  }
}
