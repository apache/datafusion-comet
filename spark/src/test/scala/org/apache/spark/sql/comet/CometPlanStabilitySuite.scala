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
import org.apache.spark.internal.config.{MEMORY_OFFHEAP_ENABLED, MEMORY_OFFHEAP_SIZE}
import org.apache.spark.sql.TPCDSBase
import org.apache.spark.sql.catalyst.expressions.{AttributeSet, Cast}
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.execution.{ReusedSubqueryExec, SparkPlan, SubqueryBroadcastExec, SubqueryExec}
import org.apache.spark.sql.execution.adaptive.DisableAdaptiveExecutionSuite
import org.apache.spark.sql.execution.exchange.{Exchange, ReusedExchangeExec, ValidateRequirements}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.TestSparkSession

import org.apache.comet.{CometConf, ExtendedExplainInfo}
import org.apache.comet.CometSparkSessionExtensions.{isSpark35Plus, isSpark40Plus, isSpark41Plus}

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
  protected val scanImpls: Seq[String] =
    Seq(CometConf.SCAN_NATIVE_ICEBERG_COMPAT, CometConf.SCAN_NATIVE_DATAFUSION)

  protected val baseResourcePath: File = {
    getWorkspaceFilePath("spark", "src", "test", "resources", "tpcds-plan-stability").toFile
  }

  private val referenceRegex = "#\\d+".r
  private val normalizeRegex = "#\\d+L?".r
  private val planIdRegex = "plan_id=\\d+".r

  private val clsName = this.getClass.getCanonicalName

  def goldenFilePath: String

  /**
   * Ordered list of fallback golden file directories. When the primary [[goldenFilePath]] does
   * not contain a directory for a given query, [[getDirForTest]] walks this list in order and
   * returns the first directory that does. This lets each Spark-version-specific plan directory
   * store only the queries whose plans actually changed against the previous version, falling
   * through unchanged queries to the older version's goldens (and ultimately to the base
   * `approved-plans-vX_Y` directory).
   */
  protected def fallbackGoldenFilePaths: Seq[String] = Nil

  private val approvedAnsiPlans: Seq[String] = Seq("q83", "q83.sf100")

  private def getDirForTest(name: String): File = {
    var goldenFileName = if (SQLConf.get.ansiEnabled && approvedAnsiPlans.contains(name)) {
      name + ".ansi"
    } else {
      name
    }
    val nativeImpl = CometConf.COMET_NATIVE_SCAN_IMPL.get()
    goldenFileName = s"$goldenFileName.$nativeImpl"
    val primary = new File(goldenFilePath, goldenFileName)
    if (regenerateGoldenFiles || primary.isDirectory) {
      primary
    } else {
      fallbackGoldenFilePaths.iterator
        .map(p => new File(p, goldenFileName))
        .find(_.isDirectory)
        .getOrElse(primary)
    }
  }

  private def writeGoldenFile(dir: File, filename: String, plan: String): Unit = {
    FileUtils.writeStringToFile(new File(dir, s"$filename.txt"), plan, StandardCharsets.UTF_8)
    logDebug(s"APPROVED: $filename")
  }

  /**
   * After regenerating goldens, drop any query directory under [[goldenFilePath]] whose contents
   * match what [[fallbackGoldenFilePaths]] would resolve to. Mirrors the read-time fallback logic
   * in [[getDirForTest]] so each version-specific directory only retains queries whose plans
   * actually diverge from the previous tier.
   */
  private def pruneDuplicateQueryDirs(): Unit = {
    if (fallbackGoldenFilePaths.isEmpty) return
    val primary = new File(goldenFilePath)
    if (!primary.isDirectory) return
    var removed = 0
    Option(primary.listFiles()).getOrElse(Array.empty).foreach { queryDir =>
      if (queryDir.isDirectory) {
        val effective = fallbackGoldenFilePaths.iterator
          .map(p => new File(p, queryDir.getName))
          .find(_.isDirectory)
        effective.foreach { eff =>
          if (directoriesContentEqual(queryDir, eff)) {
            FileUtils.deleteDirectory(queryDir)
            removed += 1
          }
        }
      }
    }
    if (removed > 0) {
      logInfo(s"Pruned $removed duplicate query director(ies) from ${primary.getName}")
    }
  }

  private def directoriesContentEqual(a: File, b: File): Boolean = {
    val aFiles = Option(a.listFiles()).getOrElse(Array.empty)
    val bFiles = Option(b.listFiles()).getOrElse(Array.empty)
    if (aFiles.length != bFiles.length) return false
    aFiles.forall { fa =>
      val fb = new File(b, fa.getName)
      fa.isFile && fb.isFile && FileUtils.contentEquals(fa, fb)
    }
  }

  override def afterAll(): Unit = {
    try {
      if (regenerateGoldenFiles) {
        pruneDuplicateQueryDirs()
      }
    } finally {
      super.afterAll()
    }
  }

  private def checkWithApproved(dir: File, name: String, filename: String, plan: String): Unit = {
    val tempDir = FileUtils.getTempDirectory
    val approvedFile = new File(dir, s"$filename.txt")
    val actualFile = new File(tempDir, s"$name.actual.$filename.txt")
    FileUtils.writeStringToFile(actualFile, plan, StandardCharsets.UTF_8)
    comparePlans(filename, approvedFile, actualFile)
  }

  private def comparePlans(planType: String, expectedFile: File, actualFile: File): Unit = {
    val expected = FileUtils.readFileToString(expectedFile, StandardCharsets.UTF_8)
    val actual = FileUtils.readFileToString(actualFile, StandardCharsets.UTF_8)
    if (expected != actual) {
      fail(s"""
              |Plans did not match:
              |last approved $planType plan: ${expectedFile.getAbsolutePath}
              |
              |$expected
              |
              |actual $planType plan: ${actualFile.getAbsolutePath}
              |
              |$actual
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
    val queryString = resourceToString(
      s"$tpcdsGroup/$query.sql",
      classLoader = Thread.currentThread().getContextClassLoader)

    withSQLConf(
      CometConf.COMET_EXPLAIN_FALLBACK_ENABLED.key -> "true",
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_EXEC_SORT_MERGE_JOIN_WITH_JOIN_FILTER_ENABLED.key -> "true",
      // as well as for v1.4/q9, v1.4/q44, v2.7.0/q6, v2.7.0/q64
      CometConf.getExprAllowIncompatConfigKey(classOf[Cast]) -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB") {
      val qe = sql(queryString).queryExecution
      val plan = qe.executedPlan
      val extendedExplain = new ExtendedExplainInfo().generateExtendedInfo(qe.executedPlan)
      val extended = normalizeLocation(normalizeIds(extendedExplain))
      assert(ValidateRequirements.validate(plan))

      val name = query + suffix
      val dir = getDirForTest(name)

      if (regenerateGoldenFiles) {
        FileUtils.deleteDirectory(dir)
        if (!dir.mkdirs()) {
          fail(s"Could not create dir: $dir")
        }
        writeGoldenFile(dir, "extended", extended)
      } else {
        checkWithApproved(dir, name, "extended", extended)
      }
    }
  }

  protected override def createSparkSession: TestSparkSession = {
    val conf = super.sparkConf
    conf.set("spark.sql.extensions", "org.apache.comet.CometSparkSessionExtensions")
    conf.set(
      "spark.shuffle.manager",
      "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
    conf.set(MEMORY_OFFHEAP_ENABLED.key, "true")
    conf.set(MEMORY_OFFHEAP_SIZE.key, "2g")
    conf.set(CometConf.COMET_ENABLED.key, "true")
    conf.set(CometConf.COMET_EXEC_ENABLED.key, "true")
    conf.set(CometConf.COMET_ONHEAP_MEMORY_OVERHEAD.key, "1g")
    conf.set(CometConf.COMET_EXEC_SHUFFLE_ENABLED.key, "true")

    new TestSparkSession(new SparkContext("local[1]", this.getClass.getCanonicalName, conf))
  }
}

/**
 * Returns the chain of plan-directory names for the active Spark version, ordered from
 * most-specific to base. The first entry is the directory written to when goldens are
 * regenerated; subsequent entries are searched in order for queries whose plans did not change
 * against the previous version.
 */
private object CometPlanStabilitySuite {
  def planNameChain(variant: String): Seq[String] = {
    Seq(
      isSpark41Plus -> s"approved-plans-${variant}-spark4_1",
      isSpark40Plus -> s"approved-plans-${variant}-spark4_0",
      isSpark35Plus -> s"approved-plans-${variant}-spark3_5")
      .dropWhile(!_._1)
      .map(_._2) :+ s"approved-plans-${variant}"
  }
}

class CometTPCDSV1_4_PlanStabilitySuite extends CometPlanStabilitySuite {
  private val planNames: Seq[String] = CometPlanStabilitySuite.planNameChain("v1_4")
  override val goldenFilePath: String =
    new File(baseResourcePath, planNames.head).getAbsolutePath
  override protected val fallbackGoldenFilePaths: Seq[String] =
    planNames.tail.map(new File(baseResourcePath, _).getAbsolutePath)

  scanImpls.foreach { scan =>
    tpcdsQueries.foreach { q =>
      test(s"check simplified (tpcds-v1.4/$q) - $scan") {
        withSQLConf(CometConf.COMET_NATIVE_SCAN_IMPL.key -> scan) {
          testQuery("tpcds", q)
        }
      }
    }
  }
}

class CometTPCDSV2_7_PlanStabilitySuite extends CometPlanStabilitySuite {
  private val planNames: Seq[String] = CometPlanStabilitySuite.planNameChain("v2_7")
  override val goldenFilePath: String =
    new File(baseResourcePath, planNames.head).getAbsolutePath
  override protected val fallbackGoldenFilePaths: Seq[String] =
    planNames.tail.map(new File(baseResourcePath, _).getAbsolutePath)

  scanImpls.foreach { scan =>
    tpcdsQueriesV2_7_0.foreach { q =>
      test(s"check simplified (tpcds-v2.7.0/$q) - $scan") {
        withSQLConf(CometConf.COMET_NATIVE_SCAN_IMPL.key -> scan) {
          testQuery("tpcds-v2.7.0", q)
        }
      }
    }
  }
}
