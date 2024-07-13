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

package org.apache.spark.sql

import java.io.File
import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.config.{MEMORY_OFFHEAP_ENABLED, MEMORY_OFFHEAP_SIZE}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.util.{fileToString, resourceToString, stringToFile}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.TestSparkSession

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.isSpark34Plus
import org.apache.comet.shims.ShimCometTPCHQuerySuite

/**
 * End-to-end tests to check TPCH query results.
 *
 * To run this test suite:
 * {{{
 *   SPARK_TPCH_DATA=<path of TPCH SF=1 data>
 *     ./mvnw -Dsuites=org.apache.spark.sql.CometTPCHQuerySuite test
 * }}}
 *
 * To re-generate golden files for this suite, run:
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 SPARK_TPCH_DATA=<path of TPCH SF=1 data>
 *     ./mvnw -Dsuites=org.apache.spark.sql.CometTPCHQuerySuite test
 * }}}
 */
class CometTPCHQuerySuite extends QueryTest with TPCBase with ShimCometTPCHQuerySuite {

  private val tpchDataPath = sys.env.get("SPARK_TPCH_DATA")

  val tpchQueries: Seq[String] = Seq(
    "q1",
    "q2",
    "q3",
    "q4",
    "q5",
    "q6",
    "q7",
    "q8",
    "q9",
    "q10",
    "q11",
    "q12",
    "q13",
    "q14",
    "q15",
    "q16",
    "q17",
    "q18",
    "q19",
    "q20",
    "q21",
    "q22")
  val disabledTpchQueries: Seq[String] = Seq()

  // To make output results deterministic
  def testSparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set(SQLConf.SHUFFLE_PARTITIONS.key, "1")
    conf.set("spark.sql.extensions", "org.apache.comet.CometSparkSessionExtensions")
    conf.set(
      "spark.shuffle.manager",
      "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
    conf.set(CometConf.COMET_ENABLED.key, "true")
    conf.set(CometConf.COMET_EXEC_ENABLED.key, "true")
    conf.set(CometConf.COMET_EXEC_ALL_OPERATOR_ENABLED.key, "true")
    conf.set(CometConf.COMET_EXEC_SHUFFLE_ENABLED.key, "true")
    conf.set(CometConf.COMET_SHUFFLE_MODE.key, "jvm")
    conf.set(MEMORY_OFFHEAP_ENABLED.key, "true")
    conf.set(MEMORY_OFFHEAP_SIZE.key, "2g")
  }

  protected override def createSparkSession: TestSparkSession = {
    new TestSparkSession(new SparkContext("local[1]", this.getClass.getSimpleName, testSparkConf))
  }

  val tableNames: Seq[String] =
    Seq("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier")

  // We use SF=1 table data here, so we cannot use SF=100 stats
  protected override val injectStats: Boolean = false

  if (tpchDataPath.nonEmpty) {
    val nonExistentTables = tableNames.filterNot { tableName =>
      Files.exists(Paths.get(s"${tpchDataPath.get}/$tableName"))
    }
    if (nonExistentTables.nonEmpty) {
      fail(
        s"Non-existent TPCH table paths found in ${tpchDataPath.get}: " +
          nonExistentTables.mkString(", "))
    }
  }

  protected val baseResourcePath: String = {
    // use the same way as `SQLQueryTestSuite` to get the resource path
    getWorkspaceFilePath(
      "spark",
      "src",
      "test",
      "resources",
      "tpch-query-results").toFile.getAbsolutePath
  }

  override def createTables(): Unit = {
    tableNames.foreach { tableName =>
      spark.catalog.createTable(tableName, s"${tpchDataPath.get}/$tableName", "parquet")
    }
  }

  override def dropTables(): Unit = {
    tableNames.foreach { tableName =>
      spark.sessionState.catalog.dropTable(TableIdentifier(tableName), true, true)
    }
  }

  private def runQuery(query: String, goldenFile: File, conf: Map[String, String]): Unit = {
    val shouldSortResults = sortMergeJoinConf != conf // Sort for other joins
    withSQLConf(conf.toSeq: _*) {
      try {
        val (schema, output) = handleExceptions(getNormalizedQueryExecutionResult(spark, query))
        val queryString = query.trim
        val outputString = output.mkString("\n").replaceAll("\\s+$", "")
        if (shouldRegenerateGoldenFiles) {
          val goldenOutput = {
            s"-- Automatically generated by ${getClass.getSimpleName}\n\n" +
              "-- !query schema\n" +
              schema + "\n" +
              "-- !query output\n" +
              outputString +
              "\n"
          }
          val parent = goldenFile.getParentFile
          if (!parent.exists()) {
            assert(parent.mkdirs(), "Could not create directory: " + parent)
          }
          stringToFile(goldenFile, goldenOutput)
        }

        // Read back the golden file.
        val (expectedSchema, expectedOutput) = {
          val goldenOutput = fileToString(goldenFile)
          val segments = goldenOutput.split("-- !query.*\n")

          // query has 3 segments, plus the header
          assert(
            segments.size == 3,
            s"Expected 3 blocks in result file but got ${segments.size}. " +
              "Try regenerate the result files.")

          (segments(1).trim, segments(2).replaceAll("\\s+$", ""))
        }

        // Expose thrown exception when executing the query
        val notMatchedSchemaOutput = if (schema == emptySchema) {
          // There might be exception. See `handleExceptions`.
          s"Schema did not match\n$queryString\nOutput/Exception: $outputString"
        } else {
          s"Schema did not match\n$queryString"
        }

        assertResult(expectedSchema, notMatchedSchemaOutput) {
          schema
        }
        if (shouldSortResults) {
          val expectSorted = expectedOutput
            .split("\n")
            .sorted
            .map(_.trim)
            .mkString("\n")
            .replaceAll("\\s+$", "")
          val outputSorted = output.sorted.map(_.trim).mkString("\n").replaceAll("\\s+$", "")
          assertResult(expectSorted, s"Result did not match\n$queryString") {
            outputSorted
          }
        } else {
          assertResult(expectedOutput, s"Result did not match\n$queryString") {
            outputString
          }
        }
      } catch {
        case e: Throwable =>
          val configs = conf.map { case (k, v) =>
            s"$k=$v"
          }
          throw new Exception(s"${e.getMessage}\nError using configs:\n${configs.mkString("\n")}")
      }
    }
  }

  val sortMergeJoinConf: Map[String, String] = Map(
    SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
    SQLConf.PREFER_SORTMERGEJOIN.key -> "true")

  val broadcastHashJoinConf: Map[String, String] = Map(
    SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10485760")

  val shuffledHashJoinConf: Map[String, String] = Map(
    SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
    "spark.sql.join.forceApplyShuffledHashJoin" -> "true")

  val allJoinConfCombinations: Seq[Map[String, String]] =
    Seq(sortMergeJoinConf, broadcastHashJoinConf, shuffledHashJoinConf)

  val joinConfs: Seq[Map[String, String]] = if (shouldRegenerateGoldenFiles) {
    require(
      !sys.env.contains("SPARK_TPCH_JOIN_CONF"),
      "'SPARK_TPCH_JOIN_CONF' cannot be set together with 'SPARK_GENERATE_GOLDEN_FILES'")
    Seq(sortMergeJoinConf)
  } else {
    sys.env
      .get("SPARK_TPCH_JOIN_CONF")
      .map { s =>
        val p = new java.util.Properties()
        p.load(new java.io.StringReader(s))
        Seq(p.asScala.toMap)
      }
      .getOrElse(allJoinConfCombinations)
  }

  assert(joinConfs.nonEmpty)
  joinConfs.foreach(conf =>
    require(
      allJoinConfCombinations.contains(conf),
      s"Join configurations [$conf] should be one of $allJoinConfCombinations"))

  if (tpchDataPath.nonEmpty) {
    tpchQueries.foreach { name =>
      if (disabledTpchQueries.contains(name)) {
        ignore(s"skipped because $name is disabled") {}
      } else {
        val queryString = resourceToString(
          s"tpch/$name.sql",
          classLoader = Thread.currentThread().getContextClassLoader)
        test(name) {
          // Only run the tests in Spark 3.4+
          assume(isSpark34Plus)

          val goldenFile = new File(s"$baseResourcePath", s"$name.sql.out")
          joinConfs.foreach { conf =>
            System.gc() // Workaround for GitHub Actions memory limitation, see also SPARK-37368
            runQuery(queryString, goldenFile, conf)
          }
        }
      }
    }
  } else {
    ignore("skipped because env `SPARK_TPCH_DATA` is not set") {}
  }

  // TODO: remove once Spark 3.3 is no longer supported
  private def shouldRegenerateGoldenFiles: Boolean =
    System.getenv("SPARK_GENERATE_GOLDEN_FILES") == "1"
}
