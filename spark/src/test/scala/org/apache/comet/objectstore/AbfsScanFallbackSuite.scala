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

package org.apache.comet.objectstore

import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{CometTestBase, DataFrame}
import org.apache.spark.sql.comet.CometNativeScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StructType}

import org.apache.comet.{CometConf, ExtendedExplainInfo}
import org.apache.comet.hadoop.fs.FakeAbfsFileSystem

/**
 * Plans scans over `abfss://` paths served by [[FakeAbfsFileSystem]], with authentication
 * resolved offline by the hadoop-azure on the test classpath. A scan Comet declines runs on
 * Spark's own scan against the local files.
 */
class AbfsScanFallbackSuite extends CometTestBase {

  private val multipleAuthoritiesReason = "more than one container or account"

  private var rootDir: File = _

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set("spark.hadoop.fs.abfss.impl", classOf[FakeAbfsFileSystem].getName)
      .set("spark.hadoop.fs.abfss.impl.disable.cache", "true")

  override def beforeAll(): Unit = {
    // java.io.tmpdir is under target/, which a fresh checkout may not have yet.
    val tmp = Files.createDirectories(Paths.get(System.getProperty("java.io.tmpdir")))
    rootDir = Files.createTempDirectory(tmp, "comet-abfs-scan").toFile
    super.beforeAll()
  }

  protected override def afterAll(): Unit = {
    if (rootDir != null) FileUtils.deleteDirectory(rootDir)
    super.afterAll()
  }

  private def localDir(name: String): String = new File(rootDir, name).getAbsolutePath

  private def abfss(container: String, account: String, dir: String): String =
    s"abfss://$container@$account.dfs.core.windows.net${localDir(dir)}"

  private def accountKeys(keys: (String, String)*): Seq[(String, String)] =
    keys.map { case (account, key) =>
      s"fs.azure.account.key.$account.dfs.core.windows.net" -> key
    }

  private def writeParquet(dir: String, from: Int): Unit =
    spark.range(from.toLong, from + 10L).toDF("id").write.parquet(localDir(dir))

  private def writeCsv(dir: String, from: Int): Unit =
    spark
      .range(from.toLong, from + 10L)
      .selectExpr("cast(id as int) as a", "cast(id * 2 as int) as b")
      .write
      .csv(localDir(dir))

  private val csvConfs =
    Seq(CometConf.COMET_CSV_V2_NATIVE_ENABLED.key -> "true", SQLConf.USE_V1_SOURCE_LIST.key -> "")

  private def readCsv(paths: String*): DataFrame =
    spark.read
      .schema(new StructType().add("a", IntegerType).add("b", IntegerType))
      .csv(paths: _*)

  test("native Parquet scan over two accounts with different keys falls back to Spark") {
    writeParquet("parquet-a", 0)
    writeParquet("parquet-b", 100)
    withSQLConf(accountKeys("accta" -> "a2V5LWE=", "acctb" -> "a2V5LWI="): _*) {
      val df =
        spark.read.parquet(
          abfss("data", "accta", "parquet-a"),
          abfss("data", "acctb", "parquet-b"))
      checkSparkAnswerAndFallbackReason(df, multipleAuthoritiesReason)
    }
  }

  test("native Parquet scan over two containers of one account falls back to Spark") {
    writeParquet("parquet-c", 0)
    writeParquet("parquet-d", 100)
    withSQLConf(accountKeys("accta" -> "a2V5LWE="): _*) {
      val df =
        spark.read.parquet(
          abfss("data", "accta", "parquet-c"),
          abfss("logs", "accta", "parquet-d"))
      checkSparkAnswerAndFallbackReason(df, multipleAuthoritiesReason)
    }
  }

  test("native Parquet scan over two directories of one container stays native") {
    writeParquet("parquet-e", 0)
    writeParquet("parquet-f", 100)
    val confs = (SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") +:
      accountKeys("accta" -> "a2V5LWE=")
    withSQLConf(confs: _*) {
      // Plan only: running the native scan would send its requests to Azure.
      val plan = spark.read
        .parquet(abfss("data", "accta", "parquet-e"), abfss("data", "accta", "parquet-f"))
        .queryExecution
        .executedPlan
      assert(plan.collect { case scan: CometNativeScanExec => scan }.size == 1, plan)
      val reasons = new ExtendedExplainInfo().getFallbackReasons(plan)
      assert(!reasons.exists(_.contains("ABFS")), reasons)
    }
  }

  test("native CSV scan over two accounts with different keys falls back to Spark") {
    writeCsv("csv-a", 0)
    writeCsv("csv-b", 100)
    withSQLConf(csvConfs ++ accountKeys("accta" -> "a2V5LWE=", "acctb" -> "a2V5LWI="): _*) {
      val df = readCsv(abfss("data", "accta", "csv-a"), abfss("data", "acctb", "csv-b"))
      checkSparkAnswerAndFallbackReason(df, multipleAuthoritiesReason)
    }
  }

  test("native CSV scan over two containers of one account falls back to Spark") {
    writeCsv("csv-c", 0)
    writeCsv("csv-d", 100)
    withSQLConf(csvConfs ++ accountKeys("accta" -> "a2V5LWE="): _*) {
      val df = readCsv(abfss("data", "accta", "csv-c"), abfss("logs", "accta", "csv-d"))
      checkSparkAnswerAndFallbackReason(df, multipleAuthoritiesReason)
    }
  }
}
