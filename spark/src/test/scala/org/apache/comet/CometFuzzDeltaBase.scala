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
import java.nio.file.Files
import java.text.SimpleDateFormat

import scala.util.Random

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.SparkConf
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.CometDeltaNativeScanExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DecimalType

import org.apache.comet.testing.{DataGenOptions, FuzzDataGenerator, SchemaGenOptions}

class CometFuzzDeltaBase extends CometTestBase with AdaptiveSparkPlanHelper {

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set(CometConf.COMET_DELTA_NATIVE_ENABLED.key, "true")
    conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    conf.set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    conf.set("spark.databricks.delta.testOnly.dataFileNamePrefix", "")
    conf.set("spark.databricks.delta.testOnly.dvFileNamePrefix", "")
    conf
  }

  var deltaDir: File = null
  val deltaTableName: String = "fuzz_delta_table"

  private def deltaSparkAvailable: Boolean = {
    try {
      Class.forName("org.apache.spark.sql.delta.DeltaParquetFileFormat")
      true
    } catch {
      case _: ClassNotFoundException => false
    }
  }

  val defaultTimezone = "Asia/Kathmandu"

  override def beforeAll(): Unit = {
    super.beforeAll()
    assume(deltaSparkAvailable, "delta-spark not available in classpath")
    deltaDir = Files.createTempDirectory("comet-delta-fuzz-test").toFile
    val random = new Random(42)
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "false",
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> defaultTimezone) {

      val schema = FuzzDataGenerator.generateSchema(
        SchemaGenOptions(
          generateArray = true,
          generateStruct = true,
          primitiveTypes = SchemaGenOptions.defaultPrimitiveTypes))

      val options =
        DataGenOptions(
          generateNegativeZero = false,
          baseDate =
            new SimpleDateFormat("YYYY-MM-DD hh:mm:ss").parse("2024-05-25 12:34:56").getTime)

      val df = FuzzDataGenerator.generateDataFrame(random, spark, schema, 1000, options)
      val tablePath = new File(deltaDir, "t").getAbsolutePath
      // Write as a single file so split-mode produces one Spark partition,
      // matching vanilla Spark's row ordering for LIMIT tests.
      df.repartition(1).write.format("delta").save(tablePath)
      spark.read.format("delta").load(tablePath).createOrReplaceTempView(deltaTableName)
    }
  }

  protected override def afterAll(): Unit = {
    try {
      spark.catalog.dropTempView(deltaTableName)
    } catch {
      case _: Exception =>
    }

    if (deltaDir != null) {
      def deleteRecursively(file: File): Unit = {
        if (file.isDirectory) {
          Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
        }
        file.delete()
      }
      deleteRecursively(deltaDir)
    }
    super.afterAll()
  }

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      // spark.sql.extensions and spark.sql.catalog.spark_catalog are static configs
      // set once at session creation via sparkConf. Only toggle dynamic configs here.
      // Set timezone to match the write timezone so timestamp comparisons are stable.
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_DELTA_NATIVE_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> defaultTimezone) {
        testFun
      }
    }
  }

  def collectDeltaNativeScans(plan: SparkPlan): Seq[CometDeltaNativeScanExec] = {
    collect(plan) { case scan: CometDeltaNativeScanExec =>
      scan
    }
  }
}
