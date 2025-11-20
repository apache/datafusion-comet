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

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.CometIcebergNativeScanExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DecimalType

import org.apache.comet.testing.{DataGenOptions, FuzzDataGenerator, SchemaGenOptions}

class CometFuzzIcebergBase extends CometTestBase with AdaptiveSparkPlanHelper {

  var warehouseDir: File = null
  val icebergTableName: String = "hadoop_catalog.db.fuzz_test"

  // Skip these tests if Iceberg is not available in classpath
  private def icebergAvailable: Boolean = {
    try {
      Class.forName("org.apache.iceberg.catalog.Catalog")
      true
    } catch {
      case _: ClassNotFoundException => false
    }
  }

  private def isIcebergVersionLessThan(targetVersion: String): Boolean = {
    try {
      val icebergVersion = org.apache.iceberg.IcebergBuild.version()
      // Parse version strings like "1.5.2" or "1.6.0-SNAPSHOT"
      val current = parseVersion(icebergVersion)
      val target = parseVersion(targetVersion)
      // Compare tuples: first by major, then minor, then patch
      if (current._1 != target._1) current._1 < target._1
      else if (current._2 != target._2) current._2 < target._2
      else current._3 < target._3
    } catch {
      case _: Exception =>
        // If we can't determine the version, assume it's old to be safe
        true
    }
  }

  private def parseVersion(version: String): (Int, Int, Int) = {
    val parts = version.split("[.-]").take(3).map(_.filter(_.isDigit))
    val major = if (parts.length > 0 && parts(0).nonEmpty) parts(0).toInt else 0
    val minor = if (parts.length > 1 && parts(1).nonEmpty) parts(1).toInt else 0
    val patch = if (parts.length > 2 && parts(2).nonEmpty) parts(2).toInt else 0
    (major, minor, patch)
  }

  /**
   * We use Asia/Kathmandu because it has a non-zero number of minutes as the offset, so is an
   * interesting edge case. Also, this timezone tends to be different from the default system
   * timezone.
   *
   * Represents UTC+5:45
   */
  val defaultTimezone = "Asia/Kathmandu"

  override def beforeAll(): Unit = {
    super.beforeAll()
    assume(icebergAvailable, "Iceberg not available in classpath")
    warehouseDir = Files.createTempDirectory("comet-iceberg-fuzz-test").toFile
    val random = new Random(42)
    withSQLConf(
      "spark.sql.catalog.hadoop_catalog" -> "org.apache.iceberg.spark.SparkCatalog",
      "spark.sql.catalog.hadoop_catalog.type" -> "hadoop",
      "spark.sql.catalog.hadoop_catalog.warehouse" -> warehouseDir.getAbsolutePath,
      CometConf.COMET_ENABLED.key -> "false",
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> defaultTimezone) {

      val schema = FuzzDataGenerator.generateSchema(
        SchemaGenOptions(
          generateArray = true,
          generateStruct = true,
          primitiveTypes = SchemaGenOptions.defaultPrimitiveTypes.filterNot { dataType =>
            // Disable decimals - iceberg-rust doesn't support FIXED_LEN_BYTE_ARRAY in page index yet
            dataType.isInstanceOf[DecimalType] ||
            // Disable ByteType and ShortType for Iceberg < 1.6.0
            // Fixed in https://github.com/apache/iceberg/pull/10349
            (isIcebergVersionLessThan(
              "1.6.0") && (dataType == org.apache.spark.sql.types.DataTypes.ByteType ||
              dataType == org.apache.spark.sql.types.DataTypes.ShortType))
          }))

      val options =
        DataGenOptions(
          generateNegativeZero = false,
          baseDate =
            new SimpleDateFormat("YYYY-MM-DD hh:mm:ss").parse("2024-05-25 12:34:56").getTime)

      val df = FuzzDataGenerator.generateDataFrame(random, spark, schema, 1000, options)
      df.writeTo(icebergTableName).using("iceberg").create()
    }
  }

  protected override def afterAll(): Unit = {
    try {
      spark.sql(s"DROP TABLE IF EXISTS $icebergTableName")
    } catch {
      case _: Exception =>
    }

    if (warehouseDir != null) {
      def deleteRecursively(file: File): Unit = {
        if (file.isDirectory) {
          file.listFiles().foreach(deleteRecursively)
        }
        file.delete()
      }

      deleteRecursively(warehouseDir)
    }
    super.afterAll()
  }

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      withSQLConf(
        "spark.sql.catalog.hadoop_catalog" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.hadoop_catalog.type" -> "hadoop",
        "spark.sql.catalog.hadoop_catalog.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {
        testFun
      }
    }
  }

  def collectIcebergNativeScans(plan: SparkPlan): Seq[CometIcebergNativeScanExec] = {
    collect(plan) { case scan: CometIcebergNativeScanExec =>
      scan
    }
  }
}
