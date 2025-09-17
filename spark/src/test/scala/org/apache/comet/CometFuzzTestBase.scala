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
import java.text.SimpleDateFormat

import scala.util.Random

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.{CometNativeScanExec, CometScanExec}
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.testing.{DataGenOptions, ParquetGenerator}

class CometFuzzTestBase extends CometTestBase with AdaptiveSparkPlanHelper {

  var filename: String = null

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
    val tempDir = System.getProperty("java.io.tmpdir")
    filename = s"$tempDir/CometFuzzTestSuite_${System.currentTimeMillis()}.parquet"
    val random = new Random(42)
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "false",
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> defaultTimezone) {
      val options =
        DataGenOptions(
          generateArray = true,
          generateStruct = true,
          generateNegativeZero = false,
          // override base date due to known issues with experimental scans
          baseDate =
            new SimpleDateFormat("YYYY-MM-DD hh:mm:ss").parse("2024-05-25 12:34:56").getTime)
      ParquetGenerator.makeParquetFile(random, spark, filename, 1000, options)
    }
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    FileUtils.deleteDirectory(new File(filename))
  }

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    Seq("native", "jvm").foreach { shuffleMode =>
      Seq(
        CometConf.SCAN_NATIVE_COMET,
        CometConf.SCAN_NATIVE_DATAFUSION,
        CometConf.SCAN_NATIVE_ICEBERG_COMPAT).foreach { scanImpl =>
        super.test(testName + s" ($scanImpl, $shuffleMode shuffle)", testTags: _*) {
          withSQLConf(
            CometConf.COMET_NATIVE_SCAN_IMPL.key -> scanImpl,
            CometConf.COMET_SCAN_ALLOW_INCOMPATIBLE.key -> "true",
            CometConf.COMET_SHUFFLE_MODE.key -> shuffleMode) {
            testFun
          }
        }
      }
    }
  }

  def collectNativeScans(plan: SparkPlan): Seq[SparkPlan] = {
    collect(plan) {
      case scan: CometScanExec => scan
      case scan: CometNativeScanExec => scan
    }
  }

  def collectCometShuffleExchanges(plan: SparkPlan): Seq[SparkPlan] = {
    collect(plan) { case exchange: CometShuffleExchangeExec =>
      exchange
    }
  }

}
