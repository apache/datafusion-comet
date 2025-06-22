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

import org.apache.comet.CometConf
import org.apache.comet.testing.{DataGenOptions, ParquetGenerator}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, ToPrettyString}
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.internal.SQLConf

import java.io.File
import java.text.SimpleDateFormat
import scala.util.Random

class CometToPrettyStringSuite extends CometTestBase {

  private var filename: String = null

  /**
   * We use Asia/Kathmandu because it has a non-zero number of minutes as the offset, so is an
   * interesting edge case. Also, this timezone tends to be different from the default system
   * timezone.
   *
   * Represents UTC+5:45
   */
  private val defaultTimezone = "Asia/Kathmandu"

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
          generateMap = true,
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

  test("ToPrettyString") {
    val df = spark.read.parquet(filename)
    df.createOrReplaceTempView("t1")
    val table = spark.sessionState.catalog.lookupRelation(TableIdentifier("t1"))
    for (col <- df.columns) {
      val prettyExpr = Alias(ToPrettyString(UnresolvedAttribute(col)), s"pretty_$col")()
      val plan = Project(Seq(prettyExpr), table)
      val analyzed = spark.sessionState.analyzer.execute(plan)
      val result: DataFrame = Dataset.ofRows(spark, analyzed)
      checkSparkAnswerAndOperator(result)
    }
  }

}
