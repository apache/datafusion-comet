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

import scala.util.Random

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.types._

import org.apache.comet.testing.{DataGenOptions, ParquetGenerator}

class CometFuzzTestSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  private val fuzzTestEnabled: Boolean = sys.env.contains("COMET_FUZZ_TEST")

  test("aggregates") {
    assume(fuzzTestEnabled)
    withTempDir { dir =>
      val path = new Path(dir.toURI.toString, "test.parquet")
      val filename = path.toString
      val random = new Random(42)
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        ParquetGenerator.makeParquetFile(random, spark, filename, 10000, DataGenOptions())
      }
      val table = spark.read.parquet(filename).coalesce(1)
      table.createOrReplaceTempView("t1")

      val numericAggs = Seq(
        "min",
        "max",
        "sum",
        "avg",
        "count",
        "median",
        "stddev",
        "stddev_pop",
        "stddev_samp",
        "variance",
        "var_pop",
        "var_samp")

      // TODO: corr, covar_pop, covar_samp needs 2 args

      val numericFields = table.schema.fields.filter(isNumeric)
      val groupingFields: Array[StructField] = table.schema.fields.filterNot(isNumeric)

      // test grouping by each non-numeric column, grouping by all non-numeric columns, and no grouping
      val groupByIndividualCols: Seq[Seq[String]] = groupingFields.map(f => Seq(f.name)).toSeq
      val groupByAllCols: Seq[Seq[String]] = Seq(groupingFields.map(_.name).toSeq)
      val noGroup: Seq[Seq[String]] = Seq(Seq.empty)
      val groupings: Seq[Seq[String]] = groupByIndividualCols ++ groupByAllCols ++ noGroup

      for (scan <- Seq(
          CometConf.SCAN_NATIVE_COMET,
          CometConf.SCAN_NATIVE_DATAFUSION,
          CometConf.SCAN_NATIVE_ICEBERG_COMPAT)) {
        for (shuffleMode <- Seq("auto", "jvm", "native")) {
          withSQLConf(
            CometConf.COMET_NATIVE_SCAN_IMPL.key -> scan,
            CometConf.COMET_SHUFFLE_MODE.key -> shuffleMode) {
            for (group <- groupings) {
              for (agg <- numericAggs) {
                for (field <- numericFields) {
                  if (agg == "avg" && field.dataType.isInstanceOf[DecimalType]) {
                    // skip known issue
                  } else {
                    val sql = if (group.isEmpty) {
                      s"SELECT $agg(${field.name}) FROM t1"
                    } else {
                      val groupCols = group.mkString(", ")
                      s"SELECT $groupCols, $agg(${field.name}) FROM t1 GROUP BY $groupCols ORDER BY $groupCols"
                    }
                    println(sql)
                    checkSparkAnswerWithTol(sql)
                    // TODO check operators
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  private def isNumeric(field: StructField) = {
    field.dataType match {
      case _: ByteType | _: ShortType | _: IntegerType | _: LongType => true
      case _: FloatType | _: DoubleType => true
      case _: DecimalType => true
      case _ => false
    }
  }
}
