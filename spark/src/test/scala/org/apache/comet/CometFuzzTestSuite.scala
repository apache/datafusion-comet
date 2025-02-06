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
import org.apache.parquet.format.IntType
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.types._

import org.apache.comet.testing.{DataGenOptions, ParquetGenerator}

class CometFuzzTestSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  test("aggregates") {
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

      val numericFields = table.schema.fields.filter { field =>
        field.dataType match {
          case _: ByteType | _: ShortType | _: IntegerType | _: LongType => true
          case _: FloatType | _: DoubleType => true
          case _: DecimalType => true
          case _ => false
        }
      }

      // TODO test more group by types
      // TODO test no grouping
      // TODO test multiple grouping expr

      for (agg <- numericAggs) {
        for (field <- numericFields) {
          if (agg == "avg" && field.dataType.isInstanceOf[DecimalType]) {
            // skip known issue
          } else {
            val sql = s"SELECT c1, $agg(${field.name}) FROM t1 GROUP BY c1 ORDER BY c1"
            println(sql)
            checkSparkAnswerWithTol(sql)
            // TODO check operators
          }
        }
      }
    }
  }
}
