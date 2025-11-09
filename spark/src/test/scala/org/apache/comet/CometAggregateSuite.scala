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

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import org.apache.comet.CometConf.COMET_EXEC_STRICT_FLOATING_POINT
import org.apache.comet.testing.{DataGenOptions, FuzzDataGenerator}

class CometAggregateSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  test("min/max floating point with negative zero") {
    val r = new Random(42)
    val schema = StructType(
      Seq(
        StructField("float_col", DataTypes.FloatType, nullable = true),
        StructField("double_col", DataTypes.DoubleType, nullable = true)))
    val df = FuzzDataGenerator.generateDataFrame(
      r,
      spark,
      schema,
      1000,
      DataGenOptions(generateNegativeZero = true))
    df.createOrReplaceTempView("tbl")

    for (col <- Seq("float_col", "double_col")) {
      // assert that data contains positive and negative zero
      assert(spark.sql(s"select * from tbl where cast($col as string) = '0.0'").count() > 0)
      assert(spark.sql(s"select * from tbl where cast($col as string) = '-0.0'").count() > 0)
      for (agg <- Seq("min", "max")) {
        withSQLConf(COMET_EXEC_STRICT_FLOATING_POINT.key -> "true") {
          checkSparkAnswerAndFallbackReason(
            s"select $agg($col) from tbl where cast($col as string) in ('0.0', '-0.0')",
            s"floating-point not supported when ${COMET_EXEC_STRICT_FLOATING_POINT.key}=true")
        }
      }
    }
  }

}
