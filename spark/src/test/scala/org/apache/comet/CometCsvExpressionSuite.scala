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
import org.apache.spark.sql.catalyst.expressions.StructsToCsv
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions._

import org.apache.comet.testing.{DataGenOptions, ParquetGenerator, SchemaGenOptions}

class CometCsvExpressionSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  test("to_csv - default options") {
    withTempDir { dir =>
      val path = new Path(dir.toURI.toString, "test.parquet")
      val filename = path.toString
      val random = new Random(42)
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        ParquetGenerator.makeParquetFile(
          random,
          spark,
          filename,
          100,
          SchemaGenOptions(generateArray = false, generateStruct = false, generateMap = false),
          DataGenOptions(allowNull = true, generateNegativeZero = true))
      }
      withSQLConf(CometConf.getExprAllowIncompatConfigKey(classOf[StructsToCsv]) -> "true") {
        val df = spark.read
          .parquet(filename)
          .select(
            to_csv(
              struct(
                col("c0"),
                col("c1"),
                col("c2"),
                col("c3"),
                col("c4"),
                col("c5"),
                col("c7"),
                col("c8"),
                col("c9"),
                col("c12"))))
        checkSparkAnswerAndOperator(df)
      }
    }
  }

  test("to_csv - string cases processing") {
    val table = "t1"
    withSQLConf(CometConf.COMET_NATIVE_SCAN_IMPL.key -> CometConf.SCAN_NATIVE_ICEBERG_COMPAT) {
      withTable(table) {
        sql(s"create table $table(col string) using parquet")
        sql(s"insert into $table values(cast(null as string))")
        sql(s"insert into $table values('abc')")
        sql(s"""insert into $table values('abc \"abc\"')""")
        sql(s"select * from $table").show(false)
        val df = sql(s"select to_csv(struct(col, 1, 'abc')) from $table")
        checkSparkAnswerAndOperator(df)
      }
    }
  }
}
