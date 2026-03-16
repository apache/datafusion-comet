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

import scala.jdk.CollectionConverters._
import scala.util.Random

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.expressions.StructsToCsv
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

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

  test("to_csv - with configurable formatting options") {
    val table = "t1"
    withSQLConf(
      CometConf.COMET_NATIVE_SCAN_IMPL.key -> CometConf.SCAN_NATIVE_ICEBERG_COMPAT,
      CometConf.getExprAllowIncompatConfigKey(classOf[StructsToCsv]) -> "true") {
      withTable(table) {
        val newLinesStr =
          """ abc
            | bcde""".stripMargin
        sql(s"create table $table(col string) using parquet")
        sql(s"insert into $table values('')")
        sql(s"insert into $table values(cast(null as string))")
        sql(s"insert into $table values('   abc')")
        sql(s"insert into $table values('abc   ')")
        sql(s"insert into $table values('  abc   ')")
        sql(s"""insert into $table values('abc \"abc\"')""")
        sql(s"""insert into $table values('$newLinesStr')""")
        sql(s"""insert into $table values('abc,def')""")
        sql(s"""insert into $table values('abc;def;ghi')""")
        sql(s"""insert into $table values('abc\tdef')""")
        sql(s"""insert into $table values('a"b"c')""")
        sql(s"""insert into $table values('"quoted"')""")
        sql(s"""insert into $table values('line1\nline2')""")
        sql(s"""insert into $table values('line1\rline2')""")
        sql(s"""insert into $table values('line1\r\nline2')""")
        sql(s"""insert into $table values('a''b')""")
        sql(s"""insert into $table values('a\\\\b')""")

        val df = sql(s"select * from $table order by col")

        // Default options
        checkSparkAnswerAndOperator(df.select(to_csv(struct(col("col"), lit(1)))))

        // Custom delimiter
        checkSparkAnswerAndOperator(
          df.select(to_csv(struct(col("col"), lit(1)), Map("delimiter" -> ";").asJava)))

        checkSparkAnswerAndOperator(
          df.select(to_csv(struct(col("col"), lit(1)), Map("delimiter" -> "|").asJava)))

        checkSparkAnswerAndOperator(
          df.select(to_csv(struct(col("col"), lit(1)), Map("delimiter" -> "\t").asJava)))

        // Whitespace handling
        checkSparkAnswerAndOperator(
          df.select(
            to_csv(
              struct(col("col"), lit(1)),
              Map(
                "delimiter" -> ";",
                "ignoreLeadingWhiteSpace" -> "false",
                "ignoreTrailingWhiteSpace" -> "false").asJava)))

        checkSparkAnswerAndOperator(
          df.select(
            to_csv(
              struct(col("col"), lit(1)),
              Map(
                "ignoreLeadingWhiteSpace" -> "true",
                "ignoreTrailingWhiteSpace" -> "false").asJava)))

        checkSparkAnswerAndOperator(
          df.select(
            to_csv(
              struct(col("col"), lit(1)),
              Map(
                "ignoreLeadingWhiteSpace" -> "false",
                "ignoreTrailingWhiteSpace" -> "true").asJava)))

        checkSparkAnswerAndOperator(df.select(to_csv(
          struct(col("col"), lit(1)),
          Map("ignoreLeadingWhiteSpace" -> "true", "ignoreTrailingWhiteSpace" -> "true").asJava)))

        // Escape character
        checkSparkAnswerAndOperator(
          df.select(to_csv(struct(col("col"), lit(1)), Map("escape" -> "\\").asJava)))

        checkSparkAnswerAndOperator(
          df.select(to_csv(struct(col("col"), lit(1)), Map("escape" -> "/").asJava)))

        // Quote options
        checkSparkAnswerAndOperator(
          df.select(to_csv(struct(col("col"), lit(1)), Map("quoteAll" -> "true").asJava)))

        checkSparkAnswerAndOperator(
          df.select(to_csv(struct(col("col"), lit(1)), Map("quoteAll" -> "false").asJava)))

        // Null value representation
        checkSparkAnswerAndOperator(
          df.select(to_csv(struct(col("col"), lit(1)), Map("nullValue" -> "NULL").asJava)))

        checkSparkAnswerAndOperator(
          df.select(to_csv(struct(col("col"), lit(1)), Map("nullValue" -> "N/A").asJava)))

        checkSparkAnswerAndOperator(
          df.select(to_csv(struct(col("col"), lit(1)), Map("nullValue" -> "").asJava)))

        // Combined options
        checkSparkAnswerAndOperator(
          df.select(
            to_csv(
              struct(col("col"), lit(1)),
              Map(
                "delimiter" -> "|",
                "quoteAll" -> "false",
                "escape" -> "\\",
                "nullValue" -> "NULL").asJava)))

        checkSparkAnswerAndOperator(
          df.select(to_csv(
            struct(col("col"), lit(1)),
            Map(
              "delimiter" -> ";",
              "quoteAll" -> "false",
              "ignoreLeadingWhiteSpace" -> "true",
              "ignoreTrailingWhiteSpace" -> "true",
              "nullValue" -> "N/A").asJava)))

        // Edge cases with multiple columns
        checkSparkAnswerAndOperator(
          df.select(
            to_csv(
              struct(col("col"), lit(1), lit("test"), lit(null).cast(StringType)),
              Map("delimiter" -> ",", "quoteAll" -> "true").asJava)))
      }
    }
  }
}
