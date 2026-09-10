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
import org.apache.comet.udf.codegen.CometScalaUDFCodegen

class CometCsvExpressionSuite
    extends CometTestBase
    with AdaptiveSparkPlanHelper
    with CometCodegenAssertions {

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
    withSQLConf(CometConf.getExprAllowIncompatConfigKey(classOf[StructsToCsv]) -> "true") {
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

  test("to_csv routes through the codegen dispatcher by default (issue #5578)") {
    // Without allowIncompatible, getSupportLevel is Incompatible and CodegenDispatchFallback
    // runs Spark's own StructsToCsv.doGenCode inside the Comet pipeline. assertCodegenRan pins
    // that the dispatcher is what kept the projection native.
    val data: Seq[(Option[Int], Option[String])] =
      Seq((Some(1), Some("alice")), (Some(2), None), (None, Some("bob")), (None, None))
    withParquetTable(data, "tbl") {
      assertCodegenRan {
        checkSparkAnswerAndOperator("SELECT to_csv(named_struct('id', _1, 'name', _2)) FROM tbl")
        checkSparkAnswerAndOperator(
          "SELECT to_csv(named_struct('id', _1, 'name', _2), map('sep', ';')) FROM tbl")
      }
    }
  }

  test("to_csv with nested array field routes through the codegen dispatcher (issue #5578)") {
    // Nested array/map/struct fields are Unsupported on the native path. The dispatcher still
    // compiles them: arrays are in CometBatchKernelCodegen.isSupportedDataType.
    withTable("t") {
      sql("CREATE TABLE t(id INT, items ARRAY<INT>) USING parquet")
      sql("INSERT INTO t VALUES (1, array(1, 2, 3)), (2, array()), (3, NULL)")
      assertCodegenRan {
        checkSparkAnswerAndOperator(
          "SELECT to_csv(named_struct('id', id, 'items', items)) FROM t")
      }
    }
  }

  test("to_csv NULL struct routes through the codegen dispatcher (issue #5578)") {
    withTable("t") {
      sql("CREATE TABLE t(s STRUCT<id:INT, name:STRING>) USING parquet")
      sql(
        "INSERT INTO t VALUES (named_struct('id', 1, 'name', 'alice')), " +
          "(named_struct('id', CAST(NULL AS INT), 'name', 'bob')), (NULL)")
      assertCodegenRan {
        checkSparkAnswerAndOperator("SELECT to_csv(s) FROM t")
      }
    }
  }

  test("to_csv falls back to Spark when the codegen dispatcher is disabled (issue #5578)") {
    withParquetTable(Seq((1, "alice"), (2, "bob")), "tbl") {
      withSQLConf(CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "false") {
        checkSparkAnswerAndFallbackReason(
          "SELECT to_csv(named_struct('id', _1, 'name', _2)) FROM tbl",
          CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key)
      }
    }
  }

  test("to_csv keeps the native path when allowIncompatible is enabled (issue #5578)") {
    withParquetTable(Seq((1, "alice"), (2, "bob")), "tbl") {
      withSQLConf(CometConf.getExprAllowIncompatConfigKey(classOf[StructsToCsv]) -> "true") {
        CometScalaUDFCodegen.resetStats()
        checkSparkAnswerAndOperator("SELECT to_csv(named_struct('id', _1, 'name', _2)) FROM tbl")
        assert(
          CometScalaUDFCodegen.stats().totalLookups == 0,
          "expected native to_csv, not codegen dispatch")
      }
    }
  }
}
