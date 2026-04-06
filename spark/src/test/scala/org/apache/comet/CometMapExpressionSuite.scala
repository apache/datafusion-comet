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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.BinaryType

import org.apache.comet.CometSparkSessionExtensions.isSpark40Plus
import org.apache.comet.serde.CometMapFromEntries
import org.apache.comet.testing.{DataGenOptions, ParquetGenerator, SchemaGenOptions}

class CometMapExpressionSuite extends CometTestBase {

  test("read map[int, int] from parquet") {

    withTempPath { dir =>
      // create input file with Comet disabled
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        val df = spark
          .range(5)
          // Spark does not allow null as a key but does allow null as a
          // value, and the entire map be null
          .select(
            when(col("id") > 1, map(col("id"), when(col("id") > 2, col("id")))).alias("map1"))
        df.write.parquet(dir.toString())
      }

      Seq("", "parquet").foreach { v1List =>
        withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> v1List) {
          val df = spark.read.parquet(dir.toString())
          if (v1List.isEmpty) {
            checkSparkAnswer(df.select("map1"))
          } else {
            checkSparkAnswerAndOperator(df.select("map1"))
          }
          // we fall back to Spark for map_keys and map_values
          checkSparkAnswer(df.select(map_keys(col("map1"))))
          checkSparkAnswer(df.select(map_values(col("map1"))))
        }
      }
    }
  }

  // repro for https://github.com/apache/datafusion-comet/issues/1754
  test("read map[struct, struct] from parquet") {

    withTempPath { dir =>
      // create input file with Comet disabled
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        val df = spark
          .range(5)
          .withColumn("id2", col("id"))
          .withColumn("id3", col("id"))
          // Spark does not allow null as a key but does allow null as a
          // value, and the entire map be null
          .select(
            when(
              col("id") > 1,
              map(
                struct(col("id"), col("id2"), col("id3")),
                when(col("id") > 2, struct(col("id"), col("id2"), col("id3"))))).alias("map1"))
        df.write.parquet(dir.toString())
      }

      Seq("", "parquet").foreach { v1List =>
        withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> v1List) {
          val df = spark.read.parquet(dir.toString())
          df.createOrReplaceTempView("tbl")
          if (v1List.isEmpty) {
            checkSparkAnswer(df.select("map1"))
          } else {
            checkSparkAnswerAndOperator(df.select("map1"))
          }
          // we fall back to Spark for map_keys and map_values
          checkSparkAnswer(df.select(map_keys(col("map1"))))
          checkSparkAnswer(df.select(map_values(col("map1"))))
          checkSparkAnswer(spark.sql("SELECT map_keys(map1).id2 FROM tbl"))
          checkSparkAnswer(spark.sql("SELECT map_values(map1).id2 FROM tbl"))
        }
      }
    }
  }

  test("map_from_arrays") {
    withTempDir { dir =>
      val path = new Path(dir.toURI.toString, "test.parquet")
      val filename = path.toString
      val random = new Random(42)
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        val schemaGenOptions =
          SchemaGenOptions(generateArray = true, generateStruct = false, generateMap = false)
        val dataGenOptions = DataGenOptions(allowNull = false, generateNegativeZero = false)
        ParquetGenerator.makeParquetFile(
          random,
          spark,
          filename,
          100,
          schemaGenOptions,
          dataGenOptions)
      }
      spark.read.parquet(filename).createOrReplaceTempView("t1")
      val df = spark.sql("SELECT map_from_arrays(array(c12), array(c3)) FROM t1")
      checkSparkAnswerAndOperator(df)
    }
  }

  test("fallback for size with map input") {
    withTempDir { dir =>
      withTempView("t1") {
        val path = new Path(dir.toURI.toString, "test.parquet")
        makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = true, 100)
        spark.read.parquet(path.toString).createOrReplaceTempView("t1")

        // Use column references in maps to avoid constant folding
        checkSparkAnswerAndFallbackReason(
          sql("SELECT size(case when _2 < 0 then map(_8, _9) else map() end) from t1"),
          "size does not support map inputs")
      }
    }
  }

  // fails with "map is not supported"
  ignore("size with map input") {
    withTempDir { dir =>
      withTempView("t1") {
        val path = new Path(dir.toURI.toString, "test.parquet")
        makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = true, 100)
        spark.read.parquet(path.toString).createOrReplaceTempView("t1")

        // Use column references in maps to avoid constant folding
        checkSparkAnswerAndOperator(
          sql("SELECT size(map(_8, _9, _10, _11)) from t1 where _8 is not null"))
        checkSparkAnswerAndOperator(
          sql("SELECT size(case when _2 < 0 then map(_8, _9) else map() end) from t1"))
      }
    }
  }

  test("map_from_entries - convert from Parquet") {
    withTempDir { dir =>
      val path = new Path(dir.toURI.toString, "test.parquet")
      val filename = path.toString
      val random = new Random(42)
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        val schemaGenOptions =
          SchemaGenOptions(
            generateArray = false,
            generateStruct = false,
            primitiveTypes = SchemaGenOptions.defaultPrimitiveTypes.filterNot(_ == BinaryType))
        val dataGenOptions = DataGenOptions(allowNull = false, generateNegativeZero = false)
        ParquetGenerator.makeParquetFile(
          random,
          spark,
          filename,
          100,
          schemaGenOptions,
          dataGenOptions)
      }
      withSQLConf(
        CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "false",
        CometConf.COMET_SPARK_TO_ARROW_ENABLED.key -> "true",
        CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.key -> "true") {
        val df = spark.read.parquet(filename)
        df.createOrReplaceTempView("t1")
        for (field <- df.schema.fieldNames) {
          checkSparkAnswerAndOperator(
            spark.sql(
              s"SELECT map_from_entries(array(struct($field as a, $field as b))) FROM t1"))
        }
      }
    }
  }

  test("map_from_entries - native Parquet reader") {
    withTempDir { dir =>
      val path = new Path(dir.toURI.toString, "test.parquet")
      val filename = path.toString
      val random = new Random(42)
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        val schemaGenOptions =
          SchemaGenOptions(
            generateArray = false,
            generateStruct = false,
            primitiveTypes = SchemaGenOptions.defaultPrimitiveTypes.filterNot(_ == BinaryType))
        val dataGenOptions = DataGenOptions(allowNull = false, generateNegativeZero = false)
        ParquetGenerator.makeParquetFile(
          random,
          spark,
          filename,
          100,
          schemaGenOptions,
          dataGenOptions)
      }
      val df = spark.read.parquet(filename)
      df.createOrReplaceTempView("t1")
      for (field <- df.schema.fieldNames) {
        checkSparkAnswerAndOperator(
          spark.sql(s"SELECT map_from_entries(array(struct($field as a, $field as b))) FROM t1"))
      }
    }
  }

  test("map_from_entries - fallback for binary type") {
    def fallbackReason(reason: String) = reason
    val table = "t2"
    withTable(table) {
      sql(
        s"create table $table using parquet as select cast(array() as array<binary>) as c1 from range(10)")
      checkSparkAnswerAndFallbackReason(
        sql(s"select map_from_entries(array(struct(c1, 0))) from $table"),
        fallbackReason(CometMapFromEntries.keyUnsupportedReason))
      checkSparkAnswerAndFallbackReason(
        sql(s"select map_from_entries(array(struct(0, c1))) from $table"),
        fallbackReason(CometMapFromEntries.valueUnsupportedReason))
    }
  }



  test("map_sort with integer keys") {
    assume(isSpark40Plus, "map_sort was added in Spark 4.0")
    withTempDir { dir =>
      withTempView("t1") {
        val path = new Path(dir.toURI.toString, "test.parquet")
        withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
          val df = spark
            .range(5)
            .select(map(lit(3), lit("c"), lit(1), lit("a"), lit(2), lit("b")).alias("m"))
          df.write.parquet(path.toString)
        }
        spark.read.parquet(path.toString).createOrReplaceTempView("t1")
        checkSparkAnswerAndOperator(sql("SELECT map_sort(m) FROM t1"))
      }
    }
  }

  test("map_sort with string keys") {
    assume(isSpark40Plus, "map_sort was added in Spark 4.0")
    withTempDir { dir =>
      withTempView("t1") {
        val path = new Path(dir.toURI.toString, "test.parquet")
        withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
          val df = spark
            .range(5)
            .select(map(lit("z"), lit(1), lit("a"), lit(2), lit("m"), lit(3)).alias("m"))
          df.write.parquet(path.toString)
        }
        spark.read.parquet(path.toString).createOrReplaceTempView("t1")
        checkSparkAnswerAndOperator(sql("SELECT map_sort(m) FROM t1"))
      }
    }
  }

  test("map_sort with double keys") {
    assume(isSpark40Plus, "map_sort was added in Spark 4.0")
    withTempDir { dir =>
      withTempView("t1") {
        val path = new Path(dir.toURI.toString, "test.parquet")
        withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
          val df = spark
            .range(5)
            .select(map(lit(3.5), lit("c"), lit(1.2), lit("a"), lit(2.8), lit("b")).alias("m"))
          df.write.parquet(path.toString)
        }
        spark.read.parquet(path.toString).createOrReplaceTempView("t1")
        checkSparkAnswerAndOperator(sql("SELECT map_sort(m) FROM t1"))
      }
    }
  }

  test("map_sort with null and empty maps") {
    assume(isSpark40Plus, "map_sort was added in Spark 4.0")
    withTempDir { dir =>
      withTempView("t1") {
        val path = new Path(dir.toURI.toString, "test.parquet")
        withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
          val df = spark
            .range(5)
            .select(
              when(col("id") === 0, lit(null))
                .when(col("id") === 1, map())
                .when(col("id") === 2, map(lit(1), lit("a")))
                .otherwise(map(lit(3), lit("c"), lit(2), lit("b")))
                .alias("m"))
          df.write.parquet(path.toString)
        }
        spark.read.parquet(path.toString).createOrReplaceTempView("t1")
        checkSparkAnswerAndOperator(sql("SELECT map_sort(m) FROM t1"))
      }
    }
  }

  test("map_sort with struct keys") {
    assume(isSpark40Plus, "map_sort was added in Spark 4.0")
    withTempDir { dir =>
      withTempView("t1") {
        val path = new Path(dir.toURI.toString, "test.parquet")
        withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
          val df = spark
            .range(3)
            .select(
              map(
                struct(lit(2), lit("b")),
                lit("second"),
                struct(lit(1), lit("a")),
                lit("first"),
                struct(lit(3), lit("c")),
                lit("third")).alias("m"))
          df.write.parquet(path.toString)
        }
        spark.read.parquet(path.toString).createOrReplaceTempView("t1")
        checkSparkAnswerAndOperator(sql("SELECT map_sort(m) FROM t1"))
      }
    }
  }

  test("map_sort with array keys") {
    assume(isSpark40Plus, "map_sort was added in Spark 4.0")
    withTempDir { dir =>
      withTempView("t1") {
        val path = new Path(dir.toURI.toString, "test.parquet")
        withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
          val df = spark
            .range(3)
            .select(
              map(
                array(lit(2), lit(3)),
                lit("array2"),
                array(lit(1), lit(2)),
                lit("array1"),
                array(lit(3), lit(4)),
                lit("array3")).alias("m"))
          df.write.parquet(path.toString)
        }
        spark.read.parquet(path.toString).createOrReplaceTempView("t1")
        checkSparkAnswerAndOperator(sql("SELECT map_sort(m) FROM t1"))
      }
    }
  }

  test("map_sort with complex values") {
    assume(isSpark40Plus, "map_sort was added in Spark 4.0")
    withTempDir { dir =>
      withTempView("t1") {
        val path = new Path(dir.toURI.toString, "test.parquet")
        withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
          val df = spark
            .range(3)
            .select(
              map(
                lit(3),
                struct(lit("c"), array(lit(30), lit(31))),
                lit(1),
                struct(lit("a"), array(lit(10), lit(11))),
                lit(2),
                struct(lit("b"), array(lit(20), lit(21)))).alias("m"))
          df.write.parquet(path.toString)
        }
        spark.read.parquet(path.toString).createOrReplaceTempView("t1")
        checkSparkAnswerAndOperator(sql("SELECT map_sort(m) FROM t1"))
      }
    }
  }

  test("map_sort fallback for non-orderable keys") {
    assume(isSpark40Plus, "map_sort was added in Spark 4.0")
    withTempDir { dir =>
      withTempView("t1") {
        val path = new Path(dir.toURI.toString, "test.parquet")
        withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
          val df = spark
            .range(3)
            .select(
              map(
                map(lit(1), lit("inner1")),
                lit("outer1"),
                map(lit(2), lit("inner2")),
                lit("outer2")).alias("m"))
          df.write.parquet(path.toString)
        }
        spark.read.parquet(path.toString).createOrReplaceTempView("t1")
        checkSparkAnswerAndFallbackReason(
          sql("SELECT map_sort(m) FROM t1"),
          "map_sort requires orderable key type")
      }
    }
  }

  test("map_sort with boolean keys") {
    assume(isSpark40Plus, "map_sort was added in Spark 4.0")
    withTempDir { dir =>
      withTempView("t1") {
        val path = new Path(dir.toURI.toString, "test.parquet")
        withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
          val df = spark
            .range(5)
            .select(map(lit(true), lit("yes"), lit(false), lit("no")).alias("m"))
          df.write.parquet(path.toString)
        }
        spark.read.parquet(path.toString).createOrReplaceTempView("t1")
        checkSparkAnswerAndOperator(sql("SELECT map_sort(m) FROM t1"))
      }
    }
  }

  test("map_sort with decimal keys") {
    assume(isSpark40Plus, "map_sort was added in Spark 4.0")
    withTempDir { dir =>
      withTempView("t1") {
        val path = new Path(dir.toURI.toString, "test.parquet")
        withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
          val df = spark
            .range(5)
            .select(
              map(
                lit(BigDecimal("3.14")),
                lit("pi"),
                lit(BigDecimal("1.41")),
                lit("sqrt2"),
                lit(BigDecimal("2.72")),
                lit("e")).alias("m"))
          df.write.parquet(path.toString)
        }
        spark.read.parquet(path.toString).createOrReplaceTempView("t1")
        checkSparkAnswerAndOperator(sql("SELECT map_sort(m) FROM t1"))
      }
    }
  }

  test("map_sort with date keys") {
    assume(isSpark40Plus, "map_sort was added in Spark 4.0")
    withTempDir { dir =>
      withTempView("t1") {
        val path = new Path(dir.toURI.toString, "test.parquet")
        withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
          val df = spark
            .range(5)
            .select(
              map(
                lit(java.sql.Date.valueOf("2024-03-15")),
                lit("march"),
                lit(java.sql.Date.valueOf("2024-01-10")),
                lit("jan"),
                lit(java.sql.Date.valueOf("2024-02-20")),
                lit("feb")).alias("m"))
          df.write.parquet(path.toString)
        }
        spark.read.parquet(path.toString).createOrReplaceTempView("t1")
        checkSparkAnswerAndOperator(sql("SELECT map_sort(m) FROM t1"))
      }
    }
  }

  test("map_sort with timestamp keys") {
    assume(isSpark40Plus, "map_sort was added in Spark 4.0")
    withTempDir { dir =>
      withTempView("t1") {
        val path = new Path(dir.toURI.toString, "test.parquet")
        withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
          val df = spark
            .range(5)
            .select(
              map(
                lit(java.sql.Timestamp.valueOf("2024-03-15 10:30:00")),
                lit("third"),
                lit(java.sql.Timestamp.valueOf("2024-01-10 08:00:00")),
                lit("first"),
                lit(java.sql.Timestamp.valueOf("2024-02-20 14:15:00")),
                lit("second")).alias("m"))
          df.write.parquet(path.toString)
        }
        spark.read.parquet(path.toString).createOrReplaceTempView("t1")
        checkSparkAnswerAndOperator(sql("SELECT map_sort(m) FROM t1"))
      }
    }
  }

}
