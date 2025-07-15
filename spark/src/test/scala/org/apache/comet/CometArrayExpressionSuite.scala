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

import scala.collection.immutable.HashSet
import scala.util.Random

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.{array, col, expr, lit, udf}

import org.apache.comet.CometSparkSessionExtensions.{isSpark35Plus, isSpark40Plus}
import org.apache.comet.serde.CometArrayExcept
import org.apache.comet.testing.{DataGenOptions, ParquetGenerator}

class CometArrayExpressionSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  test("array_remove - integer") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test.parquet")
        makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled, 10000)
        spark.read.parquet(path.toString).createOrReplaceTempView("t1")
        checkSparkAnswerAndOperator(
          sql("SELECT array_remove(array(_2, _3,_4), _2) from t1 where _2 is null"))
        checkSparkAnswerAndOperator(
          sql("SELECT array_remove(array(_2, _3,_4), _3) from t1 where _3 is not null"))
        checkSparkAnswerAndOperator(sql(
          "SELECT array_remove(case when _2 = _3 THEN array(_2, _3,_4) ELSE null END, _3) from t1"))
      }
    }
  }

  test("array_remove - test all types (native Parquet reader)") {
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
          DataGenOptions(
            allowNull = true,
            generateNegativeZero = true,
            generateArray = false,
            generateStruct = false,
            generateMap = false))
      }
      val table = spark.read.parquet(filename)
      table.createOrReplaceTempView("t1")
      // test with array of each column
      for (fieldName <- table.schema.fieldNames) {
        sql(s"SELECT array($fieldName, $fieldName) as a, $fieldName as b FROM t1")
          .createOrReplaceTempView("t2")
        val df = sql("SELECT array_remove(a, b) FROM t2")
        checkSparkAnswerAndOperator(df)
      }
    }
  }

  test("array_remove - test all types (convert from Parquet)") {
    withTempDir { dir =>
      val path = new Path(dir.toURI.toString, "test.parquet")
      val filename = path.toString
      val random = new Random(42)
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        val options = DataGenOptions(
          allowNull = true,
          generateNegativeZero = true,
          generateArray = true,
          generateStruct = true,
          generateMap = false)
        ParquetGenerator.makeParquetFile(random, spark, filename, 100, options)
      }
      withSQLConf(
        CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "false",
        CometConf.COMET_SPARK_TO_ARROW_ENABLED.key -> "true",
        CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.key -> "true") {
        val table = spark.read.parquet(filename)
        table.createOrReplaceTempView("t1")
        // test with array of each column
        for (field <- table.schema.fields) {
          val fieldName = field.name
          sql(s"SELECT array($fieldName, $fieldName) as a, $fieldName as b FROM t1")
            .createOrReplaceTempView("t2")
          val df = sql("SELECT array_remove(a, b) FROM t2")
          checkSparkAnswer(df)
        }
      }
    }
  }

  test("array_remove - fallback for unsupported type struct") {
    withTempDir { dir =>
      val path = new Path(dir.toURI.toString, "test.parquet")
      makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = true, 100)
      spark.read.parquet(path.toString).createOrReplaceTempView("t1")
      sql("SELECT array(struct(_1, _2)) as a, struct(_1, _2) as b FROM t1")
        .createOrReplaceTempView("t2")
      val expectedFallbackReasons = HashSet(
        "data type not supported: ArrayType(StructType(StructField(_1,BooleanType,true),StructField(_2,ByteType,true)),false)")
      // note that checkExtended is disabled here due to an unrelated issue
      // https://github.com/apache/datafusion-comet/issues/1313
      checkSparkAnswerAndCompareExplainPlan(
        sql("SELECT array_remove(a, b) FROM t2"),
        expectedFallbackReasons,
        checkExplainString = false)
    }
  }

  test("array_append") {
    withSQLConf(CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key -> "true") {
      Seq(true, false).foreach { dictionaryEnabled =>
        withTempDir { dir =>
          val path = new Path(dir.toURI.toString, "test.parquet")
          makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = dictionaryEnabled, 10000)
          spark.read.parquet(path.toString).createOrReplaceTempView("t1");
          checkSparkAnswerAndOperator(spark.sql("Select array_append(array(_1),false) from t1"))
          checkSparkAnswerAndOperator(
            spark.sql("SELECT array_append(array(_2, _3, _4), 4) FROM t1"))
          checkSparkAnswerAndOperator(
            spark.sql("SELECT array_append(array(_2, _3, _4), null) FROM t1"));
          checkSparkAnswerAndOperator(
            spark.sql("SELECT array_append(array(_6, _7), CAST(6.5 AS DOUBLE)) FROM t1"));
          checkSparkAnswerAndOperator(
            spark.sql("SELECT array_append(array(_8), 'test') FROM t1"));
          checkSparkAnswerAndOperator(spark.sql("SELECT array_append(array(_19), _19) FROM t1"));
          checkSparkAnswerAndOperator(
            spark.sql("SELECT array_append((CASE WHEN _2 =_3 THEN array(_4) END), _4) FROM t1"));
        }
      }
    }
  }

  test("array_prepend") {
    assume(isSpark35Plus) // in Spark 3.5 array_prepend is implemented via array_insert
    withSQLConf(CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key -> "true") {
      Seq(true, false).foreach { dictionaryEnabled =>
        withTempDir { dir =>
          val path = new Path(dir.toURI.toString, "test.parquet")
          makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = dictionaryEnabled, 10000)
          spark.read.parquet(path.toString).createOrReplaceTempView("t1");
          checkSparkAnswerAndOperator(spark.sql("Select array_prepend(array(_1),false) from t1"))
          checkSparkAnswerAndOperator(
            spark.sql("SELECT array_prepend(array(_2, _3, _4), 4) FROM t1"))
          checkSparkAnswerAndOperator(
            spark.sql("SELECT array_prepend(array(_2, _3, _4), null) FROM t1"));
          checkSparkAnswerAndOperator(
            spark.sql("SELECT array_prepend(array(_6, _7), CAST(6.5 AS DOUBLE)) FROM t1"));
          checkSparkAnswerAndOperator(
            spark.sql("SELECT array_prepend(array(_8), 'test') FROM t1"));
          checkSparkAnswerAndOperator(spark.sql("SELECT array_prepend(array(_19), _19) FROM t1"));
          checkSparkAnswerAndOperator(
            spark.sql("SELECT array_prepend((CASE WHEN _2 =_3 THEN array(_4) END), _4) FROM t1"));
        }
      }
    }
  }

  test("ArrayInsert") {
    withSQLConf(CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key -> "true") {
      Seq(true, false).foreach(dictionaryEnabled =>
        withTempDir { dir =>
          val path = new Path(dir.toURI.toString, "test.parquet")
          makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled, 10000)
          val df = spark.read
            .parquet(path.toString)
            .withColumn("arr", array(col("_4"), lit(null), col("_4")))
            .withColumn("arrInsertResult", expr("array_insert(arr, 1, 1)"))
            .withColumn("arrInsertNegativeIndexResult", expr("array_insert(arr, -1, 1)"))
            .withColumn("arrPosGreaterThanSize", expr("array_insert(arr, 8, 1)"))
            .withColumn("arrNegPosGreaterThanSize", expr("array_insert(arr, -8, 1)"))
            .withColumn("arrInsertNone", expr("array_insert(arr, 1, null)"))
          checkSparkAnswerAndOperator(df.select("arrInsertResult"))
          checkSparkAnswerAndOperator(df.select("arrInsertNegativeIndexResult"))
          checkSparkAnswerAndOperator(df.select("arrPosGreaterThanSize"))
          checkSparkAnswerAndOperator(df.select("arrNegPosGreaterThanSize"))
          checkSparkAnswerAndOperator(df.select("arrInsertNone"))
        })
    }
  }

  test("ArrayInsertUnsupportedArgs") {
    // This test checks that the else branch in ArrayInsert
    // mapping to the comet is valid and fallback to spark is working fine.
    withSQLConf(CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key -> "true") {
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test.parquet")
        makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = false, 10000)
        val df = spark.read
          .parquet(path.toString)
          .withColumn("arr", array(col("_4"), lit(null), col("_4")))
          .withColumn("idx", udf((_: Int) => 1).apply(col("_4")))
          .withColumn("arrUnsupportedArgs", expr("array_insert(arr, idx, 1)"))
        checkSparkAnswer(df.select("arrUnsupportedArgs"))
      }
    }
  }

  test("array_contains - int values") {
    withSQLConf(CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key -> "true") {
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test.parquet")
        makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = false, n = 10000)
        spark.read.parquet(path.toString).createOrReplaceTempView("t1");
        checkSparkAnswerAndOperator(
          spark.sql("SELECT array_contains(array(_2, _3, _4), _2) FROM t1"))
        checkSparkAnswerAndOperator(
          spark.sql("SELECT array_contains((CASE WHEN _2 =_3 THEN array(_4) END), _4) FROM t1"));
      }
    }
  }

  test("array_contains - test all types (native Parquet reader)") {
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
          DataGenOptions(
            allowNull = true,
            generateNegativeZero = true,
            generateArray = false,
            generateStruct = false,
            generateMap = false))
      }
      val table = spark.read.parquet(filename)
      table.createOrReplaceTempView("t1")
      for (field <- table.schema.fields) {
        val fieldName = field.name
        val typeName = field.dataType.typeName
        sql(s"SELECT array($fieldName, $fieldName) as a, $fieldName as b FROM t1")
          .createOrReplaceTempView("t2")
        checkSparkAnswerAndOperator(sql("SELECT array_contains(a, b) FROM t2"))
        checkSparkAnswerAndOperator(
          sql(s"SELECT array_contains(a, cast(null as $typeName)) FROM t2"))
        checkSparkAnswerAndOperator(
          sql(s"SELECT array_contains(cast(null as array<$typeName>), b) FROM t2"))
        checkSparkAnswerAndOperator(sql(
          s"SELECT array_contains(cast(array() as array<$typeName>), cast(null as $typeName)) FROM t2"))
        checkSparkAnswerAndOperator(sql(s"SELECT array_contains(array(), 1) FROM t2"))
      }
    }
  }

  test("array_contains - test all types (convert from Parquet)") {
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
          DataGenOptions(
            allowNull = true,
            generateNegativeZero = true,
            generateArray = true,
            generateStruct = true,
            generateMap = false))
      }
      withSQLConf(
        CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "false",
        CometConf.COMET_SPARK_TO_ARROW_ENABLED.key -> "true",
        CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.key -> "true") {
        val table = spark.read.parquet(filename)
        table.createOrReplaceTempView("t1")
        for (field <- table.schema.fields) {
          val fieldName = field.name
          val typeName = field.dataType.typeName
          sql(s"SELECT array($fieldName, $fieldName) as a, $fieldName as b FROM t1")
            .createOrReplaceTempView("t2")
          checkSparkAnswer(sql("SELECT array_contains(a, b) FROM t2"))
        }
      }
    }
  }

  test("array_distinct") {
    withSQLConf(CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key -> "true") {
      Seq(true, false).foreach { dictionaryEnabled =>
        withTempDir { dir =>
          val path = new Path(dir.toURI.toString, "test.parquet")
          makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled, n = 10000)
          spark.read.parquet(path.toString).createOrReplaceTempView("t1")
          // The result needs to be in ascending order for checkSparkAnswerAndOperator to pass
          // because datafusion array_distinct sorts the elements and then removes the duplicates
          checkSparkAnswerAndOperator(
            spark.sql("SELECT array_distinct(array(_2, _2, _3, _4, _4)) FROM t1"))
          checkSparkAnswerAndOperator(
            spark.sql("SELECT array_distinct((CASE WHEN _2 =_3 THEN array(_4) END)) FROM t1"))
          checkSparkAnswerAndOperator(spark.sql(
            "SELECT array_distinct((CASE WHEN _2 =_3 THEN array(_2, _2, _4, _4, _5) END)) FROM t1"))
          // NULL needs to be the first element for checkSparkAnswerAndOperator to pass because
          // datafusion array_distinct sorts the elements and then removes the duplicates
          checkSparkAnswerAndOperator(
            spark.sql(
              "SELECT array_distinct(array(CAST(NULL AS INT), _2, _2, _3, _4, _4)) FROM t1"))
          checkSparkAnswerAndOperator(spark.sql(
            "SELECT array_distinct(array(CAST(NULL AS INT), CAST(NULL AS INT), _2, _2, _3, _4, _4)) FROM t1"))
        }
      }
    }
  }

  test("array_union") {
    withSQLConf(CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key -> "true") {
      Seq(true, false).foreach { dictionaryEnabled =>
        withTempDir { dir =>
          val path = new Path(dir.toURI.toString, "test.parquet")
          makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled, n = 10000)
          spark.read.parquet(path.toString).createOrReplaceTempView("t1")
          checkSparkAnswerAndOperator(
            spark.sql("SELECT array_union(array(_2, _3, _4), array(_3, _4)) FROM t1"))
          checkSparkAnswerAndOperator(sql("SELECT array_union(array(_18), array(_19)) from t1"))
          checkSparkAnswerAndOperator(spark.sql(
            "SELECT array_union(array(CAST(NULL AS INT), _2, _3, _4), array(CAST(NULL AS INT), _2, _3)) FROM t1"))
          checkSparkAnswerAndOperator(spark.sql(
            "SELECT array_union(array(CAST(NULL AS INT), CAST(NULL AS INT), _2, _3, _4), array(CAST(NULL AS INT), CAST(NULL AS INT), _2, _3)) FROM t1"))
        }
      }
    }
  }

  test("array_max") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test.parquet")
        makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled, n = 10000)
        spark.read.parquet(path.toString).createOrReplaceTempView("t1");
        checkSparkAnswerAndOperator(spark.sql("SELECT array_max(array(_2, _3, _4)) FROM t1"))
        checkSparkAnswerAndOperator(
          spark.sql("SELECT array_max((CASE WHEN _2 =_3 THEN array(_4) END)) FROM t1"))
        checkSparkAnswerAndOperator(
          spark.sql("SELECT array_max((CASE WHEN _2 =_3 THEN array(_2, _4) END)) FROM t1"))
        checkSparkAnswerAndOperator(
          spark.sql("SELECT array_max(array(CAST(NULL AS INT), CAST(NULL AS INT))) FROM t1"))
        checkSparkAnswerAndOperator(
          spark.sql("SELECT array_max(array(_2, CAST(NULL AS INT))) FROM t1"))
        checkSparkAnswerAndOperator(spark.sql("SELECT array_max(array()) FROM t1"))
        checkSparkAnswerAndOperator(
          spark.sql(
            "SELECT array_max(array(double('-Infinity'), 0.0, double('Infinity'))) FROM t1"))
      }
    }
  }

  test("array_intersect") {
    // https://github.com/apache/datafusion-comet/issues/1441
    assume(!usingDataSourceExec)
    withSQLConf(CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key -> "true") {

      Seq(true, false).foreach { dictionaryEnabled =>
        withTempDir { dir =>
          val path = new Path(dir.toURI.toString, "test.parquet")
          makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled, 10000)
          spark.read.parquet(path.toString).createOrReplaceTempView("t1")
          checkSparkAnswerAndOperator(
            sql("SELECT array_intersect(array(_2, _3, _4), array(_3, _4)) from t1"))
          checkSparkAnswerAndOperator(
            sql("SELECT array_intersect(array(_2 * -1), array(_9, _10)) from t1"))
          checkSparkAnswerAndOperator(
            sql("SELECT array_intersect(array(_18), array(_19)) from t1"))
        }
      }
    }
  }

  test("array_join") {
    withSQLConf(CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key -> "true") {
      Seq(true, false).foreach { dictionaryEnabled =>
        withTempDir { dir =>
          val path = new Path(dir.toURI.toString, "test.parquet")
          makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled, 10000)
          spark.read.parquet(path.toString).createOrReplaceTempView("t1")
          checkSparkAnswerAndOperator(sql(
            "SELECT array_join(array(cast(_1 as string), cast(_2 as string), cast(_6 as string)), ' @ ') from t1"))
          checkSparkAnswerAndOperator(sql(
            "SELECT array_join(array(cast(_1 as string), cast(_2 as string), cast(_6 as string)), ' @ ', ' +++ ') from t1"))
          checkSparkAnswerAndOperator(sql(
            "SELECT array_join(array('hello', 'world', cast(_2 as string)), ' ') from t1 where _2 is not null"))
          checkSparkAnswerAndOperator(
            sql(
              "SELECT array_join(array('hello', '-', 'world', cast(_2 as string)), ' ') from t1"))
        }
      }
    }
  }

  test("arrays_overlap") {
    withSQLConf(CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key -> "true") {
      Seq(true, false).foreach { dictionaryEnabled =>
        withTempDir { dir =>
          val path = new Path(dir.toURI.toString, "test.parquet")
          makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled, 10000)
          spark.read.parquet(path.toString).createOrReplaceTempView("t1")
          checkSparkAnswerAndOperator(sql(
            "SELECT arrays_overlap(array(_2, _3, _4), array(_3, _4)) from t1 where _2 is not null"))
          checkSparkAnswerAndOperator(sql(
            "SELECT arrays_overlap(array('a', null, cast(_1 as string)), array('b', cast(_1 as string), cast(_2 as string))) from t1 where _1 is not null"))
          checkSparkAnswerAndOperator(sql(
            "SELECT arrays_overlap(array('a', null), array('b', null)) from t1 where _1 is not null"))
          checkSparkAnswerAndOperator(spark.sql(
            "SELECT arrays_overlap((CASE WHEN _2 =_3 THEN array(_6, _7) END), array(_6, _7)) FROM t1"));
        }
      }
    }
  }

  test("array_compact") {
    // TODO fix for Spark 4.0.0
    assume(!isSpark40Plus)
    withSQLConf(CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key -> "true") {
      Seq(true, false).foreach { dictionaryEnabled =>
        withTempDir { dir =>
          val path = new Path(dir.toURI.toString, "test.parquet")
          makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = dictionaryEnabled, n = 10000)
          spark.read.parquet(path.toString).createOrReplaceTempView("t1")

          checkSparkAnswerAndOperator(
            sql("SELECT array_compact(array(_2)) FROM t1 WHERE _2 IS NULL"))
          checkSparkAnswerAndOperator(
            sql("SELECT array_compact(array(_2)) FROM t1 WHERE _2 IS NOT NULL"))
          checkSparkAnswerAndOperator(
            sql("SELECT array_compact(array(_2, _3, null)) FROM t1 WHERE _2 IS NOT NULL"))
        }
      }
    }
  }

  test("array_except - basic test (only integer values)") {
    withSQLConf(CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key -> "true") {
      Seq(true, false).foreach { dictionaryEnabled =>
        withTempDir { dir =>
          val path = new Path(dir.toURI.toString, "test.parquet")
          makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled, 10000)
          spark.read.parquet(path.toString).createOrReplaceTempView("t1")

          checkSparkAnswerAndOperator(
            sql("SELECT array_except(array(_2, _3, _4), array(_3, _4)) from t1"))
          checkSparkAnswerAndOperator(sql("SELECT array_except(array(_18), array(_19)) from t1"))
          checkSparkAnswerAndOperator(
            spark.sql(
              "SELECT array_except(array(_2, _2, _4), array(_4)) FROM t1 WHERE _2 IS NOT NULL"))
        }
      }
    }
  }

  test("array_except - test all types (native Parquet reader)") {
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
          DataGenOptions(
            allowNull = true,
            generateNegativeZero = true,
            generateArray = false,
            generateStruct = false,
            generateMap = false))
      }
      withSQLConf(CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key -> "true") {
        val table = spark.read.parquet(filename)
        table.createOrReplaceTempView("t1")
        // test with array of each column
        val fields =
          table.schema.fields.filter(field => CometArrayExcept.isTypeSupported(field.dataType))
        for (field <- fields) {
          val fieldName = field.name
          val typeName = field.dataType.typeName
          sql(
            s"SELECT cast(array($fieldName, $fieldName) as array<$typeName>) as a, cast(array($fieldName) as array<$typeName>) as b FROM t1")
            .createOrReplaceTempView("t2")
          val df = sql("SELECT array_except(a, b) FROM t2")
          checkSparkAnswerAndOperator(df)
        }
      }
    }
  }

  test("array_except - test all types (convert from Parquet)") {
    withTempDir { dir =>
      val path = new Path(dir.toURI.toString, "test.parquet")
      val filename = path.toString
      val random = new Random(42)
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        val options = DataGenOptions(
          allowNull = true,
          generateNegativeZero = true,
          generateArray = true,
          generateStruct = true,
          generateMap = false)
        ParquetGenerator.makeParquetFile(random, spark, filename, 100, options)
      }
      withSQLConf(
        CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "false",
        CometConf.COMET_SPARK_TO_ARROW_ENABLED.key -> "true",
        CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.key -> "true",
        CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key -> "true") {
        val table = spark.read.parquet(filename)
        table.createOrReplaceTempView("t1")
        // test with array of each column
        val fields =
          table.schema.fields.filter(field => CometArrayExcept.isTypeSupported(field.dataType))
        for (field <- fields) {
          val fieldName = field.name
          sql(s"SELECT array($fieldName, $fieldName) as a, array($fieldName) as b FROM t1")
            .createOrReplaceTempView("t2")
          val df = sql("SELECT array_except(a, b) FROM t2")
          checkSparkAnswer(df)
        }
      }
    }
  }

  test("array_repeat") {
    withSQLConf(
      CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key -> "true",
      CometConf.COMET_EXPLAIN_FALLBACK_ENABLED.key -> "true") {
      Seq(true, false).foreach { dictionaryEnabled =>
        withTempDir { dir =>
          val path = new Path(dir.toURI.toString, "test.parquet")
          makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled, 100)
          spark.read.parquet(path.toString).createOrReplaceTempView("t1")

          checkSparkAnswerAndOperator(sql("SELECT array_repeat(_4, null) from t1"))
          checkSparkAnswerAndOperator(sql("SELECT array_repeat(_4, 0) from t1"))
          checkSparkAnswerAndOperator(
            sql("SELECT array_repeat(_2, 5) from t1 where _2 is not null"))
          checkSparkAnswerAndOperator(sql("SELECT array_repeat(_2, 5) from t1 where _2 is null"))
          checkSparkAnswerAndOperator(
            sql("SELECT array_repeat(_3, _4) from t1 where _3 is not null"))
          checkSparkAnswerAndOperator(sql("SELECT array_repeat(cast(_3 as string), 2) from t1"))
          checkSparkAnswerAndOperator(sql("SELECT array_repeat(array(_2, _3, _4), 2) from t1"))
        }
      }
    }
  }
}
