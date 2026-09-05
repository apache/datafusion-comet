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
import org.apache.spark.sql.catalyst.expressions.{ArrayAppend, ArrayExcept, ArrayInsert, ArrayIntersect, ArrayJoin, ArrayRepeat}
import org.apache.spark.sql.catalyst.expressions.{ArrayContains, ArrayRemove}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Cast, CreateArray, ElementAt, Literal, MonotonicallyIncreasingID}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, StringType}

import org.apache.comet.CometSparkSessionExtensions.{isSpark35Plus, isSpark40Plus}
import org.apache.comet.DataTypeSupport.isComplexType
import org.apache.comet.serde.{CometArrayExcept, CometArrayJoin, CometArrayRemove, CometArrayReverse, CometFlatten, Compatible, ExprOuterClass, Incompatible}
import org.apache.comet.testing.{DataGenOptions, ParquetGenerator, SchemaGenOptions}

class CometArrayExpressionSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  test("array_remove - integer") {
    withSQLConf(CometConf.getExprAllowIncompatConfigKey(classOf[ArrayRemove]) -> "true") {
      Seq(true, false).foreach { dictionaryEnabled =>
        withTempView("t1") {
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
    }
  }

  test("array_remove - test all types (native Parquet reader)") {
    withSQLConf(CometConf.getExprAllowIncompatConfigKey(classOf[ArrayRemove]) -> "true") {
      withTempDir { dir =>
        withTempView("t1") {
          val path = new Path(dir.toURI.toString, "test.parquet")
          val filename = path.toString
          val random = new Random(42)
          withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
            ParquetGenerator.makeParquetFile(
              random,
              spark,
              filename,
              100,
              SchemaGenOptions(
                generateArray = false,
                generateStruct = false,
                generateMap = false),
              DataGenOptions(allowNull = true, generateNegativeZero = true))
          }
          val table = spark.read.parquet(filename)
          table.createOrReplaceTempView("t1")
          // test with array of each column
          val fieldNames =
            table.schema.fields
              .filter(field => CometArrayRemove.isTypeSupported(field.dataType))
              .map(_.name)
          for (fieldName <- fieldNames) {
            sql(s"SELECT array($fieldName, $fieldName) as a, $fieldName as b FROM t1")
              .createOrReplaceTempView("t2")
            val df = sql("SELECT array_remove(a, b) FROM t2")
            checkSparkAnswerAndOperator(df)
          }
        }
      }
    }
  }

  test("array_remove - test all types (convert from Parquet)") {
    withTempDir { dir =>
      withTempView("t1") {
        val path = new Path(dir.toURI.toString, "test.parquet")
        val filename = path.toString
        val random = new Random(42)
        withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
          ParquetGenerator.makeParquetFile(
            random,
            spark,
            filename,
            100,
            SchemaGenOptions(generateArray = true, generateStruct = true, generateMap = false),
            DataGenOptions(allowNull = true, generateNegativeZero = true))
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
  }

  test("array_remove - fallback for unsupported type struct") {
    withTempDir { dir =>
      withTempView("t1", "t2") {
        val path = new Path(dir.toURI.toString, "test.parquet")
        makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = true, 100)
        spark.read.parquet(path.toString).createOrReplaceTempView("t1")
        sql("SELECT array(struct(_1, _2)) as a, struct(_1, _2) as b FROM t1")
          .createOrReplaceTempView("t2")
        val expectedFallbackReason =
          "data type not supported"
        checkSparkAnswerAndFallbackReason(
          sql("SELECT array_remove(a, b) FROM t2"),
          expectedFallbackReason)
      }
    }
  }

  test("array_append") {
    val incompatKey = if (isSpark40Plus) {
      classOf[ArrayInsert]
    } else {
      classOf[ArrayAppend]
    }
    withSQLConf(CometConf.getExprAllowIncompatConfigKey(incompatKey) -> "true") {
      Seq(true, false).foreach { dictionaryEnabled =>
        withTempDir { dir =>
          withTempView("t1") {
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
            checkSparkAnswerAndOperator(
              spark.sql("SELECT array_append(array(_19), _19) FROM t1"));
            checkSparkAnswerAndOperator(
              spark.sql(
                "SELECT array_append((CASE WHEN _2 =_3 THEN array(_4) END), _4) FROM t1"));
          }
        }
      }
    }
  }

  test("array_prepend") {
    assume(isSpark35Plus) // in Spark 3.5 array_prepend is implemented via array_insert
    withSQLConf(CometConf.getExprAllowIncompatConfigKey(classOf[ArrayInsert]) -> "true") {
      Seq(true, false).foreach { dictionaryEnabled =>
        withTempDir { dir =>
          withTempView("t1") {
            val path = new Path(dir.toURI.toString, "test.parquet")
            makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = dictionaryEnabled, 10000)
            spark.read.parquet(path.toString).createOrReplaceTempView("t1");
            checkSparkAnswerAndOperator(
              spark.sql("Select array_prepend(array(_1),false) from t1"))
            checkSparkAnswerAndOperator(
              spark.sql("SELECT array_prepend(array(_2, _3, _4), 4) FROM t1"))
            checkSparkAnswerAndOperator(
              spark.sql("SELECT array_prepend(array(_2, _3, _4), null) FROM t1"));
            checkSparkAnswerAndOperator(
              spark.sql("SELECT array_prepend(array(_6, _7), CAST(6.5 AS DOUBLE)) FROM t1"));
            checkSparkAnswerAndOperator(
              spark.sql("SELECT array_prepend(array(_8), 'test') FROM t1"));
            checkSparkAnswerAndOperator(
              spark.sql("SELECT array_prepend(array(_19), _19) FROM t1"));
            checkSparkAnswerAndOperator(
              spark.sql(
                "SELECT array_prepend((CASE WHEN _2 =_3 THEN array(_4) END), _4) FROM t1"));
          }
        }
      }
    }
  }

  test("ArrayInsert") {
    withSQLConf(CometConf.getExprAllowIncompatConfigKey(classOf[ArrayInsert]) -> "true") {
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
            .withColumn("arrPosIsNull", expr("array_insert(arr, cast(null as int), 1)"))
            .withColumn("arrNegPosGreaterThanSize", expr("array_insert(arr, -8, 1)"))
            .withColumn("arrInsertNone", expr("array_insert(arr, 1, null)"))
          checkSparkAnswerAndOperator(df.select("arrInsertResult"))
          checkSparkAnswerAndOperator(df.select("arrInsertNegativeIndexResult"))
          checkSparkAnswerAndOperator(df.select("arrPosGreaterThanSize"))
          checkSparkAnswerAndOperator(df.select("arrPosIsNull"))
          checkSparkAnswerAndOperator(df.select("arrNegPosGreaterThanSize"))
          checkSparkAnswerAndOperator(df.select("arrInsertNone"))
        })
    }
  }

  test("ArrayInsertUnsupportedArgs") {
    // This test checks that the else branch in ArrayInsert
    // mapping to the comet is valid and fallback to spark is working fine.
    // Disable UDF codegen dispatch so the UDF-derived position remains
    // non-convertible and forces the ArrayInsert fallback path.
    withSQLConf(
      CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "false",
      CometConf.getExprAllowIncompatConfigKey(classOf[ArrayInsert]) -> "true") {
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "test.parquet")
        makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = false, 10000)
        val df = spark.read
          .parquet(path.toString)
          .withColumn("arr", array(col("_4"), lit(null), col("_4")))
          .withColumn("idx", org.apache.spark.sql.functions.udf((_: Int) => 1).apply(col("_4")))
          .withColumn("arrUnsupportedArgs", expr("array_insert(arr, idx, 1)"))
        checkSparkAnswerAndFallbackReasons(
          df.select("arrUnsupportedArgs"),
          Set("expression has no native path", "unsupported arguments for ArrayInsert"))
      }
    }
  }

  test("array_contains - int values") {
    withSQLConf(CometConf.getExprAllowIncompatConfigKey(classOf[ArrayContains]) -> "true") {
      withTempDir { dir =>
        withTempView("t1") {
          val path = new Path(dir.toURI.toString, "test.parquet")
          makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = false, n = 10000)
          spark.read.parquet(path.toString).createOrReplaceTempView("t1");
          checkSparkAnswerAndOperator(
            spark.sql("SELECT array_contains(array(_2, _3, _4), _2) FROM t1"))
          checkSparkAnswerAndOperator(
            spark.sql(
              "SELECT array_contains((CASE WHEN _2 =_3 THEN array(_4) END), _4) FROM t1"));
        }
      }
    }
  }

  test("array_contains - test all types (native Parquet reader)") {
    withSQLConf(CometConf.getExprAllowIncompatConfigKey(classOf[ArrayContains]) -> "true") {
      withTempDir { dir =>
        withTempView("t1", "t2", "t3") {
          val path = new Path(dir.toURI.toString, "test.parquet")
          val filename = path.toString
          val random = new Random(42)
          withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
            ParquetGenerator.makeParquetFile(
              random,
              spark,
              filename,
              100,
              SchemaGenOptions(generateArray = true, generateStruct = true, generateMap = false),
              DataGenOptions(allowNull = true, generateNegativeZero = true))
          }
          val table = spark.read.parquet(filename)
          table.createOrReplaceTempView("t1")
          val complexTypeFields =
            table.schema.fields.filter(field => isComplexType(field.dataType))
          val primitiveTypeFields =
            table.schema.fields.filterNot(field => isComplexType(field.dataType))
          for (field <- primitiveTypeFields) {
            val fieldName = field.name
            val typeName = field.dataType.typeName
            sql(s"SELECT array($fieldName, $fieldName) as a, $fieldName as b FROM t1")
              .createOrReplaceTempView("t2")
            checkSparkAnswerAndOperator(sql("SELECT array_contains(a, b) FROM t2"))
            checkSparkAnswerAndOperator(
              sql(s"SELECT array_contains(a, cast(null as $typeName)) FROM t2"))
          }
          for (field <- complexTypeFields) {
            val fieldName = field.name
            sql(s"SELECT array($fieldName, $fieldName) as a, $fieldName as b FROM t1")
              .createOrReplaceTempView("t3")
            checkSparkAnswer(sql("SELECT array_contains(a, b) FROM t3"))
          }
        }
      }
    }
  }

  test("array_contains - array literals") {
    withTempDir { dir =>
      withTempView("t2") {
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
        val table = spark.read.parquet(filename)
        table.createOrReplaceTempView("t2")
        for (field <- table.schema.fields) {
          val typeName = field.dataType.typeName
          checkSparkAnswerAndOperator(sql(
            s"SELECT array_contains(cast(null as array<$typeName>), cast(null as $typeName)) FROM t2"))
        }
        checkSparkAnswerAndOperator(sql("SELECT array_contains(array(), 1) FROM t2"))
      }
    }
  }

  test("array_contains - NULL array returns NULL") {
    // Test that array_contains returns NULL when the array argument is NULL
    // This matches Spark's SQL three-valued logic behavior
    withTempDir { dir =>
      withTempView("t1") {
        val path = new Path(dir.toURI.toString, "test.parquet")
        makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = false, n = 100)
        spark.read.parquet(path.toString).createOrReplaceTempView("t1")

        // Test NULL array with non-null value
        checkSparkAnswerAndOperator(
          sql("SELECT array_contains(cast(null as array<int>), 1) FROM t1"))
        checkSparkAnswerAndOperator(
          sql("SELECT array_contains(cast(null as array<string>), 'test') FROM t1"))
        checkSparkAnswerAndOperator(
          sql("SELECT array_contains(cast(null as array<double>), 1.5) FROM t1"))

        // Test NULL array with NULL value
        checkSparkAnswerAndOperator(
          sql("SELECT array_contains(cast(null as array<int>), cast(null as int)) FROM t1"))

        // Test NULL array with column value
        checkSparkAnswerAndOperator(
          sql("SELECT array_contains(cast(null as array<int>), _2) FROM t1"))

        // Test non-null array with values (to ensure fix doesn't break normal operation)
        checkSparkAnswerAndOperator(sql("SELECT array_contains(array(1, 2, 3), 2) FROM t1"))
        checkSparkAnswerAndOperator(sql("SELECT array_contains(array(1, 2, 3), 5) FROM t1"))
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
          SchemaGenOptions(generateArray = true, generateStruct = true, generateMap = false),
          DataGenOptions(allowNull = true, generateNegativeZero = true))
      }
      withSQLConf(
        CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "false",
        CometConf.COMET_SPARK_TO_ARROW_ENABLED.key -> "true",
        CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.key -> "true") {
        withTempView("t1", "t2") {
          val table = spark.read.parquet(filename)
          table.createOrReplaceTempView("t1")
          for (field <- table.schema.fields) {
            val fieldName = field.name
            sql(s"SELECT array($fieldName, $fieldName) as a, $fieldName as b FROM t1")
              .createOrReplaceTempView("t2")
            checkSparkAnswer(sql("SELECT array_contains(a, b) FROM t2"))
          }
        }
      }
    }
  }

  test("array_distinct") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        withTempView("t1") {
          val path = new Path(dir.toURI.toString, "test.parquet")
          makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled, n = 10000)
          spark.read.parquet(path.toString).createOrReplaceTempView("t1")
          checkSparkAnswerAndOperator(
            spark.sql("SELECT array_distinct(array(_3, _2, _4, _2, _4)) FROM t1"))
          checkSparkAnswerAndOperator(
            spark.sql("SELECT array_distinct((CASE WHEN _2 =_3 THEN array(_4) END)) FROM t1"))
          checkSparkAnswerAndOperator(spark.sql(
            "SELECT array_distinct((CASE WHEN _2 =_3 THEN array(_2, _2, _4, _4, _5) END)) FROM t1"))
          checkSparkAnswerAndOperator(
            spark.sql(
              "SELECT array_distinct(array(_2, _2, CAST(NULL AS INT), _3, _4, _4)) FROM t1"))
          checkSparkAnswerAndOperator(spark.sql(
            "SELECT array_distinct(array(_2, _2, CAST(NULL AS INT), CAST(NULL AS INT), _3, _4, _4)) FROM t1"))
        }
      }
    }
  }

  test("array_union") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        withTempView("t1") {
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
        withTempView("t1") {
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
  }

  test("array_min") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        withTempView("t1") {
          val path = new Path(dir.toURI.toString, "test.parquet")
          makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled, n = 10000)
          spark.read.parquet(path.toString).createOrReplaceTempView("t1");
          checkSparkAnswerAndOperator(spark.sql("SELECT array_min(array(_2, _3, _4)) FROM t1"))
          checkSparkAnswerAndOperator(
            spark.sql("SELECT array_min((CASE WHEN _2 =_3 THEN array(_4) END)) FROM t1"))
          checkSparkAnswerAndOperator(
            spark.sql("SELECT array_min((CASE WHEN _2 =_3 THEN array(_2, _4) END)) FROM t1"))
          checkSparkAnswerAndOperator(
            spark.sql("SELECT array_min(array(CAST(NULL AS INT), CAST(NULL AS INT))) FROM t1"))
          checkSparkAnswerAndOperator(
            spark.sql("SELECT array_min(array(_2, CAST(NULL AS INT))) FROM t1"))
          checkSparkAnswerAndOperator(spark.sql("SELECT array_min(array()) FROM t1"))
          checkSparkAnswerAndOperator(
            spark.sql(
              "SELECT array_min(array(double('-Infinity'), 0.0, double('Infinity'))) FROM t1"))
        }
      }
    }
  }

  test("array_intersect") {
    withSQLConf(CometConf.getExprAllowIncompatConfigKey(classOf[ArrayIntersect]) -> "true") {
      Seq(true, false).foreach { dictionaryEnabled =>
        withTempDir { dir =>
          withTempView("t1") {
            val path = new Path(dir.toURI.toString, "test.parquet")
            makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled, 10000)
            spark.read.parquet(path.toString).createOrReplaceTempView("t1")
            checkSparkAnswerAndOperator(
              sql("SELECT array_intersect(array(_2, _3, _4), array(_3, _4)) from t1"))
            checkSparkAnswerAndOperator(
              sql("SELECT array_intersect(array(_4 * -1), array(_5)) from t1"))
            checkSparkAnswerAndOperator(
              sql("SELECT array_intersect(array(_18), array(_19)) from t1"))
          }
        }
      }
    }
  }

  // No allowIncompatible opt-in: array_join runs natively by default now.
  test("array_join") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        withTempView("t1") {
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
          // column delimiter and nullable column replacement: the guarded native shape
          checkSparkAnswerAndOperator(
            sql("SELECT array_join(array('a', cast(_2 as string), 'b'), _8, _8) from t1"))
          // a literal NULL replacement folds to Literal(null, StringType), which is
          // order-insensitive, so this takes the native path rather than the dispatcher. The
          // sql-tests fixtures cannot reach this shape because they disable ConstantFolding.
          checkSparkAnswerAndOperator(
            sql("SELECT array_join(array('a', cast(_2 as string), 'b'), ',', NULL) from t1"))
        }
      }
    }
  }

  // Result assertions cannot tell native from the dispatcher: an Incompatible verdict runs
  // Spark's own doGenCode and matches. Pin the verdict itself.
  test("array_join support level pins the native path") {
    val nullableArray = AttributeReference("arr", ArrayType(StringType), nullable = true)()
    val nullableStr = AttributeReference("s", StringType, nullable = true)()
    val delims = AttributeReference("delims", ArrayType(StringType), nullable = true)()

    // literals and column reads stay native
    Seq(
      ArrayJoin(nullableArray, Literal(","), None),
      ArrayJoin(nullableArray, Literal(","), Some(Literal("X"))),
      ArrayJoin(nullableArray, nullableStr, Some(nullableStr)),
      ArrayJoin(nullableArray, Literal(","), Some(Literal.create(null, StringType))),
      // the array is unrestricted: it is evaluated on every path
      ArrayJoin(ElementAt(delims, Literal(1)), Literal(","), None)).foreach { expr =>
      assert(
        CometArrayJoin.getSupportLevel(expr).isInstanceOf[Compatible],
        s"expected Compatible for $expr")
    }

    // Anything that can throw or carry state goes to the dispatcher instead.
    val throwingDelimiter = ElementAt(delims, Literal(0))
    val foldableThrowingDelimiter = ElementAt(CreateArray(Seq(Literal(","))), Literal(0))
    val nonDeterministicReplacement =
      Cast(MonotonicallyIncreasingID(), StringType)
    Seq(
      ArrayJoin(nullableArray, throwingDelimiter, None),
      ArrayJoin(nullableArray, throwingDelimiter, Some(nullableStr)),
      ArrayJoin(nullableArray, foldableThrowingDelimiter, None),
      ArrayJoin(nullableArray, Literal(","), Some(nonDeterministicReplacement))).foreach { expr =>
      assert(
        CometArrayJoin.getSupportLevel(expr).isInstanceOf[Incompatible],
        s"expected Incompatible for $expr")
    }
  }

  test("array_join guards only a nullable replacement") {
    val nullableArray = AttributeReference("arr", ArrayType(StringType), nullable = true)()
    val nullableStr = AttributeReference("s", StringType, nullable = true)()
    val inputs = Seq(nullableArray, nullableStr)

    def convert(expr: ArrayJoin): Option[ExprOuterClass.Expr] =
      CometArrayJoin.convert(expr, inputs, binding = false)

    // No replacement, or a non-nullable one: unchanged plan.
    val noReplacement = convert(ArrayJoin(nullableArray, Literal(","), None))
    assert(noReplacement.isDefined && !noReplacement.get.hasIf)
    val literalReplacement = convert(ArrayJoin(nullableArray, Literal(","), Some(Literal("X"))))
    assert(literalReplacement.isDefined && !literalReplacement.get.hasIf)

    // A nullable replacement nullifies the row in Spark.
    val guarded = convert(ArrayJoin(nullableArray, Literal(","), Some(nullableStr)))
    assert(guarded.isDefined && guarded.get.hasIf)
    val literalNull =
      convert(ArrayJoin(nullableArray, Literal(","), Some(Literal.create(null, StringType))))
    assert(literalNull.isDefined && literalNull.get.hasIf)

    // A nullable delimiter needs none: array_to_string already returns null for it.
    val nullableDelimiter = convert(ArrayJoin(nullableArray, nullableStr, None))
    assert(nullableDelimiter.isDefined && !nullableDelimiter.get.hasIf)
  }

  test("arrays_overlap") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        withTempView("t1") {
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

  test("arrays_overlap - runtime NaN representations") {
    val floatNaN = java.lang.Float.intBitsToFloat(0x7fc01234 | Int.MinValue)
    val doubleNaN = java.lang.Double.longBitsToDouble(0x7ff8000000001234L | Long.MinValue)

    withParquetTable(
      Seq((floatNaN, doubleNaN)),
      "floating_point_overlap",
      withDictionary = false) {
      // The behavioral cases live in arrays_overlap.sql. SQL equality cannot distinguish NaN
      // representations, so verify here that Parquet canonicalizes the inputs and that native
      // runtime negation produces noncanonical NaNs after the scan.
      val query = sql("SELECT _1, -_1, _2, -_2 FROM floating_point_overlap")
      checkSparkAnswerAndOperator(query)
      val row = query.head()
      val canonicalFloatNaNBits = java.lang.Float.floatToRawIntBits(Float.NaN)
      val canonicalDoubleNaNBits = java.lang.Double.doubleToRawLongBits(Double.NaN)
      assert(java.lang.Float.floatToRawIntBits(row.getFloat(0)) == canonicalFloatNaNBits)
      assert(
        java.lang.Float.floatToRawIntBits(row.getFloat(1)) ==
          (canonicalFloatNaNBits | Int.MinValue))
      assert(java.lang.Double.doubleToRawLongBits(row.getDouble(2)) == canonicalDoubleNaNBits)
      assert(
        java.lang.Double.doubleToRawLongBits(row.getDouble(3)) ==
          (canonicalDoubleNaNBits | Long.MinValue))
    }
  }

  test("arrays_overlap - null handling behavior verification") {
    withSQLConf(
      "spark.sql.optimizer.excludedRules" -> "org.apache.spark.sql.catalyst.optimizer.ConstantFolding") {
      withTable("t") {
        sql("create table t using parquet as select CAST(NULL as array<int>) a1 from range(1)")
        val data = Seq(
          "array(1, 2, 3)",
          "array(3, 4, 5)",
          "array(1, 2)",
          "array(3, 4)",
          "array(1, NULL, 3)",
          "array(4, 5)",
          "array(1, 4)",
          "array(1, NULL)",
          "array(2, NULL)",
          "array(NULL, 2)",
          "array(1)",
          "array(2)",
          "array()",
          "array(NULL)",
          "array(NULL, NULL)",
          "a1")
        for (y <- data; x <- data) {
          checkSparkAnswerAndOperator(sql(s"SELECT arrays_overlap($y, $x) from t"))
        }
      }
    }
  }

  test("arrays_overlap - nested array null handling behavior verification") {
    withSQLConf(
      "spark.sql.optimizer.excludedRules" -> "org.apache.spark.sql.catalyst.optimizer.ConstantFolding") {
      withTable("t") {
        sql(
          "create table t using parquet as select CAST(NULL as array<array<int>>) a1 from range(1)")
        val data = Seq(
          "array(array(1, 2), array(3, 4))",
          "array(array(1, 2), array(5, 6))",
          "array(array(1, 2))",
          "array(array(3, 4))",
          "array(array(1, NULL))",
          "array(array(NULL, 2))",
          "array(array(NULL))",
          "array(CAST(NULL as array<int>))",
          "array(array(1, 2), CAST(NULL as array<int>))",
          "array()",
          "a1")
        for (y <- data; x <- data) {
          checkSparkAnswerAndOperator(sql(s"SELECT arrays_overlap($y, $x) from t"))
        }
      }
    }
  }

  test("arrays_overlap - struct element null handling behavior verification") {
    withSQLConf(
      "spark.sql.optimizer.excludedRules" -> "org.apache.spark.sql.catalyst.optimizer.ConstantFolding") {
      withTable("t") {
        sql(
          "create table t using parquet as select CAST(NULL as array<struct<a:int,b:int>>) a1 from range(1)")
        // Cast all structs to the same nullable type to avoid Arrow schema mismatch
        val s = "struct<a:int,b:int>"
        val data = Seq(
          s"array(CAST(named_struct('a', 1, 'b', 2) AS $s), CAST(named_struct('a', 3, 'b', 4) AS $s))",
          s"array(CAST(named_struct('a', 1, 'b', 2) AS $s))",
          s"array(CAST(named_struct('a', 3, 'b', 4) AS $s))",
          s"array(CAST(named_struct('a', 1, 'b', CAST(NULL as int)) AS $s))",
          s"array(CAST(named_struct('a', CAST(NULL as int), 'b', 2) AS $s))",
          s"array(CAST(named_struct('a', CAST(NULL as int), 'b', CAST(NULL as int)) AS $s))",
          s"array(CAST(NULL as $s))",
          s"array(CAST(named_struct('a', 1, 'b', 2) AS $s), CAST(NULL as $s))",
          "array()",
          "a1")
        for (y <- data; x <- data) {
          checkSparkAnswerAndOperator(sql(s"SELECT arrays_overlap($y, $x) from t"))
        }
      }
    }
  }

  test("array_compact") {
    Seq(true, false).foreach { dictionaryEnabled =>
      withTempDir { dir =>
        withTempView("t1") {
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
    withSQLConf(CometConf.getExprAllowIncompatConfigKey(classOf[ArrayExcept]) -> "true") {
      Seq(true, false).foreach { dictionaryEnabled =>
        withTempDir { dir =>
          withTempView("t1") {
            val path = new Path(dir.toURI.toString, "test.parquet")
            makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled, 10000)
            spark.read.parquet(path.toString).createOrReplaceTempView("t1")

            checkSparkAnswerAndOperator(
              sql("SELECT array_except(array(_2, _3, _4), array(_3, _4)) from t1"))
            checkSparkAnswerAndOperator(
              sql("SELECT array_except(array(_18), array(_19)) from t1"))
            checkSparkAnswerAndOperator(
              spark.sql(
                "SELECT array_except(array(_2, _2, _4), array(_4)) FROM t1 WHERE _2 IS NOT NULL"))
          }
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
          SchemaGenOptions(generateArray = false, generateStruct = false, generateMap = false),
          DataGenOptions(allowNull = true, generateNegativeZero = true))
      }
      withSQLConf(CometConf.getExprAllowIncompatConfigKey(classOf[ArrayExcept]) -> "true") {
        withTempView("t1", "t2") {
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
  }

  test("array_except - test all types (convert from Parquet)") {
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
          SchemaGenOptions(generateArray = true, generateStruct = true, generateMap = false),
          DataGenOptions(allowNull = true, generateNegativeZero = true))
      }
      withSQLConf(
        CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "false",
        CometConf.COMET_SPARK_TO_ARROW_ENABLED.key -> "true",
        CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.key -> "true") {
        withTempView("t1", "t2") {
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
  }

  test("array_repeat") {
    withSQLConf(
      CometConf.getExprAllowIncompatConfigKey(classOf[ArrayRepeat]) -> "true",
      CometConf.COMET_EXPLAIN_FALLBACK_ENABLED.key -> "true") {
      Seq(true, false).foreach { dictionaryEnabled =>
        withTempDir { dir =>
          withTempView("t1") {
            val path = new Path(dir.toURI.toString, "test.parquet")
            makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled, 100)
            spark.read.parquet(path.toString).createOrReplaceTempView("t1")

            checkSparkAnswerAndOperator(sql("SELECT array_repeat(_4, null) from t1"))
            checkSparkAnswerAndOperator(sql("SELECT array_repeat(_4, 0) from t1"))
            checkSparkAnswerAndOperator(sql("SELECT array_repeat(_4, -1) from t1"))
            checkSparkAnswerAndOperator(
              sql("SELECT array_repeat(cast(_3 as string), -5) from t1"))
            checkSparkAnswerAndOperator(
              sql("SELECT array_repeat(_2, 5) from t1 where _2 is not null"))
            checkSparkAnswerAndOperator(
              sql("SELECT array_repeat(_2, 5) from t1 where _2 is null"))
            checkSparkAnswerAndOperator(
              sql("SELECT array_repeat(_3, _4) from t1 where _3 is not null"))
            checkSparkAnswerAndOperator(sql("SELECT array_repeat(cast(_3 as string), 2) from t1"))
            checkSparkAnswerAndOperator(sql("SELECT array_repeat(array(_2, _3, _4), 2) from t1"))
          }
        }
      }
    }
  }

  test("flatten - test all types (native Parquet reader)") {
    withTempDir { dir =>
      withTempView("t1", "t2") {
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
        val table = spark.read.parquet(filename)
        table.createOrReplaceTempView("t1")
        val fieldNames =
          table.schema.fields
            .filter(field => CometFlatten.isTypeSupported(field.dataType))
            .map(_.name)
        for (fieldName <- fieldNames) {
          sql(s"SELECT array(array($fieldName, $fieldName), array($fieldName)) as a FROM t1")
            .createOrReplaceTempView("t2")
          checkSparkAnswerAndOperator(sql("SELECT flatten(a) FROM t2"))
        }
      }
    }
  }

  test("flatten - test all types (convert from Parquet)") {
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
          SchemaGenOptions(generateArray = true, generateStruct = true, generateMap = false),
          DataGenOptions(allowNull = true, generateNegativeZero = true))
      }
      withSQLConf(
        CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "false",
        CometConf.COMET_SPARK_TO_ARROW_ENABLED.key -> "true",
        CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.key -> "true") {
        withTempView("t1", "t2") {
          val table = spark.read.parquet(filename)
          table.createOrReplaceTempView("t1")
          val fieldNames =
            table.schema.fields
              .filter(field => CometFlatten.isTypeSupported(field.dataType))
              .map(_.name)
          for (fieldName <- fieldNames) {
            sql(s"SELECT array(array($fieldName, $fieldName), array($fieldName)) as a FROM t1")
              .createOrReplaceTempView("t2")
            checkSparkAnswer(sql("SELECT flatten(a) FROM t2"))
          }
        }
      }
    }
  }

  test("array literals") {
    withSQLConf(CometConf.COMET_EXPLAIN_FALLBACK_ENABLED.key -> "true") {
      Seq(true, false).foreach { dictionaryEnabled =>
        withTempDir { dir =>
          withTempView("t1") {
            val path = new Path(dir.toURI.toString, "test.parquet")
            makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled, 100)
            spark.read.parquet(path.toString).createOrReplaceTempView("t1")
            checkSparkAnswerAndOperator(
              sql("SELECT array(array(1, 2, 3), null, array(), array(null), array(1)) from t1"))
          }
        }
      }
    }
  }

  test("array_reverse") {
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
          SchemaGenOptions(generateArray = true, generateStruct = true, generateMap = false),
          DataGenOptions(allowNull = true, generateNegativeZero = true))
      }
      withSQLConf(
        CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "false",
        CometConf.COMET_SPARK_TO_ARROW_ENABLED.key -> "true",
        CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.key -> "true") {
        withTempView("t1", "t2") {
          val table = spark.read.parquet(filename)
          table.createOrReplaceTempView("t1")
          val fieldNames =
            table.schema.fields
              .filter(field => CometArrayReverse.isTypeSupported(field.dataType))
              .map(_.name)
          for (fieldName <- fieldNames) {
            sql(s"SELECT $fieldName as a FROM t1")
              .createOrReplaceTempView("t2")
            checkSparkAnswer(sql("SELECT reverse(a) FROM t2"))
          }
        }
      }
    }
  }

  // https://github.com/apache/datafusion-comet/issues/2612
  test("array_reverse - binary array") {
    withTable("t1") {
      sql("""create table t1 using parquet as
          select cast(null as array<binary>) c1, cast(array() as array<binary>) c2
          from range(10)
        """)

      // The native path is Incompatible for arrays containing binary, so Comet routes these
      // through the codegen dispatcher and still executes natively.
      checkSparkAnswerAndOperator("select reverse(array(c1, c2)) AS x FROM t1")
      checkSparkAnswerAndOperator("select reverse(array(c1, c1)) AS x FROM t1")
      checkSparkAnswerAndOperator("select reverse(array(array(c1), array(c2))) AS x FROM t1")
    }
  }

  test("array_reverse 2") {
    // This test validates data correctness for array<binary> columns with nullable elements.
    // See https://github.com/apache/datafusion-comet/issues/2612
    withTempDir { dir =>
      val path = new Path(dir.toURI.toString, "test.parquet")
      val filename = path.toString
      val random = new Random(42)
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        val schemaOptions =
          SchemaGenOptions(generateArray = true, generateStruct = false, generateMap = false)
        val dataOptions = DataGenOptions(allowNull = true, generateNegativeZero = false)
        ParquetGenerator.makeParquetFile(random, spark, filename, 100, schemaOptions, dataOptions)
      }
      withTempView("t1") {
        val table = spark.read.parquet(filename)
        table.createOrReplaceTempView("t1")
        for (field <- table.schema.fields.filter(_.dataType.isInstanceOf[ArrayType])) {
          val sql = s"SELECT ${field.name}, reverse(${field.name}) FROM t1 ORDER BY ${field.name}"
          checkSparkAnswer(sql)
        }
      }
    }
  }

  test("size with array input") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      withTempDir { dir =>
        withTempView("t1") {
          val path = new Path(dir.toURI.toString, "test.parquet")
          makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = true, 100)
          spark.read.parquet(path.toString).createOrReplaceTempView("t1")

          // Test size function with arrays built from columns (ensures native execution)
          checkSparkAnswerAndOperator(
            sql(
              "SELECT size(array(_2, _3, _4)) from t1 where _2 is not null order by _2, _3, _4"))
          checkSparkAnswerAndOperator(
            sql("SELECT size(array(_1)) from t1 where _1 is not null order by _1"))
          checkSparkAnswerAndOperator(
            sql("SELECT size(array(_2, _3)) from t1 where _2 is null order by _2, _3"))

          // Test with conditional arrays (forces runtime evaluation)
          checkSparkAnswerAndOperator(sql(
            "SELECT size(case when _2 > 0 then array(_2, _3, _4) else array(_2) end) from t1 order by _2, _3, _4"))
          checkSparkAnswerAndOperator(sql(
            "SELECT size(case when _1 then array(_8, _9) else array(_8, _9, _10) end) from t1 order by _1, _8, _9, _10"))

          // Test empty arrays using conditional logic to avoid constant folding
          checkSparkAnswerAndOperator(sql(
            "SELECT size(case when _2 < 0 then array(_2, _3) else array() end) from t1 order by _2, _3"))

          // Test null arrays using conditional logic
          checkSparkAnswerAndOperator(sql(
            "SELECT size(case when _2 is null then cast(null as array<int>) else array(_2) end) from t1 order by _2"))

          // Test with different data types using column references
          checkSparkAnswerAndOperator(
            sql(
              "SELECT size(array(_8, _9, _10)) from t1 where _8 is not null order by _8, _9, _10"
            )
          ) // string arrays
          checkSparkAnswerAndOperator(
            sql(
              "SELECT size(array(_2, _3, _4, _5, _6)) from t1 where _2 is not null order by _2, _3, _4, _5, _6"
            )
          ) // int arrays
        }
      }
    }
  }

  test("size - respect to legacySizeOfNull") {
    val table = "t1"
    withTable(table) {
      sql(s"create table $table(col array<string>) using parquet")
      sql(s"insert into $table values(null)")
      withSQLConf(SQLConf.LEGACY_SIZE_OF_NULL.key -> "false") {
        checkSparkAnswerAndOperator(sql(s"select size(col) from $table"))
      }
      withSQLConf(
        SQLConf.LEGACY_SIZE_OF_NULL.key -> "true",
        SQLConf.ANSI_ENABLED.key -> "false") {
        checkSparkAnswerAndOperator(sql(s"select size(col) from $table"))
      }
    }
  }

  test("size - non-deterministic child under the null guard") {
    withParquetTable((0 until 16).map(i => Tuple1(i.toLong)), "t", withDictionary = false) {
      // Non-legacy size wraps a nullable child in `CASE WHEN child IS NOT NULL`, which would
      // evaluate a stateful child twice, so that shape stays in Spark; legacy mode builds no
      // guard and keeps it native.
      val nullableStateful =
        "SELECT _1, size(IF(monotonically_increasing_id() % 2 = 0, array(_1), NULL)) FROM t"
      withSQLConf(SQLConf.LEGACY_SIZE_OF_NULL.key -> "false") {
        checkSparkAnswerAndFallbackReason(
          nullableStateful,
          "non-deterministic child under a null guard is evaluated on different rows than Spark's")
      }
      withSQLConf(
        SQLConf.LEGACY_SIZE_OF_NULL.key -> "true",
        SQLConf.ANSI_ENABLED.key -> "false") {
        checkSparkAnswerAndOperator(nullableStateful)
      }
      // A non-nullable child gets no guard, so a stateful one whose length depends on the
      // counter is evaluated once and matches Spark. The lambda runs through the JVM codegen
      // dispatcher, where a guard's two copies would share one kernel and its counter.
      withSQLConf(SQLConf.LEGACY_SIZE_OF_NULL.key -> "false") {
        checkSparkAnswerAndOperator(
          "SELECT _1, size(filter(array(_1, 1, 2), x -> x < monotonically_increasing_id())) FROM t")
      }
    }
  }

  // https://github.com/apache/datafusion-comet/issues/4560
  test("array_size returns null for null input") {
    val table = "t1"
    withTable(table) {
      sql(s"create table $table(col array<int>) using parquet")
      sql(s"insert into $table values(array(1, 2, 3)), (array()), (null)")
      // array_size lowers to Size(child, legacySizeOfNull = false), so it must return null
      // for a null input regardless of the legacySizeOfNull conf.
      Seq("false", "true").foreach { legacy =>
        withSQLConf(
          SQLConf.LEGACY_SIZE_OF_NULL.key -> legacy,
          SQLConf.ANSI_ENABLED.key -> "false") {
          checkSparkAnswerAndOperator(sql(s"select array_size(col) from $table"))
        }
      }
    }
  }

  // https://github.com/apache/datafusion-comet/issues/3375
  test("(ansi) array access out of bounds - GetArrayItem") {
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "true",
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true") {
      withTable("test_array_get_item") {
        sql("CREATE TABLE test_array_get_item(arr ARRAY<INT>) USING parquet")
        sql("INSERT INTO test_array_get_item VALUES (array(1, 2, 3))")
        // Try to access array with out-of-bounds index
        val exception = intercept[Exception] {
          sql("select arr[5] from test_array_get_item").collect()
        }
        val errorMessage = exception.getMessage
        // Verify error message contains the expected error code
        assert(
          errorMessage.contains("INVALID_ARRAY_INDEX"),
          s"Error message should contain array index error: $errorMessage")

        assert(errorMessage.contains("The index 5 is out of bounds. The array has 3 elements." +
          " Use the SQL function `get()` to tolerate accessing element at invalid index and return NULL instead."))

        assert(
          errorMessage.contains("select arr[5] from test_array_get_item"),
          s"Error message should contain SQL query text but got: $errorMessage")
      }
    }
  }

  // https://github.com/apache/datafusion-comet/issues/3375
  test("(ansi) array access out of bounds - element_at with invalid index") {
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "true",
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true") {
      withTable("test_element_at_invalid") {
        sql("CREATE TABLE test_element_at_invalid(arr ARRAY<INT>) USING parquet")
        sql("INSERT INTO test_element_at_invalid VALUES (array(1, 2, 3))")
        // Try to access array with out-of-bounds index using element_at
        val exception = intercept[Exception] {
          sql("select element_at(arr, 10) from test_element_at_invalid").collect()
        }
        val errorMessage = exception.getMessage
        // Verify error message contains the expected error code
        assert(
          errorMessage.contains("INVALID_ARRAY_INDEX_IN_ELEMENT_AT"),
          s"Error message should contain array index error: $errorMessage")

        assert(errorMessage.contains("The index 10 is out of bounds. The array has 3 elements." +
          " Use `try_element_at` to tolerate accessing element at invalid index and return NULL instead"))

        assert(
          errorMessage.contains("select element_at(arr, 10) from test_element_at_invalid"),
          s"Error message should contain SQL query text but got: $errorMessage")
      }
    }
  }

  // https://github.com/apache/datafusion-comet/issues/3375
  test("(ansi) array access with zero index - element_at") {
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "true",
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true") {
      withTable("test_element_at_zero") {
        sql("CREATE TABLE test_element_at_zero(arr ARRAY<INT>) USING parquet")
        sql("INSERT INTO test_element_at_zero VALUES (array(1, 2, 3))")
        // Try to access array with zero index (invalid in Spark)
        val exception = intercept[Exception] {
          sql("select element_at(arr, 0) from test_element_at_zero").collect()
        }
        val errorMessage = exception.getMessage
        // Verify error message contains the expected error code
        assert(
          errorMessage.contains("INVALID_INDEX_OF_ZERO"),
          s"Error message should contain zero index error: $errorMessage")

        assert(
          errorMessage.contains("The index 0 is invalid. An index shall be either < 0 or > 0" +
            " (the first element has index 1)"))

        assert(
          errorMessage.contains("select element_at(arr, 0) from test_element_at_zero"),
          s"Error message should contain SQL query text but got: $errorMessage")
      }
    }
  }

  // The tests below deliberately live in this suite, not a `sql-tests` fixture: constant folding is
  // enabled by default here, so each `map(...)` collapses to a MapType Literal and the outer
  // `array(...)` reaches `CometCreateArray` with folded-Literal children, which `CometLiteral`
  // rebuilds as an equivalent `CreateMap` of primitive literals -- the folded-literal expansion path
  // under review. `CometSqlFileTestSuite` force-disables `ConstantFolding`, so an equivalent SQL
  // fixture would only exercise the constructor path (which `create_array.sql` already covers).
  test("array of folded map literals with array values (multirow)") {
    withParquetTable((0 until 3).map(i => (i, i.toLong)), "tbl") {
      checkSparkAnswerAndOperator(
        "SELECT array(map(1, array(1, 2, 3)), map(2, array(4, 5, 6))) FROM tbl")
    }
  }

  // A folded `map(1, array(1))` (value `ArrayType(IntegerType, containsNull = false)`) sits beside a
  // dynamic `map(2, array(_1))` (value `ArrayType(IntegerType, true)`). Both maps still declare
  // `valueContainsNull = false`, so only the nested array's `containsNull` differs. `CometCreateArray`
  // casts each child to the nullability-merged element type (`cast_map_to_map` widens the nested
  // array), so `make_array` sees identical Arrow types and this runs natively.
  test(
    "folded map with non-null nested array beside a dynamic map sibling runs natively (multirow)") {
    withParquetTable((0 until 3).map(i => (i, i.toLong)), "tbl") {
      checkSparkAnswerAndOperator(
        "SELECT array(map(1, array(1)), map(2, array(_1))) AS a FROM tbl")
    }
  }

  test("array of folded map literals (multirow)") {
    withParquetTable((0 until 3).map(i => (i, i.toLong)), "tbl") {
      checkSparkAnswerAndOperator("SELECT array(map(1, 10), map(2, 20)) FROM tbl")
    }
  }

  // The folded `map(1, 2)` and the dynamic `map(2, coalesce(...))` both declare
  // `MapType(IntegerType, IntegerType, valueContainsNull = false)`, so `CometCreateArray` admits
  // the pair. Rebuilding the literal has to report that exact type: widening the rebuilt map's
  // value to nullable would leave the sibling behind and `make_array` would panic, because
  // DataFusion 54.1 cannot coerce a `MapType.valueContainsNull` mismatch away.
  test("folded map literal keeps non-nullable values next to a dynamic map sibling (multirow)") {
    withParquetTable((0 until 3).map(i => (i, i.toLong)), "tbl") {
      checkSparkAnswerAndOperator(
        "SELECT _1 AS id, array(map(1, 2), map(2, coalesce(_1, 0))) AS arr FROM tbl")
    }
  }

  // The whole `array(...)` folds to one `ArrayType(MapType(IntegerType, IntegerType, true))`
  // literal, so every rebuilt element must report the unified nullable value type.
  test("folded array of maps with a NULL value (multirow)") {
    withParquetTable((0 until 3).map(i => (i, i.toLong)), "tbl") {
      checkSparkAnswerAndOperator(
        "SELECT array(map(1, CAST(NULL AS INT)), map(2, 3)) AS arr FROM tbl")
    }
  }

  // A NULL element sits next to a populated one inside a single folded
  // `ArrayType(MapType(IntegerType, IntegerType, false), containsNull = true)` literal. The null
  // slot serializes as a typed null literal carrying the declared map type, so the rebuilt sibling
  // has to report that same type for `make_array` to accept the pair.
  test("folded array mixing a map literal and a NULL element (multirow)") {
    withParquetTable((0 until 3).map(i => (i, i.toLong)), "tbl") {
      checkSparkAnswerAndOperator("SELECT array(map(1, 2), NULL) AS arr FROM tbl")
      checkSparkAnswerAndOperator("SELECT array(NULL, map(1, 2)) AS arr FROM tbl")
    }
  }

  // A map value that is itself a map or an array of maps is built by Spark's own generated code
  // inside `CometCreateMap`, so it keeps its declared nullability all the way to the consumer.
  test("folded map literals with nested map values (multirow)") {
    withParquetTable((0 until 3).map(i => (i, i.toLong)), "tbl") {
      checkSparkAnswerAndOperator("SELECT _1 AS id, map(1, map(1, 2)) AS m FROM tbl")
      checkSparkAnswerAndOperator("SELECT _1 AS id, map(1, array(map(2, 3))) AS m FROM tbl")
      checkSparkAnswerAndOperator(
        "SELECT _1 AS id, map(1, named_struct('a', 1, 'b', 'x')) AS m FROM tbl")
    }
  }

  // An empty container has no children to recover the element type from, and a NULL literal
  // serializes as a typed null without expansion, so only the empty cases fall back.
  test("folded empty and NULL complex literals") {
    withParquetTable((0 until 3).map(i => (i, i.toLong)), "tbl") {
      checkSparkAnswerAndFallbackReason(
        "SELECT _1 AS id, CAST(map() AS MAP<INT,INT>) AS m FROM tbl",
        "Unsupported data type MapType")
      checkSparkAnswerAndFallbackReason(
        "SELECT _1 AS id, CAST(array() AS ARRAY<MAP<INT,INT>>) AS a FROM tbl",
        "Unsupported data type ArrayType")
      checkSparkAnswerAndOperator("SELECT _1 AS id, CAST(NULL AS MAP<INT,INT>) AS m FROM tbl")
      checkSparkAnswerAndOperator(
        "SELECT _1 AS id, CAST(NULL AS ARRAY<MAP<INT,INT>>) AS a FROM tbl")
    }
  }

  // A folded struct literal would reach native `CreateNamedStruct` with all-scalar children,
  // which builds a 1-row `StructArray` regardless of batch size. `CometLiteral` excludes
  // StructType from expansion so the projection falls back instead of truncating the result.
  test("folded struct literal in multirow projection falls back") {
    withParquetTable((0 until 3).map(i => (i, i.toLong)), "tbl") {
      checkSparkAnswerAndFallbackReason(
        "SELECT _1 AS id, named_struct('a', 1) AS s FROM tbl",
        "Unsupported data type StructType")
    }
  }

  // Folds to one ArrayType(StructType) Literal whose structs disagree on field nullability.
  // `KnownNullable` is dropped on the wire, so expansion could not make the sibling struct
  // arrays agree and `make_array` would panic. Excluding StructType keeps this on Spark.
  test("folded array of structs with divergent field nullability falls back (multirow)") {
    withParquetTable((0 until 3).map(i => (i, i.toLong)), "tbl") {
      checkSparkAnswerAndFallbackReason(
        "SELECT array(named_struct('a', CAST(NULL AS INT)), named_struct('a', 1)) AS arr FROM tbl",
        "Unsupported data type ArrayType")
    }
  }

  // `from_json` can fold to a MapData with duplicate keys. Rebuilding it as a `CreateMap` would
  // run `ArrayBasedMapBuilder`, which throws under `MAP_KEY_DEDUP_POLICY=EXCEPTION` and silently
  // drops the earlier entry under `LAST_WIN`, so `CometLiteral` declines expansion under either
  // policy and Spark evaluates the projection.
  test("folded map literal with duplicate keys falls back (multirow)") {
    assume(isSpark35Plus)
    withParquetTable((0 until 3).map(i => (i, i.toLong)), "tbl") {
      Seq("EXCEPTION", "LAST_WIN").foreach { policy =>
        withSQLConf(SQLConf.MAP_KEY_DEDUP_POLICY.key -> policy) {
          checkSparkAnswerAndFallbackReason(
            "SELECT _1 AS id, from_json('{\"a\":1,\"a\":2}', 'MAP<STRING,INT>') AS m FROM tbl",
            "Unsupported data type MapType")
        }
      }
    }
  }

  // Spark's map cast preserves both entries, so the folded value still holds duplicate keys after
  // the key type becomes BinaryType. `Array[Byte]` has no value-based `equals`, so the duplicate
  // check has to compare keys the way `ArrayBasedMapBuilder` does, through
  // `TypeUtils.getInterpretedOrdering`.
  test("folded map literal with duplicate binary keys falls back (multirow)") {
    assume(isSpark35Plus)
    withParquetTable((0 until 3).map(i => (i, i.toLong)), "tbl") {
      Seq("EXCEPTION", "LAST_WIN").foreach { policy =>
        withSQLConf(SQLConf.MAP_KEY_DEDUP_POLICY.key -> policy) {
          checkSparkAnswerAndFallbackReason(
            "SELECT _1 AS id, CAST(from_json('{\"a\":1,\"a\":2}', 'MAP<STRING,INT>') " +
              "AS MAP<BINARY,INT>) AS m FROM tbl",
            "Unsupported data type MapType")
        }
      }
    }
  }

  // Distinct binary keys are fine: Arrow compares them by content, as Spark's ordering does.
  test("folded map literal with distinct binary keys (multirow)") {
    withParquetTable((1 until 4).map(i => (i, i.toLong)), "tbl") {
      checkSparkAnswerAndOperator(
        "SELECT _1 AS id, element_at(map(CAST('1' AS BINARY), 10, CAST('2' AS BINARY), 20), " +
          "CAST(CAST(_1 AS STRING) AS BINARY)) AS v FROM tbl")
    }
  }

  // Local table scan carries non-null array child fields (an in-memory Seq encodes
  // containsNull=false) into native kernels that promise nullable elements. ConvertToLocalRelation
  // must be disabled or the optimizer folds the expression at plan time and nothing runs natively.
  // https://github.com/apache/datafusion-comet/issues/4789
  private def withLocalTableScanNoFold(f: => Unit): Unit = {
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      "spark.sql.optimizer.excludedRules" ->
        "org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation") {
      f
    }
  }

  test("slice on non-null element array from local table scan (#4789)") {
    withLocalTableScanNoFold {
      import testImplicits._
      val df = Seq(Seq(1, 2, 3), Seq(4, 5)).toDF("x")
      checkSparkAnswerAndOperator(df.selectExpr("slice(x, 2, 2)"))
    }
  }

  test("array_insert on non-null element array from local table scan (#4789)") {
    assume(isSpark35Plus)
    withLocalTableScanNoFold {
      import testImplicits._
      val df = Seq(Seq(1, 2, 3), Seq(4, 5)).toDF("x")
      // SPARK-41233 array prepend lowers to array_insert at position 1
      checkSparkAnswerAndOperator(df.selectExpr("array_insert(x, 1, 0)"))
    }
  }

  // https://issues.apache.org/jira/browse/SPARK-55747
  test("(ansi) GetArrayItem on null array from split()") {
    withSQLConf(
      SQLConf.ANSI_ENABLED.key -> "true",
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true") {
      withTable("test_split_null") {
        sql("CREATE TABLE test_split_null(s STRING) USING parquet")
        sql("INSERT INTO test_split_null VALUES ('a,b,c'), (NULL)")
        // split(NULL, ...) yields a null array; arr[0] on a null array must return NULL
        // rather than failing the non-nullable schema validation in native execution.
        checkSparkAnswerAndOperator(sql("SELECT split(s, ',')[0] FROM test_split_null"))
      }
    }
  }
}
