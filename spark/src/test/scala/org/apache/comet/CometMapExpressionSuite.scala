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

  test("size with map input") {
    withTempDir { dir =>
      withTempView("t1") {
        val path = new Path(dir.toURI.toString, "test.parquet")
        makeParquetFileAllPrimitiveTypes(path, dictionaryEnabled = true, 100)
        spark.read.parquet(path.toString).createOrReplaceTempView("t1")

        checkSparkAnswer(
          sql("SELECT size(case when _2 < 0 then map(_8, _9) else map() end) from t1"))
      }
    }
  }

  test("size with map input - v2 reader") {
    withTempPath { dir =>
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        val df = spark
          .range(100)
          .select(
            col("id"),
            when(col("id") > 1, map(col("id"), col("id"))).alias("map1"),
            when(col("id") > 5, map(col("id"), col("id"))).alias("map2"))
        df.write.parquet(dir.toString())
      }

      Seq("", "parquet").foreach { v1List =>
        withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> v1List) {
          val df = spark.read.parquet(dir.toString())
          df.createOrReplaceTempView("t1")
          if (v1List.isEmpty) {
            checkSparkAnswer(df.select(size(col("map1"))))
            checkSparkAnswer(df.select(size(col("map2"))))
            checkSparkAnswer(
              sql("SELECT size(CASE WHEN id < 50 THEN map1 ELSE map2 END) FROM t1"))
          } else {
            checkSparkAnswerAndOperator(df.select(size(col("map1"))))
            checkSparkAnswerAndOperator(df.select(size(col("map2"))))
            checkSparkAnswerAndOperator(
              sql("SELECT size(CASE WHEN id < 50 THEN map1 ELSE map2 END) FROM t1"))
          }
        }
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

  test("group by map column with string values") {
    assume(isSpark40Plus, "Spark 4.0 inserts MapSort for group-by on map keys")
    withTable("t_map_group") {
      sql("""
        |CREATE TABLE t_map_group USING parquet AS
        |SELECT map(cast(id as string), cast(id + 100 as string)) as m
        |FROM range(5)
      """.stripMargin)
      checkSparkAnswer(sql("SELECT m, count(*) FROM t_map_group GROUP BY m"))
    }
  }

  test("map_from_entries - binary type routes through codegen dispatcher") {
    val table = "t2"
    withTable(table) {
      sql(
        s"create table $table using parquet as select cast(array() as array<binary>) as c1 from range(10)")
      checkSparkAnswerAndOperator(
        sql(s"select map_from_entries(array(struct(c1, 0))) from $table"))
      checkSparkAnswerAndOperator(
        sql(s"select map_from_entries(array(struct(0, c1))) from $table"))
    }
  }

  test("map_entries on non-null value map from local table scan (#4789)") {
    // An in-memory Map encodes valueContainsNull=false; the local scan must widen the map value
    // to nullable so map_entries' native ListArray/Struct build does not fail on the child type.
    // ConvertToLocalRelation must be disabled or the expression folds at plan time.
    withSQLConf(
      CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key -> "true",
      "spark.sql.optimizer.excludedRules" ->
        "org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation") {
      import testImplicits._
      val df = Seq(Map(1 -> 100, 2 -> 200)).toDF("m")
      checkSparkAnswerAndOperator(df.selectExpr("map_entries(m)"))
    }
  }

  // A map that reaches `map_entries` through an expression rather than a scan keeps
  // `valueContainsNull = false`, which every `map(...)` over non-null values produces. DataFusion's
  // `map_entries` declares the entry `value` field nullable but reuses the input map's entries
  // array, so the planner widens the argument before the call. Here the inner `map(1, map(1, 2))`
  // folds to a literal that `CometLiteral` rebuilds, and the outer `element_at` yields a
  // non-nullable-value map straight into native `map_entries`. This exercises the widening on the
  // default folding-on path; `map_entries.sql` covers the constructor path, where the harness
  // excludes `ConstantFolding`.
  test("map_entries on a folded non-nullable-value map (multirow)") {
    withParquetTable((1 until 4).map(i => (i, i.toLong)), "tbl") {
      checkSparkAnswerAndOperator(
        "SELECT _1 AS id, map_entries(element_at(map(1, map(1, 2)), _1)) AS e FROM tbl")
    }
  }

  // Finding E: a map nested inside a map value is handed to the JVM dispatcher whole, so its double
  // keys never revisit `CometLiteral`. The guard has to live on the lookup instead. The outer key
  // type is INT, so inspecting only the outermost map would admit the query; the fallback comes
  // from the inner `element_at`, whose child is `MapType(DoubleType, IntegerType)`. Constant folding
  // is on here, which is the only configuration where the inner map becomes such a literal, so this
  // regression cannot be expressed in a SQL fixture (the harness disables folding). The direct
  // single-level double-key lookups live in `element_at_map.sql` / `get_map_value.sql`.
  test("nested map lookup with floating-point keys falls back") {
    withParquetTable((1 until 4).map(i => (i, i.toLong)), "tbl") {
      val negZero = "CAST(concat('-', CAST(_1 - 1 AS STRING), '.0') AS DOUBLE)"
      checkSparkAnswerAndFallbackReason(
        "SELECT _1 AS id, element_at(element_at(map(1, map(CAST(0 AS DOUBLE), 7)), _1), " +
          s"$negZero) AS v FROM tbl",
        "Spark normalizes floating-point map keys")
    }
  }

  // Finding E for a collated inner key. Same folding-on-only bypass as the double-key case above;
  // the direct single-level collated lookup lives in `element_at_map_collation.sql`.
  test("nested map lookup with collated string keys falls back") {
    assume(isSpark40Plus)
    withParquetTable(Seq(("a1", 0)), "tbl") {
      checkSparkAnswerAndFallbackReason(
        "SELECT element_at(element_at(map(1, map(CAST('A1' AS STRING COLLATE UTF8_LCASE), 7)), 1), " +
          "CAST(_1 AS STRING COLLATE UTF8_LCASE)) AS v FROM tbl",
        "cannot honour a non-default collation")
    }
  }

  // A folded map with `CalendarIntervalType` keys reaches `CometLiteral`. `ArrayBasedMapBuilder`
  // dedups such keys by hash equality, but the folded-literal duplicate-key check needs an
  // interpreted ordering and `PhysicalCalendarIntervalType` has none ("does not support ordered
  // operations"). Expansion must decline these keys and keep the projection on Spark rather than
  // crash planning by asking for that ordering; such literals fell back before this rewrite too.
  test("folded map literal with calendar-interval keys falls back (multirow)") {
    withParquetTable((0 until 3).map(i => (i, i.toLong)), "tbl") {
      checkSparkAnswerAndFallbackReason(
        "SELECT _1 AS id, map(make_interval(1), 1, make_interval(2), 2) AS m FROM tbl",
        "Unsupported data type MapType")
    }
  }

  // `map_entries` widens its argument so the entry `value` field is nullable, but it must widen ONLY
  // that outer field. The outer `map(1, IF(...))` has `valueContainsNull = true`, and its value is a
  // folded `map(1, 2)` with `valueContainsNull = false`. Widening the nested map too would extract a
  // `valueContainsNull = true` map that disagrees with the dynamic `map(2, coalesce(...))` sibling,
  // and native `make_array` would panic; the shallow widen keeps the nested type intact.
  test("map_entries widening keeps nested map value nullability (multirow)") {
    withParquetTable((1 until 4).map(i => (i, i.toLong)), "tbl") {
      checkSparkAnswerAndOperator(
        "SELECT _1 AS id, array(" +
          "map_entries(map(1, IF(_1 = 1, map(1, 2), NULL)))[0].value, " +
          "map(2, coalesce(_1, 0))) AS a FROM tbl")
    }
  }

  // `map_contains_key` lowers to `array_contains(map_keys(...), key)`; neither `map_keys` nor
  // `array_contains` gates the key type, so a folded map with floating-point keys would reach them
  // and compare `-0.0` against a normalized `+0.0` key bytewise, unlike Spark. The double-keyed map
  // is nested inside an INT-keyed outer map, so the lookup guard on the outer `element_at` does not
  // see it; expansion has to inspect nested map key types and decline. Spark returns `7, NULL, NULL`.
  test("map_contains_key over nested floating-point map keys falls back (multirow)") {
    withParquetTable((1 until 4).map(i => (i, i.toLong)), "tbl") {
      val negZero = "CAST(concat('-', CAST(_1 - 1 AS STRING), '.0') AS DOUBLE)"
      checkSparkAnswerAndFallbackReason(
        "SELECT _1 AS id, map_contains_key(" +
          s"element_at(map(1, map(CAST(0 AS DOUBLE), 7)), _1), $negZero) AS present FROM tbl",
        "Unsupported data type MapType")
    }
  }

  // The collated counterpart of the double-key `map_contains_key` case above: a nested
  // `UTF8_LCASE`-keyed map would reach `array_contains` with bytewise comparison. Expansion declines
  // the folded literal so the case-insensitive lookup stays on Spark. The outer lookup key is the
  // dynamic `_1` so the inner map survives as a literal (a constant key would let Spark fold
  // `map_keys` into a collated-string array literal instead, which never reaches this guard).
  test("map_contains_key over nested collated map keys falls back (multirow)") {
    assume(isSpark40Plus)
    withParquetTable((1 until 4).map(i => (i, i.toLong)), "tbl") {
      checkSparkAnswerAndFallbackReason(
        "SELECT _1 AS id, map_contains_key(" +
          "element_at(map(1, map(CAST('A1' AS STRING COLLATE UTF8_LCASE), 7)), _1), " +
          "CAST('a1' AS STRING COLLATE UTF8_LCASE)) AS present FROM tbl",
        "Unsupported data type MapType")
    }
  }

}
