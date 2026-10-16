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

package org.apache.spark.sql.benchmark

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.comet.{CometColumnarToRowExec, CometNativeColumnarToRowExec}
import org.apache.spark.sql.execution.{ColumnarToRowExec, QueryExecution, SparkPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.comet.{CometConf, CometSparkSessionExtensions}

/**
 * Benchmark to compare Columnar to Row conversion performance:
 *   - Spark's default ColumnarToRowExec
 *   - Comet's JVM-based CometColumnarToRowExec
 *   - Comet's Native CometNativeColumnarToRowExec
 *
 * To run this benchmark:
 * {{{
 * SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometColumnarToRowBenchmark
 * }}}
 *
 * Results will be written to "spark/benchmarks/CometColumnarToRowBenchmark-**results.txt".
 */
object CometColumnarToRowBenchmark extends CometBenchmarkBase {
  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("CometColumnarToRowBenchmark")
      .set("spark.master", "local[1]")
      .setIfMissing("spark.driver.memory", "3g")
      .setIfMissing("spark.executor.memory", "3g")
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "2g")

    val sparkSession = SparkSession
      .builder()
      .config(conf)
      .withExtensions(new CometSparkSessionExtensions)
      .getOrCreate()

    // Set default configs
    sparkSession.conf.set(SQLConf.ANSI_ENABLED.key, "false")
    sparkSession.conf.set(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, "true")
    sparkSession.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
    sparkSession.conf.set(CometConf.COMET_ENABLED.key, "false")
    sparkSession.conf.set(CometConf.COMET_EXEC_ENABLED.key, "false")
    // These fixtures are written by Spark and contain no unsigned small integers.
    sparkSession.conf.set(CometConf.COMET_PARQUET_UNSIGNED_SMALL_INT_CHECK.key, "false")
    // Disable dictionary encoding to ensure consistent data representation
    sparkSession.conf.set("parquet.enable.dictionary", "false")

    sparkSession
  }

  /**
   * Helper method to add the standard benchmark cases for columnar to row conversion. Reduces
   * code duplication across benchmark methods.
   */
  private def addC2RBenchmarkCases(benchmark: Benchmark, query: String): Unit = {
    def addCase(name: String, expected: Class[_ <: SparkPlan], conf: (String, String)*): Unit = {
      withSQLConf(conf: _*) {
        // Validate the actual noop write, whose plan can differ from the SELECT's plan.
        // Keep execution and listener synchronization outside the timed benchmark cases.
        val plans = ArrayBuffer.empty[SparkPlan]
        val listener = new QueryExecutionListener {
          override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
            plans += qe.executedPlan
          }

          override def onFailure(
              funcName: String,
              qe: QueryExecution,
              exception: Exception): Unit = ()
        }
        spark.sparkContext.listenerBus.waitUntilEmpty()
        spark.listenerManager.register(listener)
        try {
          spark.sql(query).noop()
          spark.sparkContext.listenerBus.waitUntilEmpty()
        } finally {
          spark.listenerManager.unregister(listener)
        }
        val conversions = plans.flatMap { plan =>
          collect(plan) {
            case c: ColumnarToRowExec => c
            case c: CometColumnarToRowExec => c
            case c: CometNativeColumnarToRowExec => c
          }
        }
        require(
          conversions.nonEmpty && conversions.forall(expected.isInstance),
          s"$name did not execute the expected columnar-to-row conversion.\n" +
            plans.mkString("\n"))
        benchmark.out.println(s"Verified $name")
      }
      benchmark.addCase(name) { _ =>
        withSQLConf(conf: _*) {
          spark.sql(query).noop()
        }
      }
    }

    addCase(
      "Spark (ColumnarToRowExec)",
      classOf[ColumnarToRowExec],
      CometConf.COMET_ENABLED.key -> "false")

    addCase(
      "Comet JVM (CometColumnarToRowExec)",
      classOf[CometColumnarToRowExec],
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_NATIVE_COLUMNAR_TO_ROW_ENABLED.key -> "false")

    addCase(
      "Comet Native (CometNativeColumnarToRowExec)",
      classOf[CometNativeColumnarToRowExec],
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_NATIVE_COLUMNAR_TO_ROW_ENABLED.key -> "true")
  }

  /**
   * Benchmark columnar to row conversion for primitive types.
   */
  def primitiveTypesBenchmark(values: Int): Unit = {
    val benchmark =
      new Benchmark("Columnar to Row - Primitive Types", values.toLong, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        // Create a table with various primitive types (includes strings)
        val df = spark
          .range(values.toLong)
          .selectExpr(
            "id as long_col",
            "cast(id as int) as int_col",
            "cast(id as short) as short_col",
            "cast(id as byte) as byte_col",
            "cast(id % 2 as boolean) as bool_col",
            "cast(id as float) as float_col",
            "cast(id as double) as double_col",
            "cast(id as string) as string_col",
            "date_add(to_date('2024-01-01'), cast(id % 365 as int)) as date_col")

        prepareTable(dir, df)
        val query = "SELECT * FROM parquetV1Table"
        addC2RBenchmarkCases(benchmark, query)
        benchmark.run()
      }
    }
  }

  /**
   * Benchmark columnar to row conversion for fixed-width types ONLY (no strings). This tests the
   * fast path in native C2R that pre-allocates buffers.
   */
  def fixedWidthOnlyBenchmark(values: Int): Unit = {
    val benchmark =
      new Benchmark(
        "Columnar to Row - Fixed Width Only (no strings)",
        values.toLong,
        output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        // Create a table with ONLY fixed-width primitive types (no strings!)
        val df = spark
          .range(values.toLong)
          .selectExpr(
            "id as long_col",
            "cast(id as int) as int_col",
            "cast(id as short) as short_col",
            "cast(id as byte) as byte_col",
            "cast(id % 2 as boolean) as bool_col",
            "cast(id as float) as float_col",
            "cast(id as double) as double_col",
            "date_add(to_date('2024-01-01'), cast(id % 365 as int)) as date_col",
            "cast(id * 2 as long) as long_col2",
            "cast(id * 3 as int) as int_col2")

        prepareTable(dir, df)
        val query = "SELECT * FROM parquetV1Table"
        addC2RBenchmarkCases(benchmark, query)
        benchmark.run()
      }
    }
  }

  /**
   * Benchmark columnar to row conversion for string-heavy data.
   */
  def stringTypesBenchmark(values: Int): Unit = {
    val benchmark =
      new Benchmark("Columnar to Row - String Types", values.toLong, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        val df = spark
          .range(values.toLong)
          .selectExpr(
            "id",
            "concat('short_', cast(id % 100 as string)) as short_str",
            "concat('medium_string_value_', cast(id as string), '_with_more_content') as medium_str",
            "repeat(concat('long_', cast(id as string)), 10) as long_str")

        prepareTable(dir, df)
        val query = "SELECT * FROM parquetV1Table"
        addC2RBenchmarkCases(benchmark, query)
        benchmark.run()
      }
    }
  }

  /**
   * Benchmark columnar to row conversion for nested struct types.
   */
  def structTypesBenchmark(values: Int): Unit = {
    val benchmark =
      new Benchmark("Columnar to Row - Struct Types", values.toLong, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        val df = spark
          .range(values.toLong)
          .selectExpr(
            "id",
            // Simple struct
            "named_struct('a', cast(id as int), 'b', cast(id as string)) as simple_struct",
            // Nested struct (2 levels)
            """named_struct(
              'outer_int', cast(id as int),
              'inner', named_struct('x', cast(id as double), 'y', cast(id as string))
            ) as nested_struct""",
            // Deeply nested struct (3 levels)
            """named_struct(
              'level1', named_struct(
                'level2', named_struct(
                  'value', cast(id as int),
                  'name', concat('item_', cast(id as string))
                ),
                'count', cast(id % 100 as int)
              ),
              'id', id
            ) as deep_struct""")

        prepareTable(dir, df)
        val query = "SELECT * FROM parquetV1Table"
        addC2RBenchmarkCases(benchmark, query)
        benchmark.run()
      }
    }
  }

  /**
   * Benchmark columnar to row conversion for array types.
   */
  def arrayTypesBenchmark(values: Int): Unit = {
    val benchmark =
      new Benchmark("Columnar to Row - Array Types", values.toLong, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        val df = spark
          .range(values.toLong)
          .selectExpr(
            "id",
            // Array of primitives
            "array(cast(id as int), cast(id + 1 as int), cast(id + 2 as int)) as int_array",
            // Array of strings
            "array(concat('a_', cast(id as string)), concat('b_', cast(id as string))) as str_array",
            // Longer array
            """array(
              cast(id % 10 as int), cast((id + 1) % 10 as int), cast((id + 2) % 10 as int),
              cast((id + 3) % 10 as int), cast((id + 4) % 10 as int)
            ) as longer_array""")

        prepareTable(dir, df)
        val query = "SELECT * FROM parquetV1Table"
        addC2RBenchmarkCases(benchmark, query)
        benchmark.run()
      }
    }
  }

  /**
   * Benchmark columnar to row conversion for map types.
   */
  def mapTypesBenchmark(values: Int): Unit = {
    val benchmark =
      new Benchmark("Columnar to Row - Map Types", values.toLong, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        val df = spark
          .range(values.toLong)
          .selectExpr(
            "id",
            // Map with string keys and int values
            "map('key1', cast(id as int), 'key2', cast(id + 1 as int)) as str_int_map",
            // Map with int keys and string values
            "map(cast(id % 10 as int), concat('val_', cast(id as string))) as int_str_map",
            // Larger map
            """map(
              'a', cast(id as double),
              'b', cast(id + 1 as double),
              'c', cast(id + 2 as double)
            ) as larger_map""")

        prepareTable(dir, df)
        val query = "SELECT * FROM parquetV1Table"
        addC2RBenchmarkCases(benchmark, query)
        benchmark.run()
      }
    }
  }

  /**
   * Benchmark columnar to row conversion for complex nested types (arrays of structs, maps with
   * array values, etc.)
   */
  def complexNestedTypesBenchmark(values: Int): Unit = {
    val benchmark =
      new Benchmark("Columnar to Row - Complex Nested Types", values.toLong, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        val df = spark
          .range(values.toLong)
          .selectExpr(
            "id",
            // Array of structs
            """array(
              named_struct('id', cast(id as int), 'name', concat('item_', cast(id as string))),
              named_struct('id', cast(id + 1 as int), 'name', concat('item_', cast(id + 1 as string)))
            ) as array_of_structs""",
            // Struct with array field
            """named_struct(
              'values', array(cast(id as int), cast(id + 1 as int), cast(id + 2 as int)),
              'label', concat('label_', cast(id as string))
            ) as struct_with_array""",
            // Map with array values
            """map(
              'scores', array(cast(id % 100 as double), cast((id + 1) % 100 as double)),
              'ranks', array(cast(id % 10 as double))
            ) as map_with_arrays""")

        prepareTable(dir, df)
        val query = "SELECT * FROM parquetV1Table"
        addC2RBenchmarkCases(benchmark, query)
        benchmark.run()
      }
    }
  }

  /**
   * Nested columns that contain nulls at every level: null structs, null arrays and maps, empty
   * arrays, and null elements. The other nested benchmarks use `range`-derived values that are
   * never null, so they only exercise the all-valid fast paths.
   */
  def nullableNestedTypesBenchmark(values: Int): Unit = {
    val benchmark =
      new Benchmark("Columnar to Row - Nullable Nested Types", values.toLong, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        val df = spark
          .range(values.toLong)
          .selectExpr(
            "id",
            // Null parent struct, nullable fields
            """if(id % 7 = 0, null, named_struct(
              'a', if(id % 5 = 0, null, cast(id as int)),
              'b', if(id % 3 = 0, null, concat('s_', cast(id as string))))) as nullable_struct""",
            // Null array, empty array, and null elements
            """case
              when id % 6 = 0 then cast(null as array<int>)
              when id % 6 = 1 then cast(array() as array<int>)
              else array(
                cast(id as int), if(id % 3 = 0, null, cast(id + 1 as int)), cast(id + 2 as int))
            end as nullable_int_array""",
            // Null array, with null string elements
            """if(id % 6 = 0, null, array(
              concat('a_', cast(id as string)),
              if(id % 4 = 0, null, concat('b_', cast(id as string))))) as nullable_str_array""",
            // Null map, null values
            """if(id % 8 = 0, null, map(
              'k1', if(id % 3 = 0, null, cast(id as int)),
              'k2', cast(id + 1 as int))) as nullable_map""",
            // Nullable struct nested in a nullable struct
            """if(id % 9 = 0, null, named_struct(
              'inner', if(id % 4 = 0, null, named_struct(
                'x', cast(id as double),
                'y', if(id % 2 = 0, null, concat('y_', cast(id as string))))),
              'n', cast(id % 100 as int))) as nullable_nested_struct""")

        prepareTable(dir, df)
        val query = "SELECT * FROM parquetV1Table"
        addC2RBenchmarkCases(benchmark, query)
        benchmark.run()
      }
    }
  }

  /**
   * Collections of nested elements: arrays of structs, arrays of arrays, structs holding arrays
   * of structs, maps with struct values, and nested collections of decimals, timestamps and
   * binary. Arrays have variable lengths so the offsets are not uniform.
   */
  def nestedCollectionsBenchmark(values: Int): Unit = {
    val benchmark =
      new Benchmark("Columnar to Row - Nested Collections", values.toLong, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        val df = spark
          .range(values.toLong)
          .selectExpr(
            "id",
            // Array of structs, 1 to 4 elements
            """transform(sequence(0, cast(id % 4 as int)), i -> named_struct(
              'id', cast(id as int) + i,
              'name', concat('item_', cast(id as string)),
              'score', cast(id % 100 as double))) as array_of_structs""",
            // Array of arrays
            """transform(sequence(0, cast(id % 3 as int)), i ->
              transform(sequence(0, cast(id % 5 as int)), j -> cast(id as int) + i + j))
              as array_of_arrays""",
            // Struct holding an array of structs
            """named_struct(
              'label', concat('l_', cast(id as string)),
              'items', transform(sequence(0, cast(id % 3 as int)), i -> named_struct(
                'k', cast(id as long) + i,
                'v', concat('v_', cast(i as string))))) as struct_of_array_of_structs""",
            // Map with struct values
            """map(
              'first', named_struct('a', cast(id as int), 'b', concat('x_', cast(id as string))),
              'second', named_struct('a', cast(id + 1 as int), 'b', 'y')) as map_of_structs""",
            // Decimals, timestamps and binary inside nested types
            """named_struct(
              'dec', cast(id as decimal(18, 2)),
              'big_dec', cast(id as decimal(38, 10)),
              'ts', timestamp_micros(id * 1000000),
              'bin', cast(concat('bin_', cast(id as string)) as binary),
              'date', date_add(to_date('2024-01-01'), cast(id % 365 as int))) as struct_of_temporal""",
            """array(
              cast(id as decimal(10, 2)), cast(id + 1 as decimal(10, 2))) as decimal_array""",
            """array(
              timestamp_micros(id * 1000000), timestamp_micros(id * 1000000 + 1)) as ts_array""")

        prepareTable(dir, df)
        val query = "SELECT * FROM parquetV1Table"
        addC2RBenchmarkCases(benchmark, query)
        benchmark.run()
      }
    }
  }

  /**
   * Collections with many elements per row. The other nested benchmarks use 2 to 5 elements,
   * which measures per-row overhead; here the per-element cost dominates.
   */
  def largeCollectionsBenchmark(values: Int): Unit = {
    val benchmark =
      new Benchmark("Columnar to Row - Large Collections", values.toLong, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        val df = spark
          .range(values.toLong)
          .selectExpr(
            "id",
            // 100 ints
            "transform(sequence(1, 100), i -> cast(id % 1000 as int) + i) as big_int_array",
            // 20 strings
            "transform(sequence(1, 20), i -> concat('e', cast(i as string), '_', cast(id as string))) as big_str_array",
            // 20 entries
            "map_from_arrays(transform(sequence(1, 20), i -> cast(i as int)), transform(sequence(1, 20), i -> cast(id + i as double))) as big_map",
            // 10 structs
            "transform(sequence(1, 10), i -> named_struct('i', i, 'v', cast(id + i as long))) as big_struct_array")

        prepareTable(dir, df)
        val query = "SELECT * FROM parquetV1Table"
        addC2RBenchmarkCases(benchmark, query)
        benchmark.run()
      }
    }
  }

  /**
   * Benchmark with wide rows (many columns) to stress test row conversion.
   */
  def wideRowsBenchmark(values: Int): Unit = {
    val benchmark =
      new Benchmark("Columnar to Row - Wide Rows (50 columns)", values.toLong, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        // Generate 50 columns of mixed types
        val columns = (0 until 50).map { i =>
          i % 5 match {
            case 0 => s"cast(id + $i as int) as int_col_$i"
            case 1 => s"cast(id + $i as long) as long_col_$i"
            case 2 => s"cast(id + $i as double) as double_col_$i"
            case 3 => s"concat('str_${i}_', cast(id as string)) as str_col_$i"
            case 4 => s"cast((id + $i) % 2 as boolean) as bool_col_$i"
          }
        }

        val df = spark.range(values.toLong).selectExpr(columns: _*)

        prepareTable(dir, df)
        val query = "SELECT * FROM parquetV1Table"
        addC2RBenchmarkCases(benchmark, query)
        benchmark.run()
      }
    }
  }

  /**
   * Benchmark with dictionary-encoded parquet data (the session default disables dictionary
   * encoding, but real-world parquet data is usually dictionary-encoded).
   */
  def dictionaryEncodedBenchmark(values: Int): Unit = {
    val benchmark =
      new Benchmark(
        "Columnar to Row - Dictionary-encoded strings",
        values.toLong,
        output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        val df = spark
          .range(values.toLong)
          .selectExpr(
            "id",
            "concat('val_', cast(id % 1000 as string)) as low_card_str",
            "concat('city_', cast(id % 100 as string)) as city",
            "concat('cat_', cast(id % 10 as string)) as category")

        // Force dictionary encoding ON for this table only
        df.write
          .option("parquet.enable.dictionary", "true")
          .option("compression", "snappy")
          .parquet(dir.getCanonicalPath + "/parquetV1")
        spark.read
          .parquet(dir.getCanonicalPath + "/parquetV1")
          .createOrReplaceTempView("parquetV1Table")

        val query = "SELECT * FROM parquetV1Table"
        addC2RBenchmarkCases(benchmark, query)
        benchmark.run()
      }
    }
  }

  /**
   * Benchmark where a JVM operator (Scala UDF projection + aggregate) consumes the rows. Unlike
   * the noop() sink, this defeats escape analysis and exercises the WholeStageCodegen fusion
   * asymmetry between the CodegenSupport JVM C2R and the iterator-based native C2R.
   */
  def jvmConsumerBenchmark(values: Int): Unit = {
    val benchmark =
      new Benchmark("Columnar to Row - JVM UDF consumer", values.toLong, output = output)

    spark.udf.register("plus_one", (x: Long) => x + 1)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        val df = spark
          .range(values.toLong)
          .selectExpr(
            "id as long_col",
            "cast(id as int) as int_col",
            "cast(id as double) as double_col",
            "cast(id as string) as string_col")

        prepareTable(dir, df)
        val query =
          "SELECT sum(plus_one(long_col)), min(string_col), sum(double_col) FROM parquetV1Table"
        addC2RBenchmarkCases(benchmark, query)
        benchmark.run()
      }
    }
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val numRows = 1024 * 1024 // 1M rows

    runBenchmark("Columnar to Row Conversion - Fixed Width Only") {
      fixedWidthOnlyBenchmark(numRows)
    }

    runBenchmark("Columnar to Row Conversion - Primitive Types") {
      primitiveTypesBenchmark(numRows)
    }

    runBenchmark("Columnar to Row Conversion - String Types") {
      stringTypesBenchmark(numRows)
    }

    runBenchmark("Columnar to Row Conversion - Struct Types") {
      structTypesBenchmark(numRows)
    }

    runBenchmark("Columnar to Row Conversion - Array Types") {
      arrayTypesBenchmark(numRows)
    }

    runBenchmark("Columnar to Row Conversion - Map Types") {
      mapTypesBenchmark(numRows)
    }

    runBenchmark("Columnar to Row Conversion - Complex Nested Types") {
      complexNestedTypesBenchmark(numRows)
    }

    runBenchmark("Columnar to Row Conversion - Nullable Nested Types") {
      nullableNestedTypesBenchmark(numRows)
    }

    runBenchmark("Columnar to Row Conversion - Nested Collections") {
      nestedCollectionsBenchmark(numRows)
    }

    // 100-element arrays make each row large, so use fewer rows to keep the data set small.
    runBenchmark("Columnar to Row Conversion - Large Collections") {
      largeCollectionsBenchmark(numRows / 8)
    }

    runBenchmark("Columnar to Row Conversion - Wide Rows") {
      wideRowsBenchmark(numRows)
    }

    runBenchmark("Columnar to Row Conversion - Dictionary Encoded") {
      dictionaryEncodedBenchmark(numRows)
    }

    runBenchmark("Columnar to Row Conversion - JVM UDF Consumer") {
      jvmConsumerBenchmark(numRows)
    }
  }
}
