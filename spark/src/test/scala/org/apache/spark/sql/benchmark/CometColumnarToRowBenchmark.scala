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

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

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
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "2g")

    val sparkSession = SparkSession.builder
      .config(conf)
      .withExtensions(new CometSparkSessionExtensions)
      .getOrCreate()

    // Set default configs
    sparkSession.conf.set(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, "true")
    sparkSession.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
    sparkSession.conf.set(CometConf.COMET_ENABLED.key, "false")
    sparkSession.conf.set(CometConf.COMET_EXEC_ENABLED.key, "false")
    // Disable dictionary encoding to ensure consistent data representation
    sparkSession.conf.set("parquet.enable.dictionary", "false")

    sparkSession
  }

  /**
   * Benchmark columnar to row conversion for primitive types.
   */
  def primitiveTypesBenchmark(values: Int): Unit = {
    val benchmark =
      new Benchmark("Columnar to Row - Primitive Types", values, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        // Create a table with various primitive types
        val df = spark
          .range(values)
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

        // Query that forces columnar to row conversion by using a UDF or collect
        val query = "SELECT * FROM parquetV1Table"

        benchmark.addCase("Spark (ColumnarToRowExec)") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
            spark.sql(query).noop()
          }
        }

        benchmark.addCase("Comet JVM (CometColumnarToRowExec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_COLUMNAR_TO_ROW_ENABLED.key -> "false") {
            spark.sql(query).noop()
          }
        }

        benchmark.addCase("Comet Native (CometNativeColumnarToRowExec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_COLUMNAR_TO_ROW_ENABLED.key -> "true") {
            spark.sql(query).noop()
          }
        }

        benchmark.run()
      }
    }
  }

  /**
   * Benchmark columnar to row conversion for string-heavy data.
   */
  def stringTypesBenchmark(values: Int): Unit = {
    val benchmark =
      new Benchmark("Columnar to Row - String Types", values, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        val df = spark
          .range(values)
          .selectExpr(
            "id",
            "concat('short_', cast(id % 100 as string)) as short_str",
            "concat('medium_string_value_', cast(id as string), '_with_more_content') as medium_str",
            "repeat(concat('long_', cast(id as string)), 10) as long_str")

        prepareTable(dir, df)

        val query = "SELECT * FROM parquetV1Table"

        benchmark.addCase("Spark (ColumnarToRowExec)") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
            spark.sql(query).noop()
          }
        }

        benchmark.addCase("Comet JVM (CometColumnarToRowExec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_COLUMNAR_TO_ROW_ENABLED.key -> "false") {
            spark.sql(query).noop()
          }
        }

        benchmark.addCase("Comet Native (CometNativeColumnarToRowExec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_COLUMNAR_TO_ROW_ENABLED.key -> "true") {
            spark.sql(query).noop()
          }
        }

        benchmark.run()
      }
    }
  }

  /**
   * Benchmark columnar to row conversion for nested struct types.
   */
  def structTypesBenchmark(values: Int): Unit = {
    val benchmark =
      new Benchmark("Columnar to Row - Struct Types", values, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        val df = spark
          .range(values)
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

        benchmark.addCase("Spark (ColumnarToRowExec)") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
            spark.sql(query).noop()
          }
        }

        benchmark.addCase("Comet JVM (CometColumnarToRowExec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_COLUMNAR_TO_ROW_ENABLED.key -> "false") {
            spark.sql(query).noop()
          }
        }

        benchmark.addCase("Comet Native (CometNativeColumnarToRowExec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_COLUMNAR_TO_ROW_ENABLED.key -> "true") {
            spark.sql(query).noop()
          }
        }

        benchmark.run()
      }
    }
  }

  /**
   * Benchmark columnar to row conversion for array types.
   */
  def arrayTypesBenchmark(values: Int): Unit = {
    val benchmark =
      new Benchmark("Columnar to Row - Array Types", values, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        val df = spark
          .range(values)
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

        benchmark.addCase("Spark (ColumnarToRowExec)") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
            spark.sql(query).noop()
          }
        }

        benchmark.addCase("Comet JVM (CometColumnarToRowExec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_COLUMNAR_TO_ROW_ENABLED.key -> "false") {
            spark.sql(query).noop()
          }
        }

        benchmark.addCase("Comet Native (CometNativeColumnarToRowExec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_COLUMNAR_TO_ROW_ENABLED.key -> "true") {
            spark.sql(query).noop()
          }
        }

        benchmark.run()
      }
    }
  }

  /**
   * Benchmark columnar to row conversion for map types.
   */
  def mapTypesBenchmark(values: Int): Unit = {
    val benchmark =
      new Benchmark("Columnar to Row - Map Types", values, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        val df = spark
          .range(values)
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

        benchmark.addCase("Spark (ColumnarToRowExec)") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
            spark.sql(query).noop()
          }
        }

        benchmark.addCase("Comet JVM (CometColumnarToRowExec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_COLUMNAR_TO_ROW_ENABLED.key -> "false") {
            spark.sql(query).noop()
          }
        }

        benchmark.addCase("Comet Native (CometNativeColumnarToRowExec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_COLUMNAR_TO_ROW_ENABLED.key -> "true") {
            spark.sql(query).noop()
          }
        }

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
      new Benchmark("Columnar to Row - Complex Nested Types", values, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        val df = spark
          .range(values)
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

        benchmark.addCase("Spark (ColumnarToRowExec)") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
            spark.sql(query).noop()
          }
        }

        benchmark.addCase("Comet JVM (CometColumnarToRowExec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_COLUMNAR_TO_ROW_ENABLED.key -> "false") {
            spark.sql(query).noop()
          }
        }

        benchmark.addCase("Comet Native (CometNativeColumnarToRowExec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_COLUMNAR_TO_ROW_ENABLED.key -> "true") {
            spark.sql(query).noop()
          }
        }

        benchmark.run()
      }
    }
  }

  /**
   * Benchmark with wide rows (many columns) to stress test row conversion.
   */
  def wideRowsBenchmark(values: Int): Unit = {
    val benchmark =
      new Benchmark("Columnar to Row - Wide Rows (50 columns)", values, output = output)

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

        val df = spark.range(values).selectExpr(columns: _*)

        prepareTable(dir, df)

        val query = "SELECT * FROM parquetV1Table"

        benchmark.addCase("Spark (ColumnarToRowExec)") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
            spark.sql(query).noop()
          }
        }

        benchmark.addCase("Comet JVM (CometColumnarToRowExec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_COLUMNAR_TO_ROW_ENABLED.key -> "false") {
            spark.sql(query).noop()
          }
        }

        benchmark.addCase("Comet Native (CometNativeColumnarToRowExec)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_COLUMNAR_TO_ROW_ENABLED.key -> "true") {
            spark.sql(query).noop()
          }
        }

        benchmark.run()
      }
    }
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val numRows = 1024 * 1024 // 1M rows

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

    runBenchmark("Columnar to Row Conversion - Wide Rows") {
      wideRowsBenchmark(numRows)
    }
  }
}
