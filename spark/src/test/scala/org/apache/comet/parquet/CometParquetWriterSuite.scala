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

package org.apache.comet.parquet

import java.io.File

import scala.util.Random

import org.apache.spark.sql.{CometTestBase, DataFrame, Row}
import org.apache.spark.sql.comet.{CometBatchScanExec, CometNativeScanExec, CometNativeWriteExec, CometScanExec}
import org.apache.spark.sql.execution.{FileSourceScanExec, QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

import org.apache.comet.CometConf
import org.apache.comet.testing.{DataGenOptions, FuzzDataGenerator, SchemaGenOptions}

class CometParquetWriterSuite extends CometTestBase {

  import testImplicits._

  test("basic parquet write") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      // Create test data and write it to a temp parquet file first
      withTempPath { inputDir =>
        val inputPath = createTestData(inputDir)

        withSQLConf(
          CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Halifax",
          CometConf.COMET_OPERATOR_DATA_WRITING_COMMAND_ALLOW_INCOMPAT.key -> "true",
          CometConf.COMET_EXEC_ENABLED.key -> "true") {

          writeWithCometNativeWriteExec(inputPath, outputPath)

          verifyWrittenFile(outputPath)
        }
      }
    }
  }

  test("basic parquet write with native scan child") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      // Create test data and write it to a temp parquet file first
      withTempPath { inputDir =>
        val inputPath = createTestData(inputDir)

        withSQLConf(
          CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Halifax",
          CometConf.COMET_OPERATOR_DATA_WRITING_COMMAND_ALLOW_INCOMPAT.key -> "true",
          CometConf.COMET_EXEC_ENABLED.key -> "true") {

          withSQLConf(CometConf.COMET_NATIVE_SCAN_IMPL.key -> "native_datafusion") {
            val capturedPlan = writeWithCometNativeWriteExec(inputPath, outputPath)
            capturedPlan.foreach { plan =>
              val hasNativeScan = plan.exists {
                case _: CometNativeScanExec => true
                case _ => false
              }

              assert(
                hasNativeScan,
                s"Expected CometNativeScanExec in the plan, but got:\n${plan.treeString}")
            }

            verifyWrittenFile(outputPath)
          }
        }
      }
    }
  }

  test("basic parquet write with repartition") {
    withTempPath { dir =>
      // Create test data and write it to a temp parquet file first
      withTempPath { inputDir =>
        val inputPath = createTestData(inputDir)
        Seq(true, false).foreach(adaptive => {
          // Create a new output path for each AQE value
          val outputPath = new File(dir, s"output_aqe_$adaptive.parquet").getAbsolutePath

          withSQLConf(
            CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
            "spark.sql.adaptive.enabled" -> adaptive.toString,
            SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Halifax",
            CometConf.getOperatorAllowIncompatConfigKey(
              classOf[DataWritingCommandExec]) -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true") {

            writeWithCometNativeWriteExec(inputPath, outputPath, Some(10))
            verifyWrittenFile(outputPath)
          }
        })
      }
    }
  }

  test("parquet write with array type") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      val df = Seq((1, Seq(1, 2, 3)), (2, Seq(4, 5)), (3, Seq[Int]()), (4, Seq(6, 7, 8, 9)))
        .toDF("id", "values")

      writeComplexTypeData(df, outputPath, 4)
    }
  }

  test("parquet write with struct type") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      val df =
        Seq((1, ("Alice", 30)), (2, ("Bob", 25)), (3, ("Charlie", 35))).toDF("id", "person")

      writeComplexTypeData(df, outputPath, 3)
    }
  }

  // Test for issue #3429: CTAS with UNION fails in Spark 4.x with native writer
  test("parquet write with union - CTAS style") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      withSQLConf(
        CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
        CometConf.getOperatorAllowIncompatConfigKey(classOf[DataWritingCommandExec]) -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true") {

        // Create a DataFrame using UNION - simulating CTAS with UNION pattern
        val df1 = spark.range(1, 5).toDF("id")
        val df2 = spark.range(10, 15).toDF("id")
        val unionDf = df1.union(df2)

        // Write using parquet - this is similar to CTAS
        val plan = captureWritePlan(path => unionDf.write.parquet(path), outputPath)

        // Verify the write completed and data is correct
        val result = spark.read.parquet(outputPath)
        assert(result.count() == 9, "Expected 9 rows (4 + 5)")

        // Verify native write was used
        assertHasCometNativeWriteExec(plan)
      }
    }
  }

  // Corner case: UNION with multiple (3+) DataFrames
  test("parquet write with multiple unions") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      withSQLConf(
        CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
        CometConf.getOperatorAllowIncompatConfigKey(classOf[DataWritingCommandExec]) -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true") {

        val df1 = spark.range(1, 4).toDF("id")
        val df2 = spark.range(10, 13).toDF("id")
        val df3 = spark.range(20, 23).toDF("id")
        val df4 = spark.range(30, 33).toDF("id")
        val unionDf = df1.union(df2).union(df3).union(df4)

        val plan = captureWritePlan(path => unionDf.write.parquet(path), outputPath)

        val result = spark.read.parquet(outputPath)
        assert(result.count() == 12, "Expected 12 rows (3 + 3 + 3 + 3)")

        assertHasCometNativeWriteExec(plan)
      }
    }
  }

  // Corner case: UNION followed by coalesce
  test("parquet write with union and coalesce") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      withSQLConf(
        CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
        CometConf.getOperatorAllowIncompatConfigKey(classOf[DataWritingCommandExec]) -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true") {

        val df1 = spark.range(1, 50).toDF("id")
        val df2 = spark.range(100, 150).toDF("id")
        val unionDf = df1.union(df2).coalesce(2)

        val plan = captureWritePlan(path => unionDf.write.parquet(path), outputPath)

        val result = spark.read.parquet(outputPath)
        assert(result.count() == 98, "Expected 98 rows (49 + 49)")

        assertHasCometNativeWriteExec(plan)
      }
    }
  }

  // Corner case: UNION with filter
  test("parquet write with union and filter") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      withSQLConf(
        CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
        CometConf.getOperatorAllowIncompatConfigKey(classOf[DataWritingCommandExec]) -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true") {

        val df1 = spark.range(1, 10).toDF("id")
        val df2 = spark.range(20, 30).toDF("id")
        val unionDf = df1.union(df2).filter("id % 2 = 0")

        val plan = captureWritePlan(path => unionDf.write.parquet(path), outputPath)

        val result = spark.read.parquet(outputPath)
        // Even numbers: 2,4,6,8 from df1, 20,22,24,26,28 from df2 = 9 rows
        assert(result.count() == 9, "Expected 9 even rows")

        assertHasCometNativeWriteExec(plan)
      }
    }
  }

  // Corner case: UNION with complex types (struct)
  test("parquet write with union of structs") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      withSQLConf(
        CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
        CometConf.getOperatorAllowIncompatConfigKey(classOf[DataWritingCommandExec]) -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true") {

        val df = spark.sql("""
          SELECT 1 as id, named_struct('name', 'Alice', 'age', 30) as person
          UNION ALL
          SELECT 2 as id, named_struct('name', 'Bob', 'age', 25) as person
        """)

        val plan = captureWritePlan(path => df.write.parquet(path), outputPath)

        val result = spark.read.parquet(outputPath)
        assert(result.count() == 2)

        assertHasCometNativeWriteExec(plan)
      }
    }
  }

  // Corner case: Nested UNION (UNION inside subquery)
  test("parquet write with nested union in SQL") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      withSQLConf(
        CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
        CometConf.getOperatorAllowIncompatConfigKey(classOf[DataWritingCommandExec]) -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true") {

        val df = spark.sql("""
          SELECT * FROM (
            SELECT 1 as id UNION ALL SELECT 2 as id
          )
          UNION ALL
          SELECT * FROM (
            SELECT 3 as id UNION ALL SELECT 4 as id
          )
        """)

        val plan = captureWritePlan(path => df.write.parquet(path), outputPath)

        val result = spark.read.parquet(outputPath)
        assert(result.count() == 4)

        assertHasCometNativeWriteExec(plan)
      }
    }
  }

  test("parquet write with map type") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      val df = Seq(
        (1, Map("a" -> 1, "b" -> 2)),
        (2, Map("c" -> 3)),
        (3, Map[String, Int]()),
        (4, Map("d" -> 4, "e" -> 5, "f" -> 6))).toDF("id", "properties")

      writeComplexTypeData(df, outputPath, 4)
    }
  }

  test("parquet write with array of structs") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      val df = Seq(
        (1, Seq(("Alice", 30), ("Bob", 25))),
        (2, Seq(("Charlie", 35))),
        (3, Seq[(String, Int)]())).toDF("id", "people")

      writeComplexTypeData(df, outputPath, 3)
    }
  }

  test("parquet write with struct containing array") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      val df = spark.sql("""
        SELECT
          1 as id,
          named_struct('name', 'Team A', 'scores', array(95, 87, 92)) as team
        UNION ALL SELECT
          2 as id,
          named_struct('name', 'Team B', 'scores', array(88, 91)) as team
        UNION ALL SELECT
          3 as id,
          named_struct('name', 'Team C', 'scores', array(100)) as team
      """)

      writeComplexTypeData(df, outputPath, 3)
    }
  }

  test("parquet write with map with struct values") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      val df = spark.sql("""
        SELECT
          1 as id,
          map('emp1', named_struct('name', 'Alice', 'age', 30),
              'emp2', named_struct('name', 'Bob', 'age', 25)) as employees
        UNION ALL SELECT
          2 as id,
          map('emp3', named_struct('name', 'Charlie', 'age', 35)) as employees
      """)

      writeComplexTypeData(df, outputPath, 2)
    }
  }

  test("parquet write with deeply nested types") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      // Create deeply nested structure: array of maps containing arrays
      val df = spark.sql("""
        SELECT
          1 as id,
          array(
            map('key1', array(1, 2, 3), 'key2', array(4, 5)),
            map('key3', array(6, 7, 8, 9))
          ) as nested_data
        UNION ALL SELECT
          2 as id,
          array(
            map('key4', array(10, 11))
          ) as nested_data
      """)

      writeComplexTypeData(df, outputPath, 2)
    }
  }

  test("parquet write with nullable complex types") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      // Test nulls at various levels
      val df = spark.sql("""
        SELECT
          1 as id,
          array(1, null, 3) as arr_with_nulls,
          named_struct('a', 1, 'b', cast(null as int)) as struct_with_nulls,
          map('x', 1, 'y', cast(null as int)) as map_with_nulls
        UNION ALL SELECT
          2 as id,
          cast(null as array<int>) as arr_with_nulls,
          cast(null as struct<a:int, b:int>) as struct_with_nulls,
          cast(null as map<string, int>) as map_with_nulls
        UNION ALL SELECT
          3 as id,
          array(4, 5, 6) as arr_with_nulls,
          named_struct('a', 7, 'b', 8) as struct_with_nulls,
          map('z', 9) as map_with_nulls
      """)

      writeComplexTypeData(df, outputPath, 3)
    }
  }

  test("parquet write with decimal types within complex types") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      val df = spark.sql("""
        SELECT
          1 as id,
          array(cast(1.23 as decimal(10,2)), cast(4.56 as decimal(10,2))) as decimal_arr,
          named_struct('amount', cast(99.99 as decimal(10,2))) as decimal_struct,
          map('price', cast(19.99 as decimal(10,2))) as decimal_map
        UNION ALL SELECT
          2 as id,
          array(cast(7.89 as decimal(10,2))) as decimal_arr,
          named_struct('amount', cast(0.01 as decimal(10,2))) as decimal_struct,
          map('price', cast(0.50 as decimal(10,2))) as decimal_map
      """)

      writeComplexTypeData(df, outputPath, 2)
    }
  }

  test("parquet write with temporal types within complex types") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      val df = spark.sql("""
        SELECT
          1 as id,
          array(date'2024-01-15', date'2024-02-20') as date_arr,
          named_struct('ts', timestamp'2024-01-15 10:30:00') as ts_struct,
          map('event', timestamp'2024-03-01 14:00:00') as ts_map
        UNION ALL SELECT
          2 as id,
          array(date'2024-06-30') as date_arr,
          named_struct('ts', timestamp'2024-07-04 12:00:00') as ts_struct,
          map('event', timestamp'2024-12-25 00:00:00') as ts_map
      """)

      writeComplexTypeData(df, outputPath, 2)
    }
  }

  test("parquet write with empty arrays and maps") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      val df = Seq(
        (1, Seq[Int](), Map[String, Int]()),
        (2, Seq(1, 2), Map("a" -> 1)),
        (3, Seq[Int](), Map[String, Int]())).toDF("id", "arr", "mp")

      writeComplexTypeData(df, outputPath, 3)
    }
  }

  test("parquet write complex types fuzz test") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      // Generate test data with complex types enabled
      val schema = FuzzDataGenerator.generateSchema(
        SchemaGenOptions(generateArray = true, generateStruct = true, generateMap = true))
      val df = FuzzDataGenerator.generateDataFrame(
        new Random(42),
        spark,
        schema,
        500,
        DataGenOptions(generateNegativeZero = false))

      writeComplexTypeData(df, outputPath, 500)
    }
  }

  private def createTestData(inputDir: File): String = {
    val inputPath = new File(inputDir, "input.parquet").getAbsolutePath
    val schema = FuzzDataGenerator.generateSchema(
      SchemaGenOptions(generateArray = false, generateStruct = false, generateMap = false))
    val df = FuzzDataGenerator.generateDataFrame(
      new Random(42),
      spark,
      schema,
      1000,
      DataGenOptions(generateNegativeZero = false))
    withSQLConf(
      CometConf.COMET_EXEC_ENABLED.key -> "false",
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Denver") {
      df.write.parquet(inputPath)
    }
    inputPath
  }

  /**
   * Captures the execution plan during a write operation.
   *
   * @param writeOp
   *   The write operation to execute (takes output path as parameter)
   * @param outputPath
   *   The path to write to
   * @return
   *   The captured execution plan
   */
  private def captureWritePlan(writeOp: String => Unit, outputPath: String): SparkPlan = {
    var capturedPlan: Option[QueryExecution] = None

    val listener = new org.apache.spark.sql.util.QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        if (funcName == "save" || funcName.contains("command")) {
          capturedPlan = Some(qe)
        }
      }

      override def onFailure(
          funcName: String,
          qe: QueryExecution,
          exception: Exception): Unit = {}
    }

    spark.listenerManager.register(listener)

    try {
      writeOp(outputPath)

      // Wait for listener to be called with timeout
      val maxWaitTimeMs = 15000
      val checkIntervalMs = 100
      val maxIterations = maxWaitTimeMs / checkIntervalMs
      var iterations = 0

      while (capturedPlan.isEmpty && iterations < maxIterations) {
        Thread.sleep(checkIntervalMs)
        iterations += 1
      }

      assert(
        capturedPlan.isDefined,
        s"Listener was not called within ${maxWaitTimeMs}ms - no execution plan captured")

      stripAQEPlan(capturedPlan.get.executedPlan)
    } finally {
      spark.listenerManager.unregister(listener)
    }
  }

  private def assertHasCometNativeWriteExec(plan: SparkPlan): Unit = {
    var nativeWriteCount = 0
    plan.foreach {
      case _: CometNativeWriteExec =>
        nativeWriteCount += 1
      case d: DataWritingCommandExec =>
        d.child.foreach {
          case _: CometNativeWriteExec =>
            nativeWriteCount += 1
          case _ =>
        }
      case _ =>
    }

    assert(
      nativeWriteCount == 1,
      s"Expected exactly one CometNativeWriteExec in the plan, but found $nativeWriteCount:\n${plan.treeString}")
  }

  private def writeWithCometNativeWriteExec(
      inputPath: String,
      outputPath: String,
      num_partitions: Option[Int] = None): Option[SparkPlan] = {
    val df = spark.read.parquet(inputPath)

    val plan = captureWritePlan(
      path => num_partitions.fold(df)(n => df.repartition(n)).write.parquet(path),
      outputPath)

    assertHasCometNativeWriteExec(plan)

    Some(plan)
  }

  private def verifyWrittenFile(outputPath: String): Unit = {
    // Verify the data was written correctly
    val resultDf = spark.read.parquet(outputPath)
    assert(resultDf.count() == 1000, "Expected 1000 rows to be written")

    // Verify multiple part files were created
    val outputDir = new File(outputPath)
    val partFiles = outputDir.listFiles().filter(_.getName.startsWith("part-"))
    // With 1000 rows and default parallelism, we should get multiple partitions
    assert(partFiles.length > 1, "Expected multiple part files to be created")

    // read with and without Comet and compare
    val sparkRows = readSparkRows(outputPath)
    val cometRows = readCometRows(outputPath)
    val schema = spark.read.parquet(outputPath).schema
    compareRows(schema, sparkRows, cometRows)
  }

  private def writeComplexTypeData(
      inputDf: DataFrame,
      outputPath: String,
      expectedRows: Int): Unit = {
    withTempPath { inputDir =>
      val inputPath = new File(inputDir, "input.parquet").getAbsolutePath

      // First write the input data without Comet
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "false",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Denver") {
        inputDf.write.parquet(inputPath)
      }

      // read the generated Parquet file and write with Comet native writer
      withSQLConf(
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        // enable experimental native writes
        CometConf.COMET_OPERATOR_DATA_WRITING_COMMAND_ALLOW_INCOMPAT.key -> "true",
        CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
        // explicitly set scan impl to override CI defaults
        CometConf.COMET_NATIVE_SCAN_IMPL.key -> "auto",
        // Disable unsigned small int safety check for ShortType columns
        CometConf.COMET_PARQUET_UNSIGNED_SMALL_INT_CHECK.key -> "false",
        // use a different timezone to make sure that timezone handling works with nested types
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Halifax") {

        val parquetDf = spark.read.parquet(inputPath)

        // Capture plan and verify CometNativeWriteExec is used
        val plan = captureWritePlan(path => parquetDf.write.parquet(path), outputPath)
        assertHasCometNativeWriteExec(plan)
      }

      // Verify round-trip: read with Spark and Comet, compare results
      val sparkRows = readSparkRows(outputPath)
      val cometRows = readCometRows(outputPath)
      assert(sparkRows.length == expectedRows, s"Expected $expectedRows rows")
      val schema = spark.read.parquet(outputPath).schema
      compareRows(schema, sparkRows, cometRows)
    }
  }

  private def compareRows(
      schema: StructType,
      sparkRows: Array[Row],
      cometRows: Array[Row]): Unit = {
    import scala.jdk.CollectionConverters._
    // Convert collected rows back to DataFrames for checkAnswer
    val sparkDf = spark.createDataFrame(sparkRows.toSeq.asJava, schema)
    val cometDf = spark.createDataFrame(cometRows.toSeq.asJava, schema)
    checkAnswer(sparkDf, cometDf)
  }

  private def hasCometScan(plan: SparkPlan): Boolean = {
    stripAQEPlan(plan).exists {
      case _: CometScanExec => true
      case _: CometNativeScanExec => true
      case _: CometBatchScanExec => true
      case _ => false
    }
  }

  private def hasSparkScan(plan: SparkPlan): Boolean = {
    stripAQEPlan(plan).exists {
      case _: FileSourceScanExec => true
      case _ => false
    }
  }

  private def readSparkRows(path: String): Array[Row] = {
    var rows: Array[Row] = null
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      val df = spark.read.parquet(path)
      val plan = df.queryExecution.executedPlan
      assert(
        hasSparkScan(plan) && !hasCometScan(plan),
        s"Expected Spark scan (not Comet) when COMET_ENABLED=false:\n${plan.treeString}")
      rows = df.collect()
    }
    rows
  }

  private def readCometRows(path: String): Array[Row] = {
    var rows: Array[Row] = null
    withSQLConf(
      CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "true",
      // Override CI setting to use a scan impl that supports complex types
      CometConf.COMET_NATIVE_SCAN_IMPL.key -> "auto") {
      val df = spark.read.parquet(path)
      val plan = df.queryExecution.executedPlan
      assert(
        hasCometScan(plan),
        s"Expected Comet scan when COMET_NATIVE_SCAN_ENABLED=true:\n${plan.treeString}")
      rows = df.collect()
    }
    rows
  }

}
