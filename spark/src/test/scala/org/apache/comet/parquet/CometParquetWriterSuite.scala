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

import java.io.{File, IOException}

import scala.jdk.CollectionConverters._
import scala.util.{Random, Using}

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.{AnalysisException, CometTestBase, DataFrame, Row, SaveMode}
import org.apache.spark.sql.comet.{CometBatchScanExec, CometNativeScanExec, CometScanExec, CometWriteFilesExec}
import org.apache.spark.sql.execution.{FileSourceScanExec, QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.{SQLHadoopMapReduceCommitProtocol, WriteFilesExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

import org.apache.comet.{CometConf, CometExplainInfo}
import org.apache.comet.CometSparkSessionExtensions.{isSpark35Plus, isSpark40Plus}
import org.apache.comet.testing.{DataGenOptions, FuzzDataGenerator, SchemaGenOptions}

class CometParquetWriterSuite extends CometTestBase {

  import testImplicits._

  /**
   * Native Parquet writes hook into Spark's write path through `WriteFilesExecBase`, which only
   * exists in Spark 4.0+. See `CometWriteFilesExec` and the gate in `CometExecRule`.
   */
  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      assume(isSpark40Plus, "Comet native Parquet writes require Spark 4.0+")
      testFun
    }
  }

  test("partitioned write with empty string partition value") {
    withTempPath { path =>
      Seq(("", 1), ("a", 2))
        .toDF("part", "value")
        .write
        .partitionBy("part")
        .parquet(path.toString)
      Using(FileSystem.get(spark.sparkContext.hadoopConfiguration)) { fs =>
        val partitions = fs
          .listStatus(new Path(path.toString))
          .filter(_.isDirectory)
          .map(_.getPath.getName)
          .sorted
        assert(partitions.contains("part=a"))
        assert(!partitions.contains("part="))
        assert(partitions.count(_.startsWith("part=__HIVE_DEFAULT_PARTITION__")) == 1)
      }
      checkAnswer(spark.read.parquet(path.toString), Row(1, null) :: Row(2, "a") :: Nil)
    }
  }

  test("basic parquet write") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      // Create test data and write it to a temp parquet file first
      withTempPath { inputDir =>
        val inputPath = createTestData(inputDir)

        withSQLConf(
          CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Halifax",
          CometConf.COMET_OPERATOR_WRITE_FILES_ALLOW_INCOMPAT.key -> "true",
          CometConf.COMET_EXEC_ENABLED.key -> "true") {

          writeWithCometWriteFilesExec(inputPath, outputPath)

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
          CometConf.COMET_OPERATOR_WRITE_FILES_ALLOW_INCOMPAT.key -> "true",
          CometConf.COMET_EXEC_ENABLED.key -> "true") {

          val capturedPlan = writeWithCometWriteFilesExec(inputPath, outputPath)
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
            CometConf.getOperatorAllowIncompatConfigKey(classOf[WriteFilesExec]) -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true") {

            writeWithCometWriteFilesExec(inputPath, outputPath, Some(10))
            verifyWrittenFile(outputPath)
          }
        })
      }
    }
  }

  test("parquet write with each supported compression codec") {
    Seq("none", "uncompressed", "snappy", "lz4", "zstd", "gzip").foreach { codec =>
      withTempPath { dir =>
        val outputPath = new File(dir, s"output_$codec.parquet").getAbsolutePath
        val df = spark.range(0, 100).selectExpr("id", "cast(id as string) as name")

        withSQLConf(
          CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
          CometConf.getOperatorAllowIncompatConfigKey(classOf[WriteFilesExec]) -> "true",
          CometConf.COMET_EXEC_ENABLED.key -> "true",
          SQLConf.PARQUET_COMPRESSION.key -> codec) {

          val plan = captureWritePlan(path => df.write.parquet(path), outputPath)
          assertHasCometWriteFilesExec(plan)
        }

        checkAnswer(spark.read.parquet(outputPath), df.collect())
        assertParquetCodec(outputPath, expectedCodecName(codec))
      }
    }
  }

  test("parquet write honors parquet.compression option over SQLConf default") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath
      val df = spark.range(0, 100).selectExpr("id", "cast(id as string) as name")

      withSQLConf(
        CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
        CometConf.getOperatorAllowIncompatConfigKey(classOf[WriteFilesExec]) -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        SQLConf.PARQUET_COMPRESSION.key -> "snappy") {

        val plan = captureWritePlan(
          path => df.write.option("parquet.compression", "gzip").parquet(path),
          outputPath)
        assertHasCometWriteFilesExec(plan)
      }

      checkAnswer(spark.read.parquet(outputPath), df.collect())
      assertParquetCodec(outputPath, CompressionCodecName.GZIP)
    }
  }

  test("parquet write honors compression option over parquet.compression and SQLConf") {
    // Precedence, highest to lowest, matches Spark's ParquetOptions:
    //   `compression` write option > `parquet.compression` write option > spark.sql.parquet.compression.codec
    // Use a distinct wrong codec at each lower layer so any leak surfaces as a codec mismatch.
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath
      val df = spark.range(0, 100).selectExpr("id", "cast(id as string) as name")

      withSQLConf(
        CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
        CometConf.getOperatorAllowIncompatConfigKey(classOf[WriteFilesExec]) -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        SQLConf.PARQUET_COMPRESSION.key -> "zstd") {

        val plan = captureWritePlan(
          path =>
            df.write
              .option("compression", "gzip")
              .option("parquet.compression", "snappy")
              .parquet(path),
          outputPath)
        assertHasCometWriteFilesExec(plan)
      }

      checkAnswer(spark.read.parquet(outputPath), df.collect())
      assertParquetCodec(outputPath, CompressionCodecName.GZIP)
    }
  }

  test("parquet write with unsupported compression codec falls back to Spark") {
    assume(isSpark35Plus, "lz4_raw was added in Spark 3.5")
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath
      val df = spark.range(0, 100).selectExpr("id", "cast(id as string) as name")

      withSQLConf(
        CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
        CometConf.getOperatorAllowIncompatConfigKey(classOf[WriteFilesExec]) -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        SQLConf.PARQUET_COMPRESSION.key -> "lz4_raw") {

        val plan = captureWritePlan(path => df.write.parquet(path), outputPath)
        assertNoCometWriteFilesExec(plan)
      }

      checkAnswer(spark.read.parquet(outputPath), df.collect())
      assertParquetCodec(outputPath, CompressionCodecName.LZ4_RAW)
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

  test("SaveMode.ErrorIfExists writes natively when target does not exist") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath
      val sourcePath = new File(dir, "source.parquet").getAbsolutePath
      withNativeWriter {
        val df = materializeAsCometSource(
          (1 to 100).map(i => (i, s"str_$i")).toDF("id", "name"),
          sourcePath)
        val plan =
          captureWritePlan(p => df.write.mode(SaveMode.ErrorIfExists).parquet(p), outputPath)
        assertHasCometWriteFilesExec(plan)
        checkAnswer(spark.read.parquet(outputPath), df)
      }
    }
  }

  test("SaveMode.ErrorIfExists throws when target directory has data") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath
      val original = (1 to 100).map(i => (i, s"orig_$i")).toDF("id", "name")
      // Pre-populate target with Spark's writer so the second write hits the exists check.
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        original.write.parquet(outputPath)
      }
      val partFilesBefore = listPartFileNames(outputPath)

      withNativeWriter {
        val newDf = (200 to 250).map(i => (i, s"new_$i")).toDF("id", "name")
        intercept[AnalysisException] {
          newDf.write.mode(SaveMode.ErrorIfExists).parquet(outputPath)
        }
      }

      // Original files must remain untouched.
      assert(
        listPartFileNames(outputPath) == partFilesBefore,
        "ErrorIfExists must not modify the target when it already exists")
      checkAnswer(spark.read.parquet(outputPath), original)
    }
  }

  test("SaveMode.Overwrite writes natively when target does not exist") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath
      val sourcePath = new File(dir, "source.parquet").getAbsolutePath
      withNativeWriter {
        val df = materializeAsCometSource(
          (1 to 100).map(i => (i, s"str_$i")).toDF("id", "name"),
          sourcePath)
        val plan = captureWritePlan(p => df.write.mode(SaveMode.Overwrite).parquet(p), outputPath)
        assertHasCometWriteFilesExec(plan)
        checkAnswer(spark.read.parquet(outputPath), df)
      }
    }
  }

  test("SaveMode.Overwrite replaces existing parquet data") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath
      val sourcePath = new File(dir, "source.parquet").getAbsolutePath
      val original = (1 to 200).map(i => (i, s"old_$i")).toDF("id", "name")
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        original.write.parquet(outputPath)
      }

      withNativeWriter {
        val replacement = materializeAsCometSource(
          (1 to 50).map(i => (i, s"new_$i")).toDF("id", "name"),
          sourcePath)
        val plan =
          captureWritePlan(p => replacement.write.mode(SaveMode.Overwrite).parquet(p), outputPath)
        assertHasCometWriteFilesExec(plan)
        checkAnswer(spark.read.parquet(outputPath), replacement)
      }
    }
  }

  test("SaveMode.Overwrite with empty DataFrame clears target") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath
      val original = (1 to 200).map(i => (i, s"str_$i")).toDF("id", "name")
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        original.write.parquet(outputPath)
      }

      withNativeWriter {
        // Empty inline source falls back to Spark's writer (LocalTableScan is not Comet-native);
        // this test asserts the SaveMode.Overwrite + empty semantic, not writer identity.
        original.limit(0).write.mode(SaveMode.Overwrite).parquet(outputPath)
      }

      // Read with explicit schema in case the write produced no part files.
      val readback = spark.read.schema(original.schema).parquet(outputPath)
      assert(readback.count() == 0L, "Overwrite with an empty DataFrame must yield zero rows")
    }
  }

  test("SaveMode.Append writes natively when target does not exist") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath
      val sourcePath = new File(dir, "source.parquet").getAbsolutePath
      withNativeWriter {
        val df = materializeAsCometSource(
          (1 to 100).map(i => (i, s"str_$i")).toDF("id", "name"),
          sourcePath)
        val plan = captureWritePlan(p => df.write.mode(SaveMode.Append).parquet(p), outputPath)
        assertHasCometWriteFilesExec(plan)
        checkAnswer(spark.read.parquet(outputPath), df)
      }
    }
  }

  test("SaveMode.Append adds new files alongside existing data") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath
      val sourcePath = new File(dir, "source.parquet").getAbsolutePath
      val first = (1 to 100).map(i => (i, s"first_$i")).toDF("id", "name")
      // Seed the target with the vanilla Spark writer so Comet's append runs against real files.
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        first.write.parquet(outputPath)
      }
      val filesBefore = listPartFileNames(outputPath)

      withNativeWriter {
        val second = materializeAsCometSource(
          (101 to 150).map(i => (i, s"second_$i")).toDF("id", "name"),
          sourcePath)
        val plan =
          captureWritePlan(p => second.write.mode(SaveMode.Append).parquet(p), outputPath)
        assertHasCometWriteFilesExec(plan)

        val filesAfter = listPartFileNames(outputPath)
        assert(
          filesAfter.size > filesBefore.size,
          s"Append should have added new part files (before=${filesBefore.size}, " +
            s"after=${filesAfter.size})")
        assert(
          filesBefore.subsetOf(filesAfter),
          "Append must not remove the pre-existing part files")
        checkAnswer(spark.read.parquet(outputPath), first.union(second))
      }
    }
  }

  test("SaveMode.Append with empty DataFrame does not lose existing data") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath
      val original = (1 to 100).map(i => (i, s"str_$i")).toDF("id", "name")
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        original.write.parquet(outputPath)
      }
      val partFilesBefore = listPartFileNames(outputPath)

      withNativeWriter {
        val empty = original.limit(0)
        empty.write.mode(SaveMode.Append).parquet(outputPath)
      }

      // Empty append must never lose data; may or may not create empty part files.
      val partFilesAfter = listPartFileNames(outputPath)
      assert(
        partFilesBefore.subsetOf(partFilesAfter),
        "Empty append must not delete existing part files")
      checkAnswer(spark.read.parquet(outputPath), original)
    }
  }

  test("SaveMode.Ignore writes natively when target does not exist") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath
      val sourcePath = new File(dir, "source.parquet").getAbsolutePath
      withNativeWriter {
        val df = materializeAsCometSource(
          (1 to 100).map(i => (i, s"str_$i")).toDF("id", "name"),
          sourcePath)
        val plan = captureWritePlan(p => df.write.mode(SaveMode.Ignore).parquet(p), outputPath)
        assertHasCometWriteFilesExec(plan)
        checkAnswer(spark.read.parquet(outputPath), df)
      }
    }
  }

  test("SaveMode.Ignore is a no-op when target has data") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath
      val original = (1 to 100).map(i => (i, s"orig_$i")).toDF("id", "name")
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        original.write.parquet(outputPath)
      }
      val partFilesBefore = listPartFileNames(outputPath)

      withNativeWriter {
        val other = (200 to 250).map(i => (i, s"other_$i")).toDF("id", "name")
        // InsertIntoHadoopFsRelationCommand.run() skips the write on Ignore + existing target.
        other.write.mode(SaveMode.Ignore).parquet(outputPath)
      }

      assert(
        listPartFileNames(outputPath) == partFilesBefore,
        "Ignore must not add or remove files when the target already exists")
      checkAnswer(spark.read.parquet(outputPath), original)
    }
  }

  test("write creates a _SUCCESS marker") {
    // https://github.com/apache/datafusion-comet/issues/2985 - the marker comes from
    // HadoopMapReduceCommitProtocol.commitJob, which only runs because Comet leaves
    // InsertIntoHadoopFsRelationCommand in the plan.
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath
      withTempPath { srcDir =>
        val df = materializeAsCometSource(
          (1 to 100).map(i => (i, s"n_$i")).toDF("id", "name"),
          new File(srcDir, "src.parquet").getAbsolutePath)
        withNativeWriter {
          val plan = captureWritePlan(p => df.write.parquet(p), outputPath)
          assertHasCometWriteFilesExec(plan)
        }
      }
      assert(
        new File(outputPath, "_SUCCESS").exists(),
        s"Expected a _SUCCESS marker in $outputPath, found: " +
          new File(outputPath).list().mkString(", "))
    }
  }

  test("written file names follow Spark's naming convention") {
    // The file name comes from FileCommitProtocol.newTaskTempFile and must be used verbatim:
    // part-<partition>-<uuid>-c<counter>.<codec>.parquet. Committers that track individual files
    // and tools that parse these names depend on it.
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath
      withTempPath { srcDir =>
        val df = materializeAsCometSource(
          (1 to 100).map(i => (i, s"n_$i")).toDF("id", "name"),
          new File(srcDir, "src.parquet").getAbsolutePath)
        withNativeWriter {
          withSQLConf(SQLConf.PARQUET_COMPRESSION.key -> "snappy") {
            val plan = captureWritePlan(p => df.write.parquet(p), outputPath)
            assertHasCometWriteFilesExec(plan)
          }
        }
      }

      val partFiles = listPartFileNames(outputPath)
      assert(partFiles.nonEmpty, s"No part files written to $outputPath")
      val namePattern =
        """part-\d{5}-[0-9a-f\-]{36}-c\d{3}\.snappy\.parquet""".r
      partFiles.foreach { name =>
        assert(
          namePattern.pattern.matcher(name).matches(),
          s"File name '$name' does not match Spark's part-file naming convention")
      }
    }
  }

  test("INSERT INTO ... SELECT is visible to subsequent reads") {
    // https://github.com/apache/datafusion-comet/issues/3521 - reads returned no rows because the
    // bespoke write path never refreshed the catalog cache. Spark's command does that itself.
    withTable("comet_write_target", "comet_write_source") {
      withNativeWriter {
        sql("CREATE TABLE comet_write_source(id bigint, name string) USING parquet")
        sql("CREATE TABLE comet_write_target(id bigint, name string) USING parquet")
      }
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        sql("INSERT INTO comet_write_source VALUES (1, 'a'), (2, 'b')")
      }
      withNativeWriter {
        sql("INSERT INTO comet_write_target SELECT id, name FROM comet_write_source")
      }
      checkAnswer(spark.table("comet_write_target"), Row(1L, "a") :: Row(2L, "b") :: Nil)
    }
  }

  test("dynamic partition overwrite falls back to Spark") {
    // A dynamic overwrite is a partitioned write, which CometWriteFiles declines - but the
    // consequence of getting it wrong is silent data loss across untouched partitions, so assert
    // the fallback and the semantics explicitly rather than relying on the partitioning check.
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath
      val original = Seq((1, "a"), (2, "b")).toDF("id", "part")
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        original.write.partitionBy("part").parquet(outputPath)
      }

      withNativeWriter {
        withSQLConf(SQLConf.PARTITION_OVERWRITE_MODE.key -> "DYNAMIC") {
          val replacement = Seq((3, "b")).toDF("id", "part")
          val plan = captureWritePlan(
            p => replacement.write.mode(SaveMode.Overwrite).partitionBy("part").parquet(p),
            outputPath)
          assertNoCometWriteFilesExec(plan)
        }
      }

      // part=a is untouched, part=b is replaced: the defining property of a dynamic overwrite.
      checkAnswer(spark.read.parquet(outputPath), Row(1, "a") :: Row(3, "b") :: Nil)
    }
  }

  test("write with maxRecordsPerFile falls back to Spark") {
    // Spark's SingleDirectoryDataWriter rolls a new file every maxRecordsPerFile rows, bumping the
    // -c<counter> suffix. Comet asks the commit protocol for one file per task, so it must decline
    // rather than quietly produce a different file layout.
    Seq(
      "spark.sql.files.maxRecordsPerFile" -> ((df: DataFrame, p: String) => df.write.parquet(p)),
      // The write option takes precedence over the conf in FileFormatWriter, so it must be
      // honored here too - with the conf left at its default of 0.
      "maxRecordsPerFile-option" -> ((df: DataFrame, p: String) =>
        df.write.option("maxRecordsPerFile", "10").parquet(p))).foreach { case (label, write) =>
      withTempPath { dir =>
        val outputPath = new File(dir, "output.parquet").getAbsolutePath
        val sourcePath = new File(dir, "source.parquet").getAbsolutePath
        withNativeWriter {
          val df = materializeAsCometSource(
            (1 to 100).map(i => (i, s"str_$i")).toDF("id", "name").repartition(1),
            sourcePath)
          val confs =
            if (label == "spark.sql.files.maxRecordsPerFile") Seq(label -> "10") else Seq.empty
          withSQLConf(confs: _*) {
            val plan = captureWritePlan(p => write(df, p), outputPath)
            assertNoCometWriteFilesExec(plan)
          }
          checkAnswer(spark.read.parquet(outputPath), df)
        }
        // Spark's writer rolled the 100 rows of the single partition into 10 files of 10 rows.
        assert(
          listPartFileNames(outputPath).size == 10,
          s"$label: expected 10 rolled part files, got ${listPartFileNames(outputPath)}")
      }
    }
  }

  test("empty input still writes a schema-only file (SPARK-23271)") {
    // An empty input must still leave a schema behind for downstream readers: `spark.read.parquet`
    // of the output must see the write's schema, not fail. Comet reaches this in two ways - if the
    // native child has one partition producing no batches, the partition-0 branch of executeTask
    // writes a metadata-only file; if it produces zero partitions, doExecuteWrite swaps in a dummy
    // single-partition RDD to get to the same branch. This test exercises the reachable path
    // (filtered Comet scan yielding an empty batch iterator); the zero-partition swap is defensive
    // because CometWriteFiles.requiresNativeChildren rules out the sources (LocalTableScan) that
    // would otherwise produce a zero-partition RDD.
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath
      val sourcePath = new File(dir, "source.parquet").getAbsolutePath
      withNativeWriter {
        val empty = materializeAsCometSource(
          (1 to 100).map(i => (i, s"str_$i")).toDF("id", "name"),
          sourcePath).where("id < 0")
        val plan = captureWritePlan(p => empty.write.parquet(p), outputPath)
        assertHasCometWriteFilesExec(plan)

        val partFiles = listPartFileNames(outputPath)
        assert(partFiles.size == 1, s"Expected exactly one schema-only part file, got $partFiles")
        // Reading without an explicit schema is the point: the file must carry it.
        val readBack = spark.read.parquet(outputPath)
        assert(readBack.count() == 0L)
        assert(readBack.schema.map(_.name) == Seq("id", "name"))
      }
    }
  }

  test("a failing task aborts, cleans up its staging file, and the retry succeeds") {
    // CometWriteFilesExec.executeTask must call committer.abortTask and rethrow. Injecting the
    // failure through the commit protocol rather than the data lets the write get as far as
    // creating a staging file, so the cleanup is actually observable.
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath
      val sourcePath = new File(dir, "source.parquet").getAbsolutePath
      withNativeWriter {
        val df = materializeAsCometSource(
          (1 to 100).map(i => (i, s"str_$i")).toDF("id", "name").repartition(1),
          sourcePath)

        FailingCommitProtocol.reset()
        try {
          withSQLConf(
            SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key -> classOf[FailingCommitProtocol].getName) {
            FailingCommitProtocol.failOnCommitTask = true
            val e = intercept[Exception] {
              df.write.parquet(outputPath)
            }
            assert(
              causeChain(e).exists(_.getMessage == FailingCommitProtocol.message),
              s"Expected the injected failure to propagate, got: $e")
            assert(
              FailingCommitProtocol.abortTaskCalled,
              "CometWriteFilesExec must call committer.abortTask when the task fails")
          }
        } finally {
          FailingCommitProtocol.reset()
        }

        // The failed job leaves no data behind: no committed part files and no staging tree.
        assert(
          listPartFileNames(outputPath).isEmpty,
          s"A failed write must not leave part files: ${listPartFileNames(outputPath)}")
        assert(
          !new File(outputPath, "_temporary").exists(),
          "A failed write must not leave a _temporary staging directory behind")

        // The same write, without the injected failure, still produces correct output.
        val plan = captureWritePlan(p => df.write.mode(SaveMode.Overwrite).parquet(p), outputPath)
        assertHasCometWriteFilesExec(plan)
        checkAnswer(spark.read.parquet(outputPath), df)
      }
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

  private def withNativeWriter(f: => Unit): Unit = {
    withSQLConf(
      CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
      CometConf.COMET_OPERATOR_WRITE_FILES_ALLOW_INCOMPAT.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Halifax")(f)
  }

  // Persist `df` to `sourcePath` with Comet disabled and return a DataFrame that reads it back,
  // so the source plan is a Comet scan (satisfying CometExecRule.requiresNativeChildren).
  private def materializeAsCometSource(df: DataFrame, sourcePath: String): DataFrame = {
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      df.write.parquet(sourcePath)
    }
    spark.read.parquet(sourcePath)
  }

  /** `t` and everything it wraps: a task failure reaches the driver inside a SparkException. */
  private def causeChain(t: Throwable): Seq[Throwable] =
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null).toSeq

  private def listPartFileNames(dir: String): Set[String] = {
    val outputDir = new File(dir)
    if (!outputDir.exists() || !outputDir.isDirectory) {
      Set.empty
    } else {
      outputDir
        .listFiles()
        .filter(_.getName.startsWith("part-"))
        .map(_.getName)
        .toSet
    }
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

  // CometWriteFilesExec replaces only WriteFilesExec, so it appears in the plan beneath Spark's
  // DataWritingCommandExec. A single `plan.foreach` over the whole tree therefore sees it exactly
  // once; there is no separate command node to inspect.
  private def assertHasCometWriteFilesExec(plan: SparkPlan): Unit = {
    var nativeWriteCount = 0
    plan.foreach {
      case _: CometWriteFilesExec => nativeWriteCount += 1
      case _ =>
    }

    assert(
      nativeWriteCount == 1,
      s"Expected exactly one CometWriteFilesExec in the plan, but found $nativeWriteCount:\n${plan.treeString}")

    // The command is left in the plan on purpose for a fully native write, so it must not be
    // reported as a fallback - otherwise extended explain tells users an accelerated write was
    // not accelerated, and skews the "Comet accelerated N of M operators" count.
    plan.foreach {
      case d: DataWritingCommandExec =>
        val reasons = d.getTagValue(CometExplainInfo.FALLBACK_REASONS).getOrElse(Set.empty)
        assert(
          reasons.isEmpty,
          s"A fully native write must not tag ${d.nodeName} as a fallback, got: $reasons")
      case _ =>
    }
  }

  private def assertNoCometWriteFilesExec(plan: SparkPlan): Unit = {
    val hasNativeWrite = plan.exists {
      case _: CometWriteFilesExec => true
      case _ => false
    }

    assert(
      !hasNativeWrite,
      s"Expected no CometWriteFilesExec in the plan, but found one:\n${plan.treeString}")
  }

  private def writeWithCometWriteFilesExec(
      inputPath: String,
      outputPath: String,
      num_partitions: Option[Int] = None): Option[SparkPlan] = {
    val df = spark.read.parquet(inputPath)

    val plan = captureWritePlan(
      path => num_partitions.fold(df)(n => df.repartition(n)).write.parquet(path),
      outputPath)

    assertHasCometWriteFilesExec(plan)

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

  private def expectedCodecName(sparkCodec: String): CompressionCodecName = sparkCodec match {
    case "none" | "uncompressed" => CompressionCodecName.UNCOMPRESSED
    case "snappy" => CompressionCodecName.SNAPPY
    case "lz4" => CompressionCodecName.LZ4
    case "zstd" => CompressionCodecName.ZSTD
    case "gzip" => CompressionCodecName.GZIP
    case other => fail(s"unexpected codec: $other")
  }

  /**
   * Asserts that every column chunk in every part file under `outputPath` reports `expected` as
   * its compression codec. Reading the data back is not enough on its own: a Parquet reader
   * honors whatever the footer says, so a write that silently ignored the requested codec would
   * still round-trip.
   */
  private def assertParquetCodec(outputPath: String, expected: CompressionCodecName): Unit = {
    val conf = spark.sparkContext.hadoopConfiguration
    val partFiles = new File(outputPath).listFiles().filter(_.getName.startsWith("part-"))
    assert(partFiles.nonEmpty, s"No part files found under $outputPath")

    partFiles.foreach { partFile =>
      val inputFile = HadoopInputFile.fromPath(new Path(partFile.getAbsolutePath), conf)
      Using.resource(ParquetFileReader.open(inputFile)) { reader =>
        val codecs = reader.getFooter.getBlocks.asScala
          .flatMap(_.getColumns.asScala)
          .map(_.getCodec)
          .toSet
        assert(
          codecs == Set(expected),
          s"Expected all column chunks in ${partFile.getName} to use $expected, found $codecs")
      }
    }
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
        CometConf.COMET_OPERATOR_WRITE_FILES_ALLOW_INCOMPAT.key -> "true",
        CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
        // Disable unsigned small int safety check for ShortType columns
        CometConf.COMET_PARQUET_UNSIGNED_SMALL_INT_CHECK.key -> "false",
        // use a different timezone to make sure that timezone handling works with nested types
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Halifax") {

        val parquetDf = spark.read.parquet(inputPath)

        // Capture plan and verify CometWriteFilesExec is used
        val plan = captureWritePlan(path => parquetDf.write.parquet(path), outputPath)
        assertHasCometWriteFilesExec(plan)
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
    withSQLConf(CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "true") {
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

/**
 * A commit protocol that fails `commitTask` on demand, to exercise the abort branch of
 * `CometWriteFilesExec.executeTask`.
 *
 * Failing at commit rather than mid-write means the task has already asked for a staging file and
 * written it, so `abortTask` has something real to clean up. Spark instantiates this reflectively
 * per job via `spark.sql.sources.commitProtocolClass`, hence the companion object for the flags -
 * the tests run in `local[*]`, so executors share the driver's JVM and see them.
 */
class FailingCommitProtocol(jobId: String, path: String, dynamicPartitionOverwrite: Boolean)
    extends SQLHadoopMapReduceCommitProtocol(jobId, path, dynamicPartitionOverwrite) {

  override def commitTask(
      taskContext: TaskAttemptContext): FileCommitProtocol.TaskCommitMessage = {
    if (FailingCommitProtocol.failOnCommitTask) {
      throw new IOException(FailingCommitProtocol.message)
    }
    super.commitTask(taskContext)
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    FailingCommitProtocol.abortTaskCalled = true
    super.abortTask(taskContext)
  }
}

object FailingCommitProtocol {
  val message = "injected commitTask failure"

  @volatile var failOnCommitTask: Boolean = false
  @volatile var abortTaskCalled: Boolean = false

  def reset(): Unit = {
    failOnCommitTask = false
    abortTaskCalled = false
  }
}
