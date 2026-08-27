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

import scala.jdk.CollectionConverters._
import scala.util.{Random, Using}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.{MessageType, Type}
import org.apache.spark.SPARK_VERSION_SHORT
import org.apache.spark.sql.{AnalysisException, CometTestBase, DataFrame, Row, SaveMode}
import org.apache.spark.sql.comet.{CometBatchScanExec, CometNativeScanExec, CometNativeWriteExec, CometScanExec}
import org.apache.spark.sql.execution.{FileSourceScanExec, QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.functions.{array, map, struct, when}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, LongType, MapType, Metadata, MetadataBuilder, StringType, StructField, StructType}

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.isSpark35Plus
import org.apache.comet.testing.{DataGenOptions, FuzzDataGenerator, SchemaGenOptions}

class CometParquetWriterSuite extends CometTestBase {

  import testImplicits._

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

  test(
    "native parquet writer preserves Catalyst nullability and honors field ID write settings") {
    val requiredMetadata = parquetFieldMetadata(11)
    val optionalMetadata = parquetFieldMetadata(22)
    val data = spark
      .range(0, 3)
      .select(
        $"id".as("required_number", requiredMetadata),
        when($"id" === 1L, $"id".cast(StringType)).as("optional_text", optionalMetadata),
        $"id".as("unmapped_number"))

    assert(!data.schema("required_number").nullable)
    assert(data.schema("optional_text").nullable)

    Seq(None, Some(true), Some(false)).foreach { configuredValue =>
      withTempPath { dir =>
        val outputPath = new File(dir, "output.parquet").getAbsolutePath

        withNativeWriter {
          def writeAndVerify(): Unit = {
            val plan = captureWritePlan(path => data.write.parquet(path), outputPath)
            assertHasCometNativeWriteExec(plan)

            val expectedIds = configuredValue.getOrElse(true)
            assertParquetSchemas(outputPath) { schema =>
              val root = schema.asGroupType()
              val required = root.getType("required_number")
              val optional = root.getType("optional_text")
              val unmapped = root.getType("unmapped_number")

              assert(required.getRepetition == Type.Repetition.REQUIRED)
              assert(optional.getRepetition == Type.Repetition.OPTIONAL)
              assert(
                Option(required.getId).map(_.intValue()) ==
                  (if (expectedIds) Some(11) else None))
              assert(
                Option(optional.getId).map(_.intValue()) ==
                  (if (expectedIds) Some(22) else None))
              assert(unmapped.getId == null)
            }
          }

          configuredValue match {
            case Some(enabled) =>
              withSQLConf(SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED.key -> enabled.toString) {
                writeAndVerify()
              }
            case None =>
              assert(spark.conf.get(SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED.key).toBoolean)
              writeAndVerify()
          }
        }
      }
    }
  }

  test("native parquet writer preserves nested and Delta collection field IDs") {
    val detailsMetadata = parquetFieldMetadata(100)
    val requiredChildMetadata = parquetFieldMetadata(101)
    val optionalChildMetadata = parquetFieldMetadata(102)
    val innerMetadata = parquetFieldMetadata(130, "inner.element" -> 131L)
    val tagsMetadata = parquetFieldMetadata(200, "tags.element" -> 201L)
    val attrsMetadata =
      parquetFieldMetadata(300, "attrs.key" -> 301L, "attrs.value" -> 302L)

    val data = spark
      .range(0, 2)
      .select(
        struct(
          $"id".as("required_child", requiredChildMetadata),
          when($"id" === 1L, $"id").as("optional_child", optionalChildMetadata),
          array($"id").as("inner", innerMetadata)).as("details", detailsMetadata),
        array(when($"id" === 1L, $"id")).as("tags", tagsMetadata),
        map($"id".cast(StringType), when($"id" === 1L, $"id")).as("attrs", attrsMetadata))

    Seq(true, false).foreach { writeFieldIds =>
      withTempPath { dir =>
        val outputPath = new File(dir, "output.parquet").getAbsolutePath

        withNativeWriter {
          withSQLConf(SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED.key -> writeFieldIds.toString) {
            val plan = captureWritePlan(path => data.write.parquet(path), outputPath)
            assertHasCometNativeWriteExec(plan)

            assertParquetSchemas(outputPath) { schema =>
              val root = schema.asGroupType()

              def assertField(field: Type, id: Int, nullable: Boolean): Unit = {
                val expectedRepetition =
                  if (nullable) Type.Repetition.OPTIONAL else Type.Repetition.REQUIRED
                assert(field.getRepetition == expectedRepetition)
                assert(Option(field.getId).map(_.intValue()) ==
                  (if (writeFieldIds) Some(id) else None))
              }

              val details = root.getType("details")
              assertField(details, 100, nullable = false)
              val detailsGroup = details.asGroupType()
              assertField(detailsGroup.getType("required_child"), 101, nullable = false)
              assertField(detailsGroup.getType("optional_child"), 102, nullable = true)

              val inner = detailsGroup.getType("inner")
              assertField(inner, 130, nullable = false)
              val innerList = inner.asGroupType().getType(0)
              assert(innerList.getRepetition == Type.Repetition.REPEATED)
              assertField(innerList.asGroupType().getType(0), 131, nullable = false)

              val tags = root.getType("tags")
              assertField(tags, 200, nullable = false)
              val tagsList = tags.asGroupType().getType(0)
              assert(tagsList.getRepetition == Type.Repetition.REPEATED)
              assertField(tagsList.asGroupType().getType(0), 201, nullable = true)

              val attrs = root.getType("attrs")
              assertField(attrs, 300, nullable = false)
              val entries = attrs.asGroupType().getType(0)
              assert(entries.getRepetition == Type.Repetition.REPEATED)
              val entriesGroup = entries.asGroupType()
              assertField(entriesGroup.getType("key"), 301, nullable = false)
              assertField(entriesGroup.getType("value"), 302, nullable = true)
            }
          }
        }

        checkAnswer(spark.read.parquet(outputPath), data)
      }
    }
  }

  test("Spark reads native parquet output by field ID after nested columns are renamed") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath
      val numberMetadata = parquetFieldMetadata(11)
      val textMetadata = parquetFieldMetadata(22)
      val structMetadata = parquetFieldMetadata(100)
      val structChildMetadata = parquetFieldMetadata(101)
      val itemsMetadata = parquetFieldMetadata(200, "original_items.element" -> 201L)
      val itemChildMetadata = parquetFieldMetadata(202)
      val lookupMetadata =
        parquetFieldMetadata(300, "original_lookup.key" -> 301L, "original_lookup.value" -> 302L)
      val data = spark
        .range(1, 3)
        .select(
          $"id".as("original_number", numberMetadata),
          $"id".cast(StringType).as("original_text", textMetadata),
          struct($"id".as("original_child", structChildMetadata))
            .as("original_struct", structMetadata),
          array(struct($"id".as("original_item_child", itemChildMetadata)))
            .as("original_items", itemsMetadata),
          map($"id".cast(StringType), $"id")
            .as("original_lookup", lookupMetadata))

      withNativeWriter {
        withSQLConf(SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED.key -> "true") {
          val plan = captureWritePlan(path => data.write.parquet(path), outputPath)
          assertHasCometNativeWriteExec(plan)
        }
      }

      val renamedSchema = StructType(
        Seq(
          StructField(
            "renamed_lookup",
            MapType(StringType, LongType, valueContainsNull = true),
            nullable = true,
            metadata = parquetFieldMetadata(
              300,
              "renamed_lookup.key" -> 301L,
              "renamed_lookup.value" -> 302L)),
          StructField(
            "renamed_items",
            ArrayType(
              StructType(
                Seq(
                  StructField(
                    "renamed_item_child",
                    LongType,
                    nullable = true,
                    metadata = itemChildMetadata))),
              containsNull = true),
            nullable = true,
            metadata = parquetFieldMetadata(200, "renamed_items.element" -> 201L)),
          StructField(
            "renamed_struct",
            StructType(
              Seq(
                StructField(
                  "renamed_child",
                  LongType,
                  nullable = true,
                  metadata = structChildMetadata))),
            nullable = true,
            metadata = structMetadata),
          StructField("renamed_text", StringType, nullable = true, metadata = textMetadata),
          StructField("renamed_number", LongType, nullable = true, metadata = numberMetadata)))

      // Array element and map key/value names are structural in Spark, so rename the
      // collection fields and the struct field inside the array element instead.
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "false",
        SQLConf.PARQUET_FIELD_ID_READ_ENABLED.key -> "true") {
        checkAnswer(
          spark.read.schema(renamedSchema).parquet(outputPath),
          Seq(
            Row(Map("1" -> 1L), Seq(Row(1L)), Row(1L), "1", 1L),
            Row(Map("2" -> 2L), Seq(Row(2L)), Row(2L), "2", 2L)))
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
          CometConf.getOperatorAllowIncompatConfigKey(classOf[DataWritingCommandExec]) -> "true",
          CometConf.COMET_EXEC_ENABLED.key -> "true",
          SQLConf.PARQUET_COMPRESSION.key -> codec) {

          val plan = captureWritePlan(path => df.write.parquet(path), outputPath)
          assertHasCometNativeWriteExec(plan)
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
        CometConf.getOperatorAllowIncompatConfigKey(classOf[DataWritingCommandExec]) -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        SQLConf.PARQUET_COMPRESSION.key -> "snappy") {

        val plan = captureWritePlan(
          path => df.write.option("parquet.compression", "gzip").parquet(path),
          outputPath)
        assertHasCometNativeWriteExec(plan)
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
        CometConf.getOperatorAllowIncompatConfigKey(classOf[DataWritingCommandExec]) -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        SQLConf.PARQUET_COMPRESSION.key -> "zstd") {

        val plan = captureWritePlan(
          path =>
            df.write
              .option("compression", "gzip")
              .option("parquet.compression", "snappy")
              .parquet(path),
          outputPath)
        assertHasCometNativeWriteExec(plan)
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
        CometConf.getOperatorAllowIncompatConfigKey(classOf[DataWritingCommandExec]) -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        SQLConf.PARQUET_COMPRESSION.key -> "lz4_raw") {

        val plan = captureWritePlan(path => df.write.parquet(path), outputPath)
        assertNoCometNativeWriteExec(plan)
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

  test("parquet write with LEGACY datetime rebase mode falls back to Spark") {
    withTempPath { dir =>
      val df = spark.sql(
        "SELECT id, date'1000-01-01' AS d, timestamp'1000-01-01 00:00:00' AS ts FROM range(10)")

      // The native writer always writes corrected (proleptic Gregorian) values, so a LEGACY
      // write rebase mode must fall back to Spark, which rebases the values and stamps the
      // legacy markers.
      val legacyPath = new File(dir, "legacy.parquet").getAbsolutePath
      withSQLConf(
        CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
        CometConf.getOperatorAllowIncompatConfigKey(classOf[DataWritingCommandExec]) -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> "LEGACY",
        SQLConf.PARQUET_INT96_REBASE_MODE_IN_WRITE.key -> "LEGACY") {

        val plan = captureWritePlan(path => df.write.parquet(path), legacyPath)
        assertNoCometNativeWriteExec(plan)
      }
      checkAnswer(spark.read.parquet(legacyPath), df.collect())

      // The same write with corrected modes stays native.
      val correctedPath = new File(dir, "corrected.parquet").getAbsolutePath
      withSQLConf(
        CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
        CometConf.getOperatorAllowIncompatConfigKey(classOf[DataWritingCommandExec]) -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> "CORRECTED",
        SQLConf.PARQUET_INT96_REBASE_MODE_IN_WRITE.key -> "CORRECTED") {

        val plan = captureWritePlan(path => df.write.parquet(path), correctedPath)
        assertHasCometNativeWriteExec(plan)
      }
      checkAnswer(spark.read.parquet(correctedPath), df.collect())
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
        assertHasCometNativeWriteExec(plan)
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
        assertHasCometNativeWriteExec(plan)
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
        assertHasCometNativeWriteExec(plan)
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
        assertHasCometNativeWriteExec(plan)
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
        assertHasCometNativeWriteExec(plan)

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
        assertHasCometNativeWriteExec(plan)
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
      CometConf.COMET_OPERATOR_DATA_WRITING_COMMAND_ALLOW_INCOMPAT.key -> "true",
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

  private def assertNoCometNativeWriteExec(plan: SparkPlan): Unit = {
    val hasNativeWrite = plan.exists {
      case _: CometNativeWriteExec => true
      case d: DataWritingCommandExec =>
        d.child.exists {
          case _: CometNativeWriteExec => true
          case _ => false
        }
      case _ => false
    }

    assert(
      !hasNativeWrite,
      s"Expected no CometNativeWriteExec in the plan, but found one:\n${plan.treeString}")
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

    val conf = spark.sparkContext.hadoopConfiguration
    partFiles.foreach { partFile =>
      val inputFile = HadoopInputFile.fromPath(new Path(partFile.getAbsolutePath), conf)
      Using.resource(ParquetFileReader.open(inputFile)) { reader =>
        val metadata = reader.getFooter.getFileMetaData.getKeyValueMetaData
        assert(metadata.get("org.apache.spark.version") == SPARK_VERSION_SHORT)
        assert(!metadata.containsKey("org.apache.comet.datetimeRebaseMode"))
      }
    }

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

  private def parquetFieldMetadata(id: Long, nestedIds: (String, Long)*): Metadata = {
    val metadata = new MetadataBuilder().putLong("parquet.field.id", id)
    if (nestedIds.nonEmpty) {
      val nestedMetadata = new MetadataBuilder()
      nestedIds.foreach { case (name, nestedId) => nestedMetadata.putLong(name, nestedId) }
      metadata.putMetadata("parquet.field.nested.ids", nestedMetadata.build())
    }
    metadata.build()
  }

  private def assertParquetSchemas(outputPath: String)(verify: MessageType => Unit): Unit = {
    val conf = spark.sparkContext.hadoopConfiguration
    val partFiles = new File(outputPath).listFiles().filter(_.getName.startsWith("part-"))
    assert(partFiles.nonEmpty, s"No part files found under $outputPath")

    partFiles.foreach { partFile =>
      val inputFile = HadoopInputFile.fromPath(new Path(partFile.getAbsolutePath), conf)
      Using.resource(ParquetFileReader.open(inputFile)) { reader =>
        verify(reader.getFooter.getFileMetaData.getSchema)
      }
    }
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
        CometConf.COMET_OPERATOR_DATA_WRITING_COMMAND_ALLOW_INCOMPAT.key -> "true",
        CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
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
