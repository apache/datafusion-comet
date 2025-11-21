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

import java.io.File

import org.apache.spark.sql.{CometTestBase, Row}
import org.apache.spark.sql.comet.{CometExec, CometMetricNode, CometNativeWriteExec}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{IntegerType, StringType}

import org.apache.comet.serde.{OperatorOuterClass, QueryPlanSerde}
import org.apache.comet.serde.OperatorOuterClass.Operator

class CometParquetWriterSuite extends CometTestBase {
  import testImplicits._

  test("basic parquet write") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      // Create test data and write it to a temp parquet file first
      withTempPath { inputDir =>
        val inputPath = new File(inputDir, "input.parquet").getAbsolutePath
        val df = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "value")
        df.write.parquet(inputPath)

        withSQLConf(
          CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
          CometConf.COMET_EXPLAIN_NATIVE_ENABLED.key -> "true") {
          val df = spark.read.parquet(inputPath)

          df.write.parquet(outputPath)

          spark.read.parquet(outputPath).show()
          // scalastyle:off
          println(outputPath)

          // Thread.sleep(60000)
          assert(spark.read.parquet(outputPath).count() == 3)
        }
      }
    }
  }

  test("basic parquet write with native writer") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      // Create test data and write it to a temp parquet file first
      withTempPath { inputDir =>
        val inputPath = new File(inputDir, "input.parquet").getAbsolutePath
        val df = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "value")
        df.write.parquet(inputPath)

        // Now use native writer to read and write
        // Collect data as ColumnarBatches (for this small test)
        val inputBatches = spark.read
          .parquet(inputPath)
          .queryExecution
          .executedPlan
          .executeColumnar()
          .collect()
          .iterator

        // Create native plan for ParquetWriter
        val scanOp = OperatorOuterClass.Scan
          .newBuilder()
          .setSource("test_input")
          .setArrowFfiSafe(true)
          .addFields(QueryPlanSerde.serializeDataType(IntegerType).get)
          .addFields(QueryPlanSerde.serializeDataType(StringType).get)
          .build()

        val scanOperator = Operator
          .newBuilder()
          .setPlanId(1)
          .setScan(scanOp)
          .build()

        val writerOp = OperatorOuterClass.ParquetWriter
          .newBuilder()
          .setOutputPath(outputPath)
          .setCompression(OperatorOuterClass.CompressionCodec.Snappy)
          .build()

        val writerOperator = Operator
          .newBuilder()
          .setPlanId(2)
          .addChildren(scanOperator)
          .setParquetWriter(writerOp)
          .build()

        // Execute native write
        val nativeMetrics = CometMetricNode(Map.empty[String, SQLMetric])

        val cometIter = CometExec.getCometIterator(
          Seq(inputBatches),
          2, // numColumns
          writerOperator,
          nativeMetrics,
          1, // numPartitions
          0, // partitionIndex
          broadcastedHadoopConfForEncryption = None,
          encryptedFilePaths = Seq.empty)

        // Consume the iterator
        while (cometIter.hasNext) {
          cometIter.next()
        }
        cometIter.close()

        // Verify the file was written
        assert(new File(outputPath).exists(), s"Output file should exist at $outputPath")
        assert(new File(outputPath).length() > 0, "Output file should not be empty")

        // Verify we can read the data back with Spark (disable Comet for read)
        withSQLConf("spark.comet.enabled" -> "false") {
          val readDf = spark.read.parquet(outputPath)
          val result = readDf.collect().sortBy(_.getInt(0))

          assert(result.length == 3)
          assert(result(0).getInt(0) == 1)
          assert(result(0).getString(1) == "a")
          assert(result(1).getInt(0) == 2)
          assert(result(1).getString(1) == "b")
          assert(result(2).getInt(0) == 3)
          assert(result(2).getString(1) == "c")
        }
      }
    }
  }

  test("end-to-end DataFrame write with CometNativeWriteExec") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      // Create test data and write it to a temp parquet file first
      withTempPath { inputDir =>
        val inputPath = new File(inputDir, "input.parquet").getAbsolutePath
        val df = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "value")
        df.write.parquet(inputPath)

        // Read from parquet to get a columnar plan
        val childPlan = spark.read.parquet(inputPath).queryExecution.executedPlan

        // Create ParquetWriter operator
        val scanOp = OperatorOuterClass.Scan
          .newBuilder()
          .setSource("test_input")
          .setArrowFfiSafe(true)
          .addFields(QueryPlanSerde.serializeDataType(IntegerType).get)
          .addFields(QueryPlanSerde.serializeDataType(StringType).get)
          .build()

        val scanOperator = Operator
          .newBuilder()
          .setPlanId(1)
          .setScan(scanOp)
          .build()

        val writerOp = OperatorOuterClass.ParquetWriter
          .newBuilder()
          .setOutputPath(outputPath)
          .setCompression(OperatorOuterClass.CompressionCodec.Snappy)
          .build()

        val writerOperator = Operator
          .newBuilder()
          .setPlanId(2)
          .addChildren(scanOperator)
          .setParquetWriter(writerOp)
          .build()

        // Create CometNativeWriteExec
        val writeExec = CometNativeWriteExec(writerOperator, childPlan, outputPath)

        // Execute the write
        writeExec.executeColumnar().count()

        // Verify the file was written
        assert(new File(outputPath).exists(), s"Output file should exist at $outputPath")
        assert(new File(outputPath).length() > 0, "Output file should not be empty")

        // Verify we can read the data back (disable Comet for read)
        withSQLConf("spark.comet.enabled" -> "false") {
          val readDf = spark.read.parquet(outputPath)
          val result = readDf.collect().sortBy(_.getInt(0))

          assert(result.length == 3)
          assert(result(0).getInt(0) == 1)
          assert(result(0).getString(1) == "a")
          assert(result(1).getInt(0) == 2)
          assert(result(1).getString(1) == "b")
          assert(result(2).getInt(0) == 3)
          assert(result(2).getString(1) == "c")
        }
      }
    }
  }

  test("verify parquet writer operator serialization") {
    val outputPath = "/tmp/test_output.parquet"

    val writerOp = OperatorOuterClass.ParquetWriter
      .newBuilder()
      .setOutputPath(outputPath)
      .setCompression(OperatorOuterClass.CompressionCodec.Zstd)
      .build()

    val operator = Operator
      .newBuilder()
      .setPlanId(1)
      .setParquetWriter(writerOp)
      .build()

    // Verify the operator can be serialized and deserialized
    val serialized = operator.toByteArray
    val deserialized = Operator.parseFrom(serialized)

    assert(deserialized.hasParquetWriter)
    assert(deserialized.getParquetWriter.getOutputPath == outputPath)
    assert(
      deserialized.getParquetWriter.getCompression == OperatorOuterClass.CompressionCodec.Zstd)
  }
}
