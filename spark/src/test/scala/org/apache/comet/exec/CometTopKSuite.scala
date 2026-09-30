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

package org.apache.comet.exec

import java.net.URI
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.file.Files

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.{CometTestBase, Row}
import org.apache.spark.sql.comet.{CometLocalTopKExec, CometNativeScanExec, CometTakeOrderedAndProjectExec}
import org.apache.spark.sql.execution.{LocalTableScanExec, SparkPlan, TakeOrderedAndProjectExec}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.isSpark40Plus
import org.apache.comet.serde.OperatorOuterClass.Operator

class CometTopKSuite extends CometTestBase {

  private def localTopK(plan: SparkPlan): CometLocalTopKExec = {
    val nodes = collect(plan) { case topK: CometLocalTopKExec => topK }
    assert(nodes.size == 1, s"Expected one local TopK:\n$plan")
    nodes.head
  }

  private def operators(op: Operator): Seq[Operator] =
    Seq(op) ++ op.getChildrenList.asScala.toSeq.flatMap(operators)

  for (partitions <- Seq(1, 3); adaptive <- Seq(false, true)) {
    test(s"local TopK shares the native scan: partitions=$partitions, AQE=$adaptive") {
      withSQLConf(
        CometConf.COMET_EXEC_TOPK_FUSION_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
        SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key -> partitions.toString,
        SQLConf.FILES_MAX_PARTITION_BYTES.key -> "4194304") {
        withTempPath { path =>
          spark
            .range(0, 120, 1, partitions)
            .selectExpr("CAST(119 - id AS INT) AS k", "id AS payload")
            .write
            .parquet(path.getCanonicalPath)
          withParquetTable(path.getCanonicalPath, "topk_input") {
            val (_, plan) = checkSparkAnswerAndOperator(
              sql("SELECT payload FROM topk_input ORDER BY k LIMIT 5 OFFSET 7"),
              Seq(classOf[CometLocalTopKExec], classOf[CometNativeScanExec]))
            val local = localTopK(plan)
            assert(local.limit == 12)
            assert(local.executeColumnar().getNumPartitions == partitions)
            val block = Operator.parseFrom(local.serializedPlanOpt.plan.get)
            assert(block.hasSort && block.getSort.getFetch == 12)
            assert(block.getSort.getSkip == 0)
            assert(operators(block).count(_.hasNativeScan) == 1)
            assert(!operators(block).exists(_.hasScan), "Local TopK must not use Arrow input")
            assert(local.metrics("output_rows").value == 12L * partitions)
            val scans = collect(plan) { case scan: CometNativeScanExec => scan }
            assert(scans.head.metrics("output_rows").value == 120L)
            assert(scans.head.metrics("bytes_scanned").value > 0L)
            val global = collect(plan) { case topK: CometTakeOrderedAndProjectExec => topK }.head
            assert(global.limit == 12 && global.offset == 7)
            val finalPlan = global.finalNativePlan(partitions).get
            assert(finalPlan.hasProjection)
            val finalSelection = finalPlan.getChildren(0)
            if (partitions == 1) {
              assert(!operators(finalPlan).exists(_.hasSort))
              assert(finalSelection.hasLimit)
              assert(finalSelection.getLimit.getLimit == 12)
              assert(finalSelection.getLimit.getOffset == 7)
              assert(
                global
                  .copy(child = local.copy(limit = 13))
                  .finalNativePlan(1)
                  .get
                  .getChildren(0)
                  .hasSort)
              assert(
                global
                  .copy(child = local.copy(sortOrder = Seq.empty))
                  .finalNativePlan(1)
                  .get
                  .getChildren(0)
                  .hasSort)
            } else {
              assert(finalSelection.hasSort)
            }
            assert(local.canonicalized != local.copy(limit = 13).canonicalized)
          }
        }
      }
    }
  }

  for (adaptive <- Seq(false, true)) {
    test(s"disabling fusion retains native TopK: AQE=$adaptive") {
      assert(!CometConf.COMET_EXEC_TOPK_FUSION_ENABLED.get())
      withSQLConf(
        CometConf.COMET_EXEC_TOPK_FUSION_ENABLED.key -> "false",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString) {
        withParquetTable((0 until 20).map(i => (i, 20 - i)), "topk_input") {
          val (_, plan) = checkSparkAnswerAndOperator(
            sql("SELECT _2 FROM topk_input ORDER BY _1 LIMIT 5 OFFSET 2"),
            Seq(classOf[CometTakeOrderedAndProjectExec], classOf[CometNativeScanExec]))
          assert(collect(plan) { case local: CometLocalTopKExec => local }.isEmpty)
          val global = collect(plan) { case topK: CometTakeOrderedAndProjectExec => topK }.head
          assert(global.finalNativePlan(1).get.getChildren(0).hasSort)
        }
      }
    }
  }

  for (partitions <- Seq(1, 2)) {
    test(s"local TopK preserves nulls, ties and small inputs: partitions=$partitions") {
      withSQLConf(
        CometConf.COMET_EXEC_TOPK_FUSION_ENABLED.key -> "true",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key -> partitions.toString,
        SQLConf.FILES_MAX_PARTITION_BYTES.key -> "4194304") {
        withTempPath { path =>
          spark
            .range(0, 24, 1, partitions)
            .selectExpr("CASE WHEN id % 3 = 0 THEN NULL ELSE CAST(id % 5 AS INT) END AS k")
            .write
            .parquet(path.getCanonicalPath)
          withParquetTable(path.getCanonicalPath, "topk_input") {
            for (direction <- Seq("ASC", "DESC"); nulls <- Seq("FIRST", "LAST")) {
              for ((limit, offset) <- Seq((3, 0), (3, 4), (50, 0), (5, 30))) {
                checkSparkAnswerAndOperator(
                  sql(
                    s"SELECT k FROM topk_input ORDER BY k $direction NULLS $nulls " +
                      s"LIMIT $limit OFFSET $offset"),
                  Seq(classOf[CometLocalTopKExec]))
              }
            }
            checkSparkAnswerAndOperator(
              sql("SELECT k FROM topk_input ORDER BY k LIMIT 0"),
              classOf[LocalTableScanExec])
          }
        }
      }
    }

  }

  for (keyType <- Seq("TINYINT", "SMALLINT", "INT", "BIGINT")) {
    test(s"local TopK supports signed $keyType keys and final projections") {
      withSQLConf(CometConf.COMET_EXEC_TOPK_FUSION_ENABLED.key -> "true") {
        withTempPath { path =>
          spark
            .range(-120, 120, 1, 2)
            .selectExpr(s"CAST(id AS $keyType) AS k", "id * 10 AS payload")
            .write
            .parquet(path.getCanonicalPath)
          withParquetTable(path.getCanonicalPath, "topk_input") {
            for (direction <- Seq("ASC", "DESC")) {
              val query = sql(
                s"SELECT payload FROM topk_input ORDER BY k $direction " +
                  "LIMIT 7 OFFSET 3")
              checkSparkAnswerAndOperator(query, Seq(classOf[CometLocalTopKExec]))
              val expected = (if (direction == "ASC") -117L until -110L
                              else
                                116L to 110L by -1L).map(key => Row(key * 10))
              assert(query.collect().toSeq == expected)
              assert(query.collect().toSeq == expected)
            }
          }
        }
      }
    }
  }

  test("fusion leaves computed projections and unsupported sort keys on the existing path") {
    withSQLConf(CometConf.COMET_EXEC_TOPK_FUSION_ENABLED.key -> "true") {
      withParquetTable((0 until 20).map(i => (i, 20 - i, i.toDouble)), "topk_input") {
        val queries = Seq("_1 + 1", "_1, _2", "_3")
          .map(order => s"SELECT * FROM topk_input ORDER BY $order LIMIT 5") :+
          "SELECT _2 + 1 AS adjusted FROM topk_input ORDER BY _1 LIMIT 5"
        for (query <- queries) {
          val (_, plan) =
            checkSparkAnswerAndOperator(sql(query), Seq(classOf[CometTakeOrderedAndProjectExec]))
          assert(collect(plan) { case local: CometLocalTopKExec => local }.isEmpty)
        }
      }
    }
  }

  test("fusion leaves an already ordered input on the existing path") {
    withSQLConf(
      CometConf.COMET_EXEC_TOPK_FUSION_ENABLED.key -> "true",
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
        "org.apache.spark.sql.catalyst.optimizer.EliminateSorts") {
      withParquetTable((0 until 20).map(i => (i, 20 - i)), "topk_input") {
        val query = spark
          .table("topk_input")
          .sortWithinPartitions("_1", "_2")
          .orderBy("_1")
          .limit(5)
        val (_, plan) = checkSparkAnswerAndOperator(query)
        val topK = collect(plan) { case node: CometTakeOrderedAndProjectExec => node }.head
        assert(topK.orderingSatisfies)
        assert(collect(plan) { case local: CometLocalTopKExec => local }.isEmpty)
      }
    }
  }

  test("fusion requires a native scan and preserves Spark fallback") {
    withSQLConf(
      CometConf.COMET_EXEC_TOPK_FUSION_ENABLED.key -> "true",
      CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "false") {
      withParquetTable((0 until 20).map(i => (i, 20 - i)), "topk_input") {
        val (_, plan) =
          checkSparkAnswer(sql("SELECT _2 FROM topk_input ORDER BY _1 LIMIT 5 OFFSET 2"))
        assert(collect(plan) { case topK: TakeOrderedAndProjectExec => topK }.size == 1)
        assert(collect(plan) { case local: CometLocalTopKExec => local }.isEmpty)
      }
    }
  }

  test("fusion preserves empty Parquet input") {
    withSQLConf(CometConf.COMET_EXEC_TOPK_FUSION_ENABLED.key -> "true") {
      withTempPath { path =>
        spark.range(0).write.parquet(path.getCanonicalPath)
        withParquetTable(path.getCanonicalPath, "topk_empty") {
          val query = sql("SELECT id FROM topk_empty ORDER BY id LIMIT 10 OFFSET 3")
          checkSparkAnswerAndOperator(query)
          assert(query.collect().isEmpty)
        }
      }
    }
  }

  test("local TopK preserves partition columns in empty buckets") {
    val session = spark
    import session.implicits._
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.BUCKETING_ENABLED.key -> "true",
      "spark.sql.sources.bucketing.autoBucketedScan.enabled" -> "false") {
      withTable("topk_bucketed_partitioned") {
        // Two rows cannot fill eight buckets, so the native plan must also handle empty scans.
        Seq((1, 10), (2, 20))
          .toDF("id", "p")
          .coalesce(1)
          .write
          .format("parquet")
          .partitionBy("p")
          .bucketBy(8, "id")
          .saveAsTable("topk_bucketed_partitioned")
        for (fusion <- Seq(false, true)) {
          withSQLConf(CometConf.COMET_EXEC_TOPK_FUSION_ENABLED.key -> fusion.toString) {
            for ((columns, expected) <- Seq(
                "id, p" -> Seq(Row(1, 10), Row(2, 20)),
                "p, id" -> Seq(Row(10, 1), Row(20, 2)))) {
              val query =
                sql(s"SELECT $columns FROM topk_bucketed_partitioned ORDER BY p LIMIT 5")
              val plan = query.queryExecution.executedPlan
              val scans = collect(plan) { case scan: CometNativeScanExec => scan }
              assert(scans.size == 1, s"Expected one native scan:\n$plan")
              assert(scans.head.bucketedScan)
              assert(scans.head.perPartitionData.length == 8)
              assert(scans.head.perPartitionFilePaths.count(_.isEmpty) >= 6)
              val topKs = collect(plan) { case topK: CometTakeOrderedAndProjectExec => topK }
              assert(topKs.size == 1, s"Expected one native TopK:\n$plan")
              assert(topKs.head.child.executeColumnar().getNumPartitions == 8)
              val locals = collect(plan) { case local: CometLocalTopKExec => local }
              assert(locals.size == (if (fusion) 1 else 0), s"fusion=$fusion:\n$plan")
              assert(query.collect().toSeq == expected)
            }
          }
        }
      }
    }
  }

  for (corruption <- Seq("footer", "page")) {
    test(s"local TopK preserves the corrupt Parquet $corruption file in read errors") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key -> "2",
        SQLConf.FILES_MAX_PARTITION_BYTES.key -> "4194304",
        SQLConf.IGNORE_CORRUPT_FILES.key -> "false") {
        withTempPath { path =>
          spark
            .range(0, 1000, 1, 2)
            .selectExpr("CAST(id AS INT) AS k")
            .write
            .option("compression", "uncompressed")
            .option("parquet.enable.dictionary", "false")
            .parquet(path.getCanonicalPath)
          val entries = Files.list(path.toPath)
          val files =
            try {
              entries
                .iterator()
                .asScala
                .filter(_.getFileName.toString.endsWith(".parquet"))
                .toSeq
                .sortBy(_.getFileName.toString)
            } finally entries.close()
          assert(files.size == 2)
          val corruptFile = files.last
          val bytes = Files.readAllBytes(corruptFile)
          if (corruption == "footer") {
            // Keep the file length but destroy the footer magic, failing during reader setup.
            java.util.Arrays.fill(bytes, bytes.length - 4, bytes.length, 0xff.toByte)
          } else {
            val footerLength = ByteBuffer
              .wrap(bytes, bytes.length - 8, 4)
              .order(ByteOrder.LITTLE_ENDIAN)
              .getInt()
            assert(bytes.length - 8 - footerLength > 100)
            // Keep the complete footer so setup succeeds; fail while decoding the first page.
            java.util.Arrays.fill(bytes, 4, 100, 0xff.toByte)
          }
          Files.write(corruptFile, bytes)
          withTempView("topk_corrupt_input") {
            spark.read
              .schema("k INT")
              .parquet(path.getCanonicalPath)
              .createOrReplaceTempView("topk_corrupt_input")
            for (fusion <- Seq(false, true)) {
              withSQLConf(CometConf.COMET_EXEC_TOPK_FUSION_ENABLED.key -> fusion.toString) {
                val query = sql("SELECT k FROM topk_corrupt_input ORDER BY k LIMIT 5")
                val plan = query.queryExecution.executedPlan
                val scans = collect(plan) { case scan: CometNativeScanExec => scan }
                assert(scans.size == 1, s"Expected one native scan:\n$plan")
                val partitionPaths = scans.head.perPartitionFilePaths
                assert(partitionPaths.length == 2 && partitionPaths.forall(_.size == 1))
                val expectedPath = partitionPaths.flatten
                  .find(file => new URI(file) == corruptFile.toUri)
                  .getOrElse(fail(s"Missing corrupt file $corruptFile in scan partitions"))
                val topKs = collect(plan) { case topK: CometTakeOrderedAndProjectExec => topK }
                assert(topKs.size == 1, s"Expected one native TopK:\n$plan")
                assert(topKs.head.child.executeColumnar().getNumPartitions == 2)
                val locals = collect(plan) { case local: CometLocalTopKExec => local }
                assert(locals.size == (if (fusion) 1 else 0), s"fusion=$fusion:\n$plan")
                val failure = intercept[Exception](query.collect())
                // Spark 3.x uses a legacy error class, but still exposes the structured path.
                val errorClass =
                  if (isSpark40Plus) "FAILED_READ_FILE.NO_HINT" else "_LEGACY_ERROR_TEMP_2064"
                val readError = causeChain(failure)
                  .collectFirst {
                    case error: SparkThrowable if error.getErrorClass == errorClass => error
                  }
                  .getOrElse(fail(s"Missing $errorClass in ${causeChain(failure)}"))
                // A lower-level cause can mention the file even when the structured path is lost.
                // With one file per partition, the path must name only the corrupt file.
                assert(readError.getMessageParameters.get("path") == expectedPath)
              }
            }
          }
        }
      }
    }
  }
}
