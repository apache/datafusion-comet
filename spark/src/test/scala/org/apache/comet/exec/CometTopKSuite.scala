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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{CometTestBase, Row}
import org.apache.spark.sql.comet.{CometLocalTopKExec, CometNativeScanExec, CometTakeOrderedAndProjectExec}
import org.apache.spark.sql.execution.{LocalTableScanExec, SparkPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ByteType, IntegerType, LongType, ShortType}

import org.apache.comet.CometConf
import org.apache.comet.serde.OperatorOuterClass.Operator

class CometTopKSuite extends CometTestBase {

  private def localTopK(plan: SparkPlan): CometLocalTopKExec = {
    val nodes = collect(plan) { case topK: CometLocalTopKExec => topK }
    assert(nodes.size == 1, s"Expected one local TopK:\n$plan")
    nodes.head
  }

  private def operators(op: Operator): Seq[Operator] =
    Seq(op) ++ op.getChildrenList.asScala.toSeq.flatMap(operators)

  for (partitions <- Seq(1, 3); adaptive <- Seq(false, true); enabled <- Seq(false, true)) {
    test(s"local TopK shares the scan: partitions=$partitions, AQE=$adaptive, filter=$enabled") {
      withSQLConf(
        CometConf.COMET_EXEC_TOPK_DYNAMIC_FILTER_ENABLED.key -> enabled.toString,
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
            assert(local.dynamicFilterEnabled == enabled)
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

  for (adaptive <- Seq(false, true); filter <- Seq(false, true)) {
    test(s"disabling fusion retains native TopK: AQE=$adaptive, filter=$filter") {
      assert(CometConf.COMET_EXEC_TOPK_FUSION_ENABLED.get())
      withSQLConf(
        CometConf.COMET_EXEC_TOPK_FUSION_ENABLED.key -> "false",
        CometConf.COMET_EXEC_TOPK_DYNAMIC_FILTER_ENABLED.key -> filter.toString,
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
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key -> partitions.toString,
        SQLConf.FILES_MAX_PARTITION_BYTES.key -> "4194304",
        CometConf.COMET_EXEC_TOPK_DYNAMIC_FILTER_ENABLED.key -> "true") {
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

  test("computed and multiple sort keys retain the existing TopK path") {
    withParquetTable((0 until 20).map(i => (i, 20 - i)), "topk_input") {
      for (order <- Seq("_1 + 1", "_1, _2")) {
        val (_, plan) =
          checkSparkAnswerAndOperator(sql(s"SELECT * FROM topk_input ORDER BY $order LIMIT 5"))
        assert(collect(plan) { case local: CometLocalTopKExec => local }.isEmpty)
      }
    }
  }

  for {
    (keyType, parquetKey) <- Seq(
      (ByteType, "int32 k (INT_8)"),
      (ShortType, "int32 k (INT_16)"),
      (IntegerType, "int32 k"),
      (LongType, "int64 k"))
    pageIndex <- Seq(false, true)
    rowFilter <- Seq(false, true)
  } {
    test(
      s"TopK prunes its Spark Parquet reader: ${keyType.sql}, " +
        s"pageIndex=$pageIndex, rowFilter=$rowFilter") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key -> "1",
        CometConf.COMET_BATCH_SIZE.key -> "100",
        CometConf.COMET_RESPECT_DATAFUSION_CONFIGS.key -> "true",
        "spark.comet.datafusion.execution.parquet.enable_page_index" -> pageIndex.toString,
        CometConf.COMET_PARQUET_ROW_FILTER_PUSHDOWN_ENABLED.key -> rowFilter.toString) {
        withTempDir { dir =>
          val path = new org.apache.hadoop.fs.Path(dir.toURI.toString, "topk-groups.parquet")
          val schema = org.apache.parquet.schema.MessageTypeParser.parseMessageType(s"""
            |message root {
            |  required int64 payload;
            |  required $parquetKey;
            |}
            |""".stripMargin)
          val writer = createParquetWriter(schema, path, rowGroupSize = 1L)
          try {
            // Use the full signed byte range so every supported integer type can
            // share this fixture. The threshold improves 37 -> -11 -> -119;
            // groups 3, 5 and 6 become prunable only after those live updates.
            Seq(
              28 until 128,
              -20 until 80,
              0 until 100,
              -128 until -28,
              -80 until 20,
              20 until 120).flatten.foreach { key =>
              val row = new org.apache.parquet.example.data.simple.SimpleGroup(schema)
              row.add(0, key.toLong)
              if (keyType == LongType) row.add(1, key.toLong) else row.add(1, key)
              writer.write(row)
            }
          } finally {
            writer.close()
          }
          val footer = org.apache.parquet.hadoop.ParquetFileReader.open(
            org.apache.parquet.hadoop.util.HadoopInputFile
              .fromPath(path, spark.sessionState.newHadoopConf()))
          try {
            assert(
              footer.getFooter.getBlocks.asScala.map(_.getRowCount).toSeq == Seq.fill(6)(100L))
          } finally {
            footer.close()
          }
          withParquetTable(path.toString, "topk_groups") {
            assert(spark.table("topk_groups").schema("k").dataType == keyType)
            val scans = Seq.newBuilder[Map[String, Long]]
            Seq(false, true).foreach { enabled =>
              withSQLConf(
                CometConf.COMET_EXEC_TOPK_DYNAMIC_FILTER_ENABLED.key -> enabled.toString) {
                // Project the second physical column to exercise reader predicate remapping.
                val query = sql("SELECT k FROM topk_groups ORDER BY k LIMIT 10")
                val (_, plan) =
                  checkSparkAnswerAndOperator(query, Seq(classOf[CometLocalTopKExec]))
                val local = localTopK(plan)
                assert(local.metrics("output_rows").value == 10L)
                assert(local.metrics("dynamic_filter_reader_filters_attached").value ==
                  (if (enabled) 1L else 0L))
                assert(!local.metrics.contains("bytes_scanned"))
                val scan = collect(plan) { case scan: CometNativeScanExec => scan }.head
                val metrics = scan.metrics.map { case (name, metric) => name -> metric.value }
                // checkSparkAnswerAndOperator executes a copy. Execute this DataFrame
                // twice as well, so the same Spark plan cannot retain a heap threshold.
                val expected = (-128 until -118).map { key =>
                  keyType match {
                    case ByteType => Row(key.toByte)
                    case ShortType => Row(key.toShort)
                    case IntegerType => Row(key)
                    case LongType => Row(key.toLong)
                    case other => fail(s"Unexpected integer key type: $other")
                  }
                }
                checkAnswer(query, expected)
                checkAnswer(query, expected)
                scans += metrics
              }
            }
            val Seq(disabled, enabled) = scans.result()
            assert(disabled("output_rows") == 600L)
            assert(disabled("row_groups_pruned_dynamic_filter") == 0L)
            if (!pageIndex && !rowFilter) {
              assert(enabled("row_groups_pruned_dynamic_filter") >= 3L)
              assert(enabled("output_rows") <= 300L)
              assert(enabled("bytes_scanned") < disabled("bytes_scanned"))
            }
          }
        }
      }
    }
  }

  test("TopK filtering preserves ordered inputs and empty scans") {
    withSQLConf(
      CometConf.COMET_EXEC_TOPK_DYNAMIC_FILTER_ENABLED.key -> "true",
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key ->
        "org.apache.spark.sql.catalyst.optimizer.EliminateSorts") {
      withParquetTable((0 until 20).map(i => (i, 20 - i)), "topk_input") {
        val query = spark
          .table("topk_input")
          .sortWithinPartitions("_1", "_2")
          .orderBy("_1")
          .limit(5)
        val (_, plan) = checkSparkAnswerAndOperator(query)
        val topK = collect(plan) { case topK: CometTakeOrderedAndProjectExec => topK }.head
        assert(topK.orderingSatisfies)
        assert(collect(plan) { case local: CometLocalTopKExec => local }.isEmpty)
      }
      withTempPath { path =>
        spark.range(0).write.parquet(path.getCanonicalPath)
        withParquetTable(path.getCanonicalPath, "topk_empty") {
          checkSparkAnswerAndOperator(sql("SELECT id FROM topk_empty ORDER BY id LIMIT 10"))
        }
      }
    }
  }

  test("TopK filtering is opt-in and belongs to the local native plan") {
    assert(!CometConf.COMET_EXEC_TOPK_DYNAMIC_FILTER_ENABLED.get())
    withParquetTable((0 until 100).map(i => Tuple1(i)), "topk_input") {
      for (enabled <- Seq(false, true)) {
        withSQLConf(CometConf.COMET_EXEC_TOPK_DYNAMIC_FILTER_ENABLED.key -> enabled.toString) {
          val (_, plan) = checkSparkAnswerAndOperator(
            sql("SELECT _1 FROM topk_input ORDER BY _1 LIMIT 5 OFFSET 2"),
            Seq(classOf[CometLocalTopKExec]))
          val local = localTopK(plan)
          assert(local.dynamicFilterEnabled == enabled)
          val block = Operator.parseFrom(local.serializedPlanOpt.plan.get)
          assert(block.getSort.getDynamicFilterEnabled == enabled)
          assert(local.metrics("output_rows").value > 0L)
          if (enabled) {
            assert(local.metrics("dynamic_filter_reader_filters_attached").value > 0L)
          } else {
            assert(local.metrics("dynamic_filter_reader_filters_attached").value == 0L)
          }
          assert(
            local.canonicalized !=
              local.copy(dynamicFilterEnabled = !enabled).canonicalized)
        }
      }
    }
  }

  for (payload <- Seq("ts", "s", "a", "m", "map_keys", "nested")) {
    test(s"TopK preserves TIMESTAMP_MILLIS conversion overflow errors: payload=$payload") {
      def isOverflow(error: Throwable): Boolean =
        Iterator
          .iterate(error)(_.getCause)
          .takeWhile(_ != null)
          .exists(cause => Option(cause.getMessage).exists(_.toLowerCase.contains("overflow")))

      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key -> "1",
        CometConf.COMET_BATCH_SIZE.key -> "16",
        CometConf.COMET_RESPECT_DATAFUSION_CONFIGS.key -> "true",
        "spark.comet.datafusion.execution.parquet.enable_page_index" -> "false",
        CometConf.COMET_PARQUET_ROW_FILTER_PUSHDOWN_ENABLED.key -> "false") {
        withTempDir { dir =>
          val path = new org.apache.hadoop.fs.Path(dir.toURI.toString, "topk-overflow.parquet")
          val schema = org.apache.parquet.schema.MessageTypeParser.parseMessageType("""
            |message root {
            |  required int32 k;
            |  optional int64 ts(TIMESTAMP_MILLIS);
            |  optional group s {
            |    optional int64 ts(TIMESTAMP_MILLIS);
            |  }
            |  optional group a (LIST) {
            |    repeated group list {
            |      optional int64 element(TIMESTAMP_MILLIS);
            |    }
            |  }
            |  optional group m (MAP) {
            |    repeated group key_value {
            |      required int32 key;
            |      optional int64 value(TIMESTAMP_MILLIS);
            |    }
            |  }
            |  optional group map_keys (MAP) {
            |    repeated group key_value {
            |      required int64 key(TIMESTAMP_MILLIS);
            |      optional int32 value;
            |    }
            |  }
            |  optional group nested {
            |    optional group a (LIST) {
            |      repeated group list {
            |        optional group element (MAP) {
            |          repeated group key_value {
            |            required int32 key;
            |            optional int64 value(TIMESTAMP_MILLIS);
            |          }
            |        }
            |      }
            |    }
            |  }
            |}
            |""".stripMargin)
          val writer = createParquetWriter(schema, path, rowGroupSize = 1L)
          try {
            (0 until 200).foreach { key =>
              val row = new org.apache.parquet.example.data.simple.SimpleGroup(schema)
              val millis = if (key < 100) 0L else 92233720368547758L
              row.add(0, key)
              row.add(1, millis)
              row.addGroup("s").add(0, millis)
              row.addGroup("a").addGroup("list").add(0, millis)
              val mapValue = row.addGroup("m").addGroup("key_value")
              mapValue.add(0, 1)
              mapValue.add(1, millis)
              val mapKey = row.addGroup("map_keys").addGroup("key_value")
              mapKey.add(0, millis)
              mapKey.add(1, 1)
              val nested = row
                .addGroup("nested")
                .addGroup("a")
                .addGroup("list")
                .addGroup("element")
                .addGroup("key_value")
              nested.add(0, 1)
              nested.add(1, millis)
              writer.write(row)
            }
          } finally {
            writer.close()
          }
          val footer = org.apache.parquet.hadoop.ParquetFileReader.open(
            org.apache.parquet.hadoop.util.HadoopInputFile
              .fromPath(path, spark.sessionState.newHadoopConf()))
          try {
            assert(footer.getFooter.getBlocks.asScala.map(_.getRowCount).toSeq == Seq(100L, 100L))
          } finally {
            footer.close()
          }
          withParquetTable(path.toString, "topk_overflow") {
            for (enabled <- Seq(false, true)) {
              withSQLConf(
                CometConf.COMET_EXEC_TOPK_DYNAMIC_FILTER_ENABLED.key -> enabled.toString) {
                // Select the entire payload: flattening its timestamp would only test
                // the existing top-level guard and could add a projection above the scan.
                val query = sql(s"SELECT k, $payload FROM topk_overflow ORDER BY k LIMIT 1")
                val local = localTopK(query.queryExecution.executedPlan)
                assert(local.dynamicFilterEnabled == enabled)
                assert(local.child.isInstanceOf[CometNativeScanExec])
                assert(local.executeColumnar().getNumPartitions == 1)
                val (sparkError, cometError) = checkSparkAnswerMaybeThrows(query)
                assert(sparkError.exists(isOverflow), "Spark must reject overflowing timestamps")
                assert(
                  cometError.exists(isOverflow),
                  s"filtering=$enabled must preserve Spark's overflow error; Comet: $cometError")
              }
            }
          }
        }
      }
    }
  }

}
