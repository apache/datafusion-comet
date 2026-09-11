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

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.{CometLocalTopKExec, CometNativeScanExec, CometTakeOrderedAndProjectExec}
import org.apache.spark.sql.execution.{LocalTableScanExec, SparkPlan}
import org.apache.spark.sql.internal.SQLConf

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

  for (partitions <- Seq(1, 3); adaptive <- Seq(false, true)) {
    test(s"local TopK shares the native scan: partitions=$partitions, AQE=$adaptive") {
      withSQLConf(
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
      assert(CometConf.COMET_EXEC_TOPK_FUSION_ENABLED.get())
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

  test("local TopK preserves nulls, ties and small inputs") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withTempPath { path =>
        spark
          .range(0, 24, 1, 2)
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

  test("computed and multiple sort keys retain the existing TopK path") {
    withParquetTable((0 until 20).map(i => (i, 20 - i)), "topk_input") {
      for (order <- Seq("_1 + 1", "_1, _2")) {
        val (_, plan) =
          checkSparkAnswerAndOperator(sql(s"SELECT * FROM topk_input ORDER BY $order LIMIT 5"))
        assert(collect(plan) { case local: CometLocalTopKExec => local }.isEmpty)
      }
    }
  }
}
