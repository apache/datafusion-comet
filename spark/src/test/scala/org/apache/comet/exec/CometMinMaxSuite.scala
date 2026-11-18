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
import org.apache.spark.sql.catalyst.expressions.aggregate.{Final, Partial}
import org.apache.spark.sql.comet.{CometBaseAggregateExec, CometHashAggregateExec, CometNativeScanExec, CometSparkToColumnarExec}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ByteType, IntegerType, LongType, ShortType}

import org.apache.comet.CometConf

class CometMinMaxSuite extends CometTestBase {

  private val attached = "dynamic_filter_minmax_filters_attached"
  private val skipped = "dynamic_filter_minmax_filters_skipped"

  private def aggregates(plan: SparkPlan): Seq[CometBaseAggregateExec] =
    collect(plan) { case aggregate: CometBaseAggregateExec => aggregate }

  private def assertAttachments(plan: SparkPlan, expected: Long): Unit = {
    val nodes = aggregates(plan)
    assert(nodes.nonEmpty, s"Expected native aggregation:\n$plan")
    val (enabled, disabled) = nodes.partition(_.dynamicFilterEnabled)
    assert(enabled.forall(node => node.groupingExpressions.isEmpty && node.modes == Seq(Partial)))
    assert(enabled.map(_.metrics(attached).value).sum == expected)
    assert(enabled.forall(_.metrics(skipped).value == 0L))
    assert(disabled.forall(node =>
      !node.metrics.contains(attached) && !node.metrics.contains(skipped)))
  }

  test("MIN/MAX filtering is opt-in and captured in the native aggregate plan") {
    assert(!CometConf.COMET_EXEC_AGGREGATE_DYNAMIC_FILTER_ENABLED.get())
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withParquetTable(Seq(Tuple1(1), Tuple1(-2)), "minmax_input") {
        for (enabled <- Seq(false, true)) {
          withSQLConf(
            CometConf.COMET_EXEC_AGGREGATE_DYNAMIC_FILTER_ENABLED.key -> enabled.toString) {
            val (_, plan) = checkSparkAnswerAndOperator(
              sql("SELECT MIN(_1) + 1 FROM minmax_input"),
              Seq(classOf[CometHashAggregateExec]))
            val finalAggregate = aggregates(plan).find(_.modes == Seq(Final)).get
            assert(finalAggregate.nativeOp.hasProjection)
            assert(!finalAggregate.dynamicFilterEnabled)
            assert(!finalAggregate.metrics.contains(attached))
            assert(!finalAggregate.metrics.contains(skipped))
            val partial = collect(plan) {
              case aggregate: CometHashAggregateExec if aggregate.modes == Seq(Partial) =>
                aggregate
            }.head
            assert(partial.nativeOp.getHashAgg.getDynamicFilterEnabled == enabled)
            assert(partial.metrics.contains(attached) == enabled)
            assert(partial.metrics.contains(skipped) == enabled)
            withSQLConf(
              CometConf.COMET_EXEC_AGGREGATE_DYNAMIC_FILTER_ENABLED.key -> (!enabled).toString) {
              assert(partial.dynamicFilterEnabled == enabled)
              assert(!finalAggregate.dynamicFilterEnabled)
            }
            val changed = partial.nativeOp.toBuilder
              .setHashAgg(partial.nativeOp.getHashAgg.toBuilder.setDynamicFilterEnabled(!enabled))
              .build()
            assert(partial.canonicalized != partial.copy(nativeOp = changed).canonicalized)
          }
        }
      }
    }
  }

  for ((keyType, parquetKey) <- Seq(
      (ByteType, "int32 k (INT_8)"),
      (ShortType, "int32 k (INT_16)"),
      (IntegerType, "int32 k"),
      (LongType, "int64 k"))) {
    test(s"MIN/MAX prune their Spark Parquet reader for ${keyType.sql}") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key -> "1",
        CometConf.COMET_BATCH_SIZE.key -> "100",
        CometConf.COMET_RESPECT_DATAFUSION_CONFIGS.key -> "true",
        "spark.comet.datafusion.execution.parquet.enable_page_index" -> "false",
        CometConf.COMET_PARQUET_ROW_FILTER_PUSHDOWN_ENABLED.key -> "false") {
        withTempDir { dir =>
          val path = new org.apache.hadoop.fs.Path(dir.toURI.toString, "minmax-groups.parquet")
          val schema = org.apache.parquet.schema.MessageTypeParser.parseMessageType(s"""
            |message root {
            |  required int64 payload;
            |  required $parquetKey;
            |}
            |""".stripMargin)
          val writer = createParquetWriter(schema, path, rowGroupSize = 1L)
          try {
            // MIN improves 28 -> -20 -> -128; MAX reaches 127 in the first group.
            // The second physical column also exercises the reader's column mapping.
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
          withParquetTable(path.toString, "minmax_groups") {
            assert(spark.table("minmax_groups").schema("k").dataType == keyType)
            for (function <- Seq("MIN", "MAX")) {
              val scans = Seq.newBuilder[Map[String, Long]]
              for (enabled <- Seq(false, true)) {
                withSQLConf(
                  CometConf.COMET_EXEC_AGGREGATE_DYNAMIC_FILTER_ENABLED.key -> enabled.toString) {
                  val (_, plan) = checkSparkAnswerAndOperator(
                    sql(s"SELECT $function(k) FROM minmax_groups"),
                    Seq(classOf[CometHashAggregateExec], classOf[CometNativeScanExec]))
                  assertAttachments(plan, if (enabled) 1L else 0L)
                  assert(aggregates(plan).forall(aggregate =>
                    !aggregate.metrics.contains("bytes_scanned")))
                  val scan = collect(plan) { case scan: CometNativeScanExec => scan }.head
                  scans += scan.metrics.map { case (name, metric) => name -> metric.value }
                }
              }
              val Seq(disabled, enabled) = scans.result()
              withClue(s"$function(${keyType.sql}): ") {
                assert(disabled("output_rows") == 600L)
                assert(disabled("row_groups_pruned_dynamic_filter") == 0L)
                assert(enabled("row_groups_pruned_dynamic_filter") >= 3L)
                assert(enabled("output_rows") <= 300L)
                assert(enabled("bytes_scanned") < disabled("bytes_scanned"))
              }
            }
          }
        }
      }
    }
  }

  for (adaptive <- Seq(false, true)) {
    test(s"MIN/MAX filters stay within Spark partitions with AQE=$adaptive") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
        SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key -> "3",
        SQLConf.FILES_MAX_PARTITION_BYTES.key -> "4194304") {
        withTempPath { path =>
          spark
            .range(0, 120, 1, 3)
            .selectExpr("CAST(119 - id AS INT) AS k")
            .write
            .parquet(path.getCanonicalPath)
          withParquetTable(path.getCanonicalPath, "minmax_partitions") {
            for (enabled <- Seq(false, true); function <- Seq("MIN", "MAX")) {
              withSQLConf(
                CometConf.COMET_EXEC_AGGREGATE_DYNAMIC_FILTER_ENABLED.key -> enabled.toString) {
                val (_, plan) = checkSparkAnswerAndOperator(
                  sql(s"SELECT $function(k) FROM minmax_partitions"),
                  Seq(classOf[CometHashAggregateExec], classOf[CometNativeScanExec]))
                val scan = collect(plan) { case scan: CometNativeScanExec => scan }.head
                assert(scan.executeColumnar().getNumPartitions == 3)
                assertAttachments(plan, if (enabled) 3L else 0L)
              }
            }
          }
        }
      }
    }
  }

  test("unsupported MIN/MAX plans retain ordinary aggregation") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      withParquetTable((0 until 100).map(i => (i - 50, i % 2)), "minmax_input") {
        val queries = Seq(
          "SELECT DISTINCT _2 FROM minmax_input",
          "SELECT MIN(_1), MAX(_1) FROM minmax_input",
          "SELECT MIN(_1), COUNT(DISTINCT _2) FROM minmax_input",
          "SELECT _2, MIN(_1) FROM minmax_input GROUP BY _2",
          "SELECT MIN(_1 + 1) FROM minmax_input",
          "SELECT MIN(CAST(_1 AS BIGINT)) FROM minmax_input",
          "SELECT MIN(_1) FILTER (WHERE _2 = 0) FROM minmax_input",
          "SELECT MIN(_1) FROM minmax_input WHERE _2 = 0",
          "SELECT MIN(_1) FROM (SELECT _1 FROM minmax_input ORDER BY _1 LIMIT 10)")
        for (enabled <- Seq(false, true); query <- queries) {
          withSQLConf(
            CometConf.COMET_EXEC_AGGREGATE_DYNAMIC_FILTER_ENABLED.key -> enabled.toString) {
            withClue(s"enabled=$enabled: $query") {
              val (_, plan) =
                checkSparkAnswerAndOperator(sql(query), Seq(classOf[CometHashAggregateExec]))
              assertAttachments(plan, 0L)
            }
          }
        }
      }
    }
  }

  test("MIN/MAX filtering does not cross a JVM reader boundary") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "false",
      CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_AGGREGATE_DYNAMIC_FILTER_ENABLED.key -> "true") {
      withParquetTable(Seq(Tuple1(-2), Tuple1(1)), "minmax_input") {
        val (_, plan) = checkSparkAnswerAndOperator(
          sql("SELECT MIN(_1) FROM minmax_input"),
          Seq(classOf[CometHashAggregateExec], classOf[CometSparkToColumnarExec]))
        val partial = aggregates(plan).find(_.modes == Seq(Partial)).get
        assert(collect(partial.child) { case bridge: CometSparkToColumnarExec =>
          bridge
        }.nonEmpty)
        assert(collect(plan) { case scan: CometNativeScanExec => scan }.isEmpty)
        assertAttachments(plan, 0L)
      }
    }
  }

  test("MIN/MAX preserve empty, null, and duplicate integer results") {
    val inputs: Seq[Seq[Tuple1[Option[Int]]]] = Seq(
      Seq.empty[Tuple1[Option[Int]]],
      Seq(Tuple1(None), Tuple1(None)),
      Seq(Tuple1(Some(-2)), Tuple1(Some(-2)), Tuple1(None), Tuple1(Some(3))))
    for (input <- inputs) {
      withParquetTable(input, "minmax_input") {
        for (enabled <- Seq(false, true); function <- Seq("MIN", "MAX")) {
          withSQLConf(
            CometConf.COMET_EXEC_AGGREGATE_DYNAMIC_FILTER_ENABLED.key -> enabled.toString) {
            checkSparkAnswerAndOperator(s"SELECT $function(_1) FROM minmax_input")
          }
        }
      }
    }
  }
}
