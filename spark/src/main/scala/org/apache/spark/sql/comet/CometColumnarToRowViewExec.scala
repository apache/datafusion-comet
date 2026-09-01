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

package org.apache.spark.sql.comet

import scala.jdk.CollectionConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{ColumnarToRowTransition, SparkPlan}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.util.Utils

/**
 * A columnar-to-row transition that hands the consumer `ColumnarBatch.rowIterator()` directly
 * instead of materializing an `UnsafeRow` per row.
 *
 * `ColumnarBatch.rowIterator()` returns a single mutable `ColumnarBatchRow` that is advanced over
 * the batch, so each row is a zero-copy view over the underlying Arrow buffers. That makes this
 * operator roughly free, but it is only correct for a consumer that fully consumes a row before
 * requesting the next one and never retains a reference to it.
 *
 * Spark's file write path is such a consumer: `OutputWriter.write`, `FileFormatDataWriter.write`
 * and `WriteTaskStatsTracker.newRow` all take a plain `InternalRow`, and `ParquetWriteSupport`
 * reads fields through `SpecializedGetters` and encodes them immediately. Nothing there requires
 * an `UnsafeRow`, so the `UnsafeProjection` performed by [[CometColumnarToRowExec]] is a copy
 * that the writer only undoes again.
 *
 * This is deliberately NOT a `CodegenSupport` node: whole-stage codegen would generate an
 * `UnsafeRowWriter` loop and reintroduce exactly the copy this operator exists to avoid.
 *
 * Only [[org.apache.comet.rules.EliminateRedundantTransitions]] introduces this operator, and
 * only where it has proven the consumer is a non-retaining one. Do not use it as a general
 * replacement for [[CometColumnarToRowExec]].
 *
 * @param child
 *   The child plan that produces columnar batches
 */
case class CometColumnarToRowViewExec(child: SparkPlan)
    extends ColumnarToRowTransition
    with CometPlan {

  // supportsColumnar requires to be only called on driver side, see also SPARK-37779.
  assert(Utils.isInRunningSparkTask || child.supportsColumnar)

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def nodeName: String = "CometColumnarToRowView"

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "number of input batches"))

  override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val numInputBatches = longMetric("numInputBatches")
    child.executeColumnar().mapPartitionsInternal { batches =>
      batches.flatMap { batch =>
        numInputBatches += 1
        numOutputRows += batch.numRows()
        // `flatMap` does not advance to the next batch until this row iterator is exhausted, so
        // the Arrow buffers the returned rows point at stay live for as long as the rows are used.
        batch.rowIterator().asScala
      }
    }
  }

  override def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}
