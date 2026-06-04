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

import scala.collection.JavaConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.columnar.{CachedBatch, CachedBatchSerializer}
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.CometConf
import org.apache.comet.serde.CometOperatorSerde
import org.apache.comet.serde.OperatorOuterClass
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde.serializeDataType

/**
 * Reads Spark cached table data when the cache was written by Comet's cache serializer.
 *
 * Spark stores cached data through `CachedBatchSerializer`. This node keeps the scan inside Comet
 * by asking the serializer to decode cached batches directly into `ColumnarBatch` output,
 * avoiding the extra Spark columnar-to-Comet columnar conversion used by the default path.
 *
 * `relationOutput` is the full schema stored in the cache. `scanOutput` is the subset requested
 * by this scan after pruning.
 */
case class CometInMemoryTableScanExec(
    originalPlan: InMemoryTableScanExec,
    serializer: CachedBatchSerializer,
    cachedBuffers: RDD[CachedBatch],
    relationOutput: Seq[Attribute],
    scanOutput: Seq[Attribute])
    extends CometExec
    with LeafExecNode {

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def output: Seq[Attribute] = originalPlan.output

  // Use the serializer's vector types because the cached batch layout is owned by the serializer.
  override def vectorTypes: Option[Seq[String]] =
    serializer.vectorTypes(scanOutput, conf)

  // Apply Spark's cache batch filter before decoding. Spark's InMemoryTableScanExec does this in
  // filteredCachedBatches(), but that method is private. Reusing the serializer's buildFilter here
  // keeps Comet on the same stats-based pruning path instead of decoding every cached batch.
  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")

    val filteredBuffers =
      if (originalPlan.predicates.nonEmpty) {
        val filter = serializer.buildFilter(originalPlan.predicates, relationOutput)
        cachedBuffers.mapPartitionsWithIndex(filter)
      } else {
        cachedBuffers
      }

    serializer
      .convertCachedBatchToColumnarBatch(filteredBuffers, relationOutput, scanOutput, conf)
      .map { cb =>
        numOutputRows += cb.numRows()
        cb
      }
  }
}

object CometInMemoryTableScanExec extends CometOperatorSerde[InMemoryTableScanExec] {

  override def enabledConfig: Option[org.apache.comet.ConfigEntry[Boolean]] =
    Some(CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED)

  override def convert(
      op: InMemoryTableScanExec,
      builder: OperatorOuterClass.Operator.Builder,
      childOp: Operator*): Option[Operator] = {

    // Empty-output scans still need a schema for native planning, so fall back to the cache schema.
    val actualOutput =
      if (op.output.nonEmpty) op.output
      else op.relation.output

    val scanTypes = actualOutput.flatMap(attr => serializeDataType(attr.dataType))

    val scanBuilder = OperatorOuterClass.Scan
      .newBuilder()
      .setSource(op.getClass.getSimpleName)
      .addAllFields(scanTypes.asJava)
      // Cached batches are decoded on the JVM side; the native scan only receives Spark batches.
      .setArrowFfiSafe(false)

    Some(builder.setScan(scanBuilder).build())
  }

  // Reuse Spark's InMemoryRelation metadata so cache materialization, pruning, and storage
  // behavior remain controlled by Spark's cache manager.
  override def createExec(nativeOp: Operator, op: InMemoryTableScanExec): CometNativeExec = {
    val relation = op.relation

    val actualOutput =
      if (op.output.nonEmpty) op.output
      else relation.output

    CometScanWrapper(
      nativeOp,
      CometInMemoryTableScanExec(
        op,
        relation.cacheBuilder.serializer,
        relation.cacheBuilder.cachedColumnBuffers,
        relation.output,
        actualOutput))
  }
}
