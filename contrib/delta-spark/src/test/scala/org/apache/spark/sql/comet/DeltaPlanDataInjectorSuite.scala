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

import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite

import org.apache.comet.contrib.delta.DeltaSparkScanEnvelope
import org.apache.comet.serde.OperatorOuterClass
import org.apache.comet.serde.OperatorOuterClass.Operator

/**
 * Pins the Delta injector to core's split prepare/inject contract: the partition-invariant common
 * is parsed once by [[DeltaPlanDataInjector.prepareCommon]] and shared across tasks through
 * core's memo, and [[DeltaPlanDataInjector.inject]] only merges a partition's file list into it.
 */
class DeltaPlanDataInjectorSuite extends AnyFunSuite {

  private val injector = new DeltaPlanDataInjector

  private def commonScan(sourceKey: String): OperatorOuterClass.DeltaSparkScan =
    OperatorOuterClass.DeltaSparkScan
      .newBuilder()
      .setCommon(OperatorOuterClass.NativeScanCommon.newBuilder().setSource("delta-source"))
      .setDeltaCommon(
        OperatorOuterClass.DeltaSparkScanCommon
          .newBuilder()
          .setTableRoot("file:/tmp/table")
          .setColumnMappingMode("name")
          .setSourceKey(sourceKey))
      .build()

  private def partitionScan(paths: String*): OperatorOuterClass.DeltaSparkScan = {
    val partition = OperatorOuterClass.DeltaSparkFilePartition.newBuilder()
    paths.foreach { path =>
      partition.addPartitionedFile(
        OperatorOuterClass.DeltaSparkPartitionedFile
          .newBuilder()
          .setFile(OperatorOuterClass.SparkPartitionedFile.newBuilder().setFilePath(path)))
    }
    OperatorOuterClass.DeltaSparkScan.newBuilder().setFilePartition(partition).build()
  }

  private def scanOp(scan: OperatorOuterClass.DeltaSparkScan, children: Operator*): Operator = {
    val builder = Operator.newBuilder().setContribScan(DeltaSparkScanEnvelope.pack(scan))
    children.foreach(builder.addChildren)
    builder.build()
  }

  private def filePaths(op: Operator): Seq[String] =
    DeltaSparkScanEnvelope
      .unpack(op)
      .getFilePartition
      .getPartitionedFileList
      .asScala
      .map(_.getFile.getFilePath)
      .toSeq

  test("prepareCommon parses the common half and inject merges only the partition") {
    val common = commonScan("delta_k")
    val prepared = injector.prepareCommon(common.toByteArray)
    assert(prepared == common)

    val op = scanOp(common)
    assert(injector.canInject(op))
    assert(injector.getKey(op).contains("delta_k"))

    val injected =
      injector.inject(op, prepared, partitionScan("a.parquet", "b.parquet").toByteArray)
    val scan = DeltaSparkScanEnvelope.unpack(injected)
    assert(scan.getCommon == common.getCommon)
    assert(scan.getDeltaCommon == common.getDeltaCommon)
    assert(filePaths(injected) == Seq("a.parquet", "b.parquet"))
    // A fully populated scan is never a candidate for a second injection.
    assert(!injector.canInject(injected))
  }

  test("inject leaves the child list untouched so core can walk it") {
    val child = Operator.newBuilder().setPlanId(7).build()
    val op = scanOp(commonScan("delta_k"), child)

    val injected = injector.inject(
      op,
      injector.prepareCommon(commonScan("delta_k").toByteArray),
      partitionScan("a.parquet").toByteArray)

    assert(injected.getChildrenCount == 1)
    assert(injected.getChildren(0) eq child)
  }

  test("core's memo prepares the common once and serves every partition from it") {
    val common = commonScan("delta_k").toByteArray
    val memo = new ConcurrentHashMap[String, PlanDataInjector.PreparedCommon]()

    val first = PlanDataInjector.prepareShared(injector, "delta_k", common, memo)
    val second = PlanDataInjector.prepareShared(injector, "delta_k", common, memo)
    assert(second eq first, "a repeat lookup must reuse the parsed common")
    assert(memo.size == 1)

    val op = scanOp(commonScan("delta_k"))
    val p0 = injector.inject(op, first, partitionScan("p0.parquet").toByteArray)
    val p1 = injector.inject(op, second, partitionScan("p1.parquet").toByteArray)
    assert(filePaths(p0) == Seq("p0.parquet"))
    assert(filePaths(p1) == Seq("p1.parquet"))
  }

  test("core's memo replaces a prepared common whose finalized bytes changed under the key") {
    val memo = new ConcurrentHashMap[String, PlanDataInjector.PreparedCommon]()
    val stale =
      PlanDataInjector.prepareShared(injector, "delta_k", commonScan("delta_k").toByteArray, memo)

    val changed = commonScan("delta_k").toBuilder
      .setDeltaCommon(commonScan("delta_k").getDeltaCommon.toBuilder.setColumnMappingMode("id"))
      .build()
    val fresh = PlanDataInjector.prepareShared(injector, "delta_k", changed.toByteArray, memo)

    assert(fresh ne stale)
    assert(fresh.getDeltaCommon.getColumnMappingMode == "id")
    assert(memo.size == 1, "the stale slot is replaced, not accumulated")
  }
}
