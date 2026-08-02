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

import org.scalatest.funsuite.AnyFunSuite

import org.apache.comet.serde.OperatorOuterClass
import org.apache.comet.serde.OperatorOuterClass.Operator

class PlanDataInjectorSuite extends AnyFunSuite {

  /** Builds an un-injected IcebergScan operator: hasCommon, zero file_scan_tasks. */
  private def icebergScanOp(metadataLocation: String, scanHashCode: Int): Operator = {
    val common = OperatorOuterClass.IcebergScanCommon
      .newBuilder()
      .setMetadataLocation(metadataLocation)
      .setScanHashCode(scanHashCode)
      .build()
    val icebergScan = OperatorOuterClass.IcebergScan.newBuilder().setCommon(common).build()
    Operator.newBuilder().setIcebergScan(icebergScan).build()
  }

  /** Serialized (commonBytes, partitionBytes) a real CometIcebergNativeScanExec would produce. */
  private def icebergPlanData(
      metadataLocation: String,
      scanHashCode: Int,
      columnNames: Seq[String],
      dataFilePath: String): (Array[Byte], Array[Byte]) = {
    val commonBuilder = OperatorOuterClass.IcebergScanCommon
      .newBuilder()
      .setMetadataLocation(metadataLocation)
      .setScanHashCode(scanHashCode)
    columnNames.foreach { name =>
      commonBuilder.addRequiredSchema(
        OperatorOuterClass.SparkStructField.newBuilder().setName(name).setNullable(true).build())
    }
    val commonBytes = commonBuilder.build().toByteArray

    val partitionBytes = OperatorOuterClass.IcebergScan
      .newBuilder()
      .addFileScanTasks(
        OperatorOuterClass.IcebergFileScanTask.newBuilder().setDataFilePath(dataFilePath).build())
      .build()
      .toByteArray

    (commonBytes, partitionBytes)
  }

  test("injectPlanData leaves a non-scan operator tree unchanged") {
    // An operator with no injectable scan (here, an empty op_struct, but the same holds for
    // Filter/Projection/etc.) must pass through untouched. This exercises the O(1)
    // injectorsByKind miss path (`case _ =>`) that replaced the per-injector canInject walk.
    val child = Operator.newBuilder().setPlanId(2).build()
    val root = Operator.newBuilder().setPlanId(1).addChildren(child).build()

    val result = PlanDataInjector.injectPlanData(root, Map.empty, Map.empty)

    assert(result == root, "non-scan operator tree should be returned unchanged")
    assert(
      result eq root,
      "a tree with nothing to inject should be returned by reference, not rebuilt")
  }

  test("injectPlanData rebuilds only the path to the injected scan") {
    // Operators are immutable protobuf messages, so subtrees that need no injection are shared
    // rather than rebuilt. Only the root-to-scan path may be new.
    val scanOp = icebergScanOp("s3://table/metadata/v1.json", scanHashCode = 111)
    val (commonBytes, partitionBytes) =
      icebergPlanData(
        "s3://table/metadata/v1.json",
        scanHashCode = 111,
        columnNames = Seq("id", "v"),
        dataFilePath = "data.parquet")
    val key = IcebergPlanDataInjector.getKey(scanOp).get

    val filter = Operator.newBuilder().setPlanId(2).addChildren(scanOp).build()
    val untouchedSibling = Operator
      .newBuilder()
      .setPlanId(3)
      .addChildren(Operator.newBuilder().setPlanId(4).build())
      .build()
    val root = Operator
      .newBuilder()
      .setPlanId(1)
      .addChildren(filter)
      .addChildren(untouchedSibling)
      .build()

    val result =
      PlanDataInjector.injectPlanData(root, Map(key -> commonBytes), Map(key -> partitionBytes))

    assert(
      result.getChildren(1) eq untouchedSibling,
      "a sibling subtree with no injectable scan should be shared, not rebuilt")
    assert(result.getChildren(0) ne filter, "the path to the injected scan must be rebuilt")
    val injectedScan = result.getChildren(0).getChildren(0)
    assert(injectedScan.getIcebergScan.getCommon.getRequiredSchemaCount == 2)
    assert(injectedScan.getIcebergScan.getFileScanTasks(0).getDataFilePath == "data.parquet")
    // Everything outside the injected scan is preserved verbatim.
    assert(result.getPlanId == 1)
    assert(result.getChildren(0).getPlanId == 2)
  }

  test("each registered injector is reachable by its opStructCase") {
    // The O(1) lookup keys injectors by opStructCase, so two injectors sharing a kind would
    // silently shadow one another in the map. Guard that every registered injector resolves back
    // to itself via its declared opStructCase (i.e. the kinds are distinct and the map is complete).
    val injectors = Seq(IcebergPlanDataInjector, NativeScanPlanDataInjector)
    val byKind = injectors.map(i => i.opStructCase -> i).toMap
    assert(byKind.size == injectors.size, "injectors must have distinct opStructCase keys")
    injectors.foreach { i =>
      assert(byKind(i.opStructCase) eq i)
    }
    assert(IcebergPlanDataInjector.opStructCase == Operator.OpStructCase.ICEBERG_SCAN)
    assert(NativeScanPlanDataInjector.opStructCase == Operator.OpStructCase.NATIVE_SCAN)
  }

  test("two Iceberg scans of the same table with different scan_hash_code get distinct keys") {
    val targetOp = icebergScanOp("s3://table/metadata/v1.json", scanHashCode = 111)
    val sourceOp = icebergScanOp("s3://table/metadata/v1.json", scanHashCode = 222)

    assert(IcebergPlanDataInjector.getKey(targetOp) != IcebergPlanDataInjector.getKey(sourceOp))
  }

  test("two Iceberg scans of the same table with equal scan_hash_code get the same key") {
    val opA = icebergScanOp("s3://table/metadata/v1.json", scanHashCode = 111)
    val opB = icebergScanOp("s3://table/metadata/v1.json", scanHashCode = 111)

    assert(IcebergPlanDataInjector.getKey(opA) == IcebergPlanDataInjector.getKey(opB))
  }

  test(
    "self-join: scans sharing a metadataLocation but differing scan_hash_code inject their " +
      "own data, not each other's") {
    val targetOp = icebergScanOp("s3://table/metadata/v1.json", scanHashCode = 111)
    val sourceOp = icebergScanOp("s3://table/metadata/v1.json", scanHashCode = 222)

    val (targetCommon, targetPartition) =
      icebergPlanData(
        "s3://table/metadata/v1.json",
        scanHashCode = 111,
        columnNames = Seq("id", "v", "_file", "_pos"),
        dataFilePath = "target.parquet")
    val (sourceCommon, sourcePartition) =
      icebergPlanData(
        "s3://table/metadata/v1.json",
        scanHashCode = 222,
        columnNames = Seq("id", "v"),
        dataFilePath = "source.parquet")

    val targetKey = IcebergPlanDataInjector.getKey(targetOp).get
    val sourceKey = IcebergPlanDataInjector.getKey(sourceOp).get
    val commonByKey = Map(targetKey -> targetCommon, sourceKey -> sourceCommon)
    val partitionByKey = Map(targetKey -> targetPartition, sourceKey -> sourcePartition)

    val injectedTarget = PlanDataInjector.injectPlanData(targetOp, commonByKey, partitionByKey)
    val injectedSource = PlanDataInjector.injectPlanData(sourceOp, commonByKey, partitionByKey)

    assert(
      injectedTarget.getIcebergScan.getCommon.getRequiredSchemaList
        .get(0)
        .getName == "id")
    assert(injectedTarget.getIcebergScan.getCommon.getRequiredSchemaCount == 4)
    assert(injectedTarget.getIcebergScan.getFileScanTasks(0).getDataFilePath == "target.parquet")

    assert(injectedSource.getIcebergScan.getCommon.getRequiredSchemaCount == 2)
    assert(injectedSource.getIcebergScan.getFileScanTasks(0).getDataFilePath == "source.parquet")
  }
}
