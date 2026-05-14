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

package org.apache.comet.contrib.example

import org.scalatest.funsuite.AnyFunSuite

import org.apache.comet.spi.CometExtensionRegistry

/**
 * Verifies the JVM half of the contrib SPI by going through public API only:
 *
 * * `CometExtensionRegistry.load()` discovers this contrib via its
 * `META-INF/services/org.apache.comet.spi.CometScanRuleExtension` entry. * The discovered
 * extension is the `ExampleScanRuleExtension` defined in this module. * `matchesV1` honours the
 * test-only marker option so a real `CometScanRule.transformV1Scan` integration test could
 * deterministically opt in.
 *
 * Native-side dispatch (the `OpStruct::ContribOp` arm in core's planner that delegates to the
 * example's Rust `NoOpPlanner`) is exercised by core's own integration tests when built with the
 * `contrib-example` Cargo feature on -- not duplicated here.
 */
class ExampleScanRuleExtensionSuite extends AnyFunSuite {

  test("CometExtensionRegistry discovers ExampleScanRuleExtension via ServiceLoader") {
    // The registry caches discovery results across calls; reset so this test sees a
    // deterministic load against the current test classpath.
    CometExtensionRegistry.resetForTesting()
    CometExtensionRegistry.load()

    val found = CometExtensionRegistry.scanExtensions.find(_.name == "example")
    assert(found.isDefined, "ServiceLoader should have discovered the example contrib")
    assert(found.get.isInstanceOf[ExampleScanRuleExtension])
  }

  test("matchesV1 returns true only when the marker option is set") {
    val ext = new ExampleScanRuleExtension

    // We construct a minimal HadoopFsRelation just enough to call matchesV1. The trait
    // method only reads `relation.options` so we don't need a real file format/schema.
    val sparkSession = org.apache.spark.sql.SparkSession
      .builder()
      .master("local[1]")
      .appName("ExampleScanRuleExtensionSuite")
      .getOrCreate()
    try {
      val relationWithoutMarker = new org.apache.spark.sql.execution.datasources.HadoopFsRelation(
        location = new org.apache.spark.sql.execution.datasources.InMemoryFileIndex(
          sparkSession,
          Seq.empty,
          Map.empty,
          None),
        partitionSchema = new org.apache.spark.sql.types.StructType(),
        dataSchema = new org.apache.spark.sql.types.StructType(),
        bucketSpec = None,
        fileFormat = new org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat(),
        options = Map.empty)(sparkSession)
      assert(!ext.matchesV1(relationWithoutMarker), "no marker -> no match")

      val relationWithMarker = relationWithoutMarker.copy(options = Map(
        ExampleScanRuleExtension.MarkerOptionKey ->
          ExampleScanRuleExtension.MarkerOptionValue))(sparkSession)
      assert(ext.matchesV1(relationWithMarker), "marker present -> match")
    } finally {
      sparkSession.stop()
    }
  }
}
