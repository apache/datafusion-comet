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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.HadoopFsRelation

import org.apache.comet.spi.CometScanRuleExtension

/**
 * Worked-reference `CometScanRuleExtension` for new contrib authors. This implementation is
 * intentionally trivial: it does not match any scan and never transforms anything. What it proves
 * end-to-end at runtime is:
 *
 *   1. `META-INF/services/org.apache.comet.spi.CometScanRuleExtension` discovery works:
 *      `CometExtensionRegistry.load()` finds this class via ServiceLoader as soon as the contrib
 *      JAR is on the classpath.
 *
 * 2. The wiring in `CometScanRule.transformV1Scan` / `transformV2Scan` actually iterates
 * extensions: even though this one returns `false` from `matchesV1` and `matchesV2`, the registry
 * call happens for every scan.
 *
 * Real contribs replace `matchesV1` / `transformV1` with real probes against the scan's
 * `relation.fileFormat` (e.g. Delta would detect `DeltaParquetFileFormat`) and `transformV1` with
 * the contrib's native dispatch.
 *
 * The matching native-side counterpart lives in `contrib/example/native/src/lib.rs` -- it
 * registers a `ContribOperatorPlanner` under the same kind string used by any future Scala-side
 * serde this example might add.
 */
class ExampleScanRuleExtension extends CometScanRuleExtension with Logging {
  override val name: String = "example"

  override def matchesV1(relation: HadoopFsRelation): Boolean = {
    // Sentinel: only match if a synthetic option declares this contrib should claim the
    // scan. Production contribs replace this with a real file-format probe; here we want
    // the test to be able to opt in deterministically.
    relation.options
      .get(ExampleScanRuleExtension.MarkerOptionKey)
      .contains(ExampleScanRuleExtension.MarkerOptionValue)
  }

  // matchesV2 / transformV1 / transformV2 inherit the trait defaults (`false` / `None`).
  // This example only demonstrates V1 discovery. A real contrib would override the
  // transform methods to build its native plan.
}

object ExampleScanRuleExtension {

  /**
   * Test-only option key. A Spark read can set this on `HadoopFsRelation.options` to trigger
   * `ExampleScanRuleExtension.matchesV1` and verify the SPI is being consulted.
   */
  val MarkerOptionKey: String = "comet.contrib.example.marker"

  /** Sentinel value the marker option must equal. */
  val MarkerOptionValue: String = "match"
}
