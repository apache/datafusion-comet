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

package org.apache.comet.contrib.delta

import org.apache.comet.serde.OperatorOuterClass
import org.apache.comet.serde.OperatorOuterClass.Operator

/**
 * Packs and unpacks the JVM-planned `DeltaSparkScan` message in core's generic `ContribScan`
 * envelope (`contrib_scan` on `Operator`). The native dispatcher routes by `type_url`, so this
 * contrib's identifier is the only coupling between the JVM and native sides; core names no Delta
 * type.
 */
object DeltaSparkScanEnvelope {

  /**
   * Contrib-owned identifier for the message, mirrored by `DELTA_SPARK_SCAN_TYPE_NAME` in
   * native's `delta_spark_scan.rs`. Distinct from the kernel path's
   * `comet.contrib.delta.DeltaScan`.
   */
  val TypeUrl = "type.googleapis.com/comet.contrib.delta_spark.DeltaSparkScan"

  def pack(scan: OperatorOuterClass.DeltaSparkScan): OperatorOuterClass.ContribScan =
    OperatorOuterClass.ContribScan
      .newBuilder()
      .setTypeUrl(TypeUrl)
      .setValue(scan.toByteString)
      .build()

  /** Whether this operator carries this contrib's scan (and not some other contrib's). */
  def matches(op: Operator): Boolean =
    op.hasContribScan && op.getContribScan.getTypeUrl == TypeUrl

  /** Callers must check `matches` first. */
  def unpack(op: Operator): OperatorOuterClass.DeltaSparkScan =
    OperatorOuterClass.DeltaSparkScan.parseFrom(op.getContribScan.getValue)
}
