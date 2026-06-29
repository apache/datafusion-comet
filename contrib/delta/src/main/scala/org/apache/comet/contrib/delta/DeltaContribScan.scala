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

import org.apache.comet.serde.OperatorOuterClass.{ContribScan, DeltaScan, Operator}

/**
 * Pack/unpack helpers for putting a [[DeltaScan]] into core's generic `ContribScan` envelope.
 *
 * Core's `Operator` oneof has a single permanent `contrib_scan` field shared by every out-of-tree
 * contrib scan (Delta, Lance, ...) rather than one variant per format. The concrete message is
 * carried as opaque bytes and identified by [[TypeUrl]]; core routes on that string and never
 * names a contrib type. This object is the ONE place the Delta type URL is written, so the JVM
 * producer and the native consumer (`DELTA_SCAN_TYPE_NAME` in
 * `native/core/src/execution/planner/delta_scan.rs`) cannot drift apart silently.
 *
 * The envelope's field layout is deliberately identical to `google.protobuf.Any` (`type_url` +
 * `value`); it is hand-rolled only because Comet's Maven protoc plugin cannot resolve the bundled
 * well-known types.
 */
object DeltaContribScan {

  /**
   * `Any`-style type URL for [[DeltaScan]]. The `type.googleapis.com/` prefix is the
   * `google.protobuf.Any` convention; the native side matches on the `package.Message` suffix, so
   * the prefix is informational.
   *
   * The suffix is namespaced to this contrib rather than to the message's current proto package
   * (`spark.spark_operator`, which is core's). The `DeltaScan` definitions still live in core's
   * `operator.proto` only because a contrib proto build pipeline does not exist yet, and are meant
   * to move under `contrib/delta/proto` (apache/datafusion-comet#5378). Naming the owner rather
   * than the file's current home keeps that relocation invisible here and on the wire.
   */
  val TypeUrl: String = "type.googleapis.com/comet.contrib.delta.DeltaScan"

  /** Wrap a `DeltaScan` in the generic contrib envelope. */
  def pack(scan: DeltaScan): ContribScan =
    ContribScan.newBuilder().setTypeUrl(TypeUrl).setValue(scan.toByteString).build()

  /** Set `op`'s oneof to a `contrib_scan` carrying `scan`. */
  def set(builder: Operator.Builder, scan: DeltaScan): Operator.Builder =
    builder.setContribScan(pack(scan))

  /** True when `op` carries a Delta scan in its contrib envelope. */
  def isDeltaScan(op: Operator): Boolean =
    op.hasContribScan && op.getContribScan.getTypeUrl == TypeUrl

  /**
   * Decode the `DeltaScan` from `op`'s contrib envelope. Callers must have checked
   * [[isDeltaScan]] first -- reaching this on a non-Delta operator is a wiring bug, not a
   * runtime condition, so it throws rather than returning an Option.
   */
  def get(op: Operator): DeltaScan = {
    require(isDeltaScan(op), s"operator does not carry a Delta contrib_scan: ${op.getOpStructCase}")
    DeltaScan.parseFrom(op.getContribScan.getValue)
  }
}
