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

package org.apache.comet.serde.operator

import org.apache.spark.sql.catalyst.plans.QueryPlan

import org.apache.comet.serde.OperatorOuterClass
import org.apache.comet.serde.QueryPlanSerde.serializeDataType

/**
 * Shared helpers for native write serdes ([[CometDataWritingCommand]] for V1 parquet writes and
 * [[CometIcebergNativeWrite]] for V2 Iceberg writes).
 */
object NativeWriteUtils {

  /**
   * Build a synthetic `Scan` operator that lets a native write op consume Arrow batches shipped
   * from the JVM iterator over `plan`'s `executeColumnar()` RDD.
   *
   * Returns `None` if any of `plan.output`'s data types can't be serialised to the proto -- in
   * that case the caller should fall back with `withFallbackReason`.
   */
  def buildFfiScan(plan: QueryPlan[_], planId: Int): Option[OperatorOuterClass.Operator] = {
    val scanTypes = plan.output.flatMap(attr => serializeDataType(attr.dataType))
    if (scanTypes.length != plan.output.length) return None
    val scan = OperatorOuterClass.Scan
      .newBuilder()
      .setSource(plan.nodeName)
    scanTypes.foreach(scan.addFields)
    Some(
      OperatorOuterClass.Operator
        .newBuilder()
        .setPlanId(planId)
        .setScan(scan.build())
        .build())
  }
}
