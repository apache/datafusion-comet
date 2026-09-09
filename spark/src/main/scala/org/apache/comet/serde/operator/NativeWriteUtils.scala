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

import org.apache.hadoop.fs.Path
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

  /**
   * A fallback reason when `outputPath` is an HDFS destination whose path needs percent-escaping,
   * or `None` when the write can proceed.
   *
   * Comet and Spark disagree about what such a path names. The native side reaches HDFS through
   * `create_hdfs_object_store`, which hands `url.path()` -- still escaped -- to
   * `object_store::path::Path::parse`, so the native writer creates a directory literally called
   * `dir%20with%20space`. Spark's committer, meanwhile, works with the unescaped Hadoop `Path`
   * and commits `dir with space`. Job commit then succeeds while the data sits somewhere else,
   * which is worse than not accelerating the write.
   *
   * Local `file:` destinations are unaffected and deliberately not gated here: they go through a
   * different object-store constructor that does not retain the escaping.
   */
  def escapedHdfsDestination(outputPath: String): Option[String] = {
    if (!outputPath.startsWith("hdfs:")) return None
    val uri = new Path(outputPath).toUri
    // `getPath` decodes, `getRawPath` does not. They differ exactly when the path contains
    // something the URI form had to escape.
    val raw = uri.getRawPath
    val decoded = uri.getPath
    if (raw != null && decoded != null && raw != decoded) {
      Some(
        "HDFS output paths needing URI escaping are not supported: the native writer would " +
          s"write to the escaped path while Spark commits the unescaped one ($decoded)")
    } else {
      None
    }
  }
}
