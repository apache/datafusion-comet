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
   * ASCII characters the native URL parser percent-encodes inside a path. Determined against the
   * locked `url` 2.5 crate by parsing `hdfs://ns/pre<c>post/output` for every printable ASCII `c`
   * and comparing `url.path()` with the input: these nine are rewritten and the rest survive,
   * including `%`, `[`, `\`, `]`, `^` and `|`. Control characters and DEL are handled separately
   * in [[needsNativeUrlEscaping]] rather than listed here.
   */
  private val nativeUrlEscapedAscii: Set[Char] =
    Set(' ', '"', '#', '<', '>', '?', '`', '{', '}')

  /**
   * Whether the native URL parser would rewrite `path`, so that the name it creates on HDFS
   * differs from the Hadoop filename Spark commits.
   *
   * `percent_encoding`'s `should_percent_encode` is `!byte.is_ascii() || set.contains(byte)`, so
   * every non-ASCII byte is escaped regardless of the encode set. That is the case Java's URI
   * comparison cannot see, because `java.net.URI` leaves non-ASCII path characters alone: a name
   * holding U+00E9 comes back identically from `getRawPath` and `getPath`, while the native
   * parser produces `caf%C3%A9`.
   */
  private def needsNativeUrlEscaping(path: String): Boolean =
    path.exists(c => c < ' ' || c > '~' || nativeUrlEscapedAscii.contains(c))

  /**
   * A fallback reason when `outputPath` is an HDFS destination whose path the native writer and
   * Spark would spell differently, or `None` when the write can proceed.
   *
   * Comet and Spark disagree about what such a path names. The native side receives
   * `Path.toString`, which is not a URI string, and reaches HDFS through
   * `create_hdfs_object_store`, which hands `url.path()` -- now escaped by the Rust parser -- to
   * `object_store::path::Path::parse`. So the native writer creates a directory literally called
   * `dir%20with%20space`, or `caf%C3%A9` for a name holding U+00E9. Spark's committer, meanwhile,
   * works with the unescaped Hadoop `Path` and commits `dir with space`, or that same U+00E9 name
   * unescaped. Job commit then succeeds while the data sits somewhere else, which is worse than
   * not accelerating the write.
   *
   * Two conditions, because neither covers the other:
   *
   *   - the native parser escapes a character, which is the direct statement of the divergence
   *     and the only condition that catches non-ASCII names;
   *   - `java.net.URI` had to escape something, which catches a literal `%` in the Hadoop name.
   *     The native parser leaves `%` alone, so `50%off` reaches `Path::parse` as an invalid
   *     escape rather than as a rewritten name.
   *
   * Local `file:` destinations are unaffected and deliberately not gated here: they go through a
   * different object-store constructor that does not retain the escaping.
   */
  def escapedHdfsDestination(outputPath: String): Option[String] = {
    if (!outputPath.startsWith("hdfs:")) return None
    val uri = new Path(outputPath).toUri
    val raw = uri.getRawPath
    val decoded = uri.getPath
    val javaEscaped = raw != null && decoded != null && raw != decoded
    // Checked against the string handed to the native writer, which is `outputPath` itself.
    val nativeEscaped = needsNativeUrlEscaping(outputPath)
    if (javaEscaped || nativeEscaped) {
      val shown = if (decoded != null) decoded else outputPath
      Some(
        "HDFS output paths needing URI escaping are not supported: the native writer would " +
          s"write to the escaped path while Spark commits the unescaped one ($shown)")
    } else {
      None
    }
  }
}
