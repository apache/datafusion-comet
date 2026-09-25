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

import java.util.Locale

import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.serde.OperatorOuterClass
import org.apache.comet.serde.QueryPlanSerde.serializeDataType

/**
 * Shared helpers for native write serdes ([[CometDataWritingCommand]] for V1 parquet writes and
 * [[CometIcebergNativeWrite]] for V2 Iceberg writes).
 */
object NativeWriteUtils {

  /**
   * Hadoop's `FileOutputFormat.BASE_OUTPUT_NAME`, which is `protected` there. Spelled out for the
   * same reason Spark spells it out in `HadoopMapReduceCommitProtocol.getFilename`, which is
   * where a write's effective value is read.
   */
  val BASE_OUTPUT_NAME: String = "mapreduce.output.basename"

  /** The file-name prefix a write uses when [[BASE_OUTPUT_NAME]] is not set. */
  val DEFAULT_BASE_OUTPUT_NAME: String = "part"

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
   * ASCII characters the native URL parser rewrites inside a path. Determined against the locked
   * `url` 2.5 crate by parsing `hdfs://ns/pre<c>post/output` for every printable ASCII `c` and
   * comparing `url.path()` with the input: these nine are rewritten and the rest survive,
   * including `%`, `[`, `\`, `]`, `^` and `|`. Control characters and DEL are handled separately
   * in [[needsNativeUrlEscaping]] rather than listed here.
   *
   * `native/core/src/parquet/parquet_support.rs` has a `url_path_rewritten_characters` test that
   * fails if a `url` upgrade changes this set, so the two cannot drift apart silently.
   *
   * `?` and `#` are the worst of the nine: they are not escaped but treated as delimiters, so the
   * native path is *truncated* there rather than merely spelled differently.
   */
  private val nativeUrlEscapedAscii: Set[Char] =
    Set(' ', '"', '#', '<', '>', '?', '`', '{', '}')

  /**
   * Whether the native URL parser would rewrite `s`, so that the name it creates on HDFS differs
   * from the Hadoop filename Spark commits.
   *
   * `percent_encoding`'s `should_percent_encode` is `!byte.is_ascii() || set.contains(byte)`, so
   * every non-ASCII byte is escaped regardless of the encode set. That is the case Java's URI
   * comparison cannot see, because `java.net.URI` leaves non-ASCII path characters alone: a name
   * holding U+00E9 comes back identically from `getRawPath` and `getPath`, while the native
   * parser produces `caf%C3%A9`.
   */
  private def needsNativeUrlEscaping(s: String): Boolean =
    s.exists(c => c < ' ' || c > '~' || nativeUrlEscapedAscii.contains(c))

  /**
   * Whether the native writer and Spark would spell `path` differently, and how.
   *
   * The native side receives `Path.toString`, which is not a URI string, and reaches HDFS through
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
   * different object-store constructor that keeps the string verbatim.
   */
  private def hdfsPathDivergence(path: String): Option[String] = {
    if (!path.startsWith("hdfs:")) return None
    val uri = new Path(path).toUri
    val raw = uri.getRawPath
    val decoded = uri.getPath
    val javaEscaped = raw != null && decoded != null && raw != decoded
    // Checked against the string handed to the native writer, which is `path` itself.
    if (javaEscaped || needsNativeUrlEscaping(path)) {
      Some(if (decoded != null) decoded else path)
    } else {
      None
    }
  }

  /**
   * A fallback reason when a write to `outputPath` would land somewhere the committer is not
   * looking, or `None` when the write can proceed.
   *
   * Two things go into every committed file name, and the native writer has to reproduce both
   * byte for byte (see [[hdfsPathDivergence]] for why it may not):
   *
   *   - the destination directory, and
   *   - `fileNamePrefix`, the basename every file name is built from. That is
   *     `mapreduce.output.basename`, which `HadoopMapReduceCommitProtocol.getFilename`
   *     interpolates into `<basename>-<split>-<jobId>`; both native writers take their file names
   *     from the commit protocol, on every supported Spark version. A basename holding `?` or `#`
   *     is the dangerous one: the native URL parser truncates there, so *every* task writes a
   *     file with the same truncated name and they overwrite each other during commit.
   *
   * The basename is checked by running [[hdfsPathDivergence]] over the path it produces rather
   * than over the name alone. Everything else `getFilename` interpolates -- the split number, the
   * job id and the codec extension -- is Comet-independent ASCII, so the probe below covers the
   * whole committed file name. Sharing the one predicate with [[checkNativeWriteDestination]] is
   * also what keeps planning from admitting a basename the task guard then aborts on: a literal
   * `%` is invisible to [[needsNativeUrlEscaping]] but not to `java.net.URI`.
   */
  def escapedHdfsDestination(outputPath: String, fileNamePrefix: String): Option[String] = {
    if (!outputPath.startsWith("hdfs:")) return None
    hdfsPathDivergence(outputPath)
      .map(shown =>
        "HDFS output paths needing URI escaping are not supported: the native writer would " +
          s"write to the escaped path while Spark commits the unescaped one ($shown)")
      .orElse {
        hdfsPathDivergence(s"$outputPath/$fileNamePrefix").map(_ =>
          "HDFS output file names needing URI escaping are not supported: " +
            s"$BASE_OUTPUT_NAME=$fileNamePrefix would make the native writer create a " +
            "different file from the one Spark commits")
      }
  }

  /**
   * Fail the task if the native writer would not write to exactly `filePath`.
   *
   * [[escapedHdfsDestination]] declines the shapes Comet can predict at planning time, but the
   * path a write actually uses comes from `FileCommitProtocol.newTaskTempFile`, and a custom
   * commit protocol can return anything. This is the backstop: it runs before the writer opens
   * anything, so the task fails with a clear message rather than committing successfully with the
   * data left somewhere else.
   */
  def checkNativeWriteDestination(filePath: String): Unit =
    hdfsPathDivergence(filePath).foreach { shown =>
      throw new UnsupportedOperationException(
        s"Comet's native Parquet writer cannot write to '$filePath': the path it would create " +
          s"on HDFS is not the one the commit protocol chose ($shown). Set " +
          "spark.comet.parquet.write.enabled=false to write this table with Spark.")
    }

  /** Compression codecs Comet's native Parquet writer can produce. */
  val supportedCompressionCodecs: Set[String] =
    Set("none", "uncompressed", "snappy", "lz4", "zstd", "gzip")

  /**
   * The compression codec a write will use, resolved exactly as Spark's own `ParquetOptions`
   * does: `compression`, then `parquet.compression` (i.e. `ParquetOutputFormat.COMPRESSION`),
   * then `spark.sql.parquet.compression.codec`.
   *
   * The `CaseInsensitiveMap` is not cosmetic. Spark reaches these options through one
   * (`ParquetOptions.compressionCodecClassName`) and `DataFrameWriter` passes the caller's keys
   * through verbatim, so `option("Compression", "gzip")` is a gzip write to Spark. Reading it
   * case-sensitively would both name the file `...-c000.gz.parquet` while writing SNAPPY, and let
   * an unsupported codec slip past [[supportedCompressionCodecs]] by falling through to the
   * SQLConf default.
   */
  def parseCompressionCodec(options: Map[String, String]): String = {
    val caseInsensitive = CaseInsensitiveMap(options)
    caseInsensitive
      .get("compression")
      .orElse(caseInsensitive.get(ParquetOutputFormat.COMPRESSION))
      .getOrElse(
        SQLConf.get.getConfString(
          SQLConf.PARQUET_COMPRESSION.key,
          SQLConf.PARQUET_COMPRESSION.defaultValueString))
      .toLowerCase(Locale.ROOT)
  }

  /**
   * The proto codec for a Spark codec name, or `None` if Comet cannot produce it.
   *
   * At execution time the name to pass here is `CodecConfig.from(context).getCodec.name()`:
   * `ParquetUtils.prepareWrite` resolves the write's options into `parquet.compression` on the
   * job configuration, and the file extension comes from that same `CodecConfig`. Reading the
   * codec back from there is what makes the file's name and its contents agree by construction
   * rather than by two copies of the precedence rule agreeing.
   */
  def protoCompressionCodec(codec: String): Option[OperatorOuterClass.CompressionCodec] =
    codec.toLowerCase(Locale.ROOT) match {
      case "snappy" => Some(OperatorOuterClass.CompressionCodec.Snappy)
      case "lz4" => Some(OperatorOuterClass.CompressionCodec.Lz4)
      case "zstd" => Some(OperatorOuterClass.CompressionCodec.Zstd)
      case "gzip" => Some(OperatorOuterClass.CompressionCodec.Gzip)
      case "none" | "uncompressed" => Some(OperatorOuterClass.CompressionCodec.None)
      case _ => None
    }
}
