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

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf.COMET_LIBHDFS_SCHEMES_KEY
import org.apache.comet.objectstore.NativeConfig
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
   *   - `fileNamePrefix`, the basename every file name is built from. On Spark 4.0+ that is
   *     `mapreduce.output.basename`, which `HadoopMapReduceCommitProtocol.getFilename`
   *     interpolates into `<basename>-<split>-<jobId>`; on 3.x Comet names the files itself and
   *     the basename is always the literal `part`. A basename holding `?` or `#` is the dangerous
   *     one: the native URL parser truncates there, so *every* task writes a file with the same
   *     truncated name and they overwrite each other during commit.
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
   * [[unsupportedDestination]] declines the shapes Comet can predict at planning time, but the
   * path a write actually uses comes from `FileCommitProtocol.newTaskTempFile`, and a custom
   * commit protocol can return anything. This is the backstop: it runs before the writer opens
   * anything, so the task fails with a clear message rather than committing successfully with the
   * data left somewhere else.
   */
  def checkNativeWriteDestination(filePath: String): Unit = {
    hdfsPathDivergence(filePath).foreach { shown =>
      throw new UnsupportedOperationException(
        s"Comet's native Parquet writer cannot write to '$filePath': the path it would create " +
          s"on HDFS is not the one the commit protocol chose ($shown). Set " +
          "spark.comet.parquet.write.enabled=false to write this table with Spark.")
    }
    if (s3Scheme(filePath).isDefined &&
      (s3PathDivergence(filePath).isDefined || isS3AMagicPath(filePath))) {
      throw new UnsupportedOperationException(
        s"Comet's native Parquet writer cannot write to '$filePath': the object it would create " +
          "on S3 is not the one the commit protocol expects. Set " +
          "spark.comet.parquet.write.enabled=false to write this table with Spark.")
    }
  }

  /**
   * A fallback reason when the native writer cannot write `outputPath`'s files, or `None` when it
   * can. Both native write serdes gate on this, so they admit exactly the same destinations.
   *
   * @param fileNamePrefix
   *   the basename every file name starts with, see [[escapedHdfsDestination]]
   * @param hadoopConf
   *   the write's Hadoop configuration, the one its object store options are extracted from
   */
  def unsupportedDestination(
      outputPath: String,
      fileNamePrefix: String,
      hadoopConf: Configuration): Option[String] = {
    if (outputPath.startsWith("file:")) {
      None
    } else if (outputPath.startsWith("hdfs:")) {
      escapedHdfsDestination(outputPath, fileNamePrefix)
    } else {
      s3Scheme(outputPath) match {
        case Some(scheme) =>
          unsupportedS3Destination(outputPath, scheme, fileNamePrefix, hadoopConf)
        case None => Some("Supported output filesystems: local, HDFS, S3 (through S3A)")
      }
    }
  }

  /**
   * The schemes the native writer uploads through `object_store`'s S3 client. Configured
   * `fs.comet.s3Compliant.schemes` aliases are not among them even though the scan reads them:
   * the native side reads their settings through vendor key translation, so a write could reach a
   * different endpoint than the one Spark's committer then lists.
   */
  private val S3Schemes: Seq[String] = Seq("s3", "s3a")

  /**
   * The only Hadoop FileSystem whose writes the native S3 writer reproduces. Named rather than
   * referenced, so that a deployment without hadoop-aws still plans writes and falls back.
   */
  private val S3AFileSystemClassName = "org.apache.hadoop.fs.s3a.S3AFileSystem"

  /**
   * S3A settings, without their `fs.s3a.` prefix, that change the objects S3A creates. The native
   * writer does not apply them, so a write that sets one stays on Spark's writer rather than
   * produce files without the encryption, ACL, storage class or content encoding it asks for. The
   * list covers Hadoop 3.3 and 3.4.
   */
  private val UnsupportedS3AWriteOptions: Seq[String] = Seq(
    // Server-side and client-side encryption, under its current and its deprecated name
    "encryption.algorithm",
    "server-side-encryption-algorithm",
    "acl.default",
    "create.storage.class",
    "object.content.encoding")

  /** Custom object headers S3A adds to every file it creates. */
  private val S3ACreateHeaderPrefix = "create.header."

  private def s3Scheme(path: String): Option[String] =
    S3Schemes.find(scheme => path.startsWith(s"$scheme:"))

  /**
   * The first character of `path` that the native S3 writer would not carry into the object key,
   * if any.
   *
   * The native writer derives the key the way the Parquet scan does: it parses the path as a URL
   * and percent-decodes the URL's path. It is handed `Path.toString`, which Hadoop leaves
   * unescaped, so that is only right where escaping and decoding cancel out. They do for the
   * characters the URL parser escapes, among them spaces and every non-ASCII character, which
   * come back unchanged. They do not for:
   *
   *   - `%`, which can start an escape sequence that decoding then replaces. `50%25` is written
   *     as `50%`, and `%2F` even adds a directory level.
   *   - `?` and `#`, which the parser takes for the start of the query and the fragment, so the
   *     path is truncated there.
   *   - ASCII control characters. The parser drops tabs and line breaks outright, and
   *     `object_store` rejects the rest, which would fail the task.
   *
   * Returns the offending character as a fallback reason would show it.
   */
  private def s3PathDivergence(path: String): Option[String] =
    path.find(c => c == '%' || c == '?' || c == '#' || c < ' ' || c == '\u007f').map { c =>
      if (c == '%' || c == '?' || c == '#') s"'$c'" else f"control character U+${c.toInt}%04X"
    }

  /**
   * Whether `path` is under an S3A magic committer directory: an element that starts with
   * `__magic`, which is `__magic` on Hadoop 3.3 and `__magic_job-<id>` on 3.4. S3A intercepts a
   * file created there, uploading it to its final destination as a multipart upload it leaves
   * incomplete for the committer to complete at job commit. The native writer talks to S3
   * directly, so it would create a real object in the magic directory instead, which the
   * committer never lists and job cleanup deletes: the job would succeed without its data.
   */
  private def isS3AMagicPath(path: String): Boolean =
    path.split('/').exists(_.startsWith("__magic"))

  /**
   * The key S3A takes setting `key` from for `bucket`, `fs.s3a.bucket.<bucket>.<key>` over
   * `fs.s3a.<key>`, when it holds a value. A blank value, which S3A treats as no setting, does
   * not count.
   */
  private def effectiveS3AKey(
      hadoopConf: Configuration,
      bucket: String,
      key: String): Option[String] =
    Seq(s"fs.s3a.bucket.$bucket.$key", s"fs.s3a.$key")
      .find(name => hadoopConf.getTrimmed(name) != null)
      .filter(name => hadoopConf.getTrimmed(name).nonEmpty)

  /** The FileSystem class Hadoop would serve `scheme` with, or `None` when it has none. */
  private def fileSystemClassName(scheme: String, hadoopConf: Configuration): Option[String] =
    try {
      Some(FileSystem.getFileSystemClass(scheme, hadoopConf).getName)
    } catch {
      case NonFatal(_) => None
    }

  /**
   * Why the native writer cannot write an S3 destination, or `None` when it can. The native
   * writer uploads with `object_store` rather than through S3A, so this admits only destinations
   * where the two produce the same objects.
   */
  private def unsupportedS3Destination(
      outputPath: String,
      scheme: String,
      fileNamePrefix: String,
      hadoopConf: Configuration): Option[String] = {
    // The same configuration `NativeConfig.extractObjectStoreOptions` forwards, so this matches
    // the native routing, which would send the write to the HDFS writer instead.
    if (NativeConfig.parseSchemeSet(hadoopConf.get(COMET_LIBHDFS_SCHEMES_KEY)).contains(scheme)) {
      return Some(
        s"$scheme:// output routed through libhdfs by $COMET_LIBHDFS_SCHEMES_KEY is " +
          "not supported")
    }
    // Asking Hadoop rather than trusting the scheme is what keeps an `s3://` served by something
    // other than S3A, such as EMRFS, on Spark's writer: its committers can depend on its own
    // output streams, which the native writer would bypass.
    val fileSystem = fileSystemClassName(scheme, hadoopConf)
    if (!fileSystem.contains(S3AFileSystemClassName)) {
      return Some(
        s"S3 output is only supported through S3A, but $scheme:// is served by " +
          fileSystem.getOrElse("no loadable FileSystem"))
    }
    s3PathDivergence(outputPath).foreach { shown =>
      return Some(
        s"S3 output paths containing $shown are not supported: the native writer would write " +
          s"to a different key from the one Spark commits ($outputPath)")
    }
    // The basename goes into every file name, see `escapedHdfsDestination`.
    s3PathDivergence(fileNamePrefix).foreach { shown =>
      return Some(
        s"S3 output file names containing $shown are not supported: " +
          s"$BASE_OUTPUT_NAME=$fileNamePrefix would make the native writer create a different " +
          "object from the one Spark commits")
    }
    if (isS3AMagicPath(outputPath)) {
      return Some(s"S3A magic committer paths are not supported as output paths ($outputPath)")
    }
    val bucket = Option(new Path(outputPath).toUri.getAuthority).filter(_.nonEmpty) match {
      case Some(bucket) => bucket
      case None =>
        return Some(s"S3 output paths without a bucket are not supported ($outputPath)")
    }
    // S3A reads the committer name from both the job and the filesystem configuration, so decline
    // whichever of the two names the magic committer. This is conservative: the magic committer
    // is only used with a commit protocol that asks S3A for its committer, such as Spark's
    // `PathOutputCommitProtocol`.
    Seq(s"fs.s3a.bucket.$bucket.committer.name", "fs.s3a.committer.name")
      .find(key => "magic".equalsIgnoreCase(hadoopConf.getTrimmed(key)))
      .foreach { key =>
        return Some(
          s"The S3A magic committer ($key=magic) is not supported: it commits the multipart " +
            "uploads S3A leaves pending, which the native writer does not create")
      }
    UnsupportedS3AWriteOptions
      .flatMap(key => effectiveS3AKey(hadoopConf, bucket, key))
      .headOption
      .orElse(
        Seq(s"fs.s3a.bucket.$bucket.$S3ACreateHeaderPrefix", s"fs.s3a.$S3ACreateHeaderPrefix")
          .find(prefix => !hadoopConf.getPropsWithPrefix(prefix).isEmpty)
          .map(prefix => s"$prefix*"))
      .map(key =>
        s"S3A setting $key is not supported: the native writer would not apply it to the " +
          "files it writes")
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
