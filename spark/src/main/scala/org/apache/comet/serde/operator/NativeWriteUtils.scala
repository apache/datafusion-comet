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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.parquet.HadoopFileOutput
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
   *   - `fileNamePrefix`, the basename every file name is built from. Spark 4.0+
   *     `HadoopMapReduceCommitProtocol.getFilename` reads `mapreduce.output.basename` and
   *     interpolates it into `<basename>-<split>-<jobId>`; Spark 3.x hardcodes `part` instead.
   *     Both native writers take the exact file name from the commit protocol. A basename holding
   *     `?` or `#` is dangerous on 4.0+: the native URL parser truncates there, so every task
   *     writes a file with the same truncated name and they overwrite each other during commit.
   *     Custom committer paths are checked by [[checkNativeWriteDestination]].
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
          "spark.comet.write.parquet.enabled=false to write this table with Spark.")
    }

  /**
   * Whether the native writer creates the file at `filePath` itself, which it does for local and
   * HDFS files. Any other file is created through Hadoop's FileSystem API, as Spark's own writer
   * creates it, and the native writer only writes its bytes. That is always correct, whatever the
   * file system and whatever the committer expects of it, which is why this is decided per task
   * on the path the commit protocol actually returned rather than at planning.
   */
  def nativeWriterCreatesFile(filePath: String): Boolean =
    filePath.startsWith("file:") || filePath.startsWith("hdfs:")

  /**
   * Writes the task file at `filePath` with `write`, which runs the task's native write plan.
   *
   * When the native writer cannot create the file itself (see [[nativeWriterCreatesFile]]), it is
   * created here first, with the task's configuration, and `write` gets its [[HadoopFileOutput]]
   * for the plan's writer to write the file's bytes to. That is what S3A's magic committer needs:
   * S3A turns a file created under the committer's `__magic` directory into a pending upload to
   * the file's final destination, which it records when the file is closed, and only S3A can do
   * that. The file is closed once `write` returns. If `write` throws, the file is aborted
   * instead, and what the file's stream threw is rethrown, since the native writer passes on only
   * its text, as Spark's own writer would surface it.
   */
  def writeFile(filePath: String, hadoopConf: Configuration)(
      write: Option[HadoopFileOutput] => Unit): Unit = {
    val output =
      if (nativeWriterCreatesFile(filePath)) None
      else Some(HadoopFileOutput.create(filePath, hadoopConf))
    try {
      write(output)
    } catch {
      case failure: Throwable =>
        output match {
          case Some(file) =>
            file.abort(failure)
            throw Option(file.failure()).filter(_ ne failure).fold(failure) { streamFailure =>
              streamFailure.addSuppressed(failure)
              streamFailure
            }
          case None => throw failure
        }
    }
    // Only once the native writer has written every byte of the file and released it.
    output.foreach(_.close())
  }

  /**
   * A fallback reason when the native writer cannot write to `outputPath`'s file system, or
   * `None` when it can. Local and HDFS files are created natively. A write that S3A's magic
   * committer commits is supported too, since each of its files is created through S3A (see
   * [[writeFile]]), which is the only way to give the committer the pending upload it completes.
   * Other S3 writes stay on Spark until the native writer reproduces what S3A does at create
   * time.
   */
  def unsupportedFileSystem(
      outputPath: String,
      commitProtocolClass: String,
      hadoopConf: Configuration): Option[String] =
    if (outputPath.startsWith("file:") || outputPath.startsWith("hdfs:") ||
      usesS3AMagicCommitter(outputPath, commitProtocolClass, hadoopConf)) {
      None
    } else {
      Some("Supported output filesystems: local, HDFS, and S3 with the S3A magic committer")
    }

  /** Spark's commit protocol for Hadoop's `PathOutputCommitter`s, in spark-hadoop-cloud. */
  val PathOutputCommitProtocolClass: String =
    "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol"

  /**
   * Spark's Parquet output committer that delegates to a `PathOutputCommitter`, in
   * spark-hadoop-cloud.
   */
  val BindingParquetOutputCommitterClass: String =
    "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter"

  /** S3A's committer factory that always creates the magic committer. */
  private val MagicCommitterFactoryClass =
    "org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitterFactory"

  /** S3A's committer factory that creates the committer `fs.s3a.committer.name` names. */
  private val S3ACommitterFactoryClass = "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory"

  /**
   * Whether S3A's magic committer commits a write to `outputPath`.
   *
   * Spark reaches Hadoop's committer factories through one of two bindings in spark-hadoop-cloud.
   * Spark 4.1 configures both whenever that module is on the classpath (SPARK-47618), and earlier
   * versions for a bucket that sets `fs.s3a.bucket.<bucket>.committer.magic.enabled`:
   *
   *   - the commit protocol `PathOutputCommitProtocol`, or
   *   - `BindingParquetOutputCommitter` as `spark.sql.parquet.output.committer.class`, under a
   *     commit protocol that instantiates that class, as Spark's default one does.
   *
   * Either asks Hadoop's `PathOutputCommitterFactory` for the committer. Its factory is
   * `mapreduce.outputcommitter.factory.class` when set, else
   * `mapreduce.outputcommitter.factory.scheme.<scheme>`. The committer is the magic committer
   * when that factory is `MagicS3GuardCommitterFactory`, or when it is `S3ACommitterFactory` and
   * the committer it is asked for is `magic`: `fs.s3a.committer.name` in the job's configuration,
   * or failing that in the file system's, where `fs.s3a.bucket.<bucket>.committer.name` replaces
   * it.
   *
   * Only planning asks, to decide whether a write is worth running natively. A wrong answer
   * cannot misplace data: each task writes the path the committer actually returns, and creates
   * any file that is not local or on HDFS through Hadoop, see [[writeFile]].
   *
   * @param commitProtocolClass
   *   the write's `spark.sql.sources.commitProtocolClass`
   * @param hadoopConf
   *   the write's Hadoop configuration, which carries `spark.sql.parquet.output.committer.class`
   *   as Spark's Parquet writer reads it
   */
  def usesS3AMagicCommitter(
      outputPath: String,
      commitProtocolClass: String,
      hadoopConf: Configuration): Boolean = {
    val outputCommitterClass =
      hadoopConf.getTrimmed(SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key, "")
    val asksCommitterFactory = commitProtocolClass == PathOutputCommitProtocolClass ||
      (commitProtocolClass == SQLConf.FILE_COMMIT_PROTOCOL_CLASS.defaultValueString &&
        outputCommitterClass == BindingParquetOutputCommitterClass)
    val uri = new Path(outputPath).toUri
    if (!asksCommitterFactory || uri.getScheme == null) {
      return false
    }

    val factory = Seq(
      "mapreduce.outputcommitter.factory.class",
      s"mapreduce.outputcommitter.factory.scheme.${uri.getScheme}")
      .map(key => hadoopConf.getTrimmed(key, ""))
      .find(_.nonEmpty)
    factory match {
      case Some(MagicCommitterFactoryClass) => true
      case Some(S3ACommitterFactoryClass) =>
        // S3ACommitterFactory takes the name from the job's configuration when it has one, and
        // otherwise from the file system's, which S3A has resolved per bucket. A job
        // configuration set globally therefore wins over a bucket's own setting.
        val name = Option(hadoopConf.getTrimmed("fs.s3a.committer.name"))
          .orElse(Option(uri.getHost).flatMap(bucket =>
            Option(hadoopConf.getTrimmed(s"fs.s3a.bucket.$bucket.committer.name"))))
        name.contains("magic")
      case _ => false
    }
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
