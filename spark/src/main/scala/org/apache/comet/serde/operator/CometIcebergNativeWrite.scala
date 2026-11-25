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

import java.lang.reflect.Modifier
import java.util.Locale

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.comet.{CometIcebergWriteExec, CometNativeExec, IcebergWriteExec}
import org.apache.spark.sql.types.NullType

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.iceberg.{IcebergReflection, IcebergStorageSchemes, PositionDeltaWrite, ReplaceDataWrite}
import org.apache.comet.objectstore.NativeConfig
import org.apache.comet.serde.{CometOperatorSerde, Compatible, OperatorOuterClass, SupportLevel, Unsupported}
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde.exprToProto
import org.apache.comet.util.ClassLoaders

object CometIcebergNativeWrite extends CometOperatorSerde[IcebergWriteExec] {

  private case class PreparedBloomPaths(
      schema: AnyRef,
      icebergVersion: String,
      properties: Map[String, String],
      paths: Map[String, String])

  private val BloomPathsTag = TreeNodeTag[PreparedBloomPaths]("comet.iceberg.bloomPaths")

  override def enabledConfig: Option[ConfigEntry[Boolean]] =
    Some(CometConf.COMET_ICEBERG_NATIVE_WRITE_ENABLED)

  override def requiresNativeChildren: Boolean = true

  object PropertyKeys {
    lazy val ObjectStoreEnabled: String =
      IcebergReflection.tablePropertyConstant("OBJECT_STORE_ENABLED")
    lazy val WriteLocationProviderImpl: String =
      IcebergReflection.tablePropertyConstant("WRITE_LOCATION_PROVIDER_IMPL")
    lazy val BloomFilterColumnEnabledPrefix: String =
      IcebergReflection.tablePropertyConstant("PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX")
    lazy val ParquetBloomFilterMaxBytes: String =
      IcebergReflection.tablePropertyConstant("PARQUET_BLOOM_FILTER_MAX_BYTES")
    val ParquetBloomFilterColumnFppPrefix = "write.parquet.bloom-filter-fpp.column."
    val ParquetBloomFilterColumnNdvPrefix = "write.parquet.bloom-filter-ndv.column."
    lazy val ParquetRowGroupCheckMinRecordCount: String =
      IcebergReflection.tablePropertyConstant("PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT")
    lazy val ParquetRowGroupCheckMinRecordCountDefault: Int =
      IcebergReflection.tablePropertyIntConstant(
        "PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT_DEFAULT")
    lazy val ParquetRowGroupCheckMaxRecordCount: String =
      IcebergReflection.tablePropertyConstant("PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT")
    lazy val ParquetRowGroupCheckMaxRecordCountDefault: Int =
      IcebergReflection.tablePropertyIntConstant(
        "PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT_DEFAULT")
    lazy val ParquetCompressionCodec: String =
      IcebergReflection.tablePropertyConstant("PARQUET_COMPRESSION")
    lazy val ParquetCompressionLevel: String =
      IcebergReflection.tablePropertyConstant("PARQUET_COMPRESSION_LEVEL")
    lazy val ParquetRowGroupSizeBytes: String =
      IcebergReflection.tablePropertyConstant("PARQUET_ROW_GROUP_SIZE_BYTES")
    lazy val ParquetPageSizeBytes: String =
      IcebergReflection.tablePropertyConstant("PARQUET_PAGE_SIZE_BYTES")
    lazy val ParquetPageRowLimit: String =
      IcebergReflection.tablePropertyConstant("PARQUET_PAGE_ROW_LIMIT")
    lazy val ParquetDictSizeBytes: String =
      IcebergReflection.tablePropertyConstant("PARQUET_DICT_SIZE_BYTES")
    val ParquetPageVersion: String = "write.parquet.page-version"
    val ParquetPageVersionDefault: String = "v1"
    val ParquetShredVariants: String = "write.parquet.shred-variants"
    val ParquetVariantBufferSize: String = "write.parquet.variant-inference-buffer-size"
    val ParquetEnableDictionary: String = "parquet.enable.dictionary"
  }

  private val EncryptionPropertyPrefix = "encryption."
  // `uuid` plus the v3-only types. Iceberg plans `variant` as Spark's VariantType, which the
  // native writer does not handle; Spark cannot plan a write to `timestamp_ns`, `geometry` or
  // `geography` today, so those are declined in case it learns to. `unknown` is checked below.
  private val UnsupportedWriteTypeIds: Set[String] =
    Set("UUID", "VARIANT", "TIMESTAMP_NANO", "GEOMETRY", "GEOGRAPHY")
  // Loaded from the native storage factory: `builtin_storage_schemes` in
  // `native/core/src/execution/operators/iceberg_common.rs` is the single point of change and
  // explains why `oss` is read-only and `memory` write-only. Lazy so constructing the serde does
  // not touch the native library. `gs` is additionally gated on the resolved FileIO below.
  private lazy val SupportedStorageSchemes: Set[String] = IcebergStorageSchemes.write
  // Supported schemes whose native backend is local and needs no host. Every other supported
  // scheme reads its bucket from the URL host (`requireSupportedStorageScheme`).
  private val LocalStorageSchemes: Set[String] = Set("file", "memory")
  private val S3StorageSchemes: Set[String] = Set("s3", "s3a")
  private val MaxSupportedFormatVersion = 3
  private val ParquetWritePropertyPrefix = "write.parquet."
  private val CometS3CredentialProviderClassProperty =
    "s3.comet.credential.provider.class"
  // The codec parquet-mr loads for brotli. No Spark or Iceberg distribution ships it.
  private[comet] val BrotliCodecClass = "org.apache.hadoop.io.compress.BrotliCodec"

  // Hadoop S3A settings are not forwarded wholesale. Keep this allow-list in lockstep with
  // NativeConfig.s3aSuffixToIcebergGlobalKey: every admitted setting must be translated into the
  // catalog properties consumed by iceberg-rust. Derive the set from the translation itself so a
  // new mapping cannot be forwarded by the write path while this gate still rejects it.
  // Per-bucket spellings for the data bucket are admitted through the same suffix list; settings
  // for other buckets do not affect this write. The bucket name is the whole name left after the
  // property suffix, so `fs.s3a.bucket.target.other.endpoint` belongs to `target.other`, not
  // `target`.
  private val SupportedHadoopS3Suffixes: Set[String] =
    NativeConfig.s3aSuffixToIcebergGlobalKey.keySet

  // S3A settings that cannot change the written object, where it is written or who writes it,
  // by `fs.s3a.` suffix. The native writer has its own connection pool, timeouts, retries and
  // upload buffering, issues no reads, listings or deletes through S3A, and commits through table
  // metadata rather than a Hadoop output committer, so none of these may make an otherwise clean
  // S3 write ineligible. Spark seeds the vectored-read and syncable settings into every session,
  // and clusters commonly set the rest cluster-wide. The data bucket's per-bucket spellings are
  // admitted too. Anything that can change a request stays unsupported: endpoint and region
  // selection, SSL, proxies, signing, requester pays, ACLs, storage class, checksums, headers and
  // access points. Encryption and credential settings are admitted only where the write proto
  // reproduces them (see S3AEncryptionSuffixes and S3ACredentialSuffixes). Audited against
  // `org.apache.hadoop.fs.s3a.Constants` in hadoop-aws 3.3.4 and 3.4.2.
  private val IgnoredHadoopS3Suffixes: Set[String] = Set(
    // Connection pool, threads and timeouts.
    "connection.maximum",
    "connection.establish.timeout",
    "connection.timeout",
    "connection.request.timeout",
    "connection.acquisition.timeout",
    "connection.idle.time",
    "connection.ttl",
    "connection.keepalive",
    "connection.part.upload.timeout",
    "connection.expect.continue",
    "socket.send.buffer",
    "socket.recv.buffer",
    "threads.max",
    "threads.keepalivetime",
    "max.total.tasks",
    "executor.capacity",
    "io.rate.limit",
    // Retries.
    "attempts.maximum",
    "retry.limit",
    "retry.interval",
    "retry.throttle.limit",
    "retry.throttle.interval",
    "retry.http.5xx.errors",
    "experimental.aws.s3.throttling",
    // Upload buffering and multipart sizing.
    "fast.upload",
    "fast.upload.buffer",
    "fast.upload.active.blocks",
    "fast.buffer.size",
    "buffer.dir",
    "block.size",
    "multipart.size",
    "multipart.threshold",
    "multipart.purge",
    "multipart.purge.age",
    "downgrade.syncable.exceptions",
    // Reads, listings and deletes.
    "readahead.range",
    "experimental.input.fadvise",
    "input.async.drain.threshold",
    "vectored.read.max.merged.size",
    "vectored.read.min.seek.size",
    "vectored.active.ranged.reads",
    "prefetch.enabled",
    "prefetch.block.size",
    "prefetch.block.count",
    "change.detection.mode",
    "change.detection.source",
    "change.detection.version.required",
    "paging.maximum",
    "list.version",
    "bulk.delete.page.size",
    "multiobjectdelete.enable",
    "etag.checksum.enabled",
    // Hadoop output committers, which Iceberg does not use.
    "committer.magic.enabled",
    "committer.name",
    "committer.threads",
    // Not an S3A setting: no hadoop-aws release from 3.3.4 to 3.5.0 reads `fs.s3a.region`, and
    // the region Comet forwards comes from `endpoint.region`.
    "region")

  // Many Spark configurations name S3A itself as the `s3a` filesystem, which changes nothing.
  private val FileSystemImplSuffix = "impl"
  private val S3AFileSystemClass = "org.apache.hadoop.fs.s3a.S3AFileSystem"
  // Stores without multipart uploads need `false`, which makes S3A send each file in one PUT. The
  // native writer starts a multipart upload once a file passes 5 MiB and cannot be told not to.
  private val MultipartUploadsSuffix = "multipart.uploads.enabled"

  // S3A server-side encryption, newer key names first. These are admitted when
  // hadoopS3EncryptionProperties translates them into the `s3.sse.*` keys iceberg-rust reads.
  private val S3AEncryptionAlgorithmSuffixes =
    Seq("encryption.algorithm", "server-side-encryption-algorithm")
  private val S3AEncryptionKeySuffixes = Seq("encryption.key", "server-side-encryption.key")
  private val S3AEncryptionSuffixes: Set[String] =
    (S3AEncryptionAlgorithmSuffixes ++ S3AEncryptionKeySuffixes).toSet

  // S3A credential settings. With an explicit provider list, iceberg-java's S3A writer signs with
  // exactly the listed providers, which the native writer's own credential chain cannot reproduce.
  // Such a write takes its credentials from that list through HadoopS3ACredentialProviderAdapter
  // (see usesS3ACredentialAdapter), and these settings are admitted then. Delegation tokens stay
  // unsupported, because the adapter refuses them.
  private val S3ACredentialProviderSuffix = "aws.credentials.provider"
  private val S3ACredentialProviderMappingSuffix = "aws.credentials.provider.mapping"
  private val S3ACredentialStoreSuffix = "security.credential.provider.path"
  private val S3ACredentialSuffixes: Set[String] = Set(
    S3ACredentialProviderSuffix,
    S3ACredentialProviderMappingSuffix,
    "assumed.role.arn",
    "assumed.role.session.name",
    "assumed.role.session.duration",
    "assumed.role.policy",
    "assumed.role.external.id",
    "assumed.role.sts.endpoint",
    "assumed.role.sts.endpoint.region",
    "assumed.role.credentials.provider",
    S3ACredentialStoreSuffix)

  private val HadoopS3ACredentialAdapterClass =
    "org.apache.comet.cloud.s3.HadoopS3ACredentialProviderAdapter"

  // The adapter cannot serve anonymous access, so a list that names an anonymous provider,
  // directly or through the provider mapping, keeps falling back.
  private val AnonymousS3ACredentialProviders: Set[String] = Set(
    "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
    "software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider",
    "com.amazonaws.auth.AnonymousAWSCredentials")

  private val HadoopCredentialStoreKey = "hadoop.security.credential.provider.path"

  // Every suffix the gate can admit. Their per-bucket spellings resolve through this set even
  // where the runtime Hadoop version does not declare the property.
  private val AdmittedHadoopS3Suffixes: Set[String] =
    SupportedHadoopS3Suffixes ++ IgnoredHadoopS3Suffixes ++ S3AEncryptionSuffixes ++
      S3ACredentialSuffixes + FileSystemImplSuffix + MultipartUploadsSuffix

  private val FsS3aPrefix = "fs.s3a."
  private val FsS3aBucketPrefix = "fs.s3a.bucket."

  private case class HadoopS3PropertyNames(exact: Set[String], prefixes: Seq[String]) {
    def matchingSuffix(keyWithoutBucketPrefix: String): Option[String] = {
      val exactMatches = exact.iterator.filter { suffix =>
        keyWithoutBucketPrefix == suffix || keyWithoutBucketPrefix.endsWith("." + suffix)
      }
      val prefixMatches = prefixes.iterator.flatMap { prefix =>
        val boundary = keyWithoutBucketPrefix.lastIndexOf("." + prefix)
        if (boundary < 0) None else Some(keyWithoutBucketPrefix.substring(boundary + 1))
      }
      (exactMatches ++ prefixMatches).toSeq.sortBy(-_.length).headOption
    }
  }

  // Hadoop's per-bucket key syntax has no separator between a dotted bucket name and its property
  // suffix. Resolve the suffix against the property names declared by the runtime Hadoop version,
  // so `bucket.target.other.encryption.algorithm` belongs to bucket `target.other`, even though
  // `encryption.algorithm` is unsupported by the native writer. If hadoop-aws is unavailable,
  // retain the admitted suffixes and let unknown spellings take the conservative path below.
  private lazy val HadoopS3Properties: HadoopS3PropertyNames =
    try {
      val suffixes = allStaticStringConstants(
        IcebergReflection.loadClass("org.apache.hadoop.fs.s3a.Constants"))
        .filter(key => key.startsWith(FsS3aPrefix) && !key.startsWith(FsS3aBucketPrefix))
        .map(_.stripPrefix(FsS3aPrefix))
        .filter(_.nonEmpty)
        .toSet ++ AdmittedHadoopS3Suffixes
      val (prefixes, exact) = suffixes.partition(_.endsWith("."))
      HadoopS3PropertyNames(exact, prefixes.toSeq.sorted)
    } catch {
      case _: ClassNotFoundException =>
        HadoopS3PropertyNames(AdmittedHadoopS3Suffixes, Seq.empty)
      case _: LinkageError => HadoopS3PropertyNames(AdmittedHadoopS3Suffixes, Seq.empty)
      case NonFatal(_) => HadoopS3PropertyNames(AdmittedHadoopS3Suffixes, Seq.empty)
    }

  // Audited against the pinned iceberg-rust S3 parser
  // (`iceberg/src/io/storage/config/s3.rs` and `storage/opendal/src/s3.rs`). Do not broaden this
  // to every s3.* / client.* property: FileIOBuilder accepts unknown keys, but the storage backend
  // silently ignores them. The provider class, token expiry, and web-identity settings are
  // consumed by Comet's credential paths rather than the storage parser. The expiry timestamp is
  // needed by the documented REST-vended credential provider. The web-identity settings tune the
  // built-in IRSA path when no explicit provider or credentials take precedence.
  private val SupportedS3FileIOProperties: Set[String] = Set(
    "s3.endpoint",
    "s3.access-key-id",
    "s3.secret-access-key",
    "s3.session-token",
    "s3.region",
    "client.region",
    "s3.path-style-access",
    "s3.sse.type",
    "s3.sse.key",
    "s3.sse.md5",
    "client.assume-role.arn",
    "client.assume-role.external-id",
    "client.assume-role.session-name",
    "s3.allow-anonymous",
    "s3.disable-ec2-metadata",
    "s3.disable-config-load",
    CometS3CredentialProviderClassProperty,
    "s3.comet.credential.webIdentity.enabled",
    "s3.comet.credential.webIdentity.maxAttempts",
    "s3.comet.credential.webIdentity.minTtlSeconds",
    "s3.comet.credential.webIdentity.refreshJitterSeconds",
    "s3.session-token-expires-at-ms")

  // S3FileIO settings that cannot change the written object, where it is written or who writes
  // it: multipart sizing and threads, the staging directory, retries, delete batching and client
  // preloading. The native storage backend has its own equivalents, so they need no translation.
  private val IgnoredS3FileIOProperties: Set[String] = Set(
    "s3.multipart.part-size-bytes",
    "s3.multipart.num-threads",
    "s3.multipart.threshold",
    "s3.staging-dir",
    "s3.retry.num-retries",
    "s3.retry.min-wait-ms",
    "s3.retry.max-wait-ms",
    "s3.delete.batch-size",
    "s3.delete.num-threads",
    "s3.preload-client-enabled")

  // A failed native write task deletes the files it wrote, as iceberg-java's writer does through
  // `FileIO.deleteFile`. S3FileIO skips that delete when this is false, so only true is harmless.
  private val S3FileIODeleteEnabledProperty = "s3.delete-enabled"

  // iceberg-java also defines "dsse-kms", but the pinned iceberg-rust S3 backend cannot map
  // that mode into an OpenDAL server-side-encryption configuration. Check the value as well as
  // the property name so it falls back during planning instead of failing in the native task.
  private val SupportedS3SseTypes: Set[String] = Set("none", "s3", "kms", "custom")

  private[comet] case class IcebergAwsPropertyNames(exact: Set[String], prefixes: Seq[String]) {
    def contains(key: String): Boolean =
      exact.contains(key) || prefixes.exists(key.startsWith)
  }

  // Used when S3FileIOProperties / AwsClientProperties cannot be linked. Every non-allow-listed
  // s3.* / client.* key then counts as Iceberg-owned. An empty vocabulary would do the opposite
  // and admit s3.acl once a custom credential provider is configured.
  private val UnclassifiedIcebergAwsProperties =
    IcebergAwsPropertyNames(Set.empty, Seq("client.", "s3."))

  private val IcebergAwsPropertyClasses = Seq(
    "org.apache.iceberg.aws.s3.S3FileIOProperties",
    "org.apache.iceberg.aws.AwsClientProperties",
    "org.apache.iceberg.aws.AwsProperties")

  // A configured Comet credential provider receives the complete, unfiltered FileIO property
  // bag. It may therefore consume vendor-owned s3.* / client.* keys that neither iceberg-java nor
  // iceberg-rust knows about. Keep rejecting the standard Iceberg properties that the native
  // storage path cannot honour, however. Reading the constants from the runtime Iceberg version
  // keeps this classification aligned with every supported profile and makes newly-added Iceberg
  // properties fail closed without mistaking them for provider-owned configuration.
  //
  // Those classes reference the AWS SDK. HadoopFileIO and Comet's provider SPI do not require it,
  // and linking the classes then throws NoClassDefFoundError. getSupportLevel only catches
  // NonFatal, so the lookup itself must turn that linkage failure into a support decision.
  private lazy val IcebergAwsProperties: IcebergAwsPropertyNames =
    icebergAwsPropertyNames(IcebergReflection.loadClass)

  private[comet] def icebergAwsPropertyNames(
      loadClass: String => Class[_]): IcebergAwsPropertyNames =
    try {
      val propertyNames = IcebergAwsPropertyClasses.flatMap { className =>
        staticStringConstants(loadClass(className))
      }
      val (prefixes, exact) = propertyNames.distinct.partition(_.endsWith("."))
      IcebergAwsPropertyNames(exact.toSet, prefixes.sorted)
    } catch {
      case _: ClassNotFoundException => UnclassifiedIcebergAwsProperties
      case _: LinkageError => UnclassifiedIcebergAwsProperties
    }

  private def staticStringConstants(cls: Class[_]): Iterator[String] =
    allStaticStringConstants(cls)
      .filter(key => key.startsWith("s3.") || key.startsWith("client."))

  private def allStaticStringConstants(cls: Class[_]): Iterator[String] =
    cls.getDeclaredFields.iterator
      .filter(field => Modifier.isStatic(field.getModifiers) && field.getType == classOf[String])
      .flatMap { field =>
        field.setAccessible(true)
        Option(field.get(null).asInstanceOf[String])
      }

  // The parquet-mr settings that change iceberg-java's data files beyond their compressed bytes.
  // The writer copies every table and write property into its Hadoop configuration and builds the
  // ParquetProperties itself, so of all `parquet.*` keys, from either source, only the column
  // index truncation length, read by Iceberg's ParquetWriter, does. The codec-level side channels
  // only change compressed bytes, which are an accepted divergence: parquet-mr's ZstandardCodec
  // reads the zstd level and worker count, Hadoop's codecs read `zlib.compress.level` and
  // `compression.brotli.quality`, and `io.compression.codec.zstd.level` only keys Iceberg's codec
  // cache. Any other `parquet.*` key, including one parquet-mr does not declare, changes nothing.
  // `parquet.enable.dictionary` is read from the table and write properties only and has its own
  // rule. CometIcebergWriteActionSuite classifies the keys parquet-mr's ParquetOutputFormat and
  // ZstandardCodec declare, fails on a new one, and checks that iceberg-java writes the same bytes
  // with the inert ones set. A key read anywhere else is checked by hand on an Iceberg bump.
  private val UnsupportedParquetMrKeys: Set[String] = Set("parquet.columnindex.truncate.length")

  private lazy val vettedParquetWriteKeys: Set[String] = Set(
    PropertyKeys.ParquetCompressionCodec,
    PropertyKeys.ParquetCompressionLevel,
    PropertyKeys.ParquetRowGroupSizeBytes,
    PropertyKeys.ParquetPageSizeBytes,
    PropertyKeys.ParquetPageRowLimit,
    PropertyKeys.ParquetDictSizeBytes,
    PropertyKeys.ParquetRowGroupCheckMinRecordCount,
    PropertyKeys.ParquetRowGroupCheckMaxRecordCount,
    PropertyKeys.ParquetPageVersion,
    PropertyKeys.ParquetShredVariants,
    PropertyKeys.ParquetVariantBufferSize,
    PropertyKeys.ParquetBloomFilterMaxBytes)

  private lazy val vettedParquetWritePrefixes: Seq[String] =
    Seq(
      PropertyKeys.BloomFilterColumnEnabledPrefix,
      PropertyKeys.ParquetBloomFilterColumnFppPrefix,
      PropertyKeys.ParquetBloomFilterColumnNdvPrefix)

  override def getSupportLevel(op: IcebergWriteExec): SupportLevel = {
    // Detection may be repeated on the same plan under different settings.
    op.unsetTagValue(BloomPathsTag)
    try {
      checkTriggers(op) match {
        case Some(reason) =>
          op.unsetTagValue(BloomPathsTag)
          Unsupported(Some(reason))
        case None => Compatible(None)
      }
    } catch {
      case NonFatal(e) =>
        op.unsetTagValue(BloomPathsTag)
        Unsupported(Some(s"Iceberg native write detection failed: ${e.getMessage}"))
    }
  }

  private def checkTriggers(op: IcebergWriteExec): Option[String] = {
    op.dispatch match {
      case PositionDeltaWrite(_) =>
        return Some("Iceberg WriteDelta executes through the JVM DeltaWriter")
      case _ =>
    }

    val batchWrite = op.batchWrite
    if (!IcebergReflection.isIcebergBatchWrite(batchWrite)) {
      return Some(s"not an Iceberg SparkWrite: ${batchWrite.getClass.getName}")
    }

    val sparkWrite = IcebergReflection
      .getOuterSparkWrite(batchWrite)
      .getOrElse(return Some("could not unwrap SparkWrite"))
    val table = IcebergReflection
      .getTableFromSparkWrite(sparkWrite)
      .getOrElse(return Some("SparkWrite.table is null"))

    val tableProperties = IcebergReflection
      .getTableProperties(table)
      .map(_.asScala.toMap)
      .getOrElse(Map.empty[String, String])
    val writeProperties = IcebergReflection
      .getWritePropertiesFromSparkWrite(sparkWrite)
      .getOrElse(return Some("could not read SparkWrite.writeProperties"))

    val hadoopConfs = new LazyHadoopConfs(
      () => op.session.sessionState.newHadoopConf(),
      sessionConf => effectiveHadoopConf(op, table, sessionConf))
    val context =
      TriggerContext(
        table,
        tableProperties ++ writeProperties,
        sparkWrite,
        hadoopConfs,
        CometConf.COMET_ICEBERG_NATIVE_WRITE_BLOOM_FILTER_FOLDING_ENABLED.get(op.conf),
        op)
    triggers.iterator.map(rule => rule(context)).collectFirst { case Some(reason) => reason }
  }

  private case class TriggerContext(
      table: Any,
      properties: Map[String, String],
      sparkWrite: Any,
      hadoopConfs: LazyHadoopConfs,
      bloomFilterFoldingEnabled: Boolean,
      op: IcebergWriteExec) {
    def hadoopConf: Configuration = hadoopConfs.session
    def s3HadoopConf: Configuration = hadoopConfs.effective
    lazy val fileIOProperties: Map[String, String] =
      IcebergReflection.getFileIOProperties(table).getOrElse(Map.empty)
  }

  private[comet] final class LazyHadoopConfs(
      sessionConfBuilder: () => Configuration,
      effectiveConfBuilder: Configuration => Configuration) {
    lazy val session: Configuration = sessionConfBuilder()
    lazy val effective: Configuration = effectiveConfBuilder(session)
  }

  private type TriggerRule = TriggerContext => Option[String]

  private def effectiveHadoopConf(
      op: IcebergWriteExec,
      table: Any,
      sessionConf: Configuration): Configuration = {
    val catalogOverrides = IcebergReflection
      .deriveCatalogName(table)
      .map { catalogName =>
        val prefix = s"spark.sql.catalog.$catalogName.hadoop."
        op.session.sessionState.conf.getAllConfs.collect {
          case (key, value) if key.startsWith(prefix) => key.substring(prefix.length) -> value
        }
      }
      .getOrElse(Map.empty)

    IcebergReflection.getFileIOHadoopConf(table) match {
      case None =>
        // S3FileIO and other non-Hadoop FileIO implementations use their initialized
        // properties, not Spark's Hadoop options. Forwarding session or catalog settings here
        // can redirect native writes away from the FileIO used for footer reads and cleanup.
        new Configuration(false)
      case Some(fileIOConf) =>
        // HadoopFileIO stores its configuration in Iceberg's SerializableConfiguration. That
        // class rebuilds a Configuration(false) by calling set() for every entry, so Hadoop's
        // property-source metadata is lost and core-default.xml values look programmatic.
        // The initialized FileIO remains authoritative for values: SparkCatalog does not
        // reinitialize it when session or catalog options change. Recover only default-source
        // metadata, without adding or replacing any FileIO configuration. Load core-default.xml
        // separately so later session settings cannot change the default values used here.
        val coreDefaults = new Configuration(false)
        coreDefaults.addResource("core-default.xml")
        val effectiveConf = new Configuration(fileIOConf)
        fileIOConf.iterator().asScala.foreach { entry =>
          val key = entry.getKey
          val fileIOValue = fileIOConf.getRaw(key)
          val explicitlyConfiguredValue = catalogOverrides.get(key).contains(fileIOValue) ||
            (hasExplicitSource(sessionConf, key) && fileIOValue == sessionConf.getRaw(key))
          if (!explicitlyConfiguredValue &&
            Option(fileIOConf.getPropertySources(key))
              .exists(_.toSeq == Seq("programmatically")) &&
            fileIOValue == coreDefaults.getRaw(key)) {
            effectiveConf.set(key, fileIOValue, "core-default.xml")
          }
        }
        effectiveConf
    }
  }

  // A key counts as set only when a source other than Hadoop's built-in core-default.xml set it.
  // Hadoop's iterator includes the many fs.s3a.* defaults loaded from there, which are library
  // defaults rather than settings a user selected, and counting them would reject every ordinary
  // S3 write. Site XML and programmatic or Spark sources count, and so does a user resource
  // merely named `tenant-default.xml`.
  private def hasExplicitSource(conf: Configuration, key: String): Boolean =
    Option(conf.getPropertySources(key)) match {
      case Some(sources) if sources.nonEmpty => !sources.forall(isCoreDefaultResource)
      case _ => conf.getRaw(key) != null
    }

  private lazy val triggers: Seq[TriggerRule] = Seq(
    requireFormatParquet,
    requirePropertyAbsentOrNotTrue(
      PropertyKeys.ObjectStoreEnabled,
      "object-storage layout unsupported"),
    requirePropertyAbsent(
      PropertyKeys.WriteLocationProviderImpl,
      "custom location provider unsupported"),
    requireDefaultLocationProvider,
    requireSupportedFormatVersion,
    requireNoMetadataColumns,
    requireSupportedColumnTypes,
    requireNoVoidFieldWithDroppedSource,
    requireNoUnknownPartitionSource,
    requireNoEncryptionPrefix,
    requireRowGroupCheckMinRecordCountAtDefault,
    requireRowGroupCheckMaxRecordCountAtDefault,
    requireParquetPageVersionDefault,
    requireNativeSupportedCompressionCodec,
    requireNativeSupportedCompressionLevel,
    requireLoadableBrotliCodec,
    requireSupportedBloomFilterRuntime,
    requireNativeSupportedBloomFilterProperties,
    requireSupportedBloomFilterPaths,
    requireBloomFilterFoldingOptIn,
    requireOnlyVettedParquetWriteProperties,
    requirePropertyAbsent(
      PropertyKeys.ParquetEnableDictionary,
      "dictionary override unsupported"),
    requireNoUnsupportedParquetMrProperties,
    requireRecognizedTableFileIO,
    requirePlaintextEncryptionManager,
    requirePositiveIntParquetSizes,
    requireNoUnsupportedParquetMrHadoopSettings,
    requireSupportedStorageScheme,
    requireSupportedHadoopS3Settings,
    requireSupportedS3FileIOProperties,
    requireGcsFileIOForGcsDataLocation,
    requireExecutorReflectionResolvable)

  private val requireFormatParquet: TriggerRule = ctx =>
    IcebergReflection.getFormatFromSparkWrite(ctx.sparkWrite) match {
      case None => Some("could not resolve the effective write format from SparkWrite")
      case Some("parquet") => None
      case Some(other) => Some(s"resolved write format=$other (only parquet is supported)")
    }

  private def requirePropertyAbsentOrNotTrue(key: String, reason: String): TriggerRule =
    ctx => {
      if (ctx.properties.get(key).exists(_.equalsIgnoreCase("true"))) {
        Some(s"$key=true ($reason)")
      } else {
        None
      }
    }

  private def requirePropertyAbsent(key: String, reason: String): TriggerRule =
    ctx => {
      if (ctx.properties.contains(key)) Some(s"$key is set ($reason)") else None
    }

  // The property rule above only sees providers configured through table/write properties. A
  // custom TableOperations can return a LocationProvider directly, while the native writer always
  // generates `<data location>/<partition path>/<file>`. Admit only Iceberg's default provider;
  // object-storage layout is already declined by the preceding property rule.
  //
  // Before Iceberg 1.11, iceberg-java does not preserve that TableOperations-supplied provider on
  // executors: it reconstructs the provider from the table location and properties, so those
  // writes use the default layout anyway. From 1.11 on, iceberg-java keeps and uses the custom
  // provider. This gate stays unconditional and fail-closed on every Iceberg version Comet pins,
  // so a non-default provider always falls back.
  private val requireDefaultLocationProvider: TriggerRule = ctx =>
    IcebergReflection.getLocationProvider(ctx.table) match {
      case None =>
        Some("could not resolve table.locationProvider() for native write compatibility checking")
      case Some(provider)
          if provider.getClass.getName == IcebergReflection.ClassNames.DEFAULT_LOCATION_PROVIDER =>
        None
      case Some(provider) =>
        Some(
          s"table.locationProvider() is ${provider.getClass.getName}, " +
            "which the native write path would bypass")
    }

  // Format version 4 is still being specified, and its metadata may change under the writer.
  private val requireSupportedFormatVersion: TriggerRule = ctx =>
    IcebergReflection.getFormatVersion(ctx.table) match {
      case Some(v) if v > MaxSupportedFormatVersion => Some(s"format-version=$v unsupported")
      case Some(_) => None
      case None => Some("could not determine the table format-version")
    }

  // On a format-version 3 table, iceberg-java 1.10+ adds the row lineage columns `_row_id` and
  // `_last_updated_sequence_number` to the write schema when the write rewrites existing rows
  // (copy-on-write DELETE, UPDATE and MERGE, and rewrite_data_files), and fills them from each
  // row's metadata. The native writer writes the data columns only. Other v3 writes carry no
  // lineage columns: the driver assigns their rows' ids at commit time, as it does for
  // iceberg-java's files. Matching on the reserved id range rather than the column names keeps
  // any other metadata column out too.
  private val requireNoMetadataColumns: TriggerRule = ctx =>
    IcebergReflection
      .getWriteSchemaFromSparkWrite(ctx.sparkWrite)
      .flatMap(IcebergReflection.getSchemaFieldIds) match {
      case None => Some("could not resolve the write schema's field ids")
      case Some(fields) =>
        fields.collectFirst {
          case (name, id) if id > IcebergReflection.MaxDataFieldId =>
            s"write schema includes metadata column $name, which iceberg-java fills with row " +
              "lineage and the native writer does not write"
        }
    }

  // Iceberg maps `uuid` to Spark's StringType, so the native writer would receive a Utf8 column
  // while iceberg-rust's target Arrow schema demands FixedSizeBinary(16) -- no Arrow cast bridges
  // the two, so the write would pass detection and then fail the task. Decline it up front, along
  // with the v3-only types in `UnsupportedWriteTypeIds`. `fixed(N)` arrives as Binary and casts to
  // FixedSizeBinary(N), so it needs no rule.
  private val requireSupportedColumnTypes: TriggerRule = ctx =>
    IcebergReflection
      .getWriteSchemaFromSparkWrite(ctx.sparkWrite)
      .orElse(IcebergReflection.getSchema(ctx.table)) match {
      case None => Some("could not resolve the write schema for column type checking")
      case Some(schema) =>
        IcebergReflection
          .findFieldWithTypeIds(schema, UnsupportedWriteTypeIds)
          // Only top-level `unknown` fields are pruned, so a nested one would still
          // get a Parquet column that iceberg-java does not write.
          .orElse(
            IcebergReflection.findFieldWithTypeIds(schema, Set("UNKNOWN"), nestedOnly = true))
          .map { case (name, typeId) =>
            s"column $name has Iceberg type ${typeId.toLowerCase(Locale.ROOT)}, " +
              "which the native writer cannot reproduce"
          }
          // Pruning would leave no columns, which iceberg-java cannot write either.
          .orElse(if (IcebergReflection.toSparkSchema(schema).forall(_.dataType == NullType)) {
            Some("every column has Iceberg type unknown")
          } else None)
    }

  // A format-version-1 spec keeps a dropped partition field as a `void` transform, and its source
  // column can be dropped afterwards. iceberg-java cannot write through a spec that mixes such a
  // field with a live one, and the native writer fails resolving the spec's partition type, so
  // decline and let the write fail the way iceberg-java fails it. An all-`void` spec stays
  // eligible, since the native writer writes it unpartitioned.
  // https://github.com/apache/datafusion-comet/issues/6141
  private val requireNoVoidFieldWithDroppedSource: TriggerRule =
    requireOutputPartitionSpec { spec =>
      IcebergReflection.voidFieldsWithDroppedSource(spec).headOption.map { name =>
        s"partition field $name is a void transform whose source column was dropped, " +
          "beside a live partition field"
      }
    }

  // Top-level `unknown` columns are pruned from the native schema, so a partition field sourced
  // from one would fail the native partition calculator with `Field not found`.
  private val requireNoUnknownPartitionSource: TriggerRule =
    requireOutputPartitionSpec { spec =>
      IcebergReflection.partitionFieldsWithUnknownSource(spec).headOption.map { name =>
        s"partition field $name is sourced from a column of Iceberg type unknown"
      }
    }

  // Applies `check` to the output partition spec, declining if it cannot be resolved or inspected.
  private def requireOutputPartitionSpec(check: Any => Option[String]): TriggerRule = ctx =>
    IcebergReflection
      .getOutputSpecIdFromSparkWrite(ctx.sparkWrite)
      .flatMap(IcebergReflection.getPartitionSpecById(ctx.table, _)) match {
      case None => Some("could not resolve the output partition spec")
      case Some(spec) =>
        try {
          check(spec)
        } catch {
          case e: Exception =>
            Some(s"could not inspect the output partition spec: ${e.getMessage}")
        }
    }

  private val requireNoEncryptionPrefix: TriggerRule = ctx =>
    ctx.properties.keys
      .find(_.startsWith(EncryptionPropertyPrefix))
      .map(k => s"$k set: encryption unsupported")

  // No metrics-mode gate: manifest `DataFile` metrics are re-derived on the JVM from the
  // written parquet footers with Iceberg's own `MetricsConfig` logic before commit (see
  // `CometIcebergWriteExec`), so every `write.metadata.metrics.*` value behaves exactly as it
  // does on the iceberg-java path.

  // Compared exactly: Iceberg 1.11 rejects `V1` ("must be v1 or v2"), so a trimmed or
  // lowercased match would write natively a table whose iceberg-java write fails. Earlier
  // versions ignore the property and always write v1 pages, so there a fallback only gives up
  // a native write that would have been safe.
  private val requireParquetPageVersionDefault: TriggerRule = ctx => {
    val key = PropertyKeys.ParquetPageVersion
    ctx.properties
      .get(key)
      .filter(_ != PropertyKeys.ParquetPageVersionDefault)
      .map(v => s"$key=$v unsupported")
  }

  // No variant-shredding gate: iceberg-java shreds only top-level `variant` columns (Iceberg 1.11's
  // ParquetFormatModel), and requireSupportedColumnTypes declines a `variant` column at any depth.
  // Spark 4.x on Iceberg 1.11 resolves `write.parquet.shred-variants` into every Parquet write, so
  // gating it would make every write fall back in a session that turns shredding on.

  // iceberg-java resolves the codec name with `valueOf(name.toUpperCase(Locale.ENGLISH))`, so it
  // rejects `none`, a name with stray whitespace and a typo. A native write must not succeed
  // where that write fails, so those fall back and fail as they do without Comet.
  private val requireNativeSupportedCompressionCodec: TriggerRule = ctx =>
    IcebergWriteProtoTranslation.compressionCodecRejection(ctx.properties)

  // iceberg-java never validates the level -- the raw string flows into a codec-specific
  // parquet-mr writer property -- while parquet-rs enforces per-codec ranges when the native
  // writer is built. A level the JVM writer accepts (zstd 0, a negative zstd fast level) must
  // not become a mid-task native failure, and a non-integer must fail on the stock path; both
  // fall back. Range logic lives beside the codec resolution in IcebergWriteProtoTranslation.
  private val requireNativeSupportedCompressionLevel: TriggerRule = ctx =>
    IcebergWriteProtoTranslation.compressionLevelRejection(ctx.properties)

  // parquet-mr compresses brotli pages, and decompresses them for every JVM reader, with Hadoop's
  // `BrotliCodec`, which it loads by name. Neither Spark nor Iceberg ships that class. Without a
  // brotli codec jar, iceberg-java fails the write, and parquet-rs would write files that only a
  // native reader can read, so decline and let the write fail the way iceberg-java fails it.
  // https://github.com/apache/datafusion-comet/issues/6960
  private val requireLoadableBrotliCodec: TriggerRule = ctx =>
    if (IcebergWriteProtoTranslation.compressesWithBrotli(ctx.properties) &&
      !isLoadable(BrotliCodecClass)) {
      Some(
        s"${PropertyKeys.ParquetCompressionCodec}=brotli needs $BrotliCodecClass, which is not " +
          "on the classpath (iceberg-java fails the write and JVM readers cannot read the files)")
    } else {
      None
    }

  // Through the context class loader, which Spark adds application jars to, and without
  // initializing the class, so the check runs none of the codec's code.
  private def isLoadable(className: String): Boolean =
    try {
      ClassLoaders.contextOrDefault(getClass.getClassLoader).loadClass(className)
      true
    } catch {
      case _: ClassNotFoundException | _: LinkageError => false
    }

  // These are Apache Parquet Java BlockSplitBloomFilter implementation bounds, not Iceberg
  // TableProperties constants, so they cannot be obtained through IcebergReflection:
  // scalastyle:off line.size.limit
  // https://github.com/apache/parquet-java/blob/78a8d3230eb4769db93de5f2f2e18363c04cae81/parquet-column/src/main/java/org/apache/parquet/column/values/bloomfilter/BlockSplitBloomFilter.java#L40-L50
  // scalastyle:on line.size.limit
  private[operator] val MinBloomFilterBytes = 32
  private[operator] val MaxBloomFilterBytes = 128 * 1024 * 1024
  private val BloomFilterHashProbes = 8
  private val MaxNonOverflowingBloomFilterNdv = Long.MaxValue / BloomFilterHashProbes

  /**
   * Keep only Bloom shape properties interpreted by the Iceberg runtime on the classpath. Older
   * Iceberg releases leave these table properties untouched but do not pass them to parquet-mr.
   * Ignoring them here preserves that version's JVM-writer behavior while allowing the remaining
   * supported Bloom configuration to execute natively.
   */
  private def interpretedBloomFilterProperties(
      properties: Map[String, String]): Map[String, String] = {
    val unsupportedPrefixes = Seq(
      PropertyKeys.ParquetBloomFilterColumnFppPrefix ->
        "PARQUET_BLOOM_FILTER_COLUMN_FPP_PREFIX",
      PropertyKeys.ParquetBloomFilterColumnNdvPrefix ->
        "PARQUET_BLOOM_FILTER_COLUMN_NDV_PREFIX").collect {
      case (prefix, constant) if IcebergReflection.tablePropertyConstantOpt(constant).isEmpty =>
        prefix
    }
    properties.filterNot { case (key, _) => unsupportedPrefixes.exists(key.startsWith) }
  }

  /**
   * parquet-rs 59.3.0 initially represents Bloom filters as a power-of-two number of bytes and
   * may fold that allocation after values are inserted. parquet-mr accepts arbitrary caps and,
   * when one binds, serializes that exact length. Keep those writes on the classic path instead
   * of silently changing the number of usable Bloom blocks.
   */
  private val requireNativeSupportedBloomFilterProperties: TriggerRule = ctx => {
    val properties = interpretedBloomFilterProperties(ctx.properties)
    val configuredMaxBytes = properties.get(PropertyKeys.ParquetBloomFilterMaxBytes).map { raw =>
      raw -> scala.util.Try(java.lang.Integer.parseInt(raw)).toOption
    }
    val malformedMaxRejection = configuredMaxBytes.collect {
      case (raw, None) =>
        s"${PropertyKeys.ParquetBloomFilterMaxBytes}=$raw is not a Java int"
      case (raw, Some(value)) if value <= 0 =>
        s"${PropertyKeys.ParquetBloomFilterMaxBytes}=$raw must be positive"
    }
    // Iceberg visits every enabled-prefix entry and applies enabled, FPP, then NDV. With no such
    // entry, a positive, parseable maximum cannot affect the file and must not force an
    // otherwise-compatible write onto the classic path. Keep rejecting malformed and
    // non-positive values because Iceberg validates them even with no Bloom column.
    val configured = IcebergWriteProtoTranslation.configuredBloomFilterColumnNames(properties)
    if (configured.isEmpty) {
      malformedMaxRejection
    } else {
      val maxRejection = malformedMaxRejection.orElse {
        configuredMaxBytes.collect {
          case (_, Some(value))
              if value < MinBloomFilterBytes || value > MaxBloomFilterBytes ||
                (value & (value - 1)) != 0 =>
            s"${PropertyKeys.ParquetBloomFilterMaxBytes}=$value must be a power of two " +
              s"in [$MinBloomFilterBytes, $MaxBloomFilterBytes] for native writes"
        }
      }

      maxRejection.orElse {
        val maxBytes = configuredMaxBytes
          .flatMap(_._2)
          .getOrElse(IcebergWriteProtoTranslation.Defaults.BloomFilterMaxBytes)
        // Validate associated shape properties even for enabled=false; a valid NDV re-enables
        // the filter in parquet-mr.
        configured.iterator
          .flatMap { column =>
            val fppKey = PropertyKeys.ParquetBloomFilterColumnFppPrefix + column
            val ndvKey = PropertyKeys.ParquetBloomFilterColumnNdvPrefix + column
            val parsedFpp = properties.get(fppKey) match {
              case Some(raw) => scala.util.Try(java.lang.Double.parseDouble(raw)).toOption
              case None => Some(IcebergWriteProtoTranslation.Defaults.BloomFilterFpp)
            }
            val parsedNdv = properties
              .get(ndvKey)
              .flatMap(raw => scala.util.Try(java.lang.Long.parseLong(raw)).toOption)
            val fppError = properties.get(fppKey).flatMap { raw =>
              parsedFpp match {
                case Some(value)
                    if value > 0.0d && value < 1.0d && java.lang.Double.isFinite(value) &&
                      bloomFilterSizesRepresentable(maxBytes, value) =>
                  None
                case Some(value)
                    if value > 0.0d && value < 1.0d && java.lang.Double.isFinite(value) =>
                  Some(s"$fppKey=$raw cannot represent the configured native Bloom sizes")
                case _ => Some(s"$fppKey=$raw must be a finite double strictly between 0 and 1")
              }
            }
            val ndvError = properties.get(ndvKey).flatMap { raw =>
              parsedNdv match {
                case Some(value) if value > 0L && value <= MaxNonOverflowingBloomFilterNdv => None
                case Some(value) if value > MaxNonOverflowingBloomFilterNdv =>
                  Some(s"$ndvKey=$raw exceeds $MaxNonOverflowingBloomFilterNdv; " +
                    "parquet-mr Bloom sizing may overflow")
                case _ => Some(s"$ndvKey=$raw must be a positive Java long")
              }
            }
            val ignoredMinimumCapError = parsedNdv.collect {
              case ndv
                  if maxBytes == MinBloomFilterBytes && ndv > 0L &&
                    ndv <= MaxNonOverflowingBloomFilterNdv &&
                    parsedFpp.exists(
                      parquetMrRequestedBloomFilterBytes(ndv, _) > MinBloomFilterBytes) =>
                s"${PropertyKeys.ParquetBloomFilterMaxBytes}=$MinBloomFilterBytes is ignored by " +
                  s"parquet-mr for $ndvKey=$ndv"
            }
            Seq(fppError, ndvError, ignoredMinimumCapError).flatten
          }
          .toSeq
          .headOption
      }
    }
  }

  private val requireSupportedBloomFilterRuntime: TriggerRule = ctx => {
    val prefixes = Seq(
      PropertyKeys.BloomFilterColumnEnabledPrefix,
      PropertyKeys.ParquetBloomFilterColumnFppPrefix,
      PropertyKeys.ParquetBloomFilterColumnNdvPrefix)
    if (ctx.properties.keys.exists(key => prefixes.exists(key.startsWith))) {
      bloomFilterRuntimeRejection(IcebergReflection.icebergVersion(), ctx.properties)
    } else {
      None
    }
  }

  private def bloomFilterVersion(icebergVersion: String): Option[(Int, Int)] = {
    if (Option(icebergVersion).exists(_.matches("\\d+\\.\\d+\\.\\d+(?:[-+].+)?"))) {
      val parts = icebergVersion.split("[.-]", 3)
      scala.util.Try((parts(0).toInt, parts(1).toInt)).toOption
    } else {
      None
    }
  }

  private[operator] def bloomFilterRuntimeRejection(
      icebergVersion: String,
      properties: Map[String, String] = Map.empty): Option[String] = {
    bloomFilterVersion(icebergVersion) match {
      case Some((1, minor))
          if (minor == 6 || minor == 7) && properties.keys.exists(
            _.startsWith(PropertyKeys.ParquetBloomFilterColumnFppPrefix)) =>
        Some(
          s"Iceberg $icebergVersion applies ${PropertyKeys.ParquetBloomFilterColumnFppPrefix}* " +
            "independently; these settings require Java validation")
      case Some((1, minor)) if minor >= 5 => None
      case Some((1, _)) =>
        Some(s"Native Bloom writes require Iceberg 1.5 or later; found '$icebergVersion'")
      case _ =>
        Some(
          s"Cannot determine Iceberg Bloom column-name convention for version '$icebergVersion'")
    }
  }

  private val requireSupportedBloomFilterPaths: TriggerRule = ctx => {
    // Resolve disabled entries too: a second spelling can disable or change the same leaf.
    val columns = IcebergWriteProtoTranslation.configuredBloomFilterColumnNames(ctx.properties)
    if (columns.isEmpty) {
      None
    } else {
      val schema = IcebergReflection
        .getWriteSchemaFromSparkWrite(ctx.sparkWrite)
        .getOrElse(throw new IllegalStateException("Could not resolve Bloom write schema"))
      val resolution = IcebergReflection
        .getParquetPathResolution(schema)
        .getOrElse(throw new IllegalStateException("Could not resolve Bloom Parquet paths"))
      val icebergVersion = IcebergReflection.icebergVersion()
      bloomFilterParquetPaths(columns, resolution, icebergVersion, schema) match {
        case Left(reason) => Some(reason)
        case Right(paths) =>
          val properties = interpretedBloomFilterProperties(ctx.properties)
          val enabled = IcebergWriteProtoTranslation.enabledBloomFilterColumnNames(properties)
          val affected = enabled.filter { column =>
            paths.get(column).exists(resolution.floatingPointPhysicalPaths.contains)
          }
          val renamed = enabled.filter { column =>
            paths.get(column).exists(resolution.renamedPhysicalPaths.contains)
          }
          if (affected.nonEmpty) {
            // SQL-equal NaNs can have different payloads. Preserve Java's writer until the
            // native writer canonicalizes these values before hashing them.
            Some(
              s"Bloom filters on FLOAT/DOUBLE columns ${affected.sorted.mkString(", ")} " +
                "require the Java writer to preserve NaN equality")
          } else if (renamed.nonEmpty) {
            // Iceberg Java sanitizes these names while iceberg-rust preserves them. A Java
            // path would silently miss the native column until paths travel structurally.
            Some(
              "Bloom-filter columns are renamed in Iceberg Java's Parquet schema: " +
                renamed.sorted.mkString(", "))
          } else {
            ctx.op.setTagValue(
              BloomPathsTag,
              PreparedBloomPaths(schema.asInstanceOf[AnyRef], icebergVersion, properties, paths))
            None
          }
      }
    }
  }

  private[operator] def bloomFilterParquetPaths(
      configuredColumns: Seq[String],
      resolution: IcebergReflection.ParquetPathResolution,
      icebergVersion: String,
      schema: Any): Either[String, Map[String, String]] = {
    val paths = bloomFilterVersion(icebergVersion) match {
      case Some((1, minor)) if minor >= 5 && minor < 8 =>
        // ColumnPath.fromDotString uses Java split's trailing-empty-component removal. Keep
        // the original spelling as the key so its associated properties remain attached.
        configuredColumns.flatMap { name =>
          val path = name.split("\\.").mkString(".")
          if (resolution.physicalPaths.contains(path)) Some(name -> path) else None
        }.toMap
      case Some((1, minor)) if minor >= 8 && minor < 10 =>
        if (configuredColumns.exists(_.isEmpty)) {
          return Left("Empty Bloom column name requires Java validation")
        }
        configuredColumns.flatMap { name =>
          IcebergReflection
            .findFieldIdByName(schema, name)
            .flatMap(resolution.pathByFieldId.get)
            .map(name -> _)
        }.toMap
      case Some((1, minor)) if minor >= 10 =>
        resolution.pathByIcebergColumnName.filter { case (column, _) =>
          configuredColumns.contains(column)
        }
      case _ =>
        return Left(
          bloomFilterRuntimeRejection(icebergVersion)
            .getOrElse(s"Unsupported Iceberg Bloom naming convention: $icebergVersion"))
    }
    val collisions = paths.toSeq.groupBy(_._2).collect {
      case (path, entries) if entries.size > 1 =>
        s"$path (${entries.map(_._1).sorted.mkString(", ")})"
    }
    if (collisions.isEmpty) {
      Right(paths)
    } else {
      Left(
        "Multiple Bloom column names resolve to the same Parquet leaf: " +
          collisions.toSeq.sorted.mkString("; "))
    }
  }

  private val requireBloomFilterFoldingOptIn: TriggerRule = ctx => {
    val properties = interpretedBloomFilterProperties(ctx.properties)
    if (IcebergWriteProtoTranslation.hasEnabledBloomFilters(properties) &&
      !ctx.bloomFilterFoldingEnabled) {
      Some(
        s"${CometConf.COMET_ICEBERG_NATIVE_WRITE_BLOOM_FILTER_FOLDING_ENABLED.key}=false " +
          "(native Parquet Bloom-filter folding is not enabled)")
    } else {
      None
    }
  }

  // Planning-time counterpart of the native inverse-NDV check. A target B is safely encoded by
  // aiming at 3B/4, in the interior of parquet-rs's (B/2, B] round-up interval. Requiring every
  // power-of-two through the configured cap is conservative and keeps pathological-but-valid
  // floating-point FPPs on the JVM path rather than discovering them after task launch.
  private[operator] def bloomFilterSizesRepresentable(maxBytes: Int, fpp: Double): Boolean = {
    val denominator =
      -Math.log(1.0d - Math.pow(fpp, 1.0d / BloomFilterHashProbes.toDouble))
    if (!java.lang.Double.isFinite(denominator) || denominator <= 0.0d) return false

    Iterator
      .iterate(MinBloomFilterBytes)(_ * 2)
      .takeWhile(_ <= maxBytes)
      .forall { target =>
        val ndv = Math.max(1L, Math.round(target.toDouble * 0.75d * denominator))
        val calculatedBits = (-BloomFilterHashProbes.toDouble * ndv.toDouble /
          Math.log(1.0d - Math.pow(fpp, 1.0d / BloomFilterHashProbes.toDouble))).toLong
        val rawBytes = Math.max(
          MinBloomFilterBytes.toLong,
          Math.min(MaxBloomFilterBytes.toLong, calculatedBits / 8L))
        val allocated = java.lang.Long.highestOneBit(rawBytes - 1L) << 1
        allocated == target.toLong
      }
  }

  /** The uncapped byte count parquet-mr passes to its explicit-NDV constructor path. */
  private[operator] def parquetMrRequestedBloomFilterBytes(ndv: Long, fpp: Double): Int = {
    // Keep the long multiplication before floating-point conversion to match parquet-mr:
    // scalastyle:off line.size.limit
    // https://github.com/apache/parquet-java/blob/78a8d3230eb4769db93de5f2f2e18363c04cae81/parquet-column/src/main/java/org/apache/parquet/column/values/bloomfilter/BlockSplitBloomFilter.java#L277-L301
    // scalastyle:on line.size.limit
    val calculated = (-BloomFilterHashProbes.toLong * ndv).toDouble /
      Math.log(1.0d - Math.pow(fpp, 1.0d / BloomFilterHashProbes.toDouble))
    val maxBits = MaxBloomFilterBytes << 3
    var bits = calculated.toInt
    if (bits > maxBits || calculated < 0.0d) bits = maxBits
    val bitsPerBlock = MinBloomFilterBytes << 3
    bits = (bits + bitsPerBlock - 1) & ~bitsPerBlock
    Math.max(bits, bitsPerBlock) / 8
  }

  /** The power-of-two byte allocation Comet asks parquet-rs to make for parquet-mr parity. */
  private[operator] def parquetMrBloomFilterBytes(
      ndv: Option[Long],
      fpp: Double,
      maxBytes: Int): Int = {
    ndv match {
      case None => maxBytes
      case Some(value) =>
        val requested = parquetMrRequestedBloomFilterBytes(value, fpp)
        val bounded = Math.max(MinBloomFilterBytes, Math.min(MaxBloomFilterBytes, requested))
        val allocated = java.lang.Integer.highestOneBit(bounded - 1) << 1
        Math.min(allocated, maxBytes)
    }
  }

  private val requireOnlyVettedParquetWriteProperties: TriggerRule = ctx =>
    ctx.properties
      .find { case (k, _) =>
        k.startsWith(ParquetWritePropertyPrefix) &&
        !vettedParquetWriteKeys.contains(k) &&
        !vettedParquetWritePrefixes.exists(k.startsWith)
      }
      .map { case (k, v) => s"$k=$v is not a vetted parquet write property" }

  private val requireNoUnsupportedParquetMrProperties: TriggerRule = ctx =>
    ctx.properties.keys
      .find(UnsupportedParquetMrKeys.contains)
      .map(k => s"$k is set (iceberg-java's Parquet writer reads it, the native writer does not)")

  // A HadoopFileIO hands the Parquet writer its own configuration, which carries the catalog's
  // `hadoop.*` overrides. Other FileIOs hand it a new Configuration, which loads the same site
  // files as the session configuration.
  private val requireNoUnsupportedParquetMrHadoopSettings: TriggerRule = ctx => {
    val confs = ctx.hadoopConf +: IcebergReflection.getFileIOHadoopConf(ctx.table).toSeq
    UnsupportedParquetMrKeys
      .find(key => confs.exists(_.get(key) != null))
      .map(k => s"Hadoop configuration sets $k (reaches iceberg-java's writer but not native)")
  }

  /**
   * The scheme the native writer picks its storage backend from. Must follow the same rule as
   * `scheme_of` in `native/core/src/execution/operators/iceberg_common.rs`: split on the first
   * `:`, not `://`, so a hostless `hdfs:/warehouse/t` (as Hadoop normalises `hdfs:///...`) is
   * read as `hdfs` rather than admitted as `file`. An empty prefix, or one containing `/` (a `:`
   * inside a path segment such as `/tmp/a:b`), means there is no scheme. The scheme is kept as
   * written, not lowercased: `storage_factory_for` matches it case-sensitively, so `S3://` must
   * be declined here rather than fail at execution.
   *
   * String-based rather than `java.net.URI` (`NativeConfig.lowerScheme`): `URI` throws on
   * characters an Iceberg location may carry unencoded, and its scheme grammar is not the
   * first-`:` split that `scheme_of` uses.
   */
  private[comet] def storageScheme(location: String): String = {
    val colon = location.indexOf(':')
    val prefix = if (colon > 0) location.substring(0, colon) else ""
    if (prefix.isEmpty || prefix.contains('/')) "file" else prefix
  }

  /**
   * True when `location` carries a non-empty authority (`scheme://host/...`). iceberg-rust's S3
   * and GCS backends take the bucket from the URL host and never from the path, so a hostless
   * `s3:/bucket/key` or `s3:///bucket/key` fails natively with a missing-bucket error.
   *
   * The write-side counterpart of `CometScanRule.hasOpenableAuthority`, which additionally admits
   * hostless S3-compliant aliases because the native reader promotes their bucket from the path.
   * The write gate admits no aliases, so it needs no such exception.
   */
  private[comet] def hasBucketAuthority(location: String): Boolean = {
    val rest = location.substring(location.indexOf(':') + 1)
    rest.startsWith("//") && rest.length > 2 && rest.charAt(2) != '/'
  }

  private val requireSupportedStorageScheme: TriggerRule = ctx =>
    IcebergReflection.getDataLocation(ctx.table) match {
      case None => Some("could not resolve the table data location")
      case Some(location) =>
        val scheme = storageScheme(location)
        if (!SupportedStorageSchemes.contains(scheme)) {
          Some(s"unsupported storage scheme: $scheme")
        } else if (!LocalStorageSchemes.contains(scheme) && !hasBucketAuthority(location)) {
          Some(s"$scheme data location has no bucket in its authority: $location")
        } else {
          None
        }
    }

  private def isS3Location(location: String): Boolean =
    S3StorageSchemes.contains(storageScheme(location))

  private def s3DataLocation(ctx: TriggerContext): Option[String] =
    IcebergReflection.getDataLocation(ctx.table).filter(isS3Location)

  private def unsupportedSettingsReason(namespace: String, keys: Seq[String]): Option[String] =
    keys match {
      case Seq() => None
      case Seq(key) => Some(s"unsupported $namespace setting: $key")
      case _ => Some(s"unsupported $namespace settings: ${keys.mkString(", ")}")
    }

  /**
   * Return effective Hadoop S3A keys that the native write path cannot reproduce. Global keys and
   * keys scoped to the data bucket affect this write; per-bucket settings for other buckets do
   * not. The data bucket must match in full: `fs.s3a.bucket.target.other.endpoint` is bucket
   * `target.other` and suffix `endpoint`, so it does not affect a write to `target`. Values are
   * deliberately never returned because this result is used in EXPLAIN fallback reasons and may
   * include credentials. `s3aCredentialAdapter` admits the credential settings, for a write that
   * takes its credentials from S3A's provider chain (see [[usesS3ACredentialAdapter]]).
   */
  private[comet] def unsupportedHadoopS3Settings(
      hadoopConf: Configuration,
      targetBucket: Option[String],
      s3aCredentialAdapter: Boolean = false): Seq[String] = {
    val encryption =
      if (hadoopS3EncryptionProperties(hadoopConf, targetBucket).isDefined) S3AEncryptionSuffixes
      else Set.empty[String]
    val credentials = if (s3aCredentialAdapter) S3ACredentialSuffixes else Set.empty[String]
    val translated = encryption ++ credentials
    val keys = hadoopConf
      .iterator()
      .asScala
      .map(_.getKey)
      .filter(_.startsWith("fs.s3a."))
      .filter(hasExplicitSource(hadoopConf, _))
      .toSeq

    keys.filter(key => isUnsupportedHadoopS3Key(hadoopConf, key, targetBucket, translated)).sorted
  }

  private def isCoreDefaultResource(source: String): Boolean =
    source == "core-default.xml" || source.endsWith("/core-default.xml")

  /**
   * Bucket of `fs.s3a.bucket.<bucket>.<suffix>` when `<suffix>` is exactly one known S3A suffix.
   * The longest suffix wins, so `endpoint.region` stays one property and a dotted bucket name is
   * what remains.
   */
  private def perBucketProperty(key: String): Option[(String, String)] = {
    if (!key.startsWith(FsS3aBucketPrefix)) {
      None
    } else {
      val rest = key.substring(FsS3aBucketPrefix.length)
      HadoopS3Properties.matchingSuffix(rest).flatMap { matched =>
        val bucket = rest.substring(0, rest.length - matched.length).stripSuffix(".")
        if (bucket.isEmpty) None else Some(bucket -> matched)
      }
    }
  }

  private def isUnsupportedHadoopS3Key(
      hadoopConf: Configuration,
      key: String,
      targetBucket: Option[String],
      translated: Set[String]): Boolean =
    if (key.startsWith(FsS3aBucketPrefix)) {
      perBucketProperty(key) match {
        case Some((bucket, suffix)) =>
          targetBucket.contains(bucket) &&
          !isAdmittedHadoopS3Setting(hadoopConf, key, suffix, translated)
        case None =>
          // Preserve fail-closed behavior for a property unknown to the runtime Hadoop version.
          targetBucket.exists(bucket => key.startsWith(s"$FsS3aBucketPrefix$bucket."))
      }
    } else {
      !isAdmittedHadoopS3Setting(hadoopConf, key, key.stripPrefix(FsS3aPrefix), translated)
    }

  private def isAdmittedHadoopS3Setting(
      hadoopConf: Configuration,
      key: String,
      suffix: String,
      translated: Set[String]): Boolean =
    SupportedHadoopS3Suffixes.contains(suffix) || IgnoredHadoopS3Suffixes.contains(suffix) ||
      translated.contains(suffix) ||
      (suffix == FileSystemImplSuffix &&
        Option(hadoopConf.getTrimmed(key)).contains(S3AFileSystemClass)) ||
      (suffix == MultipartUploadsSuffix && "true".equalsIgnoreCase(hadoopConf.getTrimmed(key)))

  // The data bucket's spellings of `suffixes`, then the global ones, in the order S3A reads them.
  private def s3aKeys(bucket: Option[String], suffixes: Seq[String]): Seq[String] =
    bucket.toSeq.flatMap(b => suffixes.map(s => s"$FsS3aBucketPrefix$b.$s")) ++
      suffixes.map(FsS3aPrefix + _)

  /**
   * The `s3.sse.*` properties for the server-side encryption iceberg-java's S3A writer applies to
   * `bucket`, or None when the native writer cannot apply the same encryption. Mirrors
   * `S3AUtils.buildEncryptionSecrets`: the first key that is set wins, the bucket's before the
   * global one and the newer name before the deprecated one, even when its value is empty, which
   * is how a bucket turns off a global setting. The value is trimmed and matched ignoring case,
   * and SSE-S3 with a key or SSE-C without one fails the S3A write, so it falls back here. A
   * Hadoop credential store can override the configured values, so they fall back while one is
   * configured. Client-side and dual-layer encryption have no iceberg-rust equivalent.
   */
  private[comet] def hadoopS3EncryptionProperties(
      hadoopConf: Configuration,
      bucket: Option[String]): Option[Map[String, String]] = {
    def setting(suffixes: Seq[String]): String =
      s3aKeys(bucket, suffixes).iterator
        .map(key => hadoopConf.get(key))
        .find(_ != null)
        .fold("")(_.trim)
    val algorithm = setting(S3AEncryptionAlgorithmSuffixes)
    val key = setting(S3AEncryptionKeySuffixes)
    val credentialStore =
      (HadoopCredentialStoreKey +: s3aKeys(bucket, Seq(S3ACredentialStoreSuffix))).exists(k =>
        Option(hadoopConf.getTrimmed(k)).exists(_.nonEmpty))
    if (credentialStore && (algorithm.nonEmpty || key.nonEmpty)) {
      None
    } else {
      algorithm.toUpperCase(Locale.ROOT) match {
        case "" => Some(Map.empty)
        case "AES256" if key.isEmpty => Some(Map("s3.sse.type" -> "s3"))
        case "SSE-KMS" =>
          Some(Map("s3.sse.type" -> "kms") ++ Some(key).filter(_.nonEmpty).map("s3.sse.key" -> _))
        case "SSE-C" if key.nonEmpty =>
          sseCustomerKeyMd5(key).map(md5 =>
            Map("s3.sse.type" -> "custom", "s3.sse.key" -> key, "s3.sse.md5" -> md5))
        case _ => None
      }
    }
  }

  // S3A sends SSE-C keys as base64 with the base64 MD5 of the decoded key, which iceberg-rust
  // takes as `s3.sse.md5` and does not derive itself.
  private def sseCustomerKeyMd5(key: String): Option[String] =
    scala.util.Try(java.util.Base64.getDecoder.decode(key)).toOption.map { raw =>
      java.util.Base64.getEncoder
        .encodeToString(java.security.MessageDigest.getInstance("MD5").digest(raw))
    }

  /**
   * True when the native write must take its S3 credentials from Hadoop's S3A provider chain:
   * `hadoopConf`, the table FileIO's effective Hadoop configuration, explicitly names the
   * providers for `bucket`, and `fileIOProperties` name no Comet credential provider of their
   * own. iceberg-java's S3A writer signs with exactly the listed providers, so the write proto
   * then names HadoopS3ACredentialProviderAdapter, which builds that list with S3A's own factory
   * on the executor. A FileIO without a Hadoop configuration, such as S3FileIO, has an empty one
   * here.
   */
  private[comet] def usesS3ACredentialAdapter(
      hadoopConf: Configuration,
      fileIOProperties: Map[String, String],
      bucket: Option[String]): Boolean = {
    // S3A copies a bucket's settings over the global ones before it builds the list.
    def setting(suffix: String): Option[Seq[String]] =
      s3aKeys(bucket, Seq(suffix))
        .find(hasExplicitSource(hadoopConf, _))
        .map(hadoopConf.getTrimmedStrings(_).toSeq.filter(_.nonEmpty))
    // Hadoop 3.4 resolves each name through this mapping, and fails the write on an entry that
    // is not one non-empty name and value.
    val mapping = setting(S3ACredentialProviderMappingSuffix)
      .getOrElse(Nil)
      .map(_.split("=", -1).map(_.trim))
    !fileIOProperties.get(CometS3CredentialProviderClassProperty).exists(_.trim.nonEmpty) &&
    mapping.forall(pair => pair.length == 2 && pair.forall(_.nonEmpty)) &&
    setting(S3ACredentialProviderSuffix).exists { names =>
      val mapped = mapping.map(pair => pair(0) -> pair(1)).toMap
      names.nonEmpty && !names.exists(name =>
        AnonymousS3ACredentialProviders.contains(name) ||
          mapped.get(name).exists(AnonymousS3ACredentialProviders.contains))
    }
  }

  // The keys S3A's providers read: their own `fs.s3a.*` settings, and the credential-store
  // settings Hadoop's password lookup reads for them. Other `hadoop.security.*` keys, such as an
  // LDAP bind password, stay out of the write proto.
  private val S3ACredentialAdapterPrefixes =
    Seq(FsS3aPrefix, "hadoop.security.credential.", "hadoop.security.credstore.")

  /**
   * The catalog properties that route a native write's S3 credentials through
   * HadoopS3ACredentialProviderAdapter. The adapter rebuilds the S3A configuration on the
   * executor from that JVM's `spark.hadoop.*` settings, so it also gets the FileIO's explicit
   * settings under `S3ACredentialAdapterPrefixes`, which carry the catalog's `hadoop.*`
   * overrides. The native side passes the whole property bag to the adapter. It hands
   * iceberg-rust none of the `fs.s3a.*` keys, and its S3 storage ignores the `hadoop.*` ones.
   */
  private[comet] def s3aCredentialAdapterProperties(
      hadoopConf: Configuration): Map[String, String] =
    hadoopConf
      .iterator()
      .asScala
      .map(_.getKey)
      .filter(key =>
        S3ACredentialAdapterPrefixes.exists(key.startsWith) && hasExplicitSource(hadoopConf, key))
      .flatMap(key => Option(hadoopConf.get(key)).map(key -> _))
      .toMap + (CometS3CredentialProviderClassProperty -> HadoopS3ACredentialAdapterClass)

  /** Return unsupported FileIO S3/client property names in deterministic order. */
  private[comet] def unsupportedS3FileIOProperties(properties: Map[String, String]): Seq[String] =
    unsupportedS3FileIOProperties(properties, IcebergAwsProperties)

  private[comet] def unsupportedS3FileIOProperties(
      properties: Map[String, String],
      icebergAwsProperties: IcebergAwsPropertyNames): Seq[String] = {
    val customCredentialProviderConfigured = properties
      .get(CometS3CredentialProviderClassProperty)
      .exists(_.trim.nonEmpty)

    properties.iterator
      .filter { case (key, _) => key.startsWith("s3.") || key.startsWith("client.") }
      .filter { case (key, value) =>
        val unsupportedName = !SupportedS3FileIOProperties.contains(key) &&
          !IgnoredS3FileIOProperties.contains(key) &&
          // PropertyUtil.propertyAsBoolean semantics, as S3FileIOProperties reads it.
          !(key == S3FileIODeleteEnabledProperty && java.lang.Boolean.parseBoolean(value))
        val unsupportedValue =
          key == "s3.sse.type" && !Option(value).exists(value =>
            SupportedS3SseTypes.contains(value.toLowerCase(Locale.ROOT)))
        unsupportedValue || (unsupportedName &&
          (!customCredentialProviderConfigured || icebergAwsProperties.contains(key)))
      }
      .map(_._1)
      .toSeq
      .sorted
  }

  private val requireSupportedHadoopS3Settings: TriggerRule = ctx =>
    s3DataLocation(ctx).flatMap { location =>
      val dataBucket = NativeConfig.bucketForUri(new java.net.URI(location), Set.empty)
      val s3aCredentials =
        usesS3ACredentialAdapter(ctx.s3HadoopConf, ctx.fileIOProperties, dataBucket)
      unsupportedSettingsReason(
        "Hadoop S3A",
        unsupportedHadoopS3Settings(ctx.s3HadoopConf, dataBucket, s3aCredentials))
    }

  private val requireSupportedS3FileIOProperties: TriggerRule = ctx =>
    s3DataLocation(ctx).flatMap { _ =>
      unsupportedSettingsReason("S3 FileIO", unsupportedS3FileIOProperties(ctx.fileIOProperties))
    }

  // HadoopFileIO takes its GCS configuration from `fs.gs.*`, which is not forwarded to the
  // native writer (only `fs.s3a.*` is bridged). Admit a gs:// data location only when the FileIO
  // Iceberg resolves for it is a GCSFileIO, whose `gcs.*` settings are forwarded.
  private val requireGcsFileIOForGcsDataLocation: TriggerRule = ctx =>
    IcebergReflection.getDataLocation(ctx.table).filter(storageScheme(_) == "gs").flatMap {
      location =>
        val resolved = IcebergReflection
          .getFileIO(ctx.table)
          .flatMap(io => IcebergReflection.resolveFileIOClass(io, location))
        gcsDataLocationRejection(location, resolved)
    }

  private[comet] def gcsDataLocationRejection(
      location: String,
      resolvedFileIO: Option[Class[_]]): Option[String] =
    resolvedFileIO match {
      case Some(cls)
          if IcebergReflection
            .classNameInHierarchy(cls, Set(IcebergReflection.ClassNames.GCS_FILE_IO)) =>
        None
      case Some(cls) =>
        Some(
          s"gs:// data location $location is written through ${cls.getName}, whose fs.gs.* " +
            "Hadoop configuration is not forwarded to the native writer")
      case None =>
        Some(s"could not resolve the FileIO for the gs:// data location $location")
    }

  // The commit-message assembly that runs on executors after iceberg-rust has already written
  // the task's data files is pure reflection over iceberg-java internals. Resolving the whole
  // surface up front turns an Iceberg release that moves any of it into a plan-time fallback
  // instead of a mid-write task failure.
  private val requireExecutorReflectionResolvable: TriggerRule = _ =>
    IcebergReflection.executorReflectionUnresolved

  // iceberg-java reads `io-impl` from catalog properties only, never from table properties, and a
  // catalog implementation can install its own FileIO without any property, so no property rule
  // can tell which FileIO a table uses. Gate on the instantiated FileIO's class hierarchy,
  // mirroring the scan side's `isCompatibleFileIO` -- except that the EncryptingFileIO family,
  // which the scan accepts (it reads ciphertext through iceberg-rust's own storage layer), is
  // rejected here: the native writer produces plaintext data files.
  private val requireRecognizedTableFileIO: TriggerRule = ctx =>
    IcebergReflection.getFileIO(ctx.table) match {
      case None => Some("could not resolve table.io() for FileIO compatibility checking")
      case Some(io)
          if IcebergReflection.classNameInHierarchy(
            io.getClass,
            IcebergReflection.COMPATIBLE_FILE_IO_CLASSES) =>
        None
      case Some(io) =>
        Some(s"table.io() is ${io.getClass.getName}, which the native write path would bypass")
    }

  private val PlaintextEncryptionManagerClass =
    "org.apache.iceberg.encryption.PlaintextEncryptionManager"

  // The `encryption.*` property rule above infers plaintext from property absence, but the
  // output-file contract is `table.encryption()`, which a custom TableOperations can install
  // independently of table properties. The native writer writes plaintext data files, so
  // anything but Iceberg's PlaintextEncryptionManager fails closed.
  private val requirePlaintextEncryptionManager: TriggerRule = ctx =>
    IcebergReflection.getEncryptionManager(ctx.table) match {
      case None => Some("could not resolve table.encryption() for plaintext checking")
      case Some(mgr) if mgr.getClass.getName == PlaintextEncryptionManagerClass => None
      case Some(mgr) =>
        Some(
          s"table.encryption() is ${mgr.getClass.getName} " +
            "(the native writer writes plaintext data files)")
    }

  private lazy val positiveIntParquetSizeKeys: Seq[String] = Seq(
    PropertyKeys.ParquetRowGroupSizeBytes,
    PropertyKeys.ParquetPageSizeBytes,
    PropertyKeys.ParquetPageRowLimit,
    PropertyKeys.ParquetDictSizeBytes)

  // iceberg-java reads these through `PropertyUtil.propertyAsInt` -- `Integer.parseInt`
  // semantics, so no trimming and no values past Int.MaxValue -- and hands the result to
  // parquet-mr, which rejects non-positive sizes and limits at write time. A value the JVM
  // writer would fail on (or parse differently) must not be silently normalised by the native
  // translation, so anything but a positive Java int falls back and fails on the stock path.
  private val requirePositiveIntParquetSizes: TriggerRule = ctx =>
    positiveIntParquetSizeKeys.flatMap { key =>
      ctx.properties.get(key).flatMap { raw =>
        scala.util.Try(java.lang.Integer.parseInt(raw)).toOption match {
          case None => Some(s"$key=$raw is not a Java int (iceberg-java fails at write time)")
          case Some(v) if v <= 0 =>
            Some(s"$key=$v is not positive (parquet-mr rejects it at write time)")
          case Some(_) => None
        }
      }
    }.headOption

  private lazy val requireRowGroupCheckMinRecordCountAtDefault: TriggerRule =
    requireIntPropertyAtDefault(
      PropertyKeys.ParquetRowGroupCheckMinRecordCount,
      PropertyKeys.ParquetRowGroupCheckMinRecordCountDefault,
      "row-group record-count cadence unsupported")

  private lazy val requireRowGroupCheckMaxRecordCountAtDefault: TriggerRule =
    requireIntPropertyAtDefault(
      PropertyKeys.ParquetRowGroupCheckMaxRecordCount,
      PropertyKeys.ParquetRowGroupCheckMaxRecordCountDefault,
      "row-group record-count cadence unsupported")

  private def requireIntPropertyAtDefault(
      key: String,
      default: Int,
      reason: String): TriggerRule = ctx =>
    ctx.properties.get(key).flatMap { raw =>
      // `Integer.parseInt` on the raw value, as iceberg-java does: ' 100' fails there.
      scala.util.Try(java.lang.Integer.parseInt(raw)).toOption match {
        case Some(v) if v != default => Some(s"$key=$v (default=$default; $reason)")
        case Some(_) => None
        case None => Some(s"$key=$raw is not an int ($reason)")
      }
    }

  override def convert(
      op: IcebergWriteExec,
      builder: Operator.Builder,
      childOp: Operator*): Option[OperatorOuterClass.Operator] = {
    val _ = builder // unused: the write builds its own operator below
    try {
      for {
        icebergWrite <- buildIcebergWriteProto(op)
        input <- buildInputScan(op, childOp)
        writeChild <- dropNonDataColumns(op, input)
      } yield OperatorOuterClass.Operator
        .newBuilder()
        .setPlanId(op.id)
        .addChildren(writeChild)
        .setIcebergWrite(icebergWrite)
        .build()
    } catch {
      case e: Exception =>
        withFallbackReason(op, s"Failed to convert Iceberg native write: ${e.getMessage}")
        None
    }
  }

  /**
   * Spark 4.x rewrites CoW DML (`ReplaceData`) into a row stream with extra columns -- column 0
   * carries the per-row operation code (`__row_operation`: 5=WRITE, 6=WRITE_WITH_METADATA) and
   * the tail of the row carries file/partition metadata (`_file`, `_spec_id`, `_partition`). The
   * JVM-side `IcebergWriteExec.runReplaceDataWriter` handles this row-by-row by applying
   * `dispatch.rowProjection` before invoking the writer.
   *
   * The native path forwards Arrow batches as-is to the iceberg-rust writer, which expects
   * exactly the Iceberg table's data columns. Without an explicit projection step we end up
   * giving it the wider row (e.g. 6 columns when the schema has 3) and
   * `decorate_batch_with_field_ids` rejects the batch.
   *
   * Plain writes already present only data columns, so no extra projection is needed. For 4.x
   * ReplaceData we splice a `Projection` proto between our `IcebergWrite` op and the input scan
   * (`buildInputScan`), selecting the upstream attributes whose names match the Iceberg schema's
   * columns. The JVM-side child stays at the original wide output, so its batches or shuffle
   * blocks still carry the wide rows the scan declares; the projection then strips them inside
   * the native runtime before the writer sees the data.
   */
  private def dropNonDataColumns(
      op: IcebergWriteExec,
      scan: OperatorOuterClass.Operator): Option[OperatorOuterClass.Operator] = {
    // Dropping the metadata columns is behaviour-identical to the JVM writer only while the
    // write schema has no row lineage columns: when it has, Iceberg's writer reads their values
    // from the metadata columns (`ExtractRowLineage`), which this projection discards.
    // `requireNoMetadataColumns` declines those writes.
    op.dispatch match {
      case ReplaceDataWrite(_) =>
      case _ => return Some(scan)
    }

    val sparkWrite = IcebergReflection.getOuterSparkWrite(op.batchWrite).getOrElse {
      withFallbackReason(op, "Could not unwrap outer SparkWrite for ReplaceData projection")
      return None
    }
    val writeSchema = IcebergReflection.getWriteSchemaFromSparkWrite(sparkWrite).getOrElse {
      withFallbackReason(
        op,
        "SparkWrite.writeSchema reflection failed for ReplaceData projection")
      return None
    }
    val dataFieldNames = IcebergReflection.getSchemaFieldNames(writeSchema).getOrElse {
      withFallbackReason(
        op,
        "Could not extract Iceberg schema column names for ReplaceData projection")
      return None
    }
    val upstreamOutput = op.child.output
    val missing = dataFieldNames.filterNot(name => upstreamOutput.exists(_.name == name))
    if (missing.nonEmpty) {
      withFallbackReason(
        op,
        s"ReplaceData projection: columns ${missing.mkString("[", ", ", "]")} not in upstream " +
          s"output ${upstreamOutput.map(_.name).mkString("[", ", ", "]")}")
      return None
    }
    val projectList = dataFieldNames.map(name => upstreamOutput.find(_.name == name).get)
    val protoExprs = projectList.map(attr => exprToProto(attr, upstreamOutput))
    if (!protoExprs.forall(_.isDefined)) {
      withFallbackReason(op, "Could not serialise ReplaceData projection attributes to proto")
      return None
    }
    val projection = OperatorOuterClass.Projection
      .newBuilder()
      .addAllProjectList(protoExprs.map(_.get).asJava)
      .build()
    Some(
      OperatorOuterClass.Operator
        .newBuilder()
        .setPlanId(op.id)
        .addChildren(scan)
        .setProjection(projection)
        .build())
  }

  /**
   * The leaf of the write's native plan, which reads its child. A child that reads a Comet
   * shuffle directly is a sink whose native operator is a `ShuffleScan` (see
   * `CometExchangeSink`). The write takes that `ShuffleScan` as its own leaf, so the shuffle
   * blocks are decoded inside the write's plan instead of in the JVM, and are not exported and
   * imported again over FFI. Any other child is read through an FFI `Scan` of its batches.
   */
  private def buildInputScan(
      op: IcebergWriteExec,
      childOp: Seq[Operator]): Option[OperatorOuterClass.Operator] =
    childOp match {
      case Seq(child) if child.hasShuffleScan => Some(child)
      case _ => buildFfiScan(op)
    }

  private def buildFfiScan(op: IcebergWriteExec): Option[OperatorOuterClass.Operator] = {
    val scan = NativeWriteUtils.buildFfiScan(op.child, op.id)
    if (scan.isEmpty) {
      withFallbackReason(
        op,
        "Cannot serialize upstream data types for Iceberg native write FFI scan")
    }
    scan
  }

  override def createExec(nativeOp: Operator, op: IcebergWriteExec): CometNativeExec = {
    val sparkWrite = IcebergReflection
      .getOuterSparkWrite(op.batchWrite)
      .getOrElse(
        throw new IllegalStateException(
          "Native Iceberg write conversion: could not unwrap outer SparkWrite from BatchWrite"))
    val table = IcebergReflection
      .getTableFromSparkWrite(sparkWrite)
      .getOrElse(
        throw new IllegalStateException(
          "Native Iceberg write conversion: SparkWrite.table reflection failed"))
    val outputSpecId = IcebergReflection
      .getOutputSpecIdFromSparkWrite(sparkWrite)
      .getOrElse(
        throw new IllegalStateException(
          "Native Iceberg write conversion: SparkWrite.outputSpecId reflection failed"))
    CometIcebergWriteExec(
      nativeOp,
      op,
      op.child,
      op.output,
      op.batchWrite,
      table.asInstanceOf[AnyRef],
      outputSpecId)
  }

  /**
   * Assemble the per-write `IcebergWrite` protobuf. All reflection calls are localised here so a
   * missing accessor surfaces as a `withFallbackReason` fall-back rather than a planning-time
   * crash.
   */
  private def buildIcebergWriteProto(
      op: IcebergWriteExec): Option[OperatorOuterClass.IcebergWrite] = {
    val sparkWrite = IcebergReflection.getOuterSparkWrite(op.batchWrite).getOrElse {
      withFallbackReason(op, "Could not unwrap outer SparkWrite from BatchWrite")
      return None
    }
    val table = IcebergReflection.getTableFromSparkWrite(sparkWrite).getOrElse {
      withFallbackReason(op, "Could not extract Iceberg Table from SparkWrite")
      return None
    }

    val properties = IcebergReflection
      .getTableProperties(table)
      .map(_.asScala.toMap)
      .getOrElse(Map.empty[String, String])
    val fileIOProperties =
      IcebergReflection.getFileIOProperties(table).getOrElse(Map.empty[String, String])

    // A brand-new table (CTAS/RTAS before its first commit) has no metadata file yet. The
    // native side only surfaces metadata_location in plan-debug output, so an empty string is
    // fine -- FileIO is initialised from data_location.
    val metadataLocation = IcebergReflection.getMetadataLocation(table).getOrElse("")
    val outputSpecId = IcebergReflection.getOutputSpecIdFromSparkWrite(sparkWrite).getOrElse {
      withFallbackReason(op, "SparkWrite.outputSpecId reflection failed")
      return None
    }
    val partitionSpec = IcebergReflection.getPartitionSpecById(table, outputSpecId).getOrElse {
      withFallbackReason(op, s"No partition spec found for id=$outputSpecId")
      return None
    }
    val partitionSpecJson = IcebergReflection.partitionSpecToJson(partitionSpec).getOrElse {
      withFallbackReason(op, "PartitionSpecParser.toJson failed")
      return None
    }
    val writeSchema = IcebergReflection.getWriteSchemaFromSparkWrite(sparkWrite).getOrElse {
      withFallbackReason(op, "SparkWrite.writeSchema reflection failed")
      return None
    }
    val icebergSchemaJson = IcebergReflection.schemaToJson(writeSchema).getOrElse {
      withFallbackReason(op, "SchemaParser.toJson failed")
      return None
    }
    // Iceberg's Spark `SparkWrite$WriterFactory` does NOT wire the table sort order into the
    // per-file writer factory for batch appends in Iceberg 1.8.1 / 1.10.0: it builds
    // `SparkFileWriterFactory` without `.dataSortOrder(...)`, so every
    // committed data file is stamped `sort_order_id = 0` (unsorted) even when the table itself has
    // a non-default sort order. We match that exactly. The
    // `SparkWriteConf.outputSortOrderId(writeRequirements)` resolver (explicit option / table order
    // when an ordering is required / unsorted) and the matching `.dataSortOrder(...)` wiring only
    // exist in Iceberg 1.11+; reflect the resolver when it is present so this stays correct if the
    // pinned runtime is bumped, otherwise default to 0.
    val sortOrderId =
      IcebergReflection.getOutputSortOrderIdFromSparkWrite(sparkWrite).getOrElse(0)
    val dataLocation = IcebergReflection.getDataLocation(table).getOrElse {
      withFallbackReason(op, "Table.locationProvider().newDataLocation reflection failed")
      return None
    }
    val operationId = IcebergReflection.getOperationIdFromSparkWrite(sparkWrite).getOrElse {
      withFallbackReason(op, "SparkWrite.queryId reflection failed")
      return None
    }
    val targetFileSize =
      IcebergReflection.getTargetFileSizeFromSparkWrite(sparkWrite).getOrElse {
        withFallbackReason(op, "SparkWrite.targetFileSize reflection failed")
        return None
      }
    val useFanoutWriter =
      IcebergReflection.getUseFanoutWriterFromSparkWrite(sparkWrite).getOrElse {
        withFallbackReason(op, "SparkWrite.useFanoutWriter reflection failed")
        return None
      }
    val specIsUnpartitioned = isUnpartitionedSpec(partitionSpec)
    val writerMode = IcebergWriteProtoTranslation.resolveWriterMode(
      specIsUnpartitioned = specIsUnpartitioned,
      useFanoutWriter = useFanoutWriter)

    val icebergVersion = IcebergReflection.icebergVersion()
    val createdBy = s"Apache Iceberg $icebergVersion (Comet)"
    // Iceberg's `RegistryBasedFileWriterFactory` merges resolved write properties (codec, level,
    // and other effective settings carried on `SparkWrite`) over the table's properties when
    // building the per-file writer. Mirror that merge here so per-write options (e.g.
    // `option("write-parquet-compression-codec", "gzip")`) survive into the native writer.
    val resolvedWriteProperties =
      IcebergReflection.getWritePropertiesFromSparkWrite(sparkWrite).getOrElse(Map.empty)
    val effectiveProperties =
      interpretedBloomFilterProperties(properties ++ resolvedWriteProperties)
    val parquetPathByIcebergColumnName =
      if (IcebergWriteProtoTranslation.hasEnabledBloomFilters(effectiveProperties)) {
        op.getTagValue(BloomPathsTag) match {
          case Some(prepared)
              if (prepared.schema eq writeSchema.asInstanceOf[AnyRef]) &&
                prepared.icebergVersion == icebergVersion &&
                prepared.properties == effectiveProperties =>
            prepared.paths
          case _ =>
            withFallbackReason(
              op,
              "Bloom paths were not validated for the current write settings")
            return None
        }
      } else {
        Map.empty[String, String]
      }
    val parquetSettings = IcebergWriteProtoTranslation.buildParquetSettings(
      effectiveProperties,
      createdBy,
      parquetPathByIcebergColumnName)

    // `FileIO.properties()` misses configuration a HadoopFileIO carries through the Hadoop
    // Configuration instead (fs.s3a.* credentials, custom endpoint, path-style access), which
    // the JVM writer would honour but iceberg-rust would never see. Mirror the scan side
    // (`CometScanRule`): extract the object-store options for the data location from the
    // effective FileIO Hadoop configuration, translate them to the s3.* keys iceberg-rust
    // consumes, and let FileIO/vended properties win on conflict. The FileIO configuration
    // includes SparkCatalog's catalog-specific `hadoop.*` overrides.
    val writeHadoopConf =
      effectiveHadoopConf(op, table, op.session.sessionState.newHadoopConf())
    val dataUri = new java.net.URI(dataLocation)
    // Promote the data bucket's per-bucket `fs.s3a.bucket.<b>.*` settings to global, mirroring the
    // scan path: iceberg-rust's pinned S3 parser reads only global `s3.*`. Only an S3-family data
    // location yields a bucket (None for a local/GCS/OSS write, which needs no promotion).
    //
    // No opt-in S3-compliant aliases here: `requireSupportedStorageScheme` already declined any
    // data location outside `SupportedStorageSchemes`, so an alias scheme never reaches this
    // point. Alias support is scan-only. Enabling it would also mean forwarding
    // `fs.comet.s3Compliant.schemes` into `catalogProperties`, as the scan does, since
    // `storage_factory_for` reads the opt-in from there.
    val dataBucket = NativeConfig.bucketForUri(dataUri, Set.empty)
    val hadoopDerivedProperties = CometIcebergNativeScan.hadoopToIcebergS3Properties(
      NativeConfig.extractObjectStoreOptions(writeHadoopConf, dataUri),
      dataBucket)
    // iceberg-java writes an S3 data location through S3A, so the native write also takes the
    // server-side encryption S3A applies and, when S3A's provider chain supplies the credentials,
    // the adapter that runs that chain. The gate checked both. A core-default.xml value, which
    // the gate does not count as a setting, is translated too, and declines here if it cannot be.
    val isS3DataLocation = isS3Location(dataLocation)
    val s3aEncryption =
      if (isS3DataLocation) {
        hadoopS3EncryptionProperties(writeHadoopConf, dataBucket).getOrElse(
          throw new IllegalStateException(
            "S3A server-side encryption that the native writer cannot apply"))
      } else {
        Map.empty[String, String]
      }
    val s3aCredentials =
      if (isS3DataLocation &&
        usesS3ACredentialAdapter(writeHadoopConf, fileIOProperties, dataBucket)) {
        s3aCredentialAdapterProperties(writeHadoopConf)
      } else {
        Map.empty[String, String]
      }
    // Only a FileIO with a Hadoop configuration yields S3A encryption, and such a FileIO ignores
    // its own `s3.sse.*` properties, so S3A's encryption replaces them.
    val storageFileIOProperties =
      if (s3aEncryption.isEmpty) fileIOProperties
      else fileIOProperties.filter { case (k, _) => !k.startsWith("s3.sse.") }
    val catalogProperties = hadoopDerivedProperties ++ storageFileIOProperties ++
      s3aEncryption ++ s3aCredentials + CometIcebergNativeScan.ioTimeoutProperty()

    val common = IcebergWriteProtoTranslation.buildCommon(
      catalogProperties = catalogProperties,
      metadataLocation = metadataLocation,
      icebergSchemaJson = icebergSchemaJson,
      partitionSpecJson = partitionSpecJson,
      sortOrderId = sortOrderId,
      dataLocation = dataLocation,
      operationId = operationId,
      targetFileSizeBytes = targetFileSize,
      writerMode = writerMode,
      parquetSettings = parquetSettings,
      catalogName = IcebergReflection.deriveCatalogName(table))

    Some(OperatorOuterClass.IcebergWrite.newBuilder().setCommon(common).build())
  }

  /**
   * `PartitionSpec.isUnpartitioned()` -- accessed reflectively because Iceberg is `test`-scoped
   * on the main source classpath.
   */
  private def isUnpartitionedSpec(spec: Any): Boolean =
    try {
      spec.getClass.getMethod("isUnpartitioned").invoke(spec).asInstanceOf[Boolean]
    } catch {
      case _: Exception =>
        val fields = spec.getClass
          .getMethod("fields")
          .invoke(spec)
          .asInstanceOf[java.util.List[_]]
        fields.isEmpty
    }
}
