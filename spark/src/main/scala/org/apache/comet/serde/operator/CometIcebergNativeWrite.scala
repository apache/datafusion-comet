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
import org.apache.spark.sql.comet.{CometIcebergWriteExec, CometNativeExec, IcebergWriteExec}

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.iceberg.{IcebergReflection, PositionDeltaWrite, ReplaceDataWrite}
import org.apache.comet.objectstore.NativeConfig
import org.apache.comet.serde.{CometOperatorSerde, Compatible, OperatorOuterClass, SupportLevel, Unsupported}
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde.exprToProto

object CometIcebergNativeWrite extends CometOperatorSerde[IcebergWriteExec] {

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
    val FileIOImpl: String = "io-impl"
  }

  private val EncryptionPropertyPrefix = "encryption."
  // `uuid` plus the v3-only types. Iceberg plans `variant` as Spark's VariantType and `unknown`
  // as NullType, neither of which the native writer handles; Spark cannot plan a write to
  // `timestamp_ns`, `geometry` or `geography` today, so those are declined in case it learns to.
  private val UnsupportedWriteTypeIds: Set[String] =
    Set("UUID", "VARIANT", "UNKNOWN", "TIMESTAMP_NANO", "GEOMETRY", "GEOGRAPHY")
  // `oss` is deliberately absent: iceberg-rust has an OSS backend, but Comet does not forward
  // `oss.*` catalog properties to it and no functional test covers the path, so an OSS write
  // could silently drop endpoint/credential configuration. Fail closed until it is covered.
  // `gs` is additionally gated on the resolved FileIO (`requireGcsFileIOForGcsDataLocation`).
  private val SupportedStorageSchemes: Set[String] =
    Set("file", "memory", "s3", "s3a", "gs")
  // Supported schemes whose native backend is local and needs no host. Every other supported
  // scheme reads its bucket from the URL host (`requireSupportedStorageScheme`).
  private val LocalStorageSchemes: Set[String] = Set("file", "memory")
  private val MaxSupportedFormatVersion = 3
  // The Iceberg spec reserves field ids above `Integer.MAX_VALUE - 200` for metadata columns.
  private val MaxDataFieldId = Int.MaxValue - 200
  private val ParquetWritePropertyPrefix = "write.parquet."
  private val ParquetMrPropertyPrefix = "parquet."
  private val CometS3CredentialProviderClassProperty =
    "s3.comet.credential.provider.class"

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

  private val SupportedHadoopS3Keys: Set[String] =
    SupportedHadoopS3Suffixes.map("fs.s3a." + _)

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
  // retain the supported suffixes and let unknown spellings take the conservative path below.
  private lazy val HadoopS3Properties: HadoopS3PropertyNames =
    try {
      val suffixes = allStaticStringConstants(
        IcebergReflection.loadClass("org.apache.hadoop.fs.s3a.Constants"))
        .filter(key => key.startsWith(FsS3aPrefix) && !key.startsWith(FsS3aBucketPrefix))
        .map(_.stripPrefix(FsS3aPrefix))
        .filter(_.nonEmpty)
        .toSet ++ SupportedHadoopS3Suffixes
      val (prefixes, exact) = suffixes.partition(_.endsWith("."))
      HadoopS3PropertyNames(exact, prefixes.toSeq.sorted)
    } catch {
      case _: ClassNotFoundException =>
        HadoopS3PropertyNames(SupportedHadoopS3Suffixes, Seq.empty)
      case _: LinkageError => HadoopS3PropertyNames(SupportedHadoopS3Suffixes, Seq.empty)
      case NonFatal(_) => HadoopS3PropertyNames(SupportedHadoopS3Suffixes, Seq.empty)
    }

  // Spark seeds these Hadoop S3A compatibility/read settings into every session as if they came
  // from spark.hadoop.*. They do not alter an Iceberg data-file write request, so they must not
  // make every otherwise-clean S3 write ineligible. This also permits explicit overrides, which
  // are harmless on the write path for the same reason.
  private val IgnoredHadoopS3Keys: Set[String] = Set(
    "fs.s3a.downgrade.syncable.exceptions",
    "fs.s3a.vectored.read.max.merged.size",
    "fs.s3a.vectored.read.min.seek.size")

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

  // Hadoop-side `parquet.*` keys that iceberg-java's writer never consumes, so seeing them
  // in the session Hadoop configuration does not indicate the native writer would diverge.
  // `parquet.hadoop.vectored.io.enabled` is a reader-side vectored-IO knob declared by
  // parquet-hadoop as `ParquetInputFormat.HADOOP_VECTORED_IO_ENABLED` (default `true` in
  // parquet-hadoop 1.16+) and only consulted by parquet-mr's Hadoop reader path. Keep it
  // out of the writer-compatibility gate so that environments which seed it into the
  // session Hadoop configuration do not silently disable native Iceberg writes.
  private val IgnoredHadoopParquetConfKeys: Set[String] = Set(
    "parquet.hadoop.vectored.io.enabled")

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
    PropertyKeys.ParquetVariantBufferSize)

  private lazy val vettedParquetWritePrefixes: Seq[String] =
    Seq(PropertyKeys.BloomFilterColumnEnabledPrefix)

  override def getSupportLevel(op: IcebergWriteExec): SupportLevel =
    try {
      checkTriggers(op) match {
        case Some(reason) => Unsupported(Some(reason))
        case None => Compatible(None)
      }
    } catch {
      case NonFatal(e) =>
        Unsupported(Some(s"Iceberg native write detection failed: ${e.getMessage}"))
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

    val context = TriggerContext(
      table,
      tableProperties ++ writeProperties,
      sparkWrite,
      op.session.sessionState.newHadoopConf(),
      effectiveHadoopConf(op, table))
    triggers.iterator.map(rule => rule(context)).collectFirst { case Some(reason) => reason }
  }

  private case class TriggerContext(
      table: Any,
      properties: Map[String, String],
      sparkWrite: Any,
      hadoopConf: Configuration,
      s3HadoopConf: Configuration)

  private type TriggerRule = TriggerContext => Option[String]

  private def effectiveHadoopConf(op: IcebergWriteExec, table: Any): Configuration = {
    val sessionConf = op.session.sessionState.newHadoopConf()
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
    requireNoFloatingPointPartitionField,
    requireNoVoidFieldWithDroppedSource,
    requireNoEncryptionPrefix,
    requireNoBloomFilterColumnsEnabled,
    requireRowGroupCheckMinRecordCountAtDefault,
    requireRowGroupCheckMaxRecordCountAtDefault,
    requireParquetPageVersionDefault,
    requireShredVariantsDisabled,
    requireNativeSupportedCompressionLevel,
    requireOnlyVettedParquetWriteProperties,
    requirePropertyAbsent(
      PropertyKeys.ParquetEnableDictionary,
      "dictionary override unsupported"),
    requireNoUnvettedParquetMrProperties,
    requirePropertyAbsent(PropertyKeys.FileIOImpl, "custom FileIO unsupported"),
    requireRecognizedTableFileIO,
    requirePlaintextEncryptionManager,
    requirePositiveIntParquetSizes,
    requireNoParquetHadoopConfOverrides,
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
          case (name, id) if id > MaxDataFieldId =>
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
          .map { case (name, typeId) =>
            s"column $name has Iceberg type ${typeId.toLowerCase(Locale.ROOT)}, " +
              "which the native writer cannot reproduce"
          }
    }

  // iceberg-rust holds a float partition value as an `OrderedFloat`, whose equality treats -0.0
  // and 0.0 as one value, and its fanout and clustered writers group rows by that equality.
  // iceberg-java keeps the two apart, so the native writer would file both under whichever
  // arrived first, and a read that prunes on the other value would lose rows (#6138). Remove this
  // rule once the iceberg-rust pin carries a fix for apache/iceberg-rust#3325; #5643 tracks it.
  private val requireNoFloatingPointPartitionField: TriggerRule = ctx =>
    IcebergReflection
      .getOutputSpecIdFromSparkWrite(ctx.sparkWrite)
      .flatMap(IcebergReflection.getPartitionSpecById(ctx.table, _)) match {
      case None => Some("could not resolve the output partition spec for type checking")
      case Some(spec) =>
        try {
          IcebergReflection.floatingPointPartitionFields(spec).headOption.map {
            case (name, typeName) =>
              s"partition field $name has Iceberg type $typeName, and the native writer does " +
                "not keep -0.0 and 0.0 partitions apart"
          }
        } catch {
          case e: Exception =>
            Some(s"could not inspect the output partition spec: ${e.getMessage}")
        }
    }

  // A format-version-1 spec keeps a dropped partition field as a `void` transform, and its source
  // column can be dropped afterwards. iceberg-java cannot write through a spec that mixes such a
  // field with a live one, and the native writer fails resolving the spec's partition type, so
  // decline and let the write fail the way iceberg-java fails it. An all-`void` spec stays
  // eligible, since the native writer writes it unpartitioned.
  // https://github.com/apache/datafusion-comet/issues/6141
  private val requireNoVoidFieldWithDroppedSource: TriggerRule = ctx =>
    IcebergReflection
      .getOutputSpecIdFromSparkWrite(ctx.sparkWrite)
      .flatMap(IcebergReflection.getPartitionSpecById(ctx.table, _)) match {
      case None => Some("could not resolve the output partition spec for void field checking")
      case Some(spec) =>
        try {
          IcebergReflection.voidFieldsWithDroppedSource(spec).headOption.map { name =>
            s"partition field $name is a void transform whose source column was dropped, " +
              "beside a live partition field"
          }
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

  private val requireNoBloomFilterColumnsEnabled: TriggerRule = ctx => {
    val prefix = PropertyKeys.BloomFilterColumnEnabledPrefix
    ctx.properties
      .find { case (k, v) => k.startsWith(prefix) && v.equalsIgnoreCase("true") }
      .map { case (k, _) => s"$k=true: bloom filters unsupported" }
  }

  private val requireParquetPageVersionDefault: TriggerRule = ctx => {
    val key = PropertyKeys.ParquetPageVersion
    ctx.properties
      .get(key)
      .filter(_.trim.toLowerCase(Locale.ROOT) != PropertyKeys.ParquetPageVersionDefault)
      .map(v => s"$key=$v unsupported")
  }

  private val requireShredVariantsDisabled: TriggerRule = ctx => {
    val key = PropertyKeys.ParquetShredVariants
    ctx.properties
      .get(key)
      .filter(_.equalsIgnoreCase("true"))
      .map(_ => s"$key=true (variant shredding changes the parquet schema)")
  }

  // iceberg-java never validates the level -- the raw string flows into a codec-specific
  // parquet-mr writer property -- while parquet-rs enforces per-codec ranges when the native
  // writer is built. A level the JVM writer accepts (zstd 0, a negative zstd fast level) must
  // not become a mid-task native failure, and a non-integer must fail on the stock path; both
  // fall back. Range logic lives beside the codec resolution in IcebergWriteProtoTranslation.
  private val requireNativeSupportedCompressionLevel: TriggerRule = ctx =>
    IcebergWriteProtoTranslation.compressionLevelRejection(ctx.properties)

  private val requireOnlyVettedParquetWriteProperties: TriggerRule = ctx =>
    ctx.properties
      .find { case (k, _) =>
        k.startsWith(ParquetWritePropertyPrefix) &&
        !vettedParquetWriteKeys.contains(k) &&
        !vettedParquetWritePrefixes.exists(k.startsWith)
      }
      .map { case (k, v) => s"$k=$v is not a vetted parquet write property" }

  private val requireNoUnvettedParquetMrProperties: TriggerRule = ctx =>
    ctx.properties.keys
      .find(k =>
        k.startsWith(ParquetMrPropertyPrefix) && k != PropertyKeys.ParquetEnableDictionary)
      .map(k => s"$k is set (parquet-mr properties are forwarded verbatim by iceberg-java)")

  private val requireNoParquetHadoopConfOverrides: TriggerRule = ctx =>
    ctx.hadoopConf.asScala
      .map(_.getKey)
      .filter(_.startsWith(ParquetMrPropertyPrefix))
      .find(k => !IgnoredHadoopParquetConfKeys.contains(k))
      .map(k => s"Hadoop configuration sets $k (reaches iceberg-java's writer but not native)")

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

  private def s3DataLocation(ctx: TriggerContext): Option[String] =
    IcebergReflection
      .getDataLocation(ctx.table)
      .filter(location => Set("s3", "s3a").contains(storageScheme(location)))

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
   * include credentials.
   */
  private[comet] def unsupportedHadoopS3Settings(
      hadoopConf: Configuration,
      targetBucket: Option[String]): Seq[String] = {
    val keys = hadoopConf
      .iterator()
      .asScala
      .map(_.getKey)
      .filter(_.startsWith("fs.s3a."))
      .filterNot(IgnoredHadoopS3Keys.contains)
      // Hadoop's iterator includes the many fs.s3a.* defaults loaded from core-default.xml.
      // Those are library implementation defaults, not settings selected by the user, and
      // treating them as explicit would reject every ordinary S3 write. Preserve settings from
      // site XML and programmatic/Spark sources; exclude a key only when every recorded source is
      // Hadoop's built-in core-default.xml resource. A user resource merely named
      // `tenant-default.xml` is still explicit configuration.
      .filter { key =>
        Option(hadoopConf.getPropertySources(key))
          .forall(sources => sources.isEmpty || !sources.forall(isCoreDefaultResource))
      }
      .toSeq

    keys.filter(key => isUnsupportedHadoopS3Key(key, targetBucket)).sorted
  }

  private def isCoreDefaultResource(source: String): Boolean =
    source == "core-default.xml" || source.endsWith("/core-default.xml")

  /**
   * Bucket of `fs.s3a.bucket.<bucket>.<suffix>` when `<suffix>` is exactly one supported S3A
   * suffix. The longest suffix wins, so `endpoint.region` stays one property and a dotted bucket
   * name is what remains.
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

  private def isUnsupportedHadoopS3Key(key: String, targetBucket: Option[String]): Boolean =
    if (key.startsWith(FsS3aBucketPrefix)) {
      perBucketProperty(key) match {
        case Some((bucket, suffix)) =>
          targetBucket.contains(bucket) && !SupportedHadoopS3Suffixes.contains(suffix)
        case None =>
          // Preserve fail-closed behavior for a property unknown to the runtime Hadoop version.
          targetBucket.exists(bucket => key.startsWith(s"$FsS3aBucketPrefix$bucket."))
      }
    } else {
      !SupportedHadoopS3Keys.contains(key)
    }

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
        val unsupportedName = !SupportedS3FileIOProperties.contains(key)
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
      unsupportedSettingsReason(
        "Hadoop S3A",
        unsupportedHadoopS3Settings(ctx.s3HadoopConf, dataBucket))
    }

  private val requireSupportedS3FileIOProperties: TriggerRule = ctx =>
    s3DataLocation(ctx).flatMap { _ =>
      val properties = IcebergReflection.getFileIOProperties(ctx.table).getOrElse(Map.empty)
      unsupportedSettingsReason("S3 FileIO", unsupportedS3FileIOProperties(properties))
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

  // The `io-impl` property rule above only sees FileIO configured through table/write
  // properties; a catalog-level `io-impl` (or a catalog implementation installing its own
  // FileIO) leaves the properties clean while `table.io()` is still custom. Gate on the
  // instantiated FileIO's class hierarchy, mirroring the scan side's `isCompatibleFileIO` --
  // except that the EncryptingFileIO family, which the scan accepts (it reads ciphertext
  // through iceberg-rust's own storage layer), is rejected here: the native writer produces
  // plaintext data files.
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
      scala.util.Try(raw.trim.toInt).toOption match {
        case Some(v) if v != default => Some(s"$key=$v (default=$default; $reason)")
        case Some(_) => None
        case None => Some(s"$key=$raw is not an int ($reason)")
      }
    }

  override def convert(
      op: IcebergWriteExec,
      builder: Operator.Builder,
      childOp: Operator*): Option[OperatorOuterClass.Operator] = {
    val _ = (builder, childOp) // unused: we synthesise our own FFI scan child below
    try {
      for {
        icebergWrite <- buildIcebergWriteProto(op)
        ffiScan <- buildFfiScan(op)
        writeChild <- dropNonDataColumns(op, ffiScan)
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
   * ReplaceData we splice a `Projection` proto between our `IcebergWrite` op and the FFI `Scan`,
   * selecting the upstream attributes whose names match the Iceberg schema's columns. The
   * JVM-side child stays at the original wide output, so its `executeColumnar()` still emits the
   * wide batches the FFI scan declares; the projection then strips them inside the native runtime
   * before the writer sees the data.
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
      op.child,
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
    // per-file writer factory for batch appends in any Iceberg version Comet targets (1.5.2 /
    // 1.8.1 / 1.10.0): it builds `SparkFileWriterFactory` without `.dataSortOrder(...)`, so every
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

    val createdBy = s"Apache Iceberg ${IcebergReflection.icebergVersion()} (Comet)"
    // Iceberg's `RegistryBasedFileWriterFactory` merges resolved write properties (codec, level,
    // and other effective settings carried on `SparkWrite`) over the table's properties when
    // building the per-file writer. Mirror that merge here so per-write options (e.g.
    // `option("write-parquet-compression-codec", "gzip")`) survive into the native writer.
    val resolvedWriteProperties =
      IcebergReflection.getWritePropertiesFromSparkWrite(sparkWrite).getOrElse(Map.empty)
    val effectiveProperties = properties ++ resolvedWriteProperties
    val parquetSettings =
      IcebergWriteProtoTranslation.buildParquetSettings(effectiveProperties, createdBy)

    // `FileIO.properties()` misses configuration a HadoopFileIO carries through the Hadoop
    // Configuration instead (fs.s3a.* credentials, custom endpoint, path-style access), which
    // the JVM writer would honour but iceberg-rust would never see. Mirror the scan side
    // (`CometScanRule`): extract the object-store options for the data location from the
    // effective FileIO Hadoop configuration, translate them to the s3.* keys iceberg-rust
    // consumes, and let FileIO/vended properties win on conflict. The FileIO configuration
    // includes SparkCatalog's catalog-specific `hadoop.*` overrides.
    val writeHadoopConf = effectiveHadoopConf(op, table)
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
    val catalogProperties =
      hadoopDerivedProperties ++ fileIOProperties + CometIcebergNativeScan.ioTimeoutProperty()

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
