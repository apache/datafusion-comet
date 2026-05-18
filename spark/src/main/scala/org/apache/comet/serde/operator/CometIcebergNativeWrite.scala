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

import scala.jdk.CollectionConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.comet.CometNativeExec
import org.apache.spark.sql.execution.datasources.v2.V2ExistingTableWriteExec

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.iceberg.IcebergReflection
import org.apache.comet.serde.{CometOperatorSerde, Incompatible, OperatorOuterClass, SupportLevel, Unsupported}
import org.apache.comet.serde.OperatorOuterClass.Operator

/**
 * Detection-and-fall-back scaffolding for Iceberg V2 writes. Matches the four V2 write operators
 * (`AppendDataExec`, `OverwriteByExpressionExec`, `OverwritePartitionsDynamicExec`,
 * `ReplaceDataExec`), inspects the underlying Iceberg `SparkWrite`, and falls back to Spark for
 * any table-property value or table-metadata state that the planned native writer cannot
 * faithfully reproduce. When every requirement is met the serde still returns `Incompatible` and
 * `convert` returns `None`; the native path will be filled in by a follow-up commit.
 */
object CometIcebergNativeWrite extends CometOperatorSerde[V2ExistingTableWriteExec] with Logging {

  override def enabledConfig: Option[ConfigEntry[Boolean]] =
    Some(CometConf.COMET_ICEBERG_NATIVE_WRITE_ENABLED)

  override def requiresNativeChildren: Boolean = true

  /**
   * Property names pulled from Iceberg's own `TableProperties` class via reflection. Centralising
   * them here means the suite asserting on these triggers references the same strings the rule
   * checks, instead of duplicating literals that could drift from Iceberg's canonical names.
   *
   * Resolved lazily -- first access happens inside `getSupportLevel` after the write has already
   * been confirmed to be an Iceberg `SparkWrite`, so we know Iceberg is on the classpath.
   */
  object PropertyKeys {
    lazy val DefaultFileFormat: String =
      IcebergReflection.tablePropertyConstant("DEFAULT_FILE_FORMAT")
    lazy val ObjectStoreEnabled: String =
      IcebergReflection.tablePropertyConstant("OBJECT_STORE_ENABLED")
    lazy val WriteLocationProviderImpl: String =
      IcebergReflection.tablePropertyConstant("WRITE_LOCATION_PROVIDER_IMPL")
    lazy val DefaultWriteMetricsMode: String =
      IcebergReflection.tablePropertyConstant("DEFAULT_WRITE_METRICS_MODE")
    lazy val MetricsModeColumnPrefix: String =
      IcebergReflection.tablePropertyConstant("METRICS_MODE_COLUMN_CONF_PREFIX")
    lazy val ParquetBloomFilterMaxBytes: String =
      IcebergReflection.tablePropertyConstant("PARQUET_BLOOM_FILTER_MAX_BYTES")
    lazy val ParquetRowGroupCheckMinRecordCount: String =
      IcebergReflection.tablePropertyConstant("PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT")
    lazy val ParquetRowGroupCheckMaxRecordCount: String =
      IcebergReflection.tablePropertyConstant("PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT")
    lazy val MetricsMaxInferredColumnDefaults: String =
      IcebergReflection.tablePropertyConstant("METRICS_MAX_INFERRED_COLUMN_DEFAULTS")

    /** Default value Iceberg applies when `write.format.default` is unset. */
    lazy val DefaultFileFormatValue: String =
      IcebergReflection.tablePropertyConstant("DEFAULT_FILE_FORMAT_DEFAULT")

    /**
     * Default value Iceberg applies when `write.metadata.metrics.max-inferred-column-defaults` is
     * unset.
     */
    lazy val MetricsMaxInferredColumnDefaultsValue: Int =
      IcebergReflection.tablePropertyIntConstant("METRICS_MAX_INFERRED_COLUMN_DEFAULTS_DEFAULT")

    lazy val BloomFilterColumnEnabledPrefix: String =
      IcebergReflection.tablePropertyConstant("PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX")
  }

  // V3 is rejected because iceberg-rust's writer can't yet emit the features V3 introduces.
  // The headline feature is row lineage: Iceberg PR apache/iceberg#12593 ("Enable row lineage
  // for all v3 tables") landed before 1.10.0, so every V3 table written by Iceberg today
  // requires `_row_id` / `_last_updated_sequence_number` columns and per-snapshot `firstRowId`
  // stamping that iceberg-rust does not produce. V3 also brings variant types, default column
  // values, deletion vectors, and revised manifest/sequence-number rules, none of which the
  // Rust writer handles. A short 1.8.x window allowed opting out of row lineage on V3 tables;
  // we treat that as out of scope here because the other V3 changes still rule out the path.
  private val MinUnsupportedFormatVersion = 3

  /**
   * One check that an Iceberg write must pass to be eligible for native execution. Returns
   * `Some(reason)` when the write should fall back to Spark and `None` when the check passes.
   *
   * Both the table and its full property map are passed in so requirements can read either (e.g.
   * format-version comes from `TableMetadata`, codec choice comes from properties).
   */
  private type Requirement = (Any, Map[String, String]) => Option[String]

  private lazy val writeRequirements: Seq[Requirement] = Seq(
    requireFormatParquet,
    requirePropertyAbsentOrNotTrue(
      PropertyKeys.ObjectStoreEnabled,
      "iceberg-rust has no hashed-prefix object-storage location generator"),
    requirePropertyAbsent(
      PropertyKeys.WriteLocationProviderImpl,
      "custom location providers are not supported"),
    requireFormatVersionAtMostTwo,
    requireNoEncryptionPrefix,
    requireMetricsModeIsNotCounts,
    requireNoPerColumnCountsMetricsMode,
    requirePropertyAbsent(
      PropertyKeys.ParquetBloomFilterMaxBytes,
      "parquet-rs has no global bloom-filter byte cap; explicit value would diverge"),
    requireNoBloomFilterColumnsEnabled,
    requirePropertyAbsent(
      PropertyKeys.ParquetRowGroupCheckMinRecordCount,
      "parquet-rs uses a different row-group sizing cadence; explicit value would diverge"),
    requirePropertyAbsent(
      PropertyKeys.ParquetRowGroupCheckMaxRecordCount,
      "parquet-rs uses a different row-group sizing cadence; explicit value would diverge"),
    requireSchemaWithinMaxInferredColumnDefaults)

  override def getSupportLevel(op: V2ExistingTableWriteExec): SupportLevel = {
    val write = op.write
    if (!IcebergReflection.isIcebergSparkWrite(write)) {
      return Unsupported(Some("Not an Iceberg V2 write"))
    }

    val table = IcebergReflection.getTableFromSparkWrite(write) match {
      case Some(t) => t
      case None =>
        return Unsupported(Some("Could not extract Iceberg Table from SparkWrite"))
    }

    val properties = IcebergReflection
      .getTableProperties(table)
      .map(_.asScala.toMap)
      .getOrElse(Map.empty[String, String])

    val firstFailure = writeRequirements.iterator.flatMap(_(table, properties)).take(1).toSeq
    firstFailure.headOption match {
      case Some(reason) => Unsupported(Some(reason))
      case None =>
        Incompatible(
          Some(
            "Iceberg native write conversion is not yet implemented; " +
              "operator will fall back to Spark"))
    }
  }

  // -- Requirement implementations ------------------------------------------

  private val requireFormatParquet: Requirement = (_, props) => {
    val key = PropertyKeys.DefaultFileFormat
    val default = PropertyKeys.DefaultFileFormatValue
    val v = props.getOrElse(key, default).toLowerCase(Locale.ROOT)
    if (v == "parquet") None else Some(s"$key=$v (only parquet is supported)")
  }

  private def requirePropertyAbsentOrNotTrue(key: String, reason: String): Requirement =
    (_, props) =>
      if (props.get(key).exists(_.equalsIgnoreCase("true"))) Some(s"$key=true ($reason)")
      else None

  private def requirePropertyAbsent(key: String, reason: String): Requirement =
    (_, props) => if (props.contains(key)) Some(s"$key is set ($reason)") else None

  private val requireFormatVersionAtMostTwo: Requirement = (table, _) =>
    IcebergReflection.getFormatVersion(table) match {
      case Some(v) if v >= MinUnsupportedFormatVersion =>
        Some(
          s"format-version=$v introduces features the Rust writer cannot emit " +
            "(row lineage, variant types, deletion vectors, revised manifest rules)")
      case _ => None
    }

  private val requireNoEncryptionPrefix: Requirement = (_, props) =>
    props.keys.find(_.startsWith(EncryptionPropertyPrefix)).map { key =>
      s"$key is set (Parquet modular encryption not supported)"
    }

  private val EncryptionPropertyPrefix = "encryption."

  private val CountsMetricsMode = "counts"

  private val requireMetricsModeIsNotCounts: Requirement = (_, props) => {
    val key = PropertyKeys.DefaultWriteMetricsMode
    props
      .get(key)
      .filter(_.toLowerCase(Locale.ROOT).contains(CountsMetricsMode))
      .map(v =>
        s"$key=$v mentions '$CountsMetricsMode' (Parquet has no counts-without-bounds mode)")
  }

  private val requireNoPerColumnCountsMetricsMode: Requirement = (_, props) => {
    val prefix = PropertyKeys.MetricsModeColumnPrefix
    props
      .find { case (k, v) =>
        k.startsWith(prefix) && v.toLowerCase(Locale.ROOT) == CountsMetricsMode
      }
      .map { case (k, _) => s"$k=$CountsMetricsMode (counts-only mode not supported)" }
  }

  /**
   * Falls back whenever any column has `write.parquet.bloom-filter-enabled.column.<c>=true`.
   * parquet-rs has no equivalent of iceberg-java's 1 MiB total bloom-filter byte cap, so any
   * non-trivial NDV column would produce bloom files larger than iceberg-java would write.
   * Iceberg-java leaves bloom filters disabled by default, so the default path is unaffected.
   */
  private val requireNoBloomFilterColumnsEnabled: Requirement = (_, props) => {
    val prefix = PropertyKeys.BloomFilterColumnEnabledPrefix
    props
      .find { case (k, v) => k.startsWith(prefix) && v.equalsIgnoreCase("true") }
      .map { case (k, _) =>
        s"$k=true (iceberg-rust cannot enforce iceberg-java's bloom-filter byte cap)"
      }
  }

  /**
   * Falls back when the schema has more projected field IDs than
   * `write.metadata.metrics.max-inferred-column-defaults` (default 100). Iceberg-java applies
   * `MetricsModes.None` to columns beyond that index when no per-column metrics mode is set;
   * mimicking that on the Rust side would require per-column stats wiring we don't yet have.
   */
  private val requireSchemaWithinMaxInferredColumnDefaults: Requirement = (table, props) => {
    val key = PropertyKeys.MetricsMaxInferredColumnDefaults
    val limit = parseIntProperty(props, key, PropertyKeys.MetricsMaxInferredColumnDefaultsValue)
    for {
      schema <- IcebergReflection.getSchema(table)
      count <- IcebergReflection.getProjectedFieldIdCount(schema)
      reason <- {
        if (count > limit) {
          Some(
            s"schema has $count projected field IDs which exceeds $key=$limit " +
              "(iceberg-java applies MetricsModes.None to columns beyond this index)")
        } else {
          None
        }
      }
    } yield reason
  }

  private def parseIntProperty(props: Map[String, String], key: String, default: Int): Int =
    props.get(key).flatMap(s => scala.util.Try(s.trim.toInt).toOption).getOrElse(default)

  override def convert(
      op: V2ExistingTableWriteExec,
      builder: Operator.Builder,
      childOp: Operator*): Option[OperatorOuterClass.Operator] = {
    withInfo(op, "Iceberg native write conversion is not yet implemented")
    None
  }

  override def createExec(nativeOp: Operator, op: V2ExistingTableWriteExec): CometNativeExec =
    throw new UnsupportedOperationException(
      "CometIcebergNativeWrite.createExec must not be called: convert returns None in v1")

}
