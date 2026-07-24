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

import java.math.BigDecimal
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import java.util.{Base64, UUID}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.comet.{CometBatchScanExec, CometNativeExec}
import org.apache.spark.sql.comet.shims.ShimDataSourceRDDPartition
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceRDD, DataSourceRDDPartition}
import org.apache.spark.sql.types._

import com.google.protobuf.ByteString

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.iceberg.{CometIcebergNativeScanMetadata, IcebergReflection}
import org.apache.comet.serde.{CometOperatorSerde, OperatorOuterClass}
import org.apache.comet.serde.OperatorOuterClass.{Operator, SparkStructField}
import org.apache.comet.serde.QueryPlanSerde.serializeDataType

object CometIcebergNativeScan extends CometOperatorSerde[CometBatchScanExec] with Logging {

  override def enabledConfig: Option[ConfigEntry[Boolean]] = None

  /**
   * Constants specific to Iceberg expression conversion (not in shared IcebergReflection).
   */
  private object Constants {
    // Iceberg expression operation names
    object Operations {
      val IS_NULL = "IS_NULL"
      val IS_NOT_NULL = "IS_NOT_NULL"
      val NOT_NULL = "NOT_NULL"
      val EQ = "EQ"
      val NOT_EQ = "NOT_EQ"
      val LT = "LT"
      val LT_EQ = "LT_EQ"
      val GT = "GT"
      val GT_EQ = "GT_EQ"
      val IN = "IN"
    }

    // Iceberg expression class name suffixes
    object ExpressionTypes {
      val UNBOUND_PREDICATE = "UnboundPredicate"
      val AND = "And"
      val OR = "Or"
      val NOT = "Not"
    }
  }

  /** Iceberg binary comparison operation names mapped to their IcebergPredicate operator. */
  private val binaryOps: Map[String, OperatorOuterClass.IcebergPredicateOperator] = Map(
    Constants.Operations.EQ -> OperatorOuterClass.IcebergPredicateOperator.Eq,
    Constants.Operations.NOT_EQ -> OperatorOuterClass.IcebergPredicateOperator.NotEq,
    Constants.Operations.LT -> OperatorOuterClass.IcebergPredicateOperator.LessThan,
    Constants.Operations.LT_EQ -> OperatorOuterClass.IcebergPredicateOperator.LessThanOrEq,
    Constants.Operations.GT -> OperatorOuterClass.IcebergPredicateOperator.GreaterThan,
    Constants.Operations.GT_EQ -> OperatorOuterClass.IcebergPredicateOperator.GreaterThanOrEq)

  /**
   * Wraps an Iceberg partition value (a typed primitive) in a PartitionValue. The value encoding
   * is shared with predicate literals via [[icebergLiteralToProto]].
   */
  private def partitionValueToProto(
      fieldId: Int,
      fieldTypeStr: String,
      value: Any): OperatorOuterClass.PartitionValue =
    OperatorOuterClass.PartitionValue
      .newBuilder()
      .setFieldId(fieldId)
      .setLiteral(icebergLiteralToProto(fieldTypeStr, value))
      .build()

  /**
   * Converts an Iceberg primitive value to a typed IcebergLiteral, keyed on the Iceberg type
   * name. Protobuf is less verbose than JSON; timestamps/dates/decimals are serialized as their
   * integer or byte encodings rather than strings. Shared by partition values and predicate
   * literals.
   */
  private def icebergLiteralToProto(
      fieldTypeStr: String,
      value: Any): OperatorOuterClass.IcebergLiteral = {
    val builder = OperatorOuterClass.IcebergLiteral.newBuilder()

    if (value == null) {
      builder.setIsNull(true)
    } else {
      fieldTypeStr match {
        case t if t.startsWith("timestamp") =>
          val micros = value match {
            case l: java.lang.Long => l.longValue()
            case i: java.lang.Integer => i.longValue()
            case _ => value.toString.toLong
          }
          if (t.contains("tz")) {
            builder.setTimestampTzVal(micros)
          } else {
            builder.setTimestampVal(micros)
          }

        case "date" =>
          val days = value.asInstanceOf[java.lang.Integer].intValue()
          builder.setDateVal(days)

        case d if d.startsWith("decimal(") =>
          val bigDecimal = value match {
            case bd: BigDecimal => bd
            case _ => new BigDecimal(value.toString)
          }
          builder.setDecimalVal(
            OperatorOuterClass.IcebergDecimal
              .newBuilder()
              .setUnscaled(ByteString.copyFrom(bigDecimal.unscaledValue().toByteArray))
              .setPrecision(bigDecimal.precision())
              .setScale(bigDecimal.scale())
              .build())

        case "string" =>
          builder.setStringVal(value.toString)

        case "int" =>
          val intVal = value match {
            case i: java.lang.Integer => i.intValue()
            case l: java.lang.Long => l.intValue()
            case _ => value.toString.toInt
          }
          builder.setIntVal(intVal)

        case "long" =>
          val longVal = value match {
            case l: java.lang.Long => l.longValue()
            case i: java.lang.Integer => i.longValue()
            case _ => value.toString.toLong
          }
          builder.setLongVal(longVal)

        case "float" =>
          val floatVal = value match {
            case f: java.lang.Float => f.floatValue()
            case d: java.lang.Double => d.floatValue()
            case _ => value.toString.toFloat
          }
          builder.setFloatVal(floatVal)

        case "double" =>
          val doubleVal = value match {
            case d: java.lang.Double => d.doubleValue()
            case f: java.lang.Float => f.doubleValue()
            case _ => value.toString.toDouble
          }
          builder.setDoubleVal(doubleVal)

        case "boolean" =>
          val boolVal = value match {
            case b: java.lang.Boolean => b.booleanValue()
            case _ => value.toString.toBoolean
          }
          builder.setBoolVal(boolVal)

        case "uuid" =>
          // UUID as bytes (16 bytes) or string
          val uuidBytes = value match {
            case uuid: UUID =>
              val bb = ByteBuffer.wrap(new Array[Byte](16))
              bb.putLong(uuid.getMostSignificantBits)
              bb.putLong(uuid.getLeastSignificantBits)
              bb.array()
            case _ =>
              // Parse UUID string and convert to bytes
              val uuid = UUID.fromString(value.toString)
              val bb = ByteBuffer.wrap(new Array[Byte](16))
              bb.putLong(uuid.getMostSignificantBits)
              bb.putLong(uuid.getLeastSignificantBits)
              bb.array()
          }
          builder.setUuidVal(ByteString.copyFrom(uuidBytes))

        case t if t.startsWith("fixed[") || t.startsWith("binary") =>
          val bytes = value match {
            case bytes: Array[Byte] => bytes
            case _ => value.toString.getBytes("UTF-8")
          }
          if (t.startsWith("fixed")) {
            builder.setFixedVal(ByteString.copyFrom(bytes))
          } else {
            builder.setBinaryVal(ByteString.copyFrom(bytes))
          }

        // Fallback: infer type from Java type ?
        case _ =>
          value match {
            case s: String => builder.setStringVal(s)
            case i: java.lang.Integer => builder.setIntVal(i.intValue())
            case l: java.lang.Long => builder.setLongVal(l.longValue())
            case d: java.lang.Double => builder.setDoubleVal(d.doubleValue())
            case f: java.lang.Float => builder.setFloatVal(f.floatValue())
            case b: java.lang.Boolean => builder.setBoolVal(b.booleanValue())
            case other => builder.setStringVal(other.toString)
          }
      }
    }

    builder.build()
  }

  /**
   * An encrypted ContentFile's plaintext StandardKeyMetadata blob as a protobuf ByteString, or
   * None for an unencrypted file (keyMetadata() returns null). `method` is the cached
   * ContentFile.keyMetadata() accessor. A reflection failure here is intentionally not caught: by
   * serde time the plan is already committed to the native scan, so there is no fallback, and
   * silently dropping the key of an encrypted file would fail obscurely in the native reader.
   */
  private def keyMetadataBytes(
      method: java.lang.reflect.Method,
      contentFile: Any): Option[com.google.protobuf.ByteString] =
    method.invoke(contentFile) match {
      case buf: java.nio.ByteBuffer if buf.remaining() > 0 =>
        Some(com.google.protobuf.ByteString.copyFrom(buf.duplicate()))
      case _ => None
    }

  /**
   * Extracts delete files from an Iceberg FileScanTask as a list (for deduplication).
   *
   * Delete-file size is not serialized; the native scan stats each file for it (see
   * IcebergScanExec::fill_delete_file_sizes). The manifest size cannot be trusted as a substitute
   * (apache/iceberg#12554).
   */
  private def extractDeleteFilesList(
      task: Any,
      contentFileClass: Class[_],
      fileScanTaskClass: Class[_]): Seq[OperatorOuterClass.IcebergDeleteFile] = {
    try {
      val deleteFileClass = IcebergReflection.loadClass(IcebergReflection.ClassNames.DELETE_FILE)
      // keyMetadata() is declared on ContentFile; present across all supported Iceberg versions.
      val keyMetadataMethod = contentFileClass.getMethod("keyMetadata")

      val deletes = IcebergReflection.getDeleteFilesFromTask(task, fileScanTaskClass)

      deletes.asScala.map { deleteFile =>
        // The path is the one essential field. A delete file we cannot locate cannot be applied,
        // and silently skipping it would leak deleted rows, so treat a missing path as fatal.
        val deletePath = IcebergReflection
          .extractFileLocation(contentFileClass, deleteFile)
          .getOrElse(
            throw new RuntimeException("Failed to extract delete file path from FileScanTask"))

        val deleteBuilder = OperatorOuterClass.IcebergDeleteFile.newBuilder()
        deleteBuilder.setFilePath(deletePath)

        val contentType =
          try {
            val contentMethod = deleteFileClass.getMethod("content")
            val content = contentMethod.invoke(deleteFile)
            content.toString match {
              case IcebergReflection.ContentTypes.POSITION_DELETES =>
                IcebergReflection.ContentTypes.POSITION_DELETES
              case IcebergReflection.ContentTypes.EQUALITY_DELETES =>
                IcebergReflection.ContentTypes.EQUALITY_DELETES
              case other => other
            }
          } catch {
            case _: Exception =>
              IcebergReflection.ContentTypes.POSITION_DELETES
          }
        deleteBuilder.setContentType(contentType)

        val specId =
          try {
            val specIdMethod = deleteFileClass.getMethod("specId")
            specIdMethod.invoke(deleteFile).asInstanceOf[Int]
          } catch {
            case _: Exception => 0
          }
        deleteBuilder.setPartitionSpecId(specId)

        try {
          val equalityIdsMethod =
            deleteFileClass.getMethod("equalityFieldIds")
          val equalityIds = equalityIdsMethod
            .invoke(deleteFile)
            .asInstanceOf[java.util.List[Integer]]
          equalityIds.forEach(id => deleteBuilder.addEqualityIds(id))
        } catch {
          case _: Exception =>
        }

        // V3 deletion vector coordinates. These DeleteFile accessors exist only on newer Iceberg
        // versions and return null for non-deletion-vector deletes, so absence is expected and
        // non-fatal.
        try {
          deleteFileClass.getMethod("referencedDataFile").invoke(deleteFile) match {
            case path: String => deleteBuilder.setReferencedDataFile(path)
            case _ =>
          }
        } catch {
          case _: Exception =>
        }

        try {
          deleteFileClass.getMethod("contentOffset").invoke(deleteFile) match {
            case offset: java.lang.Long => deleteBuilder.setContentOffset(offset)
            case _ =>
          }
        } catch {
          case _: Exception =>
        }

        try {
          deleteFileClass.getMethod("contentSizeInBytes").invoke(deleteFile) match {
            case size: java.lang.Long => deleteBuilder.setContentSizeInBytes(size)
            case _ =>
          }
        } catch {
          case _: Exception =>
        }

        // Encrypted delete files carry a plaintext StandardKeyMetadata blob; forward it verbatim.
        // Unencrypted delete files leave the field unset.
        keyMetadataBytes(keyMetadataMethod, deleteFile).foreach(deleteBuilder.setKeyMetadata)

        deleteBuilder.build()
      }.toSeq
    } catch {
      case e: Exception =>
        val msg =
          "Iceberg reflection failure: Failed to extract deletes from FileScanTask: " +
            s"${e.getMessage}"
        logError(msg)
        throw new RuntimeException(msg, e)
    }
  }

  /**
   * Serializes partition spec and data from an Iceberg FileScanTask.
   *
   * Extracts partition specification (field definitions and transforms) and partition data
   * (actual values) from the task. This information is used by the native execution engine to
   * build a constants_map for identity-transformed partition columns and to handle
   * partition-level filtering.
   */
  private def serializePartitionData(
      task: Any,
      contentScanTaskClass: Class[_],
      fileScanTaskClass: Class[_],
      taskBuilder: OperatorOuterClass.IcebergFileScanTask.Builder,
      commonBuilder: OperatorOuterClass.IcebergScanCommon.Builder,
      partitionTypeToPoolIndex: mutable.HashMap[String, Int],
      partitionSpecToPoolIndex: mutable.HashMap[String, Int],
      partitionDataToPoolIndex: mutable.HashMap[String, Int]): Unit = {
    try {
      val specMethod = fileScanTaskClass.getMethod("spec")
      val spec = specMethod.invoke(task)

      if (spec != null) {
        // Deduplicate partition spec
        try {
          val partitionSpecParserClass =
            IcebergReflection.loadClass(IcebergReflection.ClassNames.PARTITION_SPEC_PARSER)
          val toJsonMethod = partitionSpecParserClass.getMethod(
            "toJson",
            IcebergReflection.loadClass(IcebergReflection.ClassNames.PARTITION_SPEC))
          val partitionSpecJson = toJsonMethod
            .invoke(null, spec)
            .asInstanceOf[String]

          val specIdx = partitionSpecToPoolIndex.getOrElseUpdate(
            partitionSpecJson, {
              val idx = partitionSpecToPoolIndex.size
              commonBuilder.addPartitionSpecPool(partitionSpecJson)
              idx
            })
          taskBuilder.setPartitionSpecIdx(specIdx)
        } catch {
          case e: Exception =>
            logWarning(s"Failed to serialize partition spec to JSON: ${e.getMessage}")
        }

        // Get partition data from the task (via file().partition())
        val partitionMethod = contentScanTaskClass.getMethod("partition")
        val partitionData = partitionMethod.invoke(task)

        if (partitionData != null) {
          // Get the partition type/schema from the spec
          val partitionTypeMethod = spec.getClass.getMethod("partitionType")
          val partitionType = partitionTypeMethod.invoke(spec)

          // Check if partition type has any fields before serializing
          val fieldsMethod = partitionType.getClass.getMethod("fields")
          val fields = fieldsMethod
            .invoke(partitionType)
            .asInstanceOf[java.util.List[_]]

          // Helper to get field type string (shared by both type and data serialization)
          def getFieldType(field: Any): String = {
            val typeMethod = field.getClass.getMethod("type")
            typeMethod.invoke(field).toString
          }

          // Only serialize partition type if there are actual partition fields
          if (!fields.isEmpty) {
            try {
              // Manually build StructType JSON to match iceberg-rust expectations.
              // Using Iceberg's SchemaParser.toJson() would include schema-level
              // metadata (e.g., "schema-id") that iceberg-rust's StructType
              // deserializer rejects. We need pure StructType format:
              // {"type":"struct","fields":[...]}

              // Filter out fields with unknown types (dropped partition fields).
              // Unknown type fields represent partition columns that have been dropped
              // from the schema. Per the Iceberg spec, unknown type fields are not
              // stored in data files and iceberg-rust doesn't support deserializing
              // them. Since these columns are dropped, we don't need to expose their
              // partition values when reading.
              val fieldsJson = fields.asScala.flatMap { field =>
                val fieldTypeStr = getFieldType(field)

                // Skip fields with unknown type (dropped partition columns)
                if (fieldTypeStr == IcebergReflection.TypeNames.UNKNOWN) {
                  None
                } else {
                  val fieldIdMethod = field.getClass.getMethod("fieldId")
                  val fieldId = fieldIdMethod.invoke(field).asInstanceOf[Int]

                  val nameMethod = field.getClass.getMethod("name")
                  val fieldName = nameMethod.invoke(field).asInstanceOf[String]

                  val isOptionalMethod = field.getClass.getMethod("isOptional")
                  val isOptional =
                    isOptionalMethod.invoke(field).asInstanceOf[Boolean]
                  val required = !isOptional

                  Some(
                    ("id" -> fieldId) ~
                      ("name" -> fieldName) ~
                      ("required" -> required) ~
                      ("type" -> fieldTypeStr))
                }
              }.toList

              // Only serialize if we have non-unknown fields
              if (fieldsJson.nonEmpty) {
                val partitionTypeJson = compact(
                  render(
                    ("type" -> "struct") ~
                      ("fields" -> fieldsJson)))

                val typeIdx = partitionTypeToPoolIndex.getOrElseUpdate(
                  partitionTypeJson, {
                    val idx = partitionTypeToPoolIndex.size
                    commonBuilder.addPartitionTypePool(partitionTypeJson)
                    idx
                  })
                taskBuilder.setPartitionTypeIdx(typeIdx)
              }
            } catch {
              case e: Exception =>
                logWarning(s"Failed to serialize partition type to JSON: ${e.getMessage}")
            }
          }

          // Serialize partition data to protobuf for native execution.
          // The native execution engine uses partition_data protobuf messages to
          // build a constants_map, which provides partition values to identity-
          // transformed partition columns. Non-identity transforms (bucket, truncate,
          // days, etc.) read values from data files.
          //
          // IMPORTANT: Use partition field IDs (not source field IDs) to match
          // the schema.

          // Filter out fields with unknown type (same as partition type filtering)
          val partitionValues: Seq[OperatorOuterClass.PartitionValue] =
            fields.asScala.zipWithIndex.flatMap { case (field, idx) =>
              val fieldTypeStr = getFieldType(field)

              // Skip fields with unknown type (dropped partition columns)
              if (fieldTypeStr == IcebergReflection.TypeNames.UNKNOWN) {
                None
              } else {
                // Use the partition type's field ID (same as in partition_type_json)
                val fieldIdMethod = field.getClass.getMethod("fieldId")
                val fieldId = fieldIdMethod.invoke(field).asInstanceOf[Int]

                val getMethod =
                  partitionData.getClass.getMethod("get", classOf[Int], classOf[Class[_]])
                val value = getMethod.invoke(partitionData, Integer.valueOf(idx), classOf[Object])

                Some(partitionValueToProto(fieldId, fieldTypeStr, value))
              }
            }.toSeq

          // Only serialize partition data if we have non-unknown fields
          if (partitionValues.nonEmpty) {
            val partitionDataProto = OperatorOuterClass.PartitionData
              .newBuilder()
              .addAllValues(partitionValues.asJava)
              .build()

            // Deduplicate by protobuf bytes (use Base64 string as key)
            val partitionDataBytes = partitionDataProto.toByteArray
            val partitionDataKey = Base64.getEncoder.encodeToString(partitionDataBytes)

            val partitionDataIdx = partitionDataToPoolIndex.getOrElseUpdate(
              partitionDataKey, {
                val idx = partitionDataToPoolIndex.size
                commonBuilder.addPartitionDataPool(partitionDataProto)
                idx
              })
            taskBuilder.setPartitionDataIdx(partitionDataIdx)
          }
        }
      }
    } catch {
      case e: Exception =>
        val msg =
          "Iceberg reflection failure: Failed to extract partition data from FileScanTask: " +
            s"${e.getMessage}"
        logError(msg, e)
        throw new RuntimeException(msg, e)
    }
  }

  /**
   * Transforms Hadoop S3A configuration keys to Iceberg FileIO property keys.
   *
   * Iceberg-rust's FileIO expects Iceberg-format keys (e.g., s3.access-key-id), not Hadoop keys
   * (e.g., fs.s3a.access.key). This function converts Hadoop keys extracted from Spark's
   * configuration to the format expected by iceberg-rust.
   */
  def hadoopToIcebergS3Properties(hadoopProps: Map[String, String]): Map[String, String] = {
    hadoopProps.flatMap { case (key, value) =>
      key match {
        // Global S3A configuration keys
        case "fs.s3a.access.key" => Some("s3.access-key-id" -> value)
        case "fs.s3a.secret.key" => Some("s3.secret-access-key" -> value)
        case "fs.s3a.session.token" => Some("s3.session-token" -> value)
        case "fs.s3a.endpoint" => Some("s3.endpoint" -> value)
        case "fs.s3a.path.style.access" => Some("s3.path-style-access" -> value)
        case "fs.s3a.endpoint.region" => Some("s3.region" -> value)

        // Per-bucket configuration keys (e.g., fs.s3a.bucket.mybucket.access.key)
        // Extract bucket name and property, then transform to s3.* format
        case k if k.startsWith("fs.s3a.bucket.") =>
          val parts = k.stripPrefix("fs.s3a.bucket.").split("\\.", 2)
          if (parts.length == 2) {
            val bucket = parts(0)
            val property = parts(1)
            property match {
              case "access.key" => Some(s"s3.bucket.$bucket.access-key-id" -> value)
              case "secret.key" => Some(s"s3.bucket.$bucket.secret-access-key" -> value)
              case "session.token" => Some(s"s3.bucket.$bucket.session.token" -> value)
              case "endpoint" => Some(s"s3.bucket.$bucket.endpoint" -> value)
              case "path.style.access" => Some(s"s3.bucket.$bucket.path-style-access" -> value)
              case "endpoint.region" => Some(s"s3.bucket.$bucket.region" -> value)
              case _ => None
            }
          } else {
            None
          }

        // Pass through any keys that are already in Iceberg format
        case k if k.startsWith("s3.") => Some(key -> value)

        // Ignore all other keys
        case _ => None
      }
    }
  }

  /**
   * Converts an Iceberg residual Expression into an IcebergPredicate for native row-group
   * pruning.
   *
   * Residuals come from Iceberg's ResidualEvaluator (partial evaluation of the scan filter
   * against each file's partition data). This is only a pruning hint: the CometFilter above the
   * scan enforces correctness, so any node or literal we cannot represent yields None (no
   * pushdown). Predicates over `pageIndexUnsupportedColumns` also yield None (iceberg-rust cannot
   * use those columns in the page index). Uses reflection because Iceberg's expression classes
   * are not on Spark's classpath at planning time; residuals are unbound predicates carrying a
   * NamedReference (column name) and a literal.
   */
  def icebergExprToProto(
      icebergExpr: Any,
      output: Seq[Attribute],
      pageIndexUnsupportedColumns: Set[String]): Option[OperatorOuterClass.IcebergPredicate] = {
    try {
      val exprClass = icebergExpr.getClass
      val attributeMap = output.map(attr => attr.name -> attr).toMap

      if (exprClass.getName.endsWith(Constants.ExpressionTypes.UNBOUND_PREDICATE)) {
        val operation = exprClass.getMethod("op").invoke(icebergExpr).toString
        val term = exprClass.getMethod("term").invoke(icebergExpr)
        val ref = term.getClass.getMethod("ref").invoke(term)
        val columnName = ref.getClass.getMethod("name").invoke(ref).asInstanceOf[String]

        // Iceberg names a nested reference by its dotted path ("struct.field"), which never matches
        // a top-level scan output attribute, so a residual on a nested field drops here. That miss
        // is also why the top-level-only pageIndexUnsupportedColumns gate below stays sound.
        attributeMap.get(columnName).flatMap { attribute =>
          import Constants.Operations._
          import OperatorOuterClass.IcebergPredicateOperator
          if (pageIndexUnsupportedColumns.contains(columnName)) {
            // Any predicate on this column, including a unary IS [NOT] NULL, would reach the page
            // index and fail, so drop the whole predicate; the post-scan CometFilter enforces it.
            None
          } else {
            operation match {
              case IS_NULL => Some(unaryPredicate(columnName, IcebergPredicateOperator.IsNull))
              case IS_NOT_NULL | NOT_NULL =>
                Some(unaryPredicate(columnName, IcebergPredicateOperator.NotNull))
              case op if binaryOps.contains(op) =>
                binaryPredicate(exprClass, icebergExpr, columnName, attribute, binaryOps(op))
              case IN => setPredicate(exprClass, icebergExpr, columnName, attribute)
              // NOT_IN is inherently unprunable from column stats, so it is not pushed.
              case _ => None
            }
          }
        }
      } else if (exprClass.getName.endsWith(Constants.ExpressionTypes.AND)) {
        val left = icebergExprToProto(
          exprClass.getMethod("left").invoke(icebergExpr),
          output,
          pageIndexUnsupportedColumns)
        val right = icebergExprToProto(
          exprClass.getMethod("right").invoke(icebergExpr),
          output,
          pageIndexUnsupportedColumns)
        (left, right) match {
          // Push the residual only if it converts whole. Dropping a conjunct is safe in positive
          // position but strengthens the predicate under a NOT (De Morgan), which would wrongly
          // prune, and tracking polarity across arbitrary nesting is error prone. So an
          // unconvertible conjunct elides the whole residual; the post-scan CometFilter is exact.
          case (Some(l), Some(r)) => Some(logicalPredicate(isAnd = true, l, r))
          case _ => None
        }
      } else if (exprClass.getName.endsWith(Constants.ExpressionTypes.OR)) {
        val left = icebergExprToProto(
          exprClass.getMethod("left").invoke(icebergExpr),
          output,
          pageIndexUnsupportedColumns)
        val right = icebergExprToProto(
          exprClass.getMethod("right").invoke(icebergExpr),
          output,
          pageIndexUnsupportedColumns)
        // Dropping a disjunct would strengthen the predicate and wrongly prune, so require both.
        (left, right) match {
          case (Some(l), Some(r)) => Some(logicalPredicate(isAnd = false, l, r))
          case _ => None
        }
      } else if (exprClass.getName.endsWith(Constants.ExpressionTypes.NOT)) {
        val child = exprClass.getMethod("child").invoke(icebergExpr)
        icebergExprToProto(child, output, pageIndexUnsupportedColumns).map(notPredicate)
      } else {
        None
      }
    } catch {
      // Reflection over Iceberg's expression classes can fail on an unexpected shape (e.g. an
      // Iceberg version change). A residual is only a pruning hint, so skip pushdown rather than
      // fail the scan, but log it: a persistent warning here signals a real API drift to fix.
      case e: Exception =>
        logWarning(
          "Skipping Iceberg residual pushdown; could not convert expression: " +
            s"${e.getMessage}")
        None
    }
  }

  private def unaryPredicate(
      column: String,
      op: OperatorOuterClass.IcebergPredicateOperator): OperatorOuterClass.IcebergPredicate =
    OperatorOuterClass.IcebergPredicate
      .newBuilder()
      .setUnary(OperatorOuterClass.IcebergUnaryPredicate.newBuilder().setColumn(column).setOp(op))
      .build()

  private def binaryPredicate(
      exprClass: Class[_],
      icebergExpr: Any,
      column: String,
      attribute: Attribute,
      op: OperatorOuterClass.IcebergPredicateOperator)
      : Option[OperatorOuterClass.IcebergPredicate] = {
    val literal = exprClass.getMethod("literal").invoke(icebergExpr)
    predicateLiteralToProto(attribute.dataType, icebergLiteralValue(literal)).map { lit =>
      OperatorOuterClass.IcebergPredicate
        .newBuilder()
        .setBinary(
          OperatorOuterClass.IcebergBinaryPredicate
            .newBuilder()
            .setColumn(column)
            .setOp(op)
            .setValue(lit))
        .build()
    }
  }

  private def setPredicate(
      exprClass: Class[_],
      icebergExpr: Any,
      column: String,
      attribute: Attribute): Option[OperatorOuterClass.IcebergPredicate] = {
    val literals =
      exprClass.getMethod("literals").invoke(icebergExpr).asInstanceOf[java.util.List[_]]
    val protoLiterals =
      literals.asScala.map(l =>
        predicateLiteralToProto(attribute.dataType, icebergLiteralValue(l)))
    if (protoLiterals.isEmpty || protoLiterals.exists(_.isEmpty)) {
      None
    } else {
      val setBuilder = OperatorOuterClass.IcebergSetPredicate
        .newBuilder()
        .setColumn(column)
        .setOp(OperatorOuterClass.IcebergPredicateOperator.In)
      // Sort literals so an IN list that Iceberg happens to iterate in a different order per file
      // still deduplicates in the residual pool. Encode each key once (sortBy re-invokes its key
      // function per comparison).
      protoLiterals.flatten
        .map(lit => (Base64.getEncoder.encodeToString(lit.toByteArray), lit))
        .sortBy(_._1)
        .foreach { case (_, lit) => setBuilder.addValues(lit) }
      Some(OperatorOuterClass.IcebergPredicate.newBuilder().setSet(setBuilder).build())
    }
  }

  private def logicalPredicate(
      isAnd: Boolean,
      left: OperatorOuterClass.IcebergPredicate,
      right: OperatorOuterClass.IcebergPredicate): OperatorOuterClass.IcebergPredicate = {
    val logical = OperatorOuterClass.IcebergLogicalPredicate
      .newBuilder()
      .setLeft(left)
      .setRight(right)
    val builder = OperatorOuterClass.IcebergPredicate.newBuilder()
    if (isAnd) builder.setAnd(logical) else builder.setOr(logical)
    builder.build()
  }

  private def notPredicate(
      child: OperatorOuterClass.IcebergPredicate): OperatorOuterClass.IcebergPredicate =
    OperatorOuterClass.IcebergPredicate.newBuilder().setNot(child).build()

  /** Extracts the raw Java value from an Iceberg Literal via reflection. */
  private def icebergLiteralValue(icebergLiteral: Any): Any = {
    val literalClass = IcebergReflection.loadClass(IcebergReflection.ClassNames.LITERAL)
    literalClass.getMethod("value").invoke(icebergLiteral)
  }

  /**
   * Builds a predicate literal from its Iceberg value and the column's Spark type. Returns None
   * for null (IS NULL is a separate op) and for Spark types this serde does not map to an
   * IcebergLiteral, so the predicate degrades to no pushdown rather than an incorrect one.
   * Columns backed by Parquet FIXED_LEN_BYTE_ARRAY (decimal/uuid/fixed) or BYTE_ARRAY (binary)
   * are dropped earlier by the column gate in icebergExprToProto and never reach here.
   */
  private def predicateLiteralToProto(
      sparkType: DataType,
      value: Any): Option[OperatorOuterClass.IcebergLiteral] = {
    if (value == null) {
      return None
    }
    val builder = OperatorOuterClass.IcebergLiteral.newBuilder()
    sparkType match {
      case _: BooleanType => builder.setBoolVal(value.asInstanceOf[java.lang.Boolean])
      // Spark byte/short map to Iceberg int (32-bit); iceberg-rust has no narrower integer type, so
      // widening to int_val matches how it stores and prunes these columns.
      case _: ByteType | _: ShortType | _: IntegerType =>
        builder.setIntVal(value.asInstanceOf[java.lang.Number].intValue())
      case _: LongType => builder.setLongVal(value.asInstanceOf[java.lang.Number].longValue())
      case _: FloatType => builder.setFloatVal(value.asInstanceOf[java.lang.Number].floatValue())
      case _: DoubleType =>
        builder.setDoubleVal(value.asInstanceOf[java.lang.Number].doubleValue())
      case _: DateType => builder.setDateVal(value.asInstanceOf[java.lang.Number].intValue())
      case _: StringType => builder.setStringVal(value.toString)
      case _: TimestampType =>
        builder.setTimestampTzVal(value.asInstanceOf[java.lang.Number].longValue())
      case _: TimestampNTZType =>
        builder.setTimestampVal(value.asInstanceOf[java.lang.Number].longValue())
      case _ => return None
    }
    Some(builder.build())
  }

  /**
   * Converts a CometBatchScanExec to a minimal placeholder IcebergScan operator.
   *
   * Returns a placeholder operator with only metadata_location for matching during partition
   * injection. All other fields (catalog properties, required schema, pools, partition data) are
   * set by serializePartitions() at execution time after DPP resolves.
   */
  override def convert(
      scan: CometBatchScanExec,
      builder: Operator.Builder,
      childOp: Operator*): Option[OperatorOuterClass.Operator] = {

    val metadata = scan.nativeIcebergScanMetadata.getOrElse {
      throw new IllegalStateException(
        "Programming error: CometBatchScanExec.nativeIcebergScanMetadata is None. " +
          "Metadata should have been extracted in CometScanRule.")
    }

    val icebergScanBuilder = OperatorOuterClass.IcebergScan.newBuilder()
    val commonBuilder = OperatorOuterClass.IcebergScanCommon.newBuilder()

    // Only set metadata_location - used for matching in PlanDataInjector.
    // All other fields (catalog_properties, required_schema, pools) are set by
    // serializePartitions() at execution time, so setting them here would be wasted work.
    commonBuilder.setMetadataLocation(metadata.metadataLocation)

    icebergScanBuilder.setCommon(commonBuilder.build())
    // partition field intentionally empty - will be populated at execution time

    builder.clearChildren()
    Some(builder.setIcebergScan(icebergScanBuilder).build())
  }

  /**
   * Serializes partitions from inputRDD at execution time.
   *
   * Called after doPrepare() has resolved DPP subqueries. Builds pools and per-partition data in
   * one pass from the DPP-filtered partitions.
   *
   * The result splits into a common block plus one block per Spark partition. The common block
   * holds everything shared across partitions and is broadcast once per executor in the stage
   * task binary; each per-partition block ships with its task. Protobuf serializes by value and
   * shares nothing between repeated fields, so shared values (schemas, specs, partition data,
   * residuals, delete files) are interned into pools in the common block and referenced by index.
   * Pooling also keeps a message under protobuf's 2 GiB limit (getSerializedSize returns int and
   * wraps past it), which matters for anything that scales with the number of tasks.
   *
   * Delete files use two levels: a flat pool of unique files, plus a per-task list of indices
   * into it. One delete file applies to many data files under Iceberg's default partition delete
   * granularity, so this stores each file once and references it many times, as Iceberg's own
   * DeleteFileIndex does.
   *
   * @param scanExec
   *   The BatchScanExec whose inputRDD contains the DPP-filtered partitions
   * @param output
   *   The output attributes for the scan
   * @param metadata
   *   Pre-extracted Iceberg metadata from CometScanRule
   * @return
   *   Tuple of (commonBytes, perPartitionBytes) for native execution
   */
  def serializePartitions(
      scanExec: BatchScanExec,
      output: Seq[Attribute],
      metadata: CometIcebergNativeScanMetadata): (Array[Byte], Array[Array[Byte]]) = {

    val commonBuilder = OperatorOuterClass.IcebergScanCommon.newBuilder()

    // Deduplication structures - map unique values to pool indices
    val schemaToPoolIndex = mutable.HashMap[AnyRef, Int]()
    val partitionTypeToPoolIndex = mutable.HashMap[String, Int]()
    val partitionSpecToPoolIndex = mutable.HashMap[String, Int]()
    val nameMappingToPoolIndex = mutable.HashMap[String, Int]()
    val projectFieldIdsToPoolIndex = mutable.HashMap[Seq[Int], Int]()
    val partitionDataToPoolIndex = mutable.HashMap[String, Int]()
    // Individual delete files are interned into a flat pool keyed by path (a delete file's path is
    // its identity); deleteFilesToPoolIndex then dedups the per-task sets as lists of indices into
    // that pool. One delete file applies to many data files under Iceberg's default partition
    // delete granularity, so interning avoids re-serializing it once per referencing FileScanTask.
    val deleteFileToPoolIndex = mutable.HashMap[String, Int]()
    val deleteFilesToPoolIndex =
      mutable.HashMap[Seq[Int], Int]()
    val residualToPoolIndex = mutable.HashMap[OperatorOuterClass.IcebergPredicate, Int]()
    // Columns whose Iceberg type iceberg-rust cannot use for page-index pruning; residual
    // predicates over them are dropped (see icebergExprToProto). Computed once from the full table
    // schema so a filter column projected out of the scan output is still recognized.
    val pageIndexUnsupportedColumns =
      IcebergReflection.pageIndexUnsupportedColumns(metadata.tableSchema)

    val perPartitionBuilders = mutable.ArrayBuffer[OperatorOuterClass.IcebergScan]()

    var totalTasks = 0

    commonBuilder.setMetadataLocation(metadata.metadataLocation)
    commonBuilder.setDataFileConcurrencyLimit(
      CometConf.COMET_ICEBERG_DATA_FILE_CONCURRENCY_LIMIT.get())
    metadata.catalogName.foreach(commonBuilder.setCatalogName)
    metadata.catalogProperties.foreach { case (key, value) =>
      commonBuilder.putCatalogProperties(key, value)
    }

    output.foreach { attr =>
      val field = SparkStructField
        .newBuilder()
        .setName(attr.name)
        .setNullable(attr.nullable)
      serializeDataType(attr.dataType).foreach(field.setDataType)
      commonBuilder.addRequiredSchema(field.build())
    }

    // Load Iceberg classes once (avoid repeated class loading in loop)
    val contentScanTaskClass =
      IcebergReflection.loadClass(IcebergReflection.ClassNames.CONTENT_SCAN_TASK)
    val fileScanTaskClass =
      IcebergReflection.loadClass(IcebergReflection.ClassNames.FILE_SCAN_TASK)
    val contentFileClass =
      IcebergReflection.loadClass(IcebergReflection.ClassNames.CONTENT_FILE)
    val schemaParserClass =
      IcebergReflection.loadClass(IcebergReflection.ClassNames.SCHEMA_PARSER)
    val schemaClass =
      IcebergReflection.loadClass(IcebergReflection.ClassNames.SCHEMA)

    // Cache method lookups (avoid repeated getMethod in loop)
    val fileMethod = contentScanTaskClass.getMethod("file")
    val startMethod = contentScanTaskClass.getMethod("start")
    val lengthMethod = contentScanTaskClass.getMethod("length")
    val residualMethod = contentScanTaskClass.getMethod("residual")
    val fileSizeInBytesMethod = contentFileClass.getMethod("fileSizeInBytes")
    // keyMetadata() is declared on ContentFile (present across all supported Iceberg versions).
    // For encrypted tables it returns the plaintext StandardKeyMetadata blob; null otherwise.
    val keyMetadataMethod = contentFileClass.getMethod("keyMetadata")
    val taskSchemaMethod = fileScanTaskClass.getMethod("schema")
    val toJsonMethod = schemaParserClass.getMethod("toJson", schemaClass)
    toJsonMethod.setAccessible(true)

    // Access inputRDD - safe now, DPP is resolved
    scanExec.inputRDD match {
      case rdd: DataSourceRDD =>
        val partitions = rdd.partitions
        partitions.foreach { partition =>
          val partitionBuilder = OperatorOuterClass.IcebergScan.newBuilder()

          val inputPartitions = ShimDataSourceRDDPartition
            .inputPartitions(partition.asInstanceOf[DataSourceRDDPartition])

          inputPartitions.foreach { inputPartition =>
            val inputPartClass = inputPartition.getClass

            {
              val taskGroupMethod = inputPartClass.getDeclaredMethod("taskGroup")
              taskGroupMethod.setAccessible(true)
              val taskGroup = taskGroupMethod.invoke(inputPartition)

              val taskGroupClass = taskGroup.getClass
              val tasksMethod = taskGroupClass.getMethod("tasks")
              val tasksCollection =
                tasksMethod.invoke(taskGroup).asInstanceOf[java.util.Collection[_]]

              tasksCollection.asScala.foreach { task =>
                totalTasks += 1

                val taskBuilder = OperatorOuterClass.IcebergFileScanTask.newBuilder()

                val dataFile = fileMethod.invoke(task)

                val filePathOpt =
                  IcebergReflection.extractFileLocation(contentFileClass, dataFile)

                filePathOpt match {
                  case Some(filePath) =>
                    taskBuilder.setDataFilePath(filePath)
                  case None =>
                    val msg =
                      "Iceberg reflection failure: Cannot extract file path from data file"
                    logError(msg)
                    throw new RuntimeException(msg)
                }

                val start = startMethod.invoke(task).asInstanceOf[Long]
                taskBuilder.setStart(start)

                val length = lengthMethod.invoke(task).asInstanceOf[Long]
                taskBuilder.setLength(length)

                val fileSizeInBytes =
                  fileSizeInBytesMethod.invoke(dataFile).asInstanceOf[Long]
                taskBuilder.setFileSizeInBytes(fileSizeInBytes)

                // Encrypted data files carry a plaintext StandardKeyMetadata blob; forward it
                // verbatim for iceberg-rust to decode. Unencrypted files leave the field unset.
                keyMetadataBytes(keyMetadataMethod, dataFile).foreach(taskBuilder.setKeyMetadata)

                val taskSchema = taskSchemaMethod.invoke(task)

                val deletes =
                  IcebergReflection.getDeleteFilesFromTask(task, fileScanTaskClass)
                val hasDeletes = !deletes.isEmpty

                val schema: AnyRef =
                  if (hasDeletes) {
                    // An equality delete may be keyed on a column dropped from the current schema
                    // (schema evolution). iceberg-rust must read that column to apply the delete,
                    // so union the equality-delete field ids into the task schema, resolving any
                    // dropped ones from the table's schema history (mirrors Iceberg-Java's
                    // DeleteFilter.fileProjection).
                    val equalityFieldIds = deletes.asScala.flatMap { df =>
                      IcebergReflection
                        .getEqualityFieldIds(df)
                        .asScala
                        .map(_.asInstanceOf[java.lang.Integer].intValue())
                    }.toSeq
                    if (equalityFieldIds.nonEmpty) {
                      IcebergReflection
                        .schemaWithRequiredFields(taskSchema, metadata.table, equalityFieldIds)
                        .asInstanceOf[AnyRef]
                    } else {
                      taskSchema
                    }
                  } else {
                    val scanSchemaFieldIds = IcebergReflection
                      .buildFieldIdMapping(metadata.scanSchema)
                      .values
                      .toSet
                    val tableSchemaFieldIds = IcebergReflection
                      .buildFieldIdMapping(metadata.tableSchema)
                      .values
                      .toSet
                    val hasHistoricalColumns =
                      scanSchemaFieldIds.exists(id => !tableSchemaFieldIds.contains(id))

                    if (hasHistoricalColumns) {
                      metadata.scanSchema.asInstanceOf[AnyRef]
                    } else {
                      metadata.tableSchema.asInstanceOf[AnyRef]
                    }
                  }

                val schemaIdx = schemaToPoolIndex.getOrElseUpdate(
                  schema, {
                    val idx = schemaToPoolIndex.size
                    val schemaJson = toJsonMethod.invoke(null, schema).asInstanceOf[String]
                    commonBuilder.addSchemaPool(schemaJson)
                    idx
                  })
                taskBuilder.setSchemaIdx(schemaIdx)

                val nameToFieldId = IcebergReflection.buildFieldIdMapping(schema)

                val projectFieldIds = output.flatMap { attr =>
                  nameToFieldId
                    .get(attr.name)
                    .orElse(metadata.globalFieldIdMapping.get(attr.name))
                    .orElse {
                      logWarning(s"Column '${attr.name}' not found in task or scan schema, " +
                        "skipping projection")
                      None
                    }
                }

                val projectFieldIdsIdx = projectFieldIdsToPoolIndex.getOrElseUpdate(
                  projectFieldIds, {
                    val idx = projectFieldIdsToPoolIndex.size
                    val listBuilder = OperatorOuterClass.ProjectFieldIdList.newBuilder()
                    projectFieldIds.foreach(id => listBuilder.addFieldIds(id))
                    commonBuilder.addProjectFieldIdsPool(listBuilder.build())
                    idx
                  })
                taskBuilder.setProjectFieldIdsIdx(projectFieldIdsIdx)

                val deleteFilesList =
                  extractDeleteFilesList(task, contentFileClass, fileScanTaskClass)
                if (deleteFilesList.nonEmpty) {
                  // Intern each delete file into the flat pool, then dedup this task's set as the
                  // resulting list of pool indices.
                  val deleteFileIndices = deleteFilesList.map { df =>
                    deleteFileToPoolIndex.getOrElseUpdate(
                      df.getFilePath, {
                        val idx = deleteFileToPoolIndex.size
                        commonBuilder.addDeleteFilePool(df)
                        idx
                      })
                  }
                  val deleteFilesIdx = deleteFilesToPoolIndex.getOrElseUpdate(
                    deleteFileIndices, {
                      val idx = deleteFilesToPoolIndex.size
                      val listBuilder = OperatorOuterClass.DeleteFileList.newBuilder()
                      deleteFileIndices.foreach(idx => listBuilder.addDeleteFileIndices(idx))
                      commonBuilder.addDeleteFilesPool(listBuilder.build())
                      idx
                    })
                  taskBuilder.setDeleteFilesIdx(deleteFilesIdx)
                }

                val residualExprOpt =
                  try {
                    icebergExprToProto(
                      residualMethod.invoke(task),
                      output,
                      pageIndexUnsupportedColumns)
                  } catch {
                    case e: Exception =>
                      logWarning(
                        "Failed to extract residual expression from FileScanTask: " +
                          s"${e.getMessage}")
                      None
                  }

                residualExprOpt.foreach { residual =>
                  val residualIdx = residualToPoolIndex.getOrElseUpdate(
                    residual, {
                      val idx = residualToPoolIndex.size
                      commonBuilder.addResidualPool(residual)
                      idx
                    })
                  taskBuilder.setResidualIdx(residualIdx)
                }

                serializePartitionData(
                  task,
                  contentScanTaskClass,
                  fileScanTaskClass,
                  taskBuilder,
                  commonBuilder,
                  partitionTypeToPoolIndex,
                  partitionSpecToPoolIndex,
                  partitionDataToPoolIndex)

                metadata.nameMapping.foreach { nm =>
                  val nmIdx = nameMappingToPoolIndex.getOrElseUpdate(
                    nm, {
                      val idx = nameMappingToPoolIndex.size
                      commonBuilder.addNameMappingPool(nm)
                      idx
                    })
                  taskBuilder.setNameMappingIdx(nmIdx)
                }

                partitionBuilder.addFileScanTasks(taskBuilder.build())
              }
            }
          }

          perPartitionBuilders += partitionBuilder.build()
        }
      case other if other.getClass.getName == "org.apache.spark.rdd.ParallelCollectionRDD" =>
        // Spark's BatchScanExec.inputRDD returns sparkContext.parallelize(empty, 1) when
        // DPP filtering removes all input partitions. That ParallelCollectionRDD is the only
        // non-DataSourceRDD shape its inputRDD produces, so reaching this branch means "DPP
        // pruned everything"; emit no per-partition data and let native execution return empty.
        // Re-querying scan.toBatch.planInputPartitions() to verify is unreliable because
        // Iceberg's Scan state after filter() doesn't always reflect post-DPP partitions on
        // a re-call (V2 scan state is one-shot for the materialized inputRDD). Matched by class
        // name because ParallelCollectionRDD is private[spark].
        logDebug(
          "BatchScanExec.inputRDD is ParallelCollectionRDD (DPP pruned all partitions); " +
            "skipping per-partition serialization")
      case other =>
        throw new IllegalStateException(
          "Expected DataSourceRDD or ParallelCollectionRDD from BatchScanExec, " +
            s"got ${other.getClass.getName}")
    }

    // Log deduplication summary
    val allPoolSizes = Seq(
      schemaToPoolIndex.size,
      partitionTypeToPoolIndex.size,
      partitionSpecToPoolIndex.size,
      nameMappingToPoolIndex.size,
      projectFieldIdsToPoolIndex.size,
      partitionDataToPoolIndex.size,
      deleteFileToPoolIndex.size,
      deleteFilesToPoolIndex.size,
      residualToPoolIndex.size)

    val avgDedup = if (totalTasks == 0) {
      "0.0"
    } else {
      val nonEmptyPools = allPoolSizes.filter(_ > 0)
      if (nonEmptyPools.isEmpty) {
        "0.0"
      } else {
        val avgUnique = nonEmptyPools.sum.toDouble / nonEmptyPools.length
        f"${(1.0 - avgUnique / totalTasks) * 100}%.1f"
      }
    }

    // Per-pool byte sizes to diagnose an oversized common message. Sizes sum as Long because a
    // single pool at or past protobuf's 2 GiB message limit overflows the int getSerializedSize,
    // and the logging runs before toByteArray so the breakdown survives even if that allocation
    // fails. String pools carry JSON, whose serialized size is its UTF-8 length.
    def sumSizes(sizes: Iterator[Int]): Long = sizes.map(_.toLong).sum
    def sumStrBytes(strings: mutable.Buffer[String]): Long =
      strings.iterator.map(_.getBytes(UTF_8).length.toLong).sum
    val commonPoolBytes: Seq[(String, Int, Long)] = Seq(
      (
        "schema",
        commonBuilder.getSchemaPoolCount,
        sumStrBytes(commonBuilder.getSchemaPoolList.asScala)),
      (
        "partition_type",
        commonBuilder.getPartitionTypePoolCount,
        sumStrBytes(commonBuilder.getPartitionTypePoolList.asScala)),
      (
        "partition_spec",
        commonBuilder.getPartitionSpecPoolCount,
        sumStrBytes(commonBuilder.getPartitionSpecPoolList.asScala)),
      (
        "name_mapping",
        commonBuilder.getNameMappingPoolCount,
        sumStrBytes(commonBuilder.getNameMappingPoolList.asScala)),
      (
        "project_field_ids",
        commonBuilder.getProjectFieldIdsPoolCount,
        sumSizes(
          commonBuilder.getProjectFieldIdsPoolList.asScala.iterator.map(_.getSerializedSize))),
      (
        "partition_data",
        commonBuilder.getPartitionDataPoolCount,
        sumSizes(
          commonBuilder.getPartitionDataPoolList.asScala.iterator.map(_.getSerializedSize))),
      (
        "delete_file",
        commonBuilder.getDeleteFilePoolCount,
        sumSizes(commonBuilder.getDeleteFilePoolList.asScala.iterator.map(_.getSerializedSize))),
      (
        "delete_files_set",
        commonBuilder.getDeleteFilesPoolCount,
        sumSizes(commonBuilder.getDeleteFilesPoolList.asScala.iterator.map(_.getSerializedSize))),
      (
        "residual",
        commonBuilder.getResidualPoolCount,
        sumSizes(commonBuilder.getResidualPoolList.asScala.iterator.map(_.getSerializedSize))))

    val perPartitionSizes = perPartitionBuilders.map(_.getSerializedSize.toLong)
    val perPartitionTotal = perPartitionSizes.sum
    val perPartitionMax = if (perPartitionSizes.isEmpty) 0L else perPartitionSizes.max

    logInfo(s"IcebergScan: $totalTasks tasks, ${allPoolSizes.size} pools ($avgDedup% avg dedup)")
    logInfo(
      "  Common pools (unique/bytes): " +
        commonPoolBytes
          .map { case (name, count, bytes) => s"$name=$count/$bytes" }
          .mkString(", ") +
        s"; total=${commonPoolBytes.map(_._3).sum} bytes")
    logInfo(
      s"  Per-partition messages: ${perPartitionBuilders.size}, " +
        s"total=$perPartitionTotal bytes, max=$perPartitionMax bytes")

    val commonBytes = commonBuilder.build().toByteArray
    val perPartitionBytes = perPartitionBuilders.map(_.toByteArray).toArray

    (commonBytes, perPartitionBytes)
  }

  override def createExec(nativeOp: Operator, op: CometBatchScanExec): CometNativeExec = {
    import org.apache.spark.sql.comet.CometIcebergNativeScanExec

    // Extract metadata - it must be present at this point
    val metadata = op.nativeIcebergScanMetadata.getOrElse {
      throw new IllegalStateException(
        "Programming error: CometBatchScanExec.nativeIcebergScanMetadata is None. " +
          "Metadata should have been extracted in CometScanRule.")
    }

    // Extract metadataLocation from the native operator's common data
    val metadataLocation = nativeOp.getIcebergScan.getCommon.getMetadataLocation

    // Pass BatchScanExec reference for deferred serialization (DPP support)
    // Serialization happens at execution time after doPrepare() resolves DPP subqueries
    CometIcebergNativeScanExec(nativeOp, op.wrapped, op.session, metadataLocation, metadata)
  }
}
