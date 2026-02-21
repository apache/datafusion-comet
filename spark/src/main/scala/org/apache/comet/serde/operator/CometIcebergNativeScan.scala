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

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.comet.{CometBatchScanExec, CometNativeExec}
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceRDD, DataSourceRDDPartition}
import org.apache.spark.sql.types._

import org.apache.comet.ConfigEntry
import org.apache.comet.iceberg.{CometIcebergNativeScanMetadata, IcebergReflection, ReflectionCache}
import org.apache.comet.serde.{CometOperatorSerde, OperatorOuterClass}
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.OperatorOuterClass.{Operator, SparkStructField}
import org.apache.comet.serde.QueryPlanSerde.{exprToProto, serializeDataType}

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
      val NOT_IN = "NOT_IN"
    }

    // Iceberg expression class name suffixes
    object ExpressionTypes {
      val UNBOUND_PREDICATE = "UnboundPredicate"
      val AND = "And"
      val OR = "Or"
      val NOT = "Not"
    }
  }

  /**
   * Converts an Iceberg partition value to protobuf format. Protobuf is less verbose than JSON.
   * The following types are also serialized as integer values instead of as strings - Timestamps,
   * Dates, Decimals, FieldIDs
   */
  private def partitionValueToProto(
      fieldId: Int,
      fieldTypeStr: String,
      value: Any): OperatorOuterClass.PartitionValue = {
    val builder = OperatorOuterClass.PartitionValue.newBuilder()
    builder.setFieldId(fieldId)

    if (value == null) {
      builder.setIsNull(true)
    } else {
      builder.setIsNull(false)
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
          // Serialize as unscaled BigInteger bytes
          val bigDecimal = value match {
            case bd: java.math.BigDecimal => bd
            case _ => new java.math.BigDecimal(value.toString)
          }
          val unscaledBytes = bigDecimal.unscaledValue().toByteArray
          builder.setDecimalVal(com.google.protobuf.ByteString.copyFrom(unscaledBytes))

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
            case uuid: java.util.UUID =>
              val bb = java.nio.ByteBuffer.wrap(new Array[Byte](16))
              bb.putLong(uuid.getMostSignificantBits)
              bb.putLong(uuid.getLeastSignificantBits)
              bb.array()
            case _ =>
              // Parse UUID string and convert to bytes
              val uuid = java.util.UUID.fromString(value.toString)
              val bb = java.nio.ByteBuffer.wrap(new Array[Byte](16))
              bb.putLong(uuid.getMostSignificantBits)
              bb.putLong(uuid.getLeastSignificantBits)
              bb.array()
          }
          builder.setUuidVal(com.google.protobuf.ByteString.copyFrom(uuidBytes))

        case t if t.startsWith("fixed[") || t.startsWith("binary") =>
          val bytes = value match {
            case bytes: Array[Byte] => bytes
            case _ => value.toString.getBytes("UTF-8")
          }
          if (t.startsWith("fixed")) {
            builder.setFixedVal(com.google.protobuf.ByteString.copyFrom(bytes))
          } else {
            builder.setBinaryVal(com.google.protobuf.ByteString.copyFrom(bytes))
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
   * Helper to extract a literal from an Iceberg expression and build a binary predicate.
   */
  private def buildBinaryPredicate(
      exprClass: Class[_],
      icebergExpr: Any,
      attribute: Attribute,
      builder: (Expression, Expression) => Expression): Option[Expression] = {
    try {
      val literalMethod = exprClass.getMethod("literal")
      val literal = literalMethod.invoke(icebergExpr)
      val value = convertIcebergLiteral(literal, attribute.dataType)
      Some(builder(attribute, value))
    } catch {
      case _: Exception => None
    }
  }

  private def extractDeleteFilesList(
      task: Any,
      cache: ReflectionCache): Seq[OperatorOuterClass.IcebergDeleteFile] = {
    try {
      val deletes = cache.deletesMethod.invoke(task).asInstanceOf[java.util.List[_]]
      val deletesList = if (deletes == null) new java.util.ArrayList[Any]() else deletes

      deletesList.asScala.flatMap { deleteFile =>
        try {
          IcebergReflection
            .extractFileLocation(cache.contentFileClass, deleteFile)
            .map { deletePath =>
              val deleteBuilder =
                OperatorOuterClass.IcebergDeleteFile.newBuilder()
              deleteBuilder.setFilePath(deletePath)

              val contentType =
                try {
                  val content = cache.deleteContentMethod.invoke(deleteFile)
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
                  cache.deleteSpecIdMethod.invoke(deleteFile).asInstanceOf[Int]
                } catch {
                  case _: Exception =>
                    0
                }
              deleteBuilder.setPartitionSpecId(specId)

              try {
                val equalityIds = cache.deleteEqualityIdsMethod
                  .invoke(deleteFile)
                  .asInstanceOf[java.util.List[Integer]]
                if (equalityIds != null) {
                  equalityIds.forEach(id => deleteBuilder.addEqualityIds(id))
                }
              } catch {
                case _: Exception =>
              }

              deleteBuilder.build()
            }
        } catch {
          case e: Exception =>
            logWarning(s"Failed to serialize delete file: ${e.getMessage}")
            None
        }
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

  private def serializePartitionData(
      task: Any,
      cache: ReflectionCache,
      taskBuilder: OperatorOuterClass.IcebergFileScanTask.Builder,
      commonBuilder: OperatorOuterClass.IcebergScanCommon.Builder,
      partitionTypeToPoolIndex: mutable.HashMap[String, Int],
      partitionSpecToPoolIndex: mutable.HashMap[String, Int],
      partitionDataToPoolIndex: mutable.HashMap[String, Int]): Unit = {
    try {
      val spec = cache.specMethod.invoke(task)

      if (spec != null) {
        // Deduplicate partition spec
        try {
          val partitionSpecJson = cache.specToJsonMethod
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
        val partitionData = cache.partitionMethod.invoke(task)

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
            val partitionDataKey = java.util.Base64.getEncoder.encodeToString(partitionDataBytes)

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

  /** Storage-related property prefixes passed through to native FileIO. */
  private val storagePropertyPrefixes =
    Seq("s3.", "gcs.", "adls.", "client.")

  /**
   * Filters a properties map to only include storage-related keys. FileIO.properties() may
   * contain catalog URIs, bearer tokens, and other non-storage settings that should not be passed
   * to the native FileIO builder.
   */
  def filterStorageProperties(props: Map[String, String]): Map[String, String] = {
    props.filter { case (key, _) =>
      storagePropertyPrefixes.exists(prefix => key.startsWith(prefix))
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
   * Converts Iceberg Expression objects to Spark Catalyst expressions.
   *
   * This is used to extract per-file residual expressions from Iceberg FileScanTasks. Residuals
   * are created by Iceberg's ResidualEvaluator through partial evaluation of scan filters against
   * each file's partition data. These residuals enable row-group level filtering in the Parquet
   * reader.
   *
   * The conversion uses reflection because Iceberg expressions are not directly accessible from
   * Spark's classpath during query planning.
   */
  def convertIcebergExpression(icebergExpr: Any, output: Seq[Attribute]): Option[Expression] = {
    try {
      val exprClass = icebergExpr.getClass
      val attributeMap = output.map(attr => attr.name -> attr).toMap

      // Check for UnboundPredicate
      if (exprClass.getName.endsWith(Constants.ExpressionTypes.UNBOUND_PREDICATE)) {
        val opMethod = exprClass.getMethod("op")
        val termMethod = exprClass.getMethod("term")
        val operation = opMethod.invoke(icebergExpr)
        val term = termMethod.invoke(icebergExpr)

        // Get column name from term
        val refMethod = term.getClass.getMethod("ref")
        val ref = refMethod.invoke(term)
        val nameMethod = ref.getClass.getMethod("name")
        val columnName = nameMethod.invoke(ref).asInstanceOf[String]

        val attr = attributeMap.get(columnName)

        val opName = operation.toString

        attr.flatMap { attribute =>
          opName match {
            case Constants.Operations.IS_NULL =>
              Some(IsNull(attribute))

            case Constants.Operations.IS_NOT_NULL | Constants.Operations.NOT_NULL =>
              Some(IsNotNull(attribute))

            case Constants.Operations.EQ =>
              buildBinaryPredicate(exprClass, icebergExpr, attribute, EqualTo)

            case Constants.Operations.NOT_EQ =>
              buildBinaryPredicate(
                exprClass,
                icebergExpr,
                attribute,
                (a, v) => Not(EqualTo(a, v)))

            case Constants.Operations.LT =>
              buildBinaryPredicate(exprClass, icebergExpr, attribute, LessThan)

            case Constants.Operations.LT_EQ =>
              buildBinaryPredicate(exprClass, icebergExpr, attribute, LessThanOrEqual)

            case Constants.Operations.GT =>
              buildBinaryPredicate(exprClass, icebergExpr, attribute, GreaterThan)

            case Constants.Operations.GT_EQ =>
              buildBinaryPredicate(exprClass, icebergExpr, attribute, GreaterThanOrEqual)

            case Constants.Operations.IN =>
              val literalsMethod = exprClass.getMethod("literals")
              val literals = literalsMethod.invoke(icebergExpr).asInstanceOf[java.util.List[_]]
              val values =
                literals.asScala.map(lit => convertIcebergLiteral(lit, attribute.dataType))
              Some(In(attribute, values.toSeq))

            case Constants.Operations.NOT_IN =>
              val literalsMethod = exprClass.getMethod("literals")
              val literals = literalsMethod.invoke(icebergExpr).asInstanceOf[java.util.List[_]]
              val values =
                literals.asScala.map(lit => convertIcebergLiteral(lit, attribute.dataType))
              Some(Not(In(attribute, values.toSeq)))

            case _ =>
              None
          }
        }
      } else if (exprClass.getName.endsWith(Constants.ExpressionTypes.AND)) {
        val leftMethod = exprClass.getMethod("left")
        val rightMethod = exprClass.getMethod("right")
        val left = leftMethod.invoke(icebergExpr)
        val right = rightMethod.invoke(icebergExpr)

        (convertIcebergExpression(left, output), convertIcebergExpression(right, output)) match {
          case (Some(l), Some(r)) => Some(And(l, r))
          case _ => None
        }
      } else if (exprClass.getName.endsWith(Constants.ExpressionTypes.OR)) {
        val leftMethod = exprClass.getMethod("left")
        val rightMethod = exprClass.getMethod("right")
        val left = leftMethod.invoke(icebergExpr)
        val right = rightMethod.invoke(icebergExpr)

        (convertIcebergExpression(left, output), convertIcebergExpression(right, output)) match {
          case (Some(l), Some(r)) => Some(Or(l, r))
          case _ => None
        }
      } else if (exprClass.getName.endsWith(Constants.ExpressionTypes.NOT)) {
        val childMethod = exprClass.getMethod("child")
        val child = childMethod.invoke(icebergExpr)

        convertIcebergExpression(child, output).map(Not)
      } else {
        None
      }
    } catch {
      case _: Exception =>
        None
    }
  }

  /**
   * Converts an Iceberg Literal to a Spark Literal
   */
  private def convertIcebergLiteral(icebergLiteral: Any, sparkType: DataType): Literal = {
    // Load Literal interface to get value() method (use interface to avoid package-private issues)
    // scalastyle:off classforname
    val literalClass = Class.forName(IcebergReflection.ClassNames.LITERAL)
    // scalastyle:on classforname
    val valueMethod = literalClass.getMethod("value")
    val value = valueMethod.invoke(icebergLiteral)

    // Convert Java types to Spark internal types
    val sparkValue = (value, sparkType) match {
      case (s: String, _: StringType) =>
        org.apache.spark.unsafe.types.UTF8String.fromString(s)
      case (v, _) => v
    }

    Literal(sparkValue, sparkType)
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
    val deleteFilesToPoolIndex =
      mutable.HashMap[Seq[OperatorOuterClass.IcebergDeleteFile], Int]()
    val residualToPoolIndex = mutable.HashMap[Option[Expr], Int]()

    val perPartitionBuilders = mutable.ArrayBuffer[OperatorOuterClass.IcebergScan]()

    var totalTasks = 0

    commonBuilder.setMetadataLocation(metadata.metadataLocation)
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

    val cache = ReflectionCache.create()
    val fieldIdMappingCache = mutable.HashMap[AnyRef, Map[String, Int]]()

    scanExec.inputRDD match {
      case rdd: DataSourceRDD =>
        val partitions = rdd.partitions
        partitions.foreach { partition =>
          val partitionBuilder = OperatorOuterClass.IcebergScan.newBuilder()

          val inputPartitions = partition
            .asInstanceOf[DataSourceRDDPartition]
            .inputPartitions

          inputPartitions.foreach { inputPartition =>
            val inputPartClass = inputPartition.getClass

            try {
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

                val dataFile = cache.fileMethod.invoke(task)

                val filePathOpt =
                  IcebergReflection.extractFileLocation(cache.contentFileClass, dataFile)

                filePathOpt match {
                  case Some(filePath) =>
                    taskBuilder.setDataFilePath(filePath)
                  case None =>
                    val msg =
                      "Iceberg reflection failure: Cannot extract file path from data file"
                    logError(msg)
                    throw new RuntimeException(msg)
                }

                val start = cache.startMethod.invoke(task).asInstanceOf[Long]
                taskBuilder.setStart(start)

                val length = cache.lengthMethod.invoke(task).asInstanceOf[Long]
                taskBuilder.setLength(length)

                val taskSchema = cache.taskSchemaMethod.invoke(task)

                val deletes = cache.deletesMethod.invoke(task).asInstanceOf[java.util.List[_]]
                val deletesList = if (deletes == null) new java.util.ArrayList[Any]() else deletes
                val hasDeletes = !deletesList.isEmpty

                val schema: AnyRef =
                  if (hasDeletes) {
                    taskSchema
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
                    val schemaJson =
                      cache.schemaToJsonMethod.invoke(null, schema).asInstanceOf[String]
                    commonBuilder.addSchemaPool(schemaJson)
                    idx
                  })
                taskBuilder.setSchemaIdx(schemaIdx)

                // Use cached field ID mapping to avoid repeated reflection per-task
                val nameToFieldId = fieldIdMappingCache.getOrElseUpdate(
                  schema,
                  IcebergReflection.buildFieldIdMapping(schema))

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

                val deleteFilesList = extractDeleteFilesList(task, cache)
                if (deleteFilesList.nonEmpty) {
                  val deleteFilesIdx = deleteFilesToPoolIndex.getOrElseUpdate(
                    deleteFilesList, {
                      val idx = deleteFilesToPoolIndex.size
                      val listBuilder = OperatorOuterClass.DeleteFileList.newBuilder()
                      deleteFilesList.foreach(df => listBuilder.addDeleteFiles(df))
                      commonBuilder.addDeleteFilesPool(listBuilder.build())
                      idx
                    })
                  taskBuilder.setDeleteFilesIdx(deleteFilesIdx)
                }

                val residualExprOpt =
                  try {
                    val residualExpr = cache.residualMethod.invoke(task)
                    val catalystExpr = convertIcebergExpression(residualExpr, output)
                    catalystExpr.flatMap { expr =>
                      exprToProto(expr, output, binding = false)
                    }
                  } catch {
                    case e: Exception =>
                      logWarning(
                        "Failed to extract residual expression from FileScanTask: " +
                          s"${e.getMessage}")
                      None
                  }

                residualExprOpt.foreach { residualExpr =>
                  val residualIdx = residualToPoolIndex.getOrElseUpdate(
                    Some(residualExpr), {
                      val idx = residualToPoolIndex.size
                      commonBuilder.addResidualPool(residualExpr)
                      idx
                    })
                  taskBuilder.setResidualIdx(residualIdx)
                }

                serializePartitionData(
                  task,
                  cache,
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
      case _ =>
        throw new IllegalStateException("Expected DataSourceRDD from BatchScanExec")
    }

    // Log deduplication summary
    val allPoolSizes = Seq(
      schemaToPoolIndex.size,
      partitionTypeToPoolIndex.size,
      partitionSpecToPoolIndex.size,
      nameMappingToPoolIndex.size,
      projectFieldIdsToPoolIndex.size,
      partitionDataToPoolIndex.size,
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

    val partitionDataPoolBytes = commonBuilder.getPartitionDataPoolList.asScala
      .map(_.getSerializedSize)
      .sum

    logInfo(s"IcebergScan: $totalTasks tasks, ${allPoolSizes.size} pools ($avgDedup% avg dedup)")
    if (partitionDataToPoolIndex.nonEmpty) {
      logInfo(
        s"  Partition data pool: ${partitionDataToPoolIndex.size} unique values, " +
          s"$partitionDataPoolBytes bytes (protobuf)")
    }

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
