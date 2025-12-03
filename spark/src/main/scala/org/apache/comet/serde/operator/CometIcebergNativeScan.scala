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

import scala.jdk.CollectionConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.comet.{CometBatchScanExec, CometNativeExec}
import org.apache.spark.sql.types._

import org.apache.comet.ConfigEntry
import org.apache.comet.iceberg.IcebergReflection
import org.apache.comet.serde.{CometOperatorSerde, OperatorOuterClass}
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

  /**
   * Serializes delete files from an Iceberg FileScanTask.
   *
   * Extracts delete files (position deletes and equality deletes) from a FileScanTask and adds
   * them to the task builder. Delete files are used for Iceberg's merge-on-read approach where
   * updates and deletes are stored separately from data files.
   */
  private def serializeDeleteFiles(
      task: Any,
      contentFileClass: Class[_],
      fileScanTaskClass: Class[_],
      taskBuilder: OperatorOuterClass.IcebergFileScanTask.Builder): Unit = {
    try {
      // scalastyle:off classforname
      val deleteFileClass = Class.forName(IcebergReflection.ClassNames.DELETE_FILE)
      // scalastyle:on classforname

      val deletes = IcebergReflection.getDeleteFilesFromTask(task, fileScanTaskClass)

      deletes.asScala.foreach { deleteFile =>
        try {
          IcebergReflection
            .extractFileLocation(contentFileClass, deleteFile)
            .foreach { deletePath =>
              val deleteBuilder =
                OperatorOuterClass.IcebergDeleteFile.newBuilder()
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
                  case _: Exception =>
                    0
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

              taskBuilder.addDeleteFiles(deleteBuilder.build())
            }
        } catch {
          case e: Exception =>
            logWarning(s"Failed to serialize delete file: ${e.getMessage}")
        }
      }
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
      taskBuilder: OperatorOuterClass.IcebergFileScanTask.Builder): Unit = {
    try {
      val specMethod = fileScanTaskClass.getMethod("spec")
      val spec = specMethod.invoke(task)

      if (spec != null) {
        // Serialize the entire PartitionSpec to JSON (includes spec-id)
        try {
          // scalastyle:off classforname
          val partitionSpecParserClass =
            Class.forName(IcebergReflection.ClassNames.PARTITION_SPEC_PARSER)
          val toJsonMethod = partitionSpecParserClass.getMethod(
            "toJson",
            Class.forName(IcebergReflection.ClassNames.PARTITION_SPEC))
          // scalastyle:on classforname
          val partitionSpecJson = toJsonMethod
            .invoke(null, spec)
            .asInstanceOf[String]
          taskBuilder.setPartitionSpecJson(partitionSpecJson)
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
              import org.json4s.JsonDSL._
              import org.json4s.jackson.JsonMethods._

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

                taskBuilder.setPartitionTypeJson(partitionTypeJson)
              }
            } catch {
              case e: Exception =>
                logWarning(s"Failed to serialize partition type to JSON: ${e.getMessage}")
            }
          }

          // Serialize partition data to JSON for iceberg-rust's constants_map.
          // The native execution engine uses partition_data_json +
          // partition_type_json to build a constants_map, which is the primary
          // mechanism for providing partition values to identity-transformed
          // partition columns. Non-identity transforms (bucket, truncate, days,
          // etc.) read values from data files.
          import org.json4s._
          import org.json4s.jackson.JsonMethods._

          // Filter out fields with unknown type (same as partition type filtering)
          val partitionDataMap: Map[String, JValue] =
            fields.asScala.zipWithIndex.flatMap { case (field, idx) =>
              val fieldTypeStr = getFieldType(field)

              // Skip fields with unknown type (dropped partition columns)
              if (fieldTypeStr == IcebergReflection.TypeNames.UNKNOWN) {
                None
              } else {
                val fieldIdMethod = field.getClass.getMethod("fieldId")
                val fieldId = fieldIdMethod.invoke(field).asInstanceOf[Int]

                val getMethod =
                  partitionData.getClass.getMethod("get", classOf[Int], classOf[Class[_]])
                val value = getMethod.invoke(partitionData, Integer.valueOf(idx), classOf[Object])

                val jsonValue: JValue = if (value == null) {
                  JNull
                } else {
                  value match {
                    case s: String => JString(s)
                    // NaN/Infinity are not valid JSON number literals per the
                    // JSON spec. Serialize as strings (e.g., "NaN", "Infinity")
                    // which are valid JSON and can be parsed by Rust's
                    // f32/f64::from_str().
                    case f: java.lang.Float if f.isNaN || f.isInfinite =>
                      JString(f.toString)
                    case d: java.lang.Double if d.isNaN || d.isInfinite =>
                      JString(d.toString)
                    case n: Number => JDecimal(BigDecimal(n.toString))
                    case b: java.lang.Boolean =>
                      JBool(b.booleanValue())
                    case other => JString(other.toString)
                  }
                }

                Some(fieldId.toString -> jsonValue)
              }
            }.toMap

          // Only serialize partition data if we have non-unknown fields
          if (partitionDataMap.nonEmpty) {
            val partitionJson = compact(render(JObject(partitionDataMap.toList)))
            taskBuilder.setPartitionDataJson(partitionJson)
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
   * Serializes a CometBatchScanExec wrapping an Iceberg SparkBatchQueryScan to protobuf.
   *
   * Uses pre-extracted metadata from CometScanRule to avoid redundant reflection operations. All
   * reflection and validation was done during planning, so serialization failures here would
   * indicate a programming error rather than an expected fallback condition.
   */
  override def convert(
      scan: CometBatchScanExec,
      builder: Operator.Builder,
      childOp: Operator*): Option[OperatorOuterClass.Operator] = {
    val icebergScanBuilder = OperatorOuterClass.IcebergScan.newBuilder()

    // Get pre-extracted metadata from planning phase
    // If metadata is None, this is a programming error - metadata should have been extracted
    // in CometScanRule before creating CometBatchScanExec
    val metadata = scan.nativeIcebergScanMetadata.getOrElse {
      logError(
        "Programming error: CometBatchScanExec.nativeIcebergScanMetadata is None. " +
          "Metadata should have been extracted in CometScanRule.")
      return None
    }

    // Use pre-extracted metadata (no reflection needed)
    icebergScanBuilder.setMetadataLocation(metadata.metadataLocation)

    metadata.catalogProperties.foreach { case (key, value) =>
      icebergScanBuilder.putCatalogProperties(key, value)
    }

    // Set required_schema from output
    scan.output.foreach { attr =>
      val field = SparkStructField
        .newBuilder()
        .setName(attr.name)
        .setNullable(attr.nullable)
      serializeDataType(attr.dataType).foreach(field.setDataType)
      icebergScanBuilder.addRequiredSchema(field.build())
    }

    // Extract FileScanTasks from the InputPartitions in the RDD
    try {
      scan.wrapped.inputRDD match {
        case rdd: org.apache.spark.sql.execution.datasources.v2.DataSourceRDD =>
          val partitions = rdd.partitions
          partitions.foreach { partition =>
            val partitionBuilder = OperatorOuterClass.IcebergFilePartition.newBuilder()

            val inputPartitions = partition
              .asInstanceOf[org.apache.spark.sql.execution.datasources.v2.DataSourceRDDPartition]
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
                  try {
                    val taskBuilder = OperatorOuterClass.IcebergFileScanTask.newBuilder()

                    // scalastyle:off classforname
                    val contentScanTaskClass =
                      Class.forName(IcebergReflection.ClassNames.CONTENT_SCAN_TASK)
                    val fileScanTaskClass =
                      Class.forName(IcebergReflection.ClassNames.FILE_SCAN_TASK)
                    val contentFileClass =
                      Class.forName(IcebergReflection.ClassNames.CONTENT_FILE)
                    // scalastyle:on classforname

                    val fileMethod = contentScanTaskClass.getMethod("file")
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

                    // Extract partition values for Hive-style partitioning
                    var partitionJsonOpt: Option[String] = None
                    try {
                      val partitionMethod = contentFileClass.getMethod("partition")
                      val partitionStruct = partitionMethod.invoke(dataFile)

                      if (partitionStruct != null) {
                        // scalastyle:off classforname
                        val structLikeClass =
                          Class.forName(IcebergReflection.ClassNames.STRUCT_LIKE)
                        // scalastyle:on classforname
                        val sizeMethod = structLikeClass.getMethod("size")
                        val getMethod =
                          structLikeClass.getMethod("get", classOf[Int], classOf[Class[_]])

                        val partitionSize =
                          sizeMethod.invoke(partitionStruct).asInstanceOf[Int]

                        if (partitionSize > 0) {
                          // Get the partition spec directly from the task
                          // scalastyle:off classforname
                          val partitionScanTaskClass =
                            Class.forName(IcebergReflection.ClassNames.PARTITION_SCAN_TASK)
                          // scalastyle:on classforname
                          val specMethod = partitionScanTaskClass.getMethod("spec")
                          val partitionSpec = specMethod.invoke(task)

                          // Build JSON representation of partition values using json4s
                          import org.json4s._
                          import org.json4s.jackson.JsonMethods._

                          val partitionMap = scala.collection.mutable.Map[String, JValue]()

                          if (partitionSpec != null) {
                            // Get the list of partition fields from the spec
                            val fieldsMethod = partitionSpec.getClass.getMethod("fields")
                            val fields = fieldsMethod
                              .invoke(partitionSpec)
                              .asInstanceOf[java.util.List[_]]

                            for (i <- 0 until partitionSize) {
                              val value =
                                getMethod.invoke(partitionStruct, Int.box(i), classOf[Object])

                              // Get the partition field and check its transform type
                              val partitionField = fields.get(i)

                              // Only inject partition values for IDENTITY transforms
                              val transformMethod =
                                partitionField.getClass.getMethod("transform")
                              val transform = transformMethod.invoke(partitionField)
                              val isIdentity =
                                transform.toString == IcebergReflection.Transforms.IDENTITY

                              if (isIdentity) {
                                // Get the source field ID
                                val sourceIdMethod =
                                  partitionField.getClass.getMethod("sourceId")
                                val sourceFieldId =
                                  sourceIdMethod.invoke(partitionField).asInstanceOf[Int]

                                // Convert value to appropriate JValue type
                                val jsonValue: JValue = if (value == null) {
                                  JNull
                                } else {
                                  value match {
                                    case s: String => JString(s)
                                    case i: java.lang.Integer => JInt(BigInt(i.intValue()))
                                    case l: java.lang.Long => JInt(BigInt(l.longValue()))
                                    case d: java.lang.Double => JDouble(d.doubleValue())
                                    case f: java.lang.Float => JDouble(f.doubleValue())
                                    case b: java.lang.Boolean => JBool(b.booleanValue())
                                    case n: Number => JDecimal(BigDecimal(n.toString))
                                    case other => JString(other.toString)
                                  }
                                }

                                partitionMap(sourceFieldId.toString) = jsonValue
                              }
                            }
                          }

                          val partitionJson = compact(render(JObject(partitionMap.toList)))
                          partitionJsonOpt = Some(partitionJson)
                        }
                      }
                    } catch {
                      case e: Exception =>
                        logWarning(
                          s"Failed to extract partition values from DataFile: ${e.getMessage}")
                    }

                    val startMethod = contentScanTaskClass.getMethod("start")
                    val start = startMethod.invoke(task).asInstanceOf[Long]
                    taskBuilder.setStart(start)

                    val lengthMethod = contentScanTaskClass.getMethod("length")
                    val length = lengthMethod.invoke(task).asInstanceOf[Long]
                    taskBuilder.setLength(length)

                    try {
                      // Equality deletes require the full table schema to resolve field IDs,
                      // even for columns not in the projection. Schema evolution requires
                      // using the snapshot's schema to correctly read old data files.
                      // These requirements conflict, so we choose based on delete presence.

                      val taskSchemaMethod = fileScanTaskClass.getMethod("schema")
                      val taskSchema = taskSchemaMethod.invoke(task)

                      val deletes =
                        IcebergReflection.getDeleteFilesFromTask(task, fileScanTaskClass)
                      val hasDeletes = !deletes.isEmpty

                      // Use pre-extracted scanSchema for schema evolution support
                      val schema: AnyRef =
                        if (hasDeletes) {
                          taskSchema
                        } else {
                          metadata.scanSchema.asInstanceOf[AnyRef]
                        }

                      // scalastyle:off classforname
                      val schemaParserClass =
                        Class.forName(IcebergReflection.ClassNames.SCHEMA_PARSER)
                      val schemaClass = Class.forName(IcebergReflection.ClassNames.SCHEMA)
                      // scalastyle:on classforname
                      val toJsonMethod = schemaParserClass.getMethod("toJson", schemaClass)
                      toJsonMethod.setAccessible(true)
                      val schemaJson = toJsonMethod.invoke(null, schema).asInstanceOf[String]

                      taskBuilder.setSchemaJson(schemaJson)

                      // Build field ID mapping from the schema we're using
                      val nameToFieldId = IcebergReflection.buildFieldIdMapping(schema)

                      // Extract project_field_ids for scan.output columns.
                      // For schema evolution: try task schema first, then fall back to
                      // global scan schema (pre-extracted in metadata).
                      scan.output.foreach { attr =>
                        val fieldId = nameToFieldId
                          .get(attr.name)
                          .orElse(metadata.globalFieldIdMapping.get(attr.name))

                        fieldId match {
                          case Some(id) =>
                            taskBuilder.addProjectFieldIds(id)
                          case None =>
                            logWarning(
                              s"Column '${attr.name}' not found in task or scan schema," +
                                "skipping projection")
                        }
                      }
                    } catch {
                      case e: Exception =>
                        val msg =
                          "Iceberg reflection failure: " +
                            "Failed to extract schema from FileScanTask: " +
                            s"${e.getMessage}"
                        logError(msg)
                        throw new RuntimeException(msg, e)
                    }

                    taskBuilder.setDataFileFormat(metadata.fileFormat)

                    // Serialize delete files (position deletes and equality deletes)
                    serializeDeleteFiles(task, contentFileClass, fileScanTaskClass, taskBuilder)

                    try {
                      val residualMethod = contentScanTaskClass.getMethod("residual")
                      val residualExpr = residualMethod.invoke(task)

                      val catalystExpr = convertIcebergExpression(residualExpr, scan.output)

                      catalystExpr
                        .flatMap { expr =>
                          exprToProto(expr, scan.output, binding = false)
                        }
                        .foreach { protoExpr =>
                          taskBuilder.setResidual(protoExpr)
                        }
                    } catch {
                      case e: Exception =>
                        logWarning(
                          "Failed to extract residual expression from FileScanTask: " +
                            s"${e.getMessage}")
                    }

                    // Serialize partition spec and data (field definitions, transforms, values)
                    serializePartitionData(
                      task,
                      contentScanTaskClass,
                      fileScanTaskClass,
                      taskBuilder)

                    // Set name mapping if available (shared by all tasks, pre-extracted)
                    metadata.nameMapping.foreach(taskBuilder.setNameMappingJson)

                    partitionBuilder.addFileScanTasks(taskBuilder.build())
                  }
                }
              }
            }

            val builtPartition = partitionBuilder.build()
            icebergScanBuilder.addFilePartitions(builtPartition)
          }
        case _ =>
      }
    } catch {
      case e: Exception =>
        val msg =
          "Iceberg reflection failure: Failed to extract FileScanTasks from Iceberg scan RDD: " +
            s"${e.getMessage}"
        logError(msg, e)
        return None
    }

    builder.clearChildren()
    Some(builder.setIcebergScan(icebergScanBuilder).build())
  }

  override def createExec(nativeOp: Operator, op: CometBatchScanExec): CometNativeExec = {
    import org.apache.spark.sql.comet.CometIcebergNativeScanExec

    // Extract metadata - it must be present at this point
    val metadata = op.nativeIcebergScanMetadata.getOrElse {
      throw new IllegalStateException(
        "Programming error: CometBatchScanExec.nativeIcebergScanMetadata is None. " +
          "Metadata should have been extracted in CometScanRule.")
    }

    // Extract metadataLocation from the native operator
    val metadataLocation = nativeOp.getIcebergScan.getMetadataLocation

    // Create the CometIcebergNativeScanExec using the companion object's apply method
    CometIcebergNativeScanExec(nativeOp, op.wrapped, op.session, metadataLocation, metadata)
  }
}
