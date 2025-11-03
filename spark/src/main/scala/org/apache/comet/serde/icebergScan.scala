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

package org.apache.comet.serde

import scala.jdk.CollectionConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.comet.CometBatchScanExec
import org.apache.spark.sql.comet.CometIcebergNativeScanExec
import org.apache.spark.sql.types._

import org.apache.comet.objectstore.NativeConfig
import org.apache.comet.serde.OperatorOuterClass.{Operator, SparkStructField}
import org.apache.comet.serde.QueryPlanSerde.{exprToProto, serializeDataType}

/**
 * Serialization logic for Iceberg scan operators.
 */
object IcebergScanSerde extends Logging {

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
      if (exprClass.getName.endsWith("UnboundPredicate")) {
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
        if (attr.isEmpty) {
          return None
        }

        val opName = operation.toString

        opName match {
          case "IS_NULL" =>
            Some(IsNull(attr.get))

          case "IS_NOT_NULL" | "NOT_NULL" =>
            Some(IsNotNull(attr.get))

          case "EQ" =>
            val literalMethod = exprClass.getMethod("literal")
            val literal = literalMethod.invoke(icebergExpr)
            val value = convertIcebergLiteral(literal, attr.get.dataType)
            Some(EqualTo(attr.get, value))

          case "NOT_EQ" =>
            val literalMethod = exprClass.getMethod("literal")
            val literal = literalMethod.invoke(icebergExpr)
            val value = convertIcebergLiteral(literal, attr.get.dataType)
            Some(Not(EqualTo(attr.get, value)))

          case "LT" =>
            val literalMethod = exprClass.getMethod("literal")
            val literal = literalMethod.invoke(icebergExpr)
            val value = convertIcebergLiteral(literal, attr.get.dataType)
            Some(LessThan(attr.get, value))

          case "LT_EQ" =>
            val literalMethod = exprClass.getMethod("literal")
            val literal = literalMethod.invoke(icebergExpr)
            val value = convertIcebergLiteral(literal, attr.get.dataType)
            Some(LessThanOrEqual(attr.get, value))

          case "GT" =>
            val literalMethod = exprClass.getMethod("literal")
            val literal = literalMethod.invoke(icebergExpr)
            val value = convertIcebergLiteral(literal, attr.get.dataType)
            Some(GreaterThan(attr.get, value))

          case "GT_EQ" =>
            val literalMethod = exprClass.getMethod("literal")
            val literal = literalMethod.invoke(icebergExpr)
            val value = convertIcebergLiteral(literal, attr.get.dataType)
            Some(GreaterThanOrEqual(attr.get, value))

          case "IN" =>
            val literalsMethod = exprClass.getMethod("literals")
            val literals = literalsMethod.invoke(icebergExpr).asInstanceOf[java.util.List[_]]
            val values =
              literals.asScala.map(lit => convertIcebergLiteral(lit, attr.get.dataType))
            Some(In(attr.get, values.toSeq))

          case "NOT_IN" =>
            val literalsMethod = exprClass.getMethod("literals")
            val literals = literalsMethod.invoke(icebergExpr).asInstanceOf[java.util.List[_]]
            val values =
              literals.asScala.map(lit => convertIcebergLiteral(lit, attr.get.dataType))
            Some(Not(In(attr.get, values.toSeq)))

          case _ =>
            None
        }
      } else if (exprClass.getName.endsWith("And")) {
        val leftMethod = exprClass.getMethod("left")
        val rightMethod = exprClass.getMethod("right")
        val left = leftMethod.invoke(icebergExpr)
        val right = rightMethod.invoke(icebergExpr)

        (convertIcebergExpression(left, output), convertIcebergExpression(right, output)) match {
          case (Some(l), Some(r)) => Some(And(l, r))
          case _ => None
        }
      } else if (exprClass.getName.endsWith("Or")) {
        val leftMethod = exprClass.getMethod("left")
        val rightMethod = exprClass.getMethod("right")
        val left = leftMethod.invoke(icebergExpr)
        val right = rightMethod.invoke(icebergExpr)

        (convertIcebergExpression(left, output), convertIcebergExpression(right, output)) match {
          case (Some(l), Some(r)) => Some(Or(l, r))
          case _ => None
        }
      } else if (exprClass.getName.endsWith("Not")) {
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
    val literalClass = Class.forName("org.apache.iceberg.expressions.Literal")
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
   * Injects partition values into Iceberg schema JSON as "initial-default" values.
   *
   * For Hive-style partitioned tables migrated to Iceberg, partition values are stored in
   * directory structure, not in data files. This function adds those values to the schema so
   * iceberg-rust's RecordBatchTransformer can populate partition columns.
   */
  def injectPartitionValuesIntoSchemaJson(schemaJson: String, partitionJson: String): String = {
    import org.json4s._
    import org.json4s.jackson.JsonMethods._

    try {
      // Parse both JSONs
      implicit val formats: Formats = DefaultFormats
      val schemaValue = parse(schemaJson)
      val partitionMap = parse(partitionJson).extract[Map[String, JValue]]

      // Transform the schema fields to inject initial-default values
      val transformedSchema = schemaValue.transformField { case ("fields", JArray(fields)) =>
        val updatedFields = fields.map {
          case fieldObj: JObject =>
            // Check if this field has a partition value
            fieldObj \ "id" match {
              case JInt(fieldId) =>
                partitionMap.get(fieldId.toString) match {
                  case Some(partitionValue) =>
                    // Add "initial-default" to this field
                    fieldObj merge JObject("initial-default" -> partitionValue)
                  case None =>
                    fieldObj
                }
              case _ =>
                fieldObj
            }
          case other => other
        }
        ("fields", JArray(updatedFields))
      }

      // Serialize back to JSON
      compact(render(transformedSchema))
    } catch {
      case e: Exception =>
        logWarning(s"Failed to inject partition values into schema JSON: ${e.getMessage}")
        schemaJson
    }
  }

  /**
   * Serializes a CometBatchScanExec wrapping an Iceberg SparkBatchQueryScan to protobuf.
   *
   * This handles extraction of metadata location, catalog properties, file scan tasks, schemas,
   * partition data, delete files, and residual expressions from Iceberg scans.
   */
  def serializeIcebergScan(
      scan: CometBatchScanExec,
      builder: Operator.Builder): Option[OperatorOuterClass.Operator] = {
    val icebergScanBuilder = OperatorOuterClass.IcebergScan.newBuilder()

    // Extract metadata location for native execution
    val metadataLocation =
      try {
        CometIcebergNativeScanExec.extractMetadataLocation(scan.wrapped)
      } catch {
        case e: Exception =>
          logWarning(s"Failed to extract metadata location from Iceberg scan: ${e.getMessage}")
          return None
      }

    icebergScanBuilder.setMetadataLocation(metadataLocation)

    val catalogProperties =
      try {
        val session = org.apache.spark.sql.SparkSession.active
        val hadoopConf = session.sessionState.newHadoopConf()

        val metadataUri = new java.net.URI(metadataLocation)
        val hadoopS3Options =
          NativeConfig.extractObjectStoreOptions(hadoopConf, metadataUri)

        hadoopToIcebergS3Properties(hadoopS3Options)
      } catch {
        case e: Exception =>
          logWarning(s"Failed to extract catalog properties from Iceberg scan: ${e.getMessage}")
          e.printStackTrace()
          Map.empty[String, String]
      }
    catalogProperties.foreach { case (key, value) =>
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

    // For schema evolution support: extract the scan's expected schema to use for all tasks.
    // When reading old snapshots (VERSION AS OF) after schema changes (add/drop columns),
    // individual FileScanTasks may have inconsistent schemas - some with the snapshot schema,
    // others with the current table schema. By using the scan's expectedSchema() uniformly,
    // we ensure iceberg-rust reads all files with the correct snapshot schema.
    val globalNameToFieldId = scala.collection.mutable.Map[String, Int]()
    var scanSchemaForTasks: Option[Any] = None

    try {
      // expectedSchema() is a protected method in SparkScan that returns the Iceberg Schema
      // for this scan (which is the snapshot schema for VERSION AS OF queries).
      var scanClass: Class[_] = scan.wrapped.scan.getClass
      var schemaMethod: java.lang.reflect.Method = null

      // Search through class hierarchy to find expectedSchema()
      while (scanClass != null && schemaMethod == null) {
        try {
          schemaMethod = scanClass.getDeclaredMethod("expectedSchema")
          schemaMethod.setAccessible(true)
        } catch {
          case _: NoSuchMethodException => scanClass = scanClass.getSuperclass
        }
      }

      if (schemaMethod == null) {
        throw new NoSuchMethodException(
          "Could not find expectedSchema() method in class hierarchy")
      }

      val scanSchema = schemaMethod.invoke(scan.wrapped.scan)
      scanSchemaForTasks = Some(scanSchema)

      // Build a field ID mapping from the scan schema as a fallback.
      // This is needed when scan.output includes columns that aren't in some task schemas.
      val columnsMethod = scanSchema.getClass.getMethod("columns")
      val columns = columnsMethod.invoke(scanSchema).asInstanceOf[java.util.List[_]]

      columns.forEach { column =>
        try {
          val nameMethod = column.getClass.getMethod("name")
          val name = nameMethod.invoke(column).asInstanceOf[String]

          val fieldIdMethod = column.getClass.getMethod("fieldId")
          val fieldId = fieldIdMethod.invoke(column).asInstanceOf[Int]

          globalNameToFieldId(name) = fieldId
        } catch {
          case e: Exception =>
            logWarning(s"Failed to extract field ID from scan schema column: ${e.getMessage}")
        }
      }
    } catch {
      case e: Exception =>
        logWarning(s"Failed to extract scan schema for field ID mapping: ${e.getMessage}")
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
                      Class.forName("org.apache.iceberg.ContentScanTask")
                    val fileScanTaskClass = Class.forName("org.apache.iceberg.FileScanTask")
                    val contentFileClass = Class.forName("org.apache.iceberg.ContentFile")
                    // scalastyle:on classforname

                    val fileMethod = contentScanTaskClass.getMethod("file")
                    val dataFile = fileMethod.invoke(task)

                    val filePath =
                      try {
                        val locationMethod = contentFileClass.getMethod("location")
                        locationMethod.invoke(dataFile).asInstanceOf[String]
                      } catch {
                        case _: NoSuchMethodException =>
                          val pathMethod = contentFileClass.getMethod("path")
                          pathMethod.invoke(dataFile).asInstanceOf[CharSequence].toString
                      }
                    taskBuilder.setDataFilePath(filePath)

                    // Extract partition values for Hive-style partitioning
                    var partitionJsonOpt: Option[String] = None
                    try {
                      val partitionMethod = contentFileClass.getMethod("partition")
                      val partitionStruct = partitionMethod.invoke(dataFile)

                      if (partitionStruct != null) {
                        // scalastyle:off classforname
                        val structLikeClass = Class.forName("org.apache.iceberg.StructLike")
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
                            Class.forName("org.apache.iceberg.PartitionScanTask")
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
                              val isIdentity = transform.toString == "identity"

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

                      val deletesMethod = fileScanTaskClass.getMethod("deletes")
                      val deletes = deletesMethod
                        .invoke(task)
                        .asInstanceOf[java.util.List[_]]
                      val hasDeletes = !deletes.isEmpty

                      val schema: AnyRef =
                        if (hasDeletes) {
                          taskSchema
                        } else {
                          scanSchemaForTasks.map(_.asInstanceOf[AnyRef]).getOrElse(taskSchema)
                        }

                      // scalastyle:off classforname
                      val schemaParserClass = Class.forName("org.apache.iceberg.SchemaParser")
                      val schemaClass = Class.forName("org.apache.iceberg.Schema")
                      // scalastyle:on classforname
                      val toJsonMethod = schemaParserClass.getMethod("toJson", schemaClass)
                      toJsonMethod.setAccessible(true)
                      var schemaJson = toJsonMethod.invoke(null, schema).asInstanceOf[String]

                      // Inject partition values into schema if present
                      partitionJsonOpt.foreach { partitionJson =>
                        schemaJson =
                          injectPartitionValuesIntoSchemaJson(schemaJson, partitionJson)
                      }

                      taskBuilder.setSchemaJson(schemaJson)

                      // Build field ID mapping from the schema we're using
                      val columnsMethod = schema.getClass.getMethod("columns")
                      val columns =
                        columnsMethod.invoke(schema).asInstanceOf[java.util.List[_]]

                      val nameToFieldId = scala.collection.mutable.Map[String, Int]()
                      columns.forEach { column =>
                        try {
                          val nameMethod = column.getClass.getMethod("name")
                          val name = nameMethod.invoke(column).asInstanceOf[String]

                          val fieldIdMethod = column.getClass.getMethod("fieldId")
                          val fieldId = fieldIdMethod.invoke(column).asInstanceOf[Int]

                          nameToFieldId(name) = fieldId
                        } catch {
                          case e: Exception =>
                            logWarning(s"Failed to extract field ID from column: ${e.getMessage}")
                        }
                      }

                      // Extract project_field_ids for scan.output columns.
                      // For schema evolution: try task schema first, then fall back to
                      // global scan schema.
                      scan.output.foreach { attr =>
                        val fieldId = nameToFieldId
                          .get(attr.name)
                          .orElse(globalNameToFieldId.get(attr.name))

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
                        logWarning(s"Failed to extract schema from FileScanTask: ${e.getMessage}")
                    }

                    try {
                      val formatMethod = contentFileClass.getMethod("format")
                      val format = formatMethod.invoke(dataFile)
                      taskBuilder.setDataFileFormat(format.toString)
                    } catch {
                      case e: Exception =>
                        logWarning(
                          "Failed to extract file format from FileScanTask," +
                            s"defaulting to PARQUET: ${e.getMessage}")
                        taskBuilder.setDataFileFormat("PARQUET")
                    }

                    try {
                      val deletesMethod = fileScanTaskClass.getMethod("deletes")
                      val deletes = deletesMethod
                        .invoke(task)
                        .asInstanceOf[java.util.List[_]]

                      deletes.asScala.foreach { deleteFile =>
                        try {
                          // scalastyle:off classforname
                          val deleteFileClass = Class.forName("org.apache.iceberg.DeleteFile")
                          // scalastyle:on classforname

                          val deletePath =
                            try {
                              val locationMethod = contentFileClass.getMethod("location")
                              locationMethod.invoke(deleteFile).asInstanceOf[String]
                            } catch {
                              case _: NoSuchMethodException =>
                                val pathMethod = contentFileClass.getMethod("path")
                                pathMethod
                                  .invoke(deleteFile)
                                  .asInstanceOf[CharSequence]
                                  .toString
                            }

                          val deleteBuilder =
                            OperatorOuterClass.IcebergDeleteFile.newBuilder()
                          deleteBuilder.setFilePath(deletePath)

                          val contentType =
                            try {
                              val contentMethod = deleteFileClass.getMethod("content")
                              val content = contentMethod.invoke(deleteFile)
                              content.toString match {
                                case "POSITION_DELETES" => "POSITION_DELETES"
                                case "EQUALITY_DELETES" => "EQUALITY_DELETES"
                                case other => other
                              }
                            } catch {
                              case _: Exception =>
                                "POSITION_DELETES"
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
                        } catch {
                          case e: Exception =>
                            logWarning(s"Failed to serialize delete file: ${e.getMessage}")
                        }
                      }
                    } catch {
                      case e: Exception =>
                        logWarning(
                          s"Failed to extract deletes from FileScanTask: ${e.getMessage}")
                    }

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

                    // Extract partition data and spec ID for proper constant identification
                    try {
                      // Get partition spec from the task first
                      val specMethod = fileScanTaskClass.getMethod("spec")
                      val spec = specMethod.invoke(task)

                      if (spec != null) {
                        val specIdMethod = spec.getClass.getMethod("specId")
                        val specId = specIdMethod.invoke(spec).asInstanceOf[Int]
                        taskBuilder.setPartitionSpecId(specId)

                        // Serialize the entire PartitionSpec to JSON
                        try {
                          // scalastyle:off classforname
                          val partitionSpecParserClass =
                            Class.forName("org.apache.iceberg.PartitionSpecParser")
                          val toJsonMethod = partitionSpecParserClass.getMethod(
                            "toJson",
                            Class.forName("org.apache.iceberg.PartitionSpec"))
                          // scalastyle:on classforname
                          val partitionSpecJson = toJsonMethod
                            .invoke(null, spec)
                            .asInstanceOf[String]
                          taskBuilder.setPartitionSpecJson(partitionSpecJson)
                        } catch {
                          case e: Exception =>
                            logWarning(
                              s"Failed to serialize partition spec to JSON: ${e.getMessage}")
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

                            // Only serialize partition type if there are actual partition fields
                          if (!fields.isEmpty) {
                            // Serialize partition type to JSON using Iceberg's SchemaParser
                            try {
                              // scalastyle:off classforname
                              val jsonUtilClass = Class.forName("org.apache.iceberg.util.JsonUtil")
                              val factoryMethod = jsonUtilClass.getMethod("factory")
                              val factory = factoryMethod.invoke(null)

                              val writer = new java.io.StringWriter()
                              val createGeneratorMethod =
                                factory.getClass.getMethod(
                                  "createGenerator",
                                  classOf[java.io.Writer])
                              val generator = createGeneratorMethod.invoke(factory, writer)

                              val schemaParserClass =
                                Class.forName("org.apache.iceberg.SchemaParser")
                              val typeClass = Class.forName("org.apache.iceberg.types.Type")
                              // Use shaded Jackson class from Iceberg
                              val generatorClass = Class.forName(
                                "org.apache.iceberg.shaded.com.fasterxml.jackson.core.JsonGenerator")
                              val toJsonMethod = schemaParserClass.getDeclaredMethod(
                                "toJson",
                                typeClass,
                                generatorClass)
                              // scalastyle:on classforname

                              toJsonMethod.setAccessible(true)
                              toJsonMethod.invoke(null, partitionType, generator)

                              // Close the generator to ensure all data is written
                              val closeMethod = generator.getClass.getMethod("close")
                              closeMethod.invoke(generator)
                              val partitionTypeJson = writer.toString
                              taskBuilder.setPartitionTypeJson(partitionTypeJson)
                            } catch {
                              case e: Exception =>
                                logWarning(
                                  s"Failed to serialize partition type to JSON: ${e.getMessage}")
                            }
                          } else {
                            // No partition fields to serialize (unpartitioned table or all non-identity transforms)
                          }

                          // Serialize partition data to JSON using Iceberg's StructLike
                          val jsonBuilder = new StringBuilder()
                          jsonBuilder.append("{")

                          var first = true
                          val iter = fields.iterator()
                          var idx = 0
                          while (iter.hasNext) {
                            val field = iter.next()
                            val fieldIdMethod = field.getClass.getMethod("fieldId")
                            val fieldId = fieldIdMethod.invoke(field).asInstanceOf[Int]

                            val getMethod = partitionData.getClass.getMethod(
                              "get",
                              classOf[Int],
                              classOf[Class[_]])
                            val value = getMethod.invoke(
                              partitionData,
                              Integer.valueOf(idx),
                              classOf[Object])

                            if (!first) jsonBuilder.append(",")
                            first = false

                            jsonBuilder.append("\"").append(fieldId.toString).append("\":")
                            if (value == null) {
                              jsonBuilder.append("null")
                            } else {
                              value match {
                                case s: String =>
                                  jsonBuilder.append("\"").append(s).append("\"")
                                case n: Number =>
                                  jsonBuilder.append(n.toString)
                                case b: java.lang.Boolean =>
                                  jsonBuilder.append(b.toString)
                                case _ =>
                                  jsonBuilder.append("\"").append(value.toString).append("\"")
                              }
                            }

                            idx += 1
                          }

                          jsonBuilder.append("}")
                          val partitionJson = jsonBuilder.toString()
                          taskBuilder.setPartitionDataJson(partitionJson)
                        }
                      }
                    } catch {
                      case e: Exception =>
                        logWarning(
                          "Failed to extract partition data from FileScanTask: " +
                            s"${e.getMessage}")
                        e.printStackTrace()
                    }

                    partitionBuilder.addFileScanTasks(taskBuilder.build())
                  } catch {
                    case e: Exception =>
                      logWarning(s"Failed to serialize FileScanTask: ${e.getMessage}")
                  }
                }
              } catch {
                case e: Exception =>
                  logWarning(
                    s"Failed to extract FileScanTasks from InputPartition: ${e.getMessage}")
              }
            }

            val builtPartition = partitionBuilder.build()
            icebergScanBuilder.addFilePartitions(builtPartition)
          }
        case _ =>
      }
    } catch {
      case e: Exception =>
        logWarning(s"Failed to extract FileScanTasks from Iceberg scan RDD: ${e.getMessage}")
    }

    builder.clearChildren()
    Some(builder.setIcebergScan(icebergScanBuilder).build())
  }
}
