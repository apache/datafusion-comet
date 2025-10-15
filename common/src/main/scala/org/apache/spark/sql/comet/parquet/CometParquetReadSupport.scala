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

package org.apache.spark.sql.comet.parquet

import java.util.{Locale, UUID}

import scala.jdk.CollectionConverters._

import org.apache.parquet.schema._
import org.apache.parquet.schema.LogicalTypeAnnotation.ListLogicalTypeAnnotation
import org.apache.parquet.schema.Type.Repetition
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
import org.apache.spark.sql.types._

/**
 * This class is copied & slightly modified from [[ParquetReadSupport]] in Spark. Changes:
 *   - This doesn't extend from Parquet's `ReadSupport` class since that is used for row-based
 *     Parquet reader. Therefore, there is no `init`, `prepareForRead` as well as other methods
 *     that are unused.
 */
object CometParquetReadSupport {
  val SPARK_PARQUET_SCHEMA_NAME = "spark_schema"

  val EMPTY_MESSAGE: MessageType =
    Types.buildMessage().named(SPARK_PARQUET_SCHEMA_NAME)

  def generateFakeColumnName: String = s"_fake_name_${UUID.randomUUID()}"

  def clipParquetSchema(
      parquetSchema: MessageType,
      catalystSchema: StructType,
      caseSensitive: Boolean,
      useFieldId: Boolean,
      ignoreMissingIds: Boolean): MessageType = {
    if (!ignoreMissingIds &&
      !containsFieldIds(parquetSchema) &&
      ParquetUtils.hasFieldIds(catalystSchema)) {
      throw new RuntimeException(
        "Spark read schema expects field Ids, " +
          "but Parquet file schema doesn't contain any field Ids.\n" +
          "Please remove the field ids from Spark schema or ignore missing ids by " +
          "setting `spark.sql.parquet.fieldId.read.ignoreMissing = true`\n" +
          s"""
             |Spark read schema:
             |${catalystSchema.prettyJson}
             |
             |Parquet file schema:
             |${parquetSchema.toString}
             |""".stripMargin)
    }
    clipParquetSchema(parquetSchema, catalystSchema, caseSensitive, useFieldId)
  }

  /**
   * Tailors `parquetSchema` according to `catalystSchema` by removing column paths don't exist in
   * `catalystSchema`, and adding those only exist in `catalystSchema`.
   */
  def clipParquetSchema(
      parquetSchema: MessageType,
      catalystSchema: StructType,
      caseSensitive: Boolean,
      useFieldId: Boolean): MessageType = {
    val clippedParquetFields = clipParquetGroupFields(
      parquetSchema.asGroupType(),
      catalystSchema,
      caseSensitive,
      useFieldId)
    if (clippedParquetFields.isEmpty) {
      EMPTY_MESSAGE
    } else {
      Types
        .buildMessage()
        .addFields(clippedParquetFields: _*)
        .named(SPARK_PARQUET_SCHEMA_NAME)
    }
  }

  private def clipParquetType(
      parquetType: Type,
      catalystType: DataType,
      caseSensitive: Boolean,
      useFieldId: Boolean): Type = {
    val newParquetType = catalystType match {
      case t: ArrayType if !isPrimitiveCatalystType(t.elementType) =>
        // Only clips array types with nested type as element type.
        clipParquetListType(parquetType.asGroupType(), t.elementType, caseSensitive, useFieldId)

      case t: MapType
          if !isPrimitiveCatalystType(t.keyType) ||
            !isPrimitiveCatalystType(t.valueType) =>
        // Only clips map types with nested key type or value type
        clipParquetMapType(
          parquetType.asGroupType(),
          t.keyType,
          t.valueType,
          caseSensitive,
          useFieldId)

      case t: StructType =>
        clipParquetGroup(parquetType.asGroupType(), t, caseSensitive, useFieldId)

      case _ =>
        // UDTs and primitive types are not clipped.  For UDTs, a clipped version might not be able
        // to be mapped to desired user-space types.  So UDTs shouldn't participate schema merging.
        parquetType
    }

    if (useFieldId && parquetType.getId != null) {
      newParquetType.withId(parquetType.getId.intValue())
    } else {
      newParquetType
    }
  }

  /**
   * Whether a Catalyst [[DataType]] is primitive. Primitive [[DataType]] is not equivalent to
   * [[AtomicType]]. For example, [[CalendarIntervalType]] is primitive, but it's not an
   * [[AtomicType]].
   */
  private def isPrimitiveCatalystType(dataType: DataType): Boolean = {
    dataType match {
      case _: ArrayType | _: MapType | _: StructType => false
      case _ => true
    }
  }

  /**
   * Clips a Parquet [[GroupType]] which corresponds to a Catalyst [[ArrayType]]. The element type
   * of the [[ArrayType]] should also be a nested type, namely an [[ArrayType]], a [[MapType]], or
   * a [[StructType]].
   */
  private def clipParquetListType(
      parquetList: GroupType,
      elementType: DataType,
      caseSensitive: Boolean,
      useFieldId: Boolean): Type = {
    // Precondition of this method, should only be called for lists with nested element types.
    assert(!isPrimitiveCatalystType(elementType))

    // Unannotated repeated group should be interpreted as required list of required element, so
    // list element type is just the group itself.  Clip it.
    if (parquetList.getLogicalTypeAnnotation == null &&
      parquetList.isRepetition(Repetition.REPEATED)) {
      clipParquetType(parquetList, elementType, caseSensitive, useFieldId)
    } else {
      assert(
        parquetList.getLogicalTypeAnnotation.isInstanceOf[ListLogicalTypeAnnotation],
        "Invalid Parquet schema. " +
          "Logical type annotation of annotated Parquet lists must be ListLogicalTypeAnnotation: " +
          parquetList.toString)

      assert(
        parquetList.getFieldCount == 1 && parquetList
          .getType(0)
          .isRepetition(Repetition.REPEATED),
        "Invalid Parquet schema. " +
          "LIST-annotated group should only have exactly one repeated field: " +
          parquetList)

      // Precondition of this method, should only be called for lists with nested element types.
      assert(!parquetList.getType(0).isPrimitive)

      val repeatedGroup = parquetList.getType(0).asGroupType()

      // If the repeated field is a group with multiple fields, or the repeated field is a group
      // with one field and is named either "array" or uses the LIST-annotated group's name with
      // "_tuple" appended then the repeated type is the element type and elements are required.
      // Build a new LIST-annotated group with clipped `repeatedGroup` as element type and the
      // only field.
      if (repeatedGroup.getFieldCount > 1 ||
        repeatedGroup.getName == "array" ||
        repeatedGroup.getName == parquetList.getName + "_tuple") {
        Types
          .buildGroup(parquetList.getRepetition)
          .as(LogicalTypeAnnotation.listType())
          .addField(clipParquetType(repeatedGroup, elementType, caseSensitive, useFieldId))
          .named(parquetList.getName)
      } else {
        val newRepeatedGroup = Types
          .repeatedGroup()
          .addField(
            clipParquetType(repeatedGroup.getType(0), elementType, caseSensitive, useFieldId))
          .named(repeatedGroup.getName)

        val newElementType = if (useFieldId && repeatedGroup.getId != null) {
          newRepeatedGroup.withId(repeatedGroup.getId.intValue())
        } else {
          newRepeatedGroup
        }

        // Otherwise, the repeated field's type is the element type with the repeated field's
        // repetition.
        Types
          .buildGroup(parquetList.getRepetition)
          .as(LogicalTypeAnnotation.listType())
          .addField(newElementType)
          .named(parquetList.getName)
      }
    }
  }

  /**
   * Clips a Parquet [[GroupType]] which corresponds to a Catalyst [[MapType]]. Either key type or
   * value type of the [[MapType]] must be a nested type, namely an [[ArrayType]], a [[MapType]],
   * or a [[StructType]].
   */
  private def clipParquetMapType(
      parquetMap: GroupType,
      keyType: DataType,
      valueType: DataType,
      caseSensitive: Boolean,
      useFieldId: Boolean): GroupType = {
    // Precondition of this method, only handles maps with nested key types or value types.
    assert(!isPrimitiveCatalystType(keyType) || !isPrimitiveCatalystType(valueType))

    val repeatedGroup = parquetMap.getType(0).asGroupType()
    val parquetKeyType = repeatedGroup.getType(0)
    val parquetValueType = repeatedGroup.getType(1)

    val clippedRepeatedGroup = {
      val newRepeatedGroup = Types
        .repeatedGroup()
        .as(repeatedGroup.getLogicalTypeAnnotation)
        .addField(clipParquetType(parquetKeyType, keyType, caseSensitive, useFieldId))
        .addField(clipParquetType(parquetValueType, valueType, caseSensitive, useFieldId))
        .named(repeatedGroup.getName)
      if (useFieldId && repeatedGroup.getId != null) {
        newRepeatedGroup.withId(repeatedGroup.getId.intValue())
      } else {
        newRepeatedGroup
      }
    }

    Types
      .buildGroup(parquetMap.getRepetition)
      .as(parquetMap.getLogicalTypeAnnotation)
      .addField(clippedRepeatedGroup)
      .named(parquetMap.getName)
  }

  /**
   * Clips a Parquet [[GroupType]] which corresponds to a Catalyst [[StructType]].
   *
   * @return
   *   A clipped [[GroupType]], which has at least one field.
   * @note
   *   Parquet doesn't allow creating empty [[GroupType]] instances except for empty
   *   [[MessageType]]. Because it's legal to construct an empty requested schema for column
   *   pruning.
   */
  private def clipParquetGroup(
      parquetRecord: GroupType,
      structType: StructType,
      caseSensitive: Boolean,
      useFieldId: Boolean): GroupType = {
    val clippedParquetFields =
      clipParquetGroupFields(parquetRecord, structType, caseSensitive, useFieldId)
    Types
      .buildGroup(parquetRecord.getRepetition)
      .as(parquetRecord.getLogicalTypeAnnotation)
      .addFields(clippedParquetFields: _*)
      .named(parquetRecord.getName)
  }

  /**
   * Clips a Parquet [[GroupType]] which corresponds to a Catalyst [[StructType]].
   *
   * @return
   *   A list of clipped [[GroupType]] fields, which can be empty.
   */
  private def clipParquetGroupFields(
      parquetRecord: GroupType,
      structType: StructType,
      caseSensitive: Boolean,
      useFieldId: Boolean): Seq[Type] = {
    val toParquet = new CometSparkToParquetSchemaConverter(
      writeLegacyParquetFormat = false,
      useFieldId = useFieldId)
    lazy val caseSensitiveParquetFieldMap =
      parquetRecord.getFields.asScala.map(f => f.getName -> f).toMap
    lazy val caseInsensitiveParquetFieldMap =
      parquetRecord.getFields.asScala.groupBy(_.getName.toLowerCase(Locale.ROOT))
    lazy val idToParquetFieldMap =
      parquetRecord.getFields.asScala.filter(_.getId != null).groupBy(f => f.getId.intValue())

    def matchCaseSensitiveField(f: StructField): Type = {
      caseSensitiveParquetFieldMap
        .get(f.name)
        .map(clipParquetType(_, f.dataType, caseSensitive, useFieldId))
        .getOrElse(toParquet.convertField(f))
    }

    def matchCaseInsensitiveField(f: StructField): Type = {
      // Do case-insensitive resolution only if in case-insensitive mode
      caseInsensitiveParquetFieldMap
        .get(f.name.toLowerCase(Locale.ROOT))
        .map { parquetTypes =>
          if (parquetTypes.size > 1) {
            // Need to fail if there is ambiguity, i.e. more than one field is matched
            val parquetTypesString = parquetTypes.map(_.getName).mkString("[", ", ", "]")
            throw QueryExecutionErrors.foundDuplicateFieldInCaseInsensitiveModeError(
              f.name,
              parquetTypesString)
          } else {
            clipParquetType(parquetTypes.head, f.dataType, caseSensitive, useFieldId)
          }
        }
        .getOrElse(toParquet.convertField(f))
    }

    def matchIdField(f: StructField): Type = {
      val fieldId = ParquetUtils.getFieldId(f)
      idToParquetFieldMap
        .get(fieldId)
        .map { parquetTypes =>
          if (parquetTypes.size > 1) {
            // Need to fail if there is ambiguity, i.e. more than one field is matched
            val parquetTypesString = parquetTypes.map(_.getName).mkString("[", ", ", "]")
            throw QueryExecutionErrors.foundDuplicateFieldInFieldIdLookupModeError(
              fieldId,
              parquetTypesString)
          } else {
            clipParquetType(parquetTypes.head, f.dataType, caseSensitive, useFieldId)
          }
        }
        .getOrElse {
          // When there is no ID match, we use a fake name to avoid a name match by accident
          // We need this name to be unique as well, otherwise there will be type conflicts
          toParquet.convertField(f.copy(name = generateFakeColumnName))
        }
    }

    val shouldMatchById = useFieldId && ParquetUtils.hasFieldIds(structType)
    structType.map { f =>
      if (shouldMatchById && ParquetUtils.hasFieldId(f)) {
        matchIdField(f)
      } else if (caseSensitive) {
        matchCaseSensitiveField(f)
      } else {
        matchCaseInsensitiveField(f)
      }
    }
  }

  /**
   * Whether the parquet schema contains any field IDs.
   */
  private def containsFieldIds(schema: Type): Boolean = schema match {
    case p: PrimitiveType => p.getId != null
    // We don't require all fields to have IDs, so we use `exists` here.
    case g: GroupType => g.getId != null || g.getFields.asScala.exists(containsFieldIds)
  }
}
