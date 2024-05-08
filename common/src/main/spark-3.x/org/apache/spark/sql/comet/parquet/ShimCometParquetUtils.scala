package org.apache.spark.sql.comet.parquet

import org.apache.spark.sql.types._

trait ShimCometParquetUtils {
  // The following is copied from QueryExecutionErrors
  // TODO: remove after dropping Spark 3.2.0 support and directly use
  //       QueryExecutionErrors.foundDuplicateFieldInFieldIdLookupModeError
  def foundDuplicateFieldInFieldIdLookupModeError(
      requiredId: Int,
      matchedFields: String): Throwable = {
    new RuntimeException(s"""
         |Found duplicate field(s) "$requiredId": $matchedFields
         |in id mapping mode
     """.stripMargin.replaceAll("\n", " "))
  }

  // The followings are copied from org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
  // TODO: remove after dropping Spark 3.2.0 support and directly use ParquetUtils
  /**
   * A StructField metadata key used to set the field id of a column in the Parquet schema.
   */
  val FIELD_ID_METADATA_KEY = "parquet.field.id"

  /**
   * Whether there exists a field in the schema, whether inner or leaf, has the parquet field ID
   * metadata.
   */
  def hasFieldIds(schema: StructType): Boolean = {
    def recursiveCheck(schema: DataType): Boolean = {
      schema match {
        case st: StructType =>
          st.exists(field => hasFieldId(field) || recursiveCheck(field.dataType))

        case at: ArrayType => recursiveCheck(at.elementType)

        case mt: MapType => recursiveCheck(mt.keyType) || recursiveCheck(mt.valueType)

        case _ =>
          // No need to really check primitive types, just to terminate the recursion
          false
      }
    }
    if (schema.isEmpty) false else recursiveCheck(schema)
  }

  def hasFieldId(field: StructField): Boolean =
    field.metadata.contains(FIELD_ID_METADATA_KEY)

  def getFieldId(field: StructField): Int = {
    require(
      hasFieldId(field),
      s"The key `$FIELD_ID_METADATA_KEY` doesn't exist in the metadata of " + field)
    try {
      Math.toIntExact(field.metadata.getLong(FIELD_ID_METADATA_KEY))
    } catch {
      case _: ArithmeticException | _: ClassCastException =>
        throw new IllegalArgumentException(
          s"The key `$FIELD_ID_METADATA_KEY` must be a 32-bit integer")
    }
  }
}
