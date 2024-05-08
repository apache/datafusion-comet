package org.apache.spark.sql.comet.parquet

import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
import org.apache.spark.sql.types._

trait ShimCometParquetUtils {
  def foundDuplicateFieldInFieldIdLookupModeError(
      requiredId: Int,
      matchedFields: String): Throwable = {
    QueryExecutionErrors.foundDuplicateFieldInFieldIdLookupModeError(requiredId, matchedFields)
  }

  val FIELD_ID_METADATA_KEY = ParquetUtils.FIELD_ID_METADATA_KEY

  def hasFieldIds(schema: StructType): Boolean = ParquetUtils.hasFieldIds(schema)

  def hasFieldId(field: StructField): Boolean = ParquetUtils.hasFieldId(field)

  def getFieldId(field: StructField): Int = ParquetUtils.getFieldId (field)
}
