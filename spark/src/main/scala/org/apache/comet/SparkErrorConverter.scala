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

package org.apache.comet

import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.{QueryContext, SparkException}
import org.apache.spark.sql.catalyst.trees.SQLQueryContext
import org.apache.spark.sql.comet.shims.ShimSparkErrorConverter

import com.fasterxml.jackson.core.JsonParseException

import org.apache.comet.exceptions.CometQueryExecutionException

/**
 * Converts CometQueryExecutionException from native code (with JSON payload) to appropriate Spark
 * QueryExecutionErrors.* exceptions
 *
 * Parses the JSON-encoded error information from native execution and delegates to the
 * version-specific ShimSparkErrorConverter trait for conversion to proper Spark exception types.
 *
 * The ShimSparkErrorConverter handles all error cases using the correct QueryExecutionErrors API
 * for each Spark version.
 */
object SparkErrorConverter extends ShimSparkErrorConverter {

  implicit val formats: DefaultFormats.type = DefaultFormats

  case class QueryContextJson(
      sqlText: String,
      startIndex: Int,
      stopIndex: Int,
      objectType: Option[String],
      objectName: Option[String],
      line: Int,
      startPosition: Int)

  case class ErrorJson(
      errorType: String,
      errorClass: Option[String],
      params: Option[Map[String, Any]],
      context: Option[QueryContextJson],
      summary: Option[String])

  /**
   * Parse JSON from exception and convert to appropriate Spark exception.
   *
   * @param e
   *   the CometQueryExecutionException with JSON message
   * @return
   *   the corresponding Spark exception, or the original exception if parsing fails
   */
  def convertToSparkException(e: CometQueryExecutionException): Throwable = {
    try {
      if (!e.isJsonMessage()) {
        // Not JSON, return original exception
        return e
      }
    } catch {
      // Only catch JSON parsing/mapping exceptions - let conversion exceptions propagate
      case _: MappingException | _: JsonParseException =>
        return e
    }

    val json = parse(e.getMessage)
    val errorJson = json.extract[ErrorJson]
    val params = errorJson.params.getOrElse(Map.empty)
    val errorClass = errorJson.errorClass.getOrElse("UNKNOWN_ERROR_TEMP_COMET")

    // Build Spark SQLQueryContext if context is present (Not all errors carry the query context)
    val sparkContext: Array[QueryContext] = errorJson.context match {
      case Some(ctx) =>
        Array(
          SQLQueryContext(
            sqlText = Some(ctx.sqlText),
            line = Some(ctx.line),
            startPosition = Some(ctx.startPosition),
            originStartIndex = Some(ctx.startIndex),
            originStopIndex = Some(ctx.stopIndex),
            originObjectType = ctx.objectType,
            originObjectName = ctx.objectName))
      case None => Array.empty[QueryContext] // No context
    }

    val summary: String = errorJson.summary.orNull

    // Delegate to version-specific shim - let conversion exceptions propagate
    val optEx = convertErrorType(errorJson.errorType, errorClass, params, sparkContext, summary)
    optEx match {
      case Some(exception) =>
        // successfully converted - return the proper typed exception
        exception

      case None =>
        // Unknown error type - fallback to generic SparkException
        new SparkException(
          errorClass = errorClass,
          messageParameters = paramsToStringMap(params),
          cause = null)
    }
  }

  /**
   * Convert parameter map to string-keyed map for SparkException.
   */
  private[comet] def paramsToStringMap(params: Map[String, Any]): Map[String, String] = {
    params.map { case (k, v) => (k, v.toString) }
  }
}
