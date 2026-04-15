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

package org.apache.spark.sql.comet

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * The base trait for physical Comet operators.
 *
 * Wraps execution methods with error logging so that exceptions are logged with Comet-specific
 * context before Spark can obscure them with generic INTERNAL_ERROR messages.
 */
trait CometPlan extends SparkPlan {

  private def withErrorLogging[T](methodName: String)(body: => T): T = {
    try { body }
    catch {
      case e: Throwable =>
        logError(s"Error in Comet ${getClass.getSimpleName}.$methodName", e)
        throw e
    }
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] =
    withErrorLogging("doExecuteColumnar") { doExecuteCometColumnar() }

  override def doExecute(): RDD[InternalRow] =
    withErrorLogging("doExecute") { doExecuteComet() }

  override def doExecuteBroadcast[T](): Broadcast[T] =
    withErrorLogging("doExecuteBroadcast") { doExecuteCometBroadcast() }

  protected def doExecuteCometColumnar(): RDD[ColumnarBatch] =
    super.doExecuteColumnar()

  protected def doExecuteComet(): RDD[InternalRow]

  protected def doExecuteCometBroadcast[T](): Broadcast[T] =
    super.doExecuteBroadcast()
}
