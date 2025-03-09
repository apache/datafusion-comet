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

package org.apache.comet.shims

import java.lang.reflect.InvocationTargetException

import scala.collection.mutable.Buffer

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.util.AccumulatorV2

object ShimBatchReader {

  def newPartitionedFile(partitionValues: InternalRow, file: String): PartitionedFile =
    PartitionedFile(
      partitionValues,
      SparkPath.fromPathString(file),
      -1, // -1 means we read the entire file
      -1,
      Array.empty[String],
      0,
      0)

  // Signature of externalAccums changed from returning a Buffer to returning a Seq. If comet is
  // expecting a Buffer but the Spark version returns a Seq or vice versa, we get a
  // method not found exception.
  def getTaskAccumulator(taskMetrics: TaskMetrics): Option[AccumulatorV2[_, _]] = {
    try {
      val externalAccumsMethod = classOf[TaskMetrics].getDeclaredMethod("externalAccums")
      externalAccumsMethod.setAccessible(true)
      val returnType = externalAccumsMethod.getReturnType.getName
      if (returnType == "scala.collection.mutable.Buffer") externalAccumsMethod.invoke(taskMetrics).asInstanceOf[Buffer[AccumulatorV2[_, _]]].lastOption
      else if (returnType == "scala.collection.Seq") externalAccumsMethod.invoke(taskMetrics).asInstanceOf[Seq[AccumulatorV2[_, _]]].lastOption
      else None
    } catch {
      case _ @ (_: NoSuchMethodException | _: InvocationTargetException | _: IllegalAccessException) =>
         None
    }
  }
}
