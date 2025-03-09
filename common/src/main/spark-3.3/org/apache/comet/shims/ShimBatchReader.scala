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

import scala.collection.Traversable

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.util.AccumulatorV2

object ShimBatchReader {

  def newPartitionedFile(partitionValues: InternalRow, file: String): PartitionedFile =
    PartitionedFile(
      partitionValues,
      file,
      -1, // -1 means we read the entire file
      -1,
      Array.empty[String],
      0,
      0)

  def getTaskAccumulator(taskMetrics: TaskMetrics): Option[AccumulatorV2[_, _]] = {
    classOf[TaskMetrics].getDeclaredMethods.flatMap{
      case m if m.getName == "externalAccums" =>
        m.setAccessible(true)
        m.invoke(taskMetrics).asInstanceOf[Traversable[AccumulatorV2[_, _]]].lastOption
      case _ => None
    }.headOption
  }
}
