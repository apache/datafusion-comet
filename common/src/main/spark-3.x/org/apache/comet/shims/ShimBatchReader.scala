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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile

object ShimBatchReader {

  // TODO: remove after dropping Spark 3.2 & 3.3 support and directly call PartitionedFile
  def newPartitionedFile(partitionValues: InternalRow, file: String): PartitionedFile =
    classOf[PartitionedFile].getDeclaredConstructors
      .map(c =>
        c.getParameterCount match {
          case 5 =>
            c.newInstance(
              partitionValues,
              file,
              Long.box(-1), // -1 means we read the entire file
              Long.box(-1),
              Array.empty[String])
          case 7 =>
            c.newInstance(
              partitionValues,
              c.getParameterTypes()(1)
                .getConstructor(classOf[String])
                .newInstance(file)
                .asInstanceOf[AnyRef],
              Long.box(-1), // -1 means we read the entire file
              Long.box(-1),
              Array.empty[String],
              Long.box(0),
              Long.box(0))
        })
      .head
      .asInstanceOf[PartitionedFile]
}
