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

import java.io.File

import scala.collection.mutable.ArrayBuffer

object TestUtils {

  /**
   * Spark 3.3.0 moved {{{SpecificParquetRecordReaderBase.listDirectory}}} to
   * {{{org.apache.spark.TestUtils.listDirectory}}}. Copying it here to bridge the difference
   * between Spark 3.2.0 and 3.3.0 TODO: remove after dropping Spark 3.2.0 support and use
   * {{{org.apache.spark.TestUtils.listDirectory}}}
   */
  def listDirectory(path: File): Array[String] = {
    val result = ArrayBuffer.empty[String]
    if (path.isDirectory) {
      path.listFiles.foreach(f => result.appendAll(listDirectory(f)))
    } else {
      val c = path.getName.charAt(0)
      if (c != '.' && c != '_') result.append(path.getAbsolutePath)
    }
    result.toArray
  }
}
