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

package org.apache.comet.vector

import org.apache.arrow.c.ArrowArray
import org.apache.arrow.c.ArrowSchema

/**
 * A wrapper class to hold the exported Arrow arrays and schemas.
 *
 * @param batch
 *   a list containing number of rows + pairs of memory addresses in the format of (address of
 *   Arrow array, address of Arrow schema)
 * @param arrowSchemas
 *   the exported Arrow schemas, needs to be deallocated after being moved by the native executor
 * @param arrowArrays
 *   the exported Arrow arrays, needs to be deallocated after being moved by the native executor
 */
case class ExportedBatch(
    batch: Array[Long],
    arrowSchemas: Array[ArrowSchema],
    arrowArrays: Array[ArrowArray]) {
  def close(): Unit = {
    arrowSchemas.foreach(_.close())
    arrowArrays.foreach(_.close())
  }
}
