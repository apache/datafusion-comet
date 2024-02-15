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

package org.apache.spark.shuffle.sort

import scala.collection.mutable.ArrayBuffer

class RowPartition(initialSize: Int) {
  private val rowAddresses: ArrayBuffer[Long] = new ArrayBuffer[Long](initialSize)
  private val rowSizes: ArrayBuffer[Int] = new ArrayBuffer[Int](initialSize)

  def addRow(addr: Long, size: Int): Unit = {
    rowAddresses += addr
    rowSizes += size
  }

  def getNumRows: Int = rowAddresses.size

  def getRowAddresses: Array[Long] = rowAddresses.toArray
  def getRowSizes: Array[Int] = rowSizes.toArray

  def reset(): Unit = {
    rowAddresses.clear()
    rowSizes.clear()
  }
}
