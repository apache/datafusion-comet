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

package org.apache.spark.sql.comet.shims

import java.net.URI

import org.apache.spark.sql.execution.FileSourceScanExec

/**
 * Spark 3.x exposes `FileSourceScanExec.selectedPartitions` as `Seq[PartitionDirectory]`. Spark
 * 4.x changed it to the `ScanFileListing` trait. These helpers hide the difference.
 */
object ShimFileSourceScanExec {

  def selectedPartitionCount(scan: FileSourceScanExec): Int =
    scan.selectedPartitions.partitionCount

  def firstSelectedFileUri(scan: FileSourceScanExec): Option[URI] = {
    val partitions = scan.selectedPartitions.filePartitionIterator
    while (partitions.hasNext) {
      val files = partitions.next().files
      if (files.hasNext) {
        return Some(files.next().getPath.toUri)
      }
    }
    None
  }
}
