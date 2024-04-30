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

object ShimFileFormat {

  // TODO: remove after dropping Spark 3.2 & 3.3 support and directly use FileFormat.ROW_INDEX
  val ROW_INDEX = "row_index"

  // A name for a temporary column that holds row indexes computed by the file format reader
  // until they can be placed in the _metadata struct.
  // TODO: remove after dropping Spark 3.2 & 3.3 support and directly use
  //       FileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME
  val ROW_INDEX_TEMPORARY_COLUMN_NAME: String = s"_tmp_metadata_$ROW_INDEX"

  // TODO: remove after dropping Spark 3.2 support and use FileFormat.OPTION_RETURNING_BATCH
  val OPTION_RETURNING_BATCH = "returning_batch"
}
