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

package org.apache.comet.iceberg

import org.apache.spark.sql.catalyst.plans.logical.ReplaceData

/**
 * Spark 4.x: `ReplaceData` carries `projections: ReplaceDataProjections` -- the rewritten row
 * stream is prefixed with an operation column (5=WRITE, 6=WRITE_WITH_METADATA), and we have to
 * apply `dataProj` / `metadataProj` before handing rows to the underlying `DataWriter`.
 */
private[iceberg] object IcebergReplaceDataShim {
  def extractProjections(rd: ReplaceData): Option[ReplaceDataDispatchInfo] = {
    val p = rd.projections
    Some(ReplaceDataDispatchInfo(p.rowProjection, p.metadataProjection))
  }
}
