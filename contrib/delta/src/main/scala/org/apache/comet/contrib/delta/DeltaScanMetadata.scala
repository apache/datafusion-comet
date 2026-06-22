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

package org.apache.comet.contrib.delta

import org.apache.spark.sql.types.StructType

/**
 * Delta-specific planning info captured by `DeltaScanRule` and carried on the
 * `CometDeltaScanMarker` as a field (rather than mutating the scan schema / `relation.options`).
 * Read by `CometDeltaNativeScan.convert` when building the native scan operator.
 *
 * @param analyzedSchema
 *   the analysis-time Delta schema (`DeltaParquetFileFormat.referenceSchema`), captured while the
 *   original Delta file format is still present so column-mapping physical names / field-ids
 *   resolve against the schema the query was analyzed with rather than a re-resolved latest
 *   snapshot. `None` when the table has no reference schema.
 * @param oneTaskPerPartition
 *   force one file per Spark partition (per-file `_metadata.file_path` projection needs 1:1
 *   file/partition alignment so per-file synthetic columns aren't dropped).
 */
case class DeltaScanMetadata(analyzedSchema: Option[StructType], oneTaskPerPartition: Boolean)
