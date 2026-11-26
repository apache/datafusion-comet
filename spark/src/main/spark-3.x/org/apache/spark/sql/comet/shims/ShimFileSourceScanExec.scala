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

/**
 * Positions of the filter arguments in the `FileSourceScanExec` constructor, used to rebuild a
 * scan through `makeCopy` with new filters. The constructor is `FileSourceScanExec(relation,
 * output, requiredSchema, partitionFilters, ...)`.
 */
object ShimFileSourceScanExec {
  val partitionFiltersIndex: Int = 3
  val dataFiltersIndex: Int = 6
}
