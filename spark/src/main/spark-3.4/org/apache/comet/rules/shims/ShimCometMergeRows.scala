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

package org.apache.comet.rules.shims

import org.apache.spark.sql.execution.SparkPlan

import org.apache.comet.serde.CometOperatorSerde

/**
 * Spark 3.4 predates `MergeRowsExec` (it was moved from Iceberg extensions into Spark core in
 * Iceberg 1.4.0 / SPARK-52403, first shipping in Spark 3.5). Nothing to register here; CoW MERGE
 * on 3.4 continues to run via Iceberg's own extension-provided operator, unconverted.
 */
object ShimCometMergeRows {
  val nativeExecs: Map[Class[_ <: SparkPlan], CometOperatorSerde[_]] = Map.empty
}
