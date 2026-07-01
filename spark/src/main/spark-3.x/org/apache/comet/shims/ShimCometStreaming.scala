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

import org.apache.spark.sql.execution.SparkPlan

// StreamSourceAwareSparkPlan does not exist in Spark 3.x, so fall back to
// walking the physical tree and checking each node's logical link. Inspecting
// only the root's logical link would silently miss streaming plans whenever a
// rule produced a fresh root node without copying the link over.
object ShimCometStreaming {
  def isStreamingPlan(plan: SparkPlan): Boolean =
    plan.exists(_.logicalLink.exists(_.isStreaming))
}
