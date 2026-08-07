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

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.UnaryExecNode

/**
 * Base type for [[org.apache.spark.sql.comet.CometWriteFilesExec]] on Spark 3.x.
 *
 * Spark 3.x has no `WriteFilesExecBase` trait (added in 4.0): `V1WritesUtils.getWriteFilesOpt`
 * matches the concrete `WriteFilesExec` case class, so a Comet node can never be picked up as the
 * write node there. Native writes are gated to Spark 4.0+ in `CometExecRule` and this shim exists
 * only so that the shared sources compile against 3.x. It mirrors the members that the 4.x
 * `WriteFilesExecBase` supplies.
 */
trait ShimCometWriteFilesExec extends UnaryExecNode {
  override def output: Seq[Attribute] = Seq.empty
}
