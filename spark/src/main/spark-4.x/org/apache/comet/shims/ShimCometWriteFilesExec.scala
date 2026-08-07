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

import org.apache.spark.sql.execution.datasources.WriteFilesExecBase

/**
 * Base type for [[org.apache.spark.sql.comet.CometWriteFilesExec]].
 *
 * Spark 4.0 factored `WriteFilesExec`'s contract out into the `WriteFilesExecBase` trait, and
 * `V1WritesUtils.getWriteFilesOpt` matches on that trait. Extending it is therefore what makes
 * Spark recognize Comet's node as the write node and drive it through
 * `FileFormatWriter.executeWrite` -> `SparkPlan.executeWrite` -> `doExecuteWrite`, keeping the
 * commit protocol, stats trackers and `_SUCCESS` handling on Spark's side.
 *
 * Spark 3.x has no such trait - `getWriteFilesOpt` matches the concrete `WriteFilesExec` case
 * class - so native writes are gated to Spark 4.0+ in `CometExecRule`. The 3.x variant of this
 * shim exists only to keep the shared sources compiling.
 */
trait ShimCometWriteFilesExec extends WriteFilesExecBase
