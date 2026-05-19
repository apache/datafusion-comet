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

package org.apache.spark.comet

import org.apache.spark.TaskContext

/**
 * Package-private access shim for `TaskContext.setTaskContext` / `TaskContext.unset`.
 *
 * Both methods are declared `protected[spark]` on Spark's `TaskContext` companion, so they are
 * reachable from code inside the `org.apache.spark` package tree but not from `org.apache.comet`.
 * The Comet JVM UDF bridge needs to set the thread-local `TaskContext` on its caller thread (a
 * Tokio worker thread with no `TaskContext`) so the user's UDF body and any partition-sensitive
 * built-ins (`Rand`, `Uuid`, `MonotonicallyIncreasingID`, etc.) see the driving Spark task's
 * `TaskContext`. This shim lives in `org.apache.spark.comet` so it can call through to the
 * protected methods, and exposes plain public forwarders the bridge (which lives in
 * `org.apache.comet.udf`) can use.
 */
object CometTaskContextShim {

  def set(taskContext: TaskContext): Unit = TaskContext.setTaskContext(taskContext)

  def unset(): Unit = TaskContext.unset()
}
