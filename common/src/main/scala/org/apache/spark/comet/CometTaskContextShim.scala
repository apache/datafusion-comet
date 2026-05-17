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
import org.apache.spark.memory.TaskMemoryManager

/**
 * Package-private access shim for Spark APIs that are `protected[spark]` or `private[spark]`.
 *
 * `TaskContext.setTaskContext` / `TaskContext.unset` are `protected[spark]` on the companion;
 * `TaskContext.taskMemoryManager()` is `private[spark]` on the instance. Code outside the
 * `org.apache.spark` package tree (e.g. `org.apache.comet.udf`) cannot call them directly. This
 * shim lives in `org.apache.spark.comet` so it can forward through.
 */
object CometTaskContextShim {

  def set(taskContext: TaskContext): Unit = TaskContext.setTaskContext(taskContext)

  def unset(): Unit = TaskContext.unset()

  def taskMemoryManager(taskContext: TaskContext): TaskMemoryManager =
    taskContext.taskMemoryManager()
}
