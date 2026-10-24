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

package org.apache.comet

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging

/**
 * Interrupts the native Python UDF calls of killed tasks. Spark's Python runner has a
 * `MonitorThread` that destroys the worker of a killed task. A native UDF runs in the embedded
 * interpreter on the task thread instead, where neither a Java interrupt nor a stopped input
 * reaches it. So one daemon thread per task polls its kill state and, while the task is killed
 * but still running, raises an exception in the task's running Python calls, once per call so
 * that its cleanup can run. Calls that start after the first interrupt fail without entering
 * Python.
 */
object PythonUdfMonitor extends Logging {

  // Raising the exception is cheap and lets the call unwind on its own, so unlike Spark's
  // monitor this one does not wait `spark.python.task.killTimeout` before acting. Repeating it
  // reaches calls of plans that the task registers after an earlier attempt.
  private val PollIntervalMs = 100L

  private val watchedTasks = ConcurrentHashMap.newKeySet[java.lang.Long]()

  /** Whether the native library can run Python UDFs, and so may need its calls interrupted. */
  lazy val supported: Boolean = NativeBase.supportsPythonUdf()

  /** Starts watching the task unless it is watched already. The watch ends with the task. */
  def watch(context: TaskContext): Unit = {
    val taskAttemptId = context.taskAttemptId()
    if (watchedTasks.add(taskAttemptId)) {
      val thread = new Thread(s"Comet Python UDF monitor for task $taskAttemptId") {
        override def run(): Unit = {
          try {
            while (!context.isCompleted()) {
              if (context.isInterrupted() && NativeBase.interruptPythonUdfs(taskAttemptId) > 0) {
                logInfo(s"Interrupted Python UDF calls of killed task $taskAttemptId")
              }
              Thread.sleep(PollIntervalMs)
            }
          } finally {
            val _ = watchedTasks.remove(taskAttemptId)
          }
        }
      }
      thread.setDaemon(true)
      thread.start()
    }
  }
}
