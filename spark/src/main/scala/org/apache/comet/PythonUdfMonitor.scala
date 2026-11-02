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

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging

/**
 * Stops the native Python UDF calls of killed tasks. Spark's Python runner has a `MonitorThread`
 * that destroys the worker of a killed task. A native UDF runs in the embedded interpreter on the
 * task thread instead, where neither a Java interrupt nor a stopped input reaches it. So one
 * daemon thread per task polls its kill state. Once the task is killed, its later calls fail
 * without entering Python. A call still running `spark.python.task.killTimeout` after the kill
 * gets an exception raised in it, once so that its cleanup can run, unless that is disabled.
 */
object PythonUdfMonitor extends Logging {

  // Repeating the kill reaches calls of plans that the task registers after an earlier poll.
  private val PollIntervalMs = 100L

  private val nextTask = new AtomicLong()

  private val watches = new ConcurrentHashMap[TaskContext, Watch]()

  /** Whether the native library can run Python UDFs, and so may need its calls stopped. */
  lazy val supported: Boolean = NativeBase.supportsPythonUdf()

  /**
   * The watch over one task. `task` is unique in this process, while a task attempt id is unique
   * only within one SparkContext.
   */
  final class Watch private[PythonUdfMonitor] (val task: Long) {
    private val openPlans = new AtomicInteger()

    /** Counts a native plan bound to `task`, which the watch outlives. */
    def planOpened(): Unit = {
      val _ = openPlans.incrementAndGet()
    }

    /** Ends what [[planOpened]] started, once the plan has been released. */
    def planClosed(): Unit = {
      val _ = openPlans.decrementAndGet()
    }

    private[PythonUdfMonitor] def hasOpenPlans: Boolean = openPlans.get() > 0
  }

  /**
   * Returns the task's watch, starting it unless the task is watched already. The watch ends once
   * the task has completed and its counted plans are released, since releasing a plan waits for
   * its running call.
   */
  def watch(context: TaskContext, interruptRunning: Boolean): Watch =
    watches.computeIfAbsent(
      context,
      _ => {
        val watch = new Watch(nextTask.incrementAndGet())
        start(context, watch, interruptRunning)
        watch
      })

  private def start(context: TaskContext, watch: Watch, interruptRunning: Boolean): Unit = {
    val killTimeoutNs = TimeUnit.MILLISECONDS.toNanos(
      SparkEnv.get.conf.getTimeAsMs("spark.python.task.killTimeout", "2s"))
    val taskAttemptId = context.taskAttemptId()
    val thread = new Thread(s"Comet Python UDF monitor for task $taskAttemptId") {
      override def run(): Unit = {
        try {
          var killedAt = Option.empty[Long]
          while (!context.isCompleted() || watch.hasOpenPlans) {
            if (context.isInterrupted()) {
              val now = System.nanoTime()
              val since = killedAt.getOrElse(now)
              killedAt = Some(since)
              // Like Spark's monitor, give a running call the kill timeout to return.
              val interrupt = interruptRunning && now - since >= killTimeoutNs
              if (NativeBase.killPythonUdfs(watch.task, interrupt) > 0) {
                logInfo(s"Interrupted Python UDF calls of killed task $taskAttemptId")
              }
            }
            Thread.sleep(PollIntervalMs)
          }
        } finally {
          val _ = watches.remove(context, watch)
        }
      }
    }
    thread.setDaemon(true)
    thread.start()
  }
}
