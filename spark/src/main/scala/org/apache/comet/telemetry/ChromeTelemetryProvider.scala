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

package org.apache.comet.telemetry

import java.io.FileWriter

/**
 * Default provider that writes telemetry in Chrome Trace Event Format.
 */
object ChromeTelemetryProvider extends TelemetryProvider with Serializable {

  lazy val pid: Long = {
    val processName = java.lang.management.ManagementFactory.getRuntimeMXBean.getName
    processName.split("@")(0).toLong
  }

  lazy val writer = {
    val w = new FileWriter(s"comet-events-$pid-${System.currentTimeMillis()}.log")
    w.append('[')
    w
  }

  override def startSpan(name: String): Span = new ChromeSpan(name)

  override def setGauge(name: String, value: Long): Unit = {
    val threadId = Thread.currentThread().getId
    val ts = System.currentTimeMillis()
    // scalastyle:off
    writer.write(
      s"""{ "name": "$name", "cat": "PERF", "ph": "C", "pid": $pid, "tid": "$threadId", "ts": $ts, "args": { "$name": "$value" } },\n""".stripMargin)
    // scalastyle:on
  }

  private class ChromeSpan(name: String) extends Span {
    logEvent(name, "B")

    override def end(): Unit = {
      logEvent(name, "E")
    }

    private def logEvent(name: String, ph: String): Unit = {
      val threadId = Thread.currentThread().getId
      val ts = System.currentTimeMillis()
      // scalastyle:off
      writer.write(
        s"""{ "name": "$name", "cat": "PERF", "ph": "$ph", "pid": $pid, "tid": "$threadId", "ts": $ts },\n""".stripMargin)
      // scalastyle:on
    }
  }
}
