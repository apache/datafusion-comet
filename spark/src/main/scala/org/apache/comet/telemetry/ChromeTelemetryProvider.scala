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

/**
 * Default provider that writes telemetry in Chrome Trace Event Format.
 */
class ChromeTelemetryProvider extends TelemetryProvider with Serializable {

  override def startSpan(name: String): Span = new ChromeSpan(name)

  override def setGauge(name: String, value: Long): Unit = {
    // TODO write actual Chrome Trace Event Format JSON
    // scalastyle:off println
    println(s"GAUGE $name = $value")
    // scalastyle:on println
  }

  private class ChromeSpan(name: String) extends Span {
    // TODO write actual Chrome Trace Event Format JSON
    // scalastyle:off println
    println(s"BEGIN $name")
    // scalastyle:on println

    override def end(): Unit = {
      // TODO write actual Chrome Trace Event Format JSON
      // scalastyle:off println
      println(s"END $name")
      // scalastyle:on println
    }
  }
}
