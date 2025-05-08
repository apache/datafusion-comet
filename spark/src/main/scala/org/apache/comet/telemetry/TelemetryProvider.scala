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

import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

trait TelemetryProvider {

  def setGauge(name: String, value: Long): Unit

  def startSpan(name: String): Span

  def withSpan[T](name: String, fun: => T): T = {
    val span = startSpan(name)
    try {
      fun
    } finally {
      span.end()
    }
  }

}

object TelemetryProviderFactory {
  def create(conf: SQLConf): TelemetryProvider = {
    CometConf.COMET_TELEMETRY_PROVIDER.get(conf) match {
      case "chrome" => new ChromeTelemetryProvider
      case "otel" => new OpenTelemetryProvider
      case _ => new NoopTelemetryProvider
    }
  }
}
