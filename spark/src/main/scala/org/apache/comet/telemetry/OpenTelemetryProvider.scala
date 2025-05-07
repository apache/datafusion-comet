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

import io.opentelemetry.api.metrics.Meter
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk

class OpenTelemetryProvider extends TelemetryProvider {

  lazy val sdk: OpenTelemetrySdk = AutoConfiguredOpenTelemetrySdk.initialize.getOpenTelemetrySdk

  lazy val tracerProvider: Tracer = sdk.getTracerProvider.get("Comet")

  lazy val meterProvider: Meter = sdk.getMeterProvider.meterBuilder("Comet").build()

  override def setGauge(name: String, value: Long): Unit = {
    // TODO store gauges in map to avoid creating each time?
    meterProvider.gaugeBuilder(name).build().set(value)
  }

  override def startSpan(name: String): Span = {
    new OpenTelemetrySpan(tracerProvider.spanBuilder(name).startSpan())
  }

  class OpenTelemetrySpan(span: io.opentelemetry.api.trace.Span) extends Span {
    override def end(): Unit = {
      span.end()
    }
  }
}
