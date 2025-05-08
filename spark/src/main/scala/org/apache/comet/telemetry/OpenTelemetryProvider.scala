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

import java.time.Duration
import java.util.concurrent.TimeUnit

// scalastyle and spotless have different opinions on the correct ordering for these imports
// scalastyle:off
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor
// scalastyle:on

object OpenTelemetryProvider extends TelemetryProvider with Serializable {

  lazy val sdk: OpenTelemetrySdk = {

    // TODO should use SERVICE_NAME constant from opentelemetry-semconv package
    val resource =
      Resource.getDefault.merge(Resource.builder.put("SERVICE_NAME", "CometOtlpExporter").build)

    val openTelemetrySdk = OpenTelemetrySdk.builder
      .setTracerProvider(
        SdkTracerProvider.builder
          .setResource(resource)
          .addSpanProcessor(
            BatchSpanProcessor
              .builder(OtlpGrpcSpanExporter.builder.setTimeout(2, TimeUnit.SECONDS).build)
              .setScheduleDelay(100, TimeUnit.MILLISECONDS)
              .build)
          .build)
      .setMeterProvider(
        SdkMeterProvider.builder
          .setResource(resource)
          .registerMetricReader(
            PeriodicMetricReader
              .builder(OtlpGrpcMetricExporter.getDefault)
              .setInterval(Duration.ofMillis(1000))
              .build)
          .build)
      .buildAndRegisterGlobal

    // TODO shutdown hook

    openTelemetrySdk
  }

  override def setGauge(name: String, value: Long): Unit = {
    sdk.getMeterProvider.meterBuilder("Comet").build().gaugeBuilder(name).build().set(value)
  }

  override def startSpan(name: String): Span = {
    new OpenTelemetrySpan(sdk.tracerBuilder("Comet").build().spanBuilder(name).startSpan())
  }

  class OpenTelemetrySpan(span: io.opentelemetry.api.trace.Span) extends Span {
    override def end(): Unit = {
      span.end()
    }
  }
}
