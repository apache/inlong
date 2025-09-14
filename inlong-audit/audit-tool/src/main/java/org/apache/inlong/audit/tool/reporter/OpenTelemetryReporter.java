/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.audit.tool.reporter;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.inlong.audit.tool.config.ConfigConstants.*;

public class OpenTelemetryReporter implements MetricReporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpenTelemetryReporter.class);

    protected SdkMeterProvider meterProvider;
    public Meter meter;
    public LongCounter alertCounter;
    // For Gauge, we need a way to hold the latest value for each dimension set.
    // A map is a good way to handle this dynamically.
    public final Map<Attributes, Double> dataLossRateValues = new ConcurrentHashMap<>();

    @Override
    public void init(Map<String, Object> config) {
        String endpoint = (String) config.getOrDefault(KEY_OTEL_ENDPOINT, DEFAULT_OTEL_ENDPOINT);

        OtlpGrpcMetricExporter metricExporter = OtlpGrpcMetricExporter.builder()
                .setEndpoint(endpoint)
                .build();

        this.meterProvider = SdkMeterProvider.builder()
                .registerMetricReader(PeriodicMetricReader.builder(metricExporter)
                        .setInterval(Duration.ofSeconds(30))
                        .build())
                .build();

        // We don't build the full OpenTelemetrySdk unless we need tracing/logs as well.
        // For metrics only, managing the SdkMeterProvider is enough.
        this.meter = meterProvider.get(AUDIT_TOOL_NAME);

        this.alertCounter = meter.counterBuilder(AUDIT_TOOL_ALERTS_TOTAL)
                .setDescription(DESC_AUDIT_TOOL_ALERTS_TOTAL)
                .build();

        // For Gauge, we use an observable gauge.
        // It will call our callback function periodically to get the current value.
        meter.gaugeBuilder(AUDIT_TOOL_DATA_LOSS_RATE)
                .setDescription(DESC_AUDIT_TOOL_DATA_LOSS_RATE)
                .buildWithCallback(measurement -> {
                    dataLossRateValues.forEach((attributes, value) -> measurement.record(value, attributes));
                });
    }

}
