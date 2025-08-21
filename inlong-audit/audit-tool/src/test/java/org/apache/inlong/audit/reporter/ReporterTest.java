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

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.inlong.audit.tool.metric.MetricData;
import org.apache.inlong.audit.tool.metric.MetricData.AlertInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;

import static org.apache.inlong.audit.tool.config.ConfigConstants.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for PrometheusReporter.
 * This test starts a real HTTP server on a random free port.
 */
class PrometheusReporterTest {

    private PrometheusReporter reporter;
    private int port;
    private final OkHttpClient client = new OkHttpClient();

    @BeforeEach
    void setUp() throws IOException {
        // Find a random free port
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        reporter = new PrometheusReporter();
        Map<String, Object> config = new HashMap<>();
        config.put(KEY_PROMETHEUS_PORT, port);
        reporter.init(config);
    }

    @AfterEach
    void tearDown() {
        if (reporter != null) {
            reporter.close();
        }
    }

    private String scrapeMetrics() throws IOException {
        Request request = new Request.Builder().url("http://localhost:" + port).build();
        try (Response response = client.newCall(request).execute()) {
            assertThat(response.isSuccessful()).isTrue();
            return response.body().string();
        }
    }

    @Test
    void testReportAlert() throws IOException {
        // Arrange
        AlertInfo alertInfo = new AlertInfo("DATA_LOSS");
        MetricData metricData = new MetricData(
                "group-1", "stream-1", null, alertInfo
        );

        // Act
        reporter.report(metricData);

        // Assert
        String metrics = scrapeMetrics();
        assertThat(metrics).contains(
                "inlong_audit_tool_alerts_total{groupId=\"group-1\",streamId=\"stream-1\",alertType=\"DATA_LOSS\",} 1.0"
        );
    }

    @Test
    void testReportDataLossRate() throws IOException {
        // Arrange
        MetricData metricData = new MetricData(
                "group-2", "stream-2", 0.99, null
        );

        // Act
        reporter.report(metricData);

        // Assert
        String metrics = scrapeMetrics();
        assertThat(metrics).contains(
                "inlong_audit_tool_data_loss_rate{groupId=\"group-2\",streamId=\"stream-2\",} 0.99"
        );
    }

    @Test
    void testReportCombinedData() throws IOException {
        // Arrange
        AlertInfo alertInfo = new AlertInfo("HIGH_DELAY");
        MetricData metricData = new MetricData(
                "group-3", "stream-3", 0.05, alertInfo
        );

        // Act
        reporter.report(metricData);
        reporter.report(metricData); // Report twice to check alert counter increment

        // Assert
        String metrics = scrapeMetrics();
        assertThat(metrics)
                .contains("inlong_audit_tool_alerts_total{groupId=\"group-3\",streamId=\"stream-3\",alertType=\"HIGH_DELAY\",} 2.0")
                .contains("inlong_audit_tool_data_loss_rate{groupId=\"group-3\",streamId=\"stream-3\",} 0.05");
    }
}

/**
 * Unit tests for OpenTelemetryReporter.
 * This test uses the in-memory test kit from the OpenTelemetry SDK.
 */
class OpenTelemetryReporterTest {

    // OpenTelemetryExtension registers an in-memory metric reader and exporter
    @RegisterExtension
    static final OpenTelemetryExtension otelTesting = OpenTelemetryExtension.create();

    private OpenTelemetryReporter reporter;

    @BeforeEach
    void setUp() {
        reporter = new OpenTelemetryReporter();
        reporter.meter = otelTesting.getOpenTelemetry().getMeter(AUDIT_TOOL_NAME);

        // Create and initialize the alert counter
        reporter.alertCounter = reporter.meter
                .counterBuilder(AUDIT_TOOL_ALERTS_TOTAL)
                .setDescription(DESC_AUDIT_TOOL_ALERTS_TOTAL)
                .build();

        // Create and initialize the data loss rate gauge
        reporter.meter.gaugeBuilder(AUDIT_TOOL_DATA_LOSS_RATE)
                .setDescription(DESC_AUDIT_TOOL_DATA_LOSS_RATE)
                .buildWithCallback(measurement -> {
                    reporter.dataLossRateValues.forEach((attributes, value) ->
                            measurement.record(value, attributes)
                    );
                });
    }

    @Test
    void testReportAlert() {
        // Arrange
        AlertInfo alertInfo = new AlertInfo("DATA_LOSS");
        MetricData metricData = new MetricData(
                "group-1", "stream-1", null, alertInfo
        );

        // Act
        reporter.report(metricData);
        reporter.report(metricData); // Report twice to check counter increment

        assertThat(otelTesting.getMetrics())
                .hasSize(1)
                .first()
                .satisfies(metric -> {
                    assertThat(metric.getLongSumData().getPoints())
                            .hasSize(1)
                            .first()
                            .satisfies(point -> {
                                assertThat(point.getValue()).isEqualTo(2L); // Reported twice
                                assertThat(point.getAttributes()).isEqualTo(Attributes.of(
                                        AttributeKey.stringKey(KEY_GROUP_ID), "group-1",
                                        AttributeKey.stringKey(KEY_STREAM_ID), "stream-1",
                                        AttributeKey.stringKey(KEY_ALERT_TYPE), "DATA_LOSS"
                                ));
                            });
                });
    }

    @Test
    void testReportDataLossRate() {
        // Arrange
        MetricData metricData1 = new MetricData(
                "group-2", "stream-2", 0.99, null
        );
        MetricData metricData2 = new MetricData(
                "group-2", "stream-2", 0.98, null // Update the value
        );

        // Act
        reporter.report(metricData1);
        reporter.report(metricData2); // The map will now hold the latest value (0.98)

        // Assert
        assertThat(otelTesting.getMetrics())
                .hasSize(1)
                .first()
                .satisfies(metric -> {
                    assertThat(metric.getName()).isEqualTo(AUDIT_TOOL_DATA_LOSS_RATE);
                    assertThat(metric.getDoubleGaugeData().getPoints())
                            .hasSize(1)
                            .first()
                            .satisfies(point -> {
                                assertThat(point.getValue()).isEqualTo(0.98); // Latest value
                                assertThat(point.getAttributes()).isEqualTo(Attributes.of(
                                        AttributeKey.stringKey(KEY_GROUP_ID), "group-2",
                                        AttributeKey.stringKey(KEY_STREAM_ID), "stream-2"
                                ));
                            });
                });
    }
}
