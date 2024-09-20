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

package org.apache.inlong.sort.base.util;

import io.opentelemetry.exporter.otlp.logs.OtlpGrpcLogRecordExporter;
import io.opentelemetry.instrumentation.log4j.appender.v2_17.OpenTelemetryAppender;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.OpenTelemetrySdkBuilder;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.SdkLoggerProviderBuilder;
import io.opentelemetry.sdk.logs.export.BatchLogRecordProcessor;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class OpenTelemetryLogger {

    private OpenTelemetrySdk SDK; // OpenTelemetry SDK

    private final String endpoint; // OpenTelemetry Exporter Endpoint

    private final String serviceName; // OpenTelemetry Service Name

    private final Layout<?> layout; // Log4j Layout

    private final Level logLevel; // Log4j Log Level

    private static final Logger LOG = LoggerFactory.getLogger(OpenTelemetryLogger.class);

    public OpenTelemetryLogger() {
        // Default Service Name
        serviceName = "inlong-sort-connector";
        // Get OpenTelemetry Exporter Endpoint from Environment Variable
        if (System.getenv("OTEL_EXPORTER_ENDPOINT") != null) {
            endpoint = System.getenv("OTEL_EXPORTER_ENDPOINT");
        } else {
            endpoint = "localhost:4317";
        }
        // Default Log4j Layout
        this.layout = PatternLayout.newBuilder()
                .withPattern("%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n")
                .withCharset(StandardCharsets.UTF_8)
                .build();
        // Default Log4j Log Level
        this.logLevel = Level.INFO;
    }

    public OpenTelemetryLogger(String serviceName, String endpoint, Layout<?> layout, Level logLevel) {
        this.serviceName = serviceName;
        this.endpoint = endpoint;
        this.layout = layout;
        this.logLevel = logLevel;
    }

    private void createOpenTelemetrySdk() {
        // Create OpenTelemetry SDK
        OpenTelemetrySdkBuilder sdkBuilder = OpenTelemetrySdk.builder();
        // Create Logger Provider Builder
        SdkLoggerProviderBuilder loggerProviderBuilder = SdkLoggerProvider.builder();
        // get Resource
        Resource resource = Resource.getDefault().toBuilder()
                .put(ResourceAttributes.SERVICE_NAME, this.serviceName)
                .build();
        // set Resource
        loggerProviderBuilder.setResource(resource);
        // Create OpenTelemetry Exporter
        OtlpGrpcLogRecordExporter exporter = OtlpGrpcLogRecordExporter.builder()
                .setEndpoint("http://" + this.endpoint)
                .build();
        // Create BatchLogRecordProcessor use OpenTelemetry Exporter
        BatchLogRecordProcessor batchLogRecordProcessor = BatchLogRecordProcessor.builder(exporter).build();
        // Add BatchLogRecordProcessor to Logger Provider Builder
        loggerProviderBuilder.addLogRecordProcessor(batchLogRecordProcessor);
        // set Logger Provider
        sdkBuilder.setLoggerProvider(loggerProviderBuilder.build());
        // Build OpenTelemetry SDK
        SDK = sdkBuilder.build();
    }

    public void addOpenTelemetryAppender() {
        org.apache.logging.log4j.spi.LoggerContext context = LogManager.getContext(false);
        LoggerContext loggerContext = (LoggerContext) context;
        Configuration config = loggerContext.getConfiguration();
        // Create OpenTelemetryAppender
        OpenTelemetryAppender otelAppender = OpenTelemetryAppender.builder()
                .setName("OpenTelemetryAppender")
                .setLayout(this.layout)
                .build();
        otelAppender.start();
        // add OpenTelemetryAppender to configuration
        config.addAppender(otelAppender);
        // Get Root Logger Configuration
        LoggerConfig loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
        // Add OpenTelemetryAppender to Root Logger
        loggerConfig.addAppender(otelAppender, this.logLevel, null);
        // refresh logger context
        loggerContext.updateLoggers();
    }

    public void removeOpenTelemetryAppender() {
        org.apache.logging.log4j.spi.LoggerContext context = LogManager.getContext(false);
        LoggerContext loggerContext = (LoggerContext) context;
        Configuration config = loggerContext.getConfiguration();
        config.getAppenders().values().forEach(appender -> {
            // Remove OpenTelemetryAppender
            if (appender instanceof OpenTelemetryAppender) {
                config.getRootLogger().removeAppender(appender.getName());
                appender.stop();
            }
        });
        // refresh logger context
        loggerContext.updateLoggers();
    }

    public void install() {
        addOpenTelemetryAppender();
        createOpenTelemetrySdk();
        OpenTelemetryAppender.install(SDK);
        LOG.info("OpenTelemetryLogger installed");
    }

    public void uninstall() {
        LOG.info("OpenTelemetryLogger uninstalled");
        SDK.close();
        removeOpenTelemetryAppender();

    }
}