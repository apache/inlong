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
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.message.SimpleMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * OpenTelemetryLogger to collect logs and send to OpenTelemetry
 */
public class OpenTelemetryLogger {

    private OpenTelemetrySdk SDK; // OpenTelemetry SDK

    private final String endpoint; // OpenTelemetry Exporter Endpoint

    private final String serviceName; // OpenTelemetry Service Name

    private final Layout<?> layout; // Log4j Layout

    private final Level logLevel; // Log4j Log Level

    private final String localHostIp; // Local Host IP

    private static final Logger LOG = LoggerFactory.getLogger(OpenTelemetryLogger.class);

    public OpenTelemetryLogger(Builder builder) {
        this.serviceName = builder.serviceName;
        this.endpoint = builder.endpoint;
        this.layout = builder.layout;
        this.logLevel = builder.logLevel;
        this.localHostIp = builder.localHostIp;
    }

    public OpenTelemetryLogger(String serviceName, String endpoint, Layout<?> layout, Level logLevel,
            String localHostIp) {
        this.serviceName = serviceName;
        this.endpoint = endpoint;
        this.layout = layout;
        this.logLevel = logLevel;
        this.localHostIp = localHostIp;
    }

    /**
     * OpenTelemetryLogger Builder
     */
    public static final class Builder {

        private String endpoint; // OpenTelemetry Exporter Endpoint

        private String serviceName; // OpenTelemetry Service Name

        private Layout<?> layout; // Log4j Layout

        private Level logLevel; // Log4j Log Level

        private String localHostIp;

        public Builder() {
        }

        public Builder setServiceName(String serviceName) {
            this.serviceName = serviceName;
            return this;
        }

        public Builder setLayout(Layout<?> layout) {
            this.layout = layout;
            return this;
        }

        public Builder setEndpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public Builder setLogLevel(Level logLevel) {
            this.logLevel = logLevel;
            return this;
        }

        public Builder setLocalHostIp(String localHostIp) {
            this.localHostIp = localHostIp;
            return this;
        }

        public OpenTelemetryLogger build() {
            if (this.serviceName == null) {
                this.serviceName = "unnamed_service";
            }
            if (this.endpoint == null) {
                if (System.getenv("OTEL_EXPORTER_ENDPOINT") != null) {
                    this.endpoint = System.getenv("OTEL_EXPORTER_ENDPOINT");
                } else {
                    this.endpoint = "localhost:4317";
                }
            }
            if (this.layout == null) {
                this.layout = PatternLayout.newBuilder()
                        .withPattern("%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n")
                        .withCharset(StandardCharsets.UTF_8)
                        .build();
            }
            if (this.logLevel == null) {
                this.logLevel = Level.INFO;
            }
            return new OpenTelemetryLogger(this);
        }

    }

    /**
     * Create OpenTelemetry SDK with OpenTelemetry Exporter
     */
    private void createOpenTelemetrySdk() {
        // Create OpenTelemetry SDK
        OpenTelemetrySdkBuilder sdkBuilder = OpenTelemetrySdk.builder();
        // Create Logger Provider Builder
        SdkLoggerProviderBuilder loggerProviderBuilder = SdkLoggerProvider.builder();
        // get Resource
        Resource resource = Resource.getDefault().toBuilder()
                .put(ResourceAttributes.SERVICE_NAMESPACE, "inlong_sort")
                .put(ResourceAttributes.SERVICE_NAME, this.serviceName)
                .put(ResourceAttributes.HOST_NAME, this.localHostIp)
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

    /**
     * Install OpenTelemetryLogger for the application
     */
    public boolean install() {
        synchronized (OpenTelemetryLogger.class) {
            org.apache.logging.log4j.spi.LoggerContext loggerContextSpi = LogManager.getContext(false);
            if (!(loggerContextSpi instanceof LoggerContext)) {
                LOG.warn("LoggerContext is not instance of LoggerContext");
                return false;
            }
            LoggerContext loggerContext = (LoggerContext) loggerContextSpi;
            Configuration config = loggerContext.getConfiguration();
            for (Appender appender : config.getAppenders().values()) {
                if (appender instanceof OpenTelemetryAppender) {
                    LOG.info("OpenTelemetryLogger already installed");
                    return false;
                }
            }
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
            // create OpenTelemetry SDK
            createOpenTelemetrySdk();
            // install OpenTelemetryAppender
            OpenTelemetryAppender.install(SDK);
            otelAppender.append(
                    new Log4jLogEvent.Builder()
                            .setLevel(Level.INFO)
                            .setMessage(new SimpleMessage("OpenTelemetryLogger installed"))
                            .build());
            return true;
        }
    }

    /**
     * Uninstall OpenTelemetryLogger
     */
    public boolean uninstall() {
        synchronized (OpenTelemetryLogger.class) {
            if (SDK == null) {
                LOG.warn("OpenTelemetryLogger is not installed");
                return false;
            }
            org.apache.logging.log4j.spi.LoggerContext loggerContextSpi = LogManager.getContext(false);
            if (!(loggerContextSpi instanceof LoggerContext)) {
                LOG.warn("LoggerContext is not instance of LoggerContext");
                return false;
            }
            LoggerContext loggerContext = (LoggerContext) loggerContextSpi;
            Configuration config = loggerContext.getConfiguration();
            config.getAppenders().values().forEach(appender -> {
                if (appender instanceof OpenTelemetryAppender) {
                    appender.append(
                            new Log4jLogEvent.Builder()
                                    .setLevel(Level.INFO)
                                    .setMessage(new SimpleMessage("OpenTelemetryLogger uninstalled"))
                                    .build());
                    config.getRootLogger().removeAppender(appender.getName());
                    appender.stop();
                }
            });
            SDK.close();
            return true;
        }
    }
}