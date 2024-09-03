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

package org.apache.inlong.sort.mongodb;

import org.apache.inlong.sort.base.metric.MetricOption;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.time.ZoneId;
import java.util.HashSet;
import java.util.Set;

import static com.ververica.cdc.connectors.base.options.SourceOptions.CHUNK_META_GROUP_SIZE;
import static com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceOptions.*;
import static com.ververica.cdc.debezium.utils.ResolvedSchemaUtils.getPhysicalSchema;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.inlong.sort.base.Constants.AUDIT_KEYS;
import static org.apache.inlong.sort.base.Constants.INLONG_AUDIT;
import static org.apache.inlong.sort.base.Constants.INLONG_METRIC;
import static org.apache.inlong.sort.base.Constants.SOURCE_MULTIPLE_ENABLE;

public class MongoDBTableFactory implements DynamicTableSourceFactory {

    private static final String IDENTIFIER = "mongodb-cdc-inlong";

    private static final String DOCUMENT_ID_FIELD = "_id";
    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        final ReadableConfig config = helper.getOptions();

        String hosts = config.get(HOSTS);
        String connectionOptions = config.getOptional(CONNECTION_OPTIONS).orElse(null);

        String username = config.getOptional(USERNAME).orElse(null);
        String password = config.getOptional(PASSWORD).orElse(null);

        String database = config.getOptional(DATABASE).orElse(null);
        String collection = config.getOptional(COLLECTION).orElse(null);

        Integer batchSize = config.get(BATCH_SIZE);
        Integer pollMaxBatchSize = config.get(POLL_MAX_BATCH_SIZE);
        Integer pollAwaitTimeMillis = config.get(POLL_AWAIT_TIME_MILLIS);

        Integer heartbeatIntervalMillis = config.get(HEARTBEAT_INTERVAL_MILLIS);

        Boolean copyExisting = config.get(COPY_EXISTING);
        Integer copyExistingQueueSize = config.getOptional(COPY_EXISTING_QUEUE_SIZE).orElse(null);

        String zoneId = context.getConfiguration().get(TableConfigOptions.LOCAL_TIME_ZONE);
        ZoneId localTimeZone =
                TableConfigOptions.LOCAL_TIME_ZONE.defaultValue().equals(zoneId)
                        ? ZoneId.systemDefault()
                        : ZoneId.of(zoneId);

        boolean enableParallelRead = config.get(SCAN_INCREMENTAL_SNAPSHOT_ENABLED);

        int splitSizeMB = config.get(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB);
        int splitMetaGroupSize = config.get(CHUNK_META_GROUP_SIZE);

        ResolvedSchema physicalSchema =
                getPhysicalSchema(context.getCatalogTable().getResolvedSchema());
        checkArgument(physicalSchema.getPrimaryKey().isPresent(), "Primary key must be present");
        checkPrimaryKey(physicalSchema.getPrimaryKey().get(), "Primary key must be _id field");

        String inlongMetric = config.getOptional(INLONG_METRIC).orElse(null);
        String auditHostAndPorts = config.get(INLONG_AUDIT);
        String auditKeys = config.get(AUDIT_KEYS);

        MetricOption metricOption = MetricOption.builder()
                .withInlongLabels(inlongMetric)
                .withAuditAddress(auditHostAndPorts)
                .withAuditKeys(auditKeys)
                .build();
        return new MongoDBTableSource(
                physicalSchema,
                hosts,
                username,
                password,
                database,
                collection,
                connectionOptions,
                copyExisting,
                copyExistingQueueSize,
                batchSize,
                pollMaxBatchSize,
                pollAwaitTimeMillis,
                heartbeatIntervalMillis,
                localTimeZone,
                enableParallelRead,
                splitMetaGroupSize,
                splitSizeMB,
                metricOption);
    }

    private void checkPrimaryKey(UniqueConstraint pk, String message) {
        checkArgument(
                pk.getColumns().size() == 1 && pk.getColumns().contains(DOCUMENT_ID_FIELD),
                message);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTS);
        options.add(DATABASE);
        options.add(COLLECTION);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(CONNECTION_OPTIONS);
        options.add(COPY_EXISTING);
        options.add(COPY_EXISTING_QUEUE_SIZE);
        options.add(BATCH_SIZE);
        options.add(POLL_MAX_BATCH_SIZE);
        options.add(POLL_AWAIT_TIME_MILLIS);
        options.add(HEARTBEAT_INTERVAL_MILLIS);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_ENABLED);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB);
        options.add(CHUNK_META_GROUP_SIZE);
        options.add(AUDIT_KEYS);
        options.add(INLONG_METRIC);
        options.add(INLONG_AUDIT);
        options.add(SOURCE_MULTIPLE_ENABLE);
        return options;
    }

}
