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

package org.apache.inlong.sort.tubemq.table;

import org.apache.inlong.sort.base.dirty.DirtyOptions;
import org.apache.inlong.sort.base.dirty.sink.DirtySink;
import org.apache.inlong.sort.base.dirty.utils.DirtySinkFactoryUtils;
import org.apache.inlong.sort.protocol.node.ExtractNode;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.format.Format;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import static org.apache.flink.table.factories.FactoryUtil.FORMAT;
import static org.apache.inlong.sort.base.Constants.*;
import static org.apache.inlong.sort.tubemq.table.TubeMQOptions.*;

/**
 * A dynamic table factory implementation for TubeMQ.
 */

public class TubeMQDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "tubemq-inlong";

    public static boolean innerFormat = false;

    private static DecodingFormat<DeserializationSchema<RowData>> getValueDecodingFormat(
            TableFactoryHelper helper) {
        return helper.discoverOptionalDecodingFormat(DeserializationFormatFactory.class, FORMAT)
                .orElseGet(() -> helper.discoverDecodingFormat(DeserializationFormatFactory.class, FORMAT));
    }

    private static EncodingFormat<SerializationSchema<RowData>> getValueEncodingFormat(
            TableFactoryHelper helper) {
        return helper.discoverOptionalEncodingFormat(SerializationFormatFactory.class, FORMAT)
                .orElseGet(() -> helper.discoverEncodingFormat(SerializationFormatFactory.class, FORMAT));
    }

    private static void validatePKConstraints(
            ObjectIdentifier tableName, CatalogTable catalogTable, Format format) {
        if (catalogTable.getSchema().getPrimaryKey().isPresent()
                && format.getChangelogMode().containsOnly(RowKind.INSERT)) {
            Configuration options = Configuration.fromMap(catalogTable.getOptions());
            String formatName = options.getOptional(FORMAT).orElse(options.get(FORMAT));
            throw new ValidationException(String.format(
                    "The TubeMQ table '%s' with '%s' format doesn't support defining PRIMARY KEY constraint"
                            + " on the table, because it can't guarantee the semantic of primary key.",
                    tableName.asSummaryString(), formatName));
        }
    }

    private static Optional<DecodingFormat<DeserializationSchema<RowData>>> getKeyDecodingFormat(
            TableFactoryHelper helper) {
        final Optional<DecodingFormat<DeserializationSchema<RowData>>> keyDecodingFormat = helper
                .discoverOptionalDecodingFormat(DeserializationFormatFactory.class, KEY_FORMAT);
        keyDecodingFormat.ifPresent(format -> {
            if (!format.getChangelogMode().containsOnly(RowKind.INSERT)) {
                throw new ValidationException(String.format(
                        "A key format should only deal with INSERT-only records. "
                                + "But %s has a changelog mode of %s.",
                        helper.getOptions().get(KEY_FORMAT),
                        format.getChangelogMode()));
            }
        });
        return keyDecodingFormat;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        final ReadableConfig tableOptions = helper.getOptions();

        final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat = getValueDecodingFormat(helper);

        // validate all options
        helper.validateExcept(ExtractNode.INLONG_MSG, PROPERTIES_PREFIX, DIRTY_PREFIX);

        validatePKConstraints(context.getObjectIdentifier(), context.getCatalogTable(), valueDecodingFormat);
        innerFormat = tableOptions.get(FORMAT).contains(ExtractNode.INLONG_MSG);

        final Configuration properties = getTubeMQProperties(context.getCatalogTable().getOptions());

        final DataType physicalDataType = context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

        String inlongMetric = tableOptions.getOptional(INLONG_METRIC).orElse(null);
        String auditHostAndPorts = tableOptions.get(INLONG_AUDIT);
        String auditKeys = tableOptions.get(AUDIT_KEYS);

        final DirtyOptions dirtyOptions = DirtyOptions.fromConfig(tableOptions);
        final DirtySink<byte[]> dirtySink = DirtySinkFactoryUtils.createDirtySink(context, dirtyOptions);

        return createTubeMQTableSource(
                physicalDataType,
                valueDecodingFormat,
                TubeMQOptions.getSourceTopics(tableOptions),
                TubeMQOptions.getMasterRpcAddress(tableOptions),
                TubeMQOptions.getTiSet(tableOptions),
                TubeMQOptions.getConsumerGroup(tableOptions),
                TubeMQOptions.getSessionKey(tableOptions),
                properties,
                inlongMetric,
                auditHostAndPorts,
                auditKeys,
                dirtySink,
                dirtyOptions);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        final ReadableConfig tableOptions = helper.getOptions();

        final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat = getValueEncodingFormat(helper);

        // validate all options
        helper.validateExcept(ExtractNode.INLONG_MSG);

        validatePKConstraints(context.getObjectIdentifier(), context.getCatalogTable(), valueEncodingFormat);

        final Configuration properties = getTubeMQProperties(context.getCatalogTable().getOptions());

        final DataType physicalDataType = context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

        return createTubeMQTableSink(
                physicalDataType,
                valueEncodingFormat,
                TubeMQOptions.getSinkTopics(tableOptions),
                TubeMQOptions.getMasterRpcAddress(tableOptions),
                TubeMQOptions.getTiSet(tableOptions),
                properties);
    }

    protected TubeMQTableSource createTubeMQTableSource(
            DataType physicalDataType,
            DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat,
            String topic,
            String url,
            TreeSet<String> streamId,
            String consumerGroup,
            String sessionKey,
            Configuration properties,
            String inlongMetric,
            String auditHostAndPorts,
            String auditKeys,
            DirtySink<byte[]> dirtySink,
            DirtyOptions dirtyOptions) {
        return new TubeMQTableSource(
                physicalDataType,
                valueDecodingFormat,
                url,
                topic,
                streamId,
                consumerGroup,
                sessionKey,
                properties,
                null,
                null,
                false,
                innerFormat,
                inlongMetric,
                auditHostAndPorts,
                auditKeys,
                dirtySink,
                dirtyOptions);
    }

    protected TubeMQTableSink createTubeMQTableSink(
            DataType physicalDataType,
            EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat,
            String topic,
            String masterAddress,
            TreeSet<String> streamId,
            Configuration configuration) {
        return new TubeMQTableSink(
                physicalDataType,
                valueEncodingFormat,
                topic,
                masterAddress,
                streamId,
                configuration);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(MASTER_RPC);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FORMAT);
        options.add(TOPIC);
        options.add(CONSUME_GROUP);
        options.add(STREAMID);
        options.add(SESSION_KEY);
        options.add(BOOTSTRAP_FROM_MAX);
        options.add(TOPIC_PATTERN);
        options.add(AUDIT_KEYS);
        options.add(INLONG_METRIC);
        options.add(INLONG_AUDIT);
        return options;
    }

}
