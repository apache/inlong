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

package org.apache.inlong.sort.kudu.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.inlong.sort.kudu.common.KuduOptions;
import org.apache.inlong.sort.kudu.common.KuduValidator;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.inlong.sort.base.Constants.INLONG_AUDIT;
import static org.apache.inlong.sort.base.Constants.INLONG_METRIC;
import static org.apache.inlong.sort.kudu.common.KuduOptions.*;
import static org.apache.inlong.sort.kudu.common.KuduValidator.CONNECTOR_TYPE_VALUE_KUDU;

/**
 * Factory for creating configured instances of {@link KuduDynamicTableSink}.
 */
public class KuduDynamicTableFactory
        implements
            DynamicTableSourceFactory,
            DynamicTableSinkFactory {

    private void validateProperties(DescriptorProperties descriptorProperties) {
        new SchemaValidator(true, false, false).validate(descriptorProperties);
        new KuduValidator().validate(descriptorProperties);
    }

    private Configuration getConfiguration(
            DescriptorProperties descriptorProperties) {
        Map<String, String> properties =
                descriptorProperties.getPropertiesWithPrefix(KuduValidator.CONNECTOR_PROPERTIES);

        Configuration configuration = new Configuration();
        for (Map.Entry<String, String> property : properties.entrySet()) {
            configuration.setString(property.getKey(), property.getValue());
        }

        return configuration;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        TableSchema schema = context.getCatalogTable().getSchema();
        FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        Configuration configuration = new Configuration();
        context.getCatalogTable().getOptions().forEach(configuration::setString);

        ReadableConfig options = helper.getOptions();
        String master = options.getOptional(CONNECTOR_MASTER).orElse(null);
        String tableName = options.getOptional(CONNECTOR_TABLE).orElse(null);
        String inlongMetric = options.getOptional(INLONG_METRIC).orElse(null);
        String auditHostAndPorts = options.getOptional(INLONG_AUDIT).orElse(null);

        // Query the fields through the kudu client and select the fields in the tableSchema
        return new KuduDynamicTableSink(
                context.getCatalogTable(),
                schema,
                master,
                tableName,
                configuration,
                inlongMetric,
                auditHostAndPorts);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        TableSchema schema = context.getCatalogTable().getSchema();
        FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        Configuration configuration = new Configuration();
        context.getCatalogTable().getOptions().forEach(configuration::setString);

        ReadableConfig options = helper.getOptions();
        String master = options.getOptional(CONNECTOR_MASTER).orElse(null);
        String tableName = options.getOptional(CONNECTOR_TABLE).orElse(null);
        String inlongMetric = options.getOptional(INLONG_METRIC).orElse(null);
        String auditHostAndPorts = options.getOptional(INLONG_AUDIT).orElse(null);

        return new KuduDynamicTableSource(
                schema,
                master,
                tableName,
                configuration,
                inlongMetric,
                auditHostAndPorts);
    }

    @Override
    public String factoryIdentifier() {
        return CONNECTOR_TYPE_VALUE_KUDU;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        HashSet<ConfigOption<?>> configOptions = new HashSet<>();
        configOptions.add(KuduOptions.CONNECTOR_TABLE);
        configOptions.add(KuduOptions.CONNECTOR_MASTER);
        return configOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FLUSH_MODE);
        options.add(MAX_CACHE_SIZE);
        options.add(MAX_CACHE_TIME);
        options.add(SINK_START_NEW_CHAIN);
        options.add(MAX_RETRIES);
        options.add(MAX_BUFFER_SIZE);
        options.add(WRITE_THREAD_COUNT);
        options.add(MAX_BUFFER_TIME);
        options.add(SINK_KEY_FIELD_NAMES);
        options.add(ENABLE_KEY_FIELD_CHECK);

        options.add(INLONG_METRIC);
        options.add(INLONG_AUDIT);
        return options;
    }
}
