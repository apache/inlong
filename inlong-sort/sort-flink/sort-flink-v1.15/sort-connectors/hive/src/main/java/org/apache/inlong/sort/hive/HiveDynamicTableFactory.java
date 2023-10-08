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

package org.apache.inlong.sort.hive;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.connectors.hive.HiveTableSink;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.factories.HiveCatalogFactoryOptions;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.table.catalog.hive.factories.HiveCatalogFactoryOptions.DEFAULT_DATABASE;
import static org.apache.flink.table.catalog.hive.factories.HiveCatalogFactoryOptions.HADOOP_CONF_DIR;
import static org.apache.flink.table.catalog.hive.factories.HiveCatalogFactoryOptions.HIVE_CONF_DIR;
import static org.apache.flink.table.catalog.hive.factories.HiveCatalogFactoryOptions.HIVE_VERSION;
import static org.apache.flink.table.factories.FactoryUtil.SINK_PARALLELISM;

public class HiveDynamicTableFactory implements DynamicTableSinkFactory {

    private static final HiveConf hiveConf = new HiveConf();

    @Override
    public String factoryIdentifier() {
        return HiveCatalogFactoryOptions.IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HIVE_VERSION);
        options.add(DEFAULT_DATABASE);
        options.add(HIVE_CONF_DIR);
        options.add(HADOOP_CONF_DIR);
        return options;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        final boolean isHiveTable = HiveCatalog.isHiveTable(context.getCatalogTable().getOptions());
        Map<String, String> options = context.getCatalogTable().getOptions();
        if (isHiveTable) {
            updateHiveConf(options);
            Integer configuredParallelism = helper.getOptions().get(SINK_PARALLELISM);

            return new HiveTableSink(
                    context.getConfiguration(),
                    new JobConf(hiveConf),
                    context.getObjectIdentifier(),
                    context.getCatalogTable(),
                    configuredParallelism);
        } else {
            return FactoryUtil.createTableSink(
                    null, // we already in the factory of catalog
                    context.getObjectIdentifier(),
                    context.getCatalogTable(),
                    context.getConfiguration(),
                    context.getClassLoader(),
                    context.isTemporary());
        }
    }

    private void updateHiveConf(Map<String, String> properties) {
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            hiveConf.set(entry.getKey(), entry.getValue());
        }
    }

    public static HiveConf getHiveConf() {
        return hiveConf;
    }
}
