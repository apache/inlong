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

package org.apache.inlong.sort.paimon.table.sink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.paimon.flink.FlinkTableFactory;

import java.util.Set;

import static org.apache.inlong.sort.base.Constants.INLONG_AUDIT;
import static org.apache.inlong.sort.base.Constants.INLONG_METRIC;

public class PaimonTableInlongFactory extends FlinkTableFactory {

    public static final String SORT_CONNECTOR_IDENTIFY_PAIMON = "paimon-inlong";

    public PaimonTableInlongFactory() {
        super();
    }

    @Override
    public String factoryIdentifier() {
        return SORT_CONNECTOR_IDENTIFY_PAIMON;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        return super.createDynamicTableSink(context);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> configOptions = super.optionalOptions();
        configOptions.add(INLONG_METRIC);
        configOptions.add(INLONG_AUDIT);
        return configOptions;
    }

}
