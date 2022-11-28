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

package org.apache.inlong.sort.base.dirty.sink.log;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.inlong.sort.base.dirty.sink.DirtySink;
import org.apache.inlong.sort.base.dirty.sink.DirtySinkFactory;

import java.util.HashSet;
import java.util.Set;
import static org.apache.inlong.sort.base.Constants.DIRTY_SIDE_OUTPUT_FIELD_DELIMITER;
import static org.apache.inlong.sort.base.Constants.DIRTY_SIDE_OUTPUT_FORMAT;

/**
 * Log dirty sink factory
 */
public class LogDirtySinkFactory implements DirtySinkFactory {

    private static final String IDENTIFIER = "log";

    @Override
    public <T> DirtySink<T> createDirtySink(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        FactoryUtil.validateFactoryOptions(this, helper.getOptions());
        String format = helper.getOptions().get(DIRTY_SIDE_OUTPUT_FORMAT);
        String fieldDelimiter = helper.getOptions().get(DIRTY_SIDE_OUTPUT_FIELD_DELIMITER);
        return new LogDirtySink<>(format, fieldDelimiter,
                context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType());
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DIRTY_SIDE_OUTPUT_FORMAT);
        options.add(DIRTY_SIDE_OUTPUT_FIELD_DELIMITER);
        return options;
    }
}
