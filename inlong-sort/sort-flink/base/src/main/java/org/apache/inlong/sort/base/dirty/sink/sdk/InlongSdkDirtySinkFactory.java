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

package org.apache.inlong.sort.base.dirty.sink.sdk;

import org.apache.inlong.sort.base.dirty.sink.DirtySink;
import org.apache.inlong.sort.base.dirty.sink.DirtySinkFactory;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

import static org.apache.inlong.sort.base.Constants.DIRTY_SIDE_OUTPUT_FIELD_DELIMITER;
import static org.apache.inlong.sort.base.Constants.DIRTY_SIDE_OUTPUT_FORMAT;
import static org.apache.inlong.sort.base.Constants.DIRTY_SIDE_OUTPUT_IGNORE_ERRORS;
import static org.apache.inlong.sort.base.Constants.DIRTY_SIDE_OUTPUT_LOG_ENABLE;

@Slf4j
public class InlongSdkDirtySinkFactory implements DirtySinkFactory {

    private static final String IDENTIFIER = "inlong-sdk";

    private static final ConfigOption<String> DIRTY_SIDE_OUTPUT_INLONG_MANAGER_ADDR =
            ConfigOptions.key("dirty.side-output.inlong-sdk.inlong-manager-addr")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The inlong manager addr to init inlong sdk");

    private static final ConfigOption<Integer> DIRTY_SIDE_OUTPUT_INLONG_MANAGER_PORT =
            ConfigOptions.key("dirty.side-output.inlong-sdk.inlong-manager-port")
                    .intType()
                    .defaultValue(8083)
                    .withDescription("The inlong manager port to init inlong sdk");

    private static final ConfigOption<String> DIRTY_SIDE_OUTPUT_INLONG_AUTH_ID =
            ConfigOptions.key("dirty.side-output.inlong-sdk.inlong-auth-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The inlong manager auth id to init inlong sdk");

    private static final ConfigOption<String> DIRTY_SIDE_OUTPUT_INLONG_AUTH_KEY =
            ConfigOptions.key("dirty.side-output.inlong-sdk.inlong-auth-key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The inlong manager auth id to init inlong sdk");

    private static final ConfigOption<String> DIRTY_SIDE_OUTPUT_INLONG_GROUP =
            ConfigOptions.key("dirty.side-output.inlong-sdk.inlong-group-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The inlong group id of dirty sink");

    private static final ConfigOption<String> DIRTY_SIDE_OUTPUT_INLONG_STREAM =
            ConfigOptions.key("dirty.side-output.inlong-sdk.inlong-stream-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The inlong stream id of dirty sink");

    @Override
    public <T> DirtySink<T> createDirtySink(DynamicTableFactory.Context context) {
        ReadableConfig config = Configuration.fromMap(context.getCatalogTable().getOptions());
        FactoryUtil.validateFactoryOptions(this, config);
        InlongSdkDirtyOptions options = getOptions(config);
        return new InlongSdkDirtySink<>(options,
                context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType());
    }

    private InlongSdkDirtyOptions getOptions(ReadableConfig config) {
        return InlongSdkDirtyOptions.builder()
                .inlongManagerAddr(config.get(DIRTY_SIDE_OUTPUT_INLONG_MANAGER_ADDR))
                .inlongManagerPort(config.get(DIRTY_SIDE_OUTPUT_INLONG_MANAGER_PORT))
                .sendToGroupId(config.get(DIRTY_SIDE_OUTPUT_INLONG_GROUP))
                .sendToStreamId(config.get(DIRTY_SIDE_OUTPUT_INLONG_STREAM))
                .csvFieldDelimiter(config.get(DIRTY_SIDE_OUTPUT_FIELD_DELIMITER))
                .inlongManagerAuthKey(config.get(DIRTY_SIDE_OUTPUT_INLONG_AUTH_KEY))
                .inlongManagerAuthId(config.get(DIRTY_SIDE_OUTPUT_INLONG_AUTH_ID))
                .ignoreSideOutputErrors(config.getOptional(DIRTY_SIDE_OUTPUT_IGNORE_ERRORS).orElse(true))
                .enableDirtyLog(true)
                .build();
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DIRTY_SIDE_OUTPUT_INLONG_MANAGER_ADDR);
        options.add(DIRTY_SIDE_OUTPUT_INLONG_AUTH_ID);
        options.add(DIRTY_SIDE_OUTPUT_INLONG_AUTH_KEY);
        options.add(DIRTY_SIDE_OUTPUT_INLONG_GROUP);
        options.add(DIRTY_SIDE_OUTPUT_INLONG_STREAM);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DIRTY_SIDE_OUTPUT_FORMAT);
        options.add(DIRTY_SIDE_OUTPUT_IGNORE_ERRORS);
        options.add(DIRTY_SIDE_OUTPUT_LOG_ENABLE);
        return options;
    }
}
