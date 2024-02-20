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

package org.apache.inlong.sort.formats.inlongmsgbinlog;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.inlong.sort.formats.base.TableFormatOptions.ROW_FORMAT_INFO;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.ATTRIBUTE_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.IGNORE_ERRORS;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.INCLUDE_UPDATE_BEFORE;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.METADATA_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.TIME_FIELD_NAME;

/**
 * Table format factory for providing configured instances of InLongMsgCsv-to-row
 * serializer and deserializer.
 */
public final class InLongMsgBinlogFormatFactory implements DeserializationFormatFactory {

    public static final String IDENTIFIER = "inlong-msg-binlog";

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        return new InLongMsgBinlogDecodingFormat(formatOptions);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Stream.of(ROW_FORMAT_INFO).collect(Collectors.toSet());
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(TIME_FIELD_NAME);
        options.add(ATTRIBUTE_FIELD_NAME);
        options.add(METADATA_FIELD_NAME);
        options.add(IGNORE_ERRORS);
        options.add(INCLUDE_UPDATE_BEFORE);
        return options;
    }
}
