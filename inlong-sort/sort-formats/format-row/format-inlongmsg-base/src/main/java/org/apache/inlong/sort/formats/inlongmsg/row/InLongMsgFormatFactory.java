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

package org.apache.inlong.sort.formats.inlongmsg.row;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory.Context;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;

import java.util.HashSet;
import java.util.Set;

import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.CSV_EMPTY_STRING_AS_NULL;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.CSV_IGNORE_PARSE_ERRORS;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.CSV_IGNORE_TRAILING_UNMAPPABLE;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.CSV_INSERT_NULLS_FOR_MISSING_COLUMNS;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.IGNORE_PARSE_ERRORS;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.INNER_FORMAT;
import static org.apache.inlong.sort.formats.inlongmsg.InLongMsgOptions.validateDecodingFormatOptions;

/**
 * factory class for inLong msg format
 */
public final class InLongMsgFormatFactory
        implements
            DeserializationFormatFactory,
            SerializationFormatFactory {

    public static final String IDENTIFIER = "inlong-msg";

    public static final String INLONG_PREFIX = "inlong-msg.";

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(Context context,
            ReadableConfig formatOptions) {
        validateDecodingFormatOptions(formatOptions);

        final DeserializationFormatFactory innerFactory = FactoryUtil.discoverFactory(
                context.getClassLoader(),
                DeserializationFormatFactory.class,
                formatOptions.get(INNER_FORMAT));
        Configuration allOptions = Configuration.fromMap(context.getCatalogTable().getOptions());
        String innerFormatMetaPrefix = formatOptions.get(INNER_FORMAT) + ".";
        String innerFormatPrefix = INLONG_PREFIX + innerFormatMetaPrefix;
        DecodingFormat<DeserializationSchema<RowData>> innerFormat =
                innerFactory.createDecodingFormat(context, new DelegatingConfiguration(allOptions, innerFormatPrefix));
        return new InLongMsgDecodingFormat(innerFormat, innerFormatMetaPrefix, formatOptions);
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(Context context,
            ReadableConfig formatOptions) {
        throw new RuntimeException("Do not support inlong format serialize.");
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(INNER_FORMAT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(IGNORE_PARSE_ERRORS);
        options.add(CSV_IGNORE_TRAILING_UNMAPPABLE);
        options.add(CSV_INSERT_NULLS_FOR_MISSING_COLUMNS);
        options.add(CSV_EMPTY_STRING_AS_NULL);
        options.add(CSV_IGNORE_PARSE_ERRORS);
        return options;
    }
}
